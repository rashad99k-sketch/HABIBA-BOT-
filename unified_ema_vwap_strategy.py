"""EMA9 x Session-VWAP crossover inside causal demand/supply zones.

The engine is an entry *decision* engine only. It never submits, amends or
manages an exchange order.

Rules enforced here:

* EMA9 is calculated from closed ``close`` prices only.
* VWAP is cumulative OHLC4 for the UTC session and resets at 00:00 UTC.
* A signal exists only when EMA9 crosses VWAP on the newest **closed** candle.
* BUY requires a causal bullish zone (Demand/FVG or Bullish Order Block).
  SELL requires a causal bearish zone (Supply/FVG or Bearish Order Block).
* The zone must already exist at the cross candle. No candle after the cross is
  read. Pullback/retest/rejection are diagnostic evidence only, never gates.
* Every completed decision emits an exact diagnostic tag and machine code.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import logging
import math
import os
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

try:  # Reuse the project's existing causal order-block engine; do not clone it.
    from roro_confluence import find_causal_order_block
except Exception:  # pragma: no cover - defensive for isolated tooling
    find_causal_order_block = None  # type: ignore[assignment]

REQUIRED = ("timestamp", "open", "high", "low", "close", "volume")

MAX_CROSS_AGE = 1
MAX_CROSS_AGE_BARS = MAX_CROSS_AGE
MAX_EXTENSION_ATR = 1.5
MAX_ZONE_AGE_BARS = 48
FRESH_ZONE_MAX_AGE_BARS = 8
MIN_ZONE_WIDTH_ATR = 0.10
MIN_ZONE_DISPLACEMENT_ATR = 0.50
MAX_PERSISTED_SETUPS = 1000

ZONE_ALLOWED = frozenset({"VALID_ZONE", "FRESH_ZONE", "TESTED_ZONE"})
ZONE_ALL_STATES = frozenset({
    "VALID_ZONE", "FRESH_ZONE", "TESTED_ZONE", "WEAK_ZONE",
    "INVALIDATED_ZONE", "ENTRY_WINDOW_MISSED",
})
ZONE_TO_CODE = {
    "VALID_ZONE": "VALID_ZONE",
    "FRESH_ZONE": "FRESH_ZONE",
    "TESTED_ZONE": "TESTED_ZONE",
    "WEAK_ZONE": "INVALID_ZONE",
    "INVALIDATED_ZONE": "INVALID_ZONE",
    "ENTRY_WINDOW_MISSED": "ENTRY_WINDOW_MISSED",
}
# The exact machine codes consumed by the orchestration layer and tests.
REJECT_CODES = frozenset({
    "NO_CROSS", "INVALID_ZONE", "WRONG_ZONE_DIRECTION", "STALE_MARKET_DATA",
    "INCOMPLETE_CANDLE", "DUPLICATE_SIGNAL", "ENTRY_WINDOW_MISSED",
    "EXECUTION_REJECTED", "POSITION_NOT_CONFIRMED", "TRADE_HANDOFF_FAILED",
})
TERMINAL_STATUSES = frozenset({
    "NO_CROSS", "REJECTED", "INVALIDATED", "EXPIRED", "NO_CHASE", "TRADE_OPEN",
})

DIAGNOSTIC_TAGS = (
    "[EMA9_VWAP_CROSS]", "[ZONE_DETECTED]", "[ZONE_QUALITY]", "[ENTRY_VALIDATION]",
    "[ENTRY_AUTHORIZED]", "[ENTRY_REJECTED]", "[ENTRY_EXECUTION_STARTED]",
    "[ENTRY_EXECUTION_CONFIRMED]", "[TRADE_HANDOFF]", "[TRADE_CLASSIFIED]",
)
_ENTRY_LOG = logging.getLogger("ema9_vwap_zone_entry")


def emit_entry_diagnostic(tag: str, **fields: Any) -> None:
    """Emit one exact, structured entry diagnostic tag (no exchange side effects)."""
    if tag not in DIAGNOSTIC_TAGS:  # fail closed against typo'd tags
        raise ValueError(f"unknown diagnostic tag: {tag}")
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        try:
            parts.append(f"{key}={float(value):.6g}" if isinstance(value, (int, float, np.floating)) and not isinstance(value, bool) else f"{key}={value}")
        except Exception:
            parts.append(f"{key}={value}")
    _ENTRY_LOG.info("%s %s", tag, " ".join(parts))


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _clean_closed(frame: pd.DataFrame, now_ms: Optional[int], timeframe_s: int) -> pd.DataFrame:
    """Return validated, sorted, de-duplicated, fully closed OHLCV rows only."""
    if not isinstance(frame, pd.DataFrame) or any(c not in frame.columns for c in REQUIRED):
        return pd.DataFrame(columns=REQUIRED)
    x = frame.loc[:, REQUIRED].copy()
    for col in REQUIRED:
        x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna().sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    if x.empty or (x.volume < 0).any() or (x[["open", "high", "low", "close"]] <= 0).any().any():
        return pd.DataFrame(columns=REQUIRED)
    if (x.high < x[["open", "close", "low"]].max(axis=1)).any():
        return pd.DataFrame(columns=REQUIRED)
    if (x.low > x[["open", "close", "high"]].min(axis=1)).any():
        return pd.DataFrame(columns=REQUIRED)
    if now_ms is not None:
        # Exchange OHLCV timestamps are Unix milliseconds; seconds input is ambiguous.
        if float(x.timestamp.iloc[-1]) < 100_000_000_000:
            return pd.DataFrame(columns=REQUIRED)
        x = x[x.timestamp + timeframe_s * 1000 <= int(now_ms)].reset_index(drop=True)
    return x


def _data_state(frame: pd.DataFrame, timeframe_s: int, now_ms: int) -> str:
    """Classify missing closed data as forming-candle vs stale/corrupt feed."""
    if not isinstance(frame, pd.DataFrame) or frame.empty or any(c not in frame.columns for c in REQUIRED):
        return "STALE_MARKET_DATA"
    try:
        newest = pd.to_numeric(frame["timestamp"], errors="coerce").max()
    except Exception:
        return "STALE_MARKET_DATA"
    if not _finite(newest):
        return "STALE_MARKET_DATA"
    if float(newest) + timeframe_s * 1000 > int(now_ms):
        return "INCOMPLETE_CANDLE"
    return "STALE_MARKET_DATA"


def _zone_labels(kind: str, side: str) -> tuple[str, str, str]:
    """Return (user label, legacy engine type, directional OB label)."""
    if str(kind).upper() == "ORDER_BLOCK":
        ob = "BULLISH_ORDER_BLOCK" if side == "BUY" else "BEARISH_ORDER_BLOCK"
        return ob, "ORDER_BLOCK", ob
    return ("DEMAND_ZONE" if side == "BUY" else "SUPPLY_ZONE"), "FVG", "NONE"


def find_causal_zones(frame: pd.DataFrame, cross_index: int, side: str,
                      atr: float) -> List[Dict[str, Any]]:
    """List causal FVG/displacement zones visible at ``cross_index``.

    Only rows ``<= cross_index`` are read; mitigation/tests are counted from
    formation to the cross candle, never from candles in the future.
    """
    if (cross_index < 2 or side not in {"BUY", "SELL"}
            or not _finite(atr) or float(atr) <= 0 or cross_index >= len(frame)):
        return []
    atr = float(atr)
    zones: List[Dict[str, Any]] = []

    # 1) Three-candle fair-value gaps (demand/supply zones).
    for i in range(max(2, cross_index - 24), cross_index + 1):
        a, b, c = frame.iloc[i - 2], frame.iloc[i - 1], frame.iloc[i]
        mid_range = max(float(b.high) - float(b.low), 1e-12)
        mid_body = abs(float(b.close) - float(b.open))
        body_ratio = mid_body / mid_range
        if body_ratio < 0.50:
            continue
        if side == "BUY" and float(c.low) > float(a.high) and float(b.close) > float(b.open):
            lo, hi = float(a.high), float(c.low)
        elif side == "SELL" and float(c.high) < float(a.low) and float(b.close) < float(b.open):
            lo, hi = float(c.high), float(a.low)
        else:
            continue
        width = hi - lo
        if width < 0.05 * atr or width > 2.0 * atr or float(c.volume) <= 0:
            continue
        zones.append({"low": lo, "high": hi, "type": "FVG", "index": i,
                      "timestamp": int(c.timestamp), "width_atr": width / atr,
                      "displacement_atr": mid_body / atr, "body_ratio": body_ratio,
                      "zone_strength": 5.0 + min(2.0, mid_body / atr),
                      "source": "CAUSAL_FVG"})

    # 2) Displacement order block: final opposing body before a structural break.
    cross = frame.iloc[cross_index]
    prior = frame.iloc[max(0, cross_index - 5):cross_index]
    if len(prior) >= 3:
        body = abs(float(cross.close) - float(cross.open))
        broke = (float(cross.close) > float(prior.high.max()) if side == "BUY"
                 else float(cross.close) < float(prior.low.min()))
        if broke and body >= 0.5 * atr:
            for i in range(max(0, cross_index - 5), cross_index):
                candle = frame.iloc[i]
                opposing = (float(candle.open) > float(candle.close) if side == "BUY"
                            else float(candle.open) < float(candle.close))
                opposing_body = abs(float(candle.open) - float(candle.close))
                if not opposing or opposing_body < 0.15 * atr:
                    continue
                lo, hi = float(candle.low), float(candle.high)
                if 0.05 * atr <= hi - lo <= 2.0 * atr:
                    zones.append({"low": lo, "high": hi, "type": "ORDER_BLOCK", "index": i,
                                  "timestamp": int(candle.timestamp),
                                  "width_atr": (hi - lo) / atr,
                                  "displacement_atr": body / atr,
                                  "body_ratio": body / max(hi - lo, 1e-12),
                                  "zone_strength": 5.0 + min(2.0, body / atr),
                                  "source": "CAUSAL_DISPLACEMENT_OB"})

    # 3) The project's scored causal OB engine, supplied the causal slice only.
    if find_causal_order_block is not None:
        try:
            causal = frame.iloc[:cross_index + 1].reset_index(drop=True)
            ob = find_causal_order_block(
                causal, side, float(cross.close), atr, lookback=48)
            if isinstance(ob, dict):
                lo, hi = float(ob["low"]), float(ob["high"])
                if 0 < lo < hi and float(ob.get("timestamp", 0)) <= float(cross.timestamp):
                    matches = causal.index[causal.timestamp == int(float(ob["timestamp"]))]
                    idx = int(matches[0]) if len(matches) else cross_index - 1
                    zones.append({"low": lo, "high": hi, "type": "ORDER_BLOCK",
                                  "index": idx, "timestamp": int(float(ob["timestamp"])),
                                  "width_atr": (hi - lo) / atr,
                                  "displacement_atr": max(0.5, float(ob.get("strength", 5.0)) / 5.0),
                                  "body_ratio": 1.0,
                                  "zone_strength": float(ob.get("strength", 5.0)),
                                  "distance_atr": float(ob.get("distance_atr", 0.0)),
                                  "age_bars": float(ob.get("age_bars", idx)),
                                  "volume_ratio": float(ob.get("volume_ratio", 0.0)),
                                  "source": "RORO_CAUSAL_ORDER_BLOCK"})
        except Exception:
            pass

    # De-duplicate equivalent zones found by more than one detector.
    unique: List[Dict[str, Any]] = []
    seen = set()
    for zone in zones:
        key = (zone["type"], round(float(zone["low"]), 8), round(float(zone["high"]), 8),
               int(zone["timestamp"]))
        if key not in seen:
            seen.add(key)
            unique.append(zone)
    return unique


def _find_causal_zone(frame: pd.DataFrame, cross_index: int, side: str,
                      atr: float) -> Optional[Dict[str, Any]]:
    """Backward-compatible single-zone accessor (most relevant zone first)."""
    zones = find_causal_zones(frame, cross_index, side, atr)
    if not zones:
        return None
    price = float(frame.close.iloc[cross_index])
    zones.sort(key=lambda z: (zone_distance_atr(z, price, atr, side),
                              -float(z.get("zone_strength", 0.0)), -int(z["index"])))
    return zones[0]


def zone_distance_atr(zone: Dict[str, Any], price: float, atr: float, side: str) -> float:
    """ATR-normalized distance from price to the nearest zone edge (no chase)."""
    if not _finite(atr) or float(atr) <= 0 or not _finite(price):
        return math.inf
    low, high = float(zone["low"]), float(zone["high"])
    if low <= float(price) <= high:
        return 0.0
    gap = low - float(price) if float(price) < low else float(price) - high
    return max(0.0, gap) / float(atr)


def assess_zone_quality(frame: pd.DataFrame, cross_index: int, side: str,
                        atr: float, price: float, zone: Dict[str, Any]) -> Dict[str, Any]:
    """Classify a causal zone using only data available at the cross candle."""
    low, high = float(zone["low"]), float(zone["high"])
    result: Dict[str, Any] = {
        "status": "VALID_ZONE", "reason": "", "tests": 0, "distance_atr": None,
        "width_atr": None, "displacement_atr": None, "age_bars": None,
        "zone_strength": None, "liquidity_consumed": False, "inside_zone": False,
    }
    if not (0 < low < high) or not _finite(atr) or float(atr) <= 0:
        result.update({"status": "INVALIDATED_ZONE", "reason": "invalid_zone_boundaries"})
        return result

    atr = float(atr)
    start = min(cross_index + 1, int(zone.get("index", cross_index)) + 1)
    later = frame.iloc[start:cross_index + 1]
    tests = 0
    invalidated = False
    for _, candle in later.iterrows():
        touches = float(candle["low"]) <= high and float(candle["high"]) >= low
        if not touches:
            continue
        tests += 1
        if side == "BUY" and float(candle["close"]) < low - 0.5 * atr:
            invalidated = True
            break
        if side == "SELL" and float(candle["close"]) > high + 0.5 * atr:
            invalidated = True
            break

    distance = zone_distance_atr(zone, price, atr, side)
    width = (high - low) / atr
    displacement = float(zone.get("displacement_atr", 0.0))
    age = max(0, cross_index - int(zone.get("index", cross_index)))
    consumed = bool(zone.get("liquidity_consumed", tests >= 3))
    inside = low <= price <= high
    result.update({
        "tests": tests, "distance_atr": round(distance, 6),
        "width_atr": round(width, 6), "displacement_atr": round(displacement, 6),
        "age_bars": age, "zone_strength": zone.get("zone_strength"),
        "liquidity_consumed": consumed, "inside_zone": inside,
    })
    if invalidated:
        result.update({"status": "INVALIDATED_ZONE", "reason": "zone_far_edge_closed_through"})
    elif not inside:
        result.update({"status": "ENTRY_WINDOW_MISSED", "reason": "price_not_inside_causal_zone"})
    elif distance > MAX_EXTENSION_ATR:
        result.update({"status": "ENTRY_WINDOW_MISSED", "reason": "price_too_far_from_causal_zone"})
    elif (width < MIN_ZONE_WIDTH_ATR or displacement < MIN_ZONE_DISPLACEMENT_ATR
          or age > MAX_ZONE_AGE_BARS or consumed):
        result.update({"status": "WEAK_ZONE", "reason": "weak_displacement_or_spent_liquidity"})
    elif tests >= 1:
        result.update({"status": "TESTED_ZONE", "reason": "zone_tested_before_cross"})
    elif age <= FRESH_ZONE_MAX_AGE_BARS:
        result.update({"status": "FRESH_ZONE", "reason": "fresh_unmitigated_zone"})
    else:
        result["reason"] = "valid_unmitigated_causal_zone"
    return result


def indicator_series(frame: pd.DataFrame) -> Dict[str, Any]:
    """Closed-bar EMA9/15/50, ATR14 and UTC-session OHLC4 VWAP series."""
    x = _clean_closed(frame, None, 900)
    if len(x) < 51:
        return {"valid": False, "reason": "need_at_least_51_closed_candles"}
    c = x.close
    ema9 = c.ewm(span=9, adjust=False, min_periods=9).mean()
    ema15 = c.ewm(span=15, adjust=False, min_periods=15).mean()
    ema50 = c.ewm(span=50, adjust=False, min_periods=50).mean()
    previous = c.shift(1)
    tr = pd.concat([x.high - x.low, (x.high - previous).abs(),
                    (x.low - previous).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    utc_day = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.strftime("%Y-%m-%d")
    ohlc4 = (x.open + x.high + x.low + x.close) / 4.0
    volume = x.volume.groupby(utc_day).cumsum()
    pv = (ohlc4 * x.volume).groupby(utc_day).cumsum()
    vwap = pv / volume.replace(0, np.nan)
    return {"valid": True, "ema9": ema9, "ema15": ema15, "ema50": ema50,
            "atr14": atr, "vwap_series": vwap, "utc_session": utc_day}


def indicators(frame: pd.DataFrame) -> Dict[str, Any]:
    """Scalar closed-candle indicators; EMA15/50 are observational only."""
    values = indicator_series(frame)
    if not values.get("valid"):
        return values
    idx = len(frame) - 1
    e9, e15, e50 = (values[k] for k in ("ema9", "ema15", "ema50"))
    atr, vwap = values["atr14"], values["vwap_series"]
    result = {"ema9": float(e9.iloc[idx]), "ema15": float(e15.iloc[idx]),
              "ema50": float(e50.iloc[idx]), "atr14": float(atr.iloc[idx]),
              "vwap": float(vwap.iloc[idx]) if pd.notna(vwap.iloc[idx]) else None,
              "price": float(frame.close.iloc[idx]),
              "candle_ts": int(frame.timestamp.iloc[idx]),
              "session": str(values["utc_session"].iloc[idx]), "timezone": "UTC"}
    result["valid"] = (
        all(_finite(result[k]) for k in ("ema9", "ema15", "ema50", "atr14", "price"))
        and result["atr14"] > 0 and result["vwap"] is not None
    )
    result["reason"] = "" if result["valid"] else (
        "zero_session_volume" if result["vwap"] is None else "invalid_indicator")
    return result


@dataclass
class Setup:
    symbol: str
    status: str = "NO_CROSS"
    direction: Optional[str] = None
    crossover_ts: Optional[int] = None
    crossover_index: Optional[int] = None
    crossover_price: Optional[float] = None
    ema9: Optional[float] = None
    ema15: Optional[float] = None
    ema50: Optional[float] = None
    entry_path: Optional[str] = None
    vwap: Optional[float] = None
    atr14: Optional[float] = None
    # Observational history only. None of these fields gates a new entry.
    pullback_type: Optional[str] = None
    pullback_ts: Optional[int] = None
    rejection_ts: Optional[int] = None
    rejection_high: Optional[float] = None
    rejection_low: Optional[float] = None
    confirmation_ts: Optional[int] = None
    causal_level: Optional[float] = None
    causal_zone_low: Optional[float] = None
    causal_zone_high: Optional[float] = None
    causal_zone_type: Optional[str] = None
    causal_zone_ts: Optional[int] = None
    zone_label: Optional[str] = None
    zone_quality: Optional[str] = None
    zone_quality_reason: Optional[str] = None
    zone_tests: Optional[int] = None
    zone_distance_atr: Optional[float] = None
    zone_width_atr: Optional[float] = None
    zone_displacement_atr: Optional[float] = None
    zone_age_bars: Optional[int] = None
    zone_strength: Optional[float] = None
    order_block_type: Optional[str] = None
    zone_source: Optional[str] = None
    last_candle_ts: Optional[int] = None
    last_error_code: Optional[str] = None
    reason_code: Optional[str] = None
    duplicate: bool = False
    data_source: Optional[str] = None
    data_age_ms: Optional[int] = None
    diagnostic_tags: List[str] = field(default_factory=list)
    handoff_id: Optional[str] = None
    updated_at: float = 0.0
    reason: str = ""
    entry_status: str = "NOT_READY"
    expiration_reason: Optional[str] = None
    trend_evidence: Optional[Dict[str, Any]] = None


class UnifiedEMAVWAPPullbackEngine:
    """One persistent setup per symbol with closed-bar idempotency.

    The historical class name is retained for API compatibility. The lifecycle
    is the requested immediate EMA9/VWAP cross + causal-zone decision.
    """

    def __init__(self, path: str, timeframe_s: int = 900):
        self.path = path
        self.timeframe_s = int(timeframe_s)
        self.max_data_age_ms = max(2 * self.timeframe_s * 1000, 180_000)
        self._lock = threading.RLock()
        self._items: Dict[str, Dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------ storage
    def _load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self._items = {str(k): v for k, v in raw.items() if isinstance(v, dict)}
        except (OSError, ValueError):
            self._items = {}

    def _save(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".ema-vwap-", dir=directory, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._items, f, sort_keys=True, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def get(self, symbol: str) -> Optional[Setup]:
        with self._lock:
            raw = self._items.get(str(symbol))
            if not raw:
                return None
            try:
                return Setup(**raw)
            except TypeError:
                return None

    def all(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._items.items()}

    def _persist(self, setup: Any) -> Dict[str, Any]:
        updated = asdict(setup) if isinstance(setup, Setup) else dict(setup)
        symbol = str(updated.get("symbol") or "")
        previous = self._items.get(symbol)
        if previous:
            now_comparable = dict(updated)
            old_comparable = dict(previous)
            now_comparable.pop("updated_at", None)
            old_comparable.pop("updated_at", None)
            if now_comparable == old_comparable:
                updated["updated_at"] = previous.get("updated_at", updated["updated_at"])
        self._items[symbol] = updated
        if len(self._items) > MAX_PERSISTED_SETUPS:
            candidates = sorted(
                ((k, v) for k, v in self._items.items() if k != symbol),
                key=lambda kv: (0 if kv[1].get("status") in TERMINAL_STATUSES else 1,
                                float(kv[1].get("updated_at", 0.0) or 0.0)),
            )
            while len(self._items) > MAX_PERSISTED_SETUPS and candidates:
                self._items.pop(candidates.pop(0)[0], None)
        if previous != self._items[symbol] or previous is None:
            self._save()
        return dict(self._items[symbol])

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _result(setup: Setup, code: str, **extra: Any) -> Dict[str, Any]:
        result = asdict(setup)
        result.update({"reason_code": code})
        result.update(extra)
        return result

    @staticmethod
    def _tag(setup: Setup, tag: str) -> None:
        if tag not in setup.diagnostic_tags:
            setup.diagnostic_tags.append(tag)

    def _reject(self, setup: Setup, code: str, reason: str, now_ms: int,
                **extra: Any) -> Dict[str, Any]:
        if code not in REJECT_CODES:
            raise ValueError(f"unsupported reject code: {code}")
        setup.status = "REJECTED"
        setup.entry_status = "BLOCKED"
        setup.reason = reason
        setup.last_error_code = code
        setup.expiration_reason = code
        setup.updated_at = now_ms / 1000.0
        for key, value in extra.items():
            if hasattr(setup, key):
                setattr(setup, key, value)
        self._tag(setup, "[ENTRY_REJECTED]")
        emit_entry_diagnostic("[ENTRY_REJECTED]", symbol=setup.symbol,
                              reason_code=code, reason=reason, **extra)
        return self._persist(self._result(setup, code))

    # --------------------------------------------------------------- evaluation
    def evaluate(self, symbol: str, frame: pd.DataFrame,
                 now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Evaluate one closed EMA9/VWAP cross and its causal zone exactly once."""
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        x = _clean_closed(frame, now_ms, self.timeframe_s)
        with self._lock:
            old = self.get(symbol)
            if x.empty:
                code = _data_state(frame, self.timeframe_s, now_ms)
                setup = old or Setup(str(symbol))
                return self._reject(setup, code, code.lower(), now_ms)

            latest_ts = int(x.timestamp.iloc[-1])
            data_age = max(0, now_ms - latest_ts)
            if data_age > self.max_data_age_ms:
                setup = old or Setup(str(symbol))
                return self._reject(setup, "STALE_MARKET_DATA", "stale_market_data",
                                    now_ms, last_candle_ts=latest_ts, data_age_ms=data_age)

            # Same closed candle is never advanced twice, including after restart.
            if old is not None and old.last_candle_ts == latest_ts:
                code = "DUPLICATE_SIGNAL" if old.crossover_ts is not None else (
                    old.last_error_code or "NO_CROSS")
                if old.crossover_ts is not None:
                    emit_entry_diagnostic("[ENTRY_REJECTED]", symbol=symbol,
                                          reason_code=code, reason="same_closed_candle_seen_again")
                return self._persist(self._result(old, code, duplicate=True))

            # A cross that exists only on the still-forming candle is reported as
            # incomplete; never promote it and never mix it into VWAP/EMA state.
            raw_closed = _clean_closed(frame, None, self.timeframe_s)
            if len(raw_closed) > len(x):
                forming_values = indicator_series(raw_closed)
                if forming_values.get("valid"):
                    forming_gap = (forming_values["ema9"] - forming_values["vwap_series"])
                    if len(forming_gap) >= 2:
                        previous_gap, newest_gap = float(forming_gap.iloc[-2]), float(forming_gap.iloc[-1])
                        if ((previous_gap <= 0 < newest_gap) or (previous_gap >= 0 > newest_gap)):
                            return self._reject(
                                old or Setup(str(symbol)), "INCOMPLETE_CANDLE",
                                "ema9_vwap_cross_exists_only_on_forming_candle", now_ms,
                                last_candle_ts=latest_ts, data_age_ms=data_age)

            values = indicator_series(x)
            if not values.get("valid"):
                return self._reject(old or Setup(str(symbol)), "STALE_MARKET_DATA",
                                    str(values.get("reason") or "invalid_indicators"), now_ms)

            snapshot = indicators(x)
            if not snapshot.get("valid") or snapshot.get("vwap") is None:
                code = "STALE_MARKET_DATA"
                return self._reject(old or Setup(str(symbol)), code,
                                    str(snapshot.get("reason") or code), now_ms)

            e9, vwap_series = values["ema9"], values["vwap_series"]
            k = len(x) - 1  # newest closed candle only
            gap_prev = float(e9.iloc[k - 1] - vwap_series.iloc[k - 1])
            gap_now = float(e9.iloc[k] - vwap_series.iloc[k])
            if not (_finite(gap_prev) and _finite(gap_now)):
                return self._reject(old or Setup(str(symbol)), "STALE_MARKET_DATA",
                                    "non_finite_ema_vwap_series", now_ms)

            cross_ts = int(x.timestamp.iloc[k])
            side = None
            if gap_prev <= 0 < gap_now:
                side = "BUY"
            elif gap_prev >= 0 > gap_now:
                side = "SELL"

            if side is None:
                setup = old or Setup(str(symbol))
                changed = setup.status != "NO_CROSS"
                setup.status, setup.entry_status = "NO_CROSS", "NOT_READY"
                setup.reason = "no_new_closed_ema9_vwap_cross"
                setup.last_error_code = "NO_CROSS"
                setup.expiration_reason = None
                setup.last_candle_ts = latest_ts
                setup.data_age_ms = data_age
                setup.data_source = "SCANNER_CACHE_CLOSED"
                setup.updated_at = now_ms / 1000.0
                if changed:
                    emit_entry_diagnostic("[ENTRY_REJECTED]", symbol=symbol,
                                          reason_code="NO_CROSS", reason=setup.reason)
                return self._persist(self._result(setup, "NO_CROSS"))

            close_price = float(x.close.iloc[k])
            vwap_now = float(vwap_series.iloc[k])
            ema_now = float(e9.iloc[k])
            aligned = ((close_price > vwap_now and ema_now > vwap_now) if side == "BUY"
                       else (close_price < vwap_now and ema_now < vwap_now))
            emit_entry_diagnostic(
                "[EMA9_VWAP_CROSS]", symbol=symbol, side=side, cross_ts=cross_ts,
                ema9=ema_now, vwap=vwap_now, close=close_price,
                source_ema="CLOSE", source_vwap="OHLC4_UTC_SESSION",
                candle="CLOSED", aligned=aligned)
            if not (vwap_now > 0 and aligned):
                setup = Setup(str(symbol), direction=side, crossover_ts=cross_ts,
                              crossover_index=k, crossover_price=close_price,
                              ema9=ema_now, vwap=vwap_now, atr14=float(snapshot["atr14"]),
                              last_candle_ts=latest_ts, data_age_ms=data_age,
                              data_source="SCANNER_CACHE_CLOSED")
                self._tag(setup, "[EMA9_VWAP_CROSS]")
                return self._reject(setup, "NO_CROSS", "vwap_alignment_failed",
                                    now_ms)

            atr = float(snapshot["atr14"])
            zones = find_causal_zones(x, k, side, atr)
            if not zones:
                setup = Setup(str(symbol), direction=side, crossover_ts=cross_ts,
                              crossover_index=k, crossover_price=close_price,
                              ema9=ema_now, vwap=vwap_now, atr14=atr,
                              last_candle_ts=latest_ts, data_age_ms=data_age,
                              data_source="SCANNER_CACHE_CLOSED")
                self._tag(setup, "[EMA9_VWAP_CROSS]")
                return self._reject(setup, "INVALID_ZONE", "no_causal_demand_or_supply_zone",
                                    now_ms)

            assessed = []
            for zone in zones:
                label, legacy_type, ob_type = _zone_labels(str(zone.get("type") or "FVG"), side)
                quality = assess_zone_quality(x, k, side, atr, close_price, zone)
                enriched = dict(zone)
                enriched.update({
                    "zone_label": label, "legacy_type": legacy_type,
                    "order_block_type": ob_type, "quality": quality,
                })
                assessed.append(enriched)
                emit_entry_diagnostic(
                    "[ZONE_DETECTED]", symbol=symbol, side=side, zone_type=legacy_type,
                    zone_label=label, zone_low=zone["low"], zone_high=zone["high"],
                    zone_ts=zone["timestamp"], source=zone.get("source"))
                emit_entry_diagnostic(
                    "[ZONE_QUALITY]", symbol=symbol, zone_status=quality["status"],
                    reason=quality["reason"], distance_atr=quality["distance_atr"],
                    width_atr=quality["width_atr"], age_bars=quality["age_bars"],
                    tests=quality["tests"])

            allowed = [z for z in assessed if z["quality"]["status"] in ZONE_ALLOWED]
            if not allowed:
                best = min(
                    assessed,
                    key=lambda z: (float(z["quality"]["distance_atr"] or math.inf),
                                   -float(z.get("zone_strength") or 0.0)),
                )
                status = str(best["quality"]["status"])
                code = ZONE_TO_CODE.get(status, "INVALID_ZONE")
                setup = Setup(str(symbol), direction=side, crossover_ts=cross_ts,
                              crossover_index=k, crossover_price=close_price,
                              ema9=ema_now, vwap=vwap_now, atr14=atr,
                              zone_label=best["zone_label"], zone_quality=status,
                              zone_quality_reason=best["quality"]["reason"],
                              causal_zone_low=float(best["low"]),
                              causal_zone_high=float(best["high"]),
                              causal_zone_type=best["legacy_type"],
                              causal_zone_ts=int(best["timestamp"]),
                              order_block_type=best["order_block_type"],
                              zone_source=str(best.get("source") or ""),
                              zone_tests=best["quality"]["tests"],
                              zone_distance_atr=best["quality"]["distance_atr"],
                              zone_width_atr=best["quality"]["width_atr"],
                              zone_displacement_atr=best["quality"]["displacement_atr"],
                              zone_age_bars=best["quality"]["age_bars"],
                              zone_strength=best["quality"]["zone_strength"],
                              last_candle_ts=latest_ts, data_age_ms=data_age,
                              data_source="SCANNER_CACHE_CLOSED")
                return self._reject(setup, code, "zone_quality:" + status, now_ms)

            chosen = min(
                allowed,
                key=lambda z: (float(z["quality"]["distance_atr"] or math.inf),
                               -float(z.get("zone_strength") or 0.0), -int(z["index"])),
            )
            label = str(chosen["zone_label"])
            expected = "DEMAND_ZONE" if side == "BUY" else "SUPPLY_ZONE"
            bullish_ob = "BULLISH_ORDER_BLOCK" if side == "BUY" else "BEARISH_ORDER_BLOCK"
            if label not in {expected, bullish_ob}:
                setup = Setup(str(symbol), direction=side, crossover_ts=cross_ts,
                              last_candle_ts=latest_ts, data_age_ms=data_age)
                return self._reject(setup, "WRONG_ZONE_DIRECTION",
                                    "zone_direction_does_not_match_cross", now_ms)

            quality = chosen["quality"]
            vwap_slope = float(vwap_series.iloc[k] - vwap_series.iloc[max(0, k - 4)]) / atr
            ema_slope = float(e9.iloc[k] - e9.iloc[max(0, k - 4)]) / atr
            setup = Setup(
                symbol=str(symbol), status="ENTRY_READY", entry_status="READY",
                direction=side, crossover_ts=cross_ts, crossover_index=k,
                crossover_price=close_price, ema9=ema_now,
                ema15=float(values["ema15"].iloc[k]), ema50=float(values["ema50"].iloc[k]),
                vwap=vwap_now, atr14=atr,
                causal_level=(float(chosen["low"]) + float(chosen["high"])) / 2.0,
                causal_zone_low=float(chosen["low"]), causal_zone_high=float(chosen["high"]),
                causal_zone_type=str(chosen["legacy_type"]),
                causal_zone_ts=int(chosen["timestamp"]), zone_label=label,
                zone_quality=str(quality["status"]),
                zone_quality_reason=str(quality["reason"]),
                zone_tests=int(quality["tests"]),
                zone_distance_atr=quality["distance_atr"],
                zone_width_atr=quality["width_atr"],
                zone_displacement_atr=quality["displacement_atr"],
                zone_age_bars=quality["age_bars"], zone_strength=quality["zone_strength"],
                order_block_type=str(chosen["order_block_type"]),
                zone_source=str(chosen.get("source") or "STRATEGY_CAUSAL_ZONE"),
                last_candle_ts=latest_ts, data_age_ms=data_age,
                data_source="SCANNER_CACHE_CLOSED", updated_at=now_ms / 1000.0,
                reason=f"EMA9_VWAP_CROSS_IN_{label}",
                trend_evidence={
                    "vwap_slope_atr": vwap_slope, "ema9_slope_atr": ema_slope,
                    "zone_status": quality["status"], "price_vs_vwap": "ABOVE" if side == "BUY" else "BELOW",
                    "last_closed_candle_ts": latest_ts,
                    "ema15_alignment": "OBSERVATIONAL_ONLY",
                },
            )
            for tag in ("[EMA9_VWAP_CROSS]", "[ZONE_DETECTED]", "[ZONE_QUALITY]", "[ENTRY_VALIDATION]"):
                self._tag(setup, tag)
            emit_entry_diagnostic(
                "[ENTRY_VALIDATION]", symbol=symbol, side=side, status="ENTRY_READY",
                zone_status=quality["status"], zone_label=label,
                distance_atr=quality["distance_atr"], cross_ts=cross_ts)
            return self._persist(self._result(setup, "ENTRY_READY"))

    # -------------------------------------------------------------- authorization
    def authorize(self, symbol: str, price: float, tp1: float, *,
                  side: Optional[str] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Authorize the original execution path; this function sends no order."""
        with self._lock:
            setup = self.get(symbol)
            def reject(code: str, message: str) -> Dict[str, Any]:
                if code not in REJECT_CODES:
                    raise ValueError(f"unsupported reject code: {code}")
                emit_entry_diagnostic("[ENTRY_REJECTED]", symbol=symbol,
                                      reason_code=code, reason=message)
                return {"allowed": False, "reason": code, "reason_code": code,
                        "status": setup.status if setup else "UNKNOWN", "message": message}
            if setup is None or setup.status != "ENTRY_READY":
                return reject("NO_CROSS", "setup is not in ENTRY_READY state")
            requested = str(side or "").strip().upper() if side else None
            if requested and requested != str(setup.direction or "").upper():
                return reject("WRONG_ZONE_DIRECTION", "requested side does not match setup direction")
            if setup.direction not in {"BUY", "SELL"}:
                return reject("WRONG_ZONE_DIRECTION", "setup direction is invalid")
            expected_label = "DEMAND_ZONE" if setup.direction == "BUY" else "SUPPLY_ZONE"
            expected_ob = "BULLISH_ORDER_BLOCK" if setup.direction == "BUY" else "BEARISH_ORDER_BLOCK"
            if setup.zone_label not in {expected_label, expected_ob}:
                return reject("WRONG_ZONE_DIRECTION", "authorized zone is not directional for the cross side")
            if now_ms is not None and setup.last_candle_ts is not None:
                if int(now_ms) - int(setup.last_candle_ts) > self.max_data_age_ms:
                    return reject("STALE_MARKET_DATA", "authorization data is older than the allowed window")
            if setup.zone_quality not in ZONE_ALLOWED or setup.causal_zone_type not in {"FVG", "ORDER_BLOCK"}:
                return reject("INVALID_ZONE", "zone quality or zone type is not authorized")
            values = (price, tp1, setup.atr14, setup.causal_zone_low,
                      setup.causal_zone_high, setup.crossover_price)
            if not all(_finite(v) for v in values):
                return reject("INVALID_ZONE", "price, target, ATR or zone boundaries are not finite")
            atr = float(setup.atr14)
            zone = {"low": float(setup.causal_zone_low), "high": float(setup.causal_zone_high)}
            if atr <= 0 or not (0 < zone["low"] < zone["high"]):
                return reject("INVALID_ZONE", "invalid ATR or zone boundaries")
            distance = zone_distance_atr(zone, float(price), atr, str(setup.direction))
            if not (zone["low"] <= float(price) <= zone["high"]) or distance > MAX_EXTENSION_ATR:
                setup.status, setup.entry_status = "REJECTED", "BLOCKED"
                setup.last_error_code = "ENTRY_WINDOW_MISSED"
                setup.expiration_reason = "ENTRY_WINDOW_MISSED"
                self._tag(setup, "[ENTRY_REJECTED]")
                self._persist(setup)
                return reject("ENTRY_WINDOW_MISSED", "price is too far from the causal zone")
            entry, target = float(price), float(tp1)
            if (setup.direction == "BUY" and target <= entry) or (setup.direction == "SELL" and target >= entry):
                return reject("NO_CROSS", "take-profit is not forward of entry")
            setup.entry_status = "AUTHORIZED"
            setup.reason = "EMA9_VWAP_CROSS_ZONE_AUTHORIZED"
            self._tag(setup, "[ENTRY_AUTHORIZED]")
            self._persist(setup)
            emit_entry_diagnostic(
                "[ENTRY_AUTHORIZED]", symbol=symbol, side=setup.direction,
                entry=entry, tp1=target, zone_type=setup.causal_zone_type,
                zone_quality=setup.zone_quality)
            return {"allowed": True, "reason": "AUTHORIZED", "reason_code": "ENTRY_AUTHORIZED",
                    "status": setup.status, "symbol": setup.symbol, "side": setup.direction,
                    "entry_price": entry, "tp1": target,
                    "zone_type": setup.causal_zone_type, "zone_label": setup.zone_label,
                    "order_block_type": setup.order_block_type,
                    "zone_quality": setup.zone_quality,
                    "zone_low": zone["low"], "zone_high": zone["high"],
                    "ema9": setup.ema9, "vwap": setup.vwap, "atr": atr,
                    "crossover_ts": setup.crossover_ts}

    def mark_executing(self, symbol: str) -> bool:
        """Consume an authorization exactly once before the native order call."""
        with self._lock:
            setup = self.get(symbol)
            if setup is None or setup.status != "ENTRY_READY" or setup.entry_status != "AUTHORIZED":
                return False
            setup.status = "ENTRY_EXECUTING"
            setup.entry_status = "ENTRY_EXECUTING"
            setup.reason = "AUTHORIZATION_CONSUMED_BY_EXECUTION"
            self._tag(setup, "[ENTRY_EXECUTION_STARTED]")
            self._persist(setup)
            emit_entry_diagnostic("[ENTRY_EXECUTION_STARTED]", symbol=symbol,
                                  side=setup.direction, crossover_ts=setup.crossover_ts)
            return True

    def mark_open(self, symbol: str) -> bool:
        """Mark open only after the original execution path reports success."""
        with self._lock:
            setup = self.get(symbol)
            if setup is None or setup.status != "ENTRY_EXECUTING":
                return False
            setup.status = "TRADE_OPEN"
            setup.entry_status = "TRADE_OPEN"
            setup.reason = "EXECUTION_CONFIRMED_TRADE_OPEN"
            self._tag(setup, "[ENTRY_EXECUTION_CONFIRMED]")
            self._persist(setup)
            emit_entry_diagnostic("[ENTRY_EXECUTION_CONFIRMED]", symbol=symbol,
                                  side=setup.direction)
            return True

    def mark_handoff(self, symbol: str, handoff_id: str) -> bool:
        """Record the one-time, post-confirmation management handoff."""
        with self._lock:
            setup = self.get(symbol)
            if (setup is None or setup.status != "TRADE_OPEN"
                    or setup.handoff_id or not handoff_id):
                return False
            setup.handoff_id = str(handoff_id)
            setup.entry_status = "HANDED_OFF"
            self._tag(setup, "[TRADE_HANDOFF]")
            self._persist(setup)
            emit_entry_diagnostic("[TRADE_HANDOFF]", symbol=symbol,
                                  handoff_id=setup.handoff_id)
            return True

    def mark_rejected_execution(self, symbol: str, code: str, reason: str) -> bool:
        """Release a consumed authorization after a rejected/unconfirmed execution."""
        if code not in REJECT_CODES:
            raise ValueError(f"unsupported reject code: {code}")
        with self._lock:
            setup = self.get(symbol)
            if (setup is None or setup.handoff_id
                    or setup.status not in {"ENTRY_EXECUTING", "TRADE_OPEN"}):
                return False
            setup.status, setup.entry_status = "REJECTED", "BLOCKED"
            setup.last_error_code, setup.expiration_reason = code, code
            setup.reason = reason
            self._tag(setup, "[ENTRY_REJECTED]")
            self._persist(setup)
            emit_entry_diagnostic("[ENTRY_REJECTED]", symbol=symbol,
                                  reason_code=code, reason=reason)
            return True


def execution_handoff_allowed(engine: UnifiedEMAVWAPPullbackEngine, symbol: str, side: str,
                              price: float, tp1: float, context: Dict[str, Any]) -> bool:
    """Final fail-closed check at the shared ``execute_entry`` boundary."""
    if not isinstance(context, dict) or context.get("ema_vwap_authorized") is not True:
        return False
    side = str(side or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        return False
    if str(context.get("canonical_symbol") or "") != str(symbol):
        return False
    if str(context.get("side") or "").upper() != side:
        return False
    if not all(_finite(v) for v in (price, tp1, context.get("entry_price"), context.get("tp1"))):
        return False
    if not math.isclose(float(price), float(context["entry_price"]), rel_tol=1e-10, abs_tol=1e-10):
        return False
    if not math.isclose(float(tp1), float(context["tp1"]), rel_tol=1e-10, abs_tol=1e-10):
        return False
    setup = engine.get(symbol)
    return bool(setup and setup.status == "ENTRY_EXECUTING"
                and setup.entry_status == "ENTRY_EXECUTING"
                and setup.direction == side
                and setup.zone_quality in ZONE_ALLOWED
                and setup.causal_zone_type in {"FVG", "ORDER_BLOCK"})


# Explicit alias: the engine is now a zone-gated crossover engine, not a pullback one.
UnifiedEMA9VWAPZoneEngine = UnifiedEMAVWAPPullbackEngine
