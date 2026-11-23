"""Persistent, execution-free EMA9/Session-VWAP crossover pullback strategy.

All calculations consume closed OHLCV bars. Session VWAP uses OHLC4 and actual
provided volume, resetting at 00:00 UTC (explicit calendar-day session convention
for 24/7 markets). This module never submits or manages orders.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import os
import tempfile
import threading
import time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

REQUIRED = ("timestamp", "open", "high", "low", "close", "volume")
MAX_CROSS_AGE = 8
MAX_EXTENSION_ATR = 1.5
MAX_PERSISTED_SETUPS = 1000


def _finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def _clean_closed(frame: pd.DataFrame, now_ms: Optional[int], timeframe_s: int) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or any(c not in frame.columns for c in REQUIRED):
        return pd.DataFrame(columns=REQUIRED)
    x = frame.loc[:, REQUIRED].copy()
    for col in REQUIRED:
        x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna().sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    if x.empty or (x.volume < 0).any() or (x[["open", "high", "low", "close"]] <= 0).any().any():
        return pd.DataFrame(columns=REQUIRED)
    if (x.high < x[["open", "close", "low"]].max(axis=1)).any() or (x.low > x[["open", "close", "high"]].min(axis=1)).any():
        return pd.DataFrame(columns=REQUIRED)
    if now_ms is not None:
        # Project OHLCV timestamps are Unix milliseconds; reject ambiguous seconds input.
        if float(x.timestamp.iloc[-1]) < 100_000_000_000:
            return pd.DataFrame(columns=REQUIRED)
        x = x[x.timestamp + timeframe_s * 1000 <= int(now_ms)].reset_index(drop=True)
    return x


def _find_causal_zone(frame: pd.DataFrame, cross_index: int, side: str, atr: float) -> Optional[Dict[str, Any]]:
    """Find a directional, unmitigated FVG or displacement-backed order block.

    The search is causal: it never reads candles after the EMA cross. A plain
    opposing candle is not an order block; OB requires measurable displacement
    that breaks the preceding structure.
    """
    if cross_index < 2 or side not in {"BUY", "SELL"} or not _finite(atr) or atr <= 0:
        return None
    start = max(2, cross_index - 12)
    candidates = []
    for i in range(start, cross_index + 1):
        a, b, c = frame.iloc[i - 2], frame.iloc[i - 1], frame.iloc[i]
        middle_range = max(float(b.high) - float(b.low), 1e-12)
        middle_body = abs(float(b.close) - float(b.open))
        middle_body_ratio = middle_body / middle_range
        if (side == "BUY" and float(c.low) > float(a.high)
                and float(b.close) > float(b.open) and middle_body_ratio >= 0.50):
            lo, hi, kind = float(a.high), float(c.low), "FVG"
        elif (side == "SELL" and float(c.high) < float(a.low)
                and float(b.close) < float(b.open) and middle_body_ratio >= 0.50):
            lo, hi, kind = float(c.high), float(a.low), "FVG"
        else:
            continue
        width = hi - lo
        later = frame.iloc[i + 1:cross_index + 1]
        mitigated = (not later.empty and
                     ((side == "BUY" and float(later.low.min()) <= lo) or
                      (side == "SELL" and float(later.high.max()) >= hi)))
        if 0.05 * atr <= width <= 1.5 * atr and float(c.volume) > 0 and not mitigated:
            candidates.append({"low": lo, "high": hi, "type": kind,
                               "timestamp": int(c.timestamp), "index": i})

    # A displacement order block is the last meaningful opposing body before a
    # close through the five-bar structure, with a minimum body and range.
    cross = frame.iloc[cross_index]
    prior = frame.iloc[max(0, cross_index - 5):cross_index]
    if len(prior) >= 3:
        body = abs(float(cross.close) - float(cross.open))
        if side == "BUY":
            broke = float(cross.close) > float(prior.high.max())
            opposing = [i for i in range(max(0, cross_index - 5), cross_index)
                        if float(frame.open.iloc[i]) > float(frame.close.iloc[i])
                        and abs(float(frame.open.iloc[i]) - float(frame.close.iloc[i])) >= 0.20 * atr]
        else:
            broke = float(cross.close) < float(prior.low.min())
            opposing = [i for i in range(max(0, cross_index - 5), cross_index)
                        if float(frame.open.iloc[i]) < float(frame.close.iloc[i])
                        and abs(float(frame.open.iloc[i]) - float(frame.close.iloc[i])) >= 0.20 * atr]
        if broke and body >= 0.8 * atr and opposing:
            i = opposing[-1]
            candle = frame.iloc[i]
            lo, hi = float(candle.low), float(candle.high)
            later = frame.iloc[i + 1:cross_index + 1]
            mitigated = (not later.empty and
                         ((side == "BUY" and float(later.low.min()) <= lo) or
                          (side == "SELL" and float(later.high.max()) >= hi)))
            if 0.05 * atr <= hi - lo <= 1.5 * atr and not mitigated:
                candidates.append({"low": lo, "high": hi, "type": "ORDER_BLOCK",
                                   "timestamp": int(candle.timestamp), "index": i})
    if not candidates:
        return None
    candidates.sort(key=lambda z: (z["index"], z["type"] == "FVG"), reverse=True)
    return candidates[0]


def indicator_series(frame: pd.DataFrame) -> Dict[str, Any]:
    """Closed-bar EMA9/15/50, ATR14 and UTC session OHLC4 VWAP series."""
    x = _clean_closed(frame, None, 900)
    if len(x) < 51:
        return {"valid": False, "reason": "need_at_least_51_closed_candles"}
    c = x.close
    ema9 = c.ewm(span=9, adjust=False, min_periods=9).mean()
    ema15 = c.ewm(span=15, adjust=False, min_periods=15).mean()
    ema50 = c.ewm(span=50, adjust=False, min_periods=50).mean()
    previous = c.shift(1)
    tr = pd.concat([x.high-x.low, (x.high-previous).abs(), (x.low-previous).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    utc_day = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.strftime("%Y-%m-%d")
    ohlc4 = (x.open+x.high+x.low+x.close)/4.0
    volume = x.volume.groupby(utc_day).cumsum()
    pv = (ohlc4*x.volume).groupby(utc_day).cumsum()
    vwap = pv/volume.replace(0, np.nan)
    return {"valid": True, "ema9": ema9, "ema15": ema15, "ema50": ema50,
            "atr14": atr, "vwap_series": vwap, "utc_session": utc_day}


def indicators(frame: pd.DataFrame) -> Dict[str, Any]:
    """Calculate closed-candle EMA9/15/50, ATR14 and UTC OHLC4 Session VWAP."""
    values = indicator_series(frame)
    if not values.get("valid"):
        return values
    idx = len(frame)-1
    e9, e15, e50 = (values[k] for k in ("ema9", "ema15", "ema50"))
    atr, vwap = values["atr14"], values["vwap_series"]
    result = {"ema9": float(e9.iloc[idx]), "ema15": float(e15.iloc[idx]),
              "ema50": float(e50.iloc[idx]), "atr14": float(atr.iloc[idx]),
              "vwap": float(vwap.iloc[idx]) if pd.notna(vwap.iloc[idx]) else None,
              "price": float(frame.close.iloc[idx]), "candle_ts": int(frame.timestamp.iloc[idx]),
              "session": str(values["utc_session"].iloc[idx]), "timezone": "UTC"}
    result["valid"] = all(_finite(result[k]) for k in ("ema9","ema15","ema50","atr14","price")) and result["atr14"] > 0 and result["vwap"] is not None
    result["reason"] = "" if result["valid"] else ("zero_session_volume" if result["vwap"] is None else "invalid_indicator")
    return result


@dataclass
class Setup:
    symbol: str
    status: str = "DISCOVERED"
    direction: Optional[str] = None
    crossover_ts: Optional[int] = None
    crossover_index: Optional[int] = None
    crossover_price: Optional[float] = None
    ema9: Optional[float] = None
    ema15: Optional[float] = None
    entry_path: Optional[str] = None
    vwap: Optional[float] = None
    atr14: Optional[float] = None
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
    last_candle_ts: Optional[int] = None
    updated_at: float = 0.0
    reason: str = ""
    entry_status: str = "NOT_READY"
    expiration_reason: Optional[str] = None
    trend_evidence: Optional[Dict[str, Any]] = None


class UnifiedEMAVWAPPullbackEngine:
    """One setup per symbol; atomic JSON persistence and closed-bar idempotency."""
    def __init__(self, path: str, timeframe_s: int = 900):
        self.path = path
        self.timeframe_s = int(timeframe_s)
        self._lock = threading.RLock()
        self._items: Dict[str, Dict[str, Any]] = {}
        self._load()

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
                f.flush(); os.fsync(f.fileno())
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

    def evaluate(self, symbol: str, frame: pd.DataFrame, now_ms: Optional[int] = None) -> Dict[str, Any]:
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        x = _clean_closed(frame, now_ms, self.timeframe_s)
        with self._lock:
            old = self.get(symbol)
            if len(x) < 51:
                s = old or Setup(str(symbol))
                s.status, s.reason = "WAITING_FOR_CONFIRMATION", "insufficient_valid_closed_ohlcv"
                s.updated_at = now_ms / 1000
                return self._persist(s)
            vals = indicators(x)
            if not vals.get("valid") or vals.get("vwap") is None:
                s = old or Setup(str(symbol))
                s.status, s.reason = "WAITING_FOR_CONFIRMATION", vals.get("reason") or "vwap_unknown"
                s.updated_at = now_ms / 1000
                return self._persist(s)
            series = indicator_series(x)
            e9, e15 = series["ema9"], series["ema15"]
            vwap_series = series["vwap_series"]
            latest_ts = int(x.timestamp.iloc[-1])

            # Never replace an active lifecycle with a newer cross before it resolves.
            active_statuses = {"WAITING_FOR_PULLBACK", "WAITING_FOR_REJECTION",
                               "WAITING_FOR_CONFIRMATION", "ENTRY_READY",
                               "ENTRY_EXECUTING", "TRADE_OPEN"}
            active_setup = bool(old and old.status in active_statuses)

            if not active_setup:
                first_cross = max(1, len(x) - MAX_CROSS_AGE - 1)
                cross_events = []
                for k in range(first_cross, len(x)):
                    prev_gap = float(e9.iloc[k-1] - vwap_series.iloc[k-1])
                    current_gap = float(e9.iloc[k] - vwap_series.iloc[k])
                    if not all(_finite(v) for v in (prev_gap, current_gap)):
                        continue
                    if prev_gap <= 0 and current_gap > 0:
                        cross_events.append((k, "BUY"))
                    elif prev_gap >= 0 and current_gap < 0:
                        cross_events.append((k, "SELL"))
                if cross_events:
                    i, cross_dir = cross_events[-1]
                    cross_vwap = float(vwap_series.iloc[i])
                    cross_price = float(x.close.iloc[i])
                    valid_side = (
                        cross_vwap > 0 and
                        ((cross_dir == "BUY" and cross_price > cross_vwap and float(e9.iloc[i]) > cross_vwap)
                         or (cross_dir == "SELL" and cross_price < cross_vwap and float(e9.iloc[i]) < cross_vwap))
                    )
                    same_event = bool(old and old.crossover_ts == int(x.timestamp.iloc[i])
                                      and old.direction == cross_dir)
                    if not same_event:
                        if valid_side:
                            zone = _find_causal_zone(x, i, cross_dir, float(vals["atr14"]))
                            if zone is not None:
                                old = Setup(symbol=str(symbol), status="WAITING_FOR_PULLBACK",
                                            direction=cross_dir, crossover_ts=int(x.timestamp.iloc[i]),
                                            crossover_index=i, crossover_price=cross_price,
                                            ema9=float(e9.iloc[i]), ema15=float(e15.iloc[i]),
                                            vwap=cross_vwap, atr14=float(vals["atr14"]),
                                            causal_level=(zone["low"] + zone["high"]) / 2.0,
                                            causal_zone_low=zone["low"], causal_zone_high=zone["high"],
                                            causal_zone_type=zone["type"], causal_zone_ts=zone["timestamp"],
                                            last_candle_ts=int(x.timestamp.iloc[i]), updated_at=now_ms/1000,
                                            reason="closed_ema9_vwap_cross_causal_" + zone["type"].lower())
                            else:
                                old = Setup(symbol=str(symbol), status="INVALIDATED", direction=cross_dir,
                                            crossover_ts=int(x.timestamp.iloc[i]), crossover_index=i,
                                            crossover_price=cross_price, vwap=cross_vwap,
                                            last_candle_ts=latest_ts, updated_at=now_ms/1000,
                                            reason="no_valid_causal_ob_or_fvg",
                                            expiration_reason="NO_CAUSAL_ZONE")
                        else:
                            old = Setup(symbol=str(symbol), status="INVALIDATED", direction=cross_dir,
                                        crossover_ts=int(x.timestamp.iloc[i]), crossover_index=i,
                                        crossover_price=cross_price, vwap=cross_vwap,
                                        last_candle_ts=latest_ts, updated_at=now_ms/1000,
                                        reason="ema9_vwap_cross_not_price_aligned",
                                        expiration_reason="VWAP_MISALIGNED")

            if old is None:
                s = Setup(str(symbol), status="DISCOVERED", last_candle_ts=latest_ts,
                          updated_at=now_ms/1000, reason="no_new_closed_ema9_vwap_cross")
                return self._persist(s)
            if old.last_candle_ts == latest_ts:
                return self._persist(old)
            if not old.crossover_ts or old.status in ("INVALIDATED", "EXPIRED", "NO_CHASE",
                                                       "ENTRY_EXECUTING", "TRADE_OPEN"):
                old.last_candle_ts = latest_ts
                old.updated_at = now_ms/1000
                return self._persist(old)
            bars = int((x.timestamp > old.crossover_ts).sum())
            if bars > MAX_CROSS_AGE:
                old.status, old.reason, old.expiration_reason = "EXPIRED", "entry_window_expired", "ENTRY_WINDOW_EXPIRED"
            else:
                side = old.direction
                j = len(x)-1
                price, atr = float(x.close.iloc[j]), float(vals["atr14"])
                vwap = float(vals["vwap"])
                aligned = (price > vwap and float(e9.iloc[j]) > vwap) if side == "BUY" else (price < vwap and float(e9.iloc[j]) < vwap)
                invalid = (price < vwap or float(e9.iloc[j]) <= vwap) if side == "BUY" else (price > vwap or float(e9.iloc[j]) >= vwap)
                if invalid:
                    old.status, old.reason, old.expiration_reason = "INVALIDATED", "thesis_lost_ema_or_vwap", "THESIS_INVALIDATED"
                elif old.rejection_ts is not None and latest_ts > old.rejection_ts:
                    reject_high, reject_low = float(old.rejection_high), float(old.rejection_low)
                    confirmed = price > reject_high if side == "BUY" else price < reject_low
                    if confirmed and aligned:
                        old.confirmation_ts = latest_ts
                        old.status, old.reason = "ENTRY_READY", "separate_closed_confirmation_candle"
                    else:
                        old.status, old.reason = "WAITING_FOR_CONFIRMATION", "confirmation_candle_not_yet_valid"
                elif old.pullback_type is None:
                    low, high = float(x.low.iloc[j]), float(x.high.iloc[j])
                    zone_low = float(old.causal_zone_low) - 0.10 * atr
                    zone_high = float(old.causal_zone_high) + 0.10 * atr
                    touches_zone = high >= zone_low and low <= zone_high
                    # Three named paths, each still requiring the same valid causal zone.
                    # MICRO is a shallow first revisit near the zone edge; PULLBACK is
                    # a deeper traversal; RETEST is a revisit of the broken structure level.
                    distance_to_zone = (
                        max(0.0, low - zone_high) if side == "BUY"
                        else max(0.0, zone_low - high)
                    )
                    extension = max(0.0, (price - float(old.causal_zone_high))/atr if side == "BUY"
                                    else (float(old.causal_zone_low) - price)/atr)
                    prior_structure = float(old.crossover_price or old.causal_level or price)
                    structure_retest = (
                        low <= prior_structure + 0.10 * atr and high >= prior_structure - 0.10 * atr
                    )
                    shallow_touch = (
                        touches_zone and abs(price - float(old.causal_level)) <= 0.35 * atr
                    )
                    deep_touch = (
                        touches_zone and abs(price - float(old.causal_level)) > 0.35 * atr
                    )
                    if extension > MAX_EXTENSION_ATR:
                        old.status, old.reason, old.expiration_reason = "NO_CHASE", "price_left_causal_zone_by_more_than_1_5_atr", "ENTRY_WINDOW_MISSED"
                    elif shallow_touch:
                        old.pullback_type, old.entry_path, old.pullback_ts = "MICRO_PULLBACK", "MICRO_PULLBACK", latest_ts
                        old.status, old.reason = "WAITING_FOR_REJECTION", "micro_pullback_at_causal_zone_waiting_for_rejection"
                    elif deep_touch:
                        old.pullback_type, old.entry_path, old.pullback_ts = "PULLBACK", "PULLBACK", latest_ts
                        old.status, old.reason = "WAITING_FOR_REJECTION", "deep_causal_zone_pullback_waiting_for_rejection"
                    elif structure_retest:
                        old.pullback_type, old.entry_path, old.pullback_ts = "RETEST", "RETEST", latest_ts
                        old.status, old.reason = "WAITING_FOR_REJECTION", "broken_level_retest_waiting_for_rejection"
                    else:
                        old.status, old.reason = "WAITING_FOR_PULLBACK", "waiting_for_micro_pullback_pullback_or_retest"
                elif old.pullback_type is not None and old.rejection_ts is None:
                    # The rejection is a distinct, fully closed candle after the retest.
                    o, h, l, c = map(float, (x.open.iloc[j], x.high.iloc[j], x.low.iloc[j], x.close.iloc[j]))
                    candle_range = max(h - l, 1e-12)
                    lower_wick, upper_wick = min(o, c) - l, h - max(o, c)
                    rejected = (
                        c > o and lower_wick / candle_range >= 0.25 and c >= l + 0.60 * candle_range
                        if side == "BUY" else
                        c < o and upper_wick / candle_range >= 0.25 and c <= h - 0.60 * candle_range
                    )
                    if rejected and aligned and latest_ts > int(old.pullback_ts or old.crossover_ts):
                        old.rejection_ts, old.rejection_high, old.rejection_low = latest_ts, h, l
                        old.status, old.reason = "WAITING_FOR_CONFIRMATION", "rejection_candle_recorded_confirmation_must_be_later"
                    else:
                        old.status, old.reason = "WAITING_FOR_REJECTION", "valid_rejection_candle_not_seen"
            # Observe the original causal zone and trend evidence on each new closed bar.
            zone_ok = (_finite(old.causal_zone_low) and _finite(old.causal_zone_high)
                       and float(old.causal_zone_low) < float(old.causal_zone_high))
            zone_intact = bool(zone_ok and (
                float(x.close.iloc[-1]) >= float(old.causal_zone_low) if side == "BUY"
                else float(x.close.iloc[-1]) <= float(old.causal_zone_high)))
            avg_volume = float(x.volume.iloc[-21:-1].mean()) if len(x) >= 22 else 0.0
            volume_ratio = float(x.volume.iloc[-1]) / avg_volume if avg_volume > 0 else None
            ema9_slope_atr = ((float(e9.iloc[-1]) - float(e9.iloc[-5])) / atr) if atr > 0 and len(e9) >= 5 else None
            vwap_slope_atr = ((float(vwap_series.iloc[-1]) - float(vwap_series.iloc[-5])) / atr) if atr > 0 and len(vwap_series) >= 5 else None
            old.trend_evidence = {
                "zone_intact": zone_intact,
                "zone_status": "INTACT" if zone_intact else "WEAKENED_OR_INVALIDATED",
                "ema9_slope_atr": ema9_slope_atr,
                "vwap_slope_atr": vwap_slope_atr,
                "volume_ratio": volume_ratio,
                "price_vs_vwap": "ABOVE" if price > vwap else "BELOW" if price < vwap else "AT",
                "ema15_alignment": bool(float(e9.iloc[-1]) > float(e15.iloc[-1]) if side == "BUY" else float(e9.iloc[-1]) < float(e15.iloc[-1])),
                "last_closed_candle_ts": latest_ts,
            }
            old.ema9, old.ema15, old.ema50, old.vwap, old.atr14 = float(e9.iloc[-1]), float(e15.iloc[-1]), float(vals["ema50"]), float(vals["vwap"]), float(vals["atr14"])
            old.last_candle_ts, old.updated_at = latest_ts, now_ms/1000
            return self._persist(old)

    def _persist(self, s: Setup) -> Dict[str, Any]:
        updated = asdict(s)
        previous = self._items.get(s.symbol)
        # Do not rewrite the complete state file on every scanner poll when
        # neither the state nor the observed closed candle has changed.
        if previous:
            comparable = dict(updated)
            prior_comparable = dict(previous)
            comparable.pop("updated_at", None)
            prior_comparable.pop("updated_at", None)
            if comparable == prior_comparable:
                updated["updated_at"] = previous.get("updated_at", updated["updated_at"])
        self._items[s.symbol] = updated
        if len(self._items) > MAX_PERSISTED_SETUPS:
            terminal = {"INVALIDATED", "EXPIRED", "NO_CHASE", "TRADE_OPEN"}
            candidates = sorted(
                ((k, v) for k, v in self._items.items() if k != s.symbol),
                key=lambda kv: (0 if kv[1].get("status") in terminal else 1,
                                float(kv[1].get("updated_at", 0.0) or 0.0)),
            )
            while len(self._items) > MAX_PERSISTED_SETUPS and candidates:
                key, _ = candidates.pop(0)
                self._items.pop(key, None)
        if previous != self._items[s.symbol] or previous is None:
            self._save()
        return dict(self._items[s.symbol])


def execution_handoff_allowed(engine: UnifiedEMAVWAPPullbackEngine, symbol: str, side: str,
                              price: float, tp1: float, context: Dict[str, Any]) -> bool:
    """Fail-closed check used at the final execute_entry boundary for every caller."""
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
                and setup.direction == side and setup.rejection_ts is not None
                and setup.confirmation_ts is not None
                and setup.confirmation_ts > setup.rejection_ts)
