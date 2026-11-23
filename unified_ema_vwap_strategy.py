"""Persistent, execution-free EMA9/EMA15 + session VWAP pullback strategy.

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
    """Find the nearest unmitigated, pre-entry FVG or displacement order block.

    Only candles at/before the closed crossover candle are considered. The zone
    must be directional, non-zero-volume, bounded, and have a minimum ATR width.
    """
    if cross_index < 2 or not _finite(atr) or atr <= 0:
        return None
    start = max(2, cross_index - 12)
    candidates = []
    for i in range(start, cross_index + 1):
        a, c = frame.iloc[i - 2], frame.iloc[i]
        if side == "BUY" and float(c.low) > float(a.high):
            lo, hi, kind = float(a.high), float(c.low), "FVG"
        elif side == "SELL" and float(c.high) < float(a.low):
            lo, hi, kind = float(c.high), float(a.low), "FVG"
        else:
            continue
        width = hi - lo
        if 0.05 * atr <= width <= 1.5 * atr and float(c.volume) > 0:
            # A later closed candle must not have fully traded through the gap.
            later = frame.iloc[i + 1:cross_index + 1]
            mitigated = (not later.empty and
                         ((side == "BUY" and float(later.low.min()) <= lo) or
                          (side == "SELL" and float(later.high.max()) >= hi)))
            if not mitigated:
                candidates.append({"low": lo, "high": hi, "type": kind,
                                   "timestamp": int(c.timestamp), "index": i})
    # Displacement OB fallback: final opposing candle before a decisive close
    # beyond the preceding five-bar structure, not an arbitrary candle near price.
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
            if 0.05 * atr <= hi - lo <= 1.5 * atr:
                later = frame.iloc[i + 1:cross_index + 1]
                mitigated = (not later.empty and
                             ((side == "BUY" and float(later.low.min()) <= lo) or
                              (side == "SELL" and float(later.high.max()) >= hi)))
                if not mitigated:
                    candidates.append({"low": lo, "high": hi, "type": "ORDER_BLOCK",
                                       "timestamp": int(candle.timestamp), "index": i})
    if not candidates:
        return None
    # Prefer the latest valid causal zone; FVG wins only on an exact timestamp tie.
    candidates.sort(key=lambda z: (z["index"], z["type"] == "FVG"), reverse=True)
    return candidates[0]


def indicators(frame: pd.DataFrame) -> Dict[str, Any]:
    """Calculate EMA9/15/50, ATR14 and per-UTC-day OHLC4 session VWAP."""
    x = _clean_closed(frame, None, 900)
    if len(x) < 51:
        return {"valid": False, "reason": "need_at_least_51_closed_candles"}
    c = x.close
    ema9 = c.ewm(span=9, adjust=False, min_periods=9).mean()
    ema15 = c.ewm(span=15, adjust=False, min_periods=15).mean()
    ema50 = c.ewm(span=50, adjust=False, min_periods=50).mean()
    prev = c.shift(1)
    tr = pd.concat([x.high - x.low, (x.high - prev).abs(), (x.low - prev).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    # A well-defined 24/7 market session: calendar day in UTC. Never mix sessions.
    utc_day = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.strftime("%Y-%m-%d")
    typical = (x.high + x.low + x.close) / 3.0
    # Requirement specifies OHLC-4, not the conventional HLC-3 typical price.
    typical = (x.open + x.high + x.low + x.close) / 4.0
    pv = typical * x.volume
    cum_v = x.volume.groupby(utc_day).cumsum()
    cum_pv = pv.groupby(utc_day).cumsum()
    vwap = cum_pv / cum_v.replace(0, np.nan)
    idx = len(x) - 1
    values = {"ema9": float(ema9.iloc[idx]), "ema15": float(ema15.iloc[idx]),
              "ema50": float(ema50.iloc[idx]), "atr14": float(atr.iloc[idx]),
              "vwap": float(vwap.iloc[idx]) if pd.notna(vwap.iloc[idx]) else None,
              "price": float(c.iloc[idx]), "candle_ts": int(x.timestamp.iloc[idx]),
              "session": str(utc_day.iloc[idx]), "timezone": "UTC"}
    values["valid"] = all(_finite(values[k]) for k in ("ema9", "ema15", "ema50", "atr14", "price")) and values["atr14"] > 0 and values["vwap"] is not None
    values["reason"] = "" if values["valid"] else ("zero_session_volume" if values["vwap"] is None else "invalid_indicator")
    return values


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
            closes = x.close
            e9 = closes.ewm(span=9, adjust=False, min_periods=9).mean()
            e15 = closes.ewm(span=15, adjust=False, min_periods=15).mean()
            cross_events = []
            # Scanner rotations may first observe a symbol after the cross; inspect
            # only the newest eight closed candles and retain the original event bar.
            first_cross = max(1, len(x) - MAX_CROSS_AGE - 1)
            for k in range(first_cross, len(x)):
                if e9.iloc[k-1] <= e15.iloc[k-1] and e9.iloc[k] > e15.iloc[k]:
                    cross_events.append((k, "BUY"))
                elif e9.iloc[k-1] >= e15.iloc[k-1] and e9.iloc[k] < e15.iloc[k]:
                    cross_events.append((k, "SELL"))
            if cross_events:
                i, cross_dir = cross_events[-1]
                if float(x.volume.iloc[i]) > 0:
                    utc = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.strftime("%Y-%m-%d")
                    tp = (x.open + x.high + x.low + x.close) / 4.0
                    pv = (tp * x.volume).groupby(utc).cumsum()
                    cv = x.volume.groupby(utc).cumsum()
                    cross_vwap = float(pv.iloc[i] / cv.iloc[i]) if cv.iloc[i] > 0 else None
                    cross_price = float(x.close.iloc[i])
                    valid_side = cross_vwap is not None and ((cross_dir == "BUY" and cross_price > cross_vwap) or (cross_dir == "SELL" and cross_price < cross_vwap))
                    same_event = old and old.crossover_ts == int(x.timestamp.iloc[i]) and old.direction == cross_dir
                    if not same_event and (old is None or old.crossover_ts != int(x.timestamp.iloc[i])):
                        if valid_side:
                            zone = _find_causal_zone(x, i, cross_dir, float(vals["atr14"]))
                            if zone is not None:
                                old = Setup(symbol=str(symbol), status="WAITING_FOR_PULLBACK", direction=cross_dir,
                                            crossover_ts=int(x.timestamp.iloc[i]), crossover_index=i,
                                            crossover_price=cross_price, ema9=float(e9.iloc[i]), ema15=float(e15.iloc[i]),
                                            vwap=cross_vwap, atr14=float(vals["atr14"]),
                                            causal_level=(zone["low"] + zone["high"]) / 2.0,
                                            causal_zone_low=zone["low"], causal_zone_high=zone["high"],
                                            causal_zone_type=zone["type"], causal_zone_ts=zone["timestamp"],
                                            last_candle_ts=int(x.timestamp.iloc[i]), updated_at=now_ms/1000,
                                            reason="closed_cross_vwap_aligned_causal_" + zone["type"].lower())
                            else:
                                old = Setup(symbol=str(symbol), status="INVALIDATED", direction=cross_dir,
                                            crossover_ts=int(x.timestamp.iloc[i]), crossover_index=i,
                                            crossover_price=cross_price, vwap=cross_vwap,
                                            last_candle_ts=int(x.timestamp.iloc[i]), updated_at=now_ms/1000,
                                            reason="no_valid_causal_ob_or_fvg", expiration_reason="NO_CAUSAL_ZONE")
                        else:
                            old = Setup(symbol=str(symbol), status="INVALIDATED", direction=cross_dir,
                                        crossover_ts=int(x.timestamp.iloc[i]), crossover_index=i,
                                        crossover_price=cross_price, vwap=cross_vwap,
                                        last_candle_ts=int(x.timestamp.iloc[i]), updated_at=now_ms/1000,
                                        reason="crossover_vwap_misaligned", expiration_reason="VWAP_MISALIGNED")
            if old is None:
                s = Setup(str(symbol), status="DISCOVERED", last_candle_ts=int(x.timestamp.iloc[-1]),
                          updated_at=now_ms/1000, reason="no_new_closed_ema9_ema15_cross")
                return self._persist(s)
            # Only advance once for each newly closed candle.
            latest_ts = int(x.timestamp.iloc[-1])
            if old.last_candle_ts == latest_ts:
                return self._persist(old)
            if not old.crossover_ts or old.status in ("INVALIDATED", "EXPIRED", "NO_CHASE", "ENTRY_EXECUTING", "TRADE_OPEN"):
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
                aligned = (price > vwap and float(e9.iloc[j]) > float(e15.iloc[j])) if side == "BUY" else (price < vwap and float(e9.iloc[j]) < float(e15.iloc[j]))
                invalid = (price < vwap or float(e9.iloc[j]) <= float(e15.iloc[j])) if side == "BUY" else (price > vwap or float(e9.iloc[j]) >= float(e15.iloc[j]))
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
                    zone_low, zone_high = old.causal_zone_low, old.causal_zone_high
                    if not (_finite(zone_low) and _finite(zone_high) and float(zone_low) < float(zone_high)):
                        old.status, old.reason, old.expiration_reason = "INVALIDATED", "causal_zone_missing_or_invalid", "NO_CAUSAL_ZONE"
                    else:
                        # A pullback must actually revisit the causal OB/FVG; EMA touches
                        # alone are not a substitute for a structural zone retest.
                        buffer = 0.10 * atr
                        touched = low <= float(zone_high) + buffer and high >= float(zone_low) - buffer
                        if touched:
                            old.pullback_type = str(old.causal_zone_type or "CAUSAL_ZONE")
                            old.status = "WAITING_FOR_REJECTION"
                            old.pullback_ts = latest_ts
                            old.reason = "causal_" + old.pullback_type.lower() + "_retest"
                        else:
                            old.status = "WAITING_FOR_PULLBACK"
                            old.reason = "waiting_for_causal_zone_retest"
                else:
                    o,h,l,c = map(float, (x.open.iloc[j], x.high.iloc[j], x.low.iloc[j], x.close.iloc[j]))
                    rng = max(h-l, 1e-12)
                    lower_wick, upper_wick = min(o,c)-l, h-max(o,c)
                    rejected = (c > o and lower_wick/rng >= .25 and c >= l+.60*rng) if side == "BUY" else (c < o and upper_wick/rng >= .25 and c <= h-.60*rng)
                    if rejected and aligned:
                        old.rejection_ts, old.rejection_high, old.rejection_low = latest_ts, h, l
                        old.status, old.reason = "WAITING_FOR_CONFIRMATION", "rejection_candle_recorded_confirmation_must_be_later"
                    else:
                        old.status, old.reason = "WAITING_FOR_REJECTION", "valid_rejection_candle_not_seen"
            old.ema9, old.ema15, old.ema50, old.vwap, old.atr14 = float(e9.iloc[-1]), float(e15.iloc[-1]), float(vals["ema50"]), float(vals["vwap"]), float(vals["atr14"])
            old.last_candle_ts, old.updated_at = latest_ts, now_ms/1000
            return self._persist(old)

    def authorize(self, symbol: str, current_price: float, tp1: float, *, minimum_space_atr: float = .35) -> Dict[str, Any]:
        """Final forward-space/no-chase check; TP1 must come from existing GGP target engine."""
        with self._lock:
            s = self.get(symbol)
            if s is None:
                return {"allowed": False, "reason": "strategy_setup_missing"}
            if s.status != "ENTRY_READY":
                return {"allowed": False, "reason": "strategy_not_entry_ready", "status": s.status}
            if (not all(_finite(v) for v in (current_price, tp1, s.atr14, s.causal_level,
                                                     s.causal_zone_low, s.causal_zone_high))
                    or float(s.atr14) <= 0 or float(s.causal_zone_low) >= float(s.causal_zone_high)
                    or not s.causal_zone_type):
                s.status, s.reason = "WAITING_FOR_CONFIRMATION", "tp1_atr_or_causal_zone_unknown"
                self._persist(s)
                return {"allowed": False, "reason": s.reason}
            side = s.direction
            zone_edge = float(s.causal_zone_high) if side == "BUY" else float(s.causal_zone_low)
            extension = max(0.0, (float(current_price)-zone_edge)/float(s.atr14) if side == "BUY"
                            else (zone_edge-float(current_price))/float(s.atr14))
            ahead = float(tp1) > float(current_price) if side == "BUY" else float(tp1) < float(current_price)
            space = (float(tp1)-float(current_price))/float(s.atr14) if side == "BUY" else (float(current_price)-float(tp1))/float(s.atr14)
            if extension > MAX_EXTENSION_ATR:
                s.status, s.reason, s.entry_status = "NO_CHASE", "extension_exceeds_1_5_atr", "NO_CHASE"
            elif not ahead or space < minimum_space_atr:
                s.status, s.reason, s.entry_status = "INVALIDATED", "tp1_not_forward_or_insufficient_space", "REJECTED"
            else:
                s.reason, s.entry_status = "entry_authorized_by_ema_vwap_strategy", "ENTRY_AUTHORIZED"
            s.updated_at = time.time()
            self._persist(s)
            return {"allowed": s.entry_status == "ENTRY_AUTHORIZED", "reason": s.reason,
                    "extension_atr": extension, "tp1_space_atr": space, "status": s.status}

    def mark_executing(self, symbol: str) -> bool:
        """Consume a prior authorization exactly once before handing off to execution."""
        with self._lock:
            s = self.get(symbol)
            if not s or s.status != "ENTRY_READY" or s.entry_status != "ENTRY_AUTHORIZED":
                return False
            if not s.rejection_ts or not s.confirmation_ts or s.confirmation_ts <= s.rejection_ts:
                return False
            s.status, s.entry_status, s.updated_at = "ENTRY_EXECUTING", "ENTRY_EXECUTING", time.time()
            self._persist(s)
            return True

    def mark_open(self, symbol: str) -> None:
        with self._lock:
            s = self.get(symbol)
            if s:
                s.status, s.entry_status, s.updated_at = "TRADE_OPEN", "TRADE_OPEN", time.time()
                self._persist(s)

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
