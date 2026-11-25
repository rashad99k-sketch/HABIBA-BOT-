"""GGP integration bridge for RORO institutional market analysis.

This layer is deliberately execution-free. It consumes GGP discovery sources,
performs closed-candle institutional analysis, and returns evidence to GGP's
existing single order-entry function.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import math
import time

import numpy as np
import pandas as pd

from roro_institutional import (
    ReversalEngine, ContinuationEngine, UnifiedEntryAuthority, DecisionState,
    StructuralStopLossEngine,
)
from roro_opportunity_pipeline import validate_closed_frame, classify_regime, confirmed_swings
from roro_confluence import classify_liquidity, find_causal_order_block, LiquidityEvent
from ggp_market_intelligence import (build_intelligence_decision, classify_price_pattern,
                                     build_support_resistance_zones, validate_order_block_zone,
                                     normalize_entry_type)
from naiso_unified_decision import project_decision_context, project_entry_behavior


REVERSAL = ReversalEngine()
CONTINUATION = ContinuationEngine()
AUTHORITY = UnifiedEntryAuthority()

TIMEFRAME_SECONDS = 900


def canonical_symbol(symbol: Any) -> str:
    """Return a stable base-quote key; keep the exchange symbol separately."""
    s = str(symbol or "").strip().upper().replace(" ", "")
    if not s:
        return ""
    s = s.split(":", 1)[0].replace("-PERP", "").replace("_PERP", "")
    s = s.replace("/", "").replace("-", "")
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def exchange_symbol(symbol: Any) -> str:
    """Normalize a USDT perpetual to CCXT's unified `BASE/USDT:USDT` format."""
    raw = str(symbol or "").strip().upper().replace(" ", "")
    if not raw:
        return ""
    if "/" in raw:
        base, quote_part = raw.split("/", 1)
        quote = quote_part.split(":", 1)[0]
        suffix = quote_part.split(":", 1)[1] if ":" in quote_part else None
        if suffix:
            return f"{base}/{quote}:{suffix}"
        return f"{base}/{quote}:USDT" if quote == "USDT" else f"{base}/{quote}"
    raw = raw.replace("-PERP", "").replace("_PERP", "")
    if raw.endswith(":USDT"):
        raw = raw[:-5]
    if raw.endswith("USDT"):
        base = raw[:-4]
    else:
        base = raw
    return f"{base}/USDT:USDT" if base else ""


def _frame(df: Any) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    x = df.copy()
    if "timestamp" not in x.columns:
        return pd.DataFrame()
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    if not all(c in x.columns for c in required):
        return pd.DataFrame()
    for col in required:
        x[col] = pd.to_numeric(x[col], errors="coerce")
    return x[required].dropna().sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def _indicator_snapshot(closed: pd.DataFrame, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Calculate one auditable indicator snapshot from validated closed candles."""
    if closed is None or len(closed) < 20:
        return {"quality": "DATA_UNAVAILABLE", "source": "CLOSED_CANDLE_FRAME",
                "missing": ["insufficient_closed_candles"]}
    close = pd.to_numeric(closed["close"], errors="coerce")
    ema9 = close.ewm(span=9, adjust=False, min_periods=9).mean()
    typical = (closed["open"] + closed["high"] + closed["low"] + closed["close"]) / 4.0
    stamps = pd.to_datetime(closed["timestamp"], unit="ms", utc=True, errors="coerce")
    session = stamps.dt.strftime("%Y-%m-%d")
    cumulative_volume = closed["volume"].groupby(session).cumsum()
    cumulative_tpv = (typical * closed["volume"]).groupby(session).cumsum()
    vwap = cumulative_tpv / cumulative_volume.replace(0, np.nan)
    delta = close.diff()
    gains = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    losses = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rsi = 100.0 - (100.0 / (1.0 + gains / losses.replace(0, np.nan)))
    atr_value = _atr(closed)
    price = float(close.iloc[-1])
    ema_value = float(ema9.iloc[-1]) if np.isfinite(ema9.iloc[-1]) else None
    vwap_value = float(vwap.iloc[-1]) if np.isfinite(vwap.iloc[-1]) else None
    rsi_value = float(rsi.iloc[-1]) if np.isfinite(rsi.iloc[-1]) else None
    slope = None
    if len(ema9) >= 3 and np.isfinite(atr_value) and atr_value > 0:
        slope = float((ema9.iloc[-1] - ema9.iloc[-3]) / atr_value)
    volume_base = float(closed["volume"].iloc[-21:-1].mean()) if len(closed) >= 21 else None
    volume_ratio = float(closed["volume"].iloc[-1] / volume_base) if volume_base and volume_base > 0 else None
    latest_ts = int(float(closed["timestamp"].iloc[-1]))
    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    age_seconds = max(0.0, (now_value - latest_ts) / 1000.0)
    quality = "FRESH" if age_seconds <= 2 * TIMEFRAME_SECONDS else "STALE_DATA"
    return {
        "ema9": ema_value, "vwap": vwap_value, "rsi": rsi_value,
        "atr": float(atr_value) if np.isfinite(atr_value) else None,
        "ema9_slope_atr": slope,
        "vwap_position": ("ABOVE" if price > vwap_value else "BELOW" if price < vwap_value else "AT_VWAP") if vwap_value is not None else "UNKNOWN",
        "volume_ratio": volume_ratio, "latest_closed_timestamp": latest_ts,
        "data_age_seconds": round(age_seconds, 3), "quality": quality,
        "source": "CLOSED_CANDLE_FRAME", "timeframe_seconds": TIMEFRAME_SECONDS,
    }


def _atr(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period + 1:
        return float("nan")
    prev = df.close.shift(1)
    tr = pd.concat([df.high - df.low, (df.high - prev).abs(), (df.low - prev).abs()], axis=1).max(axis=1)
    value = tr.rolling(period, min_periods=period).mean().iloc[-1]
    return float(value) if pd.notna(value) and np.isfinite(value) and value > 0 else float("nan")


def _adx(df: pd.DataFrame, period: int = 14) -> float:
    """Standard Wilder ADX with SMA-seeded Wilder smoothing; NaN means insufficient evidence."""
    if (
        not isinstance(df, pd.DataFrame)
        or not isinstance(period, int) or period < 2
        or len(df) < period * 2 + 1
        or not {"high", "low", "close"}.issubset(df.columns)
    ):
        return float("nan")
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy(dtype=float)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)
    if not (np.isfinite(high).all() and np.isfinite(low).all() and np.isfinite(close).all()):
        return float("nan")
    if np.any(high < low) or np.any(close > high) or np.any(close < low):
        return float("nan")

    up = np.diff(high, prepend=np.nan)
    down = -np.diff(low, prepend=np.nan)
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    previous_close = np.roll(close, 1)
    previous_close[0] = np.nan
    tr = np.nanmax(np.column_stack((
        high - low,
        np.abs(high - previous_close),
        np.abs(low - previous_close),
    )), axis=1)

    def _wilder_rma(values: np.ndarray) -> np.ndarray:
        out = np.full(len(values), np.nan, dtype=float)
        finite = np.isfinite(values)
        starts = np.flatnonzero(finite)
        if len(starts) < period:
            return out
        seed_start = None
        for candidate in starts:
            if candidate + period <= len(values) and finite[candidate:candidate + period].all():
                seed_start = int(candidate)
                break
        if seed_start is None:
            return out
        seed_index = seed_start + period - 1
        out[seed_index] = float(np.mean(values[seed_start:seed_index + 1]))
        for idx in range(seed_index + 1, len(values)):
            if not np.isfinite(values[idx]):
                return out
            out[idx] = (out[idx - 1] * (period - 1) + values[idx]) / period
        return out

    atr = _wilder_rma(tr)
    plus_smoothed = _wilder_rma(plus_dm)
    minus_smoothed = _wilder_rma(minus_dm)
    plus_di = np.divide(100.0 * plus_smoothed, atr, out=np.full(len(atr), np.nan), where=atr > 0)
    minus_di = np.divide(100.0 * minus_smoothed, atr, out=np.full(len(atr), np.nan), where=atr > 0)
    denominator = plus_di + minus_di
    dx = np.divide(
        100.0 * np.abs(plus_di - minus_di),
        denominator,
        out=np.full(len(denominator), np.nan),
        where=denominator > 0,
    )
    adx = _wilder_rma(dx)
    value = adx[-1] if len(adx) else np.nan
    return float(value) if np.isfinite(value) else float("nan")


def build_liquidity_pools(df: pd.DataFrame) -> Dict[str, List[float]]:
    """Build prior-known swing and clustered equal-high/low liquidity pools."""
    if not isinstance(df, pd.DataFrame) or len(df) < 12:
        return {"high_pools": [], "low_pools": []}
    highs, lows = confirmed_swings(df, left_right=2)
    atr = _atr(df)
    tolerance = max(atr * 0.12, float(df.close.iloc[-1]) * 0.00015) if np.isfinite(atr) else float(df.close.iloc[-1]) * 0.0003

    def cluster(values: List[float]) -> List[float]:
        if not values:
            return []
        groups: List[List[float]] = []
        for value in sorted(float(v) for _, v in values):
            placed = False
            for group in groups:
                if abs(value - float(np.mean(group))) <= tolerance:
                    group.append(value); placed = True; break
            if not placed:
                groups.append([value])
        # Keep both equal-level pools (2+ touches) and the most recent confirmed swings.
        clustered = [float(np.mean(g)) for g in groups if len(g) >= 2]
        recent = [float(v) for _, v in values[-4:]]
        out = []
        for v in clustered + recent:
            if not any(abs(v - old) <= tolerance * 0.5 for old in out):
                out.append(v)
        return out[-8:]

    return {"high_pools": cluster(highs), "low_pools": cluster(lows)}


def detect_fvg(df: pd.DataFrame, side: Optional[str] = None, atr: Optional[float] = None) -> Optional[Dict[str, Any]]:
    # Classic three-candle FVG; calculate deepest cumulative fill after formation.
    if not isinstance(df, pd.DataFrame) or len(df) < 3:
        return None
    a = float(atr) if atr is not None and np.isfinite(atr) and float(atr) > 0 else _atr(df)
    requested = str(side).upper() if side else None
    found = []
    for i in range(2, len(df)):
        first, middle, third = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        mid_body = abs(float(middle.close) - float(middle.open))
        if float(first.high) < float(third.low) and float(middle.close) > float(middle.open):
            lower, upper, direction = float(first.high), float(third.low), "BULLISH"
        elif float(first.low) > float(third.high) and float(middle.close) < float(middle.open):
            lower, upper, direction = float(third.high), float(first.low), "BEARISH"
        else:
            continue
        if requested in ("BUY", "SELL") and direction != ("BULLISH" if requested == "BUY" else "BEARISH"):
            continue
        size = upper - lower
        size_atr = size / a if np.isfinite(a) and a > 0 else None
        if size <= 0 or (size_atr is not None and size_atr < 0.08):
            continue
        after = df.iloc[i+1:]
        if direction == "BULLISH":
            deepest = float(after.low.min()) if not after.empty else upper
            penetration = max(0.0, min(size, upper - deepest))
            invalid = bool(not after.empty and (after.close < lower).any())
            touched_mask = after.low < upper if not after.empty else pd.Series(dtype=bool)
        else:
            deepest = float(after.high.max()) if not after.empty else lower
            penetration = max(0.0, min(size, deepest - lower))
            invalid = bool(not after.empty and (after.close > upper).any())
            touched_mask = after.high > lower if not after.empty else pd.Series(dtype=bool)
        fill_pct = max(0.0, min(100.0, penetration / size * 100.0))
        status = "INVALIDATED" if invalid else "FULLY_FILLED" if fill_pct >= 99.9 else "PARTIALLY_FILLED" if fill_pct > 0 else "UNFILLED"
        last_touch_ts = None
        if not after.empty and bool(touched_mask.any()):
            last_touch_ts = int(after.loc[touched_mask].timestamp.iloc[-1])
        quality = min(100.0, max(0.0, (float(size_atr or 0) * 35.0) + (mid_body / a * 20.0 if np.isfinite(a) and a > 0 else 0.0)))
        found.append({
            "direction": direction, "lower": lower, "upper": upper,
            "midpoint": (lower + upper) / 2.0, "size": size,
            "size_atr": round(float(size_atr), 3) if size_atr is not None else None,
            "timestamp": int(third.timestamp), "fill_pct": round(fill_pct, 1),
            "status": status, "mid_body": mid_body,
            "last_touch_timestamp": last_touch_ts,
            "quality_score": round(quality, 1),
            "bars_since_formation": int(len(df) - 1 - i),
        })
    valid = [z for z in found if z["status"] != "INVALIDATED"]
    return valid[-1] if valid else (found[-1] if found else None)


def detect_structure(df: pd.DataFrame) -> Dict[str, Any]:
    highs, lows = confirmed_swings(df, left_right=2)
    result = {
        "direction": "UNKNOWN", "pattern": "UNKNOWN", "bos": None,
        "choch": None, "mss": None, "swing_high": None, "swing_low": None,
        "confirmed": False,
    }
    if len(highs) < 2 or len(lows) < 2:
        return result
    h1, h2 = highs[-2][1], highs[-1][1]
    l1, l2 = lows[-2][1], lows[-1][1]
    bull = h2 > h1 and l2 > l1
    bear = h2 < h1 and l2 < l1
    pattern = "HH_HL" if bull else "LH_LL" if bear else "MIXED_SWINGS"
    close = float(df.close.iloc[-1])
    prior_high = float(highs[-1][1])
    prior_low = float(lows[-1][1])
    bos = "BULLISH_BOS" if close > prior_high else "BEARISH_BOS" if close < prior_low else None
    direction = "BULLISH" if bull else "BEARISH" if bear else "TRANSITION"
    # CHoCH is the close through the protected opposite swing against the prior swing sequence.
    choch = "BULLISH_CHOCH" if bear and close > prior_high else "BEARISH_CHOCH" if bull and close < prior_low else None
    result.update({"direction": direction, "pattern": pattern, "bos": bos, "choch": choch,
                   "mss": choch, "swing_high": prior_high, "swing_low": prior_low,
                   "confirmed": True})
    return result


def _decision_priority(result: Dict[str, Any]) -> Tuple[int, float, float]:
    state = str(result.get("decision_state", "UNKNOWN"))
    priority = {
        DecisionState.ENTRY_READY.value: 7,
        DecisionState.WAITING_FOR_CONFIRMATION.value: 6,
        DecisionState.WAITING_FOR_RETEST.value: 5,
        DecisionState.NO_CHASE.value: 4,
        DecisionState.SHADOW_ONLY.value: 3,
        DecisionState.REJECTED.value: 2,
        DecisionState.INVALIDATED.value: 1,
        DecisionState.EXPIRED.value: 0,
    }.get(state, 0)
    try:
        score = float(result.get("score", 0) or 0)
    except (TypeError, ValueError):
        score = 0.0
    try:
        zone_score = float((result.get("golden_zone") or {}).get("score", 0) or 0)
    except (TypeError, ValueError):
        zone_score = 0.0
    return priority, score, zone_score

def _normalize_text_items(value: Any) -> List[str]:
    """Flatten tuple/list evidence and coerce every leaf to text at the API boundary."""
    normalized: List[str] = []
    pending = list(value) if isinstance(value, (list, tuple, set)) else ([] if value is None else [value])
    while pending:
        item = pending.pop(0)
        if isinstance(item, (list, tuple, set)):
            pending[0:0] = list(item)
        elif item is not None:
            text = str(item).strip()
            if text:
                normalized.append(text)
    return normalized


def _decision_result(decision) -> Dict[str, Any]:
    data = decision.to_dict()
    # Evidence originates as immutable tuples in the strategy engines. Flatten nested
    # containers so downstream dashboard/log/entry consumers only receive strings.
    data["evidence"] = _normalize_text_items(data.get("evidence"))
    data["rejection_reasons"] = _normalize_text_items(data.get("rejection_reasons"))
    return data


def _finite_number(value: Any) -> Optional[float]:
    """Return a finite float only; booleans are never numeric stops or timestamps."""
    if isinstance(value, (bool, np.bool_)) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _current_structural_stop(row: Dict[str, Any], key: str,
                             candidate: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Validate a stop as belonging to the analysis row being written now.

    This never reads prior Watchlist state. The caller must set the key to None
    when validation fails so the broad Watchlist merge cannot resurrect an old
    stop from a previous decision.
    """
    stop = _finite_number(row.get("structural_stop"))
    if stop is None or stop <= 0:
        return None, "missing_or_non_positive"

    side = str(row.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        return None, "missing_or_invalid_side"
    if str(row.get("signal_direction") or side).strip().upper() != side:
        return None, "direction_conflict"
    if canonical_symbol(row.get("symbol")) != key:
        return None, "symbol_mismatch"
    expected_exchange = exchange_symbol(candidate.get("exchange_symbol") or key)
    if str(row.get("exchange_symbol") or "").strip().upper() != expected_exchange.strip().upper():
        return None, "exchange_symbol_mismatch"

    price = _finite_number(row.get("price"))
    if price is None or price <= 0:
        return None, "missing_or_invalid_price"
    if side == "BUY" and stop >= price:
        return None, "stop_wrong_side"
    if side == "SELL" and stop <= price:
        return None, "stop_wrong_side"

    if str(row.get("decision_state") or "").strip().upper() != DecisionState.ENTRY_READY.value:
        return None, "decision_not_entry_ready"
    if str(row.get("decision_reason") or "").strip() == "":
        return None, "missing_analysis_identity"
    if not str(row.get("event_id") or "").strip():
        return None, "missing_event_identity"
    if _finite_number(row.get("origin_timestamp")) is None:
        return None, "missing_origin_timestamp"
    updated = _finite_number(row.get("last_update", row.get("last_updated")))
    if updated is None or updated <= 0:
        return None, "missing_analysis_timestamp"
    if str(row.get("data_quality") or "").strip().upper() != "VALID_CLOSED_CANDLES":
        return None, "invalid_data_quality"
    freshness = str(row.get("data_freshness")
                    or (row.get("indicator_snapshot") or {}).get("quality")
                    or "").strip().upper()
    if freshness != "FRESH":
        return None, "stale_market_snapshot"
    return stop, "verified_current_analysis"


def _with_projected_decision(result: Dict[str, Any]) -> Dict[str, Any]:
    """Attach one read-only decision projection; legacy engine fields stay unchanged."""
    projection = project_decision_context(result)
    result["decision_context"] = projection
    result["projected_decision_state"] = projection["decision_state"]
    result["analysis_status"] = projection["analysis_status"]
    result["decision_stage"] = projection["setup_stage"]
    result["required_next_event"] = projection["required_next_event"]
    result["source_snapshot_id"] = projection["source_snapshot_id"]
    result["snapshot_freshness"] = projection["data_freshness"]
    result["zone_quality_score"] = projection["zone_quality_score"]
    result["context_quality_score"] = projection["context_quality_score"]
    result["entry_readiness_context"] = projection["entry_readiness"]
    return result


def merge_candidates(memory: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Merge all candidate feeds without losing valid CCXT symbols or direction."""
    merged: Dict[str, Dict[str, Any]] = {}
    sources = (
        ("professional_scan_candidates", memory.get("professional_scan_candidates", []), None),
        ("rf_watchlist", memory.get("rf_watchlist", []), None),
        ("scanner_v2_buy", memory.get("scanner_v2_buy", []), "BUY"),
        ("scanner_v2_sell", memory.get("scanner_v2_sell", []), "SELL"),
        ("radar_watchlist", memory.get("radar_watchlist", []), None),
        ("radar_top5", memory.get("radar_top5", []), None),
        ("top_candidates", memory.get("top_candidates", []), None),
        ("candidates", memory.get("candidates", []), None),
        ("confirmed_cross_events", memory.get("confirmed_cross_events", []), None),
        ("roro_watchlist", memory.get("roro_watchlist", {}), None),
    )
    for source, values, implied_side in sources:
        values = list(values.values()) if isinstance(values, dict) else list(values or [])
        for item in values:
            if not isinstance(item, dict) or not item.get("symbol"):
                continue
            original = str(item["symbol"]).strip()
            key = canonical_symbol(original)
            exchange = exchange_symbol(original)
            if not key or not exchange:
                continue
            entry = merged.setdefault(key, {
                "symbol": key, "exchange_symbol": exchange,
                "sources": [], "sides": set(), "score": 0.0,
            })
            if source not in entry["sources"]:
                entry["sources"].append(source)
            side = str(item.get("side") or item.get("rf_signal") or "").upper()
            if side not in {"BUY", "SELL"}:
                side = str(item.get("side_hint") or implied_side or "").upper()
            if side in {"BUY", "SELL"}:
                entry["sides"].add(side)
            # A durable EMA9/VWAP event carries its own immutable direction.
            # Keep that direction separate from soft scanner hints so the
            # institutional analyzer cannot select the opposite hypothesis
            # and accidentally inherit this event identity.
            if source == "confirmed_cross_events" and item.get("cross_event_id"):
                event_side = str(item.get("side") or "").upper()
                if event_side in {"BUY", "SELL"}:
                    existing_event_id = str(entry.get("cross_event_id") or "")
                    incoming_event_id = str(item.get("cross_event_id") or "")
                    existing_event_side = str(entry.get("cross_event_side") or "").upper()
                    if existing_event_id and existing_event_side in {"BUY", "SELL"} and existing_event_side != event_side:
                        entry["cross_event_conflict"] = True
                        ids = entry.setdefault("cross_event_ids", [existing_event_id])
                        if incoming_event_id and incoming_event_id not in ids:
                            ids.append(incoming_event_id)
                    else:
                        existing_ts = entry.get("cross_event_timestamp")
                        incoming_ts = item.get("cross_event_timestamp")
                        try:
                            prefer_incoming = (not existing_event_id or existing_event_id == incoming_event_id or
                                (incoming_ts is not None and (existing_ts is None or int(incoming_ts) >= int(existing_ts))))
                        except (TypeError, ValueError, OverflowError):
                            prefer_incoming = not existing_event_id or existing_event_id == incoming_event_id
                        if prefer_incoming:
                            entry["cross_event_id"] = incoming_event_id
                            entry["cross_event_side"] = event_side
                            for field in (
                                "cross_event_source", "cross_event_direction",
                                "cross_event_timestamp", "cross_event_status",
                                "cross_event_timeframe", "cross_event_lifecycle_status",
                                "cross_event_phase", "cross_event_lifecycle_schema",
                                "cross_event_entry_ready", "cross_event_rejection_high",
                                "cross_event_rejection_low", "cross_event_rejection_timestamp",
                                "cross_event_breakout_timestamp", "cross_event_breakout_close",
                                "cross_event_chase_atr",
                            ):
                                if item.get(field) not in (None, "", "UNKNOWN"):
                                    entry[field] = item[field]
            try:
                entry["score"] = max(float(entry["score"]), float(item.get("score", 0) or 0))
            except (TypeError, ValueError):
                pass
            # Prefer a slash-qualified exchange symbol over a stripped or ambiguous form.
            if "/" in original:
                entry["exchange_symbol"] = exchange
            for k, v in item.items():
                if k in {"symbol", "side", "rf_signal", "score"}:
                    continue
                if k not in entry or entry[k] in (None, "", "UNKNOWN"):
                    entry[k] = v
    for entry in merged.values():
        entry["sides"] = sorted(entry["sides"])
        entry["source"] = ",".join(entry["sources"])
    return merged


def _sniper_evidence_matches_side(side: str, liquidity_event: str,
                                    liquidity_direction: str, market_structure_shift: Any,
                                    causal_zone_valid: bool) -> bool:
    """Require the sweep and reversal MSS to agree with the proposed trade side."""
    direction = str(side or "").upper()
    expected = "BULLISH" if direction == "BUY" else "BEARISH" if direction == "SELL" else ""
    expected_mss = f"{expected}_CHOCH" if expected else ""
    return bool(
        expected
        and str(liquidity_event or "").upper() in {"SWEEP", "GRAB"}
        and str(liquidity_direction or "").upper() == expected
        and str(market_structure_shift or "").upper() == expected_mss
        and causal_zone_valid
    )


def _build_context(df: pd.DataFrame, side: str, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    atr = _atr(df)
    price = float(df.close.iloc[-1])
    regime = classify_regime(df, atr=atr if np.isfinite(atr) else None)
    structure = detect_structure(df)
    try:
        liquidity = classify_liquidity(df, atr, build_liquidity_pools) if np.isfinite(atr) else LiquidityEvent()
    except Exception as exc:
        liquidity = LiquidityEvent(reasons=[f"liquidity_analysis_error:{type(exc).__name__}"])
    try:
        ob = find_causal_order_block(df, side, price, atr) if np.isfinite(atr) else None
    except Exception:
        ob = None
    if isinstance(ob, dict):
        ob_validation = validate_order_block_zone(
            side=side, zone=ob, candles=df.to_dict("records"), atr=atr,
            min_strength=5.0, max_distance_atr=1.5,
        )
        ob["validation"] = ob_validation
        ob["status"] = ob_validation.get("status", "UNKNOWN")
        ob["fill_pct"] = ob_validation.get("fill_pct", 0.0)
        ob["valid"] = bool(ob_validation.get("valid"))
        if not ob["valid"]:
            ob = None
    try:
        support_resistance = build_support_resistance_zones(
            df.to_dict("records"), price=price, atr=atr, lookback=120,
            pivot_span=2, cluster_atr=0.22, min_touches=2, max_zones=6,
        ) if np.isfinite(atr) else {"status": "UNKNOWN", "supports": [], "resistances": []}
    except Exception as exc:
        support_resistance = {"status": "UNKNOWN", "supports": [], "resistances": [],
                              "reason": f"zone_analysis_error:{type(exc).__name__}"}
    fvg = detect_fvg(df, side=side, atr=atr if np.isfinite(atr) else None)
    volume_base = float(df.volume.iloc[-21:-1].mean()) if len(df) >= 21 else 0.0
    volume_ratio = float(df.volume.iloc[-1]) / volume_base if volume_base > 0 else None
    value_zone = "UNKNOWN"
    try:
        from roro_opportunity_pipeline import classify_golden_zone as _classify_value_zone
        value_zone = _classify_value_zone(df, atr).name
    except Exception:
        pass
    expected = "BULLISH" if side == "BUY" else "BEARISH"
    ob_valid = False
    if isinstance(ob, dict):
        try:
            ob_strength = float(ob.get("strength", 0) if ob.get("strength") is not None else 0)
            ob_distance = float(ob.get("distance_atr", 999) if ob.get("distance_atr") is not None else 999)
            ob_valid = (np.isfinite(ob_strength) and np.isfinite(ob_distance)
                        and ob_strength >= 5 and ob_distance <= 1.5)
        except (TypeError, ValueError):
            ob_valid = False
    fvg_valid = bool(isinstance(fvg, dict) and fvg.get("direction") == expected
                     and fvg.get("status") in ("UNFILLED", "PARTIALLY_FILLED"))
    ob_fvg_overlap = False
    if ob_valid and fvg_valid:
        ob_fvg_overlap = max(float(ob["low"]), float(fvg["lower"])) <= min(float(ob["high"]), float(fvg["upper"])) + 0.15 * atr
    sr = support_resistance if isinstance(support_resistance, dict) else {}
    sr_candidates = sr.get("supports" if side == "BUY" else "resistances", [])
    sr_aligned_zone = next((z for z in sr_candidates
                            if isinstance(z, dict) and z.get("status") == "CONFIRMED"
                            and float(z.get("distance_atr", 999) if z.get("distance_atr") is not None else 999) <= 1.5
                            and float(z.get("strength", 0) or 0) >= 5.0), None)
    sr_aligned = sr_aligned_zone is not None
    liq_event, liq_direction, liq_level = getattr(liquidity, "event", "UNKNOWN"), getattr(liquidity, "direction", "UNKNOWN"), getattr(liquidity, "level", None)
    liq_near = bool(liq_direction == expected and liq_event in ("SWEEP", "GRAB", "RUN") and liq_level is not None
                     and np.isfinite(atr) and atr > 0 and abs(price - float(liq_level)) <= 1.5 * atr)
    value_aligned = (side == "BUY" and value_zone == "DISCOUNT") or (side == "SELL" and value_zone == "PREMIUM")
    ref_count = int(ob_valid) + int(fvg_valid) + int(liq_near) + int(value_aligned) + int(sr_aligned)
    golden_score = min(100, int(ob_valid) * 25 + int(fvg_valid) * 20 + int(ob_fvg_overlap) * 15 + int(liq_near) * 15 + int(value_aligned) * 10 + int(sr_aligned) * 15)
    if ob_fvg_overlap or (ref_count >= 3 and (ob_valid or fvg_valid)):
        golden_state = "GOLDEN_CONFLUENCE"
    elif (ob_valid or fvg_valid) and value_aligned:
        golden_state = "VALUE_ZONE_CONFLUENCE"
    elif ob_valid or fvg_valid:
        golden_state = "SINGLE_INSTITUTIONAL_ZONE"
    elif value_aligned:
        golden_state = "DISCOUNT" if side == "BUY" else "PREMIUM"
    else:
        golden_state = "NO_GOLDEN_ZONE"
    golden = {
        "state": golden_state, "direction": expected, "value_zone": value_zone,
        "ob_valid": ob_valid, "fvg_valid": fvg_valid, "ob_fvg_overlap": ob_fvg_overlap,
        "support_resistance_aligned": sr_aligned, "aligned_sr_zone": sr_aligned_zone,
        "liquidity_aligned_near": liq_near, "aligned_reference_count": ref_count,
        "score": golden_score,
        "source_definition": "OB/FVG/value/liquidity confluence; not a Fibonacci 61.8% claim",
    }
    indicators = _indicator_snapshot(df, now_ms=now_ms)
    return {
        "atr": atr, "price": price, "regime": regime, "structure": structure,
        "liquidity": liquidity, "order_block": ob, "fvg": fvg,
        "support_resistance": support_resistance, "support_resistance_aligned": sr_aligned,
        "volume_ratio": volume_ratio,
        "volume_confirmed": bool(volume_ratio is not None and volume_ratio >= 1.1),
        "golden_zone": golden,
        # Strict entry policy: engines must retest a fresh, nearby institutional zone.
        # This metadata stays internal and does not alter Telegram/dashboard/API schemas.
        "location_first_required": True,
        "reference_price": price,
        "latest_timestamp": int(df.timestamp.iloc[-1]),
        "timeframe_ms": int(TIMEFRAME_SECONDS * 1000),
        "max_location_distance_atr": 1.5,
        "indicator_snapshot": indicators,
    }


def _fvg_matches_side(fvg: Any, side: str) -> bool:
    """Fail-closed FVG direction gate used by the institutional analyzer."""
    direction = str(side or "").upper()
    expected_direction = "BULLISH" if direction == "BUY" else "BEARISH" if direction == "SELL" else ""
    return bool(
        expected_direction
        and isinstance(fvg, dict)
        and str(fvg.get("direction") or "").upper() == expected_direction
        and str(fvg.get("status") or "").upper() in {"UNFILLED", "PARTIALLY_FILLED"}
    )


def analyze_symbol(symbol: str, side: str, df: pd.DataFrame,
                   now_ms: Optional[int] = None) -> Dict[str, Any]:
    x = _frame(df)
    closed = validate_closed_frame(x, now_ms=now_ms, timeframe_s=TIMEFRAME_SECONDS)
    if len(closed) < 55:
        return _with_projected_decision({
            "symbol": canonical_symbol(symbol), "exchange_symbol": exchange_symbol(symbol), "side": side,
            "decision_state": DecisionState.REJECTED.value,
            "decision_reason": "insufficient_closed_data", "rejection_reason": "insufficient_closed_data",
            "rejection_reasons": ["insufficient_closed_data"], "last_updated": time.time(),
            "data_quality": "INSUFFICIENT_CLOSED_CANDLES", "icons": {"status": "⚪"},
        })
    side = str(side or "").upper()
    if side not in {"BUY", "SELL"}:
        return _with_projected_decision({
            "symbol": canonical_symbol(symbol), "decision_state": "REJECTED", "rejection_reason": "invalid_side",
        })
    context = _build_context(closed, side, now_ms=now_ms)
    rev = REVERSAL.analyze(symbol, side, closed, context=context)
    cont = CONTINUATION.analyze(symbol, side, closed, context=context)
    decision = AUTHORITY.decide(rev, cont, risk_ok=True)
    reg, liq, ob, fvg = context["regime"], context["liquidity"], context["order_block"], context["fvg"]
    ob_state = "UNKNOWN"
    if isinstance(ob, dict):
        validation = ob.get("validation") if isinstance(ob.get("validation"), dict) else {}
        # The validator in _build_context is the single source of OB lifecycle state.
        # Do not recompute the same mitigation metric here with a second label set.
        if validation.get("valid") is True:
            ob_state = str(validation.get("status") or ob.get("status") or "UNKNOWN").upper()
            ob["status"] = ob_state
            ob["fill_pct"] = float(validation.get("fill_pct", ob.get("fill_pct", 0.0)) or 0.0)
        else:
            ob = None

    liq_name, liq_direction = getattr(liq, "event", "UNKNOWN"), getattr(liq, "direction", "UNKNOWN")
    if decision.setup_type == "FAILED_AUCTION_REVERSAL":
        liq_name, liq_direction = "FAILED_AUCTION", "BULLISH" if side == "BUY" else "BEARISH"
    ob_ok = False
    if isinstance(ob, dict):
        try:
            ob_strength = float(ob.get("strength", 0) if ob.get("strength") is not None else 0)
            ob_distance = float(ob.get("distance_atr", 999) if ob.get("distance_atr") is not None else 999)
            ob_ok = (np.isfinite(ob_strength) and np.isfinite(ob_distance)
                     and ob_strength >= 5 and ob_distance <= 1.5
                     and ob_state not in ("MITIGATED", "INVALIDATED"))
        except (TypeError, ValueError):
            ob_ok = False
    fvg_ok = _fvg_matches_side(fvg, side)
    d, golden = _decision_result(decision), context["golden_zone"]
    # Explainable mode/entry-behavior classification. This is analytical metadata only;
    # UnifiedEntryAuthority and execute_entry remain the sole entry authorization path.
    _liq_conf = getattr(liq, "confidence", 0.0)
    _disp_atr = None
    try:
        _last = closed.iloc[-1]
        _disp_atr = abs(float(_last.close) - float(_last.open)) / max(_atr(closed), 1e-12)
    except Exception:
        pass
    _adx_value = _adx(closed)
    indicators = context.get("indicator_snapshot") or {}
    momentum_strength = None
    if _disp_atr is not None:
        momentum_strength = max(0.0, min(100.0, 50.0 + _disp_atr * 20.0))
    # Prefer the causal order block as the primary zone; use a directionally
    # valid, unfilled FVG only when no qualified OB exists.
    _zone_low = _zone_high = None
    if ob_ok and isinstance(ob, dict):
        _zone_low = float(ob.get("low", 0) or 0)
        _zone_high = float(ob.get("high", 0) or 0)
    elif fvg_ok and isinstance(fvg, dict):
        _zone_low = float(fvg.get("lower", 0) or 0)
        _zone_high = float(fvg.get("upper", 0) or 0)
    if not (_zone_low is not None and _zone_high is not None
            and np.isfinite(_zone_low) and np.isfinite(_zone_high)
            and 0 < _zone_low < _zone_high):
        _zone_low = _zone_high = None
    _intelligence = build_intelligence_decision(
        regime=reg.state, adx=_adx_value,
        continuation_probability=(float(cont.score) / 100.0 if getattr(cont, "score", None) is not None else None),
        liquidity_quality=("GOOD" if float(_liq_conf or 0) >= 65.0 else "ADEQUATE" if float(_liq_conf or 0) >= 50.0 else "UNKNOWN"),
        displacement_atr=_disp_atr,
        setup_type=decision.setup_type,
        sweep_confirmed=_sniper_evidence_matches_side(
            side, liq_name, liq_direction, context["structure"].get("mss"), bool(ob_ok or fvg_ok)
        ),
        mss_confirmed=(
            str(context["structure"].get("mss") or "").upper()
            == ("BULLISH_CHOCH" if side == "BUY" else "BEARISH_CHOCH")
        ),
        causal_zone_valid=bool(ob_ok or fvg_ok),
        bos_level=(context["structure"].get("swing_high") if side == "BUY" else context["structure"].get("swing_low")),
        reference_low=_zone_low, reference_high=_zone_high,
        volatility_atr=(_atr(closed) / float(context["price"]) * 100.0 if float(context["price"] or 0) > 0 else None),
        zone_quality=golden.get("score"),
        order_block_fresh=bool(isinstance(ob, dict) and str(ob_state).upper() == "FRESH"),
        ema9_slope=indicators.get("ema9_slope_atr"),
        vwap_position=indicators.get("vwap_position", "UNKNOWN"),
        rsi=indicators.get("rsi"), momentum_strength=momentum_strength,
        volume_ratio=context.get("volume_ratio"), liquidity_confidence=_liq_conf,
        data_quality="VALID_CLOSED_CANDLES",
    ).to_dict()
    # Chart-pattern recognition is observational metadata based exclusively on
    # the validated closed-candle frame; it cannot authorize or submit orders.
    _intelligence["price_pattern"] = classify_price_pattern(
        closed.to_dict("records"), atr=_atr(closed)
    )
    row = _with_projected_decision({
        "symbol": canonical_symbol(symbol), "exchange_symbol": exchange_symbol(symbol),
        "side": side, "price": context["price"], "market_regime": reg.state,
        "regime_direction": reg.direction, "market_structure": context["structure"],
        "setup_type": "REVERSAL_ENGINE" if "REVERSAL" in decision.setup_type or decision.setup_type == "FAILED_AUCTION_REVERSAL" else "TREND_CONTINUATION_ENGINE" if "CONTINUATION" in decision.setup_type else "NONE",
        "decision_setup_type": decision.setup_type, "decision_state": decision.state,
        "decision_reason": decision.reason, "evidence": d.get("evidence", []),
        "rejection_reasons": d.get("rejection_reasons", []), "event_id": decision.event_id,
        "origin_timestamp": decision.origin_timestamp, "structural_stop": decision.structural_stop,
        "invalidation_level": decision.invalidation_level, "score": float(decision.score),
        "last_updated": time.time(), "last_update": time.time(), "data_quality": "VALID_CLOSED_CANDLES",
        "liquidity": {"event": liq_name, "direction": liq_direction, "level": getattr(liq, "level", None),
                      "confidence": getattr(liq, "confidence", 0), "reasons": list(getattr(liq, "reasons", []))},
        "order_block": ob, "order_block_state": ob_state, "fvg": fvg, "golden_zone": golden,
        "support_resistance": context.get("support_resistance", {"status": "UNKNOWN", "supports": [], "resistances": []}),
        "support_resistance_aligned": bool(context.get("support_resistance_aligned", False)),
        "volume_ratio": context["volume_ratio"], "volume_confirmed": context["volume_confirmed"],
        "ema9": indicators.get("ema9"), "vwap": indicators.get("vwap"),
        "rsi": indicators.get("rsi"), "atr": indicators.get("atr"),
        "ema9_slope": indicators.get("ema9_slope_atr"),
        "vwap_position": indicators.get("vwap_position", "UNKNOWN"),
        "trend_direction": reg.direction, "zone_type": "DEMAND" if side == "BUY" else "SUPPLY",
        "zone_quality": golden.get("score"), "order_block_status": ob_state,
        "indicator_snapshot": indicators,
        "data_freshness": indicators.get("quality", "DATA_UNAVAILABLE"),
        "signal_direction": side, "entry_readiness": decision.state,
        "market_intelligence": _intelligence,
        "trading_mode": _intelligence.get("mode", "UNCLASSIFIED"),
        "entry_behavior": _intelligence.get("entry_behavior", "WAIT_FOR_STRUCTURE"),
        "entry_type": normalize_entry_type(_intelligence.get("entry_behavior")),
        "entry_behavior_confidence": float(_intelligence.get("confidence", 0.0) or 0.0),
        "entry_behavior_reasons": list(_intelligence.get("reason_codes") or []),
        "entry_zone_low": _intelligence.get("reference_low"),
        "entry_zone_high": _intelligence.get("reference_high"),
        "adx": float(_adx_value) if np.isfinite(_adx_value) else None,
        "reversal": _decision_result(rev), "continuation": _decision_result(cont), "entry_status": decision.state,
        "icons": {
            "regime": "📈" if reg.direction == "BULLISH" else "📉" if reg.direction == "BEARISH" else "⚪",
            "structure": "🧭" if context["structure"].get("confirmed") else "⚪",
            "liquidity": "💧" if liq_name not in ("UNKNOWN", "NONE") else "⚪",
            "sweep": "🧲" if liq_name in ("SWEEP", "GRAB") else "⚪",
            "failed_auction": "🪤" if liq_name == "FAILED_AUCTION" else "⚪",
            "ob": "🧱" if ob_ok else "⚪", "fvg": "🟪" if fvg_ok else "⚪",
            "golden_zone": "🌟" if golden.get("state") == "GOLDEN_CONFLUENCE" else "🟨" if golden.get("score", 0) >= 35 else "⚪",
            "volume": "📊" if context["volume_confirmed"] else "⚪",
            "setup": "🎯" if decision.state == DecisionState.ENTRY_READY.value else "👁",
            "pullback": "🔄" if any(any(token in str(ev).lower() for token in ("retest", "pullback", "micro_pullback")) for ev in decision.evidence) else "⚪",
            "stop": "🛑" if decision.structural_stop else "⚪",
            "status": "✅" if decision.state == DecisionState.ENTRY_READY.value else "🟡" if decision.state.startswith("WAITING") or decision.state == "NO_CHASE" else "⚪",
        },
    })
    # One display projection over the canonical classifier output. It cannot
    # change decision_state, structural_stop, authorization, sizing or execution.
    behavior_context = project_entry_behavior(row, row.get("decision_context"))
    row["entry_type"] = behavior_context["entry_type"]
    row["entry_behavior_context"] = behavior_context
    return row


def analyze_symbol_both(symbol: str, frame: pd.DataFrame, now_ms: Optional[int] = None,
                        side_hints: Optional[List[str]] = None) -> Dict[str, Any]:
    # Always test both hypotheses. RF/Scanner direction is a ranking hint only.
    hints = {str(v).upper() for v in (side_hints or []) if str(v).upper() in {"BUY", "SELL"}}
    results = [analyze_symbol(symbol, side, frame, now_ms=now_ms) for side in ("BUY", "SELL")]
    ready = [row for row in results if row.get("decision_state") == DecisionState.ENTRY_READY.value]
    if len(ready) > 1:
        selected = dict(max(ready, key=lambda row: (float(row.get("score", 0) or 0), float((row.get("golden_zone") or {}).get("score", 0) or 0))))
        selected.update({"decision_state": DecisionState.REJECTED.value, "entry_status": DecisionState.REJECTED.value,
                         "decision_reason": "conflicting_opposing_setups_require_resolution",
                         "rejection_reason": "conflicting_opposing_setups_require_resolution",
                         "rejection_reasons": ["both_buy_and_sell_hypotheses_entry_ready"],
                         "conflicting_directions": [row.get("side") for row in ready],
                         "structural_stop": None, "event_id": None,
                         "icons": {**(selected.get("icons") or {}), "status": "⛔", "setup": "⚠️", "stop": "⚪"}})
        selected = _with_projected_decision(selected)
    else:
        selected = dict(max(results, key=lambda row: (_decision_priority(row), row.get("side") in hints)))
    selected["side_hints"] = sorted(hints)
    selected["side_hint_used_as_gate"] = False
    selected["side_analyses"] = {
        row.get("side"): {"state": row.get("decision_state"), "setup": row.get("decision_setup_type"),
                          "score": row.get("score"), "reason": row.get("decision_reason"),
                          "event_id": row.get("event_id"), "structural_stop": row.get("structural_stop"),
                          "projected_state": row.get("projected_decision_state"),
                          "decision_context": row.get("decision_context")}
        for row in results
    }
    return selected

def update_unified_watchlist(memory: Dict[str, Any], fetch_frame,
                             max_items: int = 150, batch_size: int = 24, enrich=None) -> Dict[str, Dict[str, Any]]:
    """Rotate all discovery sources in bounded batches and analyze both BUY/SELL."""
    out = memory.setdefault("watchlist", {})
    items = merge_candidates(memory)
    def candidate_rank(pair):
        key, item = pair
        prior = out.get(key, {}) if isinstance(out.get(key), dict) else {}
        def rank_number(value, default=0.0):
            try:
                number = float(value)
                return number if np.isfinite(number) else default
            except (TypeError, ValueError, OverflowError):
                return default
        state_bonus = {"ENTRY_READY": 3.0, "WAITING_FOR_RETEST": 2.4,
                       "WAITING_FOR_CONFIRMATION": 1.8, "NO_CHASE": 1.0,
                       "SHADOW_ONLY": 0.25}.get(str(prior.get("decision_state", "")), 0.0)
        golden = prior.get("golden_zone") if isinstance(prior.get("golden_zone"), dict) else {}
        golden_score = max(0.0, min(100.0, rank_number(golden.get("score")))) / 100.0
        ob = prior.get("order_block") if isinstance(prior.get("order_block"), dict) else {}
        ob_distance = rank_number(ob.get("distance_atr"), 999.0)
        ob_near = max(0.0, 1.0 - min(1.5, max(0.0, ob_distance)) / 1.5)
        liq = prior.get("liquidity") if isinstance(prior.get("liquidity"), dict) else {}
        liquidity_quality = max(0.0, min(1.0, rank_number(liq.get("confidence")) / 100.0))
        analysis_priority = max(0.0, min(100.0, rank_number(prior.get("analysis_priority_score")))) / 100.0
        proximity = prior.get("ema9_vwap_proximity") if isinstance(prior.get("ema9_vwap_proximity"), dict) else {}
        proximity = dict(proximity)
        proximity_observed_at = rank_number(proximity.get("updated_at"), 0.0)
        proximity_observation_age = max(0.0, time.time() - proximity_observed_at) if proximity_observed_at else float("inf")
        proximity["observation_age_seconds"] = proximity_observation_age
        cross_direction = str(proximity.get("cross_any") or "").upper()
        selected_side = str(prior.get("side") or "").upper()
        expected_cross = "BULLISH" if selected_side == "BUY" else "BEARISH" if selected_side == "SELL" else ""
        # A previously observed cross is rankable only while its candle age is
        # within two current timeframes. Unknown timestamps never earn a bonus.
        cross_age = rank_number(proximity.get("cross_age_seconds"), float("inf"))
        timeframe_seconds = rank_number(proximity.get("timeframe_seconds"), 900.0)
        cross_max_age = max(1.0, timeframe_seconds * 2.0)
        cross_fresh = (
            0.0 <= cross_age <= cross_max_age
            and bool(proximity.get("cross_fresh", True))
        )
        # Crosses affect ordering only; structural EntryAuthority remains final.
        vwap_cross_priority = (
            0.65 if cross_fresh and cross_direction and cross_direction == expected_cross
            else -0.35 if cross_fresh and cross_direction and expected_cross and cross_direction != expected_cross
            else 0.0
        )
        if bool(proximity.get("cross_conflict")) and cross_fresh:
            vwap_cross_priority = -0.50
        elif (0.0 <= rank_number(proximity.get("observation_age_seconds"), float("inf")) <= 1800.0
              and str(proximity.get("state") or "").upper() in {"NEAR_CROSS", "CONVERGING"}):
            vwap_cross_priority = max(vwap_cross_priority, 0.12)
        discovery_score = max(0.0, min(100.0, rank_number(item.get("score")))) / 100.0
        # A confirmed, still-active closed-candle cross event is an independent
        # watchlist opportunity. It is analyzed every cycle until it is entered,
        # invalidated or expires; it is not merely a ranking hint.
        confirmed_cross_event = bool(
            item.get("cross_event_id")
            and str(item.get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"
        )
        confirmed_cross_priority = 10.0 if confirmed_cross_event else 0.0
        updated = rank_number(prior.get("last_update", prior.get("last_updated", 0)))
        freshness = max(0.0, 1.0 - max(0.0, time.time() - updated) / 1800.0) if updated else 0.0
        # Re-rank using actual structural confluence/proximity from the previous analysis,
        # while rotating all symbols to prevent low-ranked candidates starving forever.
        priority = (state_bonus + 0.9 * golden_score + 0.55 * ob_near
                    + 0.35 * liquidity_quality + 0.45 * analysis_priority
                    + 0.30 * discovery_score + 0.20 * freshness
                    + vwap_cross_priority + confirmed_cross_priority)
        return (priority, state_bonus, golden_score, float(item.get("score", 0) or 0), key)
    ordered = sorted(items.items(), key=candidate_rank, reverse=True)
    if not ordered:
        memory["unified_watchlist_last_update"] = time.time()
        return out
    limit = min(max(1, int(max_items)), len(ordered))
    candidates = ordered[:limit]
    count = min(max(1, int(batch_size)), len(candidates))
    # Confirmed cross events are analyzed every cycle, independently of scanner
    # rotation. Remaining slots continue the existing bounded rotation so RF/V2
    # and other discovery sources stay supplementary but never starve.
    forced = [pair for pair in candidates
              if pair[1].get("cross_event_id")
              and str(pair[1].get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"][:count]
    forced_keys = {key for key, _ in forced}
    rotating = [pair for pair in candidates if pair[0] not in forced_keys]
    slots = count - len(forced)
    if rotating and slots > 0:
        cursor = int(memory.get("unified_watchlist_cursor", 0)) % len(rotating)
        selected = forced + [rotating[(cursor + i) % len(rotating)] for i in range(slots)]
        memory["unified_watchlist_cursor"] = (cursor + slots) % len(rotating)
    else:
        selected = forced
    for key, candidate in selected:
        try:
            frame = fetch_frame(candidate["exchange_symbol"])
        except Exception as exc:
            frame = None
            fetch_error = f"data_fetch_error:{type(exc).__name__}"
        else:
            fetch_error = "missing_or_invalid_market_data" if frame is None else None
        if frame is None:
            best = _with_projected_decision({
                "symbol": key, "exchange_symbol": candidate["exchange_symbol"], "side": "UNKNOWN",
                "decision_state": "UNKNOWN", "entry_status": "UNKNOWN",
                "decision_reason": fetch_error or "market_data_unavailable",
                "rejection_reasons": [fetch_error or "market_data_unavailable"],
                "data_quality": "DATA_UNAVAILABLE", "last_update": time.time(), "icons": {"status": "⚪"},
            })
        else:
            cross_side = str(candidate.get("cross_event_side") or "").upper()
            cross_event_active = bool(
                candidate.get("cross_event_id")
                and str(candidate.get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"
            )
            if cross_event_active and (
                candidate.get("cross_event_conflict") or cross_side not in {"BUY", "SELL"}
            ):
                best = _with_projected_decision({
                    "symbol": key, "exchange_symbol": candidate["exchange_symbol"],
                    "side": "UNKNOWN", "decision_state": DecisionState.REJECTED.value,
                    "entry_status": DecisionState.REJECTED.value,
                    "decision_reason": "conflicting_or_missing_durable_cross_direction",
                    "rejection_reasons": ["conflicting_or_missing_durable_cross_direction"],
                    "data_quality": "VALID_CLOSED_CANDLES", "last_update": time.time(),
                    "icons": {"status": "⛔"},
                })
                results = []
            else:
                analysis_sides = [cross_side] if cross_event_active else ["BUY", "SELL"]
                results = [analyze_symbol(candidate["exchange_symbol"], side, frame) for side in analysis_sides]
            for result in results:
                if enrich is not None:
                    try:
                        enriched = enrich(frame, result.get("side"), result)
                        if isinstance(enriched, dict): result.update(enriched)
                    except Exception as exc:
                        result["gpp_engine_diagnostics"] = {"status": "PARTIAL", "reason": f"{type(exc).__name__}:{str(exc)[:100]}"}
                result["source"] = candidate.get("source", "discovery")
                result["sources"] = list(candidate.get("sources", []))
                result["discovery_score"] = candidate.get("score", 0)
                result["side_hints"] = list(candidate.get("sides", []))
            if not results:
                pass
            else:
                ready = [row for row in results if row.get("decision_state") == DecisionState.ENTRY_READY.value]
                if len(ready) > 1:
                    best = dict(max(ready, key=lambda row: (float(row.get("score", 0) or 0), float((row.get("golden_zone") or {}).get("score", 0) or 0))))
                    best.update({"decision_state": DecisionState.REJECTED.value, "entry_status": DecisionState.REJECTED.value,
                                 "decision_reason": "conflicting_opposing_setups_require_resolution",
                                 "rejection_reasons": ["both_buy_and_sell_hypotheses_entry_ready"],
                                 "conflicting_directions": [row.get("side") for row in ready],
                                 "structural_stop": None, "event_id": None,
                                 "icons": {**(best.get("icons") or {}), "status": "⛔", "setup": "⚠️", "stop": "⚪"}})
                else:
                    hints = set(candidate.get("sides", []))
                    best = dict(max(results, key=lambda row: (_decision_priority(row), row.get("side") in hints)))
                    best["side_hint_used_as_gate"] = bool(cross_event_active)
                    best["side_selection_reason"] = (
                        "durable_cross_direction_is_authoritative" if cross_event_active
                        else "both_directions_analyzed; discovery_side_is_soft_hint"
                    )
            best["side_analyses"] = {
                row.get("side"): {"state": row.get("decision_state"), "setup": row.get("decision_setup_type"),
                                  "score": row.get("score"), "reason": row.get("decision_reason"),
                                  "event_id": row.get("event_id"), "structural_stop": row.get("structural_stop"),
                                  "trend_support": (row.get("institutional_trend") or {}).get("supporting_check"),
                                  "projected_state": row.get("projected_decision_state"),
                                  "decision_context": row.get("decision_context")}
                for row in results
            }
        previous = out.get(key, {})
        best["first_seen"] = previous.get("first_seen", time.time())
        best["discovery_sources"] = list(candidate.get("sources", []))
        best["discovery_score"] = float(candidate.get("score", 0) or 0)
        for cross_field in ("cross_event_id", "cross_event_source", "cross_event_direction",
                            "cross_event_timestamp", "cross_event_status", "cross_event_timeframe",
                            "cross_event_lifecycle_status", "cross_event_phase", "cross_event_lifecycle_schema",
                            "cross_event_entry_ready", "cross_event_rejection_high",
                            "cross_event_rejection_low", "cross_event_rejection_timestamp",
                            "cross_event_breakout_timestamp", "cross_event_breakout_close",
                            "cross_event_chase_atr"):
            if candidate.get(cross_field) not in (None, "", "UNKNOWN"):
                best[cross_field] = candidate[cross_field]
        if best.get("cross_event_id"):
            best["cross_event_analysis_required"] = True
        # A stop is decision-scoped. Clear it explicitly before projection and
        # before {**previous, **best}: an omitted/invalid stop in this analysis
        # must never inherit the old row's value.
        current_stop, stop_status = _current_structural_stop(best, key, candidate)
        best["structural_stop"] = current_stop
        best["structural_stop_status"] = stop_status
        best = _with_projected_decision(best)
        best["analysis_priority_score"] = round(_decision_priority(best)[0] * 10
            + float(best.get("score", 0) or 0) * 0.25
            + float((best.get("golden_zone") or {}).get("score", 0) or 0) * 0.15, 2)
        out[key] = {**previous, **best}
    now = time.time()
    protected = {"ENTRY_READY", "WAITING_FOR_RETEST", "WAITING_FOR_CONFIRMATION", "NO_CHASE"}
    # Event status in the Watchlist is a projection of the durable journal feed.
    # Once the journal marks an event ENTERED/INVALIDATED/EXPIRED and the feed is
    # re-synced, a stale row must not keep forcing analysis or TTL protection.
    active_cross_ids = {
        str(item.get("cross_event_id"))
        for item in items.values()
        if item.get("cross_event_id")
        and str(item.get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"
    }
    for key, value in list(out.items()):
        age = now - float(value.get("last_update", value.get("last_updated", now)) or now)
        if value.get("cross_event_id") and str(value.get("cross_event_id")) not in active_cross_ids:
            value["cross_event_status"] = "INACTIVE"
            value["cross_event_analysis_required"] = False
        active_cross_event = bool(
            value.get("cross_event_id")
            and str(value.get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"
            and str(value.get("cross_event_id")) in active_cross_ids
        )
        if age > 1800 and value.get("decision_state") not in protected and not active_cross_event:
            out.pop(key, None)
    memory["unified_watchlist_last_update"] = now
    memory["unified_watchlist_last_batch_count"] = len(selected)
    memory["unified_watchlist_source_count"] = len(items)
    return out
