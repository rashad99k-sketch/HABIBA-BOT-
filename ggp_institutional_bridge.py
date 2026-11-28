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
from roro_confluence import (classify_liquidity, find_causal_order_block, LiquidityEvent,
                             compute_session_vwap)
from ggp_market_intelligence import (build_intelligence_decision, classify_price_pattern,
                                     build_support_resistance_zones, validate_order_block_zone)


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


def cross_event_id(symbol: Any, direction: Any, candle_timestamp: Any) -> Optional[str]:
    """Return a stable identity for one EMA9/VWAP cross candle."""
    key = canonical_symbol(symbol)
    cross_direction = str(direction or "").upper()
    try:
        timestamp = int(candle_timestamp)
    except (TypeError, ValueError, OverflowError):
        timestamp = 0
    if not key or cross_direction not in {"BULLISH", "BEARISH"} or timestamp <= 0:
        return None
    return f"EMA9VWAP:{key}:{cross_direction}:{timestamp}"


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
    """Closed-candle EMA9/Session-VWAP event snapshot; never reads an open bar."""
    if closed is None or len(closed) < 55:
        return {"quality": "DATA_UNAVAILABLE", "source": "CLOSED_CANDLE_FRAME",
                "missing": ["insufficient_closed_candles"]}
    close = pd.to_numeric(closed["close"], errors="coerce")
    ema9 = close.ewm(span=9, adjust=False, min_periods=9).mean()
    ema50 = close.ewm(span=50, adjust=False, min_periods=50).mean()
    vwap = compute_session_vwap(closed, timeframe_seconds=TIMEFRAME_SECONDS, now_ms=now_ms)
    if vwap is None or len(vwap) != len(closed):
        return {"quality": "DATA_UNAVAILABLE", "source": "CANONICAL_SESSION_VWAP",
                "missing": ["invalid_or_truncated_session_vwap"]}
    delta = close.diff()
    gains = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    losses = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rsi = 100.0 - (100.0 / (1.0 + gains / losses.replace(0, np.nan)))
    atr_value = _atr(closed)
    price = float(close.iloc[-1])
    ema_value = float(ema9.iloc[-1]) if np.isfinite(ema9.iloc[-1]) else None
    ema50_value = float(ema50.iloc[-1]) if np.isfinite(ema50.iloc[-1]) else None
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
    gaps = (ema9 - vwap).astype(float)
    cross_direction = None
    cross_index = None
    # Keep a cross observable across the following closed candle. This avoids
    # missing a valid event merely because the 15-second watchlist cycle ran late.
    for idx in range(len(closed) - 1, max(0, len(closed) - 3), -1):
        before, after = gaps.iloc[idx - 1], gaps.iloc[idx]
        if not (np.isfinite(before) and np.isfinite(after)):
            continue
        candidate = "BULLISH" if before < 0 < after else "BEARISH" if before > 0 > after else None
        if candidate is None:
            continue
        latest_gap = gaps.iloc[-1]
        still_aligned = (latest_gap > 0) if candidate == "BULLISH" else (latest_gap < 0)
        if still_aligned:
            cross_direction, cross_index = candidate, idx
            break
    current_gap = float(gaps.iloc[-1]) if np.isfinite(gaps.iloc[-1]) else None
    previous_gap = float(gaps.iloc[-2]) if len(gaps) >= 2 and np.isfinite(gaps.iloc[-2]) else None
    approaching = bool(
        current_gap is not None and previous_gap is not None and np.isfinite(atr_value) and atr_value > 0
        and abs(current_gap) < 0.25 * atr_value and abs(current_gap) < abs(previous_gap)
    )
    dmi = _dmi_components(closed)
    cross_ts = int(float(closed["timestamp"].iloc[cross_index])) if cross_index is not None else None
    cross_age_bars = (len(closed) - 1 - cross_index) if cross_index is not None else None
    cross_age_seconds = (cross_age_bars or 0) * TIMEFRAME_SECONDS if cross_index is not None else None
    return {
        "ema9": ema_value, "ema50": ema50_value, "vwap": vwap_value, "rsi": rsi_value,
        "atr": float(atr_value) if np.isfinite(atr_value) else None,
        "ema9_slope_atr": slope,
        "vwap_position": ("ABOVE" if price > vwap_value else "BELOW" if price < vwap_value else "AT_VWAP") if vwap_value is not None else "UNKNOWN",
        "price_vwap_distance_atr": (abs(price - vwap_value) / atr_value) if vwap_value is not None and np.isfinite(atr_value) and atr_value > 0 else None,
        "volume_ratio": volume_ratio, "latest_closed_timestamp": latest_ts,
        "data_age_seconds": round(age_seconds, 3), "quality": quality,
        "source": "CLOSED_CANDLE_FRAME", "vwap_source": "roro_confluence.compute_session_vwap",
        "timeframe_seconds": TIMEFRAME_SECONDS,
        "cross_status": "CONFIRMED" if cross_direction else "APPROACHING" if approaching else "NONE",
        "cross_direction": cross_direction,
        "cross_candle_timestamp": cross_ts,
        "cross_age_bars": cross_age_bars,
        "cross_age_seconds": cross_age_seconds,
        "cross_distance": abs(current_gap) if current_gap is not None else None,
        "adx": dmi.get("adx"), "di_plus": dmi.get("di_plus"), "di_minus": dmi.get("di_minus"),
        "adx_slope": dmi.get("adx_slope"),
    }


def _atr(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period + 1:
        return float("nan")
    prev = df.close.shift(1)
    tr = pd.concat([df.high - df.low, (df.high - prev).abs(), (df.low - prev).abs()], axis=1).max(axis=1)
    value = tr.rolling(period, min_periods=period).mean().iloc[-1]
    return float(value) if pd.notna(value) and np.isfinite(value) and value > 0 else float("nan")


def _dmi_components(df: pd.DataFrame, period: int = 14) -> Dict[str, Optional[float]]:
    """Return GGP-canonical Wilder/RMA ADX and DI values from closed candles.

    This deliberately mirrors ``ggpbot.compute_adx`` and ``get_di_components``
    so the discovery ADX>=18 gate and the final execution ADX>=18 gate cannot
    disagree because they seeded the same indicator differently.
    """
    empty = {"adx": None, "di_plus": None, "di_minus": None, "adx_slope": None}
    if (not isinstance(df, pd.DataFrame) or not isinstance(period, int) or period < 2
            or len(df) < period * 2 or not {"high", "low", "close"}.issubset(df.columns)):
        return empty
    try:
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        if not (np.isfinite(high.to_numpy()).all() and np.isfinite(low.to_numpy()).all()
                and np.isfinite(close.to_numpy()).all()):
            return empty
        if bool((high < low).any() or (close > high).any() or (close < low).any()):
            return empty
        tr = pd.concat([
            (high - low).rename("hl"),
            (high - close.shift(1)).abs().rename("hc"),
            (low - close.shift(1)).abs().rename("lc"),
        ], axis=1).max(axis=1)
        rma = lambda values: values.ewm(alpha=1.0 / period, adjust=False).mean()
        atr = rma(tr) + 1e-9
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)
        plus_di = 100.0 * rma(plus_dm) / (atr + 1e-9)
        minus_di = 100.0 * rma(minus_dm) / (atr + 1e-9)
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100.0
        adx_series = rma(dx).bfill().ffill().fillna(0.0).clip(0.0, 100.0)
        adx_current = float(adx_series.iloc[-1])
        adx_previous = float(adx_series.iloc[-2]) if len(adx_series) > 1 else adx_current
        return {
            "adx": adx_current,
            "di_plus": float(plus_di.iloc[-1]),
            "di_minus": float(minus_di.iloc[-1]),
            "adx_slope": adx_current - adx_previous,
        }
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return empty


def _adx(df: pd.DataFrame, period: int = 14) -> float:
    """Compatibility wrapper retained for existing callers and tests."""
    value = _dmi_components(df, period).get("adx")
    return float(value) if value is not None else float("nan")


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


def merge_candidates(memory: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Merge all candidate feeds without losing valid CCXT symbols or direction."""
    merged: Dict[str, Dict[str, Any]] = {}
    universe = memory.get("market_universe", {}) if isinstance(memory.get("market_universe"), dict) else {}
    universe_symbols = universe.get("symbols", []) if isinstance(universe, dict) else []
    universe_items = [{"symbol": symbol, "source": "validated_market_universe"} for symbol in universe_symbols]
    sources = (
        ("validated_market_universe", universe_items, None),
        ("professional_scan_candidates", memory.get("professional_scan_candidates", []), None),
        ("rf_watchlist", memory.get("rf_watchlist", []), None),
        ("scanner_v2_buy", memory.get("scanner_v2_buy", []), "BUY"),
        ("scanner_v2_sell", memory.get("scanner_v2_sell", []), "SELL"),
        ("radar_watchlist", memory.get("radar_watchlist", []), None),
        ("radar_top5", memory.get("radar_top5", []), None),
        ("top_candidates", memory.get("top_candidates", []), None),
        ("candidates", memory.get("candidates", []), None),
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


def _zone_distance_atr(zone: Dict[str, Any], price: float, atr: float) -> Optional[float]:
    try:
        low = float(zone.get("low", zone.get("lower")))
        high = float(zone.get("high", zone.get("upper")))
        if not (np.isfinite(low) and np.isfinite(high) and 0 < low < high and atr > 0):
            return None
        return max(low - price, 0.0, price - high) / atr
    except (TypeError, ValueError, OverflowError):
        return None


def _evaluate_cross_location(df: pd.DataFrame, side: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Score location quality only; sweep/MSS/retest are evidence, never universal gates."""
    price = float(context.get("price") or 0.0)
    atr = float(context.get("atr") or 0.0)
    if price <= 0 or atr <= 0 or not np.isfinite(price) or not np.isfinite(atr):
        return {"status": "WAITING", "distance_atr": None, "reason": "location_inputs_unavailable",
                "evidence": ["location_inputs_unavailable"], "structural_stop": None,
                "zone_low": None, "zone_high": None}

    expected = "BULLISH" if side == "BUY" else "BEARISH"
    evidence: List[str] = []
    candidate_distances: List[float] = []
    stop_extremes: List[float] = []
    chosen_zone: Optional[Dict[str, Any]] = None
    factor_count = 0
    has_primary = False
    ob = context.get("order_block") if isinstance(context.get("order_block"), dict) else None
    fvg = context.get("fvg") if isinstance(context.get("fvg"), dict) else None
    sr = context.get("support_resistance") if isinstance(context.get("support_resistance"), dict) else {}

    if ob and ob.get("valid") is True and str(ob.get("status") or "").upper() not in {"MITIGATED", "INVALIDATED", "CANCELLED"}:
        dist = _zone_distance_atr(ob, price, atr)
        if dist is None:
            try: dist = float(ob.get("distance_atr"))
            except (TypeError, ValueError): dist = None
        if dist is not None and np.isfinite(dist) and 0 <= dist <= 1.5:
            factor_count += 1; has_primary = True; candidate_distances.append(float(dist))
            evidence.append(f"OB_{str(ob.get('status') or 'FRESH').upper()}_ALIGNED_DISTANCE_{dist:.2f}ATR")
            try:
                stop_extremes.append(float(ob.get("low") if side == "BUY" else ob.get("high")))
            except (TypeError, ValueError):
                pass
            chosen_zone = ob

    if fvg and str(fvg.get("direction") or "").upper() == expected and str(fvg.get("status") or "").upper() in {"UNFILLED", "PARTIALLY_FILLED"}:
        dist = _zone_distance_atr(fvg, price, atr)
        if dist is not None and 0 <= dist <= 1.5:
            factor_count += 1; has_primary = True; candidate_distances.append(float(dist))
            evidence.append(f"FVG_{str(fvg.get('status')).upper()}_ALIGNED_DISTANCE_{dist:.2f}ATR")
            try:
                stop_extremes.append(float(fvg.get("lower") if side == "BUY" else fvg.get("upper")))
            except (TypeError, ValueError):
                pass
            chosen_zone = chosen_zone or fvg

    sr_key = "supports" if side == "BUY" else "resistances"
    aligned_sr = []
    for zone in sr.get(sr_key, []) if isinstance(sr.get(sr_key), list) else []:
        if not isinstance(zone, dict) or str(zone.get("status") or "").upper() != "CONFIRMED":
            continue
        dist = _zone_distance_atr(zone, price, atr)
        if dist is None:
            try: dist = float(zone.get("distance_atr"))
            except (TypeError, ValueError): dist = None
        if dist is not None and np.isfinite(dist) and 0 <= dist <= 1.5:
            aligned_sr.append((float(dist), zone))
    if aligned_sr:
        dist, zone = min(aligned_sr, key=lambda x: x[0])
        factor_count += 1; candidate_distances.append(dist)
        evidence.append(f"CONFIRMED_{'SUPPORT' if side == 'BUY' else 'RESISTANCE'}_DISTANCE_{dist:.2f}ATR")
        try: stop_extremes.append(float(zone.get("low") if side == "BUY" else zone.get("high")))
        except (TypeError, ValueError): pass
        chosen_zone = chosen_zone or zone

    golden = context.get("golden_zone") if isinstance(context.get("golden_zone"), dict) else {}
    indicators = context.get("indicator_snapshot") if isinstance(context.get("indicator_snapshot"), dict) else {}
    value_zone = str(golden.get("value_zone") or "UNKNOWN").upper()
    vwap_distance = indicators.get("price_vwap_distance_atr")
    value_aligned = (side == "BUY" and value_zone == "DISCOUNT") or (side == "SELL" and value_zone == "PREMIUM")
    if value_aligned and vwap_distance is not None and np.isfinite(float(vwap_distance)) and float(vwap_distance) <= 1.5:
        factor_count += 1; candidate_distances.append(float(vwap_distance))
        evidence.append(f"{value_zone}_VALUE_LOCATION_WITHIN_{float(vwap_distance):.2f}ATR_OF_SESSION_VWAP")

    liquidity = context.get("liquidity")
    liquidity_direction = str(getattr(liquidity, "direction", "UNKNOWN") or "UNKNOWN").upper()
    liquidity_event = str(getattr(liquidity, "event", "UNKNOWN") or "UNKNOWN").upper()
    liquidity_level = getattr(liquidity, "level", None)
    liquidity_aligned = False
    try:
        liquidity_aligned = (liquidity_direction == expected and liquidity_event in {"SWEEP", "GRAB", "RUN"}
                             and liquidity_level is not None and abs(price - float(liquidity_level)) / atr <= 1.5)
    except (TypeError, ValueError, ZeroDivisionError):
        liquidity_aligned = False
    if liquidity_aligned:
        factor_count += 1; candidate_distances.append(abs(price - float(liquidity_level)) / atr)
        evidence.append(f"OPTIONAL_LIQUIDITY_EVIDENCE_{liquidity_event}_ALIGNED")

    min_distance = min(candidate_distances) if candidate_distances else None
    if chosen_zone and factor_count >= 3:
        status = "EXCELLENT"
    elif has_primary and factor_count >= 2:
        status = "GOOD"
    elif has_primary and min_distance is not None and min_distance <= 0.75:
        status = "GOOD"
    elif factor_count >= 1:
        status = "MODERATE"
    else:
        status = "WAITING"
        evidence.append("NO_ACTIONABLE_DIRECTIONAL_LOCATION_WITHIN_1_5_ATR")

    # Stop placement is anchored beyond the nearest valid directional zone or
    # last confirmed swing. An ATR fallback is allowed only after location passed.
    candidates = [v for v in stop_extremes if np.isfinite(v) and ((v < price) if side == "BUY" else (v > price))]
    try:
        highs, lows = confirmed_swings(df, left_right=2)
        swing_values = [float(v) for _, v in (lows[-8:] if side == "BUY" else highs[-8:])]
        candidates.extend(v for v in swing_values if np.isfinite(v) and ((v < price) if side == "BUY" else (v > price)))
    except Exception:
        pass
    if candidates:
        extreme = max(candidates) if side == "BUY" else min(candidates)
    else:
        extreme = price - (1.2 * atr) if side == "BUY" else price + (1.2 * atr)
    structural_stop, stop_reason = StructuralStopLossEngine.calculate(
        side, "EMA9_VWAP_CROSS_LOCATION", price, float(extreme), atr
    )
    if structural_stop is not None:
        evidence.append(stop_reason)
    else:
        evidence.append(f"STOP_INVALID:{stop_reason}")

    zone_low = zone_high = None
    if chosen_zone:
        try:
            zone_low = float(chosen_zone.get("low", chosen_zone.get("lower")))
            zone_high = float(chosen_zone.get("high", chosen_zone.get("upper")))
        except (TypeError, ValueError):
            zone_low = zone_high = None
    return {
        "status": status, "distance_atr": round(float(min_distance), 4) if min_distance is not None else None,
        "factor_count": factor_count, "reason": ";".join(evidence), "evidence": evidence,
        "structural_stop": structural_stop, "stop_reason": stop_reason,
        "zone_low": zone_low, "zone_high": zone_high,
        "liquidity_is_optional_evidence": True,
        "universal_sweep_mss_retest_gate": False,
    }


def _classify_cross_trade_mode(side: str, regime: Any, indicators: Dict[str, Any], volume_ratio: Any) -> Tuple[str, str, List[str]]:
    """TREND requires directional regime/ADX/DI confirmation; otherwise use SCALP."""
    adx = indicators.get("adx")
    plus = indicators.get("di_plus")
    minus = indicators.get("di_minus")
    slope = indicators.get("ema9_slope_atr")
    reg_direction = str(getattr(regime, "direction", "UNKNOWN") or "UNKNOWN").upper()
    expected = "BULLISH" if side == "BUY" else "BEARISH"
    di_aligned = (plus is not None and minus is not None and
                  ((plus > minus) if side == "BUY" else (minus > plus)))
    slope_aligned = (slope is not None and ((slope > 0.05) if side == "BUY" else (slope < -0.05)))
    volume_ok = volume_ratio is not None and float(volume_ratio) >= 0.90
    strong = (adx is not None and float(adx) >= 25.0 and reg_direction == expected
              and di_aligned and slope_aligned and volume_ok)
    evidence = [f"ADX={float(adx):.2f}" if adx is not None else "ADX_UNKNOWN",
                f"REGIME={reg_direction}", f"DI_ALIGNED={di_aligned}",
                f"EMA9_SLOPE_ALIGNED={slope_aligned}", f"VOLUME_RATIO={volume_ratio}"]
    if strong:
        return "TREND", "strong_directional_regime_confirmed", evidence
    return "SCALP", "trend_strength_not_fully_confirmed_use_scalp_management", evidence


def analyze_symbol(symbol: str, side: str, df: pd.DataFrame,
                   now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Analyze an EMA9/Session-VWAP event; institutional logic grades location only."""
    x = _frame(df)
    effective_now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    closed = validate_closed_frame(x, now_ms=effective_now_ms, timeframe_s=TIMEFRAME_SECONDS)
    side = str(side or "").upper()
    base = {"symbol": canonical_symbol(symbol), "exchange_symbol": exchange_symbol(symbol), "side": side,
            "last_updated": time.time(), "last_update": time.time(), "source": "EMA9_VWAP_CROSS_LIFECYCLE"}
    if side not in {"BUY", "SELL"}:
        return {**base, "decision_state": "REJECTED", "decision_reason": "invalid_side",
                "rejection_reason": "invalid_side", "data_quality": "INVALID"}
    if len(closed) < 55:
        return {**base, "decision_state": DecisionState.REJECTED.value,
                "decision_reason": "insufficient_closed_data", "rejection_reason": "insufficient_closed_data",
                "rejection_reasons": ["insufficient_closed_data"], "data_quality": "INSUFFICIENT_CLOSED_CANDLES",
                "icons": {"status": "⚪"}}

    context = _build_context(closed, side, now_ms=effective_now_ms)
    reg, liq = context["regime"], context["liquidity"]
    indicators = context.get("indicator_snapshot") or {}
    dmi = _dmi_components(closed)
    adx_value = dmi.get("adx")
    # The indicator snapshot and the authorization snapshot use the same validated frame.
    indicators.update({"adx": adx_value, "di_plus": dmi.get("di_plus"), "di_minus": dmi.get("di_minus"),
                       "adx_slope": dmi.get("adx_slope")})
    cross_status = str(indicators.get("cross_status") or "NONE").upper()
    cross_direction = str(indicators.get("cross_direction") or "").upper()
    expected_direction = "BULLISH" if side == "BUY" else "BEARISH"
    cross_timestamp = indicators.get("cross_candle_timestamp")
    cross_id = cross_event_id(symbol, cross_direction, cross_timestamp) if cross_status == "CONFIRMED" else None
    side_matches = cross_status == "CONFIRMED" and cross_direction == expected_direction and bool(cross_id)

    location = _evaluate_cross_location(closed, side, context)
    trade_mode, trade_mode_reason, trade_mode_evidence = _classify_cross_trade_mode(
        side, reg, indicators, context.get("volume_ratio")
    )
    adx_pass = adx_value is not None and float(adx_value) >= 18.0
    data_fresh = str(indicators.get("quality") or "UNKNOWN").upper() == "FRESH"

    if cross_status != "CONFIRMED":
        state, reason = "WAITING_FOR_CONFIRMATION", "ema9_session_vwap_cross_not_confirmed_on_closed_candle"
    elif not side_matches:
        state, reason = "WAITING_FOR_CONFIRMATION", "confirmed_cross_direction_does_not_match_side"
    elif not data_fresh:
        state, reason = "WAITING_FOR_CONFIRMATION", "cross_or_market_data_is_stale"
    elif not adx_pass:
        state, reason = "REJECTED", f"BASE_ADX_BELOW_18:{adx_value if adx_value is not None else 'UNKNOWN'}"
    elif location.get("status") not in {"EXCELLENT", "GOOD", "MODERATE"}:
        state, reason = "WAITING_FOR_CONFIRMATION", "institutional_location_not_actionable_yet"
    elif location.get("structural_stop") is None:
        state, reason = "REJECTED", "structural_stop_unavailable_or_invalid"
    else:
        state, reason = "ENTRY_READY", "confirmed_cross_adx_pass_and_location_approved"

    evidence = [
        f"CLOSED_CANDLE_EMA9_SESSION_VWAP_CROSS={cross_status}",
        f"CROSS_DIRECTION={cross_direction or 'NONE'}",
        f"BASE_ADX={adx_value if adx_value is not None else 'UNKNOWN'};REQUIRED>=18",
        f"LOCATION_DECISION={location.get('status')}",
        *list(location.get("evidence") or []),
        f"TRADE_MODE={trade_mode}:{trade_mode_reason}",
        "OB_FVG_SR_VALUE_ZONE_ARE_LOCATION_EVIDENCE_ONLY",
        "SWEEP_MSS_RETEST_ARE_NOT_UNIVERSAL_ENTRY_GATES",
    ]
    golden = context.get("golden_zone") if isinstance(context.get("golden_zone"), dict) else {}
    ob = context.get("order_block") if isinstance(context.get("order_block"), dict) else None
    fvg = context.get("fvg") if isinstance(context.get("fvg"), dict) else None
    ob_state = str((ob or {}).get("status") or "UNKNOWN").upper()
    expected_regime = expected_direction
    _intelligence = build_intelligence_decision(
        regime=reg.state, adx=float(adx_value) if adx_value is not None else None,
        continuation_probability=None,
        liquidity_quality=("GOOD" if float(getattr(liq, "confidence", 0) or 0) >= 65 else "ADEQUATE" if float(getattr(liq, "confidence", 0) or 0) >= 50 else "UNKNOWN"),
        displacement_atr=None, setup_type="EMA9_VWAP_CROSS_LOCATION",
        sweep_confirmed=False,
        mss_confirmed=False,
        causal_zone_valid=location.get("status") in {"EXCELLENT", "GOOD", "MODERATE"},
        reference_low=location.get("zone_low"), reference_high=location.get("zone_high"),
        volatility_atr=(float(context["atr"]) / float(context["price"]) * 100.0 if float(context["price"] or 0) > 0 else None),
        zone_quality=golden.get("score"), order_block_fresh=ob_state == "FRESH",
        ema9_slope=indicators.get("ema9_slope_atr"), vwap_position=indicators.get("vwap_position", "UNKNOWN"),
        rsi=indicators.get("rsi"), momentum_strength=None,
        volume_ratio=context.get("volume_ratio"), liquidity_confidence=getattr(liq, "confidence", 0),
        data_quality="VALID_CLOSED_CANDLES",
    ).to_dict()
    _intelligence["price_pattern"] = classify_price_pattern(closed.to_dict("records"), atr=_atr(closed))
    return {
        **base,
        "price": context["price"], "market_regime": reg.state, "regime_direction": reg.direction,
        "market_structure": context["structure"],
        "setup_type": "EMA9_VWAP_CROSS_LOCATION", "decision_setup_type": "EMA9_VWAP_CROSS_LOCATION",
        "decision_state": state, "entry_status": state, "entry_readiness": state,
        "decision_reason": reason, "rejection_reason": reason if state == "REJECTED" else "",
        "rejection_reasons": [reason] if state == "REJECTED" else [],
        "evidence": evidence, "event_id": cross_id if side_matches else None,
        "cross_event_id": cross_id, "cross_status": cross_status,
        "cross_direction": cross_direction or None, "cross_candle_timestamp": cross_timestamp,
        "cross_age_bars": indicators.get("cross_age_bars"), "cross_age_seconds": indicators.get("cross_age_seconds"),
        "cross_distance": indicators.get("cross_distance"),
        "institutional_analysis_status": "INSTITUTIONAL_APPROVED" if location.get("status") in {"EXCELLENT", "GOOD", "MODERATE"} else "INSTITUTIONAL_ANALYSIS_PENDING",
        "final_rejection_reason": reason if state == "REJECTED" else "",
        "origin_timestamp": cross_timestamp, "structural_stop": location.get("structural_stop"),
        "score": float(golden.get("score", 0) or 0),
        "last_updated": time.time(), "last_update": time.time(), "data_quality": "VALID_CLOSED_CANDLES",
        "liquidity": {"event": getattr(liq, "event", "UNKNOWN"), "direction": getattr(liq, "direction", "UNKNOWN"),
                      "level": getattr(liq, "level", None), "confidence": getattr(liq, "confidence", 0),
                      "reasons": list(getattr(liq, "reasons", []))},
        "order_block": ob, "order_block_state": ob_state, "fvg": fvg, "golden_zone": golden,
        "support_resistance": context.get("support_resistance", {"status": "UNKNOWN", "supports": [], "resistances": []}),
        "support_resistance_aligned": bool(context.get("support_resistance_aligned", False)),
        "volume_ratio": context.get("volume_ratio"), "volume_confirmed": bool(context.get("volume_confirmed")),
        "ema9": indicators.get("ema9"), "ema50": indicators.get("ema50"), "vwap": indicators.get("vwap"),
        "session_vwap": indicators.get("vwap"), "rsi": indicators.get("rsi"), "atr": indicators.get("atr"),
        "ema9_slope": indicators.get("ema9_slope_atr"), "vwap_position": indicators.get("vwap_position", "UNKNOWN"),
        "trend_direction": reg.direction, "zone_type": "DEMAND" if side == "BUY" else "SUPPLY",
        "zone_quality": golden.get("score"), "indicator_snapshot": indicators,
        "data_freshness": indicators.get("quality", "DATA_UNAVAILABLE"),
        "signal_direction": side, "market_intelligence": _intelligence,
        "trading_mode": trade_mode, "trade_mode": trade_mode, "profit_mode": "RANGE_SCALP" if trade_mode == "SCALP" else "TREND_RIDE",
        "trade_mode_reason": trade_mode_reason, "trade_mode_evidence": trade_mode_evidence,
        "entry_behavior": "CROSS_IMMEDIATE_LOCATION_CHECK", "entry_behavior_confidence": 1.0 if side_matches else 0.0,
        "entry_behavior_reasons": [reason], "entry_zone_low": location.get("zone_low"),
        "entry_zone_high": location.get("zone_high"), "adx": adx_value,
        "di_plus": indicators.get("di_plus"), "di_minus": indicators.get("di_minus"),
        "location_status": location.get("status"), "location_decision": location.get("status"),
        "location_distance_atr": location.get("distance_atr"), "location_reason": location.get("reason"),
        "location_evidence": location.get("evidence", []), "location_factor_count": location.get("factor_count", 0),
        "location_is_authority_not_strategy_replacement": True,
        "reversal": {"state": "DIAGNOSTIC_ONLY_NOT_USED_FOR_CROSS_ENTRY"},
        "continuation": {"state": "DIAGNOSTIC_ONLY_NOT_USED_FOR_CROSS_ENTRY"},
        "icons": {
            "regime": "📈" if reg.direction == "BULLISH" else "📉" if reg.direction == "BEARISH" else "⚪",
            "structure": "🧭" if context["structure"].get("confirmed") else "⚪",
            "liquidity": "💧" if getattr(liq, "event", "UNKNOWN") not in ("UNKNOWN", "NONE") else "⚪",
            "sweep": "🧲" if getattr(liq, "event", "UNKNOWN") in ("SWEEP", "GRAB") else "⚪",
            "ob": "🧱" if ob else "⚪", "fvg": "🟪" if fvg else "⚪",
            "golden_zone": "🌟" if golden.get("state") == "GOLDEN_CONFLUENCE" else "🟨" if golden.get("score", 0) >= 35 else "⚪",
            "volume": "📊" if context.get("volume_confirmed") else "⚪",
            "setup": "🎯" if state == "ENTRY_READY" else "👁",
            "stop": "🛑" if location.get("structural_stop") else "⚪",
            "status": "✅" if state == "ENTRY_READY" else "🟡" if state.startswith("WAITING") else "⛔" if state == "REJECTED" else "⚪",
        },
    }


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
    else:
        selected = dict(max(results, key=lambda row: (
            bool(row.get("cross_status") == "CONFIRMED" and row.get("cross_direction") == ("BULLISH" if row.get("side") == "BUY" else "BEARISH")),
            _decision_priority(row), row.get("side") in hints
        )))
    selected["side_hints"] = sorted(hints)
    selected["side_hint_used_as_gate"] = False
    selected["side_analyses"] = {
        row.get("side"): {"state": row.get("decision_state"), "setup": row.get("decision_setup_type"),
                          "score": row.get("score"), "reason": row.get("decision_reason"),
                          "event_id": row.get("event_id"), "structural_stop": row.get("structural_stop")}
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
        state_bonus = {"ENTRY_READY": 3.0, "WAITING_FOR_CONFIRMATION": 1.8,
                       "REJECTED": 0.4, "EXPIRED": 0.0}.get(str(prior.get("decision_state", "")), 0.0)
        if str(prior.get("cross_status") or "").upper() == "CONFIRMED":
            state_bonus += 2.0
        location_bonus = {"EXCELLENT": 1.0, "GOOD": 0.7, "MODERATE": 0.4, "WAITING": 0.0}.get(
            str(prior.get("location_status") or "WAITING").upper(), 0.0)
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
        # A confirmed EMA9/Session-VWAP event is now the primary lifecycle trigger.
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
        updated = rank_number(prior.get("last_update", prior.get("last_updated", 0)))
        freshness = max(0.0, 1.0 - max(0.0, time.time() - updated) / 1800.0) if updated else 0.0
        # Re-rank using actual structural confluence/proximity from the previous analysis,
        # while rotating all symbols to prevent low-ranked candidates starving forever.
        priority = (state_bonus + location_bonus + 0.9 * golden_score + 0.55 * ob_near
                    + 0.20 * liquidity_quality + 0.20 * analysis_priority
                    + 0.20 * discovery_score + 0.20 * freshness
                    + vwap_cross_priority)
        return (priority, state_bonus, golden_score, float(item.get("score", 0) or 0), key)
    ordered = sorted(items.items(), key=candidate_rank, reverse=True)
    if not ordered:
        memory["unified_watchlist_last_update"] = time.time()
        return out
    limit = min(max(1, int(max_items)), len(ordered))
    candidates = ordered[:limit]
    cursor = int(memory.get("unified_watchlist_cursor", 0)) % len(candidates)
    count = min(max(1, int(batch_size)), len(candidates))
    selected = [candidates[(cursor + i) % len(candidates)] for i in range(count)]
    memory["unified_watchlist_cursor"] = (cursor + count) % len(candidates)
    for key, candidate in selected:
        try:
            frame = fetch_frame(candidate["exchange_symbol"])
        except Exception as exc:
            frame = None
            fetch_error = f"data_fetch_error:{type(exc).__name__}"
        else:
            fetch_error = "missing_or_invalid_market_data" if frame is None else None
        if frame is None:
            best = {"symbol": key, "exchange_symbol": candidate["exchange_symbol"], "side": "UNKNOWN",
                    "decision_state": "UNKNOWN", "entry_status": "UNKNOWN",
                    "decision_reason": fetch_error or "market_data_unavailable",
                    "rejection_reasons": [fetch_error or "market_data_unavailable"],
                    "data_quality": "DATA_UNAVAILABLE", "last_update": time.time(), "icons": {"status": "⚪"}}
        else:
            results = [analyze_symbol(candidate["exchange_symbol"], side, frame) for side in ("BUY", "SELL")]
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
            ready = [row for row in results if row.get("decision_state") == DecisionState.ENTRY_READY.value]
            confirmed_crosses = [
                row for row in results
                if str(row.get("cross_status") or "").upper() == "CONFIRMED"
                and str(row.get("cross_direction") or "").upper() == ("BULLISH" if row.get("side") == "BUY" else "BEARISH")
                and bool(row.get("event_id"))
            ]
            # Cross identity outranks generic side ranking even when its ADX or
            # location result is a rejection/wait. That rejected event must still
            # be written to the independent queue for audit and UI visibility.
            if len(confirmed_crosses) == 1:
                best = dict(confirmed_crosses[0])
                best["side_hint_used_as_gate"] = False
                best["side_selection_reason"] = "confirmed_cross_event_is_authoritative_even_when_not_ready"
            elif len(ready) > 1:
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
                best["side_hint_used_as_gate"] = False
                best["side_selection_reason"] = "both_directions_analyzed; discovery_side_is_soft_hint"
            best["side_analyses"] = {
                row.get("side"): {"state": row.get("decision_state"), "setup": row.get("decision_setup_type"),
                                  "score": row.get("score"), "reason": row.get("decision_reason"),
                                  "event_id": row.get("event_id"), "structural_stop": row.get("structural_stop"),
                                  "trend_support": (row.get("institutional_trend") or {}).get("supporting_check")}
                for row in results
            }
        previous = out.get(key, {})
        best["first_seen"] = previous.get("first_seen", time.time())
        best["discovery_sources"] = list(candidate.get("sources", []))
        best["discovery_score"] = float(candidate.get("score", 0) or 0)
        best["analysis_priority_score"] = round(_decision_priority(best)[0] * 10
            + float(best.get("score", 0) or 0) * 0.25
            + float((best.get("golden_zone") or {}).get("score", 0) or 0) * 0.15, 2)
        out[key] = {**previous, **best}
        # Cross Event is an independent lifecycle object; a later neutral scan
        # must not overwrite or erase the event merely because the symbol row changed.
        event_id = str(best.get("event_id") or "")
        if best.get("cross_status") == "CONFIRMED" and event_id and best.get("side") in {"BUY", "SELL"}:
            event_queue = memory.setdefault("cross_event_watchlist", {})
            prior_event = event_queue.get(event_id, {}) if isinstance(event_queue.get(event_id), dict) else {}
            event_queue[event_id] = {
                **prior_event, **best,
                "event_id": event_id, "cross_event_id": event_id,
                "first_seen": prior_event.get("first_seen", time.time()),
                "event_lifecycle_state": prior_event.get("event_lifecycle_state", "DISCOVERED"),
            }
    now = time.time()
    now_ms = int(now * 1000)
    event_queue = memory.setdefault("cross_event_watchlist", {})
    if isinstance(event_queue, dict):
        for event_id, event in list(event_queue.items()):
            if not isinstance(event, dict):
                event_queue.pop(event_id, None)
                continue
            if str(event.get("event_lifecycle_state") or "").upper() == "CONSUMED":
                continue
            try:
                cross_ts = int(event.get("cross_candle_timestamp") or 0)
                timeframe_ms = int(float(event.get("indicator_snapshot", {}).get("timeframe_seconds", TIMEFRAME_SECONDS)) * 1000)
                stale = cross_ts <= 0 or now_ms - cross_ts > max(1, timeframe_ms * 2)
            except (TypeError, ValueError, AttributeError):
                stale = True
            if stale:
                event["decision_state"] = "EXPIRED"
                event["entry_status"] = "EXPIRED"
                event["event_lifecycle_state"] = "EXPIRED"
                event["decision_reason"] = "cross_event_expired_after_two_closed_candles"
        # The durable trade journal is the long-term idempotency authority. Keep
        # the in-memory event queue bounded: retain active events, but age out
        # terminal Demo/Live lifecycle records after one day and cap leftovers.
        terminal = []
        for event_id, event in event_queue.items():
            if not isinstance(event, dict):
                terminal.append((0.0, event_id))
                continue
            lifecycle = str(event.get("event_lifecycle_state") or "").upper()
            if lifecycle in {"CONSUMED", "EXPIRED", "REJECTED"}:
                seen_at = float(event.get("last_update", event.get("first_seen", 0.0)) or 0.0)
                terminal.append((seen_at, event_id))
        retention_cutoff = now - 86_400.0
        for seen_at, event_id in terminal:
            if seen_at <= 0 or seen_at < retention_cutoff:
                event_queue.pop(event_id, None)
        if len(event_queue) > 2000:
            terminal = sorted(
                ((float(event.get("last_update", event.get("first_seen", 0.0)) or 0.0), event_id)
                 for event_id, event in event_queue.items()
                 if isinstance(event, dict)
                 and str(event.get("event_lifecycle_state") or "").upper() in {"CONSUMED", "EXPIRED", "REJECTED"}),
                key=lambda item: item[0],
            )
            for _, event_id in terminal[:max(0, len(event_queue) - 2000)]:
                event_queue.pop(event_id, None)
    protected = {"ENTRY_READY", "WAITING_FOR_RETEST", "WAITING_FOR_CONFIRMATION", "NO_CHASE"}
    for key, value in list(out.items()):
        age = now - float(value.get("last_update", value.get("last_updated", now)) or now)
        if age > 1800 and value.get("decision_state") not in protected:
            out.pop(key, None)
    memory["unified_watchlist_last_update"] = now
    memory["unified_watchlist_last_batch_count"] = len(selected)
    memory["unified_watchlist_source_count"] = len(items)
    return out
