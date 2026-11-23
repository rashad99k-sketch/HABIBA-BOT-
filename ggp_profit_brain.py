"""Evidence-based adaptive profit policy. Pure logic only; no exchange orders."""
from __future__ import annotations

from math import isfinite
from typing import Any, Dict, Iterable, Optional, Tuple

# Price-move targets for RANGE_SCALP. Fees, funding and slippage are evaluated
# separately before the target is accepted by the caller.
RANGE_PROFIT_TARGETS_PCT = (0.25, 0.50, 1.00)
RANGE_MODE = "RANGE_SCALP"
TREND_MODE = "TREND_RIDE"
STANDARD_MODE = "TWO_STAGE_50_50"


def _number(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        result = float(value)
        return result if isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _bool(ctx: Dict[str, Any], key: str) -> bool:
    return bool(ctx.get(key, False))


def _side_aligned(side: str, plus: Optional[float], minus: Optional[float]) -> bool:
    if plus is None or minus is None:
        return False
    return plus > minus if side == "BUY" else minus > plus


def adaptive_trail_multiplier(base_multiplier: float, continuation_probability: float, adx: float,
                              distribution_risk: float, exit_warning: int, ema_state: str = "UNKNOWN",
                              peak_drawdown_roe: float = 0.0, tp1_verified: bool = True,
                              trend_evidence_score: float = 0.0) -> float:
    """Adjust runner ATR trail from continuation/structure evidence; ADX is only context."""
    mult = _number(base_multiplier, 1.5) or 1.5
    cont = _number(continuation_probability, 0.0) or 0.0
    dist = _number(distribution_risk, 0.0) or 0.0
    warning = int(_number(exit_warning, 0.0) or 0)
    giveback = max(0.0, _number(peak_drawdown_roe, 0.0) or 0.0)
    evidence = _number(trend_evidence_score, 0.0) or 0.0
    # Never widen simply because ADX is numerically high.
    if evidence >= 70 and cont >= 0.72 and dist < 35 and warning <= 1:
        mult *= 1.18
    elif evidence < 45 or cont < 0.50 or dist >= 45 or warning >= 3:
        mult *= 0.75
    if dist >= 65 or warning >= 4:
        mult *= 0.75
    if ema_state in {"TREND_RIDE", "EMA20_PULLBACK", "EMA50_PULLBACK"} and evidence >= 60 and cont >= 0.60 and dist < 45 and warning <= 2:
        mult *= 1.08
    elif ema_state == "EMA20_RETEST_PENDING" and evidence >= 55 and cont >= 0.50 and dist < 45 and warning <= 2:
        mult *= 1.04
    elif ema_state in {"EMA50_BREAK_PENDING", "EMA50_BREAK_CONFIRMED"}:
        mult *= 0.82 if ema_state == "EMA50_BREAK_PENDING" else 0.65
    if giveback >= 12:
        mult *= 0.80
    if giveback >= 25:
        mult *= 0.75
    if tp1_verified:
        mult *= 0.95
    return max(0.55, min(3.5, mult))


def ema_trend_context(df_closed: Any, side: str, atr: float = 0.0) -> Dict[str, Any]:
    """Classify completed-candle EMA20/EMA50 structure; no live/incomplete bar use."""
    result = {"state": "INSUFFICIENT_DATA", "ema20": None, "ema50": None,
              "close": None, "trend_aligned": False, "hold_structure": False,
              "break_confirmed": False}
    direction = str(side or "").upper()
    if direction not in {"BUY", "SELL"} or df_closed is None or len(df_closed) < 55:
        return result
    try:
        import pandas as pd
        close = pd.to_numeric(df_closed["close"], errors="coerce").dropna().astype(float)
        if len(close) < 55:
            return result
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        c, e20, e50 = float(close.iloc[-1]), float(ema20.iloc[-1]), float(ema50.iloc[-1])
        p1, p2 = float(close.iloc[-2]), float(close.iloc[-3])
        aligned = e20 > e50 if direction == "BUY" else e20 < e50
        if direction == "BUY":
            if aligned and c >= e20: state = "TREND_RIDE"
            elif aligned and c >= e50: state = "EMA20_PULLBACK"
            elif aligned and c < e50: state = "EMA50_BREAK_PENDING" if p1 >= float(ema50.iloc[-2]) else "EMA50_BREAK_CONFIRMED"
            else: state = "EMA50_BREAK_PENDING" if c >= e50 else "EMA50_BREAK_CONFIRMED"
        else:
            if aligned and c <= e20: state = "TREND_RIDE"
            elif aligned and c <= e50: state = "EMA20_PULLBACK"
            elif aligned and c > e50: state = "EMA50_BREAK_PENDING" if p1 <= float(ema50.iloc[-2]) else "EMA50_BREAK_CONFIRMED"
            else: state = "EMA50_BREAK_PENDING" if c <= e50 else "EMA50_BREAK_CONFIRMED"
        result.update({"state": state, "ema20": e20, "ema50": e50, "close": c,
                       "trend_aligned": bool(aligned),
                       "hold_structure": state in {"TREND_RIDE", "EMA20_PULLBACK"},
                       "break_confirmed": state == "EMA50_BREAK_CONFIRMED"})
        return result
    except Exception:
        return result


def classify_profit_mode(context: Dict[str, Any]) -> Tuple[str, str, Tuple[str, ...]]:
    """Classify only from observed evidence; UNKNOWN never means RANGE_SCALP.

    RANGE_SCALP is deliberately gated by multiple independent signs of a weak,
    choppy or thin market. Missing data falls back to the safer 50/50 plan.
    """
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return STANDARD_MODE, "invalid_side_default_two_stage", ("invalid_side",)

    regime = str(ctx.get("regime") or "UNKNOWN").upper()
    state = str(ctx.get("trade_state") or "UNKNOWN").upper()
    adx = _number(ctx.get("adx"))
    adx_slope = _number(ctx.get("adx_slope"))
    continuation = _number(ctx.get("continuation_probability"))
    plus = _number(ctx.get("di_plus"))
    minus = _number(ctx.get("di_minus"))
    volume_ratio = _number(ctx.get("volume_ratio"))
    momentum_strength = _number(ctx.get("momentum_strength"))
    spread_pct = _number(ctx.get("spread_pct"))
    depth_usdt = _number(ctx.get("orderbook_depth_usdt"))
    liquidity_quality = str(ctx.get("liquidity_quality") or "UNKNOWN").upper()
    structure_broken = bool(ctx.get("structure_broken", False))

    di_known = plus is not None and minus is not None
    di_aligned = (plus > minus) if side == "BUY" and di_known else (minus > plus) if side == "SELL" and di_known else False
    di_spread = abs(plus - minus) if di_known else None
    regime_range = regime in {"RANGE", "RANGE_CHOP", "CHOP", "CHOPPY", "COMPRESSION"} or state in {"RANGE_CHOP", "CHOP", "COMPRESSION"}
    weak_adx = adx is not None and (adx < 18.0 or (adx < 20.0 and adx_slope is not None and adx_slope <= 0.0))
    weak_continuation = continuation is not None and continuation <= 0.48
    low_volume = volume_ratio is not None and volume_ratio <= 0.75
    very_low_volume = volume_ratio is not None and volume_ratio <= 0.60
    di_tangled = di_spread is not None and di_spread <= 3.0
    weak_momentum = momentum_strength is not None and momentum_strength < 35.0
    wide_spread = spread_pct is not None and spread_pct >= 0.15
    shallow_book = depth_usdt is not None and depth_usdt > 0 and depth_usdt < _number(ctx.get("min_orderbook_depth_usdt"), 0.0)
    explicit_thin = liquidity_quality in {"THIN", "LOW", "ILLIQUID", "POOR"}
    thin_liquidity = explicit_thin or wide_spread or shallow_book

    evidence = []
    if regime_range:
        evidence.append("range_or_chop_regime")
    if weak_adx:
        evidence.append("weak_or_falling_adx")
    if weak_continuation:
        evidence.append("weak_continuation_probability")
    if low_volume:
        evidence.append("low_relative_volume")
    if di_tangled:
        evidence.append("di_pressure_tangled")
    if weak_momentum:
        evidence.append("weak_momentum")
    if thin_liquidity:
        evidence.append("thin_liquidity_evidence")
    if structure_broken:
        evidence.append("trend_structure_broken")

    # Trend riding requires several independent, side-aligned observations.
    momentum_support = (momentum_strength is not None and momentum_strength >= 55.0) or bool(ctx.get("trend_expansion", False))
    regime_trend = regime in {"STRONG_TREND", "VERY_STRONG", "EXPANSION"} or state in {"TREND_RIDE", "EXPANSION", "HEALTHY_PULLBACK"}
    confirmed_trend = (
        not structure_broken
        and regime_trend
        and adx is not None and adx >= 22.0
        and adx_slope is not None and adx_slope >= -1.5
        and continuation is not None and continuation >= 0.62
        and di_aligned
        and momentum_support
    )
    if confirmed_trend:
        return TREND_MODE, "confirmed_directional_regime_and_continuation", tuple(evidence + ["directional_di_alignment"])

    weakness_count = sum((weak_adx, weak_continuation, low_volume, di_tangled, weak_momentum, thin_liquidity, structure_broken))
    # RANGE_SCALP requires an identifiable range/weak regime plus more than one
    # supporting weakness, OR explicit thin-liquidity evidence plus weak trend data.
    range_confirmed = regime_range and weakness_count >= 2 and adx is not None and continuation is not None
    weak_market_confirmed = (
        weak_adx and weak_continuation and (low_volume or di_tangled or thin_liquidity)
    )
    thin_market_confirmed = thin_liquidity and (weak_adx or weak_continuation or low_volume) and weakness_count >= 2
    if range_confirmed or weak_market_confirmed or thin_market_confirmed:
        return RANGE_MODE, "confirmed_range_weak_market_or_thin_liquidity", tuple(evidence)

    # Crucial fail-safe: indicator/API gaps do not automatically force a short
    # scalp or a full-position exit. Keep the canonical 50/50 management model.
    return STANDARD_MODE, "range_conditions_not_proven_keep_two_stage_50_50", tuple(evidence or ("insufficient_range_evidence",))


def classify_profit_mode(context: Dict[str, Any]) -> Tuple[str, str, Tuple[str, ...]]:
    """Use independent, observable evidence. ADX never decides regime alone.

    Trend evidence is built from directional structure, VWAP/EMA alignment and
    slope, DI pressure, momentum continuation, relative volume, ATR expansion,
    candle follow-through and liquidity. Weakness is the inverse pattern.
    Unknown inputs are neutral, never silently interpreted as weak.
    """
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return STANDARD_MODE, "invalid_side_default_two_stage", ("invalid_side",)

    adx = _number(ctx.get("adx"))
    adx_slope = _number(ctx.get("adx_slope"))
    plus, minus = _number(ctx.get("di_plus")), _number(ctx.get("di_minus"))
    continuation = _number(ctx.get("continuation_probability"))
    momentum = _number(ctx.get("momentum_strength"))
    volume_ratio = _number(ctx.get("volume_ratio"))
    atr_expansion = _number(ctx.get("atr_expansion_ratio"))
    vwap_slope = _number(ctx.get("vwap_slope_atr"))
    ema_slope = _number(ctx.get("ema9_slope_atr"))
    structure_score = _number(ctx.get("structure_score"))
    trend_score = _number(ctx.get("trend_evidence_score"))
    weak_score = _number(ctx.get("weakness_evidence_score"))
    liquidity_quality = str(ctx.get("liquidity_quality") or "UNKNOWN").upper()
    spread_pct = _number(ctx.get("spread_pct"))
    depth = _number(ctx.get("orderbook_depth_usdt"))
    min_depth = _number(ctx.get("min_orderbook_depth_usdt"), 0.0) or 0.0
    structure_broken = _bool(ctx, "structure_broken")
    adverse_divergence = _bool(ctx, "adverse_divergence")
    opposing_liquidity_near = _bool(ctx, "opposing_liquidity_near")
    di_aligned = _side_aligned(side, plus, minus)
    di_spread = abs(plus-minus) if plus is not None and minus is not None else None

    trend_signals = {
        "directional_di": di_aligned and di_spread is not None and di_spread >= 5.0,
        "ema_vwap_slope": ema_slope is not None and vwap_slope is not None and
                          (ema_slope > 0 and vwap_slope > 0 if side == "BUY" else ema_slope < 0 and vwap_slope < 0),
        "structure": structure_score is not None and structure_score >= 60 and not structure_broken,
        "continuation": continuation is not None and continuation >= 0.62,
        "momentum": momentum is not None and momentum >= 55,
        "relative_volume": volume_ratio is not None and volume_ratio >= 1.0,
        "atr_expansion": atr_expansion is not None and atr_expansion >= 1.05,
        "candle_follow_through": _bool(ctx, "candle_follow_through"),
        "adx_support": adx is not None and adx_slope is not None and adx >= 18 and adx_slope >= 0,
    }
    trend_count = sum(bool(v) for v in trend_signals.values())
    if trend_score is None:
        trend_score = round(100 * trend_count / len(trend_signals), 1)
    # ADX supplies at most one supporting vote out of nine. A high ADX with
    # no directional structure/flow cannot qualify as trend ride.
    trend_qualified = (
        trend_count >= 6
        and trend_signals["directional_di"]
        and trend_signals["ema_vwap_slope"]
        and trend_signals["structure"]
        and trend_signals["continuation"]
        and not structure_broken
        and not adverse_divergence
    )
    if trend_qualified:
        evidence = tuple(k for k, v in trend_signals.items() if v)
        return TREND_MODE, "multi_factor_directional_trend_confirmed", evidence

    thin_liquidity = (
        liquidity_quality in {"THIN", "LOW", "ILLIQUID", "POOR"}
        or (spread_pct is not None and spread_pct >= 0.15)
        or (depth is not None and min_depth > 0 and depth < min_depth)
    )
    weak_signals = {
        "adx_not_supportive": adx is not None and adx_slope is not None and (adx_slope < 0 or adx < 18),
        "di_tangled_or_opposed": not di_aligned or (di_spread is not None and di_spread < 4),
        "momentum_weak": momentum is not None and momentum < 35,
        "continuation_weak": continuation is not None and continuation < 0.48,
        "relative_volume_low": volume_ratio is not None and volume_ratio < 0.75,
        "atr_compression": atr_expansion is not None and atr_expansion < 0.85,
        "flat_or_opposed_vwap_ema": (ema_slope is not None and vwap_slope is not None and
                                    (abs(ema_slope) < 0.05 or abs(vwap_slope) < 0.05 or
                                     (ema_slope * vwap_slope < 0))),
        "structure_weak_or_broken": structure_broken or (structure_score is not None and structure_score < 40),
        "liquidity_thin": thin_liquidity,
        "adverse_divergence": adverse_divergence,
        "opposing_liquidity_near": opposing_liquidity_near,
    }
    weak_count = sum(bool(v) for v in weak_signals.values())
    if weak_score is None:
        weak_score = round(100 * weak_count / len(weak_signals), 1)
    regime = str(ctx.get("regime") or "").upper()
    explicit_range = regime in {"RANGE", "RANGE_CHOP", "CHOP", "CHOPPY", "COMPRESSION"} or _bool(ctx, "range_structure")
    # Require a weak/range footprint and multiple independent weak confirmations.
    # ADX by itself cannot activate scalp mode, even at 10 or at 50.
    range_confirmed = (explicit_range and weak_count >= 3 and
                       (not trend_signals["structure"] or not trend_signals["ema_vwap_slope"]))
    thin_confirmed = thin_liquidity and weak_count >= 3 and not trend_qualified
    weak_confirmed = weak_count >= 5 and not trend_qualified and (
        trend_count <= 3 or explicit_range
    )
    evidence = tuple(k for k, v in weak_signals.items() if v)
    if range_confirmed or thin_confirmed or weak_confirmed:
        return RANGE_MODE, "multi_factor_weak_or_range_market_confirmed", evidence
    return STANDARD_MODE, "evidence_inconclusive_keep_two_stage_50_50", evidence or ("insufficient_evidence",)


def select_range_target_pct(context: Dict[str, Any]) -> Tuple[float, str]:
    """Select 0.25/0.50/1.00% price-move scalp target from entry quality and room."""
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return RANGE_PROFIT_TARGETS_PCT[0], "invalid_side_minimum_target"
    costs = _number(ctx.get("estimated_round_trip_cost_pct"))
    if costs is None:
        return RANGE_PROFIT_TARGETS_PCT[0], "cost_data_unavailable_minimum_target"
    net_min = _number(ctx.get("minimum_net_profit_pct"), 0.10) or 0.10
    space = _number(ctx.get("opposing_liquidity_distance_pct"))
    quality = _number(ctx.get("entry_quality_score"), 0.0) or 0.0
    momentum = _number(ctx.get("momentum_strength"))
    continuation = _number(ctx.get("continuation_probability"))
    volume = _number(ctx.get("volume_ratio"))
    adx = _number(ctx.get("adx"))
    plus, minus = _number(ctx.get("di_plus")), _number(ctx.get("di_minus"))
    di_ok = _side_aligned(side, plus, minus)
    adverse = _bool(ctx, "adverse_rejection") or _bool(ctx, "adverse_divergence")
    thin = _bool(ctx, "thin_liquidity") or str(ctx.get("liquidity_quality") or "").upper() in {"THIN", "LOW", "ILLIQUID", "POOR"}
    # Do not trade a scalp whose nearest counter-liquidity leaves insufficient
    # room to cover costs and a meaningful net return.
    candidates = []
    for target in RANGE_PROFIT_TARGETS_PCT:
        net = target - costs
        room_ok = space is not None and space >= target * 1.10
        if net >= net_min and room_ok:
            candidates.append(target)
    if not candidates:
        return 0.0, "no_target_has_verified_net_edge_and_forward_liquidity_room"
    max_target = max(candidates)
    strong_evidence = (
        quality >= 75 and momentum is not None and momentum >= 60
        and continuation is not None and continuation >= 0.65
        and volume is not None and volume >= 1.15 and di_ok
        and not adverse and not thin
    )
    medium_evidence = (
        quality >= 55 and momentum is not None and momentum >= 42
        and continuation is not None and continuation >= 0.50
        and not adverse and not thin
    )
    if max_target >= 1.0 and strong_evidence:
        return 1.0, "strong_entry_momentum_volume_and_forward_liquidity"
    if max_target >= 0.50 and medium_evidence:
        return 0.50, "moderate_entry_quality_and_forward_liquidity"
    if max_target >= 0.25:
        return 0.25, "selective_micro_scalp_cost_and_room_verified"
    return 0.0, "no_acceptable_target"


def trend_target_plan(side: str, entry_price: float, atr: float,
                      opposing_liquidity_levels: Iterable[float] = (),
                      target_move_pct: float = 2.0,
                      minimum_space_atr: float = 1.0) -> Tuple[float, str]:
    """Plan TP2 from the nearer of 2-3% price expansion and opposing liquidity."""
    direction = str(side or "").upper()
    entry = _number(entry_price, 0.0) or 0.0
    volatility = _number(atr, 0.0) or 0.0
    move = _number(target_move_pct, 0.0) or 0.0
    if direction not in {"BUY", "SELL"} or entry <= 0 or volatility <= 0 or move <= 0:
        return 0.0, "trend_target_insufficient_data"
    raw = entry * (1 + move/100) if direction == "BUY" else entry * (1 - move/100)
    levels = []
    for level in opposing_liquidity_levels or ():
        value = _number(level)
        if value is None:
            continue
        distance = value-entry if direction == "BUY" else entry-value
        if distance >= minimum_space_atr * volatility:
            levels.append(value)
    if levels:
        pool = min(levels, key=lambda value: value-entry if direction == "BUY" else entry-value)
        target = min(raw, pool) if direction == "BUY" else max(raw, pool)
        if (target-entry if direction == "BUY" else entry-target) <= 0:
            return 0.0, "opposing_liquidity_invalidates_trend_target"
        return round(target, 12), "trend_target_capped_before_opposing_liquidity"
    return round(raw, 12), "trend_extension_target_no_nearer_opposing_pool"


def range_target_price(side: str, entry_price: float, target_move_pct: float) -> float:
    side = str(side or "").upper()
    entry = _number(entry_price, 0.0) or 0.0
    move = _number(target_move_pct, 0.0) or 0.0
    if side not in {"BUY", "SELL"} or entry <= 0 or move <= 0:
        return 0.0
    factor = move / 100.0
    target = entry * (1.0 + factor) if side == "BUY" else entry * (1.0 - factor)
    return round(target, 12)


def target_reached(side: str, current_price: float, target_price: float) -> bool:
    side = str(side or "").upper()
    current = _number(current_price, 0.0) or 0.0
    target = _number(target_price, 0.0) or 0.0
    if side not in {"BUY", "SELL"} or current <= 0 or target <= 0:
        return False
    return current >= target if side == "BUY" else current <= target


def select_range_target_pct(context: Dict[str, Any]) -> Tuple[float, str]:
    """Select a conservative 0.5/1/2% price-move target for proven range mode.

    Thin/weak conditions always select 0.5%. 1% and 2% require completed-candle
    follow-through and independent volume/ADX/DI support, even inside a range.
    """
    ctx = context if isinstance(context, dict) else {}
    adx = _number(ctx.get("adx"), 0.0) or 0.0
    continuation = _number(ctx.get("continuation_probability"), 0.0) or 0.0
    volume_ratio = _number(ctx.get("volume_ratio"), 0.0) or 0.0
    plus = _number(ctx.get("di_plus"))
    minus = _number(ctx.get("di_minus"))
    side = str(ctx.get("side") or "").upper()
    low_liquidity = bool(ctx.get("thin_liquidity", False)) or volume_ratio <= 0.75
    if low_liquidity or adx < 18.0 or continuation < 0.45:
        return RANGE_PROFIT_TARGETS_PCT[0], "weak_or_thin_market_conservative_0.5pct"
    di_aligned = (plus > minus) if side == "BUY" and plus is not None and minus is not None else (minus > plus) if side == "SELL" and plus is not None and minus is not None else False
    aligned_count = _number(ctx.get("aligned_closed_candles"), 0.0) or 0.0
    adverse_rejection = bool(ctx.get("adverse_rejection", False))
    if aligned_count >= 2 and continuation >= 0.72 and adx >= 22 and volume_ratio >= 1.20 and di_aligned and not adverse_rejection:
        return RANGE_PROFIT_TARGETS_PCT[2], "strong_closed_candle_follow_through_2pct"
    if aligned_count >= 2 and continuation >= 0.55 and adx >= 18 and volume_ratio >= 0.85 and di_aligned and not adverse_rejection:
        return RANGE_PROFIT_TARGETS_PCT[1], "moderate_follow_through_1pct"
    return RANGE_PROFIT_TARGETS_PCT[0], "weak_follow_through_conservative_0.5pct"


def tp2_reassessment(side: str, current_price: float, tp2_price: float, atr: float,
                     continuation_probability: float, adx: float, adx_slope: float,
                     structure_aligned: bool, max_atr_distance: float = 3.0,
                     min_continuation: float = 0.45) -> Tuple[str, str]:
    side = str(side or "").upper()
    current = _number(current_price, 0.0) or 0.0
    target = _number(tp2_price, 0.0) or 0.0
    atr_value = _number(atr, 0.0) or 0.0
    continuation = _number(continuation_probability, 0.0) or 0.0
    adx_value = _number(adx, 0.0) or 0.0
    slope = _number(adx_slope, 0.0) or 0.0
    if side not in {"BUY", "SELL"} or current <= 0 or target <= 0 or atr_value <= 0:
        return "HOLD", "insufficient_tp2_evidence"
    distance = target - current if side == "BUY" else current - target
    if distance <= 0:
        return "HOLD", "target_reached"
    remote = distance / atr_value > max_atr_distance
    weakening = continuation < min_continuation or (adx_value < 18 and slope <= 0)
    if remote and weakening:
        return "CLOSE", "tp2_remote_and_continuation_weak"
    if remote and not structure_aligned:
        return "CLOSE", "tp2_remote_and_structure_lost"
    return "HOLD", "runner_structure_and_continuation_acceptable"


def thesis_failure_action(side: str, roe_pct: float, failure_score: float,
                          failure_reasons: Iterable[str], structural_invalidation: bool = False) -> Tuple[str, str]:
    """Return EXIT/PROTECT/HOLD from independent thesis-failure evidence.

    One weak signal does not force liquidation. A structural invalidation, a very
    high failure score, or two independent severe signals at a failed-thesis score
    are required for a strict exit. A positive trade with a moderate warning is
    protected at breakeven rather than force-closed.
    """
    direction = str(side or "").upper()
    reasons = {str(item).lower() for item in (failure_reasons or [])}
    roe = _number(roe_pct, 0.0) or 0.0
    score = _number(failure_score, 0.0) or 0.0
    opposite_candle = "strong_bearish_candle" in reasons if direction == "BUY" else "strong_bullish_candle" in reasons
    di_flip = "di_flip_bearish" in reasons if direction == "BUY" else "di_flip_bullish" in reasons
    severe_count = sum((
        "strong_reclaim" in reasons,
        di_flip,
        opposite_candle,
        "low_continuation_pressure" in reasons,
    ))
    if structural_invalidation:
        return "EXIT", "structural_invalidation"
    if score >= 70.0:
        return "EXIT", f"confirmed_thesis_failure_{score:.0f}"
    if score >= 50.0 and severe_count >= 2:
        return "EXIT", "multi_evidence_thesis_failure:" + ",".join(sorted(reasons))
    if score >= 50.0 and roe > 0.0:
        return "PROTECT", "soft_thesis_warning_protect_positive_trade"
    return "HOLD", "thesis_failure_not_confirmed"


def adaptive_range_target_price(side: str, entry_price: float, atr: float,
                                swing_levels: Iterable[float] = (),
                                min_atr: float = 0.55, max_atr: float = 2.50,
                                pool_buffer_atr: float = 0.10) -> Tuple[float, str]:
    """Choose a range target from the nearest opposing swing-liquidity pool.

    Only levels beyond entry are considered. A target is placed just before the
    pool; if no credible nearby pool exists, use a conservative ATR fallback.
    This is a price target planner only; it never submits or modifies orders.
    """
    direction = str(side or "").upper()
    entry = _number(entry_price, 0.0) or 0.0
    volatility = _number(atr, 0.0) or 0.0
    if direction not in {"BUY", "SELL"} or entry <= 0 or volatility <= 0:
        return 0.0, "adaptive_target_insufficient_data"
    levels = []
    for raw in swing_levels or ():
        level = _number(raw)
        if level is None or level <= 0:
            continue
        distance = (level - entry) if direction == "BUY" else (entry - level)
        if min_atr * volatility <= distance <= max_atr * volatility:
            levels.append((distance, level))
    if levels:
        distance, level = min(levels, key=lambda item: item[0])
        buffer = min(pool_buffer_atr * volatility, distance * 0.20)
        target = level - buffer if direction == "BUY" else level + buffer
        if (direction == "BUY" and target > entry) or (direction == "SELL" and target < entry):
            return round(target, 12), "nearest_opposing_swing_liquidity_pool"
    fallback_distance = 1.10 * volatility
    target = entry + fallback_distance if direction == "BUY" else entry - fallback_distance
    return round(target, 12), "atr_fallback_no_credible_nearby_liquidity"
