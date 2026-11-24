"""Single, deterministic profit policy. Pure functions only; no exchange calls."""
from __future__ import annotations
from math import isfinite
from typing import Any, Dict, Iterable, Optional, Tuple

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
    """Adapt runner ATR defense from trend evidence; ADX is not a numeric trigger."""
    mult = _number(base_multiplier, 1.5) or 1.5
    cont = _number(continuation_probability, 0.0) or 0.0
    dist = _number(distribution_risk, 0.0) or 0.0
    warning = int(_number(exit_warning, 0.0) or 0)
    giveback = max(0.0, _number(peak_drawdown_roe, 0.0) or 0.0)
    evidence = _number(trend_evidence_score, 0.0) or 0.0
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
    """Classify weak/range vs trend from independent evidence; ADX has one small vote."""
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return STANDARD_MODE, "invalid_side_default_two_stage", ("invalid_side",)
    adx, adx_slope = _number(ctx.get("adx")), _number(ctx.get("adx_slope"))
    plus, minus = _number(ctx.get("di_plus")), _number(ctx.get("di_minus"))
    continuation = _number(ctx.get("continuation_probability"))
    momentum = _number(ctx.get("momentum_strength"))
    volume = _number(ctx.get("volume_ratio"))
    atr_exp = _number(ctx.get("atr_expansion_ratio"))
    vwap_slope = _number(ctx.get("vwap_slope_atr"))
    ema_slope = _number(ctx.get("ema9_slope_atr"))
    structure = _number(ctx.get("structure_score"))
    liquidity = str(ctx.get("liquidity_quality") or "UNKNOWN").upper()
    spread = _number(ctx.get("spread_pct"))
    depth = _number(ctx.get("orderbook_depth_usdt"))
    min_depth = _number(ctx.get("min_orderbook_depth_usdt"), 0.0) or 0.0
    broken = _bool(ctx, "structure_broken")
    adverse_div = _bool(ctx, "adverse_divergence")
    opposing_pool = _bool(ctx, "opposing_liquidity_near")
    di_ok = _side_aligned(side, plus, minus)
    di_spread = abs(plus-minus) if plus is not None and minus is not None else None
    trend = {
        "directional_di": di_ok and di_spread is not None and di_spread >= 5,
        "ema_vwap_slope": ema_slope is not None and vwap_slope is not None and
            (ema_slope > 0 and vwap_slope > 0 if side == "BUY" else ema_slope < 0 and vwap_slope < 0),
        "structure": structure is not None and structure >= 60 and not broken,
        "continuation": continuation is not None and continuation >= .62,
        "momentum": momentum is not None and momentum >= 55,
        "relative_volume": volume is not None and volume >= 1.0,
        "atr_expansion": atr_exp is not None and atr_exp >= 1.05,
        "candle_follow_through": _bool(ctx, "candle_follow_through"),
        "adx_support": adx is not None and adx_slope is not None and adx >= 18 and adx_slope >= 0,
    }
    trend_count = sum(bool(v) for v in trend.values())
    # Mandatory directional structure, VWAP/EMA slope and continuation, plus at
    # least three other independent confirmations. ADX is only one vote.
    if (trend_count >= 6 and trend["directional_di"] and trend["ema_vwap_slope"]
            and trend["structure"] and trend["continuation"] and not broken and not adverse_div):
        return TREND_MODE, "multi_factor_directional_trend_confirmed", tuple(k for k,v in trend.items() if v)

    thin = (liquidity in {"THIN","LOW","ILLIQUID","POOR"}
            or (spread is not None and spread >= .15)
            or (depth is not None and min_depth > 0 and depth < min_depth))
    weak = {
        "adx_not_supportive": adx is not None and adx_slope is not None and (adx_slope < 0 or adx < 18),
        "di_tangled_or_opposed": not di_ok or (di_spread is not None and di_spread < 4),
        "momentum_weak": momentum is not None and momentum < 35,
        "continuation_weak": continuation is not None and continuation < .48,
        "relative_volume_low": volume is not None and volume < .75,
        "atr_compression": atr_exp is not None and atr_exp < .85,
        "flat_or_opposed_vwap_ema": ema_slope is not None and vwap_slope is not None and
            (abs(ema_slope) < .05 or abs(vwap_slope) < .05 or ema_slope*vwap_slope < 0),
        "structure_weak_or_broken": broken or (structure is not None and structure < 40),
        "liquidity_thin": thin,
        "adverse_divergence": adverse_div,
        "opposing_liquidity_near": opposing_pool,
    }
    weak_count = sum(bool(v) for v in weak.values())
    regime = str(ctx.get("regime") or "").upper()
    range_regime = regime in {"RANGE","RANGE_CHOP","CHOP","CHOPPY","COMPRESSION"} or _bool(ctx,"range_structure")
    if (not trend_count >= 6 and
        ((range_regime and weak_count >= 3) or (thin and weak_count >= 3) or weak_count >= 5)):
        return RANGE_MODE, "multi_factor_weak_or_range_market_confirmed", tuple(k for k,v in weak.items() if v)
    return STANDARD_MODE, "evidence_not_proven_keep_two_stage_50_50", tuple(k for k,v in weak.items() if v) or ("insufficient_evidence",)


MANAGEMENT_REGIME_LABELS = ("SCALP", "TREND", "CONTINUATION", "HIGH MOMENTUM", "WEAK MOMENTUM")
MANAGEMENT_AUTHORITY = "TradeManagementBoard / LiveTradeManager"


def classify_management_regime(context: Dict[str, Any]) -> Dict[str, Any]:
    """Diagnostic-only regime label for the Unified Trade Management Brain.

    This helper never authorizes an entry, changes sizing, sets ``profit_mode``,
    or emits an order. The entry engine supplies evidence at handoff; the Brain
    and its management board remain the sole routine exit authority.
    """
    ctx = context if isinstance(context, dict) else {}
    mode, mode_reason, mode_evidence = classify_profit_mode(ctx)
    side = str(ctx.get("side") or "").upper()
    continuation = _number(ctx.get("continuation_probability"))
    momentum = _number(ctx.get("momentum_strength"))
    volume = _number(ctx.get("volume_ratio"))
    structure = _number(ctx.get("structure_score"))
    vwap_slope = _number(ctx.get("vwap_slope_atr"))
    ema_slope = _number(ctx.get("ema9_slope_atr"))
    broken = _bool(ctx, "structure_broken")
    adverse = _bool(ctx, "adverse_divergence")

    if side not in {"BUY", "SELL"}:
        label, reason = "WEAK MOMENTUM", "invalid_side_no_directional_regime"
        evidence = ("invalid_side",)
    elif mode == RANGE_MODE:
        label, reason = "SCALP", "brain_selected_range_scalp_policy"
        evidence = tuple(mode_evidence) + ("profit_mode:" + mode,)
    elif mode == TREND_MODE and not broken and not adverse:
        label, reason = "TREND", "brain_confirmed_multifactor_trend"
        evidence = tuple(mode_evidence) + ("profit_mode:" + mode,)
    elif (momentum is not None and momentum >= 70
          or (momentum is not None and momentum >= 60 and volume is not None and volume >= 1.30)):
        label, reason = "HIGH MOMENTUM", "brain_detected_expansive_momentum"
        evidence = ("momentum_strong",)
    elif (continuation is not None and continuation >= 0.62
          and structure is not None and structure >= 55 and not broken):
        label, reason = "CONTINUATION", "brain_detected_continuation_bias"
        evidence = ("continuation_strong", "structure_supports_continuation")
    elif (continuation is not None and continuation < 0.48
          or momentum is not None and momentum < 35
          or broken or adverse
          or (vwap_slope is not None and ema_slope is not None
              and (abs(vwap_slope) < 0.05 or abs(ema_slope) < 0.05 or vwap_slope * ema_slope < 0))):
        label, reason = "WEAK MOMENTUM", "brain_detected_weak_or_opposed_momentum"
        evidence = tuple(mode_evidence) + ("weak_momentum_evidence",)
    else:
        label, reason = "WEAK MOMENTUM", "insufficient_brain_evidence_keep_conservative"
        evidence = tuple(mode_evidence) or ("insufficient_evidence",)

    return {
        "label": label,
        "reason": reason,
        "evidence": tuple(evidence),
        "source_profit_mode": mode,
        "source_profit_mode_reason": mode_reason,
        "authoritative": False,
        "authority": MANAGEMENT_AUTHORITY,
    }


def select_range_target_pct(context: Dict[str, Any]) -> Tuple[float, str]:
    """Select 0.25/0.50/1.00% price move after cost and opposing liquidity tests."""
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    costs = _number(ctx.get("estimated_round_trip_cost_pct"))
    room = _number(ctx.get("opposing_liquidity_distance_pct"))
    if side not in {"BUY","SELL"} or costs is None or room is None:
        return 0.0, "missing_side_cost_or_opposing_liquidity"
    min_net = _number(ctx.get("minimum_net_profit_pct"), .10) or .10
    quality = _number(ctx.get("entry_quality_score"), 0) or 0
    momentum = _number(ctx.get("momentum_strength"))
    cont = _number(ctx.get("continuation_probability"))
    volume = _number(ctx.get("volume_ratio"))
    di_ok = _side_aligned(side, _number(ctx.get("di_plus")), _number(ctx.get("di_minus")))
    adverse = _bool(ctx,"adverse_rejection") or _bool(ctx,"adverse_divergence")
    thin = _bool(ctx,"thin_liquidity") or str(ctx.get("liquidity_quality") or "").upper() in {"THIN","LOW","ILLIQUID","POOR"}
    avail = [t for t in RANGE_PROFIT_TARGETS_PCT if t-costs >= min_net and room >= t*1.10]
    if not avail:
        return 0.0, "no_target_has_net_edge_and_forward_room"
    mx = max(avail)
    strong = quality >= 75 and momentum is not None and momentum >= 60 and cont is not None and cont >= .65 and volume is not None and volume >= 1.15 and di_ok and not adverse and not thin
    medium = quality >= 55 and momentum is not None and momentum >= 42 and cont is not None and cont >= .50 and not adverse and not thin
    if mx >= 1.0 and strong:
        return 1.0, "strong_entry_quality_and_forward_liquidity"
    if mx >= .50 and medium:
        return .50, "moderate_entry_quality_and_forward_liquidity"
    return .25, "selective_micro_scalp_cost_and_room_verified"


def plan_trend_targets(side: str, entry_price: float, atr: float,
                         opposing_liquidity_levels: Iterable[float] = (),
                         target1_atr: float = 1.0, target2_price_move_pct: float = 2.0,
                         min_target2_atr: float = 1.5,
                         pool_buffer_atr: float = 0.10) -> Tuple[float, float, str]:
    """Plan 50/50 TP1/TP2 from volatility and opposing liquidity.

    TP1 is the nearer of 1 ATR and a credible nearby opposing pool; TP2 is the
    lesser of the requested 2% price expansion and a farther opposing pool.
    The caller must supply directionally filtered liquidity levels.
    """
    direction = str(side or "").upper()
    entry = _number(entry_price, 0.0) or 0.0
    volatility = _number(atr, 0.0) or 0.0
    if direction not in {"BUY", "SELL"} or entry <= 0 or volatility <= 0:
        return 0.0, 0.0, "invalid_side_entry_or_atr"
    def distance(level: float) -> float:
        return level-entry if direction == "BUY" else entry-level
    levels = []
    for raw in opposing_liquidity_levels or ():
        level = _number(raw)
        if level is not None and distance(level) > 0:
            levels.append((distance(level), level))
    levels.sort(key=lambda item: item[0])
    tp1_dist = max(0.0, target1_atr) * volatility
    if levels and levels[0][0] < tp1_dist:
        tp1_dist = levels[0][0] - min(pool_buffer_atr*volatility, levels[0][0]*0.20)
    if tp1_dist <= 0:
        return 0.0, 0.0, "tp1_blocked_by_opposing_liquidity"
    tp1 = entry + tp1_dist if direction == "BUY" else entry - tp1_dist
    target2_move = max(0.0, target2_price_move_pct)/100.0*entry
    target2_dist = max(target2_move, min_target2_atr*volatility)
    if levels:
        farther = [item for item in levels if item[0] >= min_target2_atr*volatility]
        if farther:
            pool_dist = farther[0][0] - min(pool_buffer_atr*volatility, farther[0][0]*0.20)
            target2_dist = min(target2_dist, pool_dist)
        else:
            target2_dist = 0.0
    if target2_dist <= tp1_dist:
        return round(tp1, 12), 0.0, "tp2_unavailable_before_opposing_liquidity"
    tp2 = entry + target2_dist if direction == "BUY" else entry - target2_dist
    return round(tp1, 12), round(tp2, 12), "tp1_tp2_capped_by_opposing_liquidity"


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


def tp2_reassessment(side: str, current_price: float, tp2_price: float, atr: float,
                     continuation_probability: float, adx: float = 0.0, adx_slope: float = 0.0,
                     structure_aligned: bool = False, max_atr_distance: float = 3.0,
                     min_continuation: float = 0.45, *, adverse_divergence: bool = False,
                     zone_intact: bool = True, opposing_liquidity_blocked: bool = False,
                     volume_ratio: Optional[float] = None) -> Tuple[str, str]:
    """Reassess TP2 runner from structure, original zone, flow and continuation.

    ADX inputs remain accepted for backwards compatibility but do not decide the
    runner exit. A low or high ADX value alone cannot close or preserve a trade.
    """
    direction = str(side or "").upper()
    current = _number(current_price, 0.0) or 0.0
    target = _number(tp2_price, 0.0) or 0.0
    volatility = _number(atr, 0.0) or 0.0
    continuation = _number(continuation_probability, 0.0) or 0.0
    volume = _number(volume_ratio)
    if direction not in {"BUY", "SELL"} or current <= 0 or target <= 0 or volatility <= 0:
        return "HOLD", "insufficient_tp2_evidence"
    distance = target-current if direction == "BUY" else current-target
    if distance <= 0:
        return "HOLD", "target_reached"
    remote = distance/volatility > max_atr_distance
    weak_votes = sum((
        continuation < min_continuation,
        bool(adverse_divergence),
        not bool(zone_intact),
        bool(opposing_liquidity_blocked),
        volume is not None and volume < 0.70,
    ))
    weakening = weak_votes >= 2
    if remote and weakening:
        return "CLOSE", "tp2_remote_and_multi_evidence_thesis_weakening"
    if remote and not structure_aligned:
        return "CLOSE", "tp2_remote_and_structure_lost"
    return "HOLD", "runner_structure_zone_and_continuation_acceptable"


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
