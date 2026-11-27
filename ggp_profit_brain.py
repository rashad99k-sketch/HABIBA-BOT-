"""Single, deterministic profit-mode policy for the GGP trade manager.

This module makes no exchange calls and never sends orders. It only classifies
trade-management mode and computes price targets; execution and fill verification
remain in the existing GGP execution layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any, Dict, Iterable, Optional, Tuple


RANGE_PROFIT_TARGETS_PCT = (0.5, 1.0, 2.0)
RANGE_MODE = "RANGE_SCALP"
TREND_MODE = "TREND_RIDE"
STANDARD_MODE = "TWO_STAGE_50_50"


def _number(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        result = float(value)
        return result if isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default



def adaptive_trail_multiplier(base_multiplier: float, continuation_probability: float, adx: float,
                              distribution_risk: float, exit_warning: int, ema_state: str = "UNKNOWN",
                              peak_drawdown_roe: float = 0.0, tp1_verified: bool = True) -> float:
    """RORO-derived adaptive runner defense; only returns a multiplier, never sends orders.

    A confirmed expanding trend gets room. Distribution, warnings and giveback
    progressively tighten the stop. It never widens an already-committed stop;
    the caller applies the existing monotonic ratchet.
    """
    mult = _number(base_multiplier, 1.5) or 1.5
    cont = _number(continuation_probability, 0.0) or 0.0
    adx_value = _number(adx, 0.0) or 0.0
    dist = _number(distribution_risk, 0.0) or 0.0
    warning = int(_number(exit_warning, 0.0) or 0)
    giveback = max(0.0, _number(peak_drawdown_roe, 0.0) or 0.0)
    if cont >= 0.78 and adx_value >= 25 and dist < 35 and warning <= 1:
        mult *= 1.20
    elif cont < 0.55 or dist >= 45 or warning >= 3:
        mult *= 0.75
    if dist >= 65 or warning >= 4:
        mult *= 0.75
    if ema_state in {"TREND_RIDE", "EMA20_PULLBACK", "EMA50_PULLBACK"} and cont >= 0.60 and dist < 45 and warning <= 2:
        mult *= 1.08
    elif ema_state == "EMA20_RETEST_PENDING" and cont >= 0.50 and dist < 45 and warning <= 2:
        mult *= 1.04
    elif ema_state in {"EMA50_BREAK_PENDING", "EMA50_BREAK_CONFIRMED"}:
        mult *= 0.82 if ema_state == "EMA50_BREAK_PENDING" else 0.65
    # Once meaningful MFE exists, reduce the amount of profit given back.
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


# ---------------------------------------------------------------------------
# Adaptive SCALP/TREND trade-state policy (pure decision support only)
#
# These helpers never call the exchange and never create orders. The existing
# TradeManagementBoard remains the only authority that emits trade decisions;
# TradeExecutionService/CloseVerificationPipeline remain the only execution path.
# ---------------------------------------------------------------------------

SCALP_STATE = "SCALP"
TREND_STATE = "TREND"
STANDARD_STATE = "STANDARD"
EXIT_REQUIRED_STATE = "EXIT_REQUIRED"
CLOSED_STATE = "CLOSED"

SCALP_ROE_TARGETS_PCT = (5.0, 10.0, 20.0)
TREND_ROE_TARGETS_PCT = (40.0, 60.0)

BEHAVIOR_RETEST = "RETEST"
BEHAVIOR_PULLBACK = "PULLBACK"
BEHAVIOR_MICRO_PULLBACK = "MICRO_PULLBACK"
BEHAVIOR_TREND_CONTINUATION = "TREND_CONTINUATION"
BEHAVIOR_DISTRIBUTION = "DISTRIBUTION_PHASE"
BEHAVIOR_INSTITUTIONAL_EXIT = "INSTITUTIONAL_EXIT"
BEHAVIOR_INSUFFICIENT = "INSUFFICIENT_DATA"


def verify_roe_inputs(context: Dict[str, Any]) -> Dict[str, Any]:
    """Verify that ROE targets may be used for a live position.

    ROE is derived from the exchange-reported position quantity, entry, margin
    and unrealized PnL. The configured 20x value is never used as a silent
    fallback: an unverified actual leverage blocks ROE-target decisions.
    """
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    entry = _number(ctx.get("entry"))
    mark = _number(ctx.get("mark_price") or ctx.get("current_price"))
    qty = _number(ctx.get("remaining_qty") or ctx.get("qty"))
    margin = _number(ctx.get("initial_margin") if ctx.get("initial_margin") is not None else ctx.get("margin"))
    unrealized = _number(ctx.get("unrealized_pnl") if ctx.get("unrealized_pnl") is not None else ctx.get("unrealized_pnl_usdt"))
    leverage = _number(ctx.get("actual_leverage") if ctx.get("actual_leverage") is not None else ctx.get("leverage"))
    leverage_verified = bool(ctx.get("leverage_verified", False))
    reported = _number(ctx.get("reported_roe_pct") if ctx.get("reported_roe_pct") is not None else ctx.get("roe_pct"))
    fees = _number(ctx.get("fees_usdt"), 0.0) or 0.0
    funding = _number(ctx.get("funding_usdt"), 0.0) or 0.0

    result: Dict[str, Any] = {
        "ok": False, "reason": "", "side": side, "leverage": leverage, "margin": margin,
        "qty": qty, "entry": entry, "computed_roe_pct": None,
        "reported_roe_pct": reported, "reported_roe_agreement": None,
        "implied_contract_size": None, "net_unrealized_pnl": None,
    }
    if side not in {"BUY", "SELL"}:
        result["reason"] = "invalid_side"
        return result
    if not entry or entry <= 0 or not mark or mark <= 0 or not qty or qty <= 0:
        result["reason"] = "missing_entry_mark_or_quantity"
        return result
    if not leverage_verified or not leverage or leverage <= 0:
        result["reason"] = "actual_leverage_not_verified"
        return result
    if not margin or margin <= 0:
        result["reason"] = "initial_margin_unavailable"
        return result
    if unrealized is None:
        result["reason"] = "unrealized_pnl_unavailable"
        return result

    net = unrealized - fees - funding
    computed = net / margin * 100.0
    implied_cs = margin * leverage / (qty * entry)
    result.update({
        "computed_roe_pct": computed, "net_unrealized_pnl": net,
        "implied_contract_size": implied_cs if implied_cs > 0 else None,
    })
    if reported is not None:
        tolerance = max(0.25, abs(reported) * 0.01)
        agreed = abs(computed - reported) <= tolerance
        result["reported_roe_agreement"] = agreed
        if not agreed:
            result["reason"] = (
                f"reported_roe_mismatch computed={computed:.4f} reported={reported:.4f}"
            )
            return result

    if abs(leverage - 20.0) > 1e-9:
        # ROE is margin-normalized, so it stays compatible with the verified
        # actual leverage. The diagnostic is mandatory for observability.
        result["ok"] = True
        result["reason"] = (
            f"roe_normalized_to_actual_leverage_{leverage:g}_target_policy_is_20x"
        )
    else:
        result["ok"] = True
        result["reason"] = "verified_20x_margin_basis"
    return result


def roe_target_price(context: Dict[str, Any], target_roe_pct: float) -> float:
    """Translate an ROE target into a directional price level for logging only.

    Decisions compare ROE directly. This level is a diagnostic aid and uses the
    verified actual leverage plus exchange margin/quantity (which already
    reflect the venue contract size).
    """
    ctx = context if isinstance(context, dict) else {}
    side = str(ctx.get("side") or "").upper()
    entry = _number(ctx.get("entry"), 0.0) or 0.0
    margin = _number(ctx.get("initial_margin") if ctx.get("initial_margin") is not None else ctx.get("margin"), 0.0) or 0.0
    qty = _number(ctx.get("remaining_qty") or ctx.get("qty"), 0.0) or 0.0
    leverage = _number(ctx.get("actual_leverage") if ctx.get("actual_leverage") is not None else ctx.get("leverage"), 0.0) or 0.0
    target = _number(target_roe_pct, 0.0) or 0.0
    if side not in {"BUY", "SELL"} or entry <= 0 or margin <= 0 or qty <= 0 or leverage <= 0 or target <= 0:
        return 0.0
    contract_size = _number(ctx.get("contract_size"))
    if not contract_size or contract_size <= 0:
        contract_size = margin * leverage / (qty * entry)
    if not contract_size or contract_size <= 0:
        return 0.0
    price_step = (target / 100.0) * margin / (qty * contract_size)
    if price_step <= 0:
        return 0.0
    level = entry + price_step if side == "BUY" else entry - price_step
    return round(level, 12)


def select_scalp_roe_target(context: Dict[str, Any]) -> Tuple[float, str]:
    """Pick exactly one 5/10/20% ROE exit from measurable post-entry evidence.

    Opposing-liquidity evidence from ``adaptive_range_target_price()`` can only
    narrow the approved ROE target set; it never introduces a new target and
    never widens one. A verified leverage basis is required for that conversion,
    so an unverified leverage never silently defaults to 20x here either.
    """
    ctx = context if isinstance(context, dict) else {}
    adx = _number(ctx.get("adx"), 0.0) or 0.0
    adx_slope = _number(ctx.get("adx_slope"), 0.0) or 0.0
    continuation = _number(ctx.get("continuation_probability"), 0.0) or 0.0
    volume_ratio = _number(ctx.get("volume_ratio"), 0.0) or 0.0
    distance = _number(ctx.get("distance_to_opposing_pct"), 0.0) or 0.0
    aligned = _number(ctx.get("aligned_closed_candles"), 0.0) or 0.0
    zone_strength = _number(ctx.get("zone_strength"), 0.0) or 0.0
    di_aligned = bool(ctx.get("di_aligned", False))
    adverse_rejection = bool(ctx.get("adverse_rejection", False))
    distribution_risk = _number(ctx.get("distribution_risk"), 0.0) or 0.0
    behavior = str(ctx.get("post_entry_behavior") or "").upper()
    weak = (
        adverse_rejection or distribution_risk >= 50 or continuation < 0.45 or adx < 18
        or volume_ratio < 0.75 or distance < 0.25
        or behavior in {BEHAVIOR_DISTRIBUTION, BEHAVIOR_PULLBACK, BEHAVIOR_MICRO_PULLBACK}
    )
    strong = (
        adx >= 24 and adx_slope >= 0 and continuation >= 0.72 and volume_ratio >= 1.20
        and aligned >= 2 and di_aligned and not adverse_rejection and distribution_risk < 35
        and distance >= 0.80 and zone_strength >= 0.60
        and behavior in {BEHAVIOR_TREND_CONTINUATION, BEHAVIOR_RETEST}
    )
    moderate = (
        adx >= 20 and continuation >= 0.58 and volume_ratio >= 0.90
        and aligned >= 1 and distance >= 0.35 and not adverse_rejection
    )
    if weak:
        target, reason = SCALP_ROE_TARGETS_PCT[0], "weak_momentum_near_liquidity_or_rejection_conservative_5pct_roe"
    elif strong:
        target, reason = SCALP_ROE_TARGETS_PCT[2], "strong_zone_closed_candle_and_volume_follow_through_20pct_roe"
    elif moderate:
        target, reason = SCALP_ROE_TARGETS_PCT[1], "moderate_follow_through_10pct_roe"
    else:
        target, reason = SCALP_ROE_TARGETS_PCT[0], "insufficient_continuation_evidence_conservative_5pct_roe"

    # Convert the selected ROE to its approximate price move using the VERIFIED
    # leverage only. If a credible opposing pool sits inside that move, keep the
    # largest approved target whose implied move fits before the pool.
    leverage = _number(ctx.get("actual_leverage"), 0.0) or 0.0
    if bool(ctx.get("opposing_liquidity_strong", False)) and leverage > 0 and distance > 0:
        implied_move = target / leverage
        if distance < implied_move:
            capped = min((pct for pct in SCALP_ROE_TARGETS_PCT if pct / leverage <= distance),
                         default=SCALP_ROE_TARGETS_PCT[0])
            if capped < target:
                target = capped
            # Record the liquidity evidence even when 5% is already the only
            # approved target that can fit before the opposing pool.
            reason += (f"|opposing_liquidity_at_{distance:.2f}pct_"
                       f"capped_to_{capped:.0f}pct_roe")
    return target, reason


def analyze_post_entry_behavior(df_closed: Any, side: str, entry_price: float, atr: float,
                                context: Optional[Dict[str, Any]] = None) -> Tuple[str, str, Dict[str, Any]]:
    """Classify price behaviour from completed candles only.

    Every returned label is tied to observable candle/zone evidence. A counter
    candle alone is never an exit; structural invalidation is required for
    INSTITUTIONAL_EXIT.
    """
    ctx = context if isinstance(context, dict) else {}
    direction = str(side or "").upper()
    entry = _number(entry_price, 0.0) or 0.0
    volatility = _number(atr, 0.0) or 0.0
    evidence: Dict[str, Any] = {
        "source": "completed_candles_only", "aligned_closes": 0,
        "counter_candles": 0, "adverse_atr": 0.0, "zone_touch": False,
        "retest_success": False,
    }
    if direction not in {"BUY", "SELL"} or entry <= 0 or volatility <= 0 or df_closed is None or len(df_closed) < 3:
        return BEHAVIOR_INSUFFICIENT, "insufficient_completed_candle_evidence", evidence
    try:
        import pandas as pd
        recent = df_closed.tail(6)
        highs = pd.to_numeric(recent["high"], errors="coerce").dropna().astype(float)
        lows = pd.to_numeric(recent["low"], errors="coerce").dropna().astype(float)
        closes = pd.to_numeric(recent["close"], errors="coerce").dropna().astype(float)
        opens = pd.to_numeric(recent["open"], errors="coerce").dropna().astype(float)
        if min(len(highs), len(lows), len(closes), len(opens)) < 3:
            return BEHAVIOR_INSUFFICIENT, "insufficient_ohlcv_columns", evidence
    except Exception:
        return BEHAVIOR_INSUFFICIENT, "ohlcv_parse_error", evidence

    last_close = float(closes.iloc[-1])
    aligned = 0
    counter = 0
    for i in range(max(1, len(closes) - 3), len(closes)):
        body = abs(float(closes.iloc[i]) - float(opens.iloc[i]))
        if direction == "BUY":
            if float(closes.iloc[i]) > float(closes.iloc[i - 1]):
                aligned += 1
            if float(closes.iloc[i]) < float(opens.iloc[i]) and body >= 0.25 * volatility:
                counter += 1
        else:
            if float(closes.iloc[i]) < float(closes.iloc[i - 1]):
                aligned += 1
            if float(closes.iloc[i]) > float(opens.iloc[i]) and body >= 0.25 * volatility:
                counter += 1
    adverse = (entry - float(lows.min())) if direction == "BUY" else (float(highs.max()) - entry)
    adverse_atr = max(0.0, adverse) / volatility
    zone_low = _number(ctx.get("zone_low"))
    zone_high = _number(ctx.get("zone_high"))
    if zone_low is None or zone_high is None or zone_low > zone_high:
        zone_low, zone_high = entry - 0.25 * volatility, entry + 0.25 * volatility
    zone_touch = any(float(low) <= zone_high and float(high) >= zone_low
                     for low, high in zip(lows.tolist(), highs.tolist()))
    # A retest must be recent; merely touching the entry zone on the first bar
    # of a strong impulse is continuation, not a retest.
    zone_touch_recent = any(float(low) <= zone_high and float(high) >= zone_low
                            for low, high in zip(lows.tolist()[-3:], highs.tolist()[-3:]))
    reclaimed = last_close > zone_high if direction == "BUY" else last_close < zone_low
    continuation = _number(ctx.get("continuation_probability"), 0.5) or 0.5
    adx = _number(ctx.get("adx"), 0.0) or 0.0
    distribution = _number(ctx.get("distribution_risk"), 0.0) or 0.0
    volume_ratio = _number(ctx.get("volume_ratio"), 0.0) or 0.0
    thesis_score = _number(ctx.get("thesis_failure_score"), 0.0) or 0.0
    di_aligned = bool(ctx.get("di_aligned", False))
    structure_aligned = bool(ctx.get("structure_aligned", False))
    structure_broken = bool(ctx.get("structure_broken", False))
    structural_invalidation = bool(ctx.get("structural_invalidation", False))
    evidence.update({
        "aligned_closes": aligned, "counter_candles": counter,
        "adverse_atr": adverse_atr, "zone_touch": zone_touch_recent,
        "retest_success": bool(zone_touch_recent and reclaimed),
        "last_close": last_close, "zone_low": zone_low, "zone_high": zone_high,
    })
    if structural_invalidation or thesis_score >= 70.0 or (structure_broken and distribution >= 60.0):
        return BEHAVIOR_INSTITUTIONAL_EXIT, "thesis_or_structure_invalidated", evidence
    if distribution >= 55.0 or (counter >= 2 and volume_ratio >= 1.20 and continuation < 0.50):
        return BEHAVIOR_DISTRIBUTION, "distribution_or_adverse_volume_expansion", evidence
    near_zone = ((last_close <= zone_high + 0.75 * volatility) if direction == "BUY"
                 else (last_close >= zone_low - 0.75 * volatility))
    if zone_touch_recent and reclaimed and near_zone and adverse_atr <= 0.75 and continuation >= 0.50 and not structure_broken:
        return BEHAVIOR_RETEST, "retest_held_inside_entry_zone", evidence
    if zone_touch_recent and not reclaimed and not structure_broken and adverse_atr <= 1.50:
        return BEHAVIOR_PULLBACK, "entry_zone_retested_without_reclaim", evidence
    if aligned >= 2 and adx >= 22.0 and di_aligned and structure_aligned and continuation >= 0.60 and distribution < 45.0:
        return BEHAVIOR_TREND_CONTINUATION, "adx_di_structure_and_closed_candles_aligned", evidence
    if adverse_atr <= 0.35 and counter <= 1 and continuation >= 0.55:
        return BEHAVIOR_MICRO_PULLBACK, "shallow_short_counter_move_with_continuation", evidence
    if adverse_atr <= 1.25 and structure_aligned and continuation >= 0.50:
        return BEHAVIOR_PULLBACK, "temporary_correction_inside_intact_structure", evidence
    return "RANGE_CHOP", "no_dominant_post_entry_behavior", evidence


def map_profit_mode_to_regime(profit_mode: str, behavior: str = "") -> str:
    """Map observed evidence onto the explicit state machine.

    STANDARD is a first-class regime. Missing or conflicting evidence must never
    be reinterpreted as SCALP: only RANGE_MODE (proven weakness/thin liquidity)
    maps to SCALP, and only TREND_MODE (proven continuation) maps to TREND.
    """
    mode = str(profit_mode or "").upper()
    if str(behavior or "").upper() == BEHAVIOR_INSTITUTIONAL_EXIT:
        return EXIT_REQUIRED_STATE
    if mode == TREND_MODE:
        return TREND_STATE
    if mode == RANGE_MODE:
        return SCALP_STATE
    return STANDARD_STATE


def transition_trade_regime(current: str, candidate: str, behavior: str,
                            confirmations: int, evidence: Optional[Dict[str, Any]] = None) -> Tuple[str, str, int]:
    """Apply two-candle hysteresis across SCALP/TREND/STANDARD.

    Structural invalidation or an institutional-exit behaviour overrides the
    hysteresis immediately. Profit alone is not evidence: a TREND upgrade still
    needs closed-candle continuation, and a SCALP downgrade needs confirmed
    weakness. Insufficient evidence stays in / returns to STANDARD (50/50).
    """
    current_state = str(current or "").upper()
    candidate_state = str(candidate or "").upper()
    active_behavior = str(behavior or "").upper()
    evidence = evidence if isinstance(evidence, dict) else {}
    if current_state == CLOSED_STATE:
        return CLOSED_STATE, "already_closed", 0
    if active_behavior == BEHAVIOR_INSTITUTIONAL_EXIT or bool(evidence.get("structural_invalidation", False)):
        return EXIT_REQUIRED_STATE, "institutional_exit_confirmed", 0
    if current_state == EXIT_REQUIRED_STATE:
        return EXIT_REQUIRED_STATE, "exit_already_required", 0

    valid_states = {SCALP_STATE, TREND_STATE, STANDARD_STATE}
    if candidate_state not in valid_states:
        candidate_state = STANDARD_STATE
    count = max(0, int(confirmations or 0))
    if not current_state or current_state not in valid_states:
        return candidate_state, f"initial_classification_{candidate_state.lower()}", count
    if candidate_state == current_state:
        return current_state, "candidate_matches_current_regime", 0
    if count < 2:
        return current_state, f"waiting_second_confirmed_candle_for_{candidate_state.lower()}", count

    if candidate_state == TREND_STATE:
        if (active_behavior in {BEHAVIOR_TREND_CONTINUATION, BEHAVIOR_RETEST}
                and not bool(evidence.get("adverse_rejection", False))
                and not bool(evidence.get("structural_invalidation", False))):
            return TREND_STATE, ("scalp_to_trend_upgrade_confirmed"
                                 if current_state == SCALP_STATE
                                 else "standard_to_trend_upgrade_confirmed"), 0
        return current_state, "trend_upgrade_rejected_no_fresh_continuation_evidence", count

    if candidate_state == SCALP_STATE:
        # Insufficient evidence is never a reason to shorten into SCALP. The
        # candidate must carry observed weakness on closed candles, not merely
        # absent data.
        weakness_confirmed = (
            active_behavior in {BEHAVIOR_DISTRIBUTION, BEHAVIOR_PULLBACK,
                                BEHAVIOR_MICRO_PULLBACK, "RANGE_CHOP"}
            or bool(evidence.get("weak_market_confirmed", False))
        )
        if weakness_confirmed:
            reason = ("trend_downgraded_after_confirmed_structure_weakening"
                      if current_state == TREND_STATE
                      else "standard_to_scalp_confirmed_by_weakness_evidence")
            return SCALP_STATE, reason, 0
        return current_state, "scalp_downgrade_rejected_no_weakness_evidence", count

    # STANDARD candidate: two consecutive candles failed to prove either plan.
    return STANDARD_STATE, "standard_plan_retained_on_insufficient_evidence", 0


def regime_plan_consistency(regime_state: str, profit_mode: str, *,
                            tp1_hit: bool = False, tp2_hit: bool = False,
                            remaining_qty: float = 0.0, initial_qty: float = 0.0,
                            scalp_target_roe_pct: float = 0.0,
                            tp1_target_roe_pct: float = 0.0,
                            tp2_target_roe_pct: float = 0.0) -> Tuple[bool, str]:
    """Prove the explicit regime and the legacy diagnostic plan do not conflict.

    ``profit_mode`` is retained only for compatibility and diagnostics. Board
    decisions route on ``trade_regime_state``; this helper exposes any drift
    between the two so target management can fail closed instead of guessing.
    """
    regime = str(regime_state or "").upper()
    mode = str(profit_mode or "").upper()
    if regime not in {SCALP_STATE, TREND_STATE, STANDARD_STATE, EXIT_REQUIRED_STATE, CLOSED_STATE}:
        return False, "unknown_regime_state"
    if regime == CLOSED_STATE:
        return True, "closed_terminal_state"
    initial = _number(initial_qty, 0.0) or 0.0
    remaining = _number(remaining_qty, 0.0) or 0.0
    half = initial * 0.5
    tolerance = max(initial * 0.002, 1e-10)
    scalp_target = _number(scalp_target_roe_pct, 0.0) or 0.0
    tp1_target = _number(tp1_target_roe_pct, 0.0) or 0.0
    tp2_target = _number(tp2_target_roe_pct, 0.0) or 0.0

    if regime == SCALP_STATE:
        if mode != RANGE_MODE:
            return False, "scalp_profit_mode_conflict"
        if scalp_target not in SCALP_ROE_TARGETS_PCT:
            return False, "scalp_target_outside_approved_set"
        if tp1_hit or tp2_hit:
            return False, "scalp_must_not_have_partial_close"
        if initial > 0 and abs(remaining - initial) > tolerance:
            return False, "scalp_quantity_was_split"
        return True, "scalp_plan_consistent"
    if regime == TREND_STATE:
        if mode != TREND_MODE:
            return False, "trend_profit_mode_conflict"
        if abs(tp1_target - TREND_ROE_TARGETS_PCT[0]) > 1e-9 or abs(tp2_target - TREND_ROE_TARGETS_PCT[1]) > 1e-9:
            return False, "trend_roe_targets_missing"
        if tp2_hit and remaining > tolerance:
            return False, "trend_tp2_hit_with_remaining_quantity"
        if tp1_hit and initial > 0 and remaining > half + tolerance:
            return False, "trend_tp1_quantity_above_half"
        return True, "trend_plan_consistent"
    if regime == STANDARD_STATE:
        if mode != STANDARD_MODE:
            return False, "standard_profit_mode_conflict"
        if scalp_target > 0 or tp1_target > 0 or tp2_target > 0:
            return False, "standard_must_not_use_adaptive_roe_targets"
        if tp1_hit and initial > 0 and remaining > half + tolerance:
            return False, "standard_tp1_quantity_above_half"
        return True, "standard_plan_consistent"
    # EXIT_REQUIRED: no profit-target plan may survive an invalidated thesis.
    if scalp_target > 0 or tp1_target > 0 or tp2_target > 0:
        return False, "exit_required_must_not_retain_profit_targets"
    return True, "exit_required_plan_consistent"
