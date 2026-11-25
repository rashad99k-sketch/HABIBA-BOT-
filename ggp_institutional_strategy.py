"""Additive institutional decision reporting for the existing GGP engines.

This module is deliberately execution-free. It converts observations already
produced by the Scanner, Watchlist, Unified Entry Authority, profit brain and
risk engines into one auditable report. It never grants entry permission, never
places/cancels orders, and never performs an exchange or market-data call.
"""
from __future__ import annotations

from math import isfinite
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ggp_market_intelligence import normalize_entry_type
from ggp_profit_brain import (
    ESTIMATED_SLIPPAGE_PCT,
    ESTIMATED_TAKER_FEE_PCT,
    POLICY_LEVERAGE,
    RANGE_MODE,
    STANDARD_MODE,
    TREND_MODE,
    classify_profit_mode,
    exchange_pnl_at_move,
    profit_target_convention,
)

ENTRY_CLASSIFICATIONS = (
    "IMMEDIATE_ENTRY",
    "MICRO_PULLBACK",
    "PULLBACK",
    "RETEST",
)

# Additive dashboard labels only. The canonical profit modes remain the single
# management authority; these names never select an independent exit path.
MANAGEMENT_MODE_ALIASES = {
    "SCALP": RANGE_MODE,
    "STANDARD": STANDARD_MODE,
    "TREND_EXPANSION": TREND_MODE,
}
CANONICAL_MODE_ALIASES = {canonical: alias for alias, canonical in MANAGEMENT_MODE_ALIASES.items()}

# Conservative, explicit reporting thresholds. They do not authorize entries;
# they only explain whether an already-authorized setup has price room.
STRONG_OPPOSING_ZONE_MIN_STRENGTH = 5.0
MIN_AVAILABLE_ROOM_PRICE_PCT = 0.25
MIN_AVAILABLE_ROOM_ATR = 0.5
DEFAULT_MAX_ANALYSIS_AGE_SEC = 900.0


def _number(value: Any, default: Optional[float] = None) -> Optional[float]:
    if isinstance(value, bool):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if isfinite(number) else default


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _mapping(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _items(value: Any) -> List[str]:
    if value is None:
        return []
    values = value if isinstance(value, (list, tuple, set)) else [value]
    result: List[str] = []
    for item in values:
        if isinstance(item, (list, tuple, set)):
            result.extend(_items(item))
        elif item is not None and _text(item):
            result.append(_text(item))
    return result


def _confidence(value: Any) -> Optional[float]:
    number = _number(value)
    if number is None:
        return None
    if number < 0.0 or number > 100.0:
        return None
    return round(number, 4)


def normalize_management_mode(profit_mode: Any) -> Dict[str, Any]:
    """Map one canonical profit-brain mode to its additive dashboard label."""
    canonical = _text(profit_mode).upper()
    if canonical in CANONICAL_MODE_ALIASES:
        alias = CANONICAL_MODE_ALIASES[canonical]
        return {
            "management_mode": alias,
            "canonical_profit_mode": canonical,
            "mapping_status": "MAPPED_FROM_UNIFIED_PROFIT_BRAIN",
            "authority": "TradeManagementBoard",
            "reason": "",
        }
    alias = canonical if canonical in MANAGEMENT_MODE_ALIASES else "STANDARD"
    if alias in MANAGEMENT_MODE_ALIASES:
        canonical = MANAGEMENT_MODE_ALIASES[alias]
        status = "MAPPED_FROM_DASHBOARD_ALIAS"
    else:
        canonical = STANDARD_MODE
        status = "UNKNOWN_MODE_DEFAULTED_TO_STANDARD"
    return {
        "management_mode": alias,
        "canonical_profit_mode": canonical,
        "mapping_status": status,
        "authority": "TradeManagementBoard",
        "reason": "unknown_management_mode_defaults_to_standard" if status.endswith("STANDARD") else "",
    }


def _move_pct_from_price(side: str, entry: Optional[float], target: Optional[float]) -> Optional[float]:
    direction = str(side or "").strip().upper()
    entry_v, target_v = _number(entry), _number(target)
    if direction not in {"BUY", "SELL"} or not entry_v or not target_v or entry_v <= 0 or target_v <= 0:
        return None
    signed = (target_v - entry_v) if direction == "BUY" else (entry_v - target_v)
    move = signed / entry_v * 100.0
    return round(move, 8)


def build_management_mode_report(
    context: Any,
    *,
    profit_mode: Any = None,
    reason: Any = None,
    evidence: Iterable[Any] = (),
) -> Dict[str, Any]:
    """Report the current management mode using the existing profit brain only."""
    ctx = _mapping(context)
    if profit_mode is None:
        profit_mode, classified_reason, classified_evidence = classify_profit_mode(ctx)
        reason = reason or classified_reason
        evidence = evidence or classified_evidence
    mapped = normalize_management_mode(profit_mode)
    convention = profit_target_convention()
    side = str(ctx.get("side") or "").upper()
    entry = _number(ctx.get("entry") or ctx.get("entry_price"))
    tp1 = _number(ctx.get("tp1_price") or ctx.get("synthetic_tp1"))
    tp2 = _number(ctx.get("tp2_price") or ctx.get("synthetic_tp2"))
    range_target = _number(ctx.get("range_target_price") or ctx.get("scalp_target_price"))
    selected_price = range_target if mapped["management_mode"] == "SCALP" else tp1
    selected_move = _move_pct_from_price(side, entry, selected_price)
    if selected_move is None:
        selected_move = _number(ctx.get("range_target_move_pct") if mapped["management_mode"] == "SCALP" else None)
    return {
        **mapped,
        "classification_reason": _text(reason),
        "classification_evidence": _items(evidence),
        "target_convention": convention,
        "selected_target_price": selected_price,
        "selected_target_price_move_pct": selected_move,
        "selected_target_gross_roe_pct": (selected_move * POLICY_LEVERAGE) if selected_move is not None else None,
        "tp1_price_move_pct": _move_pct_from_price(side, entry, tp1),
        "tp2_price_move_pct": _move_pct_from_price(side, entry, tp2),
        "mode_change_is_reporting_only": True,
    }


def _opposing_zone_room(record: Dict[str, Any], side: str, price: float,
                        atr: Optional[float]) -> Dict[str, Any]:
    sr = _mapping(record.get("support_resistance"))
    # BUY looks up into supply/resistance; SELL looks down into demand/support.
    key = "resistances" if side == "BUY" else "supports"
    zones = sr.get(key) if isinstance(sr.get(key), list) else []
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for raw in zones:
        zone = _mapping(raw)
        if _text(zone.get("status")).upper() != "CONFIRMED":
            continue
        if (_number(zone.get("strength"), 0.0) or 0.0) < STRONG_OPPOSING_ZONE_MIN_STRENGTH:
            continue
        low, high = _number(zone.get("low")), _number(zone.get("high"))
        level = _number(zone.get("level"))
        if low is not None and high is not None and 0 < low <= high:
            zone_low, zone_high = low, high
        elif level is not None and level > 0:
            zone_low = zone_high = level
        else:
            continue
        # Only zones still ahead of (or touching) price constrain this side.
        # A former supply zone already traded through is not an opposing zone
        # for a BUY; the same applies to demand below a SELL.
        if side == "BUY":
            if zone_high < price:
                continue
            boundary = zone_low
            room = boundary - price
        else:
            if zone_low > price:
                continue
            boundary = zone_high
            room = price - boundary
        candidates.append((room, {"boundary": boundary, "zone": dict(zone)}))
    if not candidates:
        return {
            "status": "CLEAR",
            "opposing_zone_type": "SUPPLY" if side == "BUY" else "DEMAND",
            "boundary": None,
            "room_price": None,
            "room_price_pct": None,
            "room_atr": None,
            "required_min_price_pct": MIN_AVAILABLE_ROOM_PRICE_PCT,
            "required_min_atr": MIN_AVAILABLE_ROOM_ATR,
            "reasons": ["no_confirmed_strong_opposing_zone"],
        }
    room, best = min(candidates, key=lambda item: item[0])
    boundary = float(best["boundary"])
    room_pct = room / price * 100.0 if price > 0 else None
    room_atr = room / atr if atr is not None and atr > 0 else None
    reasons: List[str] = []
    insufficient = False
    if room <= 0:
        reasons.append("execution_price_already_inside_strong_opposing_zone")
    if room_pct is not None and room_pct < MIN_AVAILABLE_ROOM_PRICE_PCT:
        insufficient = True
        reasons.append("available_price_room_below_minimum_price_move")
    if room_atr is not None and room_atr < MIN_AVAILABLE_ROOM_ATR:
        insufficient = True
        reasons.append("available_price_room_below_minimum_atr_distance")
    if room > 0 and not insufficient:
        reasons.append("confirmed_strong_opposing_zone_has_sufficient_room")
    status = "INSIDE_OPPOSING_ZONE" if room <= 0 else "INSUFFICIENT" if insufficient else "SUFFICIENT"
    return {
        "status": status,
        "opposing_zone_type": "SUPPLY" if side == "BUY" else "DEMAND",
        "boundary": boundary,
        "room_price": room,
        "room_price_pct": room_pct,
        "room_atr": room_atr,
        "required_min_price_pct": MIN_AVAILABLE_ROOM_PRICE_PCT,
        "required_min_atr": MIN_AVAILABLE_ROOM_ATR,
        "reasons": reasons,
    }


def build_institutional_report(record: Any, *, side: Any = None,
                               now_ts: Optional[float] = None) -> Dict[str, Any]:
    """Build one evidence report from fields already present in a Watchlist row.

    Every section is descriptive. Any missing or invalid evidence is reported as
    UNKNOWN/INSUFFICIENT_DATA rather than inferred or scored optimistically.
    """
    row = _mapping(record)
    direction = str(side or row.get("side") or row.get("signal_direction") or "").strip().upper()
    if direction not in {"BUY", "SELL"}:
        direction = ""
    now = _number(now_ts, 0.0) or 0.0
    price = _number(row.get("execution_price") or row.get("price") or row.get("current_price"))
    indicator = _mapping(row.get("indicator_snapshot"))
    market_intel = _mapping(row.get("market_intelligence"))
    golden = _mapping(row.get("golden_zone"))
    structure = _mapping(row.get("market_structure"))
    liquidity = _mapping(row.get("liquidity"))
    cross_context = _mapping(row.get("cross_context"))
    behavior_context = _mapping(row.get("entry_behavior_context"))
    atr = _number(row.get("atr"))
    if atr is None or atr <= 0:
        atr_pct = _number(row.get("market_intelligence", {}).get("volatility_atr") if isinstance(row.get("market_intelligence"), dict) else None)
        if atr_pct is not None and price:
            atr = price * abs(atr_pct) / 100.0

    cross_state = _text(cross_context.get("cross_state") or row.get("cross_state") or "NO_CROSS").upper()
    durable = bool(cross_context.get("durable_event") or row.get("cross_event_id") or row.get("event_id"))
    cross_relation = "UNKNOWN"
    ema9, vwap = _number(row.get("ema9")), _number(row.get("vwap"))
    if ema9 is not None and vwap is not None and vwap > 0:
        cross_relation = "ABOVE_VWAP" if ema9 > vwap else "BELOW_VWAP" if ema9 < vwap else "AT_VWAP"
    cross_location_state = (
        "CONFIRMED_CLOSED_EVENT" if durable and "CONFIRMED" in cross_state
        else "ACTIVE_CLOSED_EVENT" if durable
        else "RANKING_ONLY" if "CROSS" in cross_state and cross_state not in {"NO_CROSS", "INVALID_DATA"}
        else "NO_CROSS"
    )
    cross_location = {
        "state": cross_location_state,
        "cross_state": cross_state or "UNKNOWN",
        "phase": _text(row.get("cross_event_phase") or _mapping(row.get("metadata")).get("phase")).upper() or "UNKNOWN",
        "durable_event": durable,
        "ema9": ema9,
        "vwap": vwap,
        "relation": cross_relation,
        "signal_candle_timestamp": _number(row.get("cross_event_timestamp") or row.get("signal_candle_timestamp")),
    }

    regime = _text(row.get("market_regime") or market_intel.get("regime")).upper() or "UNKNOWN"
    institutional_zone = {
        "state": _text(golden.get("state") or row.get("golden_zone_state")).upper() or "UNKNOWN",
        "zone_type": _text(row.get("zone_type") or ("DEMAND" if direction == "BUY" else "SUPPLY" if direction == "SELL" else "UNKNOWN")).upper(),
        "quality": _number(row.get("zone_quality") if row.get("zone_quality") is not None else golden.get("score")),
        "low": _number(row.get("entry_zone_low") or market_intel.get("reference_low")),
        "high": _number(row.get("entry_zone_high") or market_intel.get("reference_high")),
        "status": _text(row.get("order_block_status")).upper() or "UNKNOWN",
    }

    trend_engine = _mapping(row.get("institutional_trend"))
    mss = _text(structure.get("mss") or structure.get("choch")).upper()
    bos = _text(structure.get("bos")).upper()
    expected_structure = f"{'BULLISH' if direction == 'BUY' else 'BEARISH'}_CHOCH"
    structure_bias = {
        "state": _text(structure.get("direction") or row.get("trend_direction")).upper() or "UNKNOWN",
        "mss": mss or "NONE",
        "bos": bos or "NONE",
        "confirmed": bool(structure.get("confirmed", False)),
        "side_aligned": bool(mss == expected_structure),
        "trade_state": _text(row.get("trade_state")).upper() or "UNKNOWN",
        "trend_engine_state": _text(trend_engine.get("state")).upper() or "UNKNOWN",
    }

    liq_confidence = _number(liquidity.get("confidence"))
    expected_liq = "BULLISH" if direction == "BUY" else "BEARISH" if direction == "SELL" else ""
    liquidity_context = {
        "event": _text(liquidity.get("event") or row.get("liquidity_event")).upper() or "UNKNOWN",
        "direction": _text(liquidity.get("direction")).upper() or "UNKNOWN",
        "confidence": liq_confidence,
        "side_aligned": bool(expected_liq and _text(liquidity.get("direction")).upper() == expected_liq),
    }

    volume_ratio = _number(row.get("volume_ratio") if row.get("volume_ratio") is not None else market_intel.get("volume_ratio"))
    volume_confirmation = {
        "ratio": volume_ratio,
        "state": "CONFIRMED" if bool(row.get("volume_confirmed", False)) or (volume_ratio is not None and volume_ratio >= 1.1)
                 else "WEAK" if volume_ratio is not None else "UNKNOWN",
        "confirmed": bool(row.get("volume_confirmed", False)) or (volume_ratio is not None and volume_ratio >= 1.1),
    }

    adx = _number(row.get("adx") if row.get("adx") is not None else market_intel.get("adx"))
    adx_di = _mapping(row.get("adx_di"))
    di_plus = _number(adx_di.get("di_plus"))
    di_minus = _number(adx_di.get("di_minus"))
    di_spread = abs(di_plus - di_minus) if di_plus is not None and di_minus is not None else None
    continuation = _number(row.get("continuation_probability"))
    if adx is None:
        trend_state = "UNKNOWN"
    elif adx >= 25.0 and (continuation is None or continuation >= 0.62):
        trend_state = "STRONG"
    elif adx < 18.0 or (continuation is not None and continuation < 0.48):
        trend_state = "WEAK"
    else:
        trend_state = "MODERATE"
    trend_strength = {
        "state": trend_state,
        "adx": adx,
        "adx_slope": _number(adx_di.get("adx_slope") if adx_di.get("adx_slope") is not None else row.get("adx_slope")),
        "di_plus": di_plus,
        "di_minus": di_minus,
        "di_spread": di_spread,
        "continuation_probability": continuation,
    }

    room = _opposing_zone_room(row, direction, price or 0.0, atr) if direction and price and price > 0 else {
        "status": "INSUFFICIENT_DATA",
        "opposing_zone_type": "SUPPLY" if direction == "BUY" else "DEMAND" if direction == "SELL" else "UNKNOWN",
        "boundary": None, "room_price": None, "room_price_pct": None, "room_atr": None,
        "required_min_price_pct": MIN_AVAILABLE_ROOM_PRICE_PCT,
        "required_min_atr": MIN_AVAILABLE_ROOM_ATR,
        "reasons": ["price_or_side_unavailable"],
    }
    invalidation_level = _number(row.get("invalidation_level"))
    structural_stop = _number(row.get("structural_stop"))
    structural_invalidation = {
        "level": invalidation_level,
        "structural_stop": structural_stop,
        "valid": bool(
            (invalidation_level is not None and price and (
                (direction == "BUY" and invalidation_level < price)
                or (direction == "SELL" and invalidation_level > price)
            ))
            or (structural_stop is not None and price and (
                (direction == "BUY" and structural_stop < price)
                or (direction == "SELL" and structural_stop > price)
            ))
        ),
        "reason": _text(row.get("invalidation_reason")) or "NONE",
    }

    freshness_state = _text(row.get("data_freshness") or indicator.get("quality")).upper() or "UNKNOWN"
    max_age = _number(row.get("analysis_max_age_seconds"), None)
    if max_age is None:
        timeframe_ms = _number(row.get("timeframe_ms"))
        max_age = (timeframe_ms / 1000.0 * 2.0) if timeframe_ms and timeframe_ms > 0 else DEFAULT_MAX_ANALYSIS_AGE_SEC
    updated = _number(row.get("last_updated") or row.get("last_update"))
    age = max(0.0, now - updated) if updated and now else None
    if freshness_state == "FRESH" and age is not None and age > max_age:
        freshness_state = "STALE"
    data_freshness = {
        "state": freshness_state,
        "age_seconds": age,
        "max_age_seconds": max_age,
        "closed_frame_source": _text(row.get("closed_candle_source") or row.get("frame_source")).upper() or "UNKNOWN",
        "data_quality": _text(row.get("data_quality")).upper() or "UNKNOWN",
    }

    raw_behavior = _text(market_intel.get("entry_behavior") or row.get("entry_behavior") or behavior_context.get("raw_behavior"))
    entry_type = _text(behavior_context.get("entry_type") or row.get("entry_type") or normalize_entry_type(raw_behavior))
    classification_reasons = _items(
        market_intel.get("reason_codes") or row.get("entry_behavior_reasons") or behavior_context.get("reason_codes")
    )
    classification_confidence = _confidence(
        behavior_context.get("confirmation")
        if behavior_context.get("confirmation") is not None
        else row.get("entry_behavior_confidence")
        if row.get("entry_behavior_confidence") is not None
        else market_intel.get("confidence")
    )
    if entry_type not in ENTRY_CLASSIFICATIONS:
        classification_status = "NOT_CLASSIFIED"
        entry_type = ""
    else:
        classification_status = "CLASSIFIED"
    entry_classification = {
        "status": classification_status,
        "entry_type": entry_type,
        "reason_codes": classification_reasons,
        "confidence": classification_confidence,
        "recommendation_only": True,
        "final_authority": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED",
    }

    gate_state = "INSUFFICIENT_DATA"
    gate_reasons: List[str] = []
    if room["status"] in {"INSUFFICIENT", "INSIDE_OPPOSING_ZONE"}:
        gate_state = "BLOCKED"
        gate_reasons = list(room["reasons"])
    elif room["status"] in {"SUFFICIENT", "CLEAR"}:
        gate_state = "ALLOWED"
        gate_reasons = list(room["reasons"])
    else:
        gate_reasons = list(room["reasons"])
    # Fresh, validated closed data is required before a location report may be
    # presented as an allowance. Missing evidence stays neutral; stale evidence
    # is downgraded and never described as a fresh allowance.
    validated_closed = data_freshness["data_quality"] == "VALID_CLOSED_CANDLES" and freshness_state == "FRESH"
    if not validated_closed:
        if gate_state == "ALLOWED":
            gate_state = "INSUFFICIENT_DATA"
        gate_reasons.append("location_report_requires_fresh_validated_closed_data")

    confidence_sources = []
    if classification_confidence is not None and classification_status == "CLASSIFIED":
        confidence_sources.append("entry_behavior_confidence")
    zone_quality = institutional_zone["quality"]
    if zone_quality is not None:
        confidence_sources.append("institutional_zone_quality")
    if liq_confidence is not None:
        confidence_sources.append("liquidity_confidence")
    analysis_confidence = {
        "score": classification_confidence,
        "status": "CLASSIFIER_CONFIDENCE" if classification_confidence is not None else "NOT_AVAILABLE",
        "sources": confidence_sources,
        "is_success_probability": False,
    }

    return {
        "schema": 1,
        "source": "EXISTING_WATCHLIST_AND_ENGINE_EVIDENCE",
        "side": direction or "UNKNOWN",
        "cross_location": cross_location,
        "market_regime": {"state": regime, "direction": _text(row.get("regime_direction") or row.get("trend_direction")).upper() or "UNKNOWN"},
        "institutional_zone": institutional_zone,
        "structure_bias": structure_bias,
        "liquidity_context": liquidity_context,
        "volume_confirmation": volume_confirmation,
        "trend_strength": trend_strength,
        "available_price_room": room,
        "structural_invalidation": structural_invalidation,
        "data_freshness": data_freshness,
        "analysis_confidence": analysis_confidence,
        "entry_classification": entry_classification,
        "location_gate": {"state": gate_state, "reasons": gate_reasons, "authority": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"},
        "execution_authority": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED",
    }


def validate_institutional_location(report: Any) -> Dict[str, Any]:
    """Terminal read-only gate over the report's observed location evidence."""
    mapped = _mapping(report)
    gate = _mapping(mapped.get("location_gate"))
    state = _text(gate.get("state")).upper() or "INSUFFICIENT_DATA"
    reasons = _items(gate.get("reasons"))
    return {
        "allowed": state == "ALLOWED",
        "state": state,
        "reasons": reasons,
        "blocking_reason": "|".join(reasons) if state == "BLOCKED" else "",
    }


def build_entry_risk_plan(
    *,
    side: str,
    entry_price: float,
    stop_price: float,
    quantity: float,
    target_price: Optional[float] = None,
    target_move_pct: Optional[float] = None,
    account_equity: Optional[float] = None,
    contract_size: float = 1.0,
    leverage: int = POLICY_LEVERAGE,
    sizing_decision: Any = None,
) -> Dict[str, Any]:
    """Pre-trade risk record; leverage is applied once through the sizing decision."""
    direction = str(side or "").strip().upper()
    entry = _number(entry_price)
    stop = _number(stop_price)
    qty = _number(quantity)
    contracts = _number(contract_size, 1.0)
    lev = int(_number(leverage, POLICY_LEVERAGE) or POLICY_LEVERAGE)
    if direction not in {"BUY", "SELL"} or not entry or not stop or not qty or not contracts:
        raise ValueError("side, entry_price, stop_price, quantity and contract_size must be valid")
    if min(entry, stop, qty, contracts) <= 0 or lev <= 0:
        raise ValueError("prices, quantity, contract_size and leverage must be positive")
    if lev != POLICY_LEVERAGE:
        raise ValueError(f"leverage must be exactly {POLICY_LEVERAGE}x")
    if (direction == "BUY" and stop >= entry) or (direction == "SELL" and stop <= entry):
        raise ValueError("stop_price must be on the protective side of entry_price")

    sizing = _mapping(sizing_decision)
    notional = entry * qty * contracts
    # Use the already-frozen sizing values when available. These are aliases of
    # the one sizing decision, not a second sizing calculation.
    margin = (_number(sizing.get("margin_required"))
              or _number(sizing.get("estimated_margin"))
              or notional / lev)
    stop_move = abs(entry - stop) / entry * 100.0
    # The helper prices a favourable move for the supplied side. Price the
    # protective stop as an adverse move of the opposite side, then ADD the
    # estimated costs: for a loss, fees and slippage increase the expected loss.
    adverse_side = "SELL" if direction == "BUY" else "BUY"
    stop_plan = exchange_pnl_at_move(
        side=adverse_side, entry_price=entry, quantity=qty, move_pct=stop_move,
        leverage=lev, contract_size=contracts,
        taker_fee_pct=ESTIMATED_TAKER_FEE_PCT, slippage_pct=ESTIMATED_SLIPPAGE_PCT,
    )
    gross_stop_loss = abs(_number(stop_plan.get("gross_pnl"), 0.0) or 0.0)
    estimated_stop_costs = max(0.0, _number(stop_plan.get("estimated_total_costs"), 0.0) or 0.0)
    net_stop_loss = gross_stop_loss + estimated_stop_costs

    target_plan = None
    move = _number(target_move_pct)
    if move is None and target_price is not None:
        move = _move_pct_from_price(direction, entry, target_price)
    if move is not None and move > 0:
        target_plan = exchange_pnl_at_move(
            side=direction, entry_price=entry, quantity=qty, move_pct=move,
            leverage=lev, contract_size=contracts,
            taker_fee_pct=ESTIMATED_TAKER_FEE_PCT, slippage_pct=ESTIMATED_SLIPPAGE_PCT,
        )
    target_gross = abs(_number(target_plan.get("gross_pnl"), 0.0) or 0.0) if target_plan else None
    target_net = _number(target_plan.get("estimated_net_pnl"), 0.0) if target_plan else None
    equity = _number(account_equity)
    account_risk_pct = (net_stop_loss / equity * 100.0) if equity and equity > 0 else None
    budget = None
    budget_source = "NONE"
    for key in ("risk_amount", "risk_budget", "risk_budget_usdt", "max_risk_usdt"):
        candidate = _number(sizing.get(key))
        if candidate is not None and candidate > 0:
            budget = candidate
            budget_source = f"sizing_decision.{key}"
            break
    return {
        "basis": "PRE_TRADE_ESTIMATE_NOT_REALIZED_PNL",
        "side": direction,
        "leverage": lev,
        "leverage_applied_once": True,
        "notional_exposure": notional,
        "margin_allocation": margin,
        "account_equity": equity,
        "account_risk_pct": account_risk_pct,
        "price_stop_move_pct": stop_move,
        "gross_stop_loss": gross_stop_loss,
        "estimated_stop_costs": estimated_stop_costs,
        "estimated_net_loss": net_stop_loss,
        "risk_budget_from_sizing": budget,
        "risk_budget_source": budget_source,
        "risk_within_sizing_budget": (budget is not None and budget > 0 and net_stop_loss <= budget * 1.0000001) if budget else None,
        "price_target_move_pct": move,
        "gross_roe_pct": _number(target_plan.get("gross_roe_pct")) if target_plan else None,
        "estimated_net_roe_pct": _number(target_plan.get("estimated_net_roe_pct")) if target_plan else None,
        "estimated_net_return_pct_of_margin": _number(target_plan.get("estimated_net_roe_pct")) if target_plan else None,
        "gross_target_pnl": target_gross,
        "estimated_net_target_pnl": target_net,
        "risk_reward_gross": (target_gross / gross_stop_loss) if target_gross is not None and gross_stop_loss > 0 else None,
        "risk_reward_net": (abs(target_net) / net_stop_loss) if target_net is not None and net_stop_loss > 0 else None,
        "idealized_liquidation_distance_pct": 100.0 / lev,
        "liquidation_distance_basis": "100/leverage before fees, funding, maintenance margin and slippage; exchange value is authoritative",
        "binding_constraint": _text(sizing.get("binding_constraint")) or "UNKNOWN",
        "margin_achieved_ratio": _number(sizing.get("margin_achieved_ratio")),
        "profit_target_convention": profit_target_convention(),
    }
