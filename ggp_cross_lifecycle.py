"""Cross-event lifecycle and cross-location authority (execution-free).

This module turns the existing EMA9 / Session-VWAP proximity signal into an
explicit cross candidate and evaluates *the cross location itself* against the
institutional context that the production bridge already builds.

Design boundaries:
- It never fetches market data, never submits orders and never creates an
  independent opportunity. It annotates candidates that the two existing
  scanners already discovered.
- It does not replace UnifiedEntryAuthority, TradeExecutionService or
  TradeManagementBoard. It only supplies an explainable quality class, an
  event identity and lifecycle transitions to the existing production path.
- ADX >= 18 is a hard gate. A high structural score never overrides it.
"""
from __future__ import annotations

from enum import Enum
import math
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

MIN_ENTRY_ADX = 18.0
DEFAULT_RETENTION_SECONDS = 2700.0
EXCELLENT_QUALITY = "EXCELLENT"
GOOD_QUALITY = "GOOD"
MODERATE_QUALITY = "MODERATE"
WEAK_QUALITY = "WEAK"
INVALID_QUALITY = "INVALID"
# Backwards-compatible constant names for existing callers and tests.
HIGH_QUALITY = EXCELLENT_QUALITY

APPROVABLE_QUALITIES = {EXCELLENT_QUALITY, GOOD_QUALITY}


class CrossLifecycleState(str, Enum):
    CROSS_DETECTED = "CROSS_DETECTED"
    WAITING_FOR_INSTITUTIONAL_ANALYSIS = "WAITING_FOR_INSTITUTIONAL_ANALYSIS"
    INSTITUTIONAL_ANALYSIS_PENDING = "INSTITUTIONAL_ANALYSIS_PENDING"
    ZONE_ANALYSIS_COMPLETE = "ZONE_ANALYSIS_COMPLETE"
    ENTRY_CANDIDATE = "ENTRY_CANDIDATE"
    INSTITUTIONAL_APPROVED = "INSTITUTIONAL_APPROVED"
    INSTITUTIONAL_REJECTED = "INSTITUTIONAL_REJECTED"
    EXECUTION_READY = "EXECUTION_READY"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTRY_FILLED = "ENTRY_FILLED"
    ENTRY_REJECTED = "ENTRY_REJECTED"
    CONSUMED = "CONSUMED"


_ALLOWED_TRANSITIONS: Dict[str, set] = {
    CrossLifecycleState.CROSS_DETECTED.value: {
        CrossLifecycleState.WAITING_FOR_INSTITUTIONAL_ANALYSIS.value,
        CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.WAITING_FOR_INSTITUTIONAL_ANALYSIS.value: {
        CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value: {
        CrossLifecycleState.ZONE_ANALYSIS_COMPLETE.value,
        CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.ZONE_ANALYSIS_COMPLETE.value: {
        CrossLifecycleState.ENTRY_CANDIDATE.value,
        CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.ENTRY_CANDIDATE.value: {
        # Execution-side gates can re-confirm the institutional approval before
        # the final single-flight cross readiness check.
        CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
        CrossLifecycleState.EXECUTION_READY.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.INSTITUTIONAL_APPROVED.value: {
        CrossLifecycleState.EXECUTION_READY.value,
        CrossLifecycleState.ENTRY_CANDIDATE.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
        # A later scan re-runs the same closed-candle analysis before entry.
        CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value,
    },
    CrossLifecycleState.EXECUTION_READY.value: {
        CrossLifecycleState.ENTRY_SUBMITTED.value,
        CrossLifecycleState.INSTITUTIONAL_REJECTED.value,
    },
    CrossLifecycleState.ENTRY_SUBMITTED.value: {
        CrossLifecycleState.ENTRY_FILLED.value,
        CrossLifecycleState.ENTRY_REJECTED.value,
    },
    CrossLifecycleState.ENTRY_REJECTED.value: {
        CrossLifecycleState.CROSS_DETECTED.value,
    },
    CrossLifecycleState.ENTRY_FILLED.value: {
        CrossLifecycleState.CONSUMED.value,
    },
    CrossLifecycleState.CONSUMED.value: set(),
    CrossLifecycleState.INSTITUTIONAL_REJECTED.value: {
        CrossLifecycleState.CROSS_DETECTED.value,
        CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value,
    },
}


def finite(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Return a finite float or ``default``; never propagate NaN/inf."""
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def adx_gate(adx: Any) -> Tuple[bool, str]:
    """Single ADX gate shared by cross-location analysis and entry policy."""
    value = finite(adx)
    if value is None:
        return False, "adx_unavailable"
    if value < MIN_ENTRY_ADX:
        return False, f"adx_below_{MIN_ENTRY_ADX:g}"
    return True, "adx_gate_passed"


def make_cross_event_id(symbol: str, timeframe_seconds: int, direction: str,
                        candle_timestamp: int) -> str:
    """Stable identity for one confirmed cross on one closed candle."""
    return (f"{str(symbol or '').strip().upper()}|{int(timeframe_seconds)}s|"
            f"{str(direction or '').strip().upper()}|{int(candle_timestamp)}")


def normalize_cross_event(symbol: str, timeframe_seconds: int, proximity: Dict[str, Any],
                          adx: Any = None, now_ms: Optional[int] = None,
                          retention_seconds: float = DEFAULT_RETENTION_SECONDS) -> Optional[Dict[str, Any]]:
    """Convert an existing proximity result into the unified cross candidate.

    Only an EMA9 / Session-VWAP cross is a confirmed core signal. EMA50 and
    EMA50/VWAP crosses stay diagnostic, exactly as in the existing strategy.
    An APPROACHING candidate keeps its identity across later scans even after
    the proximity state disappears, until it is expired or invalidated.
    """
    if not isinstance(proximity, dict):
        return None
    tf = int(timeframe_seconds or 900)
    candle_ts = finite(proximity.get("candle_timestamp"), None)
    if candle_ts is None:
        return None
    candle_ts = int(candle_ts)
    state = str(proximity.get("state") or "").upper()
    freshness = str(proximity.get("data_freshness") or "").upper()
    direction = str(proximity.get("direction") or "").upper()
    cross9 = str(proximity.get("cross_ema9") or "").upper()
    expected_cross = "BULLISH" if direction == "BUY" else "BEARISH" if direction == "SELL" else ""
    confirmed = (cross9 == expected_cross and expected_cross != ""
                 and (freshness == "FRESH" or state == "CROSS_CONFIRMED"))
    if confirmed:
        cross_status = "CONFIRMED"
        cross_direction = direction
    elif state in {"NEAR_CROSS", "CONVERGING", "AT_VWAP"} and direction in {"BUY", "SELL"}:
        cross_status = "APPROACHING"
        cross_direction = direction
    else:
        return None
    adx_value = finite(adx)
    event_id = (make_cross_event_id(symbol, tf, cross_direction, candle_ts)
                if cross_status == "CONFIRMED" else None)
    candidate_id = (event_id or
                    f"{str(symbol or '').strip().upper()}|{tf}s|APPROACHING|{candle_ts}")
    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    return {
        "symbol": symbol,
        "cross_direction": cross_direction,
        "cross_status": cross_status,
        "candle_timestamp": candle_ts,
        "ema9": finite(proximity.get("ema9")),
        "ema50": finite(proximity.get("ema50")),
        "session_vwap": finite(proximity.get("vwap")),
        "cross_distance_atr": finite(proximity.get("gap_atr")),
        "adx": adx_value,
        "data_freshness": freshness or "UNKNOWN",
        "cross_event_id": event_id,
        "candidate_id": candidate_id,
        "institutional_analysis_status": CrossLifecycleState.CROSS_DETECTED.value,
        "crossover_state": state,
        "final_rejection_reason": None,
        "evidence": [f"proximity_state:{state}", f"cross_status:{cross_status}"],
        "first_seen_ms": now_value,
        "last_seen_ms": now_value,
        "expires_at_ms": candle_ts + tf * 1000 + int(retention_seconds * 1000),
        "timeframe_seconds": tf,
    }


def merge_cross_events(previous: Dict[str, Dict[str, Any]],
                       observed: Iterable[Dict[str, Any]],
                       now_ms: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """Retain cross candidates after the visual cross disappears.

    Confirmed crosses are keyed by their unique event id. Approaching setups
    are keyed by symbol/timeframe/candle so the same approach is not duplicated.
    Existing candidates are never deleted merely because one scan no longer
    sees the cross; they are expired only by their own timestamp.
    """
    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    out: Dict[str, Dict[str, Any]] = {
        str(key): dict(value) for key, value in (previous or {}).items() if isinstance(value, dict)
    }
    for candidate in observed or ():
        if not isinstance(candidate, dict):
            continue
        key = str(candidate.get("cross_event_id") or candidate.get("candidate_id") or "")
        if not key:
            continue
        old = out.get(key)
        if isinstance(old, dict):
            merged = dict(old)
            merged.update(candidate)
            # Later scans refresh observation metadata only; they must not erase
            # analysis results, quality evidence or already-consumed lifecycle state.
            for preserved in ("institutional_analysis_status", "cross_quality", "cross_location",
                              "final_rejection_reason", "state_history", "first_seen_ms",
                              "cross_event_id_consumed"):
                if preserved in old:
                    merged[preserved] = old[preserved]
            merged["first_seen_ms"] = old.get("first_seen_ms", candidate.get("first_seen_ms", now_value))
            merged["last_seen_ms"] = now_value
            merged["evidence"] = list(dict.fromkeys(
                list(old.get("evidence") or []) + list(candidate.get("evidence") or [])))
            out[key] = merged
        else:
            out[key] = dict(candidate)
    for key, value in list(out.items()):
        expiry = finite(value.get("expires_at_ms"), None)
        if expiry is not None and now_value > expiry:
            value["cross_status"] = "EXPIRED"
            value["final_rejection_reason"] = "cross_signal_expired_without_reentry"
            value["institutional_analysis_status"] = CrossLifecycleState.INSTITUTIONAL_REJECTED.value
    return out


def transition_state(candidate: Dict[str, Any], new_state: str,
                     reason: str = "") -> Dict[str, Any]:
    """Apply an auditable lifecycle transition without ever skipping a gate."""
    updated = dict(candidate or {})
    current = str(updated.get("institutional_analysis_status")
                  or CrossLifecycleState.CROSS_DETECTED.value)
    target = str(new_state)
    prior_reason = str(updated.get("final_rejection_reason") or "")
    # A duplicate or expired identity can never be re-approved by a later scan.
    if (current == CrossLifecycleState.INSTITUTIONAL_REJECTED.value
            and prior_reason in {"duplicate_cross_event_id",
                                 "cross_signal_expired_without_reentry"}
            and target != current):
        updated["final_rejection_reason"] = prior_reason
        return updated
    if target not in _ALLOWED_TRANSITIONS:
        updated["final_rejection_reason"] = f"invalid_lifecycle_transition:{current}->{target}"
        return updated
    allowed = _ALLOWED_TRANSITIONS[current]
    if target != current and target not in allowed:
        updated["final_rejection_reason"] = f"blocked_lifecycle_transition:{current}->{target}"
        return updated
    updated["institutional_analysis_status"] = target
    history = list(updated.get("state_history") or [])
    history.append({"state": target, "reason": reason, "ts": time.time()})
    updated["state_history"] = history[-40:]
    if target in {CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
                  CrossLifecycleState.EXECUTION_READY.value,
                  CrossLifecycleState.ENTRY_SUBMITTED.value,
                  CrossLifecycleState.ENTRY_FILLED.value}:
        updated["final_rejection_reason"] = None
    elif target == CrossLifecycleState.INSTITUTIONAL_REJECTED.value and reason:
        updated["final_rejection_reason"] = reason
    return updated


_PERMANENT_REJECTIONS = {"duplicate_cross_event_id", "cross_signal_expired_without_reentry"}


def cross_lifecycle_badge(row: Optional[Dict[str, Any]]) -> str:
    """Return the Watchlist badge driven by the real event state, never a label."""
    item = row if isinstance(row, dict) else {}
    state = str(item.get("institutional_analysis_status") or item.get("lifecycle_state") or "").upper()
    status = str(item.get("cross_status") or "").upper()
    decision_state = str(item.get("decision_state") or "").upper()
    reason = str(item.get("final_rejection_reason") or "")
    consumed = bool(item.get("cross_event_id_consumed") or item.get("consumed"))
    if (status == "EXPIRED" or consumed
            or state in {CrossLifecycleState.CONSUMED.value,
                         CrossLifecycleState.WAITING_FOR_INSTITUTIONAL_ANALYSIS.value}):
        return "⚪ EXPIRED / WAITING"
    if (state == CrossLifecycleState.INSTITUTIONAL_REJECTED.value
            or status == "REJECTED" or decision_state == "REJECTED"
            or reason in _PERMANENT_REJECTIONS):
        return "🔴 REJECTED"
    if state == CrossLifecycleState.EXECUTION_READY.value:
        return "🚀 EXECUTION READY"
    if state in {CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
                 CrossLifecycleState.ENTRY_CANDIDATE.value}:
        return "🟢 ENTRY CANDIDATE"
    if state in {CrossLifecycleState.INSTITUTIONAL_ANALYSIS_PENDING.value,
                 CrossLifecycleState.ZONE_ANALYSIS_COMPLETE.value}:
        return "🟣 INSTITUTIONAL ZONE ANALYSIS"
    if status == "CONFIRMED":
        return "🟡 CROSS CONFIRMED — ANALYZING"
    if status == "APPROACHING":
        return "🔵 APPROACHING CROSS"
    return "⚪ EXPIRED / WAITING"


def cross_lifecycle_identity(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Flatten the required Watchlist identity fields from the retained event."""
    item = row if isinstance(row, dict) else {}
    event = item.get("cross_event") if isinstance(item.get("cross_event"), dict) else {}
    proximity = (item.get("ema9_vwap_proximity")
                 if isinstance(item.get("ema9_vwap_proximity"), dict) else {})
    icons = item.get("icons") if isinstance(item.get("icons"), dict) else {}
    adx_di = item.get("adx_di") if isinstance(item.get("adx_di"), dict) else {}
    golden = item.get("golden_zone") if isinstance(item.get("golden_zone"), dict) else {}
    ob = item.get("order_block") if isinstance(item.get("order_block"), dict) else {}
    fvg = item.get("fvg") if isinstance(item.get("fvg"), dict) else {}
    structure = (item.get("market_structure")
                 if isinstance(item.get("market_structure"), dict) else {})
    liquidity = item.get("liquidity") if isinstance(item.get("liquidity"), dict) else {}
    evidence = [str(value) for value in (item.get("evidence") or [])]
    retest = next((value for value in evidence
                   if any(token in value.lower() for token in ("retest", "pullback"))), None)
    if retest is None and str(icons.get("pullback") or "") == "🔄":
        retest = "retest_observed"
    zone = str(golden.get("state") or "").upper()
    if not zone and ob:
        zone = "ORDER_BLOCK:" + str(ob.get("status") or "UNKNOWN").upper()
    if not zone and fvg:
        zone = "FVG:" + str(fvg.get("status") or "UNKNOWN").upper()
    return {
        "badge": cross_lifecycle_badge(item),
        "lifecycle_state": str(item.get("institutional_analysis_status") or "UNKNOWN"),
        "symbol": item.get("symbol") or event.get("symbol") or "UNKNOWN",
        "cross_direction": str(
            item.get("cross_direction") or event.get("cross_direction")
            or item.get("side") or item.get("signal_direction") or "UNKNOWN"
        ).upper(),
        "cross_status": str(item.get("cross_status") or event.get("cross_status") or "NONE").upper(),
        "candle_timestamp": item.get("candle_timestamp") or event.get("candle_timestamp"),
        "cross_event_id": item.get("cross_event_id") or event.get("cross_event_id"),
        "adx": finite(item.get("adx") or adx_di.get("adx") or event.get("adx")),
        "ema9": finite(item.get("ema9") or event.get("ema9") or proximity.get("ema9")),
        "session_vwap": finite(item.get("vwap") or event.get("session_vwap") or proximity.get("vwap")),
        "distance_from_vwap": finite(item.get("cross_distance_atr")
                                     or event.get("cross_distance_atr")
                                     or proximity.get("gap_atr")),
        "institutional_zone": zone or "UNKNOWN",
        "zone_quality": item.get("cross_quality") or item.get("zone_quality") or "UNRATED",
        "market_structure": str(structure.get("pattern") or structure.get("mss")
                                or item.get("market_regime") or "UNKNOWN").upper(),
        "liquidity_condition": str(liquidity.get("event") or item.get("liquidity_condition")
                                   or "UNKNOWN").upper(),
        "retest_status": retest or "NOT_CONFIRMED",
        "volume_condition": str(item.get("volume_condition")
                                or ("CONFIRMED" if item.get("volume_confirmed") else "NOT_CONFIRMED")),
        "opportunity_classification": str(item.get("trading_mode")
                                          or item.get("opportunity_classification")
                                          or item.get("decision_setup_type") or "UNCLASSIFIED"),
        "waiting_or_rejection_reason": (item.get("final_rejection_reason")
                                        or item.get("rejection_reason")
                                        or item.get("waiting_reason") or ""),
    }


def opportunity_summary(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Explainable BEST BUY / BEST SELL summary without invented probabilities."""
    item = row if isinstance(row, dict) else {}
    identity = cross_lifecycle_identity(item)
    location = item.get("cross_location") if isinstance(item.get("cross_location"), dict) else {}
    reasons: List[str] = []
    if identity["zone_quality"] and identity["zone_quality"] != "UNRATED":
        reasons.append(f"zone_quality:{identity['zone_quality']}")
    if identity["adx"] is not None:
        reasons.append(f"adx:{identity['adx']:g}")
    reasons.extend(str(value) for value in (location.get("evidence") or [])[:6])
    if item.get("cross_rank") is not None:
        reasons.append(f"cross_rank:{item.get('cross_rank')}")
    return {
        "symbol": identity["symbol"],
        "cross_event_id": identity["cross_event_id"],
        "direction": identity["cross_direction"],
        "cross_timestamp": identity["candle_timestamp"],
        "adx": identity["adx"],
        "best_institutional_zone": identity["institutional_zone"],
        "zone_quality": identity["zone_quality"],
        "entry_area": {"low": item.get("entry_zone_low"), "high": item.get("entry_zone_high")},
        "stop_loss_reference": item.get("structural_stop"),
        "opportunity_type": identity["opportunity_classification"],
        "selection_reasons": reasons,
        "badge": identity["badge"],
        "cross_rank": item.get("cross_rank"),
    }


def rank_cross_candidates_by_side(items: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Rank BUY and SELL queues independently and publish explainable best picks."""
    ranked = rank_cross_candidates(items)
    queues: Dict[str, List[Dict[str, Any]]] = {"BUY": [], "SELL": []}
    for row in ranked:
        side = str(row.get("side") or row.get("cross_direction") or "").upper()
        if side in queues:
            queues[side].append(row)

    def _eligible(row: Dict[str, Any]) -> bool:
        status = str(row.get("cross_status") or "").upper()
        quality = str(row.get("cross_quality") or row.get("quality") or "").upper()
        reason = str(row.get("final_rejection_reason") or "")
        lifecycle = str(row.get("institutional_analysis_status") or "").upper()
        adx_ok, _ = adx_gate(row.get("adx"))
        return bool(
            status == "CONFIRMED" and row.get("cross_event_id")
            and not reason and quality in APPROVABLE_QUALITIES and adx_ok
            and lifecycle in {
                CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
                CrossLifecycleState.ENTRY_CANDIDATE.value,
                CrossLifecycleState.EXECUTION_READY.value,
            }
        )

    best_buy = next((opportunity_summary(row) for row in queues["BUY"] if _eligible(row)), None)
    best_sell = next((opportunity_summary(row) for row in queues["SELL"] if _eligible(row)), None)
    return {
        "BUY": queues["BUY"],
        "SELL": queues["SELL"],
        "queue": ranked,
        "best_buy": best_buy,
        "best_sell": best_sell,
        "updated_at": time.time(),
    }


def _zone_quality(features: Dict[str, Any]) -> str:
    """Explainable cross-location class; score is supporting evidence only."""
    side = str(features.get("cross_direction") or features.get("side") or "").upper()
    reasons: List[str] = []
    if side not in {"BUY", "SELL"}:
        return INVALID_QUALITY
    adx_ok, adx_reason = adx_gate(features.get("adx"))
    if not adx_ok:
        reasons.append(adx_reason)
    expected = "BULLISH" if side == "BUY" else "BEARISH"
    cross_dir = str(features.get("cross_ema9") or features.get("cross_direction_signal") or "").upper()
    if cross_dir and cross_dir != expected:
        return INVALID_QUALITY
    indicator_quality = str(features.get("indicator_quality", "")).upper()
    critical = {
        "valid_closed_data": bool(features.get("valid_closed_data", True)),
        "fresh_indicators": indicator_quality not in {"STALE_DATA", "DATA_UNAVAILABLE"},
        "real_value_area": bool(features.get("real_value_area", False)),
        "room_before_opposing_liquidity": bool(features.get("room_before_opposing_liquidity", False)),
        "room_before_opposing_structure": bool(features.get("room_before_opposing_structure", False)),
        "market_structure_confirmed": bool(features.get("market_structure_confirmed", False)),
        "no_opposing_pressure": not bool(features.get("opposing_pressure", False)),
    }
    if side == "BUY":
        core = {
            "demand_zone_near": bool(features.get("demand_zone_near", False)),
            "bullish_order_block_valid": bool(features.get("bullish_order_block_valid", False)),
            "bullish_fvg_valid": bool(features.get("bullish_fvg_valid", False)),
        }
        event = bool(features.get("sweep_reclaim_confirmed", False))
    else:
        core = {
            "supply_zone_near": bool(features.get("supply_zone_near", False)),
            "bearish_order_block_valid": bool(features.get("bearish_order_block_valid", False)),
            "bearish_fvg_valid": bool(features.get("bearish_fvg_valid", False)),
        }
        event = bool(features.get("buy_side_sweep_rejection", False))
    flow = {
        "smart_money_support": bool(features.get("smart_money_support", False)),
        "momentum_support": bool(features.get("momentum_support", False)),
        "volume_confirmed": bool(features.get("volume_confirmed", False)),
        "institutional_flow_support": bool(features.get("institutional_flow_support", False)),
    }
    for name, passed in list(critical.items()) + list(core.items()) + list(flow.items()):
        if not passed:
            reasons.append(f"missing:{name}")
    if not event:
        reasons.append("missing:liquidity_sweep_or_reclaim_confirmation")
    if not features.get("premium_discount_aligned", False):
        reasons.append("missing:premium_discount_alignment")
    space = finite(features.get("available_price_space_atr"), 0.0) or 0.0
    if space <= 0.0:
        reasons.append("no_available_price_space")
    failed_critical = [name for name, passed in critical.items() if not passed]
    if not adx_ok or failed_critical:
        return INVALID_QUALITY
    core_count = sum(1 for passed in core.values() if passed)
    flow_count = sum(1 for passed in flow.values() if passed)
    if (core_count >= 2 and flow_count >= 2 and event
            and features.get("premium_discount_aligned", False) and space >= 1.0):
        return EXCELLENT_QUALITY
    if core_count >= 1 and flow_count >= 1 and space >= 0.7:
        return GOOD_QUALITY
    if (core_count + flow_count) >= 2 and space >= 0.5:
        return MODERATE_QUALITY
    return WEAK_QUALITY


def evaluate_cross_location(features: Dict[str, Any],
                            direction: Optional[str] = None) -> Dict[str, Any]:
    """Evaluate a confirmed cross at its own location, with retained evidence."""
    features = dict(features or {})
    if direction:
        features["cross_direction"] = str(direction).upper()
    side = str(features.get("cross_direction") or "").upper()
    adx_ok, adx_reason = adx_gate(features.get("adx"))
    quality = _zone_quality(features)
    evidence = [
        f"cross_side:{side}",
        f"adx:{finite(features.get('adx'))}::{adx_reason}",
        f"quality:{quality}",
    ]
    for key in sorted(k for k in features if k not in {"adx", "cross_direction"}):
        value = features.get(key)
        if isinstance(value, bool):
            evidence.append(f"{key}:{'PASS' if value else 'FAIL'}")
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            evidence.append(f"{key}:{value}")
        elif value is not None:
            # Keep categorical evidence (indicator freshness, cross direction,
            # zone state) auditable in the same stream as boolean/numeric checks.
            evidence.append(f"{key}:{str(value)[:80]}")
    approved = quality in APPROVABLE_QUALITIES and adx_ok
    expected_signal = "BULLISH" if side == "BUY" else "BEARISH"
    observed_signal = str(features.get("cross_ema9") or features.get("cross_direction_signal") or "").upper()
    mismatch = bool(observed_signal and observed_signal != expected_signal)
    rejection = [] if approved else [
        reason for reason in (
            adx_reason if not adx_ok else "",
            "cross_direction_mismatch" if mismatch else "",
            None if quality in APPROVABLE_QUALITIES else f"cross_location_quality:{quality}",
        ) if reason
    ]
    return {
        "quality": quality,
        "quality_class": quality,
        "approved": approved,
        "adx_ok": adx_ok,
        "adx_reason": adx_reason,
        "side": side,
        "evidence": evidence,
        "rejection_reasons": rejection,
        "features": features,
    }


def build_location_features(context: Dict[str, Any],
                            cross_event: Optional[Dict[str, Any]] = None,
                            side: Optional[str] = None) -> Dict[str, Any]:
    """Map the production institutional context to cross-location features."""
    ctx = context if isinstance(context, dict) else {}
    cross = cross_event if isinstance(cross_event, dict) else {}
    direction = str(side or cross.get("cross_direction") or "").upper()
    buy = direction == "BUY"
    golden = ctx.get("golden_zone") if isinstance(ctx.get("golden_zone"), dict) else {}
    structure = ctx.get("market_structure") if isinstance(ctx.get("market_structure"), dict) else {}
    liquidity_raw = ctx.get("liquidity")
    if isinstance(liquidity_raw, dict):
        liquidity = liquidity_raw
    else:
        # The production bridge builds a LiquidityEvent object; the enriched
        # Watchlist row stores the same evidence as a dict. Support both shapes.
        liquidity = {
            "event": getattr(liquidity_raw, "event", None),
            "direction": getattr(liquidity_raw, "direction", None),
            "level": getattr(liquidity_raw, "level", None),
        }
    ob = ctx.get("order_block") if isinstance(ctx.get("order_block"), dict) else {}
    fvg = ctx.get("fvg") if isinstance(ctx.get("fvg"), dict) else {}
    sr = ctx.get("support_resistance") if isinstance(ctx.get("support_resistance"), dict) else {}
    smart = ctx.get("smart_money") if isinstance(ctx.get("smart_money"), dict) else {}
    momentum = ctx.get("momentum") if isinstance(ctx.get("momentum"), dict) else {}
    trend = ctx.get("institutional_trend") if isinstance(ctx.get("institutional_trend"), dict) else {}
    indicators = ctx.get("indicator_snapshot") if isinstance(ctx.get("indicator_snapshot"), dict) else {}
    expected = "BULLISH" if buy else "BEARISH"
    value_zone = str(golden.get("value_zone") or "").upper()
    golden_state = str(golden.get("state") or "").upper()
    ob_ok = bool(golden.get("ob_valid")) and str(ob.get("status") or "").upper() not in {
        "MITIGATED", "INVALIDATED"}
    fvg_ok = (bool(golden.get("fvg_valid"))
              and str(fvg.get("direction") or "").upper() == expected)
    sr_aligned = bool(golden.get("support_resistance_aligned") or ctx.get("support_resistance_aligned"))
    value_aligned = value_zone == ("DISCOUNT" if buy else "PREMIUM")
    real_value_area = bool(value_aligned and golden_state != "NO_GOLDEN_ZONE"
                           and (ob_ok or fvg_ok or sr_aligned))
    liq_event = str(liquidity.get("event") or "").upper()
    liq_direction = str(liquidity.get("direction") or "").upper()
    liquidity_confirmed = bool(liq_direction == expected and liq_event in {"SWEEP", "GRAB", "RUN", "FAILED_AUCTION"})
    mss = str(structure.get("mss") or "").upper()
    structure_ok = bool(structure.get("confirmed")) and (
        (buy and mss.startswith("BULLISH")) or (not buy and mss.startswith("BEARISH")))
    opposing_level = sr.get("resistances" if buy else "supports") if isinstance(sr, dict) else []
    # Available price space is the distance to the NEAREST opposing boundary,
    # never the EMA9/VWAP gap and never a farther structural level that happens
    # to be large. For BUY the nearest resistance boundary is its low; for SELL
    # the nearest support boundary is its high.
    atr = finite(ctx.get("atr") or indicators.get("atr"), None)
    price = finite(ctx.get("price"), None)
    space_distances: List[float] = []
    near_opposing = False
    if atr and atr > 0 and price:
        def _add_opposing_level(level_value: Any) -> None:
            level = finite(level_value, None)
            if level is None:
                return
            forward = (level - price) if buy else (price - level)
            distance_atr = max(0.0, forward) / atr
            space_distances.append(distance_atr)
            if distance_atr <= 0.5:
                nonlocal near_opposing
                near_opposing = True

        if isinstance(opposing_level, list):
            boundary_keys = ("low", "level", "high") if buy else ("high", "level", "low")
            for zone in opposing_level:
                if not isinstance(zone, dict):
                    continue
                for key in boundary_keys:
                    level = finite(zone.get(key), None)
                    if level is not None:
                        _add_opposing_level(level)
                        break
        # Previous swing highs/lows and opposite-side liquidity are structural
        # boundaries; a cross immediately before them has no usable space.
        _add_opposing_level(structure.get("swing_high" if buy else "swing_low"))
        if liq_direction and liq_direction != expected:
            _add_opposing_level(liquidity.get("level"))
    space_atr = min(space_distances) if space_distances else 0.0
    smart_direction = str(smart.get("institutional_bias") or smart.get("smart_money_direction") or "").upper()
    momentum_direction = str(momentum.get("flow_bias") or momentum.get("momentum_direction") or "").upper()
    trend_direction = str(trend.get("state") or "").upper()
    oppose_trend = bool(trend_direction and not (
        (buy and "BULLISH" in trend_direction) or (not buy and "BEARISH" in trend_direction)))
    return {
        "cross_direction": direction,
        "cross_ema9": str(cross.get("cross_ema9") or
                          ("BULLISH" if cross.get("cross_direction") == "BUY" else "BEARISH")
                          if cross else "").upper(),
        "adx": finite(ctx.get("adx") or indicators.get("adx")),
        "indicator_quality": str(indicators.get("quality") or ctx.get("indicator_quality") or "FRESH").upper(),
        "valid_closed_data": str(ctx.get("data_quality") or "VALID_CLOSED_CANDLES") == "VALID_CLOSED_CANDLES",
        "demand_zone_near": bool(buy and value_aligned and (ob_ok or fvg_ok or sr_aligned)),
        "supply_zone_near": bool(not buy and value_aligned and (ob_ok or fvg_ok or sr_aligned)),
        "bullish_order_block_valid": bool(buy and ob_ok),
        "bearish_order_block_valid": bool(not buy and ob_ok),
        "bullish_fvg_valid": bool(buy and fvg_ok),
        "bearish_fvg_valid": bool(not buy and fvg_ok),
        "sweep_reclaim_confirmed": bool(buy and liquidity_confirmed and structure_ok),
        "buy_side_sweep_rejection": bool(not buy and liquidity_confirmed and structure_ok),
        "liquidity_confirmation": liquidity_confirmed,
        "market_structure_confirmed": structure_ok,
        "room_before_opposing_liquidity": bool(liquidity_confirmed and not near_opposing),
        "room_before_opposing_structure": bool(not near_opposing and bool(space_atr > 0)),
        "smart_money_support": bool(smart_direction == expected),
        "momentum_support": bool((momentum_direction == expected) or bool(momentum.get("trend_expansion"))),
        "volume_confirmed": bool(ctx.get("volume_confirmed", False)),
        "institutional_flow_support": bool(trend.get("supporting_check", False)),
        "real_value_area": real_value_area,
        "premium_discount_aligned": value_aligned,
        "available_price_space_atr": round(space_atr, 4),
        "opposing_pressure": bool(oppose_trend or momentum.get("momentum_decay")),
        "golden_zone_score": finite(golden.get("score"), 0.0) or 0.0,
    }


def cross_rank(item: Dict[str, Any]) -> float:
    """Rank qualified crosses by the requested explainable criteria."""
    row = item if isinstance(item, dict) else {}
    quality = str(row.get("cross_quality") or row.get("quality") or "").upper()
    quality_points = {
        EXCELLENT_QUALITY: 40.0, GOOD_QUALITY: 27.0, MODERATE_QUALITY: 14.0,
        WEAK_QUALITY: 8.0, INVALID_QUALITY: 0.0,
    }.get(quality, 0.0)
    features = row.get("cross_location") if isinstance(row.get("cross_location"), dict) else {}
    features = features.get("features") if isinstance(features.get("features"), dict) else features
    def flag(name: str) -> float:
        return 1.0 if features.get(name) else 0.0
    def number(value: Any, default: float = 0.0) -> float:
        return finite(value, default) or default
    distance = number(row.get("cross_distance_atr"), 999.0)
    proximity = max(0.0, 10.0 * (1.0 - min(1.5, distance) / 1.5))
    adx = number(row.get("adx"), 0.0)
    freshness_age = number(row.get("cross_age_seconds"), 9999.0)
    return round(
        quality_points + proximity
        + 8.0 * flag("bullish_order_block_valid") + 8.0 * flag("bearish_order_block_valid")
        + 6.0 * flag("bullish_fvg_valid") + 6.0 * flag("bearish_fvg_valid")
        + 7.0 * flag("liquidity_confirmation")
        + 7.0 * flag("market_structure_confirmed")
        + min(8.0, max(0.0, (adx - 18.0) / 2.0))
        + 4.0 * flag("momentum_support") + 4.0 * flag("institutional_flow_support")
        + min(6.0, max(0.0, number(features.get("available_price_space_atr")) * 2.0))
        + min(5.0, max(0.0, number(row.get("risk_reward"), 0.0)))
        + max(0.0, 5.0 - freshness_age / 180.0)
        + (6.0 if str(row.get("institutional_analysis_status")) ==
           CrossLifecycleState.INSTITUTIONAL_APPROVED.value else 0.0),
        3,
    )


def rank_cross_candidates(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate by Event ID and rank approved crosses before all others."""
    seen = set()
    rows: List[Dict[str, Any]] = []
    for item in items or ():
        if not isinstance(item, dict):
            continue
        row = dict(item)
        event_id = str(row.get("cross_event_id") or "")
        duplicate = bool(event_id and event_id in seen)
        if event_id:
            seen.add(event_id)
        row["cross_rank"] = cross_rank(row)
        if duplicate:
            row["institutional_analysis_status"] = CrossLifecycleState.INSTITUTIONAL_REJECTED.value
            row["final_rejection_reason"] = "duplicate_cross_event_id"
            row["cross_location_approved"] = False
            row["cross_rank"] = -1.0
        rows.append(row)
    rows.sort(key=lambda row: (row.get("cross_rank", 0.0),
                               str(row.get("cross_event_id") or row.get("candidate_id") or "")),
              reverse=True)
    return rows


def execution_readiness(candidate: Dict[str, Any], *, event_consumed: bool = False,
                        adx: Any = None) -> Tuple[bool, str]:
    """Final cross-lifecycle check; all exchange/risk checks remain in execute_entry."""
    row = candidate if isinstance(candidate, dict) else {}
    if str(row.get("cross_status") or "").upper() != "CONFIRMED":
        return False, "cross_not_confirmed_on_closed_candle"
    if not row.get("cross_event_id"):
        return False, "missing_cross_event_id"
    if event_consumed:
        return False, "cross_event_already_consumed"
    rejected_reason = str(row.get("final_rejection_reason") or "")
    if rejected_reason in {"duplicate_cross_event_id", "cross_signal_expired_without_reentry"}:
        return False, rejected_reason
    adx_ok, adx_reason = adx_gate(adx if adx is not None else row.get("adx"))
    if not adx_ok:
        return False, adx_reason
    quality = str(row.get("cross_quality") or "").upper()
    if quality not in APPROVABLE_QUALITIES:
        return False, f"cross_location_quality:{quality or 'UNRATED'}"
    if str(row.get("institutional_analysis_status")) != CrossLifecycleState.INSTITUTIONAL_APPROVED.value:
        return False, "institutional_analysis_not_approved"
    return True, "cross_lifecycle_execution_ready"
