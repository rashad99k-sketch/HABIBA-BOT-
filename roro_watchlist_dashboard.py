"""Compact, read-only Watchlist projection for the dashboard API.

The full analytical records remain in MEMORY for strategy and execution. This
module only shapes a bounded display payload; it never changes setup state,
entry authority, scanner scheduling, or exchange access.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping

_STATE_PRIORITY = {"ENTRY_READY": 7, "WAITING_FOR_RETEST": 6, "WAITING_FOR_CONFIRMATION": 5, "WAITING_FOR_PULLBACK": 4, "WAITING_FOR_LIQUIDITY_CONFIRMATION": 3, "NO_CHASE": 2, "DISCOVERED": 1}

def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}

def _first_text(*values: Any, default: str = "UNKNOWN") -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()[:120]
    return default

def build_watchlist_dashboard_payload(watchlist: Any, limit: int = 30) -> Dict[str, Dict[str, Any]]:
    """Return at most limit compact rows, prioritizing actionable lifecycle states."""
    if not isinstance(watchlist, Mapping):
        return {}
    bounded_limit = max(1, min(int(limit), 30))
    rows = []
    for key, raw in watchlist.items():
        row = _mapping(raw)
        if not row:
            continue
        decision = _mapping(row.get("decision_context"))
        unified = _mapping(row.get("unified_decision_context"))
        if not decision:
            decision = _mapping(unified.get("decision_context"))
        zone = _mapping(decision.get("zone_context") or unified.get("zone_context"))
        liquidity = _mapping(decision.get("liquidity_context") or unified.get("liquidity_context"))
        structure = _mapping(decision.get("structure_context") or unified.get("structure_context"))
        cross = _mapping(decision.get("cross_context") or unified.get("cross_context") or row.get("cross_context"))
        supply_demand = _mapping(zone.get("supply_demand"))
        state = _first_text(row.get("projected_decision_state"), row.get("decision_state"), row.get("state"))
        try:
            score = float(row.get("score", row.get("discovery_score", 0)) or 0)
        except (TypeError, ValueError):
            score = 0.0
        try:
            updated = float(row.get("last_updated", row.get("last_update", 0)) or 0)
        except (TypeError, ValueError):
            updated = 0.0
        rejection = row.get("rejection_reason")
        if not rejection:
            reasons = row.get("rejection_reasons")
            rejection = reasons[0] if isinstance(reasons, (list, tuple)) and reasons else ""
        if not rejection:
            blockers = decision.get("blocking_reasons")
            rejection = blockers[0] if isinstance(blockers, (list, tuple)) and blockers else ""
        institutional = _mapping(row.get("institutional_report") or unified.get("institutional_report"))
        readiness = _mapping(decision.get("entry_readiness") or row.get("entry_readiness_context") or unified.get("entry_readiness"))
        behavior = _mapping(decision.get("entry_behavior_context") or row.get("entry_behavior_context"))
        classification = _mapping(institutional.get("entry_classification"))
        location_gate = _mapping(institutional.get("location_gate"))
        room = _mapping(institutional.get("available_price_room"))
        report_liquidity = _mapping(institutional.get("liquidity_context"))
        report_invalidation = _mapping(institutional.get("structural_invalidation"))
        report_freshness = _mapping(institutional.get("data_freshness"))
        report_confidence = _mapping(institutional.get("analysis_confidence"))
        missing_stop = readiness.get("missing")
        stop_default = (
            "missing_or_non_positive"
            if isinstance(missing_stop, (list, tuple)) and "STRUCTURAL_STOP_UNKNOWN" in missing_stop
            else "invalid_or_unverified" if readiness.get("sl_valid") is False
            else "UNSPECIFIED"
        )
        stop_validation = _first_text(
            row.get("structural_stop_status"), decision.get("stop_validation_status"),
            default=stop_default,
        )
        diagnostics = {
            "decision_state": _first_text(decision.get("decision_state"), state),
            "cross_state": _first_text(cross.get("display_cross_state"), cross.get("cross_state"), row.get("cross_state"), default="NO_CROSS"),
            "cross_durable": bool(cross.get("durable_event", cross.get("durable", row.get("cross_durable", False))) and cross.get("authoritative", True)),
            "cross_role": _first_text(cross.get("role"), default="UNIFIED_ENTRY_AUTHORITY_UNCHANGED"),
            "zone_quality": zone.get("zone_quality_score", row.get("zone_quality_score")),
            "zone_valid": supply_demand.get("valid", row.get("zone_valid")),
            "liquidity_state": _first_text(liquidity.get("state"), report_liquidity.get("event"), default="UNKNOWN"),
            "liquidity_stage": _first_text(liquidity.get("stage"), default="PENDING"),
            "confirmation": _first_text(structure.get("confirmation_status"), default="UNKNOWN"),
            "pullback": _first_text(decision.get("setup_stage"), row.get("decision_stage"), default="UNKNOWN"),
            "stop_valid": bool(readiness.get("sl_valid") is True),
            "stop_validation": stop_validation,
            "gate": _first_text(decision.get("execution_gate_state"), readiness.get("execution_gate_state"), default="NOT_READY"),
            "blocking": _first_text(decision.get("blocking_reason"), row.get("rejection_reason"), rejection, default="none"),
            "next_event": _first_text(decision.get("required_next_event"), row.get("required_next_event"), default="SETUP_DIAGNOSTICS_REQUIRED"),
            "authority": _first_text(readiness.get("authoritative_gate"), classification.get("final_authority"), default="UNIFIED_ENTRY_AUTHORITY_UNCHANGED"),
            "entry_type": _first_text(behavior.get("entry_type"), row.get("entry_type"), default="NOT_CLASSIFIED"),
            "entry_reason": _first_text(behavior.get("reason"), default="none"),
            "entry_confirmation": behavior.get("confirmation"),
            "entry_invalidation": ", ".join(str(x) for x in (behavior.get("invalidation") or [])[:3]) or "none",
            "entry_source": _first_text(behavior.get("classification_source"), default="NONE"),
            "report_cross": _first_text(_mapping(institutional.get("cross_location")).get("state"), default="UNKNOWN"),
            "report_regime": _first_text(_mapping(institutional.get("market_regime")).get("state"), default="UNKNOWN"),
            "report_zone": _first_text(_mapping(institutional.get("institutional_zone")).get("state"), default="UNKNOWN"),
            "report_structure": _first_text(_mapping(institutional.get("structure_bias")).get("state"), default="UNKNOWN"),
            "report_liquidity": _first_text(report_liquidity.get("event"), default="UNKNOWN"),
            "report_volume": _first_text(_mapping(institutional.get("volume_confirmation")).get("state"), default="UNKNOWN"),
            "report_trend": _first_text(_mapping(institutional.get("trend_strength")).get("state"), default="UNKNOWN"),
            "room_pct": room.get("room_price_pct"), "room_status": _first_text(room.get("status"), default="UNKNOWN"),
            "invalidation_status": "VALID" if report_invalidation.get("valid") else "INVALID",
            "report_freshness": _first_text(report_freshness.get("state"), default="UNKNOWN"),
            "confidence": report_confidence.get("score"),
            "classification": _first_text(classification.get("entry_type"), default="NOT_CLASSIFIED"),
            "classification_reasons": ", ".join(str(x) for x in (classification.get("reason_codes") or [])[:4]) or "none",
            "cross_zone": _first_text(location_gate.get("state"), default="UNKNOWN"),
            "cross_zone_reasons": ", ".join(str(x) for x in (location_gate.get("reasons") or [])[:3]) or "none",
            "lifecycle": _first_text(decision.get("lifecycle_status"), default="INSTITUTIONAL_ANALYSIS"),
            "waiting": _first_text(decision.get("waiting_condition"), default="none"),
            "analysis_status": _first_text(_mapping(decision.get("lifecycle_context")).get("analysis_status"), default="PENDING"),
        }
        item = {
            "symbol": _first_text(row.get("symbol"), key, default="UNKNOWN"),
            "side": _first_text(row.get("side"), default="UNKNOWN"),
            "state": state, "score": round(score, 1), "price": row.get("price"),
            "regime": _first_text(row.get("market_regime"), default="UNKNOWN"),
            "structure": _first_text(structure.get("pattern"), structure.get("direction"), default="UNKNOWN"),
            "liquidity": _first_text(liquidity.get("event"), row.get("liquidity_event"), default="NONE"),
            "liquidity_stage": _first_text(liquidity.get("stage"), default="PENDING"),
            "zone_quality": zone.get("zone_quality_score", row.get("zone_quality_score")),
            "zone_valid": supply_demand.get("valid", row.get("zone_valid")),
            "setup_stage": _first_text(decision.get("setup_stage"), row.get("decision_stage"), default="UNKNOWN"),
            "next_event": _first_text(decision.get("required_next_event"), row.get("required_next_event"), default="WAIT_FOR_UPDATE"),
            "blocking": _first_text(decision.get("blocking_reason"), row.get("rejection_reason"), rejection, default=""),
            "stop_status": stop_validation,
            "freshness": _first_text(row.get("snapshot_freshness"), row.get("data_freshness"), row.get("data_quality"), default="UNKNOWN"),
            "updated_at": updated, "cross": _first_text(cross.get("display_cross_state"), cross.get("cross_state"), row.get("cross_state"), default="NO_CROSS"),
            "durable": bool(cross.get("durable_event", cross.get("durable", row.get("cross_durable", False)))
                            and cross.get("authoritative", True)),
            "structural_stop": row.get("structural_stop"),
            "diagnostics": diagnostics,
        }
        rows.append((_STATE_PRIORITY.get(state, 0), score, updated, str(key), item))
    rows.sort(key=lambda value: (value[0], value[1], value[2]), reverse=True)
    return {entry[3]: entry[4] for entry in rows[:bounded_limit]}
