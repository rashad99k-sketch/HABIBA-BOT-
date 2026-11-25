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
    bounded_limit = max(1, min(int(limit), 60))
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
            "stop_status": _first_text(row.get("structural_stop_status"), decision.get("stop_validation_status"), default="UNKNOWN"),
            "freshness": _first_text(row.get("snapshot_freshness"), row.get("data_freshness"), row.get("data_quality"), default="UNKNOWN"),
            "updated_at": updated, "cross": _first_text(cross.get("state"), default="NO_CROSS"),
            "durable": bool(cross.get("durable_event", cross.get("durable", row.get("cross_durable", False)))
                            and cross.get("authoritative", True)),
        }
        rows.append((_STATE_PRIORITY.get(state, 0), score, updated, str(key), item))
    rows.sort(key=lambda value: (value[0], value[1], value[2]), reverse=True)
    return {entry[3]: entry[4] for entry in rows[:bounded_limit]}
