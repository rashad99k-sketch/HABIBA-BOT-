"""Pre-merge proof for the structural_stop trace and UNKNOWN diagnosis.

These are test-only assertions. No stop formula, risk rule, entry condition,
Watchlist merge order or execution path is modified.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

import ggpbot
import naiso_unified_decision as proj
from roro_institutional import (
    DecisionState, InstitutionalDecision, StructuralStopLossEngine,
    UnifiedEntryAuthority,
)
from test_naiso_unified_decision_integration import _patch_dashboard_runtime


FIXED_NOW = 1_800_000_000.0
SOURCE_ROOT = Path(__file__).resolve().parent


def _projected_row(stop, state="ENTRY_READY"):
    row = {
        "symbol": "IOTX",
        "exchange_symbol": "IOTX/USDT:USDT",
        "side": "BUY",
        "direction": "BUY",
        "decision_state": state,
        "decision_reason": "waiting_for_retest_trigger_close" if stop is None else "closed_retest_confirmed",
        "structural_stop": stop,
        "price": 100.0,
        "data_quality": "VALID_CLOSED_CANDLES",
        "evidence": ["closed_ema9_vwap_cross"],
        "last_update": FIXED_NOW,
        "last_updated": FIXED_NOW,
    }
    projection = proj.project_watchlist_row(row, now_ts=FIXED_NOW)
    # Match update_unified_watchlist: projection is additive to the Watchlist row.
    return {**row, **projection}


def test_stop_engine_and_authority_explain_absent_stop_at_source():
    valid_stop, valid_reason = StructuralStopLossEngine.calculate(
        "BUY", "TREND_CONTINUATION_RETEST", 100.0, 98.0, 1.0, max_risk_pct=5.0,
    )
    assert valid_stop is not None
    assert 0.0 < valid_stop < 100.0
    assert valid_reason.startswith("structural_stop:")

    invalid_stop, invalid_reason = StructuralStopLossEngine.calculate(
        "BUY", "TREND_CONTINUATION_RETEST", 100.0, 98.0, 0.0, max_risk_pct=5.0,
    )
    assert invalid_stop is None
    assert invalid_reason == "invalid_stop_inputs"

    waiting = InstitutionalDecision(
        DecisionState.WAITING_FOR_RETEST.value,
        "TREND_CONTINUATION_RETEST",
        "BUY",
        "waiting_for_retest_trigger_close",
        evidence=("closed_ema9_vwap_cross",),
    )
    decision = UnifiedEntryAuthority().decide(waiting, waiting, risk_ok=True)
    assert decision.state == DecisionState.WAITING_FOR_RETEST.value
    assert decision.structural_stop is None

    projected = _projected_row(decision.structural_stop, decision.state)
    readiness = projected["decision_context"]["entry_readiness"]
    assert "STRUCTURAL_STOP_UNKNOWN" in readiness["missing"]
    assert readiness["sl_valid"] is False
    assert readiness["state"] == "NOT_READY"
    assert readiness["authoritative_gate"] == "UNCHANGED_EXECUTE_ENTRY"


@pytest.mark.parametrize("stop,is_valid", [
    (None, False),
    (0.0, False),
    (-1.0, False),
    (float("nan"), False),
    (float("inf"), False),
    (98.0, True),
])
def test_stop_validity_matrix_is_absent_or_invalid_not_a_lost_value(stop, is_valid):
    readiness = proj.project_execution_readiness(
        {"structural_stop": stop}, decision_state="ENTRY_READY",
    )
    assert readiness["sl_valid"] is is_valid
    assert ("STRUCTURAL_STOP_UNKNOWN" in readiness["missing"]) is not is_valid
    assert readiness["state"] == ("ENTRY_READY" if is_valid else "NOT_READY")
    assert readiness["conditions_complete"] is is_valid


def test_dashboard_and_data_expose_the_same_missing_stop_reason(monkeypatch):
    row = _projected_row(None, DecisionState.WAITING_FOR_RETEST.value)
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {"IOTX": row})
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    _patch_dashboard_runtime(monkeypatch)

    client = ggpbot.app.test_client()
    html = client.get("/").get_data(as_text=True)
    payload = client.get("/data").get_json()

    assert 'Number.isFinite(Number(w.structural_stop)) && Number(w.structural_stop) > 0' in html
    assert ': "UNKNOWN"' in html
    assert 'cardStopState = cardReadiness.sl_valid === true ? "VALID" : "STOP_INVALID_NOT_READY"' in html

    exposed = payload["watchlist"]["IOTX"]
    assert exposed["structural_stop"] is None
    readiness = exposed["decision_context"]["entry_readiness"]
    assert "STRUCTURAL_STOP_UNKNOWN" in readiness["missing"]
    assert readiness["sl_valid"] is False
    assert readiness["state"] == "NOT_READY"


def test_bridge_and_execution_guards_preserve_the_existing_stop_contract():
    bridge_source = (SOURCE_ROOT / "ggp_institutional_bridge.py").read_text(encoding="utf-8")
    bot_source = (SOURCE_ROOT / "ggpbot.py").read_text(encoding="utf-8")

    assert '"structural_stop": decision.structural_stop' in bridge_source
    assert 'if sl is None or not np.isfinite(float(sl)) or float(sl) <= 0:' in bot_source
    assert 'item["rejection_reason"] = "structural_stop_missing"' in bot_source
    assert 'raise ValueError("structural stop must be finite and positive")' in bot_source
