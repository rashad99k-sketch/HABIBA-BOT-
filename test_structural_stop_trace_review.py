"""Pre-merge proof for the structural_stop trace and UNKNOWN diagnosis.

These are test-only assertions. No stop formula, risk rule, entry condition,
Watchlist merge order or execution path is modified.
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import ggpbot
import ggp_institutional_bridge as bridge
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
    assert 'q.stop_valid ? "VALID" : "STOP_INVALID_NOT_READY"' in html

    exposed = payload["watchlist"]["IOTX"]
    assert exposed["structural_stop"] is None
    assert exposed["diagnostics"]["stop_valid"] is False
    assert exposed["diagnostics"]["stop_validation"] == "missing_or_non_positive"
    assert exposed["stop_status"] == "missing_or_non_positive"


def test_bridge_and_execution_guards_preserve_the_existing_stop_contract():
    bridge_source = (SOURCE_ROOT / "ggp_institutional_bridge.py").read_text(encoding="utf-8")
    bot_source = (SOURCE_ROOT / "ggpbot.py").read_text(encoding="utf-8")

    assert '"structural_stop": decision.structural_stop' in bridge_source
    assert "_current_structural_stop(best, key, candidate)" in bridge_source
    assert 'if sl is None or not np.isfinite(float(sl)) or float(sl) <= 0:' in bot_source
    assert 'item["rejection_reason"] = "structural_stop_missing"' in bot_source
    assert 'raise ValueError("structural stop must be finite and positive")' in bot_source


def _stale_stop_memory():
    return {
        "scanner_v2_buy": [{"symbol": "BTC/USDT:USDT", "side": "BUY", "score": 8}],
        "watchlist": {
            "BTC": {
                "symbol": "BTC",
                "exchange_symbol": "BTC/USDT:USDT",
                "side": "BUY",
                "decision_state": "ENTRY_READY",
                "event_id": "old-event",
                "structural_stop": 95.0,
                "first_seen": FIXED_NOW - 100.0,
                "legacy_marker": "must-survive-merge",
            },
        },
    }


def _current_analysis(stop_marker="omit", **overrides):
    row = {
        "symbol": "BTC",
        "exchange_symbol": "BTC/USDT:USDT",
        "side": "BUY",
        "signal_direction": "BUY",
        "decision_state": "ENTRY_READY",
        "decision_reason": "current_analysis",
        "event_id": "current-event",
        "origin_timestamp": 1_790_000_000_000,
        "score": 80.0,
        "price": 100.0,
        "golden_zone": {"score": 70},
        "evidence": [],
        "data_quality": "VALID_CLOSED_CANDLES",
        "data_freshness": "FRESH",
        "last_update": time.time(),
    }
    if stop_marker != "omit":
        row["structural_stop"] = stop_marker
    row.update(overrides)
    return row


def _run_watchlist_merge(analysis_row):
    memory = _stale_stop_memory()

    def fake_analyze(_symbol, side, _frame, now_ms=None):
        result = dict(analysis_row, side=side)
        if side != str(analysis_row.get("side", "BUY")).upper():
            result.update({
                "decision_state": "WAITING_FOR_CONFIRMATION",
                "decision_reason": "opposite_side_context",
                "event_id": "opposite-side-event",
                "structural_stop": None,
                "score": 10.0,
            })
        return result

    original = bridge.analyze_symbol
    bridge.analyze_symbol = fake_analyze
    try:
        return bridge.update_unified_watchlist(
            memory, lambda _symbol: _frame_stub(), max_items=10, batch_size=10,
        )
    finally:
        bridge.analyze_symbol = original


def _frame_stub():
    return pd.DataFrame()


def test_previous_stop_cannot_be_inherited_when_current_analysis_omits_it():
    out = _run_watchlist_merge(_current_analysis(
        decision_state="WAITING_FOR_CONFIRMATION",
        decision_reason="current_waiting_analysis",
        event_id="current-waiting-event",
    ))
    row = out["BTC"]
    readiness = row["decision_context"]["entry_readiness"]

    assert row["structural_stop"] is None
    assert row["structural_stop_status"] == "missing_or_non_positive"
    assert row["event_id"] == "current-waiting-event"
    assert row["first_seen"] == pytest.approx(FIXED_NOW - 100.0)
    assert row["legacy_marker"] == "must-survive-merge"
    assert "STRUCTURAL_STOP_UNKNOWN" in readiness["missing"]
    assert readiness["sl_valid"] is False
    assert readiness["state"] == "NOT_READY"


@pytest.mark.parametrize("stop, expected_status, overrides", [
    (None, "missing_or_non_positive", {}),
    (0.0, "missing_or_non_positive", {}),
    (-1.0, "missing_or_non_positive", {}),
    (float("nan"), "missing_or_non_positive", {}),
    (float("inf"), "missing_or_non_positive", {}),
    (101.0, "stop_wrong_side", {}),
    (98.0, "stale_market_snapshot", {"data_freshness": "STALE_DATA"}),
    (98.0, "exchange_symbol_mismatch", {"exchange_symbol": "ETH/USDT:USDT"}),
    (98.0, "missing_event_identity", {"event_id": None}),
])
def test_invalid_current_stops_never_reuse_the_previous_value(stop, expected_status, overrides):
    out = _run_watchlist_merge(_current_analysis(stop, **overrides))
    row = out["BTC"]
    readiness = row["decision_context"]["entry_readiness"]

    assert row["structural_stop"] is None
    assert row["structural_stop_status"] == expected_status
    assert row["event_id"] != "old-event"
    assert "STRUCTURAL_STOP_UNKNOWN" in readiness["missing"]
    assert readiness["sl_valid"] is False
    assert readiness["state"] == "NOT_READY"


def test_verified_current_stop_is_preserved_without_falling_back():
    out = _run_watchlist_merge(_current_analysis(98.0))
    row = out["BTC"]

    assert row["structural_stop"] == pytest.approx(98.0)
    assert row["structural_stop_status"] == "verified_current_analysis"
    assert row["decision_context"]["entry_readiness"]["sl_valid"] is True


def test_stops_and_provenance_are_isolated_across_symbols():
    memory = {
        "scanner_v2_buy": [
            {"symbol": "BTC/USDT:USDT", "side": "BUY", "score": 8},
            {"symbol": "ETH/USDT:USDT", "side": "BUY", "score": 8},
        ],
        "watchlist": {
            "BTC": {"symbol": "BTC", "exchange_symbol": "BTC/USDT:USDT",
                    "side": "BUY", "structural_stop": 1.0, "legacy_btc": True},
            "ETH": {"symbol": "ETH", "exchange_symbol": "ETH/USDT:USDT",
                    "side": "BUY", "structural_stop": 2.0, "legacy_eth": True},
        },
    }
    stops = {"BTC": 98.0, "ETH": 198.0}
    prices = {"BTC": 100.0, "ETH": 200.0}

    def fake_analyze(symbol, side, _frame, now_ms=None):
        key = "BTC" if "BTC" in symbol else "ETH"
        if side != "BUY":
            return _current_analysis(
                None, symbol=key, exchange_symbol=symbol, price=prices[key],
                decision_state="WAITING_FOR_CONFIRMATION", decision_reason="opposite_hypothesis",
                event_id=f"{key}-opposite",
            )
        return _current_analysis(
            stops[key], symbol=key, exchange_symbol=symbol, price=prices[key],
            event_id=f"{key}-current",
        )

    original = bridge.analyze_symbol
    bridge.analyze_symbol = fake_analyze
    try:
        out = bridge.update_unified_watchlist(
            memory, lambda _symbol: _frame_stub(), max_items=10, batch_size=10,
        )
    finally:
        bridge.analyze_symbol = original

    assert out["BTC"]["structural_stop"] == pytest.approx(98.0)
    assert out["ETH"]["structural_stop"] == pytest.approx(198.0)
    assert out["BTC"]["event_id"] == "BTC-current"
    assert out["ETH"]["event_id"] == "ETH-current"
    assert out["BTC"]["legacy_btc"] is True and "legacy_eth" not in out["BTC"]
    assert out["ETH"]["legacy_eth"] is True and "legacy_btc" not in out["ETH"]
    assert out["BTC"]["decision_context"]["source_snapshot_id"] != out["ETH"]["decision_context"]["source_snapshot_id"]
