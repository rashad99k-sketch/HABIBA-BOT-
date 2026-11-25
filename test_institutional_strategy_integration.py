"""Targeted proof for the additive institutional strategy/reporting layer.

The new module is evidence-only: it must not become a second classifier,
authority, sizing engine or order path. These tests verify the report fields,
the fail-closed location gate, the 20x ROE convention, read-only dashboard
projection, and that the single executor/management board still own all actions.
"""
from __future__ import annotations

import inspect
import time

import pytest

import ggpbot
import naiso_unified_decision as proj
from ggp_institutional_strategy import (
    ENTRY_CLASSIFICATIONS,
    MANAGEMENT_MODE_ALIASES,
    build_entry_risk_plan,
    build_institutional_report,
    build_management_mode_report,
    normalize_management_mode,
    validate_institutional_location,
)
from ggp_profit_brain import RANGE_MODE, STANDARD_MODE, TREND_MODE, profit_target_convention
from test_naiso_unified_decision_integration import _patch_dashboard_runtime


REPORT_SECTIONS = (
    "cross_location",
    "market_regime",
    "institutional_zone",
    "structure_bias",
    "liquidity_context",
    "volume_confirmation",
    "trend_strength",
    "available_price_room",
    "structural_invalidation",
    "data_freshness",
    "analysis_confidence",
    "entry_classification",
    "location_gate",
)


def _zone(kind: str, low: float, high: float, *, status: str = "CONFIRMED",
          strength: float = 6.0) -> dict:
    return {
        "type": kind, "low": low, "high": high, "level": (low + high) / 2.0,
        "touches": 3, "reaction_atr": 0.8, "distance_atr": 0.5,
        "strength": strength, "status": status,
    }


def _base_row(*, side: str = "BUY", price: float = 100.0, now: float | None = None,
              resistances=None, supports=None, **extra) -> dict:
    ts = now if now is not None else time.time()
    row = {
        "symbol": "BTC/USDT:USDT",
        "exchange_symbol": "BTC/USDT:USDT",
        "side": side,
        "price": price,
        "atr": 1.0,
        "timeframe_ms": 900_000,
        "last_updated": ts,
        "last_update": ts,
        "data_freshness": "FRESH",
        "data_quality": "VALID_CLOSED_CANDLES",
        "closed_candle_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
        "cross_context": {
            "cross_state": "CONFIRMED_BUY_CROSS" if side == "BUY" else "CONFIRMED_SELL_CROSS",
            "durable_event": True,
            "authoritative": True,
        },
        "market_regime": "STRONG_TREND",
        "golden_zone": {"state": "CONFIRMED", "score": 82.0},
        "market_structure": {"direction": "BULLISH", "mss": "BULLISH_CHOCH", "confirmed": True},
        "liquidity": {"event": "LIQUIDITY_SWEEP", "direction": "BULLISH", "confidence": 72.0},
        "volume_ratio": 1.4,
        "volume_confirmed": True,
        "adx": 28.0,
        "adx_di": {"di_plus": 30.0, "di_minus": 17.0, "adx_slope": 1.2},
        "continuation_probability": 0.74,
        "invalidation_level": 98.0 if side == "BUY" else 102.0,
        "structural_stop": 99.0 if side == "BUY" else 101.0,
        "support_resistance": {
            "status": "VALID",
            "supports": supports or [],
            "resistances": resistances or [],
        },
    }
    row.update(extra)
    return row


def test_report_contains_required_sections_and_fails_closed_on_missing_evidence():
    report = build_institutional_report({}, side="BUY", now_ts=0.0)
    assert all(section in report for section in REPORT_SECTIONS)
    assert report["execution_authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"
    assert report["cross_location"]["state"] == "NO_CROSS"
    assert report["market_regime"]["state"] == "UNKNOWN"
    assert report["trend_strength"]["state"] == "UNKNOWN"
    assert report["data_freshness"]["state"] == "UNKNOWN"
    assert report["analysis_confidence"]["is_success_probability"] is False
    assert report["entry_classification"]["status"] == "NOT_CLASSIFIED"
    assert report["entry_classification"]["recommendation_only"] is True
    assert report["location_gate"]["state"] == "INSUFFICIENT_DATA"


@pytest.mark.parametrize("raw, expected", [
    ("IMMEDIATE", "IMMEDIATE_ENTRY"),
    ("MICRO_PULLBACK", "MICRO_PULLBACK"),
    ("PULLBACK", "PULLBACK"),
    ("RETEST", "RETEST"),
])
def test_entry_classification_aliases_are_evidence_based_and_advisory(raw, expected):
    assert expected in ENTRY_CLASSIFICATIONS
    row = _base_row(entry_behavior=raw, entry_behavior_reasons=["closed_candle_evidence"],
                    entry_behavior_confidence=0.72)
    report = build_institutional_report(row, now_ts=row["last_updated"])
    classification = report["entry_classification"]
    assert classification["status"] == "CLASSIFIED"
    assert classification["entry_type"] == expected
    assert classification["reason_codes"] == ["closed_candle_evidence"]
    assert classification["confidence"] == pytest.approx(0.72)
    assert classification["recommendation_only"] is True
    assert classification["final_authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"
    assert report["execution_authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"


def test_wait_for_structure_is_never_presented_as_an_entry_classification():
    row = _base_row(entry_behavior="WAIT_FOR_STRUCTURE", entry_behavior_confidence=0.6)
    report = build_institutional_report(row, now_ts=row["last_updated"])
    assert report["entry_classification"]["status"] == "NOT_CLASSIFIED"
    assert report["entry_classification"]["entry_type"] == ""
    assert report["analysis_confidence"]["is_success_probability"] is False


@pytest.mark.parametrize("side, price, zones, expected_state, expected_status", [
    ("BUY", 100.0, {"resistances": [_zone("RESISTANCE", 101.0, 101.4)]}, "ALLOWED", "SUFFICIENT"),
    ("BUY", 100.0, {"resistances": [_zone("RESISTANCE", 100.1, 100.4)]}, "BLOCKED", "INSUFFICIENT"),
    ("BUY", 100.8, {"resistances": [_zone("RESISTANCE", 100.5, 101.0)]}, "BLOCKED", "INSIDE_OPPOSING_ZONE"),
    ("BUY", 101.5, {"resistances": [_zone("RESISTANCE", 100.5, 100.9)]}, "ALLOWED", "CLEAR"),
    ("BUY", 100.0, {"resistances": [_zone("RESISTANCE", 100.1, 100.4, status="DEVELOPING")]}, "ALLOWED", "CLEAR"),
    ("SELL", 100.0, {"supports": [_zone("SUPPORT", 98.6, 99.0)]}, "ALLOWED", "SUFFICIENT"),
    ("SELL", 100.0, {"supports": [_zone("SUPPORT", 99.6, 99.9)]}, "BLOCKED", "INSUFFICIENT"),
    ("SELL", 100.1, {"supports": [_zone("SUPPORT", 99.6, 100.4)]}, "BLOCKED", "INSIDE_OPPOSING_ZONE"),
    ("SELL", 98.5, {"supports": [_zone("SUPPORT", 99.1, 99.5)]}, "ALLOWED", "CLEAR"),
])
def test_location_gate_uses_room_to_strong_opposing_zones(side, price, zones, expected_state, expected_status):
    row = _base_row(side=side, price=price, **zones)
    report = build_institutional_report(row, now_ts=row["last_updated"])
    room = report["available_price_room"]
    assert room["status"] == expected_status
    assert report["location_gate"]["state"] == expected_state
    if expected_state == "BLOCKED":
        assert validate_institutional_location(report)["allowed"] is False
        assert validate_institutional_location(report)["blocking_reason"]
    else:
        assert validate_institutional_location(report)["allowed"] is True


def test_stale_report_is_not_presented_as_a_fresh_location_allowance():
    now = time.time()
    row = _base_row(now=now, resistances=[_zone("RESISTANCE", 101.0, 101.4)])
    row["last_updated"] = now - 10_000.0
    report = build_institutional_report(row, now_ts=now)
    assert report["data_freshness"]["state"] == "STALE"
    assert report["location_gate"]["state"] == "INSUFFICIENT_DATA"
    assert "location_report_requires_fresh_validated_closed_data" in report["location_gate"]["reasons"]


@pytest.mark.parametrize("canonical, alias", [
    (RANGE_MODE, "SCALP"),
    (STANDARD_MODE, "STANDARD"),
    (TREND_MODE, "TREND_EXPANSION"),
])
def test_management_mode_aliases_map_back_to_one_canonical_authority(canonical, alias):
    assert MANAGEMENT_MODE_ALIASES[alias] == canonical
    report = build_management_mode_report({}, profit_mode=canonical, reason="test", evidence=["e"])
    assert report["management_mode"] == alias
    assert report["canonical_profit_mode"] == canonical
    assert report["mapping_status"] == "MAPPED_FROM_UNIFIED_PROFIT_BRAIN"
    assert report["authority"] == "TradeManagementBoard"
    assert report["mode_change_is_reporting_only"] is True
    assert normalize_management_mode(alias)["canonical_profit_mode"] == canonical
    assert normalize_management_mode("NOT_A_MODE")["management_mode"] == "STANDARD"


@pytest.mark.parametrize("mode, context, expected_gross_roe", [
    (RANGE_MODE, {"range_target_price": 100.25}, 5.0),
    (RANGE_MODE, {"range_target_price": 100.50}, 10.0),
    (RANGE_MODE, {"range_target_price": 101.00}, 20.0),
    (TREND_MODE, {"tp1_price": 102.00, "tp2_price": 103.00}, 40.0),
    (TREND_MODE, {"tp1_price": 103.00, "tp2_price": 103.00}, 60.0),
])
def test_selected_target_roe_follows_twenty_x_price_move_convention(mode, context, expected_gross_roe):
    ctx = {"side": "BUY", "entry": 100.0, **context}
    report = build_management_mode_report(ctx, profit_mode=mode, reason="test")
    assert report["selected_target_gross_roe_pct"] == pytest.approx(expected_gross_roe)
    assert report["selected_target_price_move_pct"] == pytest.approx(expected_gross_roe / 20.0)
    assert report["target_convention"]["gross_roe_multiplier"] == 20


def test_profit_target_convention_exposes_all_requested_roe_levels():
    convention = profit_target_convention()
    assert convention["gross_roe_multiplier"] == 20
    assert convention["gross_roe_pct"] == {
        "0.25": 5.0, "0.50": 10.0, "1.00": 20.0, "2.00": 40.0, "3.00": 60.0,
    }


@pytest.mark.parametrize("budget_key", ["risk_amount", "risk_budget", "risk_budget_usdt", "max_risk_usdt"])
def test_entry_risk_plan_separates_leverage_margin_risk_and_uses_sizing_budget_alias(budget_key):
    sizing = {
        "margin_required": 5.0,
        "binding_constraint": "RISK_BUDGET",
        "margin_achieved_ratio": 1.0,
        budget_key: 1.0,
    }
    plan = build_entry_risk_plan(
        side="BUY", entry_price=100.0, stop_price=99.5, quantity=1.0,
        target_price=101.0, account_equity=100.0, contract_size=1.0,
        leverage=20, sizing_decision=sizing,
    )
    assert plan["basis"] == "PRE_TRADE_ESTIMATE_NOT_REALIZED_PNL"
    assert plan["leverage"] == 20
    assert plan["leverage_applied_once"] is True
    assert plan["notional_exposure"] == pytest.approx(100.0)
    assert plan["margin_allocation"] == pytest.approx(5.0)
    assert plan["price_stop_move_pct"] == pytest.approx(0.5)
    assert plan["estimated_net_loss"] > plan["gross_stop_loss"] > 0
    assert plan["account_risk_pct"] == pytest.approx(plan["estimated_net_loss"])
    assert plan["gross_roe_pct"] == pytest.approx(20.0)
    assert 0 < plan["estimated_net_roe_pct"] < 20.0
    assert plan["estimated_net_return_pct_of_margin"] == plan["estimated_net_roe_pct"]
    assert plan["risk_reward_net"] > 0
    assert plan["idealized_liquidation_distance_pct"] == pytest.approx(5.0)
    assert "exchange value is authoritative" in plan["liquidation_distance_basis"]
    assert plan["binding_constraint"] == "RISK_BUDGET"
    assert plan["margin_achieved_ratio"] == 1.0
    assert plan["risk_budget_source"] == f"sizing_decision.{budget_key}"
    assert plan["risk_within_sizing_budget"] is True


def test_entry_risk_plan_rejects_second_leverage_and_wrong_stop_side():
    with pytest.raises(ValueError, match="exactly 20x"):
        build_entry_risk_plan(side="BUY", entry_price=100.0, stop_price=99.0, quantity=1.0, leverage=10)
    with pytest.raises(ValueError, match="protective side"):
        build_entry_risk_plan(side="BUY", entry_price=100.0, stop_price=101.0, quantity=1.0)


def test_watchlist_projection_reuses_report_and_keeps_it_nested():
    row = _base_row(entry_behavior="MICRO_PULLBACK")
    report = build_institutional_report(row, now_ts=row["last_updated"])
    row["institutional_report"] = report
    projected = proj.project_watchlist_row(row, now_ts=row["last_updated"])
    assert projected["institutional_report"] == report
    assert projected["institutional_location_gate"] == report["location_gate"]
    sections = proj.project_dashboard_sections(watchlist={"BTC": row}, now_ts=row["last_updated"])
    assert sections["institutional_report"][0] == report
    assert sections["institutional_analysis"][0]["institutional_report"] == report


def test_dashboard_renders_institutional_report_and_exposes_nested_data(monkeypatch):
    now = time.time()
    row = _base_row(now=now, entry_behavior="PULLBACK",
                    entry_behavior_reasons=["test_closed_candle_reason"],
                    resistances=[_zone("RESISTANCE", 101.0, 101.4)])
    row["institutional_report"] = build_institutional_report(row, now_ts=now)
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {"BTC": row})
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    monkeypatch.setitem(ggpbot.MEMORY, "ema9_vwap_proximity_scanner", [])
    _patch_dashboard_runtime(monkeypatch)

    client = ggpbot.app.test_client()
    html = client.get("/").get_data(as_text=True)
    assert "Institutional report:" in html
    assert "Entry classification:" in html
    assert "cross-zone:" in html
    assert "cardInstitutional.cross_location" in html
    assert "cardClassification.reason_codes" in html

    payload = client.get("/data").get_json()
    for key in ("confirmed_crosses", "watchlist_ranking", "institutional_analysis",
                "execution_readiness", "unified_decision_schema"):
        assert key not in payload
    nested_report = payload["unified_decision"]["institutional_report"][0]
    assert nested_report["entry_classification"]["entry_type"] == "PULLBACK"
    assert nested_report["entry_classification"]["reason_codes"] == ["test_closed_candle_reason"]
    assert nested_report["available_price_room"]["status"] == "SUFFICIENT"


def test_decision_feed_exposes_management_mode_target_and_risk(monkeypatch):
    risk_plan = build_entry_risk_plan(
        side="BUY", entry_price=100.0, stop_price=99.5, quantity=1.0,
        target_price=100.25, account_equity=100.0,
    )
    mode_report = build_management_mode_report(
        {"side": "BUY", "entry": 100.0, "range_target_price": 100.25},
        profit_mode=RANGE_MODE, reason="test",
    )
    patches = {
        "open": True, "current_symbol": "BTC/USDT:USDT", "side": "BUY",
        "profit_mode": RANGE_MODE, "management_mode": "SCALP",
        "management_mode_report": mode_report, "management_mode_history": [mode_report],
        "entry_risk_plan": risk_plan, "last_close_reason": "SCALP_TARGET",
    }
    for key, value in patches.items():
        monkeypatch.setitem(ggpbot.STATE, key, value)
    client = ggpbot.app.test_client()
    payload = client.get("/decision").get_json()
    active = payload["active_trade"]
    assert active["management_mode"] == "SCALP"
    assert active["selected_target_gross_roe_pct"] == pytest.approx(5.0)
    assert active["entry_risk_plan"]["leverage"] == 20
    assert active["actual_exit_reason"] == "SCALP_TARGET"
    dashboard_source = inspect.getsource(ggpbot.dashboard)
    assert "Mgmt: ${{active.management_mode" in dashboard_source
    assert "item.actual_exit_reason" in dashboard_source


def test_board_metadata_and_mode_change_event_are_reporting_only(monkeypatch):
    captured = []
    monkeypatch.setattr(ggpbot, "record_trade_management_event",
                        lambda *args, **kwargs: captured.append((args, kwargs)))
    board = ggpbot.TradeManagementBoard()
    board.evaluate({
        "indicator_decisions_allowed": False,
        "symbol": "BTC/USDT:USDT", "side": "BUY", "entry": 100.0,
        "current_price": 100.1, "atr": 1.0, "roe": 0.2,
        "remaining_qty": 1.0, "initial_qty": 1.0,
        "profit_mode": RANGE_MODE, "range_target_price": 100.25,
        "profit_mode_reason": "test", "profit_mode_evidence": ["closed_candle"],
    })
    meta = board._decision_history[-1]
    assert meta["management_mode"] == "SCALP"
    assert meta["selected_target_roe_pct"] == pytest.approx(5.0)
    assert meta["target_convention_basis"] == profit_target_convention()["basis"]

    event = ggpbot.TRADE_MANAGEMENT_LOG.record(
        "MODE_CHANGE", "DECIDED", management_mode="SCALP", selected_target_roe_pct=5.0,
    )
    assert event["icon"] == "🧭"
    assert event["label"] == "MANAGEMENT MODE CHANGE"
    assert event["management_mode"] == "SCALP"


def test_single_executor_and_single_management_authority_are_unchanged():
    entry_source = inspect.getsource(ggpbot._original_execute_entry_for_journal)
    assert "validate_institutional_location" in entry_source
    assert entry_source.index("validate_institutional_location") < entry_source.index("open_position(")
    board_source = inspect.getsource(ggpbot.TradeManagementBoard.evaluate)
    assert "build_management_mode_report" in board_source
    assert board_source.index("build_management_mode_report") < board_source.index("def decision(")
