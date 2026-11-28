import pytest

from habiba_demo import DemoLifecycleEngine


def test_preview_contains_independent_confirmed_cross_events_and_all_required_scenarios():
    state = DemoLifecycleEngine(seed=7).snapshot()
    ids = [event["id"] for event in state["events"]]
    scenarios = {event["scenario"] for event in state["events"]}
    assert len(ids) == len(set(ids))
    assert all(event["crossStatus"] == "CONFIRMED" for event in state["events"])
    assert {"ready_trend", "ready_scalp", "low_adx", "bad_zone", "waiting", "expired"}.issubset(scenarios)
    assert len(state["decisionLog"]) == len(state["events"])


def test_adx_below_18_is_an_absolute_block_and_score_cannot_override_it():
    state = DemoLifecycleEngine(seed=8).snapshot()
    low_adx = next(event for event in state["events"] if event["scenario"] == "low_adx")
    adx_gate = next(gate for gate in low_adx["licence"] if gate["code"] == "base_adx_18")
    assert low_adx["adx"] < 18
    assert adx_gate["outcome"] == "FAIL"
    assert low_adx["stage"] == "REJECTED"
    assert low_adx["entryReady"] is False
    assert low_adx["authorized"] is False
    assert "BASE ADX" in low_adx["rejectionReason"]


def test_b_specific_wait_does_not_turn_into_global_a_gate_failure():
    state = DemoLifecycleEngine(seed=9).snapshot()
    waiting = next(event for event in state["events"] if event["scenario"] == "waiting")
    assert waiting["stage"] == "INSTITUTIONAL_ANALYSIS"
    assert all(gate["outcome"] == "PASS" for gate in waiting["licence"])
    assert waiting["authorized"] is False
    assert waiting["entryReady"] is False
    assert "post-MSS retest" in waiting["waitReason"]
    assert all(gate["blocksEntry"] is False for gate in state["gateCatalog"]["B"])
    assert all(gate["blocksEntry"] is False for gate in state["gateCatalog"]["C"])


def test_rejected_zone_cannot_be_authorized():
    state = DemoLifecycleEngine(seed=10).snapshot()
    invalid = next(event for event in state["events"] if event["scenario"] == "bad_zone")
    assert invalid["zone"]["decision"] == "REJECTED"
    assert invalid["stage"] == "REJECTED"
    assert invalid["entryReady"] is False
    assert invalid["authorized"] is False
    assert any(gate["code"] == "location_not_invalid" and gate["outcome"] == "FAIL" for gate in invalid["licence"])


def test_execution_requires_all_eight_steps_and_opens_only_a_simulated_position():
    engine = DemoLifecycleEngine(seed=11)
    state = engine.force_cycle()
    opened = [event for event in state["events"] if event["stage"] == "POSITION_OPEN"]
    assert opened
    for event in opened:
        assert event["authorized"] is True
        assert event["entryReady"] is True
        assert event["simulatedOrderId"].startswith("SIM-")
        assert all(step["status"] == "COMPLETE" for step in event["executionSteps"])
    assert all(position["source"] == "DEMO_SIMULATION_ONLY" for position in state["positions"])


def test_trend_and_scalp_profit_policy_is_simulated_and_reconciled():
    engine = DemoLifecycleEngine(seed=12)
    engine.force_cycle()
    for _ in range(4):
        state = engine.step()
    closed = state["closedPositions"]
    scalp = next(position for position in closed if position["tradeMode"] == "SCALP")
    trend = next(position for position in closed if position["tradeMode"] == "TREND")
    assert scalp["closeReason"] == "SCALP_TP1_FULL_CLOSE"
    assert trend["tp1Hit"] is True and trend["tp2Hit"] is True
    assert trend["closeReason"] == "TREND_TP2_REMAINDER_CLOSE"
    assert scalp["reconciliation"] == trend["reconciliation"] == "SIMULATED_CLOSED_CONFIRMED"


def test_demo_flask_routes_are_separate_and_reject_unknown_controls():
    flask = pytest.importorskip("flask")
    from flask import Flask
    from habiba_demo_web import demo_blueprint
    app = Flask(__name__)
    app.register_blueprint(demo_blueprint)
    app.config.update(TESTING=True)
    client = app.test_client()
    response = client.get("/lifecycle/api/state")
    assert response.status_code == 200
    assert response.get_json()["mode"] == "DEMO_SIMULATION_ONLY"
    page = client.get("/lifecycle/")
    assert page.status_code == 200
    assert b"DEMO ONLY" in page.data
    bad = client.post("/lifecycle/api/control", json={"action": "place_live_order"})
    assert bad.status_code == 400
    assert "unsupported_demo_action" in bad.get_json()["error"]


def test_scanner_score_never_overrides_failed_adx_gate():
    engine = DemoLifecycleEngine(seed=13)
    event = next(event for event in engine._events.values() if event["scenario"] == "low_adx")
    event["scoredByScanner"] = 9999.0
    assert event["stage"] == "REJECTED"
    assert event["authorized"] is False
    assert event["entryReady"] is False


def test_missing_sl_blocks_execution_even_after_authorization_was_granted():
    engine = DemoLifecycleEngine(seed=14)
    event = engine._new_event("ready_trend", "ADA/USDT")
    engine._advance_event(event)  # CROSS_CONFIRMED -> INSTITUTIONAL_ANALYSIS
    engine._advance_event(event)  # A gates pass -> READY
    assert event["authorized"] is True
    event["stopLoss"] = None
    engine._advance_event(event)
    assert event["stage"] == "REJECTED"
    assert event["rejectionCode"] == "MISSING_STOP_LOSS"
    assert event["simulatedOrderId"] is None
    assert event["entryReady"] is False


def test_duplicate_demo_order_is_blocked_before_execution_progresses():
    engine = DemoLifecycleEngine(seed=15)
    event = engine._new_event("ready_trend", "LINK/USDT")
    engine._advance_event(event)
    engine._advance_event(event)
    assert event["stage"] == "READY"
    event["simulatedOrderId"] = "SIM-ALREADY-SUBMITTED"
    engine._advance_event(event)
    assert event["stage"] == "REJECTED"
    assert event["rejectionCode"] == "DUPLICATE_ORDER_BLOCKED"


def test_newly_discovered_cross_event_is_visible_before_institutional_analysis():
    engine = DemoLifecycleEngine(seed=16)
    before = {event["id"] for event in engine.snapshot()["events"]}
    engine.step()  # cycle 2
    state = engine.step()  # cycle 3 creates a new, unadvanced Cross Event
    fresh = [event for event in state["events"] if event["id"] not in before]
    assert len(fresh) == 1
    assert fresh[0]["stage"] == "CROSS_CONFIRMED"
    assert fresh[0]["watchStatus"] == "CROSS_CONFIRMED"
    next_state = engine.step()
    advanced = next(event for event in next_state["events"] if event["id"] == fresh[0]["id"])
    assert advanced["stage"] == "INSTITUTIONAL_ANALYSIS"


def test_demo_soak_preserves_event_identity_and_execution_invariants_over_120_steps():
    engine = DemoLifecycleEngine(seed=28062026)
    state = engine.snapshot()
    for _ in range(120):
        state = engine.step()

    event_ids = [event["id"] for event in state["events"]]
    assert len(event_ids) == len(set(event_ids))

    for event in state["events"]:
        if event["stage"] in {"READY", "EXECUTING", "POSITION_OPEN", "COMPLETED"}:
            assert event["authorized"] is True
            assert event["entryReady"] is True
            assert all(gate["outcome"] == "PASS" for gate in event["licence"])
        if event["stage"] in {"POSITION_OPEN", "COMPLETED"}:
            assert event["stopLoss"] is not None
            assert event["simulatedOrderId"].startswith("SIM-")
            assert len(event["executionSteps"]) == 8
            assert all(step["status"] == "COMPLETE" for step in event["executionSteps"])

    positions = state["positions"] + state["closedPositions"]
    assert all(position["source"] == "DEMO_SIMULATION_ONLY" for position in positions)
    assert all(position["id"].startswith("SIM-POS-") for position in positions)
