"""Deterministic tests for the six-slot runtime state registry."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from portfolio_allocator import (
    CLASS_NEWS,
    CLASS_TECHNICAL,
    TOTAL_CAPACITY,
)
from portfolio_runtime import (
    PortfolioRuntimeError,
    PortfolioRuntime,
    MANAGER_STATE_KEYS,
)


class _Brain:
    def __init__(self):
        self.current_state = "RANGE_CHOP"
        self.unrelated = "brain-unchanged"


class _Manager:
    """Minimal stand-in for the singleton manager used by the bot."""

    def __init__(self):
        self.lifecycle_state = "IDLE"
        self.last_management_ts = 0.0
        self.last_log_ts = 0.0
        self.last_live_debug_ts = 0.0
        self.last_heavy_calc_ts = 0.0
        self.last_position_sync_ts = 0.0
        self.current_snapshot = None
        self._last_decision = None
        self.last_decision_blocked_reason = None
        self.brain = _Brain()
        self.board = {"sentinel": "board-unchanged"}
        self.event_bus = "shared-event-bus"


def _manager_state(lifecycle="LIVE", ts=1.0, brain="TREND_RIDE", decision=None, blocked=None):
    return {
        "lifecycle_state": lifecycle,
        "last_management_ts": ts,
        "last_log_ts": ts + 0.1,
        "last_live_debug_ts": ts + 0.2,
        "last_heavy_calc_ts": ts + 0.3,
        "last_position_sync_ts": ts + 0.4,
        "current_snapshot": {"symbol": "SNAP", "qty": ts},
        "_last_decision": decision,
        "last_decision_blocked_reason": blocked,
        "brain.current_state": brain,
    }


def _set_open(state, trade_state, dashboard, symbol, *, nested=1):
    state.clear()
    state.update({
        "open": True,
        "current_symbol": symbol,
        "qty": 1.0,
        "trade_state": "TREND",
        "close_verified": False,
        "last_close_verified": False,
        "nested": {"value": nested},
    })
    trade_state.clear()
    trade_state.update({"in_position": True, "symbol": symbol, "qty": 1.0})
    dashboard.clear()
    dashboard.update({
        "position": {"symbol": symbol},
        "lifecycle_state": "LIVE",
        "live_trade_mode": True,
    })


def _flat(symbol=None):
    evidence = {"verified_flat": True, "qty": 0, "source": "exchange-reconciliation"}
    if symbol is not None:
        evidence["snapshot"] = [{"symbol": symbol, "qty": 0}]
    return evidence


def _runtime(symbol="BTCUSDT", *, lock=None):
    state, trade_state, dashboard = {}, {}, {}
    manager = _Manager()
    _set_open(state, trade_state, dashboard, symbol)
    runtime = PortfolioRuntime(state, trade_state, dashboard, manager, lock=lock)
    return runtime, state, trade_state, dashboard, manager


def _active_ids(runtime):
    return tuple(item["trade_id"] for item in runtime.status()["active"])


def test_import_does_not_load_ggpbot():
    code = (
        "import sys, portfolio_runtime; "
        "assert 'ggpbot' not in sys.modules; "
        "print('IMPORT_SAFE')"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(Path(__file__).resolve().parent),
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "IMPORT_SAFE"


def test_adopt_deep_copies_state_and_focus_isolates_positions():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    first = runtime.adopt(
        "t1", "BTCUSDT",
        class_hint="EMA15_RF",
        manager_state=_manager_state(lifecycle="LIVE", ts=11, brain="TREND_ONE"),
    )
    assert first.ok, first.reason
    assert first.slot_class == CLASS_TECHNICAL

    # Mutating the source after adoption must not change the stored snapshot.
    state["nested"]["value"] = 99
    _set_open(state, trade_state, dashboard, "ETHUSDT", nested=2)
    second = runtime.adopt(
        "t2", "ETHUSDT",
        class_hint="NEWS_TRADING",
        manager_state=_manager_state(lifecycle="LIVE", ts=22, brain="TREND_TWO"),
    )
    assert second.ok, second.reason
    assert second.slot_class == CLASS_NEWS

    with runtime.focus("t1"):
        assert state["current_symbol"] == "BTCUSDT"
        assert state["nested"]["value"] == 1
        assert manager.last_management_ts == 11
        assert manager.brain.current_state == "TREND_ONE"
        state["nested"]["value"] = 7
        state["only_t1"] = True
        manager.last_management_ts = 111
        manager.brain.current_state = "T1_MUTATED"

    assert state["current_symbol"] == "ETHUSDT"
    assert state["nested"]["value"] == 2
    assert "only_t1" not in state
    assert manager.last_management_ts == 0.0
    assert manager.brain.current_state == "RANGE_CHOP"

    with runtime.focus("t2"):
        assert state["current_symbol"] == "ETHUSDT"
        assert "only_t1" not in state
        assert manager.last_management_ts == 22
        assert manager.brain.current_state == "TREND_TWO"
        state["only_t2"] = True

    with runtime.focus("t1"):
        assert state["only_t1"] is True
        assert "only_t2" not in state
        assert manager.last_management_ts == 111
        assert manager.brain.current_state == "T1_MUTATED"


def test_focus_saves_restores_and_manager_state_only_touches_allowlist():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    board = manager.board
    event_bus = manager.event_bus
    baseline = {
        "last_management_ts": 0.0,
        "last_log_ts": 0.0,
        "brain.current_state": "RANGE_CHOP",
    }
    assert runtime.adopt(
        "t1", "BTCUSDT", class_hint="TECHNICAL",
        manager_state=_manager_state(ts=5, decision="HOLD", blocked="none"),
    ).ok
    state["transient"] = "source-value"
    state_before = deepcopy(state)

    with runtime.focus("t1"):
        assert "transient" not in state
        assert manager.last_management_ts == 5
        assert manager._last_decision == "HOLD"
        assert manager.last_decision_blocked_reason == "none"
        state["inside"] = True
        manager.last_log_ts = 999.0
        manager.brain.current_state = "EXHAUSTION"

    assert state == state_before
    assert "inside" not in state
    assert manager.last_management_ts == baseline["last_management_ts"]
    assert manager.last_log_ts == baseline["last_log_ts"]
    assert manager.brain.current_state == baseline["brain.current_state"]
    assert manager.board is board and manager.board == {"sentinel": "board-unchanged"}
    assert manager.event_bus == event_bus

    with pytest.raises(PortfolioRuntimeError, match="manager_state_unknown_keys"):
        runtime.apply_manager_state({"lifecycle_state": "LIVE", "board": {"bad": True}})
    assert manager.board is board and manager.board == {"sentinel": "board-unchanged"}

    rejected = runtime.adopt(
        "t2", "ETHUSDT", class_hint="TECHNICAL",
        manager_state={"lifecycle_state": "LIVE", "event_bus": "stolen"},
    )
    assert not rejected.ok
    assert _active_ids(runtime) == ("t1",)


def test_close_during_focus_removes_only_that_position():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    _set_open(state, trade_state, dashboard, "ETHUSDT")
    assert runtime.adopt("t2", "ETHUSDT", class_hint="TECHNICAL").ok

    with runtime.focus("t1"):
        state["open"] = False
        state["close_verified"] = False
        state["last_close_verified"] = False
        state["remaining_qty"] = 0.0
        trade_state["in_position"] = False
        manager.lifecycle_state = "CLOSED"
        dashboard["lifecycle_state"] = "CLOSED"
        dashboard["live_trade_mode"] = False

    assert _active_ids(runtime) == ("t2",)
    assert runtime.status()["counts"][CLASS_TECHNICAL] == 1
    assert state["open"] is True
    assert state["current_symbol"] == "ETHUSDT"
    assert manager.lifecycle_state == "IDLE"
    with pytest.raises(PortfolioRuntimeError, match="unknown_trade_id"):
        with runtime.focus("t1"):
            pass


def test_release_requires_evidence_and_currently_focused_trade():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    _set_open(state, trade_state, dashboard, "ETHUSDT")
    assert runtime.adopt("t2", "ETHUSDT", class_hint="TECHNICAL").ok
    assert runtime.status()["focused"] == "t2"

    wrong_focus = runtime.release("t1", verified_flat=_flat("BTCUSDT"))
    assert not wrong_focus.ok and wrong_focus.reason == "not_focused_trade"
    bare = runtime.release("t2", verified_flat=True)
    assert not bare.ok and bare.reason.startswith("flat_evidence_invalid")
    non_flat = runtime.release("t2", verified_flat={"verified_flat": True, "qty": 1})
    assert not non_flat.ok and non_flat.reason.startswith("flat_evidence_invalid")
    wrong_symbol = runtime.release("t2", verified_flat=_flat("BTCUSDT"))
    assert not wrong_symbol.ok and wrong_symbol.reason == "snapshot_does_not_confirm_flat"
    assert _active_ids(runtime) == ("t1", "t2")

    with runtime.focus("t1"):
        released = runtime.release("t1", verified_flat=_flat("BTCUSDT"))
        assert released.ok and released.slot_class == CLASS_TECHNICAL

    assert _active_ids(runtime) == ("t2",)
    assert state["current_symbol"] == "ETHUSDT"
    assert not runtime.release("t1", verified_flat=_flat("BTCUSDT")).ok


def test_failed_manager_cycle_keeps_positions_and_saves_partial_state():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    _set_open(state, trade_state, dashboard, "ETHUSDT")
    assert runtime.adopt("t2", "ETHUSDT", class_hint="TECHNICAL").ok

    def failing_cycle():
        state["cycle_marker"] = state["current_symbol"]
        if state["current_symbol"] == "BTCUSDT":
            raise RuntimeError("boom")

    summary = runtime.manage_all(failing_cycle)
    assert summary.processed == ("t1", "t2")
    assert summary.closed == ()
    assert summary.failed == (("t1", "RuntimeError: boom"),)
    assert summary.failed_ids == ("t1",)
    assert _active_ids(runtime) == ("t1", "t2")

    with runtime.focus("t1"):
        assert state["cycle_marker"] == "BTCUSDT"
    with runtime.focus("t2"):
        assert state["cycle_marker"] == "ETHUSDT"

    clean = runtime.manage_all(lambda: None)
    assert clean.processed == ("t1", "t2")
    assert clean.closed == ()
    assert clean.failed == ()
    assert _active_ids(runtime) == ("t1", "t2")


def test_manage_all_closes_only_the_finalized_position():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    _set_open(state, trade_state, dashboard, "ETHUSDT")
    assert runtime.adopt("t2", "ETHUSDT", class_hint="NEWS_TRADING").ok

    def closing_cycle():
        if state["current_symbol"] == "BTCUSDT":
            state["open"] = False
            state["close_verified"] = False
            trade_state["in_position"] = False
            manager.lifecycle_state = "CLOSED"
            dashboard["lifecycle_state"] = "CLOSED"

    summary = runtime.manage_all(closing_cycle)
    assert summary.processed == ("t1", "t2")
    assert summary.closed == ("t1",)
    assert summary.failed == ()
    assert _active_ids(runtime) == ("t2",)
    assert runtime.status()["counts"] == {
        CLASS_TECHNICAL: 0, CLASS_NEWS: 1, "TOTAL": 1,
    }


def test_adopt_rejects_malformed_duplicate_unknown_and_over_capacity():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    bad_ids = ("", "bad id", "!!!", None, 123, "x" * 129)
    for bad_id in bad_ids:
        result = runtime.adopt(bad_id, "BTCUSDT", class_hint="TECHNICAL")
        assert not result.ok, bad_id
    assert not runtime.adopt("good", "bad symbol", class_hint="TECHNICAL").ok
    assert runtime.status()["active"] == []

    assert runtime.adopt("t0", "SYM0", class_hint="TECHNICAL").ok
    duplicate = runtime.adopt("t0", "OTHER", class_hint="TECHNICAL")
    assert not duplicate.ok and duplicate.reason == "duplicate_trade_id"
    assert not runtime.adopt("unknown", "SYM", class_hint="MYSTERY_SOURCE").ok
    assert not runtime.adopt("labelled", "SYM", class_hint={"slot_class": CLASS_NEWS}).ok

    alternate, alternate_state, alternate_ts, alternate_dash, alternate_manager = _runtime(
        "NEWSCOIN"
    )
    assert alternate.adopt("n1", "NEWSCOIN", class_hint="NEWS").ok
    second_news = alternate.adopt("n2", "NEWS2", class_hint="NEWS_TRADING")
    assert not second_news.ok
    assert second_news.reason == "class_capacity_exceeded"

    for index in range(1, 5):
        result = runtime.adopt(f"t{index}", f"SYM{index}", class_hint="EMA15")
        assert result.ok, result.reason
    sixth_technical = runtime.adopt("t5", "SYM5", class_hint="CONFLUENCE")
    assert not sixth_technical.ok
    assert sixth_technical.reason == "class_capacity_exceeded"

    news = runtime.adopt("news-1", "NEWSCOIN", class_hint="HEADLINE")
    assert news.ok and news.slot_class == CLASS_NEWS
    seventh = runtime.adopt("t6", "SYM6", class_hint="TECHNICAL")
    assert not seventh.ok
    assert seventh.reason == "total_capacity_exceeded"
    eighth = runtime.adopt("news-2", "NEWS2", class_hint="NEWS_TRADING")
    assert not eighth.ok
    assert eighth.reason == "total_capacity_exceeded"
    assert runtime.status()["counts"] == {
        CLASS_TECHNICAL: 5, CLASS_NEWS: 1, "TOTAL": TOTAL_CAPACITY,
    }


def test_adopt_fails_closed_on_inconsistent_registry_counts():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    runtime._positions["t1"].slot_class = "NEWS_TRADING"
    runtime._positions["t1"].slot_index = 2  # outside NEWS capacity
    rejected = runtime.adopt("t2", "ETHUSDT", class_hint="TECHNICAL")
    assert not rejected.ok
    assert rejected.reason.startswith("state_invalid")
    assert runtime.status()["ok"] is False
    assert _active_ids(runtime) == ()


def test_status_is_compact_deterministic_and_does_not_mutate_sources():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    before = (deepcopy(state), deepcopy(trade_state), deepcopy(dashboard))
    assert runtime.adopt("t-b", "BTCUSDT", class_hint="TECHNICAL").ok
    assert runtime.adopt("t-a", "ETHUSDT", class_hint="NEWS_TRADING").ok
    assert (state, trade_state, dashboard) == before

    first = runtime.status()
    second = runtime.status()
    assert first == second
    assert set(first) == {"ok", "focused", "counts", "capacity", "active"}
    assert tuple(item["trade_id"] for item in first["active"]) == ("t-a", "t-b")
    assert all(
        set(item) == {"trade_id", "symbol", "slot_class", "slot_index"}
        for item in first["active"]
    )
    assert first["counts"] == {CLASS_TECHNICAL: 1, CLASS_NEWS: 1, "TOTAL": 2}
    assert first["capacity"]["TOTAL"] == TOTAL_CAPACITY
    assert MANAGER_STATE_KEYS == frozenset(_manager_state())

    first["active"].clear()
    first["counts"]["TOTAL"] = 99
    assert runtime.status() == second
    assert (state, trade_state, dashboard) == before


def test_focus_rejects_unknown_and_nested_cycles():
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT")
    with pytest.raises(PortfolioRuntimeError, match="unknown_trade_id"):
        with runtime.focus("missing"):
            pass
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok
    _set_open(state, trade_state, dashboard, "ETHUSDT")
    assert runtime.adopt("t2", "ETHUSDT", class_hint="TECHNICAL").ok

    with runtime.focus("t1"):
        with pytest.raises(PortfolioRuntimeError, match="nested_focus_rejected"):
            with runtime.focus("t2"):
                pass
        foreign = runtime.adopt("t3", "SYM3", class_hint="TECHNICAL")
        assert not foreign.ok and foreign.reason == "adopt_during_foreign_focus"
        assert state["current_symbol"] == "BTCUSDT"
    assert _active_ids(runtime) == ("t1", "t2")


def test_concurrent_adopt_and_focus_cannot_corrupt_state():
    external_lock = threading.Lock()
    runtime, state, trade_state, dashboard, manager = _runtime("BTCUSDT", lock=external_lock)
    assert runtime.adopt("t1", "BTCUSDT", class_hint="TECHNICAL").ok

    focus_entered = threading.Event()
    adopt_attempted = threading.Event()
    proceed = threading.Event()
    results = []

    def focuser():
        with runtime.focus("t1"):
            state["marker"] = "inside"
            focus_entered.set()
            assert proceed.wait(5)
            assert state["marker"] == "inside"

    def adopter():
        adopt_attempted.set()
        results.append(runtime.adopt("t2", "ETHUSDT", class_hint="NEWS_TRADING"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        focus_future = pool.submit(focuser)
        assert focus_entered.wait(5)
        adopt_future = pool.submit(adopter)
        assert adopt_attempted.wait(5)
        # The focus cycle still owns the shared lock; the adopt cannot interleave.
        assert external_lock.acquire(blocking=False) is False
        proceed.set()
        focus_future.result(timeout=5)
        adopt_future.result(timeout=5)

    assert results[0].ok, results[0].reason
    assert _active_ids(runtime) == ("t1", "t2")
    assert state.get("marker") != "inside"
    with runtime.focus("t1"):
        assert state["marker"] == "inside"
    with runtime.focus("t2"):
        assert "marker" not in state
