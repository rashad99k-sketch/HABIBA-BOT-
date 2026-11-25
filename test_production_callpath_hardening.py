"""Production call-path safety regressions.

These tests exercise the actual Watchlist worker publication boundary and the
non-idempotent BingX close-order path. They do not alter strategy decisions.
"""
from __future__ import annotations

import inspect
import threading

import pytest

import ggpbot


def test_late_watchlist_worker_cannot_publish_after_generation_timeout(monkeypatch):
    baseline = {"BTC/USDT": {"symbol": "BTC/USDT", "decision_state": "WAITING_FOR_RETEST",
                             "last_update": 100.0}}
    memory = {"watchlist": baseline.copy(), "_unified_watchlist_refresh_ok": False}
    monkeypatch.setattr(ggpbot, "MEMORY", memory)

    state = ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        state.update({
            "in_progress": True, "generation": 41, "timed_out": False,
            "restart_requested": False, "phase": "TEST", "worker_name": "test-worker",
            "last_error": "",
        })

    entered = threading.Event()
    release = threading.Event()

    def fake_cross_sync(**_kwargs):
        return []

    def blocked_update(staged_memory, *_args, **_kwargs):
        staged_memory["watchlist"]["BTC/USDT"] = {
            "symbol": "BTC/USDT", "decision_state": "ENTRY_READY", "last_update": 200.0,
        }
        staged_memory["watchlist"]["ETH/USDT"] = {
            "symbol": "ETH/USDT", "decision_state": "ENTRY_READY", "last_update": 200.0,
        }
        entered.set()
        assert release.wait(2.0), "test did not release the simulated late worker"
        return staged_memory["watchlist"]

    monkeypatch.setattr(ggpbot, "_sync_confirmed_cross_events", fake_cross_sync)
    monkeypatch.setattr(ggpbot, "update_unified_watchlist", blocked_update)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)

    worker = threading.Thread(
        target=ggpbot._run_unified_institutional_watchlist_refresh,
        args=(41,),
        daemon=True,
    )
    worker.start()
    assert entered.wait(2.0), "worker did not reach the controlled analysis point"

    # Simulate the scheduler declaring this generation stalled and fencing it.
    with state["lock"]:
        state["timed_out"] = True
        state["generation"] = 42
        state["restart_requested"] = True
    memory["_unified_watchlist_refresh_ok"] = False

    release.set()
    worker.join(2.0)
    assert not worker.is_alive()

    assert memory["watchlist"] == baseline
    assert "ETH/USDT" not in memory["watchlist"]
    assert memory["_unified_watchlist_refresh_ok"] is False
    with state["lock"]:
        assert state["generation"] == 42
        assert state["timed_out"] is True
        assert state["in_progress"] is True


def test_watchlist_worker_publishes_only_when_generation_is_current(monkeypatch):
    memory = {
        "watchlist": {
            "BTC/USDT": {"symbol": "BTC/USDT", "decision_state": "WAITING_FOR_RETEST",
                         "last_update": 100.0},
        },
        "_unified_watchlist_refresh_ok": False,
    }
    monkeypatch.setattr(ggpbot, "MEMORY", memory)
    state = ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        state.update({
            "in_progress": True, "generation": 51, "timed_out": False,
            "restart_requested": False, "phase": "TEST", "worker_name": "test-worker",
            "last_error": "",
        })

    def fake_cross_sync(**_kwargs):
        return []

    def update(staged_memory, *_args, **_kwargs):
        staged_memory["watchlist"]["BTC/USDT"] = {
            "symbol": "BTC/USDT", "decision_state": "ENTRY_READY", "last_update": 200.0,
        }
        staged_memory["watchlist"]["ETH/USDT"] = {
            "symbol": "ETH/USDT", "decision_state": "ENTRY_READY", "last_update": 200.0,
        }
        return staged_memory["watchlist"]

    monkeypatch.setattr(ggpbot, "_sync_confirmed_cross_events", fake_cross_sync)
    monkeypatch.setattr(ggpbot, "update_unified_watchlist", update)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)

    ggpbot._run_unified_institutional_watchlist_refresh(51)

    assert memory["watchlist"]["BTC/USDT"]["decision_state"] == "ENTRY_READY"
    assert memory["watchlist"]["ETH/USDT"]["decision_state"] == "ENTRY_READY"
    assert memory["_unified_watchlist_refresh_ok"] is True
    with state["lock"]:
        assert state["generation"] == 51
        assert state["in_progress"] is False
        assert state["timed_out"] is False


def test_live_trade_execution_service_instances_use_shared_api_governor():
    # Runtime construction sites must inject safe_api_call. Without this, the
    # service's default api_call path invokes CCXT directly and bypasses the
    # global scheduler/resilience policy.
    for function in (
        ggpbot.open_position,
        ggpbot._move_native_stop_verified,
        ggpbot._repair_native_stop_from_journal,
    ):
        source = inspect.getsource(function)
        if "TradeExecutionService(" in source:
            assert "api_caller=safe_api_call" in source


def test_verified_market_close_uses_one_shot_governed_order_submission():
    source = inspect.getsource(ggpbot._verified_market_reduce_impl)
    assert "safe_api_call(ex.create_order" in source
    assert "order = ex.create_order(" not in source


def test_ambiguous_close_order_is_not_retried_by_safe_api_call(monkeypatch):
    calls = []
    class Probe:
        max_attempts = 4
        def before_call(self, _method):
            return None
        def count_request(self):
            return None
        def record_failure(self, _method, _exc):
            return None
        def record_success(self, _method):
            return None

    def create_order(*_args, **_kwargs):
        calls.append(1)
        raise TimeoutError("exchange may have accepted order")

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", Probe())
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setitem(ggpbot.MEMORY, "recovery_status", "READY")
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(create_order, "BTC/USDT:USDT", "market", "sell", 1.0)

    assert calls == [1]
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.MEMORY["recovery_status"] == "ORDER_SUBMISSION_UNCERTAIN"
