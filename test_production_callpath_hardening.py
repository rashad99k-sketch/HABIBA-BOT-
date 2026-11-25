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
        ggpbot._repair_native_sl_from_journal,
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


def test_main_loop_dispatches_entry_revalidation_off_foreground_thread():
    source = inspect.getsource(ggpbot._original_main_loop_sniper)
    assert 'dispatch_scanner_task(' in source
    assert '"institutional_entry_revalidation", smart_opportunity_selection' in source
    assert 'smart_opportunity_selection()' not in source


def test_entry_revalidation_reports_real_candidate_progress():
    source = inspect.getsource(ggpbot.smart_opportunity_selection)
    assert 'candidate_cycle_start' in source
    assert 'candidate_ohlcv_complete' in source
    assert 'candidate_analysis_complete' in source
    assert 'entry_execution_complete' in source


def test_partial_entry_fill_keeps_execution_lock_and_requires_reconciliation(monkeypatch):
    from types import SimpleNamespace
    from bingx_execution import ExecutionState

    class Exchange:
        def market(self, _symbol):
            return {"contractSize": 1.0}
        def amount_to_precision(self, _symbol, amount):
            return str(amount)
        def fetch_balance(self):
            return {"free": {"USDT": 1000.0}}
        def fetch_ticker(self, _symbol):
            return {"last": 100.0}

    stop_calls = []
    class PartialFillService:
        def __init__(self, *_args, **_kwargs):
            pass
        def submit_market_entry(self, **_kwargs):
            return SimpleNamespace(
                state=ExecutionState.PARTIAL_FILL,
                trade_id="partial-trade",
                client_order_id="partial-client",
                exchange_order_id="partial-order",
                requested_qty=1.0,
                filled_qty=0.4,
                remaining_qty=0.6,
                average_fill_price=100.0,
                position_mode="ONE_WAY",
                position_side="BOTH",
                raw_position={"contracts": 0.4},
                reason="partially filled",
            )
        def place_and_verify_native_stop(self, **kwargs):
            stop_calls.append(kwargs)
            return "partial-native-stop"

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "TradeExecutionService", PartialFillService)
    monkeypatch.setattr(ggpbot, "safe_api_call", lambda func, *a, **k: func(*a, **k))
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda value: value)
    monkeypatch.setattr(ggpbot, "dynamic_spread_tolerance", lambda _symbol: 10.0)
    monkeypatch.setattr(ggpbot, "get_spread_bps", lambda _symbol: 1.0)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "execution_unresolved": False, "recovery_hold": False,
        "close_reconciliation_required": False,
    })
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {"position_status": "FLAT"})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_ACTIVE_TRADE", False)

    result = ggpbot.open_position(
        "BUY", 1.0, "BTC/USDT:USDT", stop_loss=90.0,
        trade_id="partial-trade", management_state={"sl": 90.0},
    )

    assert result is None
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.STATE["entry_filled_qty"] == pytest.approx(0.4)
    assert ggpbot.STATE["entry_remaining_qty"] == pytest.approx(0.6)
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.MEMORY["position_status"] == "UNKNOWN"
    assert ggpbot.MEMORY["recovery_status"] == "PARTIAL_FILL_PROTECTION_VERIFIED_RECOVERY_REQUIRED"
    assert ggpbot.STATE["native_sl_verified"] is True
    assert ggpbot.STATE["native_sl_order_id"] == "partial-native-stop"
    assert len(stop_calls) == 1
    assert stop_calls[0]["quantity"] == pytest.approx(0.4)
    assert stop_calls[0]["trigger_price"] == pytest.approx(90.0)
    assert ggpbot._ACTIVE_TRADE is True


def test_open_position_boundary_refuses_unknown_account_before_api_calls(monkeypatch):
    calls = []

    class Exchange:
        def market(self, _symbol):
            calls.append("market")
            return {"contractSize": 1.0}

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda value: value)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "execution_unresolved": False, "recovery_hold": False,
        "close_reconciliation_required": False,
    })
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {"position_status": "UNKNOWN"})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_ACTIVE_TRADE", False)

    assert ggpbot.open_position("BUY", 1.0, "BTC/USDT:USDT", stop_loss=90.0) is None
    assert calls == []
    assert ggpbot._ACTIVE_TRADE is False


def test_open_position_market_data_reads_are_one_shot_per_management_cycle(monkeypatch):
    rows = [
        [1_700_000_000_000 + i * 900_000, 100.0, 101.0, 99.0, 100.5, 10.0]
        for i in range(100)
    ]
    calls = []

    class Exchange:
        def fetch_ohlcv(self, *_args, **_kwargs):
            return rows
        def fetch_ticker(self, *_args, **_kwargs):
            return {"last": 100.5}

    def governed(func, *args, **kwargs):
        calls.append((func.__name__, kwargs.get("_api_max_attempts")))
        return func(*args, **{k: v for k, v in kwargs.items() if k != "_api_max_attempts"})

    monkeypatch.setattr(ggpbot, "STATE", {"open": True})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": True})
    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "safe_api_call", governed)
    monkeypatch.setattr(ggpbot, "_ohlcv_rate_limit_cooldown_remaining", lambda _symbol: 0.0)
    monkeypatch.setattr(ggpbot, "OHLCV_FETCH_DIAGNOSTICS", {})
    monkeypatch.setattr(ggpbot, "_LAST_API_RATE_LIMIT", {"ts": 0.0, "method": "", "message": ""})
    monkeypatch.setattr(ggpbot, "INTERVAL", "15m")
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda symbol: symbol)

    frame = ggpbot.fetch_ohlcv("BTC/USDT:USDT")
    ticker = ggpbot.fetch_ticker("BTC/USDT:USDT")

    assert frame is not None and len(frame) == 100
    assert ticker["last"] == 100.5
    assert calls == [("fetch_ohlcv", 1), ("fetch_ticker", 1)]


def test_main_loop_dispatches_sniper_and_flow_scans_without_synchronous_execution():
    source = inspect.getsource(ggpbot._original_main_loop_sniper)
    assert '"institutional_flow_context", update_institutional_flow_scanner' in source
    assert '"sniper_scan_complete"' not in source
    assert '"sniper_scan_dispatched"' in source
    sniper_dispatch = inspect.getsource(ggpbot._run_sniper_scan_if_due)
    assert 'dispatch_scanner_task("sniper_v2", sniper_engine_v2)' in sniper_dispatch
    assert "return sniper_engine_v2()" not in sniper_dispatch


def test_sniper_due_check_only_dispatches_a_worker(monkeypatch):
    calls = []
    monkeypatch.setattr(ggpbot, "SNIPER_V2_ENABLED", True)
    monkeypatch.setattr(ggpbot, "SNIPER_V2_SCAN_INTERVAL", 5.0)
    monkeypatch.setattr(ggpbot, "_SNIPER_V2_LAST_SCAN", 0.0)
    monkeypatch.setattr(ggpbot.time, "time", lambda: 100.0)
    monkeypatch.setattr(
        ggpbot, "dispatch_scanner_task",
        lambda name, function: calls.append((name, function)) or True,
    )

    assert ggpbot._run_sniper_scan_if_due() is True
    assert calls == [("sniper_v2", ggpbot.sniper_engine_v2)]


def test_flat_position_snapshot_does_not_clear_ambiguous_order_outcome(monkeypatch):
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "_main_loop_blocked_phase", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "_main_loop_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("FLAT", []))
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "execution_unresolved": True, "recovery_hold": True,
        "close_reconciliation_required": False, "entry_client_order_id": "client-uncertain",
    })
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {"position_status": "UNKNOWN"})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)

    ggpbot.sync_all_states()

    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.MEMORY["position_status"] == "UNKNOWN"
    assert ggpbot.MEMORY["recovery_status"] == "ORDER_OUTCOME_REQUIRES_RECONCILIATION"


def test_verified_flat_snapshot_releases_only_a_resolved_local_state(monkeypatch):
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "_main_loop_blocked_phase", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "_main_loop_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("FLAT", []))
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "execution_unresolved": False, "recovery_hold": False,
        "close_reconciliation_required": False,
    })
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {"position_status": "UNKNOWN"})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", True)

    ggpbot.sync_all_states()

    assert ggpbot.STATE["execution_unresolved"] is False
    assert ggpbot.STATE["recovery_hold"] is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is False
    assert ggpbot.MEMORY["position_status"] == "FLAT"
