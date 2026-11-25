"""Health/readiness and controlled-restart regressions (offline, paper-safe).

Covers the 2026-11-25 production blocker: a live /health response could hide a
permanently stalled critical main_loop, and supervisor exhaustion looked like a
clean process exit. All tests are deterministic, mocked and offline.
"""
from __future__ import annotations

import inspect
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from ggp_instance_lock import acquire_instance_lock, release_instance_lock
from naiso_health import HealthWatchdog, RestartPolicy


SYMBOL = "BTC/USDT:USDT"


class JournalStub:
    def __init__(self, record=None):
        self.record = dict(record) if isinstance(record, dict) else None
        self.writes = []

    def open_trades(self):
        return []

    def get_runtime_state(self, key, default=None):
        return dict(self.record) if self.record is not None else default

    def set_runtime_state(self, key, value):
        self.record = dict(value)
        self.writes.append((key, dict(value)))


class LockStub:
    def __init__(self):
        self.close_count = 0

    def fileno(self):
        raise OSError("stub has no OS descriptor")

    @property
    def closed(self):
        return self.close_count > 0

    def close(self):
        self.close_count += 1


def _fresh_shutdown_state():
    return {"requested": None, "reason": "", "reconciling": False,
            "trading_stopped": False}


def _healthy_runtime(monkeypatch, *, paper=True, clock=None):
    clock = clock if clock is not None else [100.0]
    wall_now = time.time()
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(
        time=lambda: wall_now, monotonic=lambda: clock[0], sleep=lambda _s: None))
    watchdog = HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=3, monotonic_fn=lambda: clock[0]),
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        alert_fn=lambda *_a, **_k: None)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0,
                      critical=True, restartable=True)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", paper)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "recovery_hold": False, "execution_unresolved": False,
        "close_reconciliation_required": False, "current_symbol": None})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {"recovery_status": "READY"})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STATE", {"until": 0.0})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_RESTART_STATE", {
        "in_progress": False, "attempts": 0, "last_reason": "",
        "supervisor_exhausted": False})
    thread = threading.current_thread()
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_OWNER", {
        "thread_id": thread.ident, "thread_name": thread.name, "registered": True})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_PROGRESS_SEQ", 1)
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_LAST_OWNER_PROGRESS", {
        "seq": 1, "ts": clock[0], "thread_id": thread.ident,
        "thread_name": thread.name})
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start", progress=True)
    return watchdog, clock


def _exit_runtime(monkeypatch, *, paper=False, journal_record=None):
    monkeypatch.setattr(ggpbot, "PAPER_MODE", paper)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "current_symbol": None, "side": "", "trade_id": "",
        "remaining_qty": 0.0, "recovery_hold": False, "recovery_reason": "",
        "execution_unresolved": False, "close_reconciliation_required": False,
        "native_sl_status": "UNKNOWN", "native_sl_order_id": "",
        "native_sl_original_price": 0.0, "tp1_price": 0.0, "tp2_price": 0.0})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_RESTART_STATE", {
        "in_progress": False, "attempts": 0, "last_reason": "",
        "supervisor_exhausted": False})
    monkeypatch.setattr(ggpbot, "_SHUTDOWN_STATE", _fresh_shutdown_state())
    journal = JournalStub(journal_record)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    return journal


def _registry_runtime(monkeypatch, items, *, finalize_on_exit=False):
    """Minimal registry surface for exit-gate tests (no exchange I/O)."""
    active = list(items)
    positions = {
        item["trade_id"]: SimpleNamespace(symbol=item["symbol"], state=item["state"])
        for item in items
    }

    class _Focus:
        def __init__(self, trade_id):
            self.trade_id = trade_id

        def __enter__(self):
            ggpbot.STATE["current_symbol"] = positions[self.trade_id].symbol
            return self

        def __exit__(self, *_exc):
            if finalize_on_exit:
                active[:] = [item for item in active if item["trade_id"] != self.trade_id]
                positions.pop(self.trade_id, None)
            return False

    runtime = SimpleNamespace(
        status=lambda: {"active": list(active)},
        focus=lambda trade_id: _Focus(str(trade_id)),
        _positions=positions,
    )
    monkeypatch.setattr(ggpbot, "PORTFOLIO_RUNTIME", runtime, raising=False)
    monkeypatch.setattr(ggpbot, "PORTFOLIO_ALLOCATOR", SimpleNamespace(
        counts=lambda: {"TOTAL": 0},
    ), raising=False)
    return runtime


def _protected_paper_state(symbol, side="BUY"):
    return {
        "open": True, "current_symbol": symbol, "side": side,
        "remaining_qty": 1.0, "synthetic_sl": 100.0,
        "native_sl_status": "PAPER_SIMULATED", "execution_unresolved": False,
        "close_reconciliation_required": False,
    }


# --- Health/readiness state transitions ------------------------------------ #

def test_health_reports_running_readiness_and_genuine_progress(monkeypatch):
    watchdog, clock = _healthy_runtime(monkeypatch)
    client = ggpbot.app.test_client()

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["readiness"] == ggpbot.HEALTH_TRADING_READY
    assert payload["status"] == ggpbot.HEALTH_TRADING_READY
    assert payload["process"]["state"] == ggpbot.HEALTH_PROCESS_LIVE
    assert payload["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_RUNNING
    assert payload["main_loop"]["progress_seq"] == 1
    assert payload["main_loop"]["last_progress_age_sec"] <= 0.001
    assert payload["main_loop"]["last_progress_ts"] > 1_000_000_000
    assert payload["recovery_state"]["state"] == ggpbot.HEALTH_RECOVERY_NONE
    assert payload["blockers"] == []
    assert payload["naiso"]["threads"]["main_loop"]["status"] == "HEALTHY"

    detailed = client.get("/naiso/health")
    assert detailed.status_code == 200
    assert detailed.get_json()["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_RUNNING


def test_health_fails_closed_on_stalled_main_loop_without_writing_beats(monkeypatch):
    watchdog, clock = _healthy_runtime(monkeypatch)
    component = watchdog._components["main_loop"]
    component.status = "STALLED"
    component.reason = "TEST_REAL_STALL"
    clock[0] += 65.0
    # Status-only supervision must not refresh genuine progress age.
    watchdog.beat("main_loop", phase="supervisor_backoff", progress=False)

    def forbidden_beat(*_a, **_k):
        raise AssertionError("health endpoints must not fabricate heartbeats")

    monkeypatch.setattr(watchdog, "beat", forbidden_beat)
    client = ggpbot.app.test_client()
    response = client.get("/health")
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["readiness"] == ggpbot.HEALTH_TRADING_BLOCKED
    assert payload["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_STALLED
    assert payload["main_loop"]["last_progress_age_sec"] >= 65.0
    assert payload["main_loop"]["reason"] == "TEST_REAL_STALL"
    assert ggpbot.HEALTH_MAIN_LOOP_STALLED in payload["process_failures"]
    assert client.get("/naiso/health").status_code == 503


def test_open_position_is_trading_restriction_not_process_failure(monkeypatch):
    _healthy_runtime(monkeypatch)
    ggpbot.STATE["open"] = True
    client = ggpbot.app.test_client()

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["readiness"] == ggpbot.HEALTH_TRADING_BLOCKED
    assert payload["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_RUNNING
    assert "OPEN_POSITION_MANAGEMENT" in payload["blockers"]
    assert payload["process_failures"] == []


def test_health_reports_recovery_in_progress_then_failed(monkeypatch):
    _healthy_runtime(monkeypatch)
    client = ggpbot.app.test_client()

    ggpbot._MAIN_LOOP_RESTART_STATE["in_progress"] = True
    in_progress = client.get("/health").get_json()
    assert in_progress["recovery_state"]["state"] == ggpbot.HEALTH_RECOVERY_IN_PROGRESS
    assert in_progress["recovery_state"]["restart_in_progress"] is True
    assert in_progress["status"] == ggpbot.HEALTH_TRADING_BLOCKED

    ggpbot._MAIN_LOOP_RESTART_STATE.update({"in_progress": False, "supervisor_exhausted": True})
    ggpbot.STATE["recovery_hold"] = True
    ggpbot.STATE["recovery_reason"] = "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
    failed = client.get("/health")
    assert failed.status_code == 503
    payload = failed.get_json()
    assert payload["recovery_state"]["state"] == ggpbot.HEALTH_RECOVERY_FAILED
    assert payload["recovery_state"]["reason"] == "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
    assert ggpbot.HEALTH_RECOVERY_FAILED in payload["process_failures"]


def test_bounded_startup_block_is_not_process_failure(monkeypatch):
    watchdog, clock = _healthy_runtime(monkeypatch)
    ggpbot._MAIN_LOOP_PROGRESS_SEQ = 0
    ggpbot._MAIN_LOOP_LAST_OWNER_PROGRESS.update({
        "seq": 0, "ts": 0.0, "thread_id": None, "thread_name": ""})
    watchdog.beat("main_loop", state="BLOCKED", phase="startup_recovery",
                  reason="STARTUP_RECOVERY", progress=False,
                  blocked_until=clock[0] + 300.0)

    response = ggpbot.app.test_client().get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_UNKNOWN
    assert payload["main_loop"]["bounded_startup"] is True
    assert payload["main_loop"]["reason"] == "MAIN_LOOP_BOUNDED_STARTUP"
    assert payload["process_failures"] == []
    assert payload["readiness"] == ggpbot.HEALTH_TRADING_BLOCKED


def test_health_snapshot_exposes_grace_and_genuine_progress(monkeypatch):
    watchdog, _clock = _healthy_runtime(monkeypatch)
    row = watchdog.snapshot()["components"]["main_loop"]
    assert row["grace_sec"] == 60.0
    assert row["last_progress_monotonic"] is not None
    progress = watchdog.progress_snapshot("main_loop")
    assert progress["age_sec"] <= 0.001
    assert progress["status"] == "HEALTHY"


def test_health_readiness_tolerates_mocked_minimal_snapshot():
    report = ggpbot._health_readiness({"status": "TEST"})
    assert report["process"]["state"] == ggpbot.HEALTH_PROCESS_LIVE
    assert report["main_loop"]["state"] == ggpbot.HEALTH_MAIN_LOOP_UNKNOWN
    assert report["readiness"] == ggpbot.HEALTH_TRADING_BLOCKED
    assert report["http_status"] == 503


# --- Controlled exit gate --------------------------------------------------- #

def test_exit_gate_refuses_unknown_exchange_and_persists_failed_state(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=False)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("UNKNOWN", None))

    def forbidden_verifier(*_a, **_k):
        raise AssertionError("UNKNOWN exchange state must never be verified as protected")

    monkeypatch.setattr(ggpbot, "verify_native_stop_loss_detailed", forbidden_verifier)

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.MEMORY["recovery_status"] == "EXIT_REFUSED_UNVERIFIED_EXPOSURE"
    assert journal.record["exposure_status"] == "UNKNOWN"
    assert journal.record["safe_to_exit"] is False


def test_exit_gate_verified_flat_persists_safe_exit(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=False)
    ggpbot.STATE.update({"open": True, "current_symbol": SYMBOL, "side": "BUY"})
    cleared = []

    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("FLAT", []))
    monkeypatch.setattr(ggpbot, "_clear_confirmed_flat",
                        lambda symbol, reason: cleared.append((symbol, reason)) or True)

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is True
    assert cleared == [(SYMBOL, "SUPERVISOR_RESTART_LIMIT_EXHAUSTED")]
    assert journal.record["exposure_status"] == "FLAT"
    assert journal.record["safe_to_exit"] is True
    assert journal.record["details"]["exchange_flat_confirmed"] is True


def test_exit_gate_refuses_when_any_registered_position_is_unprotected(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=True)
    good = _protected_paper_state("BTC/USDT:USDT")
    bad = dict(good, symbol="ETH/USDT:USDT", current_symbol="ETH/USDT:USDT", synthetic_sl=0.0)
    _registry_runtime(monkeypatch, [
        {"trade_id": "t-btc", "symbol": "BTC/USDT:USDT", "state": good},
        {"trade_id": "t-eth", "symbol": "ETH/USDT:USDT", "state": bad},
    ])
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", []))

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False
    assert ggpbot.MEMORY["recovery_status"] == "EXIT_REFUSED_UNVERIFIED_EXPOSURE"
    assert journal.record["safe_to_exit"] is False


def test_exit_gate_verifies_every_registered_open_position(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=True)
    _registry_runtime(monkeypatch, [
        {"trade_id": "t-btc", "symbol": "BTC/USDT:USDT",
         "state": _protected_paper_state("BTC/USDT:USDT")},
        {"trade_id": "t-eth", "symbol": "ETH/USDT:USDT",
         "state": _protected_paper_state("ETH/USDT:USDT", side="SELL")},
    ])
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", []))

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is True
    assert ggpbot.MEMORY["recovery_status"] == "EXIT_VERIFIED_PROTECTED_OPEN"
    assert journal.record["safe_to_exit"] is True
    assert journal.record["exposure_status"] == "OPEN"


def test_exit_gate_finalizes_every_registered_position_before_flat(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=True)
    _registry_runtime(monkeypatch, [
        {"trade_id": "t-btc", "symbol": "BTC/USDT:USDT",
         "state": _protected_paper_state("BTC/USDT:USDT")},
        {"trade_id": "t-eth", "symbol": "ETH/USDT:USDT",
         "state": _protected_paper_state("ETH/USDT:USDT", side="SELL")},
    ], finalize_on_exit=True)
    cleared = []
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("FLAT", []))
    monkeypatch.setattr(ggpbot, "_clear_confirmed_flat",
                        lambda symbol, reason: cleared.append((symbol, reason)) or True)

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is True
    assert cleared == [
        ("BTC/USDT:USDT", "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"),
        ("ETH/USDT:USDT", "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"),
    ]
    assert ggpbot.MEMORY["recovery_status"] == "EXIT_RECONCILED_FLAT"
    assert journal.record["safe_to_exit"] is True


def test_exit_gate_refuses_when_persistence_unavailable(monkeypatch):
    _exit_runtime(monkeypatch, paper=False)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("FLAT", []))

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


def test_exit_gate_verified_open_native_stop_preserves_exchange_protection(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=False)
    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": "BUY",
        "remaining_qty": 2.0, "exchange_qty": 2.0,
        "position_side": "BOTH", "native_sl_original_price": 100.0,
        "tp1_price": 110.0, "tp2_price": 120.0})
    position = {"symbol": SYMBOL, "contracts": 2.0, "entryPrice": 100.0}

    class Exchange:
        def __init__(self):
            self.create_calls = []
            self.cancel_calls = []

    exchange = Exchange()
    monkeypatch.setattr(ggpbot, "ex", exchange)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", [position]))
    verified = []

    def verifier(symbol, side, trigger, qty, position_side=None, **_k):
        verified.append((symbol, side, trigger, qty, position_side))
        return "VERIFIED", "SL-1", 100.0

    monkeypatch.setattr(ggpbot, "verify_native_stop_loss_detailed", verifier)

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is True
    assert verified == [(SYMBOL, "BUY", 100.0, 2.0, "BOTH")]
    assert ggpbot.STATE["native_sl_status"] == "VERIFIED"
    assert ggpbot.STATE["native_sl_verified"] is True
    assert ggpbot.STATE["native_sl_order_id"] == "SL-1"
    assert exchange.create_calls == [] and exchange.cancel_calls == []
    assert journal.record["exposure_status"] == "OPEN"
    assert journal.record["safe_to_exit"] is True
    assert journal.record["details"]["exchange_native_stop_verified"] is True
    assert journal.record["native_tp1_price"] == 110.0


def test_exit_gate_refuses_missing_native_stop(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=False)
    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": "BUY",
        "remaining_qty": 2.0, "position_side": "BOTH",
        "native_sl_original_price": 100.0})
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", [{"symbol": SYMBOL}]))
    monkeypatch.setattr(ggpbot, "verify_native_stop_loss_detailed",
                        lambda *_a, **_k: ("MISSING", None, None))

    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False
    assert journal.record["safe_to_exit"] is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.MEMORY["recovery_status"] == "EXIT_REFUSED_UNVERIFIED_EXPOSURE"


def test_exit_gate_refuses_unrestored_journal_exposure(monkeypatch):
    _exit_runtime(monkeypatch, paper=False)
    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": "BUY",
        "remaining_qty": 2.0, "native_sl_original_price": 0.0,
        "recovery_reason": "journal_row_not_found"})
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", [{"symbol": SYMBOL}]))

    def forbidden_verifier(*_a, **_k):
        raise AssertionError("no native stop evidence means no verification attempt")

    monkeypatch.setattr(ggpbot, "verify_native_stop_loss_detailed", forbidden_verifier)
    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False


def test_exit_gate_refuses_while_verifier_is_single_flight(monkeypatch):
    _exit_runtime(monkeypatch, paper=False)
    ggpbot._MAIN_LOOP_RESTART_STATE["in_progress"] = True
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda *_a, **_k: ("OPEN", [{"symbol": SYMBOL}]))

    def forbidden_verifier(*_a, **_k):
        raise AssertionError("a second verifier must never start while one is active")

    monkeypatch.setattr(ggpbot, "verify_native_stop_loss_detailed", forbidden_verifier)
    assert ggpbot._recovery_exit_gate("SUPERVISOR_RESTART_LIMIT_EXHAUSTED") is False


def test_run_trade_supervisor_exits_nonzero_after_safe_gate(monkeypatch):
    _exit_runtime(monkeypatch)
    monkeypatch.setattr(ggpbot, "_install_shutdown_handlers", lambda: None)
    monkeypatch.setattr(ggpbot, "safe_main_loop", lambda: False)
    gate_calls = []
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate",
                        lambda reason, **_k: gate_calls.append(reason) or True)
    stopped = []
    monkeypatch.setattr(ggpbot, "_stop_runtime_services", lambda: stopped.append(True))
    lock = LockStub()

    with pytest.raises(SystemExit) as exc:
        ggpbot.run_trade_supervisor(lock_handle=lock, lock_path="/tmp/ggpbot.lock")

    assert exc.value.code == ggpbot.EXIT_RECOVERY_EXHAUSTED
    assert gate_calls == ["SUPERVISOR_RESTART_LIMIT_EXHAUSTED"]
    assert stopped == [True]
    assert lock.close_count == 1


def test_run_trade_supervisor_holds_process_while_gate_refuses(monkeypatch):
    _exit_runtime(monkeypatch)
    monkeypatch.setattr(ggpbot, "_install_shutdown_handlers", lambda: None)
    monkeypatch.setattr(ggpbot, "safe_main_loop", lambda: False)
    results = iter([False, True])
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda *_a, **_k: next(results))
    sleeps = []
    monkeypatch.setattr(ggpbot.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(ggpbot, "_stop_runtime_services", lambda: None)

    with pytest.raises(SystemExit) as exc:
        ggpbot.run_trade_supervisor(lock_handle=LockStub())
    assert exc.value.code == ggpbot.EXIT_RECOVERY_EXHAUSTED
    assert sleeps == [ggpbot.RECOVERY_EXIT_RECHECK_SEC]


def test_sigterm_blocks_entries_and_exits_143_after_safe_gate(monkeypatch):
    _exit_runtime(monkeypatch)
    monkeypatch.setattr(ggpbot, "_install_shutdown_handlers", lambda: None)
    monkeypatch.setattr(ggpbot, "safe_main_loop", lambda: None)
    gate_calls = []
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate",
                        lambda reason, **kwargs: gate_calls.append((reason, kwargs)) or True)
    monkeypatch.setattr(ggpbot, "_stop_runtime_services", lambda: None)
    lock = LockStub()

    ggpbot._request_shutdown(signal.SIGTERM, None)
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.STATE["recovery_reason"] == "SHUTDOWN_SIGTERM"

    with pytest.raises(SystemExit) as exc:
        ggpbot.run_trade_supervisor(lock_handle=lock, lock_path="/tmp/ggpbot.lock")
    assert exc.value.code == ggpbot.EXIT_SIGTERM
    assert gate_calls == [("SIGTERM", {"requested_signal": "SIGTERM"})]
    assert lock.close_count == 1


def test_supervisor_exhaustion_persists_recovery_state(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=True)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", SimpleNamespace(beat=lambda *_a, **_k: None))
    monkeypatch.setattr(ggpbot, "main_loop",
                        lambda: (_ for _ in ()).throw(RuntimeError("loop dead")))
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(
        time=lambda: 1000.0, monotonic=lambda: 1000.0, sleep=lambda _s: None))
    monkeypatch.setattr(ggpbot, "_NAISO_SUPERVISOR_STATE", {
        "window_started": 0.0, "restarts": 0, "exhausted": False,
        "last_reason": "", "last_attempt": 0})
    monkeypatch.setattr(ggpbot, "_NAISO_LOOP_RESTARTS", [])

    assert ggpbot.safe_main_loop() is False
    assert journal.record["reason"] == "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
    assert journal.record["safe_to_exit"] is False
    assert journal.record["details"]["phase"] == "supervisor_exhausted"


# --- Restart reconciliation ------------------------------------------------- #

def test_startup_recovery_reads_persisted_hold_before_entries(monkeypatch):
    journal = _exit_runtime(monkeypatch, paper=True, journal_record={
        "version": 1, "reason": "SUPERVISOR_RESTART_LIMIT_EXHAUSTED",
        "exposure_status": "UNKNOWN", "safe_to_exit": False})
    monkeypatch.setattr(ggpbot, "paper", {"position": {"stale": True}})

    assert ggpbot.startup_trade_recovery() is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.MEMORY["recovery_status"] == "PERSISTED_RECOVERY_HOLD"
    assert ggpbot.paper["position"] == {"stale": True}

    journal.record["safe_to_exit"] = True
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    assert ggpbot.startup_trade_recovery() is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is False
    assert ggpbot.paper["position"] is None


def test_startup_recovery_precedes_original_loop():
    source = inspect.getsource(ggpbot.main_loop_sniper)
    assert source.index("startup_trade_recovery()") < source.index("_original_main_loop_sniper()")


# --- Lock lifecycle --------------------------------------------------------- #

def test_release_instance_lock_is_idempotent(tmp_path):
    path = str(tmp_path / "ggpbot.lock")
    first = acquire_instance_lock(path)
    assert first is not None
    assert acquire_instance_lock(path) is None
    assert release_instance_lock(first) is True
    assert release_instance_lock(first) is False
    second = acquire_instance_lock(path)
    assert second is not None
    release_instance_lock(second)


def test_os_lock_is_released_after_normal_exit_and_stale_file(tmp_path):
    path = tmp_path / "ggpbot.lock"
    path.write_bytes(b"stale lock content must not matter")
    code = (
        "import sys; from ggp_instance_lock import acquire_instance_lock; "
        "h = acquire_instance_lock(sys.argv[1]); sys.exit(0 if h is not None else 1)"
    )
    result = subprocess.run([sys.executable, "-c", code, str(path)],
                            capture_output=True, text=True, timeout=15)

    assert result.returncode == 0, result.stderr
    handle = acquire_instance_lock(str(path))
    assert handle is not None
    release_instance_lock(handle)


def test_os_lock_is_released_after_interrupted_process(tmp_path):
    path = tmp_path / "ggpbot.lock"
    code = (
        "import sys; from ggp_instance_lock import acquire_instance_lock; "
        "h = acquire_instance_lock(sys.argv[1]); print('READY', flush=True); "
        "sys.stdin.read()" if os.name != "nt" else
        "import sys, time; from ggp_instance_lock import acquire_instance_lock; "
        "h = acquire_instance_lock(sys.argv[1]); print('READY', flush=True); "
        "time.sleep(30)"
    )
    process = subprocess.Popen([sys.executable, "-c", code, str(path)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
    try:
        assert process.stdout.readline().strip() == "READY"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    handle = acquire_instance_lock(str(path))
    assert handle is not None
    release_instance_lock(handle)


def test_main_block_releases_lock_and_exits_before_unverified_shutdown():
    source = (Path(__file__).resolve().parent / "ggpbot.py").read_text(encoding="utf-8")
    main_block = source.split('if __name__ == "__main__":', 1)[1]
    assert 'if _GGP_INSTANCE_LOCK is None:' in main_block
    assert '[SINGLE_INSTANCE] Another JGB_GGP process already owns the runtime lock' in main_block
    assert 'run_trade_supervisor(lock_handle=_GGP_INSTANCE_LOCK' in main_block
    assert 'release_instance_lock(_GGP_INSTANCE_LOCK)' in main_block
    assert main_block.index('if _GGP_INSTANCE_LOCK is None:') < main_block.index('start_dashboard_servers()')
