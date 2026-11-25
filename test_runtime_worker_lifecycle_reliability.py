"""Deterministic lifecycle, Waitress teardown and fault-injection regressions."""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import waitress

import ggpbot


PROJECT_ROOT = Path(__file__).resolve().parent


@pytest.fixture(autouse=True)
def _restore_runtime_flags():
    keys = (
        "_RUNTIME_WORKERS_STARTED", "_RUNTIME_SHUTTING_DOWN",
        "_NAISO_SERVICES_STARTED",
    )
    saved = {key: getattr(ggpbot, key) for key in keys}
    yield
    for key, value in saved.items():
        setattr(ggpbot, key, value)


class _CountingService:
    def __init__(self, name, calls):
        self.name = name
        self.calls = calls

    def start(self):
        self.calls.append(("start", self.name))
        return True

    def stop(self):
        self.calls.append(("stop", self.name))

    def close(self):
        self.calls.append(("close", self.name))

    def reopen(self):
        self.calls.append(("reopen", self.name))
        return True


class _FakeThread:
    def __init__(self, name, *, alive=True):
        self.name = name
        self.alive = alive
        self.joined = []

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.joined.append(timeout)
        self.alive = False


class _FakeWaitressServer:
    def __init__(self, port, calls):
        self.port = port
        self.calls = calls
        self.stop = threading.Event()

    def run(self):
        self.stop.wait(5.0)

    def close(self):
        self.calls.append(("close", self.port))
        self.stop.set()


def test_importing_ggpbot_does_not_start_owned_workers():
    code = """
import threading
import ggpbot
names = sorted(t.name for t in threading.enumerate())
print('LIFECYCLE_THREADS=' + ','.join(names))
"""
    env = os.environ.copy()
    env["PAPER_MODE"] = "true"
    env["ENABLE_LIVE_TRADING"] = "false"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PROJECT_ROOT), env=env, capture_output=True, text=True, timeout=90,
    )
    assert completed.returncode == 0, completed.stderr
    line = next(
        line for line in completed.stdout.splitlines()
        if line.startswith("LIFECYCLE_THREADS=")
    )
    names = set(line.split("=", 1)[1].split(","))
    for forbidden in (
        "ggp-event-bus", "ggp-exchange-reconciliation",
        "ggp_institutional_updater", "ggp_keep_alive",
    ):
        assert forbidden not in names


def test_start_runtime_workers_is_idempotent_and_starts_every_required_worker(monkeypatch):
    calls = []
    monkeypatch.setattr(ggpbot, "_event_bus", _CountingService("event_bus", calls))
    monkeypatch.setattr(
        ggpbot, "_reconciliation_supervisor", _CountingService("reconciliation", calls))
    monkeypatch.setattr(ggpbot, "TELEGRAM_NOTIFIER", _CountingService("telegram", calls))
    monkeypatch.setattr(
        ggpbot, "_start_named_worker",
        lambda name, _target: calls.append(("start", name)) or True,
    )
    monkeypatch.setattr(ggpbot, "_RUNTIME_WORKERS_STARTED", False)
    monkeypatch.setattr(ggpbot, "_RUNTIME_SHUTTING_DOWN", False)

    assert ggpbot.start_runtime_workers() is True
    assert ggpbot.start_runtime_workers() is False

    assert calls == [
        ("start", "event_bus"),
        ("start", "reconciliation"),
        ("start", "institutional_updater"),
        ("start", "keep_alive"),
        ("reopen", "telegram"),
    ]


def test_shutdown_joins_owned_services_and_explicit_restart_is_allowed(monkeypatch):
    calls = []
    stops = {
        "institutional_updater": threading.Event(),
        "keep_alive": threading.Event(),
    }
    threads = {
        "institutional_updater": _FakeThread("ggp_institutional_updater"),
        "keep_alive": _FakeThread("ggp_keep_alive"),
    }
    dashboard_thread = _FakeThread("ggp_dashboard_18080")
    dashboard = _CountingService("dashboard", calls)
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_STOPS", stops)
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_THREADS", threads)
    monkeypatch.setattr(ggpbot, "_event_bus", _CountingService("event_bus", calls))
    monkeypatch.setattr(
        ggpbot, "_reconciliation_supervisor", _CountingService("reconciliation", calls))
    monkeypatch.setattr(ggpbot, "TELEGRAM_NOTIFIER", _CountingService("telegram", calls))
    monkeypatch.setattr(ggpbot, "DASHBOARD_SERVERS", [(dashboard, dashboard_thread)])
    monkeypatch.setattr(ggpbot, "DASHBOARD_ACTIVE_PORTS", [18080])
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", _CountingService("watchdog", calls))
    monkeypatch.setattr(
        ggpbot, "_start_named_worker",
        lambda name, _target: calls.append(("start", name)) or True,
    )
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    ggpbot._stop_runtime_services()

    assert all(event.is_set() for event in stops.values())
    assert all(thread.joined for thread in threads.values())
    assert dashboard_thread.joined
    assert ("close", "dashboard") in calls
    assert ggpbot.DASHBOARD_SERVERS == []
    assert ggpbot.DASHBOARD_ACTIVE_PORTS == []
    assert ggpbot._RUNTIME_WORKERS_STARTED is False
    assert ggpbot._NAISO_SERVICES_STARTED is False

    # Late implicit startup must not reverse a completed shutdown; only an
    # explicit fresh lifecycle cycle may restart owned workers.
    assert ggpbot.start_runtime_workers() is False
    assert ggpbot.start_runtime_workers(restart=True) is True


def test_timed_out_named_worker_is_never_replaced_while_still_alive(monkeypatch):
    monkeypatch.setattr(ggpbot, "_RUNTIME_SHUTTING_DOWN", False)
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_THREADS", {})
    stop = threading.Event()
    started = threading.Event()
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_STOPS", {"quarantined": stop})

    def blocked_worker():
        started.set()
        stop.wait(2.0)

    assert ggpbot._start_named_worker("quarantined", blocked_worker) is True
    first_thread = ggpbot._PERIODIC_WORKER_THREADS["quarantined"]
    assert started.wait(1.0)
    try:
        # Simulate a watchdog timeout: the handle is still alive, so no second
        # thread may be created for the same worker.
        assert ggpbot._start_named_worker("quarantined", lambda: None) is False
        assert ggpbot._PERIODIC_WORKER_THREADS["quarantined"] is first_thread
        assert ggpbot._PERIODIC_WORKER_THREADS["quarantined"].is_alive()
    finally:
        stop.set()
        first_thread.join(timeout=2.0)

    assert ggpbot._start_named_worker("quarantined", lambda: None) is True
    ggpbot._PERIODIC_WORKER_THREADS["quarantined"].join(timeout=2.0)


def test_crashed_named_worker_is_recorded_and_can_be_replaced(monkeypatch):
    thread_errors = []
    monkeypatch.setattr(threading, "excepthook", lambda args: thread_errors.append(args.exc_type))
    monkeypatch.setattr(ggpbot, "_RUNTIME_SHUTTING_DOWN", False)
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_THREADS", {})
    monkeypatch.setattr(ggpbot, "_PERIODIC_WORKER_STOPS", {"crashy": threading.Event()})

    def crashing_worker():
        raise RuntimeError("simulated worker crash")

    assert ggpbot._start_named_worker("crashy", crashing_worker) is True
    crashed = ggpbot._PERIODIC_WORKER_THREADS["crashy"]
    crashed.join(timeout=2.0)
    assert crashed.is_alive() is False
    assert RuntimeError in thread_errors
    assert ggpbot.named_worker_status()["crashy"]["thread_alive"] is False

    assert ggpbot._start_named_worker("crashy", lambda: None) is True
    ggpbot._PERIODIC_WORKER_THREADS["crashy"].join(timeout=2.0)


def test_repeated_lifecycle_cycles_do_not_accumulate_threads(monkeypatch):
    expected_threads = {
        "ggp_institutional_updater", "ggp_keep_alive",
        "ggp_dashboard_18081", "ggp_dashboard_18082",
    }
    baseline = {
        thread.name for thread in threading.enumerate()
        if thread.name in expected_threads
    }
    calls = []
    server_calls = []

    def fake_create_server(_app, host, port, threads):
        assert host == "127.0.0.1"
        assert threads == 8
        server = _FakeWaitressServer(port, server_calls)
        return server

    monkeypatch.setattr(waitress, "create_server", fake_create_server)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.setenv("DASHBOARD_LOCAL_PORTS", "18081,18082")
    monkeypatch.setenv("DASHBOARD_BIND_HOST", "127.0.0.1")
    monkeypatch.setattr(ggpbot, "DASHBOARD_SERVERS", [])
    monkeypatch.setattr(ggpbot, "DASHBOARD_ACTIVE_PORTS", [])
    monkeypatch.setattr(ggpbot, "_RUNTIME_WORKERS_STARTED", False)
    monkeypatch.setattr(ggpbot, "_RUNTIME_SHUTTING_DOWN", False)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", None)
    monkeypatch.setattr(ggpbot, "_event_bus", _CountingService("event_bus", calls))
    monkeypatch.setattr(
        ggpbot, "_reconciliation_supervisor", _CountingService("reconciliation", calls))
    monkeypatch.setattr(ggpbot, "TELEGRAM_NOTIFIER", _CountingService("telegram", calls))
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    def owned_worker():
        name = threading.current_thread().name
        stop = ggpbot._PERIODIC_WORKER_STOPS.get(
            "institutional_updater" if "institutional" in name else "keep_alive")
        if stop is not None:
            while not stop.wait(0.01):
                pass

    monkeypatch.setattr(ggpbot, "live_institutional_updater", owned_worker)
    monkeypatch.setattr(ggpbot, "keep_alive", owned_worker)

    try:
        for _cycle in range(3):
            calls.clear()
            assert ggpbot.start_runtime_workers(restart=True) is True
            assert calls == [
                ("start", "event_bus"),
                ("start", "reconciliation"),
                ("reopen", "telegram"),
            ]
            assert ggpbot.start_dashboard_servers() == [18081, 18082]
            alive = {
                thread.name for thread in threading.enumerate()
                if thread.name in expected_threads
            }
            assert alive == expected_threads
            ggpbot._stop_runtime_services()
            remaining = {
                thread.name for thread in threading.enumerate()
                if thread.name in expected_threads
            }
            assert remaining == baseline
    finally:
        ggpbot._stop_runtime_services()

    assert [port for name, port in server_calls if name == "close"] == [18081, 18082] * 3


def test_partial_waitress_bind_failure_closes_already_started_servers(monkeypatch):
    calls = []
    created = []

    class FakeThread:
        def __init__(self, target, daemon, name):
            self.target = target
            self.daemon = daemon
            self.name = name
            self.alive = True

        def start(self):
            pass

        def join(self, timeout=None):
            calls.append(("join", self.name, timeout))
            self.alive = False

        def is_alive(self):
            return self.alive

    def fake_create_server(_app, host, port, threads):
        assert host == "127.0.0.1"
        assert threads == 8
        if port == 18084:
            raise OSError("simulated bind failure")
        server = _FakeWaitressServer(port, calls)
        created.append(server)
        return server

    monkeypatch.setattr(waitress, "create_server", fake_create_server)
    monkeypatch.setattr(ggpbot.threading, "Thread", FakeThread)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.setenv("DASHBOARD_LOCAL_PORTS", "18083,18084")
    monkeypatch.setenv("DASHBOARD_BIND_HOST", "127.0.0.1")
    monkeypatch.setattr(ggpbot, "DASHBOARD_SERVERS", [])
    monkeypatch.setattr(ggpbot, "DASHBOARD_ACTIVE_PORTS", [])
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    with pytest.raises(RuntimeError, match="could not bind every configured port"):
        ggpbot.start_dashboard_servers()

    assert len(created) == 1
    assert ("close", 18083) in calls
    assert any(item[0] == "join" and item[1] == "ggp_dashboard_18083" for item in calls)
    assert ggpbot.DASHBOARD_SERVERS == []
    assert ggpbot.DASHBOARD_ACTIVE_PORTS == []


def test_waitress_teardown_uses_close_without_werkzeug_methods(monkeypatch):
    calls = []

    class WaitressOnlyServer:
        def run(self):
            return None

        def close(self):
            calls.append("close")

    class ImmediateThread:
        def __init__(self, target, daemon, name):
            self.target = target
            self.daemon = daemon
            self.name = name

        def join(self, timeout=None):
            calls.append(("join", timeout))

        def is_alive(self):
            return False

    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    ggpbot._stop_dashboard_server(WaitressOnlyServer(), ImmediateThread(None, True, "fake"))
    assert calls == ["close", ("join", 2.0)]


def test_waitress_teardown_survives_close_and_join_failures(monkeypatch):
    warnings = []

    class BrokenServer:
        def close(self):
            raise RuntimeError("simulated close failure")

    class BrokenThread:
        def join(self, timeout=None):
            raise RuntimeError("simulated join failure")

    monkeypatch.setattr(
        ggpbot, "log_execution", lambda message, *_a, **_k: warnings.append(str(message)))

    ggpbot._stop_dashboard_server(BrokenServer(), BrokenThread())

    assert any("dashboard close failed" in message for message in warnings)
    assert any("dashboard join failed" in message for message in warnings)


def test_database_interruption_on_signal_consumption_blocks_new_entries(monkeypatch):
    class BrokenJournal:
        def consume_event(self, _event_id, _trade_id):
            raise sqlite3.OperationalError("simulated database interruption")

        def consume_cross_event(self, _event_id, _trade_id):
            return True

    monkeypatch.setitem(ggpbot.STATE, "open", True)
    monkeypatch.setitem(ggpbot.STATE, "trade_id", "trade-db-fault")
    monkeypatch.setitem(ggpbot.MEMORY, "recovery_status", "READY")
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", BrokenJournal())
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_start", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    def verified_entry(*_args, **_kwargs):
        ggpbot._LAST_EXECUTION_GATE_RESULT = {"durable_event_id": "evt-db-fault"}
        return True

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", verified_entry)

    assert ggpbot.execute_entry("BTC/USDT:USDT", "buy") is True
    assert ggpbot.STATE["entry_event_persistence_failed"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


def test_database_interruption_on_entry_context_persistence_blocks_new_entries(monkeypatch):
    class BrokenJournal:
        def persist_entry_context(self, _trade_id, _entry_context):
            raise sqlite3.OperationalError("simulated database interruption")

    monkeypatch.setitem(ggpbot.STATE, "open", True)
    monkeypatch.setitem(ggpbot.STATE, "trade_id", "trade-context-fault")
    monkeypatch.setitem(
        ggpbot.MEMORY, "_INSTITUTIONAL_ENTRY_CONTEXT",
        {"persistent_entry_context": {"setup_type": "TEST"}},
    )
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", BrokenJournal())
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_start", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    def verified_entry(*_args, **_kwargs):
        ggpbot._LAST_EXECUTION_GATE_RESULT = {}
        return True

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", verified_entry)

    assert ggpbot.execute_entry("BTC/USDT:USDT", "buy") is True
    assert ggpbot.STATE["entry_context_persistence_failed"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
