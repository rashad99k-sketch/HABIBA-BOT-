"""Regression coverage for NAISO watchdog incidents and Dashboard error routing.

All tests are offline/PAPER-safe. Process-exit calls are monkeypatched; no
exchange credentials, real orders, or live service restarts are used.
"""
from __future__ import annotations

import os
import threading

import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from naiso_health import ComponentHealth, HealthWatchdog, RestartPolicy


def test_main_thread_stack_is_captured_for_main_loop():
    watchdog = HealthWatchdog()
    component = ComponentHealth(
        name="main_loop", interval_sec=5.0, grace_sec=60.0,
        thread_name="MainThread",
    )
    snapshot = watchdog._stack_snapshot(component)
    assert snapshot["main_thread_stack"]
    assert "test_main_thread_stack_is_captured_for_main_loop" in snapshot["main_thread_stack"]


def test_stall_alert_includes_bounded_main_thread_stack():
    clock = [0.0]
    alerts = []
    watchdog = HealthWatchdog(
        monotonic_fn=lambda: clock[0],
        sleep_fn=lambda _seconds: None,
        alert_fn=lambda message, level: alerts.append((message, level)),
    )
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", thread_name="MainThread")
    clock[0] = 11.0
    report = watchdog.check()["main_loop"]
    assert report["status"] == "STALLED"
    assert watchdog._components["main_loop"].diagnostics["last_stall_stack"]
    stall_alert = next(message for message, _ in alerts if "stalled for" in message)
    assert "[MAIN_THREAD_STACK]" in stall_alert
    assert len(stall_alert) < 4000


def test_restart_failure_payload_preserves_main_thread_stack():
    clock = [0.0]
    alerts = []
    watchdog = HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=2, monotonic_fn=lambda: clock[0]),
        monotonic_fn=lambda: clock[0],
        sleep_fn=lambda _seconds: None,
        thread_alive_fn=lambda _name: True,
        alert_fn=lambda message, level: alerts.append((message, level)),
        safety_gate=lambda: (True, "READY"),
    )
    watchdog.register(
        "main_loop", interval_sec=5.0, grace_sec=10.0, restartable=True,
        restart_fn=lambda _context: False,
    )
    watchdog.beat("main_loop", state="RUNNING", thread_name="MainThread")
    clock[0] = 11.0
    report = watchdog.check()["main_loop"]
    assert report["restart"] == "RESTART_FAILED"
    assert report["main_thread_stack"]
    failure_alert = next(message for message, _ in alerts if "restart failed" in message)
    assert "[MAIN_THREAD_STACK]" in failure_alert


def test_critical_watchdog_alert_is_saved_to_dashboard_and_telegram(monkeypatch):
    dashboard = {"logs": [], "errors": [], "system_errors": []}
    telegram = []
    monkeypatch.setattr(ggpbot, "DASHBOARD_STATE", dashboard)
    monkeypatch.setattr(ggpbot, "MEMORY", {"log_debounce": {}})
    monkeypatch.setattr(ggpbot, "_get_runtime_logger", lambda: None)
    monkeypatch.setattr(ggpbot, "tg_error", lambda message, error_type: telegram.append((message, error_type)))

    ggpbot.log_execution(
        "[WATCHDOG] main_loop stalled for 60.8s (critical=True)\n"
        "[MAIN_THREAD_STACK]\nblocking_exchange_call()",
        "CRITICAL",
    )

    assert dashboard["errors"]
    assert len(dashboard["system_errors"]) == 1
    incident = dashboard["system_errors"][0]
    assert incident["severity"] == "CRITICAL"
    assert incident["error_type"] == "WATCHDOG_STALL"
    assert incident["component"] == "main_loop"
    assert "progress heartbeat" in incident["cause"]
    assert "blocking_exchange_call" in incident["stack_trace"]
    assert len(telegram) == 1
    assert telegram[0][1] == "NAISO_HEALTH"


def test_process_restart_requires_external_supervisor_and_verified_exit_gate(monkeypatch):
    exits = []
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setattr(ggpbot, "_main_loop_owner", lambda: {
        "thread_name": "MainThread", "thread_id": threading.main_thread().ident, "registered": True,
    })
    monkeypatch.setattr(ggpbot, "_main_loop_owner_alive", lambda: True)
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda _reason: True)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ggpbot.os, "_exit", lambda code: exits.append(code))

    context = {}
    assert ggpbot._restart_main_loop_process_after_stall(context) is True
    assert exits == [ggpbot.EXIT_RECOVERY_EXHAUSTED]
    assert context["engine_resumed"] is True


def test_process_restart_is_blocked_when_exchange_safety_gate_refuses(monkeypatch):
    exits = []
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setattr(ggpbot, "_main_loop_owner", lambda: {
        "thread_name": "MainThread", "thread_id": threading.main_thread().ident, "registered": True,
    })
    monkeypatch.setattr(ggpbot, "_main_loop_owner_alive", lambda: True)
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda _reason: False)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ggpbot.os, "_exit", lambda code: exits.append(code))

    context = {}
    assert ggpbot._restart_main_loop_process_after_stall(context) is True
    assert exits == []
    assert context["engine_resumed"] is False
    assert context["process_restart_blocked_reason"] == "RECOVERY_EXIT_GATE_REFUSED_UNVERIFIED_EXPOSURE"


def test_process_restart_stays_in_safe_hold_without_supervisor(monkeypatch):
    exits = []
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("NAISO_PROCESS_RESTART_ON_MAIN_LOOP_STALL", raising=False)
    monkeypatch.setattr(ggpbot, "_main_loop_owner", lambda: {
        "thread_name": "MainThread", "thread_id": threading.main_thread().ident, "registered": True,
    })
    monkeypatch.setattr(ggpbot, "_main_loop_owner_alive", lambda: True)
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda _reason: pytest.fail("exit gate must not run"))
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ggpbot.os, "_exit", lambda code: exits.append(code))

    context = {}
    assert ggpbot._restart_main_loop_process_after_stall(context) is True
    assert exits == []
    assert context["engine_resumed"] is False
    assert context["process_restart_blocked_reason"] == "EXTERNAL_PROCESS_SUPERVISOR_NOT_DETECTED"


def test_dashboard_payload_and_renderer_use_structured_system_errors():
    from pathlib import Path

    source = Path(ggpbot.__file__).read_text(encoding="utf-8")
    assert '"system_errors": DASHBOARD_STATE.get("system_errors", [])[-20:]' in source
    assert 'const systemErrors = (d.system_errors || [])' in source
    assert "uniEsc(e.stack_trace)" in source
