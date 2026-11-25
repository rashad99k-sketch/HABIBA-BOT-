"""Main-loop 60.3s stall incident regressions (offline and paper-safe).

Covers the 2026-11-25 incident: status-only supervision beats, owner-thread
liveness, bounded verifier execution, restart accounting and fail-closed
recovery holds. No credentials, network calls, real orders or watchdog
threshold changes are involved.
"""
from __future__ import annotations

import os
import sys
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from naiso_health import HealthWatchdog, RestartPolicy

SYMBOL = "BTC/USDT:USDT"


def _healthy_runtime(monkeypatch):
    """Install an isolated, genuinely progressing runtime for health-route tests."""
    clock = [100.0]
    wall_now = 1_700_000_000.0
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(
        time=lambda: wall_now, monotonic=lambda: clock[0], sleep=lambda _s: None))
    watchdog = HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=3, monotonic_fn=lambda: clock[0]),
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        alert_fn=lambda *_a, **_k: None)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0,
                      critical=True, restartable=True)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
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


def _watchdog(clock, alerts, *, safety_gate=None, **kwargs):
    return HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=3, monotonic_fn=lambda: clock[0]),
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        alert_fn=lambda message, level: alerts.append((message, level)),
        safety_gate=safety_gate, **kwargs)


def _liveness_watchdog(clock, alerts):
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0, critical=True,
                      restartable=True, lifecycle="RUNNING", resume_requires_liveness=True)
    return watchdog


def _runtime(monkeypatch, *, paper=True, open_position=True, symbol=SYMBOL):
    """Isolate the recovery globals used by these tests."""
    state = {"open": open_position, "current_symbol": symbol, "recovery_hold": False,
             "trade_id": "T-1", "side": "BUY", "remaining_qty": 2.0}
    monkeypatch.setattr(ggpbot, "PAPER_MODE", paper)
    monkeypatch.setattr(ggpbot, "STATE", state)
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {
        "in_position": open_position, "symbol": symbol, "side": "BUY", "qty": 2.0})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "DASHBOARD_STATE", {})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "_live_manager", SimpleNamespace(lifecycle_state=None))
    monkeypatch.setattr(ggpbot, "_ENGINE_RECOVERY_GUARD", {"ts": 0.0, "symbol": None})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_RESTART_STATE", {
        "in_progress": False, "attempts": 0, "last_reason": "",
        "supervisor_exhausted": False})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_OWNER", {
        "thread_id": None, "thread_name": "MainThread", "registered": False})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_LAST_OWNER_PROGRESS", {
        "seq": 0, "ts": 0.0, "thread_id": None, "thread_name": ""})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_PROGRESS_SEQ", 0)
    return state


class NoPositionExchange:
    def __init__(self):
        self.calls = []
        self.markets = {}

    def fetch_positions(self, symbols=None):
        self.calls.append(("fetch_positions", symbols))
        return []


# --- Detection: real stalls and non-progressing status beats -------------- #

def test_genuinely_stalled_main_loop_is_detected_once():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")

    clock[0] = 11.0
    first = watchdog.check()["main_loop"]
    clock[0] = 12.0
    second = watchdog.check()["main_loop"]

    assert first["status"] == second["status"] == "STALLED"
    assert len([message for message, _ in alerts if "stalled for" in message]) == 1


def test_status_only_beats_do_not_advance_liveness():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")
    for phase in ("position_sync_failed", "api_status_only"):
        watchdog.beat("main_loop", state="RUNNING", phase=phase, progress=False,
                      thread_name="MainThread")

    component = watchdog._components["main_loop"]
    assert component.last_progress == 0.0
    clock[0] = 11.0
    assert watchdog.check()["main_loop"]["status"] == "STALLED"


def test_supervisor_backoff_beat_cannot_mask_stall():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")

    clock[0] = 5.0
    watchdog.beat("main_loop", phase="supervisor_backoff", progress=False,
                  state="RUNNING", last_restart_reason="RuntimeError: loop dead",
                  supervisor_attempt=1, thread_name="MainThread")
    clock[0] = 11.0
    report = watchdog.check()["main_loop"]

    assert report["status"] == "STALLED"
    assert watchdog._components["main_loop"].last_progress == 0.0


def test_lock_timeout_and_busy_status_beats_never_claim_progress(monkeypatch):
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")
    watchdog.beat("main_loop", state="RUNNING", phase="position_sync",
                  reason="LOCK_TIMEOUT", progress=False, thread_name="MainThread")
    watchdog.beat("main_loop", state="RUNNING", phase="position_sync",
                  reason="LOCK_BUSY", progress=False, thread_name="MainThread")
    clock[0] = 11.0
    assert watchdog.check()["main_loop"]["status"] == "STALLED"

    beats = []

    class Recorder:
        def monotonic(self):
            return 500.0

        def beat(self, name, **details):
            beats.append(dict(details))

    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", Recorder())
    # A caller passing progress=True must not override the status-only helpers.
    ggpbot._main_loop_blocked_phase("position_sync", "LOCK_TIMEOUT", progress=True)
    ggpbot._main_loop_status("position_sync", "LOCK_BUSY", progress=True)
    assert len(beats) == 2
    assert all(beat["progress"] is False for beat in beats)
    assert all("progress_seq" not in beat for beat in beats)


def test_dead_main_loop_thread_is_fail_closed_without_restart():
    clock = [0.0]
    alerts = []
    restarts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"),
                         thread_alive_fn=lambda _name: False)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=True,
                      restart_fn=lambda _ctx: restarts.append(1) or True)
    watchdog.beat("main_loop", state="BLOCKED", phase="position_sync_start",
                  reason="EXCHANGE_POSITION_SYNC", blocked_until=100.0, progress=False,
                  thread_name="dead-main-loop")

    clock[0] = 11.0
    report = watchdog.check()["main_loop"]

    assert report["status"] == "FAILED"
    assert report["restart"] == "SAFETY_HOLD"
    assert report["thread_name"] == "dead-main-loop"
    assert restarts == []
    assert any("lost its thread" in message for message, _ in alerts)


def test_alive_but_blocked_thread_does_not_extend_liveness():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"),
                         thread_alive_fn=lambda _name: True)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="BLOCKED", phase="position_sync_start",
                  reason="EXCHANGE_POSITION_SYNC", blocked_until=30.0, progress=False,
                  thread_name="MainThread")

    clock[0] = 5.0
    blocked = watchdog.check()["main_loop"]
    assert blocked["status"] == "BLOCKED"

    # The BLOCKED deadline is hard-capped by the unchanged grace window.
    clock[0] = 11.0
    stalled = watchdog.check()["main_loop"]
    assert stalled["status"] == "STALLED"
    assert watchdog._components["main_loop"].last_progress == 0.0


def test_watchdog_distinguishes_running_idle_blocked_and_failed():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts, thread_alive_fn=lambda _name: True)
    for name, lifecycle in (("running", "RUNNING"), ("idle", "IDLE"),
                            ("blocked", "BLOCKED"), ("failed", "RUNNING")):
        watchdog.register(name, interval_sec=5.0, grace_sec=10.0,
                          lifecycle=lifecycle, restartable=False)
    watchdog.beat("running", state="RUNNING", phase="iteration_start")
    watchdog.beat("idle", state="IDLE", idle_until=100.0, progress=False)
    watchdog.beat("blocked", state="BLOCKED", blocked_until=100.0, progress=False,
                  thread_name="blocked-worker")
    watchdog.beat("failed", state="FAILED", reason="engine_failed", progress=False)

    report = watchdog.check()
    assert report["running"]["status"] == "HEALTHY"
    assert report["idle"]["status"] == "IDLE"
    assert report["blocked"]["status"] == "BLOCKED"
    assert report["failed"]["status"] == "FAILED"


# --- Owner-thread liveness ------------------------------------------------ #

def test_non_owner_progress_cannot_satisfy_liveness(monkeypatch):
    state = _runtime(monkeypatch)
    clock = [0.0]
    alerts = []
    watchdog = _liveness_watchdog(clock, alerts)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    ggpbot._main_loop_register_owner(force=True)

    errors = []

    def worker():
        try:
            ggpbot._main_loop_progress("worker_observation")
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    thread = threading.Thread(target=worker, name="worker-diagnostic")
    thread.start()
    thread.join(timeout=5.0)

    assert errors == []
    assert ggpbot._MAIN_LOOP_PROGRESS_SEQ == 0
    worker_beats = [beat for beat in
                    [watchdog._components["main_loop"].diagnostics]
                    if beat.get("last_phase") == "worker_observation"]
    assert worker_beats and worker_beats[0]["caller_thread_name"] == "worker-diagnostic"
    assert ggpbot._wait_for_main_loop_progress(0, timeout_sec=0.05) is False

    # The registered owner still advances liveness with a genuine phase.
    ggpbot._main_loop_progress("iteration_start")
    assert ggpbot._MAIN_LOOP_PROGRESS_SEQ == 1
    assert ggpbot._wait_for_main_loop_progress(0, timeout_sec=0.05) is True
    assert state is not None


def test_restart_guard_rejects_alive_progressing_owner_and_reports_reason(monkeypatch):
    _runtime(monkeypatch)
    clock = [0.0]
    alerts = []
    watchdog = _liveness_watchdog(clock, alerts)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    ggpbot._main_loop_register_owner(force=True)
    ggpbot._main_loop_progress("iteration_start")

    def forbidden_recovery():
        pytest.fail("an alive, progressing main_loop must not be restarted")

    monkeypatch.setattr(ggpbot, "startup_trade_recovery", forbidden_recovery)
    context = {"open_position_verified": True, "symbol": SYMBOL}

    assert ggpbot._resume_main_loop_after_verification(context) is False
    assert context["engine_resumed"] is False
    assert context["restart_guard"] == "MAIN_LOOP_ALIVE_AND_PROGRESSING"
    assert context["main_loop_thread_name"] == "MainThread"
    assert context["main_loop_thread_alive"] is True


def test_duplicate_restart_attempt_is_rejected(monkeypatch):
    _runtime(monkeypatch)
    ggpbot._MAIN_LOOP_RESTART_STATE["in_progress"] = True

    def forbidden_recovery():
        pytest.fail("duplicate restart attempts must be rejected")

    monkeypatch.setattr(ggpbot, "startup_trade_recovery", forbidden_recovery)
    context = {"open_position_verified": True, "symbol": SYMBOL}

    assert ggpbot._resume_main_loop_after_verification(context) is False
    assert context["restart_guard"] == "RESTART_IN_PROGRESS"
    assert context["restart_guard_reason"] == "RESTART_IN_PROGRESS"
    assert context["engine_resumed"] is False


# --- Fail-closed recovery holds ------------------------------------------- #

@pytest.mark.parametrize("paper", [True, False])
def test_invalid_base_asset_with_open_exposure_holds_fail_closed(monkeypatch, paper):
    state = _runtime(monkeypatch, paper=paper, open_position=True, symbol=":USDT")
    exchange = NoPositionExchange()
    monkeypatch.setattr(ggpbot, "ex", exchange)

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert "invalid_base_asset" in reason
    assert context["symbol_status"] == "INVALID"
    assert state["recovery_hold"] is True
    assert state["recovery_reason"] == reason
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert exchange.calls == []


# --- Supervisor budget and bounded verifier ------------------------------- #

def test_supervisor_backoff_is_status_only_bounded_and_fail_closed(monkeypatch):
    _runtime(monkeypatch)
    beats = []

    class Recorder:
        def beat(self, name, **details):
            beats.append(dict(details))

    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", Recorder())
    monkeypatch.setattr(ggpbot, "main_loop",
                        lambda: (_ for _ in ()).throw(RuntimeError("loop dead")))
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(
        time=lambda: 1000.0, monotonic=lambda: 1000.0, sleep=lambda _s: None))
    monkeypatch.setattr(ggpbot, "_NAISO_SUPERVISOR_STATE", {
        "window_started": 0.0, "restarts": 0, "exhausted": False,
        "last_reason": "", "last_attempt": 0})
    monkeypatch.setattr(ggpbot, "_NAISO_LOOP_RESTARTS", [])

    assert ggpbot.safe_main_loop() is False

    backoffs = [beat for beat in beats if beat.get("phase") == "supervisor_backoff"]
    assert len(backoffs) == ggpbot._NAISO_SUPERVISOR_MAX_RESTARTS
    assert all(beat.get("progress") is False for beat in backoffs)
    assert all("progress_seq" not in beat for beat in backoffs)
    final = beats[-1]
    assert final["state"] == "FAILED"
    assert final["reason"] == "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
    assert final["progress"] is False
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot._MAIN_LOOP_RESTART_STATE["supervisor_exhausted"] is True


def test_hung_verifier_times_out_without_consuming_restart_attempt():
    clock = [0.0]
    alerts = []
    restarts = []
    release = threading.Event()
    calls = []

    def verifier():
        calls.append(clock[0])
        release.wait(timeout=5.0)
        return True, "VERIFIED", {}

    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=2.0, critical=True,
                      restartable=True,
                      recovery_verifier=verifier,
                      restart_fn=lambda _ctx: restarts.append(1) or True)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")
    clock[0] = 10.0

    try:
        report = watchdog.check()["main_loop"]
    finally:
        release.set()

    assert restarts == []
    assert calls == [10.0]
    assert report["restart"] == "VERIFICATION_HOLD"
    assert report["reason"] == "VERIFIER_TIMEOUT"
    assert report["verifier_timeout"] is True
    assert report["verifier_timeout_sec"] <= 2.0
    assert watchdog.restart_policy.records("main_loop") == []
    assert watchdog._verifier_bound_sec(watchdog._components["main_loop"]) == 1.0


# --- Restart accounting and diagnostics ----------------------------------- #

def test_recovery_after_successful_restart_rearms_detection():
    clock = [0.0]
    alerts = []
    restarts = []

    def restart_fn(context):
        restarts.append(clock[0])
        context["engine_resumed"] = True
        return True

    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, critical=True,
                      restartable=True, resume_requires_liveness=True,
                      recovery_verifier=lambda: (True, "VERIFIED", {}),
                      restart_fn=restart_fn)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")

    clock[0] = 11.0
    first = watchdog.check()["main_loop"]
    assert first["status"] == "RECOVERED"
    assert restarts == [11.0]

    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")
    clock[0] = 22.0
    second = watchdog.check()["main_loop"]
    assert second["status"] == "RECOVERED"
    assert restarts == [11.0, 22.0]
    assert second["attempt"] == 2
    assert len([message for message, _ in alerts if "stalled for" in message]) == 2


def test_recovery_deferred_alerts_and_holds_without_spending_budget():
    clock = [0.0]
    alerts = []
    calls = []

    def restart_fn(context):
        calls.append(clock[0])
        context["engine_resumed"] = False
        return True

    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"))
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, critical=True,
                      restartable=True, resume_requires_liveness=True,
                      restart_fn=restart_fn)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")

    clock[0] = 11.0
    first = watchdog.check()["main_loop"]
    clock[0] = 22.0
    second = watchdog.check()["main_loop"]

    assert calls == [11.0, 22.0]
    assert first["status"] == second["status"] == "STALLED_SAFE_HOLD"
    assert first["restart"] == second["restart"] == "RECOVERY_DEFERRED"
    assert first["attempt"] == second["attempt"] == 1
    assert all(not row["counts"] for row in watchdog.restart_policy.records("main_loop"))
    assert any("recovery deferred" in message for message, _ in alerts)


def test_restart_failed_diagnostics_include_attempt_thread_and_guard_context():
    clock = [0.0]
    alerts = []
    restarts = []

    def verifier():
        return True, "VERIFIED", {
            "open_position_verified": True, "thread_name": "MainThread",
            "restart_guard_reason": "RESTART_IN_PROGRESS",
            "recovery_guard_reason": "RECOVERY_GUARD_REJECTED",
        }

    def restart_fn(context):
        restarts.append(1)
        raise RuntimeError("restart exploded")

    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"),
                         thread_alive_fn=lambda name: name == "MainThread")
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=10.0, critical=True,
                      restartable=True, recovery_verifier=verifier, restart_fn=restart_fn)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")
    clock[0] = 11.0

    report = watchdog.check()["main_loop"]

    assert restarts == [1]
    assert report["status"] == "RESTART_FAILED"
    assert report["restart"] == "RESTART_EXCEPTION"
    assert report["attempt_id"] == "main_loop:1"
    assert report["thread_name"] == "MainThread"
    assert report["thread_alive"] is True
    assert report["restart_guard"] == "RESTART_IN_PROGRESS"
    assert report["safety_gate_reason"] == "READY"
    assert report["recovery_guard_reason"] == "RECOVERY_GUARD_REJECTED"
    assert report["exception_detail"] == "RuntimeError: restart exploded"
    assert "restart exploded" in report["exception_traceback"]
    assert report["recent_restarts"][0]["attempt"] == 1
    failure_alerts = [message for message, _ in alerts if message.startswith(
        "[WATCHDOG] main_loop restart failed:")]
    assert len(failure_alerts) == 1
    alert = failure_alerts[0]
    for fragment in ("attempt=main_loop:1", "thread=MainThread", "thread_alive=True",
                     "guard=RESTART_IN_PROGRESS", "safety_gate=READY",
                     "recovery_guard=RECOVERY_GUARD_REJECTED",
                     "exception=RuntimeError: restart exploded"):
        assert fragment in alert


# --- Verifier single-flight, generation and unique attempt IDs -------------- #

def test_blocked_verifier_is_single_flight_and_generation_tagged():
    clock = [0.0]
    alerts = []
    release = threading.Event()
    calls = []
    restarted_with = []

    def verifier():
        calls.append(clock[0])
        token = "late" if len(calls) == 1 else "current"
        release.wait(timeout=5.0)
        return True, "VERIFIED", {"token": token}

    def restart_fn(context):
        restarted_with.append(context.get("token"))
        return True

    watchdog = _watchdog(clock, alerts, safety_gate=lambda: (True, "READY"),
                         verification_retry_sec=1.0)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=2.0, critical=True,
                      restartable=True, recovery_verifier=verifier, restart_fn=restart_fn)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")

    clock[0] = 3.0
    first = watchdog.check()["main_loop"]
    component = watchdog._components["main_loop"]
    assert first["restart"] == "VERIFICATION_HOLD"
    assert first["verifier_timeout"] is True
    assert first["verification_generation"] == 1
    assert first["verification_id"] == "main_loop:v1"
    assert calls == [3.0]
    assert component.verifier_thread is not None and component.verifier_thread.is_alive()

    # The verification backoff expires while the first verifier is still blocked.
    clock[0] = 5.0
    second = watchdog.check()["main_loop"]
    assert calls == [3.0], "a blocked verifier must not be duplicated"
    assert second["reason"] == "VERIFIER_ALREADY_RUNNING"
    assert second["verifier_already_running"] is True
    assert second["verification_generation"] == 1
    assert component.verification_generation == 1

    blocked = component.verifier_thread
    release.set()
    blocked.join(timeout=2.0)
    assert not blocked.is_alive()

    clock[0] = 7.0
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start",
                  thread_name="MainThread")
    clock[0] = 10.0
    third = watchdog.check()["main_loop"]
    assert calls == [3.0, 10.0]
    assert component.verification_generation == 2
    assert restarted_with == ["current"], "a late verifier result must not win"


def test_idle_verifier_timeout_cannot_accumulate_threads(monkeypatch):
    clock = [0.0]
    alerts = []
    release = threading.Event()
    calls = []

    def verifier():
        calls.append(clock[0])
        release.wait(timeout=5.0)
        return True, "VERIFIED", {}

    watchdog = _watchdog(clock, alerts, verification_retry_sec=1.0)
    watchdog.register("scanner:global_discovery", interval_sec=10.0, grace_sec=2.0,
                      critical=False, restartable=False, lifecycle="IDLE",
                      recovery_verifier=verifier)
    watchdog.beat("scanner:global_discovery", state="IDLE", idle_until=1.0,
                  progress=False)
    component = watchdog._components["scanner:global_discovery"]

    clock[0] = 2.0
    first = watchdog.check()["scanner:global_discovery"]
    assert first["verifier_timeout"] is True
    assert first["verification_id"] == "scanner:global_discovery:v1"

    # Repeated IDLE due dates must not start a second verifier while the first
    # one is still blocked.
    for due in (5.0, 8.0, 11.0):
        clock[0] = due
        report = watchdog.check()["scanner:global_discovery"]
        assert report["restart"] == "VERIFICATION_HOLD"
    assert calls == [2.0]
    assert component.verification_generation == 1

    blocked = component.verifier_thread
    release.set()
    blocked.join(timeout=2.0)
    assert not blocked.is_alive()
    monkeypatch.setattr(watchdog, "alert_fn", lambda message, level: None)


def test_restart_attempt_uids_are_unique_when_recovery_is_deferred():
    policy = RestartPolicy(max_attempts=3, monotonic_fn=lambda: 0.0)
    deferred = policy.begin("main_loop", "deferred", 0.05)
    policy.finish(deferred, "RECOVERY_DEFERRED", counts=False)
    real = policy.begin("main_loop", "real", 0.05)
    policy.finish(real, "RESTARTED")

    assert deferred.attempt == real.attempt == 1, "budget attempt must not be consumed"
    assert deferred.uid != real.uid, "diagnostic attempt identity must be unique"
    rows = policy.records("main_loop")
    assert [row["uid"] for row in rows] == [deferred.uid, real.uid]


# --- Background Watchlist refresh: never run discovery OHLCV on MainThread -- #

def test_unified_watchlist_refresh_schedules_one_background_worker_and_fails_closed(monkeypatch):
    calls = []
    workers = []

    class FakeThread:
        def __init__(self, *, target, name, daemon, args=()):
            self.target = target
            self.args = args
            self.name = name
            self.daemon = daemon
            workers.append(self)

        def start(self):
            calls.append(("start", self.name, self.daemon))

    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {}})
    monkeypatch.setattr(ggpbot, "_last_unified_watchlist_refresh", 0.0)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS", 20)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_BATCH_SIZE", 4)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS", 30)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_RESOURCE_MANAGER", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot.threading, "Thread", FakeThread)
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", {
        "lock": __import__("threading").Lock(), "in_progress": False,
        "last_attempt": 0.0, "last_attempt_monotonic": -1000.0,
        "last_completed_monotonic": -1000.0, "phase": "IDLE",
        "last_error": "", "worker_name": "",
    })

    def sync_cross_events(**_kwargs):
        calls.append(("sync",))

    def update(memory, fetch_frame, **kwargs):
        calls.append(("update", kwargs["batch_size"], kwargs["max_items"]))
        return {"BTC": {"decision_state": "WAITING_FOR_CONFIRMATION"}}

    monkeypatch.setattr(ggpbot, "_sync_confirmed_cross_events", sync_cross_events)
    monkeypatch.setattr(ggpbot, "update_unified_watchlist", update)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda _symbol: None)
    monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *_a: {})

    returned = ggpbot.refresh_unified_institutional_watchlist()

    # The foreground call only starts a daemon worker; it never runs data fetches.
    assert returned == {}
    assert calls == [("start", "unified-institutional-watchlist", True)]
    assert len(workers) == 1
    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is False
    assert ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE["in_progress"] is True

    # Re-entry while the worker is active must not create a second worker.
    ggpbot.refresh_unified_institutional_watchlist()
    assert len(workers) == 1

    # Run the captured worker body deterministically and verify publication.
    workers[0].target(*workers[0].args)
    assert calls[1:] == [("sync",), ("update", 4, 30)]
    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is True
    assert ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE["in_progress"] is False
    assert ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE["last_error"] == ""


def test_unified_watchlist_worker_failure_keeps_entry_gate_closed_and_backs_off(monkeypatch):
    workers = []

    class FakeThread:
        def __init__(self, *, target, name, daemon, args=()):
            self.target = target
            self.args = args
            workers.append(self)

        def start(self):
            return None

    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {"BTC": {"decision_state": "ENTRY_READY"}}})
    monkeypatch.setattr(ggpbot, "_last_unified_watchlist_refresh", 0.0)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS", 20)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot.threading, "Thread", FakeThread)
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", {
        "lock": __import__("threading").Lock(), "in_progress": False,
        "last_attempt": 0.0, "last_attempt_monotonic": -1000.0,
        "last_completed_monotonic": -1000.0, "phase": "IDLE",
        "last_error": "", "worker_name": "",
    })
    monkeypatch.setattr(
        ggpbot, "_sync_confirmed_cross_events",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("simulated market-data failure")),
    )

    ggpbot.refresh_unified_institutional_watchlist()
    assert len(workers) == 1
    workers[0].target(*workers[0].args)

    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is False
    assert "simulated market-data failure" in ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE["last_error"]
    assert ggpbot._UNIFIED_WATCHLIST_REFRESH_STATE["in_progress"] is False

    # Failed refresh is not retried on every main-loop iteration.
    ggpbot.refresh_unified_institutional_watchlist()
    assert len(workers) == 1


# --- Unified Watchlist worker liveness and process-level recovery ------------ #

def test_stalled_watchlist_worker_requests_verified_process_recovery_once(monkeypatch):
    clock = [200.0]
    state = {
        "lock": threading.Lock(), "in_progress": True, "last_attempt": 100.0,
        "last_attempt_monotonic": 100.0, "started_monotonic": 100.0,
        "last_progress_monotonic": 100.0, "last_completed_monotonic": 0.0,
        "phase": "OHLCV_FETCH", "current_symbol": "ETC/USDT:USDT",
        "timed_out": False, "restart_requested": False,
        "last_error": "", "worker_name": "unified-institutional-watchlist",
    }
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(
        time=lambda: 1000.0, monotonic=lambda: clock[0], sleep=lambda _s: None))
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC", 90.0)
    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {"BTC": {"decision_state": "ENTRY_READY"}}})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    restart_calls = []
    monkeypatch.setattr(
        ggpbot, "_restart_main_loop_process_after_stall",
        lambda context: restart_calls.append(dict(context)) or True,
    )

    returned = ggpbot.refresh_unified_institutional_watchlist()

    assert returned == ggpbot.MEMORY["watchlist"]
    assert len(restart_calls) == 1
    assert restart_calls[0]["reason"] == "UNIFIED_WATCHLIST_WORKER_STALLED"
    assert restart_calls[0]["worker_phase"] == "OHLCV_FETCH"
    assert restart_calls[0]["worker_symbol"] == "ETC/USDT:USDT"
    assert state["timed_out"] is True
    assert state["restart_requested"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is False

    # A wedged/timed-out worker is never replaced by a concurrent second worker.
    ggpbot.refresh_unified_institutional_watchlist()
    assert len(restart_calls) == 1


def test_late_watchlist_worker_completion_cannot_reopen_entry_gate(monkeypatch):
    state = {
        "lock": threading.Lock(), "in_progress": True, "last_attempt": 100.0,
        "last_attempt_monotonic": 100.0, "started_monotonic": 100.0,
        "last_progress_monotonic": 100.0, "last_completed_monotonic": 0.0,
        "phase": "OHLCV_FETCH", "current_symbol": "BTC/USDT:USDT",
        "timed_out": True, "restart_requested": True,
        "last_error": "WORKER_STALLED", "worker_name": "unified-institutional-watchlist",
    }
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {}})
    monkeypatch.setattr(ggpbot, "_sync_confirmed_cross_events", lambda **_kwargs: None)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda _symbol: None)
    monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *_a: {})
    monkeypatch.setattr(
        ggpbot, "update_unified_watchlist",
        lambda *_a, **_k: {"BTC": {"decision_state": "ENTRY_READY"}},
    )
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_RESOURCE_MANAGER", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    ggpbot._run_unified_institutional_watchlist_refresh()

    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is False
    assert state["timed_out"] is True
    assert state["in_progress"] is False


def test_health_marks_stalled_watchlist_worker_as_process_failure(monkeypatch):
    watchdog, clock = _healthy_runtime(monkeypatch)
    state = {
        "lock": threading.Lock(), "in_progress": True, "last_attempt": 100.0,
        "last_attempt_monotonic": 100.0, "started_monotonic": 100.0,
        "last_progress_monotonic": clock[0] - 95.0, "last_completed_monotonic": 0.0,
        "phase": "OHLCV_FETCH", "current_symbol": "ETC/USDT:USDT",
        "timed_out": False, "restart_requested": False,
        "last_error": "", "worker_name": "unified-institutional-watchlist",
    }
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC", 90.0)

    response = ggpbot.app.test_client().get("/health")
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["watchlist_worker"]["stalled"] is True
    assert "UNIFIED_WATCHLIST_WORKER_STALLED" in payload["process_failures"]


def test_normal_watchlist_refresh_is_not_reported_as_process_failure(monkeypatch):
    watchdog, clock = _healthy_runtime(monkeypatch)
    state = {
        "lock": threading.Lock(), "in_progress": True, "last_attempt": 100.0,
        "last_attempt_monotonic": 100.0, "started_monotonic": 100.0,
        "last_progress_monotonic": clock[0] - 2.0, "last_completed_monotonic": 0.0,
        "phase": "OHLCV_FETCH", "current_symbol": "ETC/USDT:USDT",
        "timed_out": False, "restart_requested": False,
        "last_error": "", "worker_name": "unified-institutional-watchlist",
    }
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC", 90.0)

    response = ggpbot.app.test_client().get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["watchlist_worker"]["in_progress"] is True
    assert payload["watchlist_worker"]["stalled"] is False
    assert "UNIFIED_WATCHLIST_WORKER_STALLED" not in payload["process_failures"]


def test_confirmed_pivots_returns_empty_for_short_or_truncated_frames():
    import pandas as pd
    from roro_institutional import _confirmed_pivots

    short = pd.DataFrame({"high": [1.0, 2.0, 3.0], "low": [0.5, 1.0, 2.0]})
    assert _confirmed_pivots(short, left_right=2) == ([], [])
    assert _confirmed_pivots(pd.DataFrame({"close": [1.0] * 10}), left_right=2) == ([], [])
    complete = pd.DataFrame({
        "high": [5.0, 6.0, 10.0, 9.0, 8.0],
        "low": [5.0, 4.0, 1.0, 2.0, 3.0],
    })
    assert _confirmed_pivots(complete, end=3, left_right=2) == ([], [])


def test_confirmed_pivots_skips_non_finite_windows_without_changing_valid_pivots():
    import math
    import pandas as pd
    from roro_institutional import _confirmed_pivots

    frame = pd.DataFrame({
        "high": [5.0, 6.0, 10.0, 9.0, 8.0, 7.0, 8.0],
        "low": [5.0, 4.0, 1.0, 2.0, 3.0, 4.0, 3.0],
    })
    highs, lows = _confirmed_pivots(frame, left_right=2)
    assert (2, 10.0) in highs
    assert (2, 1.0) in lows

    frame.loc[1, "high"] = math.nan
    highs, lows = _confirmed_pivots(frame, left_right=2)
    assert (2, 10.0) not in highs
    assert (2, 1.0) not in lows


def test_digitalocean_liveness_restarts_only_after_exchange_exit_gate_authorizes(monkeypatch):
    _healthy_runtime(monkeypatch)
    state = {
        "lock": threading.Lock(), "in_progress": False, "last_attempt": 100.0,
        "last_attempt_monotonic": 100.0, "started_monotonic": 100.0,
        "last_progress_monotonic": 100.0, "last_completed_monotonic": 100.0,
        "phase": "IDLE", "current_symbol": "", "timed_out": True,
        "restart_requested": True, "last_error": "WORKER_STALLED",
        "worker_name": "",
    }
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "recovery_status": "EXIT_REFUSED_UNVERIFIED_EXPOSURE",
    })
    ggpbot.STATE["open"] = True

    client = ggpbot.app.test_client()
    held = client.get("/liveness")
    assert held.status_code == 200
    assert held.get_json()["restart_authorized_by_exchange_reconciliation"] is False

    # Once the exchange exit gate records a verified FLAT result, the liveness
    # probe may ask DigitalOcean to replace this stalled process.
    ggpbot.STATE["open"] = False
    ggpbot.MEMORY["recovery_status"] = "EXIT_RECONCILED_FLAT"
    authorized = client.get("/liveness")
    assert authorized.status_code == 503
    assert authorized.get_json()["restart_authorized_by_exchange_reconciliation"] is True


def test_confirmed_cross_lifecycle_fetches_are_bounded_and_rotate(monkeypatch):
    calls = []
    progress = []

    class Journal:
        def update_cross_event_lifecycle(self, event_id, lifecycle):
            return True

    rows = [
        {
            "event_id": f"event-{i}", "symbol": f"COIN{i}",
            "exchange_symbol": f"COIN{i}/USDT:USDT", "side": "BUY",
            "metadata": {"lifecycle_schema": ggpbot.CROSS_LIFECYCLE_SCHEMA},
        }
        for i in range(5)
    ]
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", Journal())
    monkeypatch.setattr(ggpbot, "_evaluate_post_cross_rejection_breakout",
                        lambda *_a, **_k: {"entry_ready": False, "phase": "WAITING_FOR_PULLBACK"})
    monkeypatch.setattr(ggpbot, "_get_scanner_frame",
                        lambda symbol: calls.append(symbol) or {"symbol": symbol})
    monkeypatch.setattr(ggpbot, "_roro_timeframe_seconds", lambda _tf: 900)
    monkeypatch.setattr(ggpbot, "ema", lambda *_a, **_k: [])
    monkeypatch.setattr(ggpbot, "MEMORY", {"_cross_event_lifecycle_cursor": 0})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    first = ggpbot._advance_cross_event_lifecycles(
        rows, progress_callback=lambda phase, symbol: progress.append((phase, symbol)),
        max_items=2,
    )
    second = ggpbot._advance_cross_event_lifecycles(
        rows, progress_callback=lambda phase, symbol: progress.append((phase, symbol)),
        max_items=2,
    )
    third = ggpbot._advance_cross_event_lifecycles(
        rows, progress_callback=lambda phase, symbol: progress.append((phase, symbol)),
        max_items=2,
    )

    assert first == second == third == 2
    assert calls == [
        "COIN0/USDT:USDT", "COIN1/USDT:USDT",
        "COIN2/USDT:USDT", "COIN3/USDT:USDT",
        "COIN4/USDT:USDT", "COIN0/USDT:USDT",
    ]
    assert len(progress) == 12
    assert ggpbot.MEMORY["_cross_event_lifecycle_cursor"] == 1


def test_watchlist_worker_completion_and_entry_readiness_publish_atomically(monkeypatch):
    class ImmediateThread:
        def __init__(self, *, target, name, daemon, args=()):
            self.target = target
            self.args = args
            self.name = name
            self.daemon = daemon

        def start(self):
            self.target(*self.args)

    state = {
        "lock": threading.Lock(), "in_progress": False, "last_attempt": 0.0,
        "last_attempt_monotonic": -1000.0, "started_monotonic": 0.0,
        "last_progress_monotonic": 0.0, "last_completed_monotonic": -1000.0,
        "phase": "IDLE", "current_symbol": "", "timed_out": False,
        "restart_requested": False, "last_error": "", "worker_name": "",
    }
    monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
    monkeypatch.setattr(ggpbot, "_last_unified_watchlist_refresh", 0.0)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS", 20.0)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_BATCH_SIZE", 4)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS", 30)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_RESOURCE_MANAGER", None)
    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {}})
    monkeypatch.setattr(ggpbot, "threading", SimpleNamespace(
        Lock=threading.Lock, Thread=ImmediateThread, current_thread=threading.current_thread,
        get_ident=threading.get_ident,
    ))
    monkeypatch.setattr(ggpbot, "_sync_confirmed_cross_events", lambda **_kwargs: None)
    monkeypatch.setattr(ggpbot, "update_unified_watchlist", lambda *_a, **_k: {})
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda _symbol: None)
    monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *_a: {})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    ggpbot.refresh_unified_institutional_watchlist()

    assert state["in_progress"] is False
    assert state["phase"] == "IDLE"
    assert ggpbot.MEMORY["_unified_watchlist_refresh_ok"] is True


def test_closed_frame_validator_rejects_bad_rows_instead_of_repairing_snapshot():
    import numpy as np
    import pandas as pd
    from roro_opportunity_pipeline import validate_closed_frame

    count = 60
    frame = pd.DataFrame({
        "timestamp": [1_700_000_000_000 + i * 900_000 for i in range(count)],
        "open": [100.0] * count,
        "high": [101.0] * count,
        "low": [99.0] * count,
        "close": [100.5] * count,
        "volume": [10.0] * count,
    })
    now_ms = int(frame.timestamp.iloc[-1]) + 900_001
    valid = validate_closed_frame(frame, now_ms=now_ms, timeframe_s=900)
    assert len(valid) == count

    duplicate = frame.copy()
    duplicate.loc[10, "timestamp"] = duplicate.loc[9, "timestamp"]
    unordered = frame.copy()
    unordered.loc[[10, 11], "timestamp"] = unordered.loc[[11, 10], "timestamp"].to_numpy()
    infinite = frame.copy()
    infinite.loc[10, "high"] = np.inf
    malformed_ohlc = frame.copy()
    malformed_ohlc.loc[10, "high"] = 99.5
    negative_volume = frame.copy()
    negative_volume.loc[10, "volume"] = -1.0
    missing_value = frame.copy()
    missing_value.loc[10, "close"] = np.nan
    fractional_timestamp = frame.copy()
    fractional_timestamp.loc[10, "timestamp"] += 0.5

    for invalid in (
        duplicate, unordered, infinite, malformed_ohlc,
        negative_volume, missing_value, fractional_timestamp,
    ):
        assert validate_closed_frame(invalid, now_ms=now_ms, timeframe_s=900).empty


def test_closed_frame_validator_filters_only_the_still_open_candle():
    import pandas as pd
    from roro_opportunity_pipeline import validate_closed_frame

    frame = pd.DataFrame({
        "timestamp": [1_700_000_000_000 + i * 900_000 for i in range(60)],
        "open": [100.0] * 60, "high": [101.0] * 60,
        "low": [99.0] * 60, "close": [100.5] * 60, "volume": [10.0] * 60,
    })
    now_ms = int(frame.timestamp.iloc[-1]) + 900_000 - 1
    closed = validate_closed_frame(frame, now_ms=now_ms, timeframe_s=900)
    assert len(closed) == 59
    assert int(closed.timestamp.iloc[-1]) == int(frame.timestamp.iloc[-2])


def test_dashboard_uses_single_process_waitress_and_closes_cleanly(monkeypatch):
    created = []

    class FakeServer:
        def __init__(self, app, host, port, threads):
            self.app = app
            self.host = host
            self.port = port
            self.threads = threads
            self.closed = False

        def run(self):
            return None

        def close(self):
            self.closed = True

    class FakeThread:
        def __init__(self, target, daemon, name):
            self.target = target
            self.daemon = daemon
            self.name = name
            self.started = False
            self.joined = False

        def start(self):
            self.started = True

        def join(self, timeout=None):
            self.joined = True

    def fake_create_server(app, host, port, threads):
        server = FakeServer(app, host, port, threads)
        created.append(server)
        return server

    monkeypatch.setitem(sys.modules, "waitress", SimpleNamespace(create_server=fake_create_server))
    monkeypatch.setattr(ggpbot, "threading", SimpleNamespace(Thread=FakeThread))
    monkeypatch.setattr(ggpbot, "DASHBOARD_SERVERS", [])
    monkeypatch.setattr(ggpbot, "DASHBOARD_ACTIVE_PORTS", [])
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setenv("PORT", "8080")

    assert ggpbot.start_dashboard_servers() == [8080]
    assert len(created) == 1
    server, thread = ggpbot.DASHBOARD_SERVERS[0]
    assert server is created[0]
    assert server.app is ggpbot.app
    assert server.host == "0.0.0.0"
    assert server.port == 8080
    assert server.threads == 8
    assert thread.target == server.run
    assert thread.started is True

    ggpbot._stop_runtime_services()
    assert server.closed is True
    assert thread.joined is True
