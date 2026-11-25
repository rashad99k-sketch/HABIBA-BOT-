"""Watchdog, heartbeat registry and bounded restart policy for NAISO-LIVE.

The watchdog never kills a thread and never restarts a critical engine while a
safety gate is unresolved.  It detects stalled/hung engines, records their
stacks for diagnosis, alerts once per incident, and asks the caller-supplied
restart callback to act within a bounded exponential-backoff budget.
"""
from __future__ import annotations

import inspect
import random
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class RestartRecord:
    name: str
    reason: str
    ts: float
    attempt: int
    delay: float
    result: str = "PENDING"
    detail: str = ""
    # A deferred recovery performed no restart, so it must not consume budget.
    counts: bool = True
    # Stable diagnostic identity. Unlike ``attempt``, it never repeats after
    # deferred callbacks or diagnostic-history trimming.
    uid: int = 0


class RestartPolicy:
    """Strictly bounded restart attempts with exponential backoff and jitter."""

    MAX_RECORDS_PER_COMPONENT = 200

    def __init__(
        self,
        *,
        max_attempts: int = 3,
        window_sec: float = 300.0,
        base_delay_sec: float = 2.0,
        max_delay_sec: float = 60.0,
        jitter_fraction: float = 0.20,
        seed: Optional[int] = None,
        monotonic_fn=None,
        sleep_fn=None,
    ) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.window_sec = max(1.0, float(window_sec))
        self.base_delay_sec = max(0.1, float(base_delay_sec))
        self.max_delay_sec = max(self.base_delay_sec, float(max_delay_sec))
        self.jitter_fraction = min(0.5, max(0.0, float(jitter_fraction)))
        self._rng = random.Random(seed)
        self._monotonic = monotonic_fn or time.monotonic
        self._sleep = sleep_fn or time.sleep
        self._records: Dict[str, List[RestartRecord]] = {}
        self._attempt_seq: Dict[str, int] = {}
        self._lock = threading.RLock()

    def next_delay(self, name: str) -> Optional[float]:
        """Return the next bounded delay, or None when the budget is exhausted."""
        now = float(self._monotonic())
        with self._lock:
            records = self._records.setdefault(str(name), [])
            recent = [r for r in records if r.counts and r.result != "PENDING"
                      and now - r.ts <= self.window_sec]
            if len(recent) >= self.max_attempts:
                return None
            attempt = len(recent)
            base = min(self.base_delay_sec * (2 ** attempt), self.max_delay_sec)
            return max(0.05, base + base * self.jitter_fraction * self._rng.uniform(-1.0, 1.0))

    def begin(self, name: str, reason: str, delay: float) -> RestartRecord:
        record = RestartRecord(str(name), str(reason), float(self._monotonic()),
                               0, float(delay))
        with self._lock:
            records = self._records.setdefault(str(name), [])
            # Attempt IDs only advance on consumed attempts; deferred callbacks
            # did not restart anything and must not inflate the next ID.
            record.attempt = 1 + sum(1 for r in records if r.counts)
            self._attempt_seq[str(name)] = int(self._attempt_seq.get(str(name), 0)) + 1
            record.uid = int(self._attempt_seq[str(name)])
            records.append(record)
            # Restart history is diagnostic, not an audit ledger. Cap it so a
            # long-running process cannot grow memory without bound; the active
            # attempt budget is computed from the recent window only.
            if len(records) > self.MAX_RECORDS_PER_COMPONENT:
                del records[:len(records) - self.MAX_RECORDS_PER_COMPONENT]
        return record

    def finish(self, record: RestartRecord, result: str, detail: str = "",
               *, counts: bool = True) -> RestartRecord:
        with self._lock:
            record.result = str(result)
            record.detail = str(detail)[:300]
            record.counts = bool(counts)
        return record

    def records(self, name: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            selected = self._records.items() if name is None else [(str(name), self._records.get(str(name), []))]
            rows = [r for _, values in selected for r in values]
        rows.sort(key=lambda r: r.ts, reverse=True)
        return [{
            "name": r.name, "reason": r.reason, "ts": r.ts, "attempt": r.attempt,
            "uid": int(r.uid),
            "delay": round(r.delay, 3), "result": r.result, "detail": r.detail,
            "counts": bool(r.counts),
        } for r in rows[:max(1, int(limit))]]

    def reset(self, name: str) -> None:
        with self._lock:
            self._records.pop(str(name), None)


@dataclass
class ComponentHealth:
    name: str
    interval_sec: float
    grace_sec: float
    critical: bool = True
    restartable: bool = False
    last_beat: float = 0.0
    has_beat: bool = False
    last_restart: float = 0.0
    status: str = "UNKNOWN"
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    recovery_verifier: Optional[Callable[[], tuple]] = None
    lifecycle: str = "RUNNING"
    last_progress: float = 0.0
    idle_until: float = 0.0
    blocked_until: float = 0.0
    reason: str = ""
    thread_name: str = ""
    cycle_id: int = 0
    resume_requires_liveness: bool = False
    # One verifier per component at a time. A timed-out verifier is abandoned,
    # not duplicated; a permanently blocked verifier fails closed until it exits
    # or the process is restarted.
    verifier_thread: Optional[threading.Thread] = None
    verification_generation: int = 0


class HealthWatchdog:
    """Heartbeat-based liveness monitor with safe, bounded recovery callbacks."""

    def __init__(
        self,
        *,
        restart_policy: Optional[RestartPolicy] = None,
        alert_fn: Optional[Callable[[str, str], None]] = None,
        safety_gate: Optional[Callable[[], (bool, str)]] = None,
        check_interval_sec: float = 2.0,
        verification_retry_sec: float = 30.0,
        verification_max_attempts: int = 3,
        verification_hold_max_sec: float = 120.0,
        thread_alive_fn=None,
        monotonic_fn=None,
        sleep_fn=None,
    ) -> None:
        self.restart_policy = restart_policy or RestartPolicy()
        self.alert_fn = alert_fn
        self.safety_gate = safety_gate
        self.check_interval_sec = max(0.2, float(check_interval_sec))
        # Bounded backoff for *failed verification*, never for stall detection or
        # grace windows. A real progress beat re-arms verification immediately.
        self.verification_retry_sec = max(1.0, float(verification_retry_sec))
        self.verification_max_attempts = max(1, int(verification_max_attempts))
        self.verification_hold_max_sec = max(self.verification_retry_sec,
                                             float(verification_hold_max_sec))
        self._thread_alive = thread_alive_fn or self._default_thread_alive
        self._monotonic = monotonic_fn or time.monotonic
        self._sleep = sleep_fn or time.sleep
        self._components: Dict[str, ComponentHealth] = {}
        self._restart_callbacks: Dict[str, Callable[[], bool]] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._alarms: Dict[str, str] = {}

    def monotonic(self) -> float:
        """Expose the injected clock so callers can schedule real deadlines."""
        return float(self._monotonic())

    @staticmethod
    def _default_thread_alive(name: str) -> bool:
        target = str(name or "")
        if not target:
            return True
        return any(thread.name == target for thread in threading.enumerate())

    def register(
        self,
        name: str,
        *,
        interval_sec: float,
        grace_sec: Optional[float] = None,
        critical: bool = True,
        restartable: bool = False,
        restart_fn: Optional[Callable[[], bool]] = None,
        recovery_verifier: Optional[Callable[[], tuple]] = None,
        lifecycle: str = "RUNNING",
        refresh: bool = True,
        resume_requires_liveness: bool = False,
    ) -> None:
        key = str(name)
        grace = float(grace_sec if grace_sec is not None else max(3.0, float(interval_sec) * 2.0))
        with self._lock:
            component = self._components.get(key)
            if component is None:
                component = ComponentHealth(key, float(interval_sec), grace, bool(critical), bool(restartable))
                self._components[key] = component
            else:
                component.interval_sec = float(interval_sec)
                component.grace_sec = grace
                component.critical = bool(critical)
                component.restartable = bool(restartable)
            component.recovery_verifier = recovery_verifier
            component.resume_requires_liveness = bool(resume_requires_liveness)
            if refresh:
                now = float(self._monotonic())
                component.last_beat = now
                component.last_progress = now
                component.has_beat = True
                component.status = "HEALTHY"
                component.lifecycle = str(lifecycle or "RUNNING")
                component.idle_until = 0.0
                component.blocked_until = 0.0
                component.reason = ""
            elif component.last_progress <= 0:
                component.last_progress = float(self._monotonic())
            if restart_fn is not None:
                self._restart_callbacks[key] = restart_fn

    def beat(self, name: str, **diagnostics: Any) -> None:
        """Record a real progress point plus optional lifecycle metadata.

        ``state`` is one of RUNNING/IDLE/BLOCKED/FAILED. ``idle_until`` and
        ``blocked_until`` are absolute monotonic deadlines. ``progress=False``
        records a status-only observation without advancing last_progress.
        """
        with self._lock:
            component = self._components.get(str(name))
            if component is None:
                self.register(name, interval_sec=self.check_interval_sec)
                component = self._components[str(name)]
            now = float(self._monotonic())
            is_progress = diagnostics.pop("progress", True) is not False
            if is_progress:
                component.last_beat = now
                component.last_progress = now
                component.has_beat = True
                # Dedupe within one incident, not forever. Real progress after a
                # stall/recovery re-arms the same component for a future event.
                if component.status in {"STALLED", "STALLED_SAFE_HOLD", "RECOVERED",
                                        "RESTART_FAILED", "FAILED", "RECOVERY_DEFERRED",
                                        "IDLE_SAFE_HOLD"}:
                    for alarm_key in (f"stalled:{component.name}", f"failed:{component.name}",
                                      f"recovery_deferred:{component.name}",
                                      f"verify_blocked:{component.name}",
                                      f"restart_failed:{component.name}",
                                      f"schedule_blocked:{component.name}",
                                      f"schedule_failed:{component.name}"):
                        self._alarms.pop(alarm_key, None)
                    for key in ("verification_next_attempt", "verification_attempts",
                                "verification_last_reason", "verification_progress_at",
                                "verification_first_attempt_ts", "verification_exhausted",
                                "verification_id", "verification_generation"):
                        component.diagnostics.pop(key, None)
            state = diagnostics.pop("state", None)
            if state is not None and str(state).upper() in {"RUNNING", "IDLE", "BLOCKED", "FAILED"}:
                component.lifecycle = str(state).upper()
                if component.lifecycle == "RUNNING":
                    component.idle_until = 0.0
                    component.blocked_until = 0.0
            reason = diagnostics.pop("reason", None)
            if reason is not None:
                component.reason = str(reason)[:200]
            idle_until = diagnostics.pop("idle_until", None)
            if idle_until is not None:
                component.idle_until = float(idle_until)
            blocked_until = diagnostics.pop("blocked_until", None)
            if blocked_until is not None:
                component.blocked_until = float(blocked_until)
            phase = diagnostics.pop("phase", None)
            if phase is not None:
                diagnostics["last_phase"] = phase
            thread_name = diagnostics.pop("thread_name", None)
            if thread_name is not None:
                component.thread_name = str(thread_name)[:120]
            cycle_id = diagnostics.pop("cycle_id", None)
            if cycle_id is not None:
                try:
                    component.cycle_id = int(cycle_id)
                except (TypeError, ValueError):
                    pass
            if component.lifecycle == "FAILED":
                component.status = "FAILED"
            elif is_progress:
                component.status = "HEALTHY"
            if diagnostics:
                component.diagnostics.update({k: str(v)[:120] for k, v in diagnostics.items()})

    def start(self) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="naiso-health-watchdog", daemon=True)
            self._thread.start()
            return True

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.1, float(timeout)))

    def _run(self) -> None:
        while not self._stop.wait(self.check_interval_sec):
            try:
                self.check()
            except Exception as exc:
                self._alert("watchdog_error", "ERROR", f"{type(exc).__name__}: {exc}")

    def _alert(self, key: str, level: str, message: str) -> None:
        """Alert once per event key, even when the message embeds a changing age.

        Stall messages include an age that grows on every watchdog tick. Comparing
        the full message caused the same stall to alert repeatedly and spam the
        operator channel without representing a new event.
        """
        with self._lock:
            if key in self._alarms:
                return
            self._alarms[key] = message
        if callable(self.alert_fn):
            try:
                self.alert_fn(message, level)
            except Exception:
                pass

    def _stack_snapshot(self, component: ComponentHealth) -> Dict[str, Any]:
        stacks: Dict[str, str] = {}
        try:
            short_name = component.name.split(":", 1)[-1]
            for thread_id, frame in sys._current_frames().items():
                name = threading._active.get(thread_id)
                if name is None:
                    continue
                matches_component = (component.name in name.name or short_name in name.name)
                matches_foreground = (component.name == "main_loop" and name.name == "MainThread")
                if not matches_component and not matches_foreground:
                    continue
                stacks[str(thread_id)] = "".join(traceback.format_stack(frame))[-2000:]
        except Exception as exc:
            stacks["error"] = f"{type(exc).__name__}: {exc}"
        return {"possible_stacks": stacks}

    @staticmethod
    def _invoke_restart(callback: Callable, context: Dict[str, Any]) -> bool:
        """Pass verification context only to callbacks that accept it."""
        try:
            signature = inspect.signature(callback)
            params = signature.parameters
            accepts_context = ("context" in params or
                               any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()))
        except (TypeError, ValueError):
            accepts_context = False
        return bool(callback(context) if accepts_context else callback())

    @staticmethod
    def _verifier_bound_sec(component: ComponentHealth) -> float:
        """Verifier bound is strictly inside the component's existing grace window."""
        grace = max(0.0, float(component.grace_sec))
        return round(min(5.0, max(0.01, grace / 2.0), grace) if grace > 0 else 0.01, 4)

    def _run_recovery_verifier(self, component: ComponentHealth):
        """Run at most one verifier per component on a bounded daemon worker.

        Returns ``(verified, reason, context, timed_out)``. A verifier that hangs
        is abandoned after the bounded join and reported as ``VERIFIER_TIMEOUT``
        with no restart attempt consumed. The timed-out daemon thread is tracked
        until it exits: no second verifier can start for the same component while
        it is alive, so repeated incidents cannot accumulate blocked verifiers.
        A permanently blocked verifier therefore fails closed until process
        restart rather than being replaced with a newer overlapping worker.
        """
        with self._lock:
            active = component.verifier_thread
            if active is not None and active.is_alive():
                generation = int(component.verification_generation)
                return False, "VERIFIER_ALREADY_RUNNING", {
                    "verifier_already_running": True,
                    "verification_generation": generation,
                    "verification_id": f"{component.name}:v{generation}",
                    "verifier_thread_name": str(active.name),
                    "component": component.name,
                }, False
            component.verification_generation = int(component.verification_generation) + 1
            generation = int(component.verification_generation)
            base_context = {
                "component": component.name,
                "verification_generation": generation,
                "verification_id": f"{component.name}:v{generation}",
            }

            def verification_context(**extra):
                payload = dict(base_context)
                payload.update(extra)
                return payload

            outcome: Dict[str, Any] = {}

            def worker():
                try:
                    outcome["value"] = component.recovery_verifier()
                except BaseException as exc:
                    outcome["exception"] = f"{type(exc).__name__}: {exc}"
                    outcome["traceback"] = traceback.format_exc()[-1200:]
                finally:
                    with self._lock:
                        if component.verification_generation == generation:
                            component.verifier_thread = None

            thread = threading.Thread(
                target=worker, daemon=True,
                name=f"naiso-verifier-{component.name}-g{generation}"[-80:])
            component.verifier_thread = thread

        bound = self._verifier_bound_sec(component)
        try:
            thread.start()
        except BaseException:
            with self._lock:
                if component.verification_generation == generation:
                    component.verifier_thread = None
            raise
        thread.join(bound)
        if thread.is_alive():
            return False, "VERIFIER_TIMEOUT", verification_context(
                verifier_blocked=True, verifier_timeout=True,
                verifier_timeout_sec=bound, verifier_thread_name=thread.name), True
        if "exception" in outcome:
            detail = str(outcome["exception"])
            kind = detail.split(":", 1)[0]
            return False, f"VERIFIER_EXCEPTION:{kind}", verification_context(
                verifier_exception_detail=detail,
                verifier_exception_traceback=outcome.get("traceback", "")), False
        value = outcome.get("value")
        try:
            verified, reason, result_context = value
        except (TypeError, ValueError):
            return False, "VERIFIER_INVALID_RESULT", verification_context(), False
        merged = dict(result_context or {})
        merged.update(base_context)  # generation and component are watchdog-owned
        return bool(verified), str(reason), merged, False

    def check(self) -> Dict[str, Any]:
        now = float(self._monotonic())
        report: Dict[str, Any] = {}
        with self._lock:
            components = list(self._components.values())
        for component in components:
            lifecycle = component.lifecycle or "RUNNING"
            progress_at = max(component.last_beat, component.last_progress or 0.0)
            base_age = max(0.0, now - progress_at)
            scheduled_due = None
            if lifecycle == "IDLE":
                scheduled_due = component.idle_until or (progress_at + component.interval_sec)
                if now < scheduled_due:
                    component.status = "IDLE"
                    report[component.name] = {
                        "status": "IDLE", "lifecycle": "IDLE", "age_sec": round(base_age, 3),
                        "next_due_sec": round(max(0.0, scheduled_due - now), 3),
                        "reason": component.reason,
                    }
                    continue
            elif lifecycle == "BLOCKED":
                # A BLOCKED deadline is a bounded status, not a liveness extension.
                # It is hard-capped at the component's existing grace window from
                # the last REAL progress beat, so it cannot hide a dead thread or
                # a permanently hung call.
                hard_deadline = progress_at + component.grace_sec
                requested_deadline = float(component.blocked_until or hard_deadline)
                scheduled_due = min(requested_deadline, hard_deadline)
                if component.thread_name and not self._thread_alive(component.thread_name):
                    component.status = "FAILED"
                    self._alert(
                        f"thread_dead:{component.name}", "ERROR",
                        f"[WATCHDOG] {component.name} blocked phase lost its thread "
                        f"{component.thread_name}; safety hold",
                    )
                    report[component.name] = {
                        "status": component.status, "lifecycle": "BLOCKED",
                        "age_sec": round(base_age, 3), "restart": "SAFETY_HOLD",
                        "reason": "BLOCKED_THREAD_DEAD",
                        "blocked_reason": component.reason,
                        "thread_name": component.thread_name,
                    }
                    continue
                if now < scheduled_due:
                    component.status = "BLOCKED"
                    report[component.name] = {
                        "status": "BLOCKED", "lifecycle": "BLOCKED", "age_sec": round(base_age, 3),
                        "next_due_sec": round(max(0.0, scheduled_due - now), 3),
                        "hard_deadline_sec": round(max(0.0, hard_deadline - now), 3),
                        "reason": component.reason,
                        "thread_name": component.thread_name,
                    }
                    continue
                # Past the hard deadline, fall through with no scheduled_due so
                # the normal grace-based stall path evaluates the REAL last
                # progress timestamp instead of the status-only deadline.
                scheduled_due = None
            if lifecycle == "IDLE" and scheduled_due is not None and now >= scheduled_due:
                # A scheduled worker is allowed to wake exactly when due. This is
                # not a stall/restart: it must not consume the restart budget or
                # emit a false STALLED alert after a healthy 20-minute sleep.
                verification_context: Dict[str, Any] = {}
                verified = True
                verify_reason = "SCHEDULED_DUE"
                if callable(component.recovery_verifier):
                    verified, verify_reason, raw_context, _timed_out = self._run_recovery_verifier(component)
                    verification_context = dict(raw_context or {})
                if not verified:
                    component.status = "IDLE_SAFE_HOLD"
                    component.idle_until = now + min(60.0, component.interval_sec * 0.25)
                    self._alert(f"schedule_blocked:{component.name}", "WARN",
                                f"[WATCHDOG] scheduled restart of {component.name} deferred: {verify_reason}")
                    report[component.name] = {"status": component.status, "lifecycle": "IDLE",
                                              "age_sec": round(now - scheduled_due, 3),
                                              "restart": "VERIFICATION_HOLD", "reason": str(verify_reason),
                                              "verification_id": verification_context.get("verification_id", ""),
                                              "verification_generation": int(
                                                  verification_context.get("verification_generation", 0) or 0),
                                              "verifier_timeout": bool(_timed_out)}
                    continue
                if callable(self.safety_gate):
                    allowed, reason = self.safety_gate()
                    if not allowed and not (
                        verification_context.get("flat_verified") and
                        str(reason) in {"OPEN_POSITION", "OPEN_POSITION_VERIFIED"}
                    ):
                        component.status = "IDLE_SAFE_HOLD"
                        component.idle_until = now + min(60.0, component.interval_sec * 0.25)
                        self._alert(f"schedule_blocked:{component.name}", "WARN",
                                    f"[WATCHDOG] scheduled restart of {component.name} deferred: {reason}")
                        report[component.name] = {"status": component.status, "lifecycle": "IDLE",
                                                  "age_sec": round(now - scheduled_due, 3),
                                                  "restart": "SAFETY_GATE", "reason": str(reason)}
                        continue
                callback = self._restart_callbacks.get(component.name)
                restarted = False
                if callable(callback):
                    try:
                        restarted = self._invoke_restart(callback, verification_context)
                    except Exception as exc:
                        self._alert(f"schedule_failed:{component.name}", "ERROR",
                                    f"[WATCHDOG] scheduled restart of {component.name} failed: "
                                    f"{type(exc).__name__}: {exc}")
                    if restarted:
                        # The dispatched cycle is now the liveness source. Leave
                        # IDLE, clear the stale wake deadline and let the new
                        # cycle's beats govern the next check; otherwise every
                        # subsequent tick would try to start another cycle.
                        component.lifecycle = "RUNNING"
                        component.idle_until = 0.0
                        component.last_beat = now
                        component.last_progress = now
                        component.has_beat = True
                        component.status = "RECOVERED"
                        report[component.name] = {"status": component.status, "lifecycle": "RUNNING",
                                                  "age_sec": round(now - scheduled_due, 3),
                                                  "restart": "SCHEDULED"}
                        continue
                component.status = "IDLE_SAFE_HOLD"
                component.idle_until = now + min(60.0, component.interval_sec * 0.25)
                self._alert(f"schedule_failed:{component.name}", "ERROR",
                            f"[WATCHDOG] scheduled restart of {component.name} unavailable")
                report[component.name] = {"status": component.status, "lifecycle": "IDLE",
                                          "age_sec": round(now - scheduled_due, 3),
                                          "restart": "SCHEDULE_UNAVAILABLE"}
                continue
            age = max(0.0, now - scheduled_due) if scheduled_due is not None else base_age
            # RUNNING progress and scheduled due dates are judged against grace,
            # not against a 20-minute scheduling interval. Using the larger
            # interval here hid genuine in-cycle stalls for scheduled workers.
            stall_window = component.grace_sec
            stalled = component.has_beat and age > stall_window
            if not stalled:
                component.status = "FAILED" if lifecycle == "FAILED" else "HEALTHY"
                report[component.name] = {"status": component.status, "lifecycle": lifecycle,
                                          "age_sec": round(age, 3), "reason": component.reason}
                continue
            component.status = "FAILED" if lifecycle == "FAILED" else "STALLED"
            diagnostics = self._stack_snapshot(component)
            component.diagnostics.update(diagnostics)
            if lifecycle == "FAILED":
                self._alert(f"failed:{component.name}", "ERROR",
                            f"[WATCHDOG] {component.name} failed: {component.reason or 'unknown error'}")
            else:
                self._alert(f"stalled:{component.name}", "CRITICAL",
                            f"[WATCHDOG] {component.name} stalled for {age:.1f}s (critical={component.critical})")
            if not component.restartable or component.name not in self._restart_callbacks:
                report[component.name] = {"status": component.status, "lifecycle": lifecycle,
                                          "age_sec": round(age, 3), "restart": "NOT_RESTARTABLE",
                                          "reason": component.reason}
                continue
            verification_context: Dict[str, Any] = {}
            verified_open_resume = False
            non_trading_resume = False
            verifier_timed_out = False
            if callable(component.recovery_verifier):
                diag = component.diagnostics
                marker_raw = diag.get("verification_progress_at")
                marker = -1.0 if marker_raw is None else float(marker_raw)
                if marker != float(progress_at):
                    # New real progress (or a different stall incident) re-arms
                    # verification; a blocked verifier is never retried forever.
                    for key in ("verification_next_attempt", "verification_attempts",
                                "verification_last_reason", "verification_progress_at",
                                "verification_first_attempt_ts", "verification_exhausted",
                                "verification_id", "verification_generation"):
                        diag.pop(key, None)
                if diag.get("verification_exhausted"):
                    component.status = "STALLED_SAFE_HOLD"
                    first = float(diag.get("verification_first_attempt_ts", now))
                    report[component.name] = {
                        "status": component.status, "age_sec": round(age, 3),
                        "restart": "VERIFICATION_EXHAUSTED",
                        "reason": str(diag.get("verification_last_reason") or ""),
                        "verification_attempts": int(diag.get("verification_attempts", 0) or 0),
                        "verification_hold_sec": round(max(0.0, now - first), 3),
                        "verification_id": str(diag.get("verification_id", "")),
                        "verification_generation": int(diag.get("verification_generation", 0) or 0),
                        "caller": diag.get("verification_caller", ""),
                        "component": diag.get("verification_component", component.name),
                        "symbol_status": diag.get("verification_symbol_status", ""),
                    }
                    continue
                next_attempt = float(diag.get("verification_next_attempt", 0.0) or 0.0)
                if next_attempt > now:
                    component.status = "STALLED_SAFE_HOLD"
                    remaining = round(next_attempt - now, 3)
                    report[component.name] = {
                        "status": component.status, "age_sec": round(age, 3),
                        "restart": "VERIFICATION_BACKOFF",
                        "reason": str(diag.get("verification_last_reason") or ""),
                        "next_verification_sec": remaining,
                        "verification_attempts": int(diag.get("verification_attempts", 0) or 0),
                        "verification_id": str(diag.get("verification_id", "")),
                        "verification_generation": int(diag.get("verification_generation", 0) or 0),
                        "caller": diag.get("verification_caller", ""),
                        "component": diag.get("verification_component", component.name),
                    }
                    continue
                verified, verify_reason, context, verifier_timed_out = self._run_recovery_verifier(component)
                verification_context = dict(context or {})
                if verification_context.get("verification_id"):
                    diag["verification_id"] = str(verification_context["verification_id"])[:80]
                    diag["verification_generation"] = int(
                        verification_context.get("verification_generation", 0) or 0)
                if verifier_timed_out:
                    # Fail closed before any restart budget is touched.
                    verification_context["verifier_timeout"] = True
                    verification_context["verifier_blocked"] = True
                if not verified:
                    component.status = "STALLED_SAFE_HOLD"
                    diag["last_verification"] = str(verify_reason)[:120]
                    diag["verification_last_reason"] = str(verify_reason)[:120]
                    diag["verification_attempts"] = int(diag.get("verification_attempts", 0) or 0) + 1
                    diag.setdefault("verification_first_attempt_ts", now)
                    diag["verification_next_attempt"] = now + self.verification_retry_sec
                    diag["verification_progress_at"] = float(progress_at)
                    for key in ("caller", "component", "verification_source", "symbol_source",
                                "symbol_status"):
                        if key in verification_context:
                            diag[f"verification_{key}"] = str(verification_context[key])[:120]
                    attempts = int(diag["verification_attempts"])
                    hold_sec = max(0.0, now - float(diag["verification_first_attempt_ts"]))
                    exhausted = (attempts >= self.verification_max_attempts or
                                 hold_sec >= self.verification_hold_max_sec)
                    diag["verification_exhausted"] = exhausted
                    self._alert(f"verify_blocked:{component.name}", "WARN",
                                f"[WATCHDOG] restart of {component.name} deferred by verification: {verify_reason}")
                    verification_report = {
                        "status": component.status, "age_sec": round(age, 3),
                        "restart": "VERIFICATION_EXHAUSTED" if exhausted else "VERIFICATION_HOLD",
                        "reason": str(verify_reason),
                        "next_verification_sec": round(self.verification_retry_sec, 3),
                        "verification_attempts": attempts,
                        "verification_hold_sec": round(hold_sec, 3),
                        "verification_id": verification_context.get("verification_id", ""),
                        "verification_generation": int(
                            verification_context.get("verification_generation", 0) or 0),
                        "verifier_already_running": bool(
                            verification_context.get("verifier_already_running")),
                        "verifier_timeout": bool(verifier_timed_out),
                        "verifier_timeout_sec": (self._verifier_bound_sec(component)
                                                 if verifier_timed_out else None),
                        "caller": verification_context.get("caller", "direct"),
                        "component": verification_context.get("component", component.name),
                        "symbol_status": verification_context.get("symbol_status", ""),
                    }
                    if exhausted:
                        self._alert(
                            f"verification_exhausted:{component.name}", "ERROR",
                            f"[WATCHDOG] {component.name} recovery verification exhausted "
                            f"after {attempts} attempts / {hold_sec:.1f}s: {verify_reason}; "
                            f"keeping safety hold",
                        )
                    report[component.name] = verification_report
                    continue
                verified_open_resume = bool(verification_context.get("open_position_verified"))
                non_trading_resume = bool(verification_context.get("non_trading_engine"))
            safety_gate_reason = "NOT_CONFIGURED"
            if callable(self.safety_gate):
                allowed, reason = self.safety_gate()
                safety_gate_reason = str(reason)
                if not allowed and not (
                    verified_open_resume and str(reason) in {"OPEN_POSITION", "OPEN_POSITION_VERIFIED"}
                ) and not (
                    non_trading_resume and str(reason) in {"OPEN_POSITION", "OPEN_POSITION_VERIFIED"}
                ):
                    component.status = "STALLED_SAFE_HOLD"
                    self._alert(f"restart_blocked:{component.name}", "WARN",
                                f"[WATCHDOG] restart of {component.name} deferred: {reason}")
                    report[component.name] = {"status": component.status, "age_sec": round(age, 3),
                                              "restart": "SAFETY_GATE", "reason": str(reason)}
                    continue
            delay = self.restart_policy.next_delay(component.name)
            if delay is None:
                component.status = "RESTART_BUDGET_EXHAUSTED"
                self._alert(f"budget:{component.name}", "CRITICAL",
                            f"[WATCHDOG] restart budget exhausted for {component.name}")
                report[component.name] = {"status": component.status, "age_sec": round(age, 3),
                                          "restart": "BUDGET_EXHAUSTED"}
                continue
            self._sleep(delay)
            record = self.restart_policy.begin(component.name, f"stalled_{age:.1f}s", delay)
            callback = self._restart_callbacks[component.name]
            previous_status = component.status
            exception_detail = ""
            exception_traceback = ""
            try:
                restarted = self._invoke_restart(callback, verification_context)
                result = "RESTARTED" if restarted else "RESTART_FAILED"
                detail = ""
            except Exception as exc:
                restarted = False
                result = "RESTART_EXCEPTION"
                detail = f"{type(exc).__name__}: {exc}"
                exception_detail = detail
                exception_traceback = traceback.format_exc()[-1200:]
            liveness_confirmed = (
                bool(verification_context.get("engine_resumed"))
                if component.resume_requires_liveness else True)
            if restarted and not liveness_confirmed:
                # Foreground engines such as main_loop cannot be replaced by an
                # extra thread. Recovery work may have completed, but the loop
                # itself has not proved liveness; never refresh its heartbeat.
                restarted = False
                result = "RECOVERY_DEFERRED"
                detail = "engine liveness not confirmed; no duplicate thread started"
            # No restart happened on deferral, so the attempt must not be charged.
            consumed = result != "RECOVERY_DEFERRED"
            self.restart_policy.finish(record, result, detail, counts=consumed)
            component.last_restart = float(self._monotonic())
            if restarted:
                component.last_beat = float(self._monotonic())
                component.status = "RECOVERED"
                self._alert(f"recovered:{component.name}", "SUCCESS",
                            f"[WATCHDOG] {component.name} restarted after {age:.1f}s stall")
            elif result == "RECOVERY_DEFERRED":
                component.status = "STALLED_SAFE_HOLD"
                self._alert(f"recovery_deferred:{component.name}", "WARN",
                            f"[WATCHDOG] {component.name} recovery deferred: {detail}")
            else:
                component.status = "RESTART_FAILED"
                thread_name = str(component.thread_name or "UNKNOWN")
                thread_alive = self._thread_alive(component.thread_name) if component.thread_name else False
                guard_reason = str(verification_context.get("restart_guard_reason") or
                                   verification_context.get("restart_guard") or
                                   component.diagnostics.get("restart_guard_reason") or "NOT_REPORTED")
                recovery_guard_reason = str(
                    verification_context.get("recovery_guard_reason") or
                    component.diagnostics.get("recovery_guard_reason") or "NOT_REPORTED")
                diagnostic = (
                    f"attempt={component.name}:{record.uid} status={component.status} "
                    f"previous_status={previous_status} lifecycle={lifecycle} "
                    f"thread={thread_name} thread_alive={thread_alive} "
                    f"restart_guard={guard_reason} safety_gate={safety_gate_reason} "
                    f"recovery_guard={recovery_guard_reason}")
                if exception_detail:
                    diagnostic += f" exception={exception_detail[:200]}"
                self._alert(f"restart_failed:{component.name}", "CRITICAL",
                            f"[WATCHDOG] {component.name} restart failed: {result} {detail} "
                            f"| {diagnostic}")
                report[component.name] = {
                    "status": component.status, "previous_status": previous_status,
                    "lifecycle": lifecycle, "age_sec": round(age, 3),
                    "restart": result, "attempt": record.attempt,
                    "attempt_id": f"{component.name}:{record.uid}",
                    "attempt_uid": int(record.uid),
                    "thread_name": thread_name, "thread_alive": bool(thread_alive),
                    "restart_guard": guard_reason, "safety_gate_reason": safety_gate_reason,
                    "recovery_guard_reason": recovery_guard_reason,
                    "exception_detail": exception_detail,
                    "exception_traceback": exception_traceback,
                    "recent_restarts": self.restart_policy.records(component.name, limit=5),
                }
                continue
            report[component.name] = {"status": component.status, "age_sec": round(age, 3),
                                      "restart": result, "attempt": record.attempt,
                                      "attempt_id": f"{component.name}:{record.uid}",
                                      "attempt_uid": int(record.uid), "counts": bool(consumed)}
        return report

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = float(self._monotonic())
            rows = {}
            for name, component in sorted(self._components.items()):
                progress_at = max(component.last_beat, component.last_progress or 0.0)
                rows[name] = {
                    "status": component.status,
                    "lifecycle": component.lifecycle,
                    "critical": component.critical,
                    "restartable": component.restartable,
                    "age_sec": round(max(0.0, now - progress_at), 3),
                    "reason": component.reason,
                    "thread_name": component.thread_name,
                    "cycle_id": component.cycle_id,
                    "next_due_sec": (round(max(0.0, (component.idle_until or component.blocked_until) - now), 3)
                                     if (component.idle_until or component.blocked_until) else None),
                    "last_restart_age_sec": (round(max(0.0, now - component.last_restart), 3)
                                             if component.last_restart else None),
                    "diagnostics": dict(component.diagnostics),
                }
            return {
                "running": bool(self._thread is not None and self._thread.is_alive()),
                "components": rows,
                "restart_history": self.restart_policy.records(limit=30),
                "alerts": dict(self._alarms),
            }
