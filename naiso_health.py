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
        self._lock = threading.RLock()

    def next_delay(self, name: str) -> Optional[float]:
        """Return the next bounded delay, or None when the budget is exhausted."""
        now = float(self._monotonic())
        with self._lock:
            records = self._records.setdefault(str(name), [])
            recent = [r for r in records if r.result != "PENDING" and now - r.ts <= self.window_sec]
            if len(recent) >= self.max_attempts:
                return None
            attempt = len(recent)
            base = min(self.base_delay_sec * (2 ** attempt), self.max_delay_sec)
            return max(0.05, base + base * self.jitter_fraction * self._rng.uniform(-1.0, 1.0))

    def begin(self, name: str, reason: str, delay: float) -> RestartRecord:
        record = RestartRecord(str(name), str(reason), float(self._monotonic()),
                               len(self._records.get(str(name), [])) + 1, float(delay))
        with self._lock:
            records = self._records.setdefault(str(name), [])
            records.append(record)
            # Restart history is diagnostic, not an audit ledger. Cap it so a
            # long-running process cannot grow memory without bound; the active
            # attempt budget is computed from the recent window only.
            if len(records) > self.MAX_RECORDS_PER_COMPONENT:
                del records[:len(records) - self.MAX_RECORDS_PER_COMPONENT]
        return record

    def finish(self, record: RestartRecord, result: str, detail: str = "") -> RestartRecord:
        with self._lock:
            record.result = str(result)
            record.detail = str(detail)[:300]
        return record

    def records(self, name: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            selected = self._records.items() if name is None else [(str(name), self._records.get(str(name), []))]
            rows = [r for _, values in selected for r in values]
        rows.sort(key=lambda r: r.ts, reverse=True)
        return [{
            "name": r.name, "reason": r.reason, "ts": r.ts, "attempt": r.attempt,
            "delay": round(r.delay, 3), "result": r.result, "detail": r.detail,
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


class HealthWatchdog:
    """Heartbeat-based liveness monitor with safe, bounded recovery callbacks."""

    def __init__(
        self,
        *,
        restart_policy: Optional[RestartPolicy] = None,
        alert_fn: Optional[Callable[[str, str], None]] = None,
        safety_gate: Optional[Callable[[], (bool, str)]] = None,
        check_interval_sec: float = 2.0,
        monotonic_fn=None,
        sleep_fn=None,
    ) -> None:
        self.restart_policy = restart_policy or RestartPolicy()
        self.alert_fn = alert_fn
        self.safety_gate = safety_gate
        self.check_interval_sec = max(0.2, float(check_interval_sec))
        self._monotonic = monotonic_fn or time.monotonic
        self._sleep = sleep_fn or time.sleep
        self._components: Dict[str, ComponentHealth] = {}
        self._restart_callbacks: Dict[str, Callable[[], bool]] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._alarms: Dict[str, str] = {}

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
            component.last_beat = float(self._monotonic())
            component.has_beat = True
            component.status = "HEALTHY"
            if restart_fn is not None:
                self._restart_callbacks[key] = restart_fn

    def beat(self, name: str, **diagnostics: Any) -> None:
        with self._lock:
            component = self._components.get(str(name))
            if component is None:
                self.register(name, interval_sec=self.check_interval_sec)
                component = self._components[str(name)]
            component.last_beat = float(self._monotonic())
            component.has_beat = True
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
        with self._lock:
            if self._alarms.get(key) == message:
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
            for thread_id, frame in sys._current_frames().items():
                name = threading._active.get(thread_id)
                if name is None or component.name not in name.name:
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

    def check(self) -> Dict[str, Any]:
        now = float(self._monotonic())
        report: Dict[str, Any] = {}
        with self._lock:
            components = list(self._components.values())
        for component in components:
            age = now - component.last_beat
            stalled = component.has_beat and age > max(component.grace_sec, component.interval_sec)
            if not stalled:
                component.status = "HEALTHY"
                report[component.name] = {"status": "HEALTHY", "age_sec": round(age, 3)}
                continue
            component.status = "STALLED"
            diagnostics = self._stack_snapshot(component)
            component.diagnostics.update(diagnostics)
            self._alert(f"stalled:{component.name}", "CRITICAL",
                        f"[WATCHDOG] {component.name} stalled for {age:.1f}s (critical={component.critical})")
            if not component.restartable or component.name not in self._restart_callbacks:
                report[component.name] = {"status": "STALLED", "age_sec": round(age, 3),
                                          "restart": "NOT_RESTARTABLE"}
                continue
            verification_context: Dict[str, Any] = {}
            verified_open_resume = False
            non_trading_resume = False
            if callable(component.recovery_verifier):
                try:
                    verified, verify_reason, context = component.recovery_verifier()
                    verification_context = dict(context or {})
                except Exception as exc:
                    verified, verify_reason, verification_context = False, f"VERIFIER_EXCEPTION:{type(exc).__name__}", {}
                if not verified:
                    component.status = "STALLED_SAFE_HOLD"
                    component.diagnostics["last_verification"] = str(verify_reason)[:120]
                    self._alert(f"verify_blocked:{component.name}", "WARN",
                                f"[WATCHDOG] restart of {component.name} deferred by verification: {verify_reason}")
                    report[component.name] = {"status": component.status, "age_sec": round(age, 3),
                                              "restart": "VERIFICATION_HOLD", "reason": str(verify_reason)}
                    continue
                verified_open_resume = bool(verification_context.get("open_position_verified"))
                non_trading_resume = bool(verification_context.get("non_trading_engine"))
            if callable(self.safety_gate):
                allowed, reason = self.safety_gate()
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
            try:
                restarted = self._invoke_restart(callback, verification_context)
                result = "RESTARTED" if restarted else "RESTART_FAILED"
                detail = ""
            except Exception as exc:
                restarted = False
                result = "RESTART_EXCEPTION"
                detail = f"{type(exc).__name__}: {exc}"
            self.restart_policy.finish(record, result, detail)
            component.last_restart = float(self._monotonic())
            if restarted:
                component.last_beat = float(self._monotonic())
                component.status = "RECOVERED"
                self._alert(f"recovered:{component.name}", "SUCCESS",
                            f"[WATCHDOG] {component.name} restarted after {age:.1f}s stall")
            else:
                component.status = "RESTART_FAILED"
                self._alert(f"restart_failed:{component.name}", "CRITICAL",
                            f"[WATCHDOG] {component.name} restart failed: {result} {detail}")
            report[component.name] = {"status": component.status, "age_sec": round(age, 3),
                                      "restart": result, "attempt": record.attempt}
        return report

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = float(self._monotonic())
            rows = {}
            for name, component in sorted(self._components.items()):
                rows[name] = {
                    "status": component.status,
                    "critical": component.critical,
                    "restartable": component.restartable,
                    "age_sec": round(max(0.0, now - component.last_beat), 3),
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
