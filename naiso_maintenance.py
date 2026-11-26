"""Trade-state-aware maintenance for NAISO-LIVE v1.1.0.

Heavy work (explicit GC, WAL checkpoint, retention pruning, integrity checks)
only runs when the account is flat and no safety gate is unresolved.  Light
bounded cache cleanup is always allowed, including during an open trade or a
daily-loss pause, but it never touches trade state, stops or journal rows.
"""
from __future__ import annotations

import gc
import time
from typing import Any, Callable, Dict, Optional


class MaintenanceManager:
    def __init__(
        self,
        *,
        state_provider: Callable[[], Dict[str, Any]],
        journal_provider: Callable[[], Any],
        resource_provider: Callable[[], Any],
        alert_fn: Optional[Callable[[str, str], None]] = None,
        min_interval_sec: float = 300.0,
        runtime_ttl_days: float = 30.0,
        enable_gc: bool = True,
        monotonic_fn=None,
    ) -> None:
        self.state_provider = state_provider
        self.journal_provider = journal_provider
        self.resource_provider = resource_provider
        self.alert_fn = alert_fn
        self.min_interval_sec = max(1.0, float(min_interval_sec))
        self.runtime_ttl_days = max(1.0, float(runtime_ttl_days))
        self.enable_gc = bool(enable_gc)
        self._monotonic = monotonic_fn or time.monotonic
        self._last_light = 0.0
        self._last_heavy = 0.0
        self._last_result: Dict[str, Any] = {"status": "NEVER_RUN"}
        self._history = []
        self._protected_runtime_keys = (
            "daily_drawdown_cooldown_v1", "recovery_state_v1", "recovery_hold_v1",
        )

    @staticmethod
    def trade_state_flags(state: Dict[str, Any]) -> Dict[str, bool]:
        return {
            "position_open": bool(state.get("open") or state.get("in_position")),
            "recovery_hold": bool(state.get("recovery_hold")),
            "execution_unresolved": bool(state.get("execution_unresolved")),
            "close_reconciliation_required": bool(state.get("close_reconciliation_required")),
            "native_sl_unverified": bool(state.get("open")) and not bool(state.get("native_sl_verified", True)),
            "daily_loss_cooldown": bool(state.get("daily_loss_limit_hit") or state.get("loss_cooldown_active")),
        }

    def _alert(self, message: str, level: str) -> None:
        if callable(self.alert_fn):
            try:
                self.alert_fn(message, level)
            except Exception:
                pass

    def _remember(self, result: Dict[str, Any]) -> Dict[str, Any]:
        self._last_result = result
        self._history.append(dict(result))
        if len(self._history) > 50:
            self._history.pop(0)
        return result

    def light_cleanup(self) -> Dict[str, Any]:
        """Safe bounded cache cleanup. Does not GC, checkpoint or prune journal rows."""
        manager = self.resource_provider()
        removed = None
        error = None
        if manager is not None and hasattr(manager, "cleanup"):
            try:
                removed = manager.cleanup(preserve_keys=())
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
        result = {
            "mode": "LIGHT", "ts": time.time(), "cache_removed": removed,
            "gc_collected": 0, "journal_checkpoint": None, "integrity": None,
            "error": error, "status": "OK" if error is None else "ERROR",
        }
        if error:
            self._alert(f"[MAINTENANCE] light cleanup failed: {error}", "WARN")
        return self._remember(result)

    def heavy_cleanup(self) -> Dict[str, Any]:
        """Full maintenance, only invoked when all trading safety gates are clear."""
        result = {
            "mode": "HEAVY", "ts": time.time(), "cache_removed": None,
            "gc_collected": 0, "journal_checkpoint": None, "integrity": None,
            "runtime_prune": None, "error": None, "status": "OK",
        }
        manager = self.resource_provider()
        if manager is not None and hasattr(manager, "cleanup"):
            try:
                result["cache_removed"] = manager.cleanup(preserve_keys=())
            except Exception as exc:
                result["error"] = f"cache:{type(exc).__name__}: {exc}"
        journal = self.journal_provider()
        if journal is not None:
            try:
                result["journal_checkpoint"] = journal.checkpoint_wal("PASSIVE")
            except Exception as exc:
                result["error"] = (result["error"] or "") + f" checkpoint:{type(exc).__name__}: {exc}"
            try:
                result["integrity"] = journal.integrity_check()
            except Exception as exc:
                result["error"] = (result["error"] or "") + f" integrity:{type(exc).__name__}: {exc}"
            try:
                result["runtime_prune"] = journal.prune_runtime_state(
                    ttl_days=self.runtime_ttl_days, protected_keys=self._protected_runtime_keys)
            except Exception as exc:
                result["error"] = (result["error"] or "") + f" prune:{type(exc).__name__}: {exc}"
        if self.enable_gc:
            try:
                result["gc_collected"] = int(gc.collect())
            except Exception as exc:
                result["error"] = (result["error"] or "") + f" gc:{type(exc).__name__}: {exc}"
        result["status"] = "OK" if not result["error"] else "PARTIAL"
        if result["status"] != "OK":
            self._alert(f"[MAINTENANCE] heavy cleanup partial: {result['error']}", "WARN")
        return self._remember(result)

    def run(self, *, force: bool = False) -> Dict[str, Any]:
        """Run the maximum safe maintenance level for the current trading state."""
        state = {}
        try:
            state = dict(self.state_provider() or {})
        except Exception as exc:
            state = {"maintenance_state_error": str(exc)}
        flags = self.trade_state_flags(state)
        unsafe = any(v for k, v in flags.items() if k != "daily_loss_cooldown")
        now = float(self._monotonic())
        if unsafe:
            if not force and now - self._last_light < self.min_interval_sec:
                return {"mode": "SKIPPED", "status": "THROTTLED", "flags": flags}
            self._last_light = now
            result = self.light_cleanup()
            result["flags"] = flags
            result["reason"] = "trading_safety_gate_active"
            return self._remember(result)
        if not force and now - self._last_heavy < self.min_interval_sec:
            return {"mode": "SKIPPED", "status": "THROTTLED", "flags": flags}
        self._last_heavy = now
        result = self.heavy_cleanup()
        result["flags"] = flags
        return self._remember(result)

    def metrics(self) -> Dict[str, Any]:
        return {
            "last": dict(self._last_result),
            "history": list(self._history[-10:]),
            "min_interval_sec": self.min_interval_sec,
            "runtime_ttl_days": self.runtime_ttl_days,
            "enable_gc": self.enable_gc,
        }
