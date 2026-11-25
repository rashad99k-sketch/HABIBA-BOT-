"""Standalone, deterministic runtime registry for six simultaneous positions.

This module complements ``portfolio_allocator``: the allocator owns durable slot
claims, while this runtime owns *in-memory state isolation* for a bot whose
entry points still mutate a single set of global dictionaries and a singleton
manager object.

Design invariants
-----------------
* Import-safe: no ``ggpbot`` import at module import time.
* ``adopt`` deep-copies the currently focused global state under a validated
  trade id. No caller label is trusted for class derivation;
  ``portfolio_allocator.classify_slot`` is the only classifier.
* Capacity and class counts always come from ``portfolio_allocator`` constants.
  Malformed, duplicate, unknown-class and over-capacity adopts fail closed.
* ``focus`` swaps the supplied dictionaries in place (identity preserved) for
  one position at a time, and holds a lock for the whole cycle so concurrent
  cycles cannot interleave.
* A position is removed on focus exit only after explicit verified-flat
  finalization evidence; otherwise the mutated state is saved back and the
  previous focus is restored.
* The singleton manager is swapped only through an explicit allowlist; engine
  collaborators (event bus, exchange sync, board, ...) are never touched.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import threading
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Tuple

from portfolio_allocator import (
    CAPACITY,
    CLASS_NEWS,
    CLASS_TECHNICAL,
    SLOT_CLASSES,
    TOTAL_CAPACITY,
    FlatEvidenceError,
    InvalidReservationRequest,
    UnknownClassHintError,
    _TRUSTED_HINT_KEYS,
    _collect_evidence,
    _validate_identifier,
    classify_slot,
)

__all__ = [
    "PortfolioRuntime",
    "PortfolioRuntimeError",
    "AdoptResult",
    "ReleaseResult",
    "ManageAllSummary",
    "MANAGER_STATE_KEYS",
]


class PortfolioRuntimeError(Exception):
    """Raised for invalid focus/manager operations (always fail closed)."""


@dataclass(frozen=True)
class AdoptResult:
    """Result of :meth:`PortfolioRuntime.adopt`; truthy only on real adoption."""

    ok: bool
    reason: str
    trade_id: str = ""
    symbol: str = ""
    slot_class: Optional[str] = None
    slot_index: Optional[int] = None

    def __bool__(self) -> bool:
        return self.ok

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "slot_class": self.slot_class,
            "slot_index": self.slot_index,
        }


@dataclass(frozen=True)
class ReleaseResult:
    """Result of :meth:`PortfolioRuntime.release`; truthy only on removal."""

    ok: bool
    reason: str
    trade_id: str
    slot_class: Optional[str] = None

    def __bool__(self) -> bool:
        return self.ok


@dataclass(frozen=True)
class ManageAllSummary:
    """Deterministic cycle summary. ``closed``/``failed`` are subsets of ``processed``."""

    processed: Tuple[str, ...] = ()
    closed: Tuple[str, ...] = ()
    failed: Tuple[Tuple[str, str], ...] = ()

    @property
    def failed_ids(self) -> Tuple[str, ...]:
        return tuple(trade_id for trade_id, _ in self.failed)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "processed": list(self.processed),
            "closed": list(self.closed),
            "failed": [{"trade_id": tid, "reason": reason} for tid, reason in self.failed],
        }


@dataclass
class _Position:
    """One isolated position snapshot; never exposed by ``status``."""

    trade_id: str
    symbol: str
    slot_class: str
    slot_index: int
    state: Dict[str, Any]
    trade_state: Dict[str, Any]
    dashboard_state: Dict[str, Any]
    manager_state: Dict[str, Any]


_MANAGER_FIELDS: Tuple[str, ...] = (
    "lifecycle_state",
    "last_management_ts",
    "last_log_ts",
    "last_live_debug_ts",
    "last_heavy_calc_ts",
    "last_position_sync_ts",
    "current_snapshot",
    "_last_decision",
    "last_decision_blocked_reason",
)
MANAGER_STATE_KEYS = frozenset(_MANAGER_FIELDS + ("brain.current_state",))


def _lifecycle_name(value: Any) -> str:
    """Normalize plain strings and enums to a single uppercase lifecycle token."""
    if value is None:
        return ""
    raw = getattr(value, "value", value)
    return str(raw).strip().upper().rsplit(".", 1)[-1]


def _copy_dict(value: Mapping[str, Any]) -> Dict[str, Any]:
    return deepcopy(dict(value))


class PortfolioRuntime:
    """Registry that isolates per-position global state for six simultaneous trades.

    ``state_dict``, ``trade_state_dict`` and ``dashboard_state`` are mutated in
    place by :meth:`focus`; their object identities must remain stable so the
    rest of the bot observes the swap. Passing ``lock`` (an RLock is safest)
    additionally serializes cycles against external holders of that lock.
    """

    def __init__(
        self,
        state_dict: MutableMapping[str, Any],
        trade_state_dict: Optional[MutableMapping[str, Any]] = None,
        dashboard_state: Optional[MutableMapping[str, Any]] = None,
        manager: Any = None,
        lock: Any = None,
    ) -> None:
        if not isinstance(state_dict, MutableMapping):
            raise TypeError("state_dict must be a mutable mapping")
        for name, value in (
            ("trade_state_dict", trade_state_dict),
            ("dashboard_state", dashboard_state),
        ):
            if value is not None and not isinstance(value, MutableMapping):
                raise TypeError(f"{name} must be a mutable mapping or None")
        self.state = state_dict
        self.trade_state = trade_state_dict if trade_state_dict is not None else {}
        self.dashboard_state = dashboard_state if dashboard_state is not None else {}
        self._manager = manager
        self._external_lock = lock
        self._guard = threading.RLock()
        self._local = threading.local()
        self._positions: Dict[str, _Position] = {}
        self._focus_stack: List[str] = []
        self._focused_trade_id: Optional[str] = None

    # --- locking -----------------------------------------------------------
    @contextmanager
    def _locked(self) -> Iterator[None]:
        """Serialize runtime operations, including the whole body of ``focus``.

        The external lock is acquired once per outermost runtime operation;
        nested calls from ``manager_fn`` stay reentrant through ``_guard``.
        """
        depth = int(getattr(self._local, "depth", 0))
        self._guard.acquire()
        external_acquired = False
        if depth == 0 and self._external_lock is not None:
            self._external_lock.acquire()
            external_acquired = True
        self._local.depth = depth + 1
        try:
            yield
        finally:
            self._local.depth -= 1
            if external_acquired:
                self._external_lock.release()
            self._guard.release()

    # --- singleton manager allowlist --------------------------------------
    def capture_manager_state(self) -> Dict[str, Any]:
        """Deep-copy only allowlisted mutable manager fields."""
        manager = self._manager
        if manager is None:
            return {}
        with self._locked():
            snapshot: Dict[str, Any] = {}
            for field_name in _MANAGER_FIELDS:
                if hasattr(manager, field_name):
                    snapshot[field_name] = deepcopy(getattr(manager, field_name))
            brain = getattr(manager, "brain", None)
            if brain is not None and hasattr(brain, "current_state"):
                snapshot["brain.current_state"] = deepcopy(brain.current_state)
            return snapshot

    def apply_manager_state(self, manager_state: Mapping[str, Any]) -> None:
        """Apply only allowlisted fields; reject unknown keys instead of guessing."""
        if not isinstance(manager_state, Mapping):
            raise PortfolioRuntimeError("manager_state_must_be_a_mapping")
        unknown = sorted(str(key) for key in manager_state if key not in MANAGER_STATE_KEYS)
        if unknown:
            raise PortfolioRuntimeError("manager_state_unknown_keys:" + ",".join(unknown))
        with self._locked():
            manager = self._manager
            if manager is None:
                return
            for field_name in _MANAGER_FIELDS:
                if field_name in manager_state and hasattr(manager, field_name):
                    setattr(manager, field_name, deepcopy(manager_state[field_name]))
            if "brain.current_state" in manager_state:
                brain = getattr(manager, "brain", None)
                if brain is not None and hasattr(brain, "current_state"):
                    brain.current_state = deepcopy(manager_state["brain.current_state"])

    def _sanitize_manager_state(self, manager_state: Any) -> Dict[str, Any]:
        if manager_state is None:
            return self.capture_manager_state()
        if not isinstance(manager_state, Mapping):
            raise PortfolioRuntimeError("manager_state_must_be_a_mapping")
        unknown = sorted(str(key) for key in manager_state if key not in MANAGER_STATE_KEYS)
        if unknown:
            raise PortfolioRuntimeError("manager_state_unknown_keys:" + ",".join(unknown))
        return {key: deepcopy(value) for key, value in manager_state.items()}

    # --- snapshot helpers --------------------------------------------------
    def _capture_globals(self) -> Dict[str, Any]:
        return {
            "state": _copy_dict(self.state),
            "trade_state": _copy_dict(self.trade_state),
            "dashboard_state": _copy_dict(self.dashboard_state),
            "manager_state": self.capture_manager_state(),
        }

    @staticmethod
    def _replace_dict(target: MutableMapping[str, Any], values: Mapping[str, Any]) -> None:
        target.clear()
        target.update(deepcopy(dict(values)))

    def _apply_globals(self, snapshot: Mapping[str, Any]) -> None:
        self._replace_dict(self.state, snapshot["state"])
        self._replace_dict(self.trade_state, snapshot["trade_state"])
        self._replace_dict(self.dashboard_state, snapshot["dashboard_state"])
        self.apply_manager_state(snapshot.get("manager_state", {}))

    @staticmethod
    def _entry_snapshot(entry: _Position) -> Dict[str, Any]:
        return {
            "state": _copy_dict(entry.state),
            "trade_state": _copy_dict(entry.trade_state),
            "dashboard_state": _copy_dict(entry.dashboard_state),
            "manager_state": deepcopy(entry.manager_state),
        }

    def _save_current_into(self, entry: _Position) -> None:
        entry.state = _copy_dict(self.state)
        entry.trade_state = _copy_dict(self.trade_state)
        entry.dashboard_state = _copy_dict(self.dashboard_state)
        entry.manager_state = self.capture_manager_state()

    def _counts_unlocked(self) -> Dict[str, int]:
        counts = {slot_class: 0 for slot_class in SLOT_CLASSES}
        used_slots: set[Tuple[str, int]] = set()
        for entry in self._positions.values():
            if entry.slot_class not in CAPACITY:
                raise PortfolioRuntimeError("state_invalid:unknown_slot_class")
            index = entry.slot_index
            if isinstance(index, bool) or not isinstance(index, int):
                raise PortfolioRuntimeError("state_invalid:malformed_slot_index")
            if index < 1 or index > CAPACITY[entry.slot_class]:
                raise PortfolioRuntimeError("state_invalid:slot_index_out_of_range")
            key = (entry.slot_class, index)
            if key in used_slots:
                raise PortfolioRuntimeError("state_invalid:duplicate_slot")
            used_slots.add(key)
            counts[entry.slot_class] += 1
        counts["TOTAL"] = len(self._positions)
        return counts

    def _is_finalized_flat(self) -> bool:
        """True only with explicit flat evidence plus a closed/finalized marker."""
        if self.state.get("open") is not False:
            return False
        if self.trade_state.get("in_position") is True:
            return False
        if self.state.get("close_verified") is True or self.state.get("last_close_verified") is True:
            return True
        manager = self._manager
        if manager is not None and _lifecycle_name(getattr(manager, "lifecycle_state", None)) == "CLOSED":
            return True
        if _lifecycle_name(self.dashboard_state.get("lifecycle_state")) == "CLOSED":
            return True
        return False

    # --- public API --------------------------------------------------------
    def adopt(
        self,
        trade_id: Any,
        symbol: Any,
        *,
        class_hint: Any = None,
        manager_state: Any = None,
    ) -> AdoptResult:
        """Adopt the currently focused global state as one isolated position."""
        try:
            trade_value = _validate_identifier(trade_id, "trade_id")
            symbol_value = _validate_identifier(symbol, "symbol")
        except InvalidReservationRequest as exc:
            return AdoptResult(False, str(exc))

        with self._locked():
            if trade_value in self._positions:
                return AdoptResult(False, "duplicate_trade_id", trade_value, symbol_value)
            if self._focus_stack and trade_value != self._focus_stack[-1]:
                return AdoptResult(
                    False, "adopt_during_foreign_focus", trade_value, symbol_value
                )

            hint = class_hint
            if hint is None:
                hint = {key: self.state[key] for key in _TRUSTED_HINT_KEYS if key in self.state}
                if not hint:
                    return AdoptResult(False, "class_hint_required", trade_value, symbol_value)
            try:
                slot_class = classify_slot(hint)
                if slot_class not in CAPACITY:
                    return AdoptResult(False, "state_invalid:unknown_slot_class", trade_value, symbol_value)
                counts = self._counts_unlocked()
            except UnknownClassHintError as exc:
                return AdoptResult(False, str(exc), trade_value, symbol_value)
            except PortfolioRuntimeError as exc:
                return AdoptResult(False, str(exc), trade_value, symbol_value)

            # Once all six slots are occupied, the total cap is the binding
            # constraint. Before that, report the specific class-capacity breach.
            if counts["TOTAL"] >= TOTAL_CAPACITY:
                return AdoptResult(False, "total_capacity_exceeded", trade_value, symbol_value)
            if counts[slot_class] >= CAPACITY[slot_class]:
                return AdoptResult(False, "class_capacity_exceeded", trade_value, symbol_value)

            used = {
                entry.slot_index
                for entry in self._positions.values()
                if entry.slot_class == slot_class
            }
            slot_index = next(
                index for index in range(1, CAPACITY[slot_class] + 1) if index not in used
            )
            try:
                manager_snapshot = self._sanitize_manager_state(manager_state)
                entry = _Position(
                    trade_id=trade_value,
                    symbol=symbol_value,
                    slot_class=slot_class,
                    slot_index=slot_index,
                    state=_copy_dict(self.state),
                    trade_state=_copy_dict(self.trade_state),
                    dashboard_state=_copy_dict(self.dashboard_state),
                    manager_state=manager_snapshot,
                )
            except (PortfolioRuntimeError, TypeError, ValueError) as exc:
                return AdoptResult(
                    False, f"manager_state_invalid:{exc}", trade_value, symbol_value
                )

            self._positions[trade_value] = entry
            self._focused_trade_id = trade_value
            return AdoptResult(
                True, "adopted", trade_value, symbol_value, slot_class, slot_index
            )

    def release(self, trade_id: Any, *, verified_flat: Any) -> ReleaseResult:
        """Remove the currently focused position with explicit verified-flat evidence."""
        try:
            trade_value = _validate_identifier(trade_id, "trade_id")
        except InvalidReservationRequest as exc:
            return ReleaseResult(False, str(exc), str(trade_id))
        try:
            evidence = _collect_evidence(verified_flat, None)
        except FlatEvidenceError as exc:
            return ReleaseResult(False, f"flat_evidence_invalid:{exc}", trade_value)

        with self._locked():
            if self._focused_trade_id != trade_value:
                return ReleaseResult(False, "not_focused_trade", trade_value)
            entry = self._positions.get(trade_value)
            if entry is None:
                return ReleaseResult(False, "unknown_trade_id", trade_value)
            snapshot_positions = evidence.get("snapshot_positions")
            if snapshot_positions is not None:
                target = entry.symbol.upper()
                if target not in snapshot_positions or snapshot_positions[target] != 0.0:
                    return ReleaseResult(
                        False, "snapshot_does_not_confirm_flat", trade_value, entry.slot_class
                    )
            del self._positions[trade_value]
            return ReleaseResult(True, "released", trade_value, entry.slot_class)

    @contextmanager
    def focus(self, trade_id: Any) -> Iterator["PortfolioRuntime"]:
        """Atomically swap global state to one position for a whole management cycle."""
        try:
            trade_value = _validate_identifier(trade_id, "trade_id")
        except InvalidReservationRequest as exc:
            raise PortfolioRuntimeError(str(exc)) from None

        with self._locked():
            if self._focus_stack:
                raise PortfolioRuntimeError("nested_focus_rejected")
            entry = self._positions.get(trade_value)
            if entry is None:
                raise PortfolioRuntimeError("unknown_trade_id")
            previous_globals = self._capture_globals()
            previous_focused = self._focused_trade_id
            self._apply_globals(self._entry_snapshot(entry))
            self._focus_stack.append(trade_value)
            self._focused_trade_id = trade_value
            try:
                yield self
            except BaseException:
                # A failed cycle is never treated as finalization: keep the
                # position and save whatever state was reached.
                self._finish_focus(
                    trade_value, previous_globals, previous_focused, allow_close=False
                )
                raise
            else:
                self._finish_focus(
                    trade_value, previous_globals, previous_focused, allow_close=True
                )

    def _finish_focus(
        self,
        trade_id: str,
        previous_globals: Mapping[str, Any],
        previous_focused: Optional[str],
        *,
        allow_close: bool,
    ) -> None:
        try:
            entry = self._positions.get(trade_id)
            if entry is not None:
                if allow_close and self._is_finalized_flat():
                    del self._positions[trade_id]
                else:
                    self._save_current_into(entry)
        finally:
            try:
                self._apply_globals(previous_globals)
            finally:
                if self._focus_stack and self._focus_stack[-1] == trade_id:
                    self._focus_stack.pop()
                self._focused_trade_id = previous_focused

    def manage_all(self, manager_fn: Any) -> ManageAllSummary:
        """Run ``manager_fn()`` once per stable trade-id snapshot, isolating failures."""
        if not callable(manager_fn):
            raise TypeError("manager_fn must be callable")
        with self._locked():
            trade_ids = tuple(sorted(self._positions))
        processed: List[str] = []
        closed: List[str] = []
        failed: List[Tuple[str, str]] = []
        for trade_id in trade_ids:
            processed.append(trade_id)
            try:
                with self._locked():
                    present = trade_id in self._positions
                if not present:
                    failed.append((trade_id, "missing_before_cycle"))
                    continue
                with self.focus(trade_id):
                    manager_fn()
            except Exception as exc:
                failed.append((trade_id, f"{type(exc).__name__}: {exc}"))
                continue
            with self._locked():
                if trade_id not in self._positions:
                    closed.append(trade_id)
        return ManageAllSummary(tuple(processed), tuple(closed), tuple(failed))

    def status(self) -> Dict[str, Any]:
        """Compact deterministic view; never includes position snapshots."""
        with self._locked():
            try:
                counts = self._counts_unlocked()
            except PortfolioRuntimeError as exc:
                return {
                    "ok": False,
                    "reason": str(exc),
                    "focused": self._focused_trade_id,
                    "counts": {slot_class: 0 for slot_class in (*SLOT_CLASSES, "TOTAL")},
                    "capacity": {**CAPACITY, "TOTAL": TOTAL_CAPACITY},
                    "active": [],
                }
            active = [
                {
                    "trade_id": entry.trade_id,
                    "symbol": entry.symbol,
                    "slot_class": entry.slot_class,
                    "slot_index": entry.slot_index,
                }
                for entry in sorted(self._positions.values(), key=lambda item: item.trade_id)
            ]
            return {
                "ok": True,
                "focused": self._focused_trade_id,
                "counts": {key: counts[key] for key in (*SLOT_CLASSES, "TOTAL")},
                "capacity": {**CAPACITY, "TOTAL": TOTAL_CAPACITY},
                "active": active,
            }
