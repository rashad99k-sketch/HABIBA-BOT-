"""Bounded, observational-only diagnostics for the institutional entry path.

This module deliberately has no strategy dependencies and never returns a
trading decision. Recording failures are swallowed so telemetry cannot alter
entry behavior.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Any, Dict, Optional

_LOCK = threading.RLock()
_EVENTS = deque(maxlen=5000)
_LATEST: Dict[str, Dict[str, Any]] = {}
_LOG = logging.getLogger("entry_path_diagnostics")


def _safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in list(value.items())[:40]}
    if isinstance(value, (list, tuple, set)):
        return [_safe(v) for v in list(value)[:40]]
    return str(value)[:240]


def record_entry_path_event(
    symbol: str,
    stage: str,
    *,
    side: Optional[str] = None,
    decision_state: Optional[str] = None,
    rejection_reason: Optional[str] = None,
    cross_timestamp: Optional[Any] = None,
    cross_age: Optional[Any] = None,
    details: Optional[Dict[str, Any]] = None,
    logger=None,
) -> Dict[str, Any]:
    """Record one stage without raising into the trading path."""
    try:
        now = time.time()
        ts = None
        if cross_timestamp not in (None, ""):
            try:
                ts = float(cross_timestamp)
                if ts > 10_000_000_000:
                    ts /= 1000.0
            except (TypeError, ValueError, OverflowError):
                ts = None
        age = None
        if cross_age not in (None, ""):
            try:
                age = max(0.0, float(cross_age))
            except (TypeError, ValueError, OverflowError):
                age = None
        elif ts is not None:
            age = max(0.0, now - ts)
        event = {
            "timestamp": now,
            "symbol": str(symbol or "UNKNOWN"),
            "side": str(side or "").upper() or None,
            "stage": str(stage or "UNKNOWN"),
            "cross_timestamp": ts,
            "cross_age": age,
            "decision_state": str(decision_state or "UNKNOWN"),
            "rejection_reason": str(rejection_reason or "") or None,
            "details": _safe(details or {}),
        }
        key = event["symbol"]
        with _LOCK:
            _EVENTS.append(event)
            state = dict(_LATEST.get(key) or {})
            state.update(event)
            state["last_stage"] = event["stage"]
            state["event_count"] = int(state.get("event_count", 0)) + 1
            _LATEST[key] = state
        line = "[ENTRY_PATH_DIAG] " + json.dumps(event, sort_keys=True, separators=(",", ":"))
        target = logger if callable(logger) else _LOG.info
        try:
            target(line)
        except Exception:
            _LOG.info(line)
        return dict(event)
    except Exception:
        # Telemetry must never throw into a decision or execution function.
        return {}


def get_entry_path_diagnostics(symbol: Optional[str] = None) -> Dict[str, Any]:
    with _LOCK:
        if symbol:
            return dict(_LATEST.get(str(symbol), {}))
        return {key: dict(value) for key, value in _LATEST.items()}


def get_entry_path_events(symbol: Optional[str] = None) -> list:
    with _LOCK:
        rows = list(_EVENTS)
    if symbol:
        rows = [row for row in rows if row.get("symbol") == str(symbol)]
    return [dict(row) for row in rows]


def clear_entry_path_diagnostics() -> None:
    with _LOCK:
        _EVENTS.clear()
        _LATEST.clear()
