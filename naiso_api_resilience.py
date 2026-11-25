"""Shared API resilience primitives for NAISO-LIVE v1.1.0.

This module is deliberately independent of the trading engine so it can be
unit-tested without exchange credentials.  It does not replace the existing
``ggpbot.rate_limit`` scheduler; that scheduler remains the single global
request pacer.  This module adds the missing circuit breaker, bounded
exponential backoff with jitter, explicit error classification and observability.

Safety rules:

* Order-writing calls are never retried automatically.
* A circuit breaker only suppresses *reads* after a high, endpoint-scoped
  failure budget is exhausted.  It does not cancel, modify or create orders.
* UNKNOWN transport results are never converted into a business rejection.
"""
from __future__ import annotations

import errno
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple


ORDER_WRITE_METHODS = frozenset({
    "create_order", "create_market_order", "create_limit_order",
    "cancel_order", "cancel_all_orders", "swapV1PrivatePostTradeCancelReplace",
})

# These failures cannot be repaired by repeating the same request. Retrying them
# wastes budget and may hide a configuration or authorization problem.
NON_RETRYABLE_PATTERNS = (
    "authentication", "unauthorized", "permission", "forbidden", "api-key",
    "api key", "invalid symbol", "invalidsymbol", "bad symbol", "symbol not found",
    "unknown symbol", "does not have market symbol", "not have market symbol",
    "market symbol :", "invalid signature", "signature error", "timestamp",
    "recvwindow", "configuration", "misconfigured", "invalid parameter",
    "invalid order", "unsupported", "insufficient balance", "insufficient funds",
    "insufficient margin", "not enough balance",
)

# Only confirmed transport/transient classes may open a read circuit. Business,
# configuration and unknown application failures are surfaced but never treated
# as evidence that the exchange endpoint is down.
CIRCUIT_FAILURE_KINDS = frozenset({"RATE_LIMITED", "TIMEOUT", "NETWORK", "SERVER_TRANSIENT"})

TRANSIENT_PATTERNS = (
    "429", "100410", "rate limit", "too many request", "timeout", "timedout",
    "connection", "network", "temporarily unavailable", "service unavailable",
    "gateway", "internal server error", "500", "502", "503", "504",
)

# Bare digits in a quantity ("need 500 USDT") are not HTTP status evidence.
_STATUS_CODE_PATTERN = re.compile(
    r"(?:http|status(?:\s+code)?|error)\D{0,12}\b(?:500|502|503|504)\b",
    re.IGNORECASE,
)

# CCXT and requests are optional at import time: when unavailable, message
# fallback still works but typed exceptions are never guessed from a name.
try:
    import ccxt as _ccxt
except Exception:  # pragma: no cover - exercised only in minimal installs
    _ccxt = None

try:
    from requests.exceptions import ConnectionError as _RequestsConnectionError
    from requests.exceptions import Timeout as _RequestsTimeout
except Exception:  # pragma: no cover - exercised only in minimal installs
    _RequestsConnectionError = ()
    _RequestsTimeout = ()


def _ccxt_types(*names: str) -> Tuple[type, ...]:
    if _ccxt is None:
        return ()
    result = []
    for name in names:
        value = getattr(_ccxt, name, None)
        if isinstance(value, type) and value not in result:
            result.append(value)
    return tuple(result)


_CCXT_RATE_LIMIT_TYPES = _ccxt_types("RateLimitExceeded", "DDoSProtection")
_CCXT_TIMEOUT_TYPES = _ccxt_types("RequestTimeout")
_CCXT_NETWORK_TYPES = _ccxt_types(
    "NetworkError", "ExchangeNotAvailable", "OnMaintenance", "OperationFailed",
)
# Specific business errors must be checked before the generic ExchangeError:
# ccxt.RateLimitExceeded and RequestTimeout inherit from NetworkError, while
# BadSymbol inherits from BadRequest/ExchangeError.
_CCXT_BUSINESS_TYPES = _ccxt_types(
    "AuthenticationError", "PermissionDenied", "AccountSuspended",
    "InsufficientFunds", "BadSymbol", "BadRequest", "InvalidOrder", "InvalidNonce",
)
_CCXT_GENERIC_BUSINESS_TYPES = _ccxt_types("ExchangeError")
_CCXT_UNCERTAIN_TYPES = _ccxt_types("CancelPending")

_BUILTIN_BUSINESS_TYPES = tuple(
    item for item in (PermissionError,) if isinstance(item, type)
)
_BUILTIN_TIMEOUT_TYPES = tuple(
    item for item in (TimeoutError, _RequestsTimeout) if isinstance(item, type)
)
_BUILTIN_NETWORK_TYPES = tuple(
    item for item in (_RequestsConnectionError, ConnectionError) if isinstance(item, type)
)
_NETWORK_ERRNOS = frozenset({
    errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED, errno.EPIPE,
    errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH,
})
_NETWORK_OS_ERROR_TOKENS = (
    "econnreset", "econnrefused", "econnaborted", "epipe", "broken pipe",
    "connection reset", "connection refused", "network is unreachable",
)


def _is_network_os_error(exc: BaseException) -> bool:
    if not isinstance(exc, OSError):
        return False
    if getattr(exc, "errno", None) in _NETWORK_ERRNOS:
        return True
    lowered = str(exc).lower()
    return any(token in lowered for token in _NETWORK_OS_ERROR_TOKENS)

# Lower rank wins when an exception chain contains multiple signals. Business
# and scheduler failures must beat retryable transport signals; unknown errors
# must never mask a typed retryable cause.
_CLASSIFICATION_RANK = {
    "LOCAL_SCHEDULER_CONTENTION": 0,
    "NON_RETRYABLE_BUSINESS": 0,
    "ORDER_EXECUTION_UNKNOWN": 0,
    "RATE_LIMITED": 1,
    "TIMEOUT": 2,
    "NETWORK": 3,
    "SERVER_TRANSIENT": 4,
    "API_FAILURE": 5,
}


class CircuitOpenError(RuntimeError):
    """Raised when an endpoint is temporarily fenced off after repeated failures."""


@dataclass(frozen=True)
class ApiFailureClass:
    kind: str
    retryable: bool
    detail: str = ""


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    """Walk cause/context safely; never loop on a self-referential chain."""
    seen = set()
    node = exc
    for _ in range(8):
        if node is None or id(node) in seen:
            return
        seen.add(id(node))
        yield node
        next_node = node.__cause__
        if next_node is None and not getattr(node, "__suppress_context__", False):
            next_node = node.__context__
        node = next_node


def _is_local_scheduler_error(exc: BaseException) -> bool:
    # Match subclasses and dynamically generated types by walking the MRO. The
    # name fallback preserves compatibility with synthetic test exceptions.
    return any(base.__name__ == "ApiRateLockTimeout" for base in type(exc).__mro__)


def _classify_one(exc: BaseException) -> ApiFailureClass:
    name = type(exc).__name__
    lowered = str(exc).lower()

    if _is_local_scheduler_error(exc):
        return ApiFailureClass("LOCAL_SCHEDULER_CONTENTION", False, name)
    # An exchange cancellation that may still be pending is never treated as a
    # transport retry: only reconciliation can resolve the order state.
    if isinstance(exc, _CCXT_UNCERTAIN_TYPES):
        return ApiFailureClass("ORDER_EXECUTION_UNKNOWN", False, name)

    # Typed signals take precedence over text. Check the narrow transport
    # subclasses first so their broad ExchangeError/NetworkError bases cannot
    # misclassify them as business failures or generic network errors.
    if isinstance(exc, _CCXT_RATE_LIMIT_TYPES):
        return ApiFailureClass("RATE_LIMITED", True, name)
    if isinstance(exc, _CCXT_TIMEOUT_TYPES + _BUILTIN_TIMEOUT_TYPES):
        return ApiFailureClass("TIMEOUT", True, name)
    if isinstance(exc, _CCXT_NETWORK_TYPES + _BUILTIN_NETWORK_TYPES):
        return ApiFailureClass("NETWORK", True, name)
    if _is_network_os_error(exc):
        return ApiFailureClass("NETWORK", True, name)
    if isinstance(exc, _CCXT_BUSINESS_TYPES + _BUILTIN_BUSINESS_TYPES
                  + _CCXT_GENERIC_BUSINESS_TYPES):
        return ApiFailureClass("NON_RETRYABLE_BUSINESS", False, name)

    if any(pattern in lowered for pattern in NON_RETRYABLE_PATTERNS):
        return ApiFailureClass("NON_RETRYABLE_BUSINESS", False, name)
    if any(pattern in lowered for pattern in TRANSIENT_PATTERNS[:4]):
        return ApiFailureClass("RATE_LIMITED", True, name)
    if "timeout" in lowered or "timedout" in lowered or name in {"ReadTimeout", "ConnectTimeout"}:
        return ApiFailureClass("TIMEOUT", True, name)
    if any(pattern in lowered for pattern in (
            "connection", "network", "temporarily unavailable",
            "service unavailable", "gateway")):
        return ApiFailureClass("NETWORK", True, name)
    if (_STATUS_CODE_PATTERN.search(str(exc))
            or "internal server error" in lowered):
        return ApiFailureClass("SERVER_TRANSIENT", True, name)
    # Unknown application errors are not assumed safe to repeat.
    return ApiFailureClass("API_FAILURE", False, name)


def classify_api_error(exc: BaseException, *, method: str = "") -> ApiFailureClass:
    """Classify transport/rate-limit/business failures without guessing fills.

    Typed CCXT/builtin exceptions take precedence over message text. Wrapped
    exceptions are inspected through their cause/context chain, and an unknown
    outer wrapper never masks a decisive retryable or business inner cause.
    """
    if method in ORDER_WRITE_METHODS:
        return ApiFailureClass(
            "ORDER_EXECUTION_UNKNOWN", False,
            f"{type(exc).__name__}: {str(exc)[:160]}",
        )

    chain = list(_exception_chain(exc))
    candidates = [_classify_one(item) for item in chain]
    best = min(candidates, key=lambda item: _CLASSIFICATION_RANK.get(item.kind, 99))
    detail = " -> ".join(
        f"{type(item).__name__}: {str(item)[:80]}" for item in chain[:3]
    )
    return ApiFailureClass(best.kind, best.retryable, detail[:480])


@dataclass
class _EndpointState:
    failures: int = 0
    opened_at: float = 0.0
    half_open_inflight: int = 0
    last_failure_kind: str = ""
    last_failure_at: float = 0.0


class ApiResilience:
    """Endpoint-scoped circuit breaker plus bounded retry timing.

    ``sleep_fn``/``monotonic_fn`` are injectable to keep tests deterministic.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 10,
        cooldown_sec: float = 20.0,
        max_attempts: int = 3,
        base_backoff_sec: float = 0.25,
        max_backoff_sec: float = 4.0,
        jitter_fraction: float = 0.20,
        seed: Optional[int] = None,
        sleep_fn=None,
        monotonic_fn=None,
    ) -> None:
        self.failure_threshold = max(2, int(failure_threshold))
        self.cooldown_sec = max(1.0, float(cooldown_sec))
        self.max_attempts = max(1, min(5, int(max_attempts)))
        self.base_backoff_sec = max(0.0, float(base_backoff_sec))
        self.max_backoff_sec = max(self.base_backoff_sec, float(max_backoff_sec))
        self.jitter_fraction = min(0.5, max(0.0, float(jitter_fraction)))
        self._rng = random.Random(seed)
        self._sleep = sleep_fn or time.sleep
        self._monotonic = monotonic_fn or time.monotonic
        self._lock = threading.RLock()
        self._endpoints: Dict[str, _EndpointState] = {}
        self._counters: Dict[str, int] = {
            "requests": 0,
            "rate_limited": 0,
            "timeouts": 0,
            "failures": 0,
            "successes": 0,
            "retries": 0,
            "circuit_opens": 0,
            "circuit_rejections": 0,
            "half_open_successes": 0,
            "business_rejections": 0,
            "scheduler_contention": 0,
        }
        self._enabled = True

    @property
    def lock(self):
        return self._lock

    def set_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._enabled = bool(enabled)
            if self._enabled:
                self._endpoints.clear()

    def before_call(self, method: str) -> None:
        """Fail fast when an endpoint circuit is open; hand off half-open probes."""
        if not self._enabled:
            return
        with self._lock:
            state = self._endpoints.setdefault(str(method), _EndpointState())
            now = float(self._monotonic())
            if state.failures >= self.failure_threshold:
                if state.half_open_inflight > 0:
                    self._counters["circuit_rejections"] += 1
                    raise CircuitOpenError(f"{method}: circuit half-open probe already in flight")
                if now - state.opened_at < self.cooldown_sec:
                    self._counters["circuit_rejections"] += 1
                    raise CircuitOpenError(
                        f"{method}: circuit open for {self.cooldown_sec:.1f}s after "
                        f"{state.failures} failures ({state.last_failure_kind})"
                    )
                state.half_open_inflight = 1

    def record_success(self, method: str) -> None:
        with self._lock:
            state = self._endpoints.setdefault(str(method), _EndpointState())
            self._counters["successes"] += 1
            if state.half_open_inflight > 0 or state.failures >= self.failure_threshold:
                self._counters["half_open_successes"] += 1
            state.failures = 0
            state.opened_at = 0.0
            state.half_open_inflight = 0
            state.last_failure_kind = ""

    def record_failure(self, method: str, exc: BaseException) -> ApiFailureClass:
        kind = classify_api_error(exc, method=str(method))
        counts_toward_circuit = kind.kind in CIRCUIT_FAILURE_KINDS
        with self._lock:
            state = self._endpoints.setdefault(str(method), _EndpointState())
            now = float(self._monotonic())
            self._counters["failures"] += 1
            if kind.kind == "RATE_LIMITED":
                self._counters["rate_limited"] += 1
            elif kind.kind == "TIMEOUT":
                self._counters["timeouts"] += 1
            elif kind.kind == "LOCAL_SCHEDULER_CONTENTION":
                self._counters["scheduler_contention"] += 1
            elif not counts_toward_circuit:
                self._counters["business_rejections"] += 1
            state.last_failure_kind = kind.kind
            state.last_failure_at = now
            # A half-open probe is released whether it succeeds, fails with a
            # transport error, or is rejected by a business rule.
            state.half_open_inflight = 0
            if counts_toward_circuit:
                state.failures += 1
                if state.failures >= self.failure_threshold and state.opened_at <= 0:
                    state.opened_at = now
                    self._counters["circuit_opens"] += 1
                elif state.opened_at > 0:
                    # A failed half-open transport probe restarts the full
                    # cooldown. Cooldown expiry alone never proves exchange health.
                    state.opened_at = now
            # Business/configuration failures are recorded for diagnostics but
            # never open, extend or clear a transport circuit.
        return kind

    def backoff_delay(self, attempt: int, *, rate_limited: bool = False) -> float:
        """Exponential backoff with bounded jitter; attempt is 0-based."""
        exponent = min(int(attempt), 6)
        base = self.base_backoff_sec * (2 ** exponent) if rate_limited else self.base_backoff_sec * (1.5 ** exponent)
        base = min(base, self.max_backoff_sec)
        jitter = base * self.jitter_fraction * self._rng.uniform(-1.0, 1.0)
        return max(0.01, base + jitter)

    def sleep_backoff(self, attempt: int, *, rate_limited: bool = False) -> float:
        delay = self.backoff_delay(attempt, rate_limited=rate_limited)
        self._sleep(delay)
        return delay

    def count_request(self) -> None:
        with self._lock:
            self._counters["requests"] += 1

    def count_retry(self) -> None:
        with self._lock:
            self._counters["retries"] += 1

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            now = float(self._monotonic())
            endpoints = {}
            for method, state in sorted(self._endpoints.items()):
                open_remaining = 0.0
                if state.failures >= self.failure_threshold and state.opened_at > 0:
                    open_remaining = max(0.0, self.cooldown_sec - (now - state.opened_at))
                endpoints[method] = {
                    "failures": state.failures,
                    "state": ("OPEN" if open_remaining > 0 else
                              "HALF_OPEN" if state.failures >= self.failure_threshold else "CLOSED"),
                    "open_remaining_sec": round(open_remaining, 3),
                    "last_failure_kind": state.last_failure_kind,
                    "last_failure_age_sec": round(now - state.last_failure_at, 3) if state.last_failure_at else None,
                }
            return {
                "enabled": self._enabled,
                "failure_threshold": self.failure_threshold,
                "cooldown_sec": self.cooldown_sec,
                "max_attempts": self.max_attempts,
                "counters": dict(self._counters),
                "endpoints": endpoints,
            }
