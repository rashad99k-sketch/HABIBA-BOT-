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

import random
import re
import threading
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional


ORDER_WRITE_METHODS = frozenset({
    "create_order", "create_market_order", "create_limit_order",
    "cancel_order", "cancel_all_orders", "swapV1PrivatePostTradeCancelReplace",
})

# These failures cannot be repaired by repeating the same request. Retrying them
# wastes budget and may hide a configuration or authorization problem.
NON_RETRYABLE_PATTERNS = (
    "authentication", "unauthorized", "permission", "forbidden", "api-key",
    "api key", "invalid symbol", "invalidsymbol", "symbol not found",
    "unknown symbol", "does not have market symbol", "not have market symbol",
    "market symbol :", "invalid signature", "signature error", "timestamp",
    "recvwindow", "configuration", "misconfigured", "invalid parameter",
    "invalid order", "unsupported", "insufficient balance", "insufficient margin",
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


class CircuitOpenError(RuntimeError):
    """Raised when an endpoint is temporarily fenced off after repeated failures."""


@dataclass(frozen=True)
class ApiFailureClass:
    kind: str
    retryable: bool
    detail: str = ""


_HTTP_STATUS_RE = re.compile(
    r"\b(?:HTTP(?:\s+status)?|status(?:_code)?|status\s+code)\s*[:=]?\s*(\d{3})\b",
    re.IGNORECASE,
)


def _extract_http_status(exc: BaseException) -> Optional[int]:
    """Extract an actual response status without treating URL text as status."""
    for obj in (exc, getattr(exc, "response", None)):
        if obj is None:
            continue
        if isinstance(obj, dict):
            candidates = (obj.get("status_code"), obj.get("status"), obj.get("http_status"))
        else:
            candidates = (
                getattr(obj, "status_code", None),
                getattr(obj, "status", None),
                getattr(obj, "http_status", None),
            )
        for candidate in candidates:
            try:
                value = int(candidate)
            except (TypeError, ValueError):
                continue
            if 100 <= value <= 599:
                return value
    match = _HTTP_STATUS_RE.search(str(exc))
    if match:
        return int(match.group(1))
    return None


def _structured_error_text(exc: BaseException) -> str:
    """Return exchange-provided error fields, excluding request URLs."""
    values = []
    for obj in (getattr(exc, "last_json", None), getattr(exc, "body", None),
                getattr(exc, "response", None)):
        if isinstance(obj, dict):
            for key in ("code", "msg", "message", "error", "errorCode", "error_code"):
                if obj.get(key) is not None:
                    values.append(str(obj[key]))
    return " ".join(values)


def _sanitized_business_text(exc: BaseException) -> str:
    """Remove URLs/query strings before inspecting incidental business words."""
    text = str(exc)
    text = re.sub(r"https?://[^\s]+", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?:\?|&)(?:[A-Za-z0-9_.-]+)=[^\s&]+", " ", text)
    return text.lower()


def retry_after_seconds(exc: BaseException, *, now: Optional[float] = None) -> Optional[float]:
    """Read numeric or HTTP-date Retry-After from a response, when supplied."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) if response is not None else None
    if headers is None and isinstance(response, dict):
        headers = response.get("headers")
    if not isinstance(headers, dict):
        headers = getattr(exc, "headers", None)
    if not isinstance(headers, dict):
        return None
    raw = next((value for key, value in headers.items() if str(key).lower() == "retry-after"), None)
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        try:
            reference = time.time() if now is None else float(now)
            return max(0.0, parsedate_to_datetime(str(raw)).timestamp() - reference)
        except (TypeError, ValueError, OverflowError):
            return None


def classify_api_error(exc: BaseException, *, method: str = "") -> ApiFailureClass:
    """Classify failures in response/type/structured-message priority order."""
    name = type(exc).__name__
    text = str(exc)
    lowered = _sanitized_business_text(exc)
    detail = f"{name}: {text[:160]}"

    # A write result is ambiguous whenever the exchange call raises. Preserve
    # the no-blind-retry contract even if the response itself was a 5xx.
    if method in ORDER_WRITE_METHODS:
        return ApiFailureClass("ORDER_EXECUTION_UNKNOWN", False, detail)

    # HTTP status is authoritative and is extracted before any message scan.
    # This prevents URL query parameters such as ``timestamp=...`` from
    # converting a gateway failure into a timestamp business rejection.
    status = _extract_http_status(exc)
    if status == 429:
        return ApiFailureClass("RATE_LIMITED", True, detail)
    if status in {500, 502, 503, 504}:
        return ApiFailureClass("SERVER_TRANSIENT", True, detail)

    # CCXT transport/rate-limit exception types are more reliable than free
    # text and remain useful when an adapter omits the HTTP response object.
    if name in {"RateLimitExceeded", "DDoSProtection"}:
        return ApiFailureClass("RATE_LIMITED", True, detail)
    if name in {"RequestTimeout", "ReadTimeout", "ConnectTimeout", "TimeoutError"}:
        return ApiFailureClass("TIMEOUT", True, detail)
    if name in {"NetworkError", "ExchangeNotAvailable", "RequestNetworkError"}:
        return ApiFailureClass("NETWORK", True, detail)

    structured = _structured_error_text(exc).lower()
    business_text = f"{structured} {lowered}"
    if any(pattern in business_text for pattern in NON_RETRYABLE_PATTERNS):
        return ApiFailureClass("NON_RETRYABLE_BUSINESS", False, detail)
    if any(pattern in business_text for pattern in TRANSIENT_PATTERNS[:4]):
        return ApiFailureClass("RATE_LIMITED", True, detail)
    if "timeout" in lowered or "timedout" in lowered or name in {"TimeoutError", "ReadTimeout", "ConnectTimeout"}:
        return ApiFailureClass("TIMEOUT", True, detail)
    if any(pattern in business_text for pattern in ("connection", "network", "temporarily unavailable",
                                              "service unavailable", "gateway")):
        return ApiFailureClass("NETWORK", True, detail)
    if any(pattern in business_text for pattern in TRANSIENT_PATTERNS[8:]):
        return ApiFailureClass("SERVER_TRANSIENT", True, detail)
    # Unknown application errors are not assumed safe to repeat.
    return ApiFailureClass("API_FAILURE", False, detail)


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

    def sleep_backoff(self, attempt: int, *, rate_limited: bool = False,
                      minimum_delay: float = 0.0) -> float:
        delay = max(self.backoff_delay(attempt, rate_limited=rate_limited),
                    max(0.0, float(minimum_delay)))
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
