"""Typed and chained API error classification reliability tests."""
from __future__ import annotations

import errno
import functools
from types import SimpleNamespace

import ccxt
import pytest
import requests

import ggpbot
from naiso_api_resilience import ApiResilience, classify_api_error


def _wrap(exc):
    try:
        raise exc
    except BaseException as cause:
        wrapped = RuntimeError("fetch failed")
        wrapped.__cause__ = cause
        wrapped.__suppress_context__ = True
        return wrapped


def test_ccxt_subclasses_without_messages_are_typed_as_retryable():
    class SubRateLimit(ccxt.RateLimitExceeded):
        pass

    class SubTimeout(ccxt.RequestTimeout):
        pass

    class SubNetwork(ccxt.NetworkError):
        pass

    assert classify_api_error(SubRateLimit(""), method="fetch_ohlcv").kind == "RATE_LIMITED"
    assert classify_api_error(SubTimeout(""), method="fetch_ohlcv").kind == "TIMEOUT"
    assert classify_api_error(SubNetwork(""), method="fetch_ohlcv").kind == "NETWORK"
    assert classify_api_error(SubRateLimit(""), method="fetch_ohlcv").retryable is True
    assert classify_api_error(SubTimeout(""), method="fetch_ohlcv").retryable is True
    assert classify_api_error(SubNetwork(""), method="fetch_ohlcv").retryable is True


def test_wrapped_exceptions_inspect_cause_and_choose_safe_decision():
    rate_limited = classify_api_error(_wrap(ccxt.RateLimitExceeded("429")), method="fetch_ohlcv")
    business = classify_api_error(
        _wrap(ccxt.AuthenticationError("bad api key")), method="fetch_balance")
    scheduler = classify_api_error(
        _wrap(ggpbot.ApiRateLockTimeout("scheduler lock unavailable")), method="fetch_ohlcv")

    assert (rate_limited.kind, rate_limited.retryable) == ("RATE_LIMITED", True)
    assert (business.kind, business.retryable) == ("NON_RETRYABLE_BUSINESS", False)
    assert (scheduler.kind, scheduler.retryable) == ("LOCAL_SCHEDULER_CONTENTION", False)


def test_business_exceptions_are_not_misclassified_by_numeric_text():
    insufficient = classify_api_error(
        ccxt.InsufficientFunds("need 500 USDT"), method="create_order")
    bad_symbol = classify_api_error(
        ccxt.BadSymbol("BTC/USDT not found"), method="fetch_ohlcv")

    # Order writes stay fail-closed regardless of the underlying exception.
    assert (insufficient.kind, insufficient.retryable) == ("ORDER_EXECUTION_UNKNOWN", False)
    assert (bad_symbol.kind, bad_symbol.retryable) == ("NON_RETRYABLE_BUSINESS", False)

    resilience = ApiResilience(failure_threshold=2, sleep_fn=lambda _s: None)
    failure = resilience.record_failure("fetch_balance", ccxt.InsufficientFunds("need 500 USDT"))
    assert failure.kind == "NON_RETRYABLE_BUSINESS"
    assert resilience.metrics()["endpoints"]["fetch_balance"]["failures"] == 0


def test_connection_reset_and_unknown_errors_have_distinct_retryability():
    reset = classify_api_error(
        requests.exceptions.ConnectionError("connection reset by peer"),
        method="fetch_ticker",
    )
    errno_reset = classify_api_error(
        OSError(errno.ECONNRESET, "connection reset"), method="fetch_ticker")
    unknown = classify_api_error(RuntimeError("opaque local fault"), method="fetch_ticker")

    assert (reset.kind, reset.retryable) == ("NETWORK", True)
    assert (errno_reset.kind, errno_reset.retryable) == ("NETWORK", True)
    assert (unknown.kind, unknown.retryable) == ("API_FAILURE", False)


def test_scheduler_timeout_subclass_is_not_counted_as_exchange_failure():
    class SubApiRateLockTimeout(ggpbot.ApiRateLockTimeout):
        pass

    result = classify_api_error(SubApiRateLockTimeout("pacing deadline"), method="fetch_ohlcv")
    assert (result.kind, result.retryable) == ("LOCAL_SCHEDULER_CONTENTION", False)

    resilience = ApiResilience(sleep_fn=lambda _s: None)
    resilience.record_failure("fetch_ohlcv", SubApiRateLockTimeout("pacing deadline"))
    counters = resilience.metrics()["counters"]
    assert counters["scheduler_contention"] == 1
    assert counters["business_rejections"] == 0


def test_safe_api_call_without_circuit_breaker_does_not_retry_business_error(monkeypatch):
    calls = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)

    def exchange_read():
        calls.append(True)
        raise ccxt.BadSymbol("invalid symbol BTC/UNKNOWN")

    with pytest.raises(ccxt.BadSymbol):
        ggpbot.safe_api_call(exchange_read, _api_max_attempts=3)
    assert len(calls) == 1


def test_safe_api_call_without_circuit_breaker_retries_only_transport_reads(monkeypatch):
    calls = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(sleep=lambda _s: None, time=lambda: 0.0))

    def exchange_read():
        calls.append(True)
        raise TimeoutError("gateway timeout")

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(exchange_read, _api_max_attempts=3)
    assert len(calls) == 3


def test_typed_ccxt_rate_limit_read_retries_then_succeeds(monkeypatch):
    calls = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(sleep=lambda _s: None, time=lambda: 0.0))

    def exchange_read():
        calls.append(True)
        if len(calls) < 3:
            raise ccxt.RateLimitExceeded("429 too many requests")
        return {"ok": True}

    assert ggpbot.safe_api_call(exchange_read, _api_max_attempts=3) == {"ok": True}
    assert len(calls) == 3


@pytest.mark.parametrize("fault", [
    ConnectionResetError("connection reset by peer"),
    OSError("ECONNRESET"),
    requests.exceptions.ConnectionError("RemoteDisconnected"),
])
def test_connection_reset_read_is_retried_bounded(monkeypatch, fault):
    calls = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(sleep=lambda _s: None, time=lambda: 0.0))

    def exchange_read():
        calls.append(True)
        raise fault

    with pytest.raises(type(fault)):
        ggpbot.safe_api_call(exchange_read, _api_max_attempts=3)
    assert len(calls) == 3


@pytest.mark.parametrize("fault", [
    ConnectionResetError("connection reset after submission"),
    ccxt.NetworkError("connection reset after submission"),
])
def test_connection_reset_order_write_is_never_retried(monkeypatch, fault):
    calls = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", dict(ggpbot.STATE))
    monkeypatch.setattr(ggpbot, "MEMORY", dict(ggpbot.MEMORY))

    def create_order():
        calls.append(True)
        raise fault

    with pytest.raises(type(fault)):
        ggpbot.safe_api_call(create_order)
    assert len(calls) == 1
    assert ggpbot.STATE["execution_unresolved"] is True


def test_wrapped_order_write_cannot_be_retried_as_a_read(monkeypatch):
    calls = []

    def create_order():
        calls.append(True)
        raise TimeoutError("order response lost")

    @functools.wraps(create_order)
    def wrapped():
        return create_order()

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", dict(ggpbot.STATE))
    monkeypatch.setattr(ggpbot, "MEMORY", dict(ggpbot.MEMORY))

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(wrapped)
    assert len(calls) == 1
    assert ggpbot.STATE["execution_unresolved"] is True
