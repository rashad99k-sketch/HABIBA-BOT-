"""Close-order API governance regression.

A MARKET close is non-idempotent: it must use the shared API pacing and
uncertainty accounting, but it must never be retried automatically.
"""
import inspect

import pytest

import ggpbot


class _ResilienceProbe:
    max_attempts = 3

    def __init__(self):
        self.calls = []

    def before_call(self, method):
        self.calls.append(("before", method))

    def count_request(self):
        self.calls.append(("request",))

    def record_success(self, method):
        self.calls.append(("success", method))

    def record_failure(self, method, exc):
        self.calls.append(("failure", method, type(exc).__name__))
        return None


def test_close_order_path_uses_shared_one_shot_api_governor():
    source = inspect.getsource(ggpbot._verified_market_reduce_impl)
    assert "safe_api_call(ex.create_order" in source
    assert "ex.create_order(sym, \"market\"" not in source


def test_order_write_safe_api_call_is_paced_and_never_retried(monkeypatch):
    probe = _ResilienceProbe()
    calls = []
    pacing = []
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", probe)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda func: pacing.append(func.__name__))
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setitem(ggpbot.MEMORY, "recovery_status", "READY")
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)

    def create_order(*args, **kwargs):
        calls.append((args, kwargs))
        raise TimeoutError("exchange response timed out after submission")

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(create_order, "BTC/USDT:USDT", "market", "sell", 1.0)

    assert len(calls) == 1
    assert pacing == ["create_order"]
    assert ("before", "create_order") in probe.calls
    assert ("request",) in probe.calls
    assert ("failure", "create_order", "TimeoutError") in probe.calls
    assert not any(item[0] == "success" for item in probe.calls)
