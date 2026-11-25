"""Regression tests for bounded foreground exchange synchronization reads."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from naiso_api_resilience import ApiResilience


def test_safe_api_call_one_attempt_override_bounds_retrying_reads(monkeypatch):
    calls = []
    resilience = ApiResilience(
        max_attempts=3,
        sleep_fn=lambda _seconds: pytest.fail("one-shot read must not back off"),
        monotonic_fn=lambda: 1.0,
    )

    class Exchange:
        def fetch_positions(self):
            calls.append("fetch_positions")
            raise TimeoutError("simulated read timeout")

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", resilience)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_args, **_kwargs: None)

    with pytest.raises(TimeoutError, match="simulated read timeout"):
        ggpbot.safe_api_call(Exchange().fetch_positions, _api_max_attempts=1)

    metrics = resilience.metrics()
    assert calls == ["fetch_positions"]
    assert metrics["requests"] == 1
    assert metrics["retries"] == 0
    assert metrics["timeouts"] == 1


def test_api_attempt_override_cannot_exceed_global_retry_budget(monkeypatch):
    calls = []
    resilience = ApiResilience(
        max_attempts=2,
        sleep_fn=lambda _seconds: None,
        monotonic_fn=lambda: 1.0,
    )

    class Exchange:
        def fetch_positions(self):
            calls.append(1)
            raise TimeoutError("simulated read timeout")

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", resilience)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_args, **_kwargs: None)

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(Exchange().fetch_positions, _api_max_attempts=99)

    assert len(calls) == 2
    assert resilience.metrics()["retries"] == 1


def test_position_snapshot_timeout_returns_unknown_after_one_physical_attempt(monkeypatch):
    calls = []

    class Exchange:
        def fetch_positions(self, *args):
            calls.append(args)
            raise TimeoutError("BingX positions read timed out")

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)

    status, positions = ggpbot.fetch_position_tri_state(None)

    assert status == "UNKNOWN"
    assert positions is None
    assert calls == [()]


def test_realized_pnl_uses_one_shot_reads_and_one_shot_balance(monkeypatch):
    calls = []

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda symbol: (symbol, "OK", {}),
    )

    def safe_api_spy(func, *args, **kwargs):
        calls.append((getattr(func, "__name__", ""), kwargs.get("_api_max_attempts")))
        return [{"side": "buy", "amount": 1.0, "price": 100.0}]

    balance_calls = []
    monkeypatch.setattr(ggpbot, "safe_api_call", safe_api_spy)
    monkeypatch.setattr(
        ggpbot, "get_balance_safe",
        lambda **kwargs: balance_calls.append(kwargs) or 100.0,
    )

    pnl, pnl_pct = ggpbot.get_realized_pnl("BTC/USDT:USDT")

    assert pnl == -100.0
    assert pnl_pct == -100.0
    assert calls == [("fetch_my_trades", 1)]
    assert balance_calls == [{"api_max_attempts": 1}]
