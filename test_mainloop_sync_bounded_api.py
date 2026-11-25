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
    assert metrics["counters"]["requests"] == 1
    assert metrics["counters"]["retries"] == 0
    assert metrics["counters"]["timeouts"] == 1


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
    assert resilience.metrics()["counters"]["retries"] == 1


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

    class Exchange:
        def fetch_my_trades(self, *args, **kwargs):
            return [{"side": "buy", "amount": 1.0, "price": 100.0}]

    balance_calls = []
    monkeypatch.setattr(ggpbot, "ex", Exchange())
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


def test_render_service_identity_is_accepted_as_process_supervisor(monkeypatch):
    import threading

    monkeypatch.setenv("RENDER", "")
    monkeypatch.setenv("RENDER_SERVICE_ID", "srv-test")
    monkeypatch.delenv("RENDER_INSTANCE_ID", raising=False)
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("NAISO_PROCESS_RESTART_ON_MAIN_LOOP_STALL", raising=False)
    monkeypatch.setattr(
        ggpbot, "_main_loop_owner",
        lambda: {"thread_id": threading.current_thread().ident,
                 "thread_name": "MainThread", "registered": True},
    )
    monkeypatch.setattr(ggpbot, "_main_loop_owner_alive", lambda: True)
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda _reason: True)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    exits = []
    monkeypatch.setattr(ggpbot.os, "_exit", lambda code: exits.append(code))

    context = {}
    assert ggpbot._restart_main_loop_process_after_stall(context) is True
    assert context["process_supervisor_detected"] is True
    assert context["process_supervisor_kind"] == "RENDER"
    assert exits == [ggpbot.EXIT_RECOVERY_EXHAUSTED]


def test_exchange_timeout_configuration_has_a_hard_15_second_ceiling(monkeypatch):
    monkeypatch.setenv("EXCHANGE_HTTP_TIMEOUT_MS", "60000")
    bounded = ggpbot._env_int_range("EXCHANGE_HTTP_TIMEOUT_MS", 12000, 5000, 15000)
    assert bounded == 15000


def test_digitalocean_app_platform_identity_is_accepted_as_supervisor(monkeypatch):
    import threading

    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_ID", raising=False)
    monkeypatch.delenv("RENDER_INSTANCE_ID", raising=False)
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.setenv("APP_ID", "digitalocean-app-test")
    monkeypatch.delenv("NAISO_PROCESS_RESTART_ON_MAIN_LOOP_STALL", raising=False)
    monkeypatch.setattr(
        ggpbot, "_main_loop_owner",
        lambda: {"thread_id": threading.current_thread().ident,
                 "thread_name": "MainThread", "registered": True},
    )
    monkeypatch.setattr(ggpbot, "_main_loop_owner_alive", lambda: True)
    monkeypatch.setattr(ggpbot, "_recovery_exit_gate", lambda _reason: True)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_args, **_kwargs: None)
    exits = []
    monkeypatch.setattr(ggpbot.os, "_exit", lambda code: exits.append(code))

    context = {}
    assert ggpbot._restart_main_loop_process_after_stall(context) is True
    assert context["process_supervisor_detected"] is True
    assert context["process_supervisor_kind"] == "DIGITALOCEAN_APP_PLATFORM"
    assert exits == [ggpbot.EXIT_RECOVERY_EXHAUSTED]
