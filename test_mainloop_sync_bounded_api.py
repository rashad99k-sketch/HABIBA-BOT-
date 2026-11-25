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


def test_api_rate_limit_releases_global_lock_before_pacing_sleep(monkeypatch):
    clock = [100.0]
    sleeps = []
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", 1.0)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {})
    monkeypatch.setattr(ggpbot, "_last_api_call", 99.5)
    monkeypatch.setattr(ggpbot.time, "monotonic", lambda: clock[0])

    def fake_sleep(seconds):
        assert not ggpbot._API_RATE_LOCK.locked()
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(ggpbot.time, "sleep", fake_sleep)
    ggpbot.rate_limit(lambda: None)

    assert sleeps
    assert all(0 < delay <= 0.25 for delay in sleeps)
    assert ggpbot._last_api_call >= 100.5


def test_api_rate_lock_contention_fails_closed_with_bounded_wait(monkeypatch):
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.02)
    assert ggpbot._API_RATE_LOCK.acquire(timeout=0.1)
    try:
        with pytest.raises(ggpbot.ApiRateLockTimeout, match="API_RATE_LOCK acquisition timed out"):
            ggpbot.rate_limit(lambda: None)
    finally:
        ggpbot._API_RATE_LOCK.release()


def test_api_rate_lock_timeout_does_not_send_exchange_request_or_retry(monkeypatch):
    calls = []
    resilience = ApiResilience(
        max_attempts=3,
        sleep_fn=lambda _seconds: pytest.fail("scheduler lock contention must not back off/retry"),
        monotonic_fn=lambda: 1.0,
    )

    class Exchange:
        def fetch_ohlcv(self, *args, **kwargs):
            calls.append((args, kwargs))
            return []

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", resilience)
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.02)
    assert ggpbot._API_RATE_LOCK.acquire(timeout=0.1)
    try:
        with pytest.raises(ggpbot.ApiRateLockTimeout):
            ggpbot.safe_api_call(Exchange().fetch_ohlcv, "BTC/USDT:USDT", "15m", limit=150)
    finally:
        ggpbot._API_RATE_LOCK.release()

    assert calls == []
    counters = resilience.metrics()["counters"]
    assert counters["requests"] == 0  # no exchange request was dispatched
    assert counters["retries"] == 0
    assert counters["scheduler_contention"] == 1
    assert counters["business_rejections"] == 0


def test_scheduler_contention_is_not_classified_as_exchange_failure():
    from naiso_api_resilience import classify_api_error

    failure = classify_api_error(
        type("ApiRateLockTimeout", (RuntimeError,), {})("scheduler lock unavailable"),
        method="fetch_ohlcv",
    )
    assert failure.kind == "LOCAL_SCHEDULER_CONTENTION"
    assert failure.retryable is False


def test_bingx_public_market_reads_share_documented_one_second_ip_pacing():
    expected = {
        "fetch_ohlcv", "fetch_order_book", "fetch_trades", "fetch_ticker",
        "fetch_tickers", "fetch_bids_asks", "fetch_funding_rate_history",
        "fetch_markets", "fetch_open_interest",
    }
    assert expected.issubset(set(ggpbot._API_METHOD_MIN_INTERVALS))
    assert all(
        ggpbot._API_METHOD_MIN_INTERVALS[name] >= 1.0
        for name in expected
    )


def _valid_ohlcv_rows(count=100):
    return [
        [1_700_000_000_000 + index * 900_000, 100.0, 101.0, 99.0, 100.5, 12.0]
        for index in range(count)
    ]


def _stub_ohlcv_fetch(monkeypatch, rows):
    class Exchange:
        def fetch_ohlcv(self, *_args, **_kwargs):
            return rows

    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda symbol: symbol)
    monkeypatch.setattr(ggpbot, "_ohlcv_rate_limit_cooldown_remaining", lambda _symbol: 0.0)
    monkeypatch.setattr(ggpbot, "safe_api_call", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(ggpbot, "OHLCV_FETCH_DIAGNOSTICS", {})
    monkeypatch.setattr(ggpbot, "_LAST_API_RATE_LIMIT", {"ts": 0.0, "method": "", "message": ""})
    monkeypatch.setattr(ggpbot, "INTERVAL", "15m")


def test_fetch_ohlcv_rejects_malformed_candle_bounds(monkeypatch):
    rows = _valid_ohlcv_rows()
    rows[50] = [rows[50][0], 100.0, 99.0, 98.0, 100.5, 12.0]
    _stub_ohlcv_fetch(monkeypatch, rows)

    assert ggpbot.fetch_ohlcv("BTC/USDT:USDT") is None
    assert ggpbot.OHLCV_FETCH_DIAGNOSTICS["BTC/USDT:USDT"]["reason"] == "OHLCV_MALFORMED"


def test_fetch_ohlcv_rejects_nonfinite_values_and_negative_volume(monkeypatch):
    rows = _valid_ohlcv_rows()
    rows[25][2] = float("inf")
    _stub_ohlcv_fetch(monkeypatch, rows)

    assert ggpbot.fetch_ohlcv("BTC/USDT:USDT") is None
    assert ggpbot.OHLCV_FETCH_DIAGNOSTICS["BTC/USDT:USDT"]["reason"] == "OHLCV_MALFORMED"

    rows = _valid_ohlcv_rows()
    rows[25][5] = -1.0
    _stub_ohlcv_fetch(monkeypatch, rows)
    assert ggpbot.fetch_ohlcv("BTC/USDT:USDT") is None
    assert ggpbot.OHLCV_FETCH_DIAGNOSTICS["BTC/USDT:USDT"]["reason"] == "OHLCV_MALFORMED"


def test_fetch_ohlcv_keeps_valid_closed_candle_frame(monkeypatch):
    rows = _valid_ohlcv_rows()
    _stub_ohlcv_fetch(monkeypatch, rows)

    frame = ggpbot.fetch_ohlcv("BTC/USDT:USDT")
    assert frame is not None
    assert len(frame) == 100
    assert list(frame.columns) == ["timestamp", "open", "high", "low", "close", "volume"]


def test_digitalocean_explicit_bindable_app_id_enables_supervisor_detection(monkeypatch):
    import threading

    for key in ("RENDER", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID",
                "RAILWAY_ENVIRONMENT", "APP_ID", "NAISO_PROCESS_RESTART_ON_MAIN_LOOP_STALL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DIGITALOCEAN_APP_ID", "digitalocean-app-test")
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
