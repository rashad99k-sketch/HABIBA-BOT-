"""Deadline-bounded API scheduler reliability and fault-injection tests."""
from __future__ import annotations

import math
import threading
import time as real_time
from types import SimpleNamespace

import pytest

import ggpbot


@pytest.fixture(autouse=True)
def _isolate_scheduler_reservations(monkeypatch):
    monkeypatch.setattr(ggpbot, "_API_RATE_METHOD_LAST_RESERVED", {})
    monkeypatch.setattr(ggpbot, "_API_RATE_METHOD_LAST_START", {})
    monkeypatch.setattr(ggpbot, "_API_RATE_LAST_START", 0.0)


def _install_fake_clock(monkeypatch, *, start=1000.0, advance=True):
    clock = [float(start)]
    sleeps = []

    def fake_monotonic():
        return clock[0]

    def fake_sleep(seconds):
        assert float(seconds) > 0.0, "zero-progress sleeps are forbidden"
        sleeps.append(float(seconds))
        if advance:
            clock[0] += float(seconds)

    real_monotonic = real_time.monotonic
    real_sleep = real_time.sleep
    monkeypatch.setattr(ggpbot, "time", SimpleNamespace(monotonic=fake_monotonic, sleep=fake_sleep))
    # The module facade must not mutate the process-global clock used by other
    # threads or by the hard (non-injectable) scheduler deadline.
    assert real_time.monotonic is real_monotonic
    assert real_time.sleep is real_sleep
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(
        ggpbot, "_API_RATE_LOCK_OWNER",
        {"thread_id": None, "thread_name": "", "since_monotonic": 0.0, "method": ""},
    )
    monkeypatch.setattr(ggpbot, "_last_api_call", float(start))
    monkeypatch.setattr(ggpbot, "_API_RATE_METHOD_LAST_RESERVED", {})
    monkeypatch.setattr(ggpbot, "_API_RATE_METHOD_LAST_START", {})
    monkeypatch.setattr(ggpbot, "_API_RATE_LAST_START", 0.0)
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", 0.001)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {})
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(ggpbot, "API_RATE_WAIT_TIMEOUT_SEC", 10.0)
    monkeypatch.setattr(ggpbot, "API_RATE_MIN_SLEEP_SEC", 0.001)
    monkeypatch.setattr(ggpbot, "API_RATE_MAX_WAIT_ITERATIONS", 4096)
    return clock, sleeps


def test_fake_clock_pacing_reserves_after_endpoint_interval(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch)
    ggpbot._API_METHOD_MIN_INTERVALS["probe_endpoint"] = 1.05
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_endpoint"] = clock[0]
    monkeypatch.setattr(ggpbot, "_last_api_call", clock[0])

    def probe_endpoint():
        return None

    ggpbot.rate_limit(probe_endpoint)

    assert sleeps == pytest.approx([1.05])
    assert ggpbot._last_api_call == pytest.approx(clock[0])
    assert math.isclose(clock[0], 1001.05)
    assert not ggpbot._API_RATE_LOCK.locked()
    assert ggpbot._API_RATE_LOCK_OWNER["thread_id"] is None


def test_real_monotonic_pacing_never_starts_early(monkeypatch):
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(
        ggpbot, "_API_RATE_LOCK_OWNER",
        {"thread_id": None, "thread_name": "", "since_monotonic": 0.0, "method": ""},
    )
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", 0.02)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {"probe_real": 0.02})
    monkeypatch.setattr(ggpbot, "_last_api_call", real_time.monotonic())
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_real"] = real_time.monotonic()
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    monkeypatch.setattr(ggpbot, "API_RATE_WAIT_TIMEOUT_SEC", 2.0)
    monkeypatch.setattr(ggpbot, "API_RATE_MIN_SLEEP_SEC", 0.001)

    def probe_real():
        return None

    started = real_time.monotonic()
    ggpbot.rate_limit(probe_real)
    elapsed = real_time.monotonic() - started
    assert elapsed >= 0.02
    assert elapsed < 1.0
    assert not ggpbot._API_RATE_LOCK.locked()


def test_floating_point_boundary_uses_one_positive_sleep(monkeypatch):
    start = 1_000_000_000.0
    clock, sleeps = _install_fake_clock(monkeypatch, start=start)
    interval = 0.25
    ggpbot._API_METHOD_MIN_INTERVALS["probe_boundary"] = interval
    # At this magnitude a sub-microsecond residual is close to the float ULP.
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_boundary"] = clock[0] - 1e-6
    monkeypatch.setattr(ggpbot, "_last_api_call", clock[0])

    def probe_boundary():
        return None

    ggpbot.rate_limit(probe_boundary)

    assert len(sleeps) == 1
    assert sleeps[0] >= ggpbot.API_RATE_MIN_SLEEP_SEC
    assert clock[0] >= start + interval - 1e-6
    assert not ggpbot._API_RATE_LOCK.locked()


@pytest.mark.parametrize("offset", [-1e-3, -1e-6, 0.0, 1e-6])
def test_floating_point_boundaries_never_reserve_early(monkeypatch, offset):
    start = 1_000_000_000.0
    interval = 0.25
    clock, sleeps = _install_fake_clock(monkeypatch, start=start)
    ggpbot._API_METHOD_MIN_INTERVALS["probe_ulp"] = interval
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_ulp"] = clock[0] + offset
    monkeypatch.setattr(ggpbot, "_last_api_call", clock[0])

    def probe_ulp():
        return None

    ggpbot.rate_limit(probe_ulp)

    assert clock[0] >= start + interval + offset
    assert len(sleeps) <= 1
    assert all(value > 0.0 for value in sleeps)
    assert not ggpbot._API_RATE_LOCK.locked()


def test_non_finite_pacing_state_fails_closed_without_sleep_or_lock(monkeypatch):
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(ggpbot, "_last_api_call", math.nan)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {})

    def probe_non_finite():
        return None

    with pytest.raises(ggpbot.ApiRateLockTimeout, match="non-finite pacing state"):
        ggpbot.rate_limit(probe_non_finite)
    assert not ggpbot._API_RATE_LOCK.locked()


@pytest.mark.parametrize("bad_interval", [math.nan, math.inf, -math.inf])
def test_non_finite_endpoint_interval_fails_closed(monkeypatch, bad_interval):
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(ggpbot, "_last_api_call", 0.0)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {"probe_bad_interval": bad_interval})

    def probe_bad_interval():
        return None

    with pytest.raises(ggpbot.ApiRateLockTimeout, match="non-finite"):
        ggpbot.rate_limit(probe_bad_interval)
    assert not ggpbot._API_RATE_LOCK.locked()


def test_non_finite_logical_clock_fails_closed_without_sleep(monkeypatch):
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(ggpbot, "_last_api_call", 0.0)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {})
    monkeypatch.setattr(
        ggpbot, "time",
        SimpleNamespace(monotonic=lambda: math.nan, sleep=lambda _s: pytest.fail("sleep on NaN clock")),
    )

    def probe_nan_clock():
        return None

    with pytest.raises(ggpbot.ApiRateLockTimeout, match="non-finite pacing state"):
        ggpbot.rate_limit(probe_nan_clock)
    assert not ggpbot._API_RATE_LOCK.locked()


def test_frozen_clock_fails_closed_without_zero_progress_livelock(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch, advance=False)
    ggpbot._API_METHOD_MIN_INTERVALS["probe_frozen"] = 0.25
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_frozen"] = clock[0]
    monkeypatch.setattr(ggpbot, "API_RATE_MAX_WAIT_ITERATIONS", 4)

    def probe_frozen():
        return None

    with pytest.raises(ggpbot.ApiRateLockTimeout):
        ggpbot.rate_limit(probe_frozen)

    assert 0 < len(sleeps) <= 4
    assert all(value > 0.0 for value in sleeps)
    assert not ggpbot._API_RATE_LOCK.locked()


def test_scheduler_lock_contention_fails_closed_without_sending_request(monkeypatch):
    calls = []
    acquire_calls = []

    class ContendedLock:
        def acquire(self, timeout=None):
            acquire_calls.append(timeout)
            return False

        def locked(self):
            return True

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.02)
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", ContendedLock())

    def exchange_call():
        calls.append(True)
        return []

    with pytest.raises(ggpbot.ApiRateLockTimeout):
        ggpbot.safe_api_call(exchange_call)
    assert calls == []
    assert len(acquire_calls) == 1
    assert 0.0 < acquire_calls[0] <= 0.02


def test_simultaneous_callers_are_serialized_by_real_lock(monkeypatch):
    monkeypatch.setattr(ggpbot, "_API_RATE_LOCK", threading.Lock())
    monkeypatch.setattr(
        ggpbot, "_API_RATE_LOCK_OWNER",
        {"thread_id": None, "thread_name": "", "since_monotonic": 0.0, "method": ""},
    )
    interval = 0.02
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", interval)
    monkeypatch.setattr(ggpbot, "_API_METHOD_MIN_INTERVALS", {"probe_concurrent": interval})
    monkeypatch.setattr(ggpbot, "_last_api_call", real_time.monotonic())
    monkeypatch.setattr(ggpbot, "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 1.0)
    monkeypatch.setattr(ggpbot, "API_RATE_WAIT_TIMEOUT_SEC", 5.0)

    def probe_concurrent():
        return None

    barrier = threading.Barrier(8)
    starts = []
    errors = []

    def worker():
        try:
            barrier.wait(timeout=2.0)
            ggpbot.rate_limit(probe_concurrent)
            starts.append(real_time.monotonic())
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, name=f"rate-probe-{index}") for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert len(starts) == 8
    starts.sort()
    assert all(later - earlier >= interval * 0.5 for earlier, later in zip(starts, starts[1:]))
    assert starts[-1] - starts[0] >= interval * 4
    assert not ggpbot._API_RATE_LOCK.locked()


def test_actual_request_starts_only_after_required_interval(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch)
    interval = 0.75
    ggpbot._API_METHOD_MIN_INTERVALS["exchange_read"] = interval
    ggpbot._API_RATE_METHOD_LAST_RESERVED["exchange_read"] = clock[0]
    monkeypatch.setattr(ggpbot, "_last_api_call", clock[0])
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    request_starts = []

    def exchange_read():
        request_starts.append(clock[0])
        return ["ok"]

    assert ggpbot.safe_api_call(exchange_read, _api_max_attempts=1) == ["ok"]
    assert request_starts == pytest.approx([1000.0 + interval])
    assert sum(sleeps) == pytest.approx(interval)
    assert not ggpbot._API_RATE_LOCK.locked()


def test_high_iteration_pacing_stress(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch)
    interval = 0.001
    iterations = 2000
    ggpbot._API_METHOD_MIN_INTERVALS["probe_stress"] = interval
    ggpbot._API_RATE_METHOD_LAST_RESERVED["probe_stress"] = clock[0]
    monkeypatch.setattr(ggpbot, "_last_api_call", clock[0])

    def probe_stress():
        return None

    for _ in range(iterations):
        ggpbot.rate_limit(probe_stress)

    assert len(sleeps) == iterations
    assert all(value >= interval for value in sleeps)
    assert clock[0] == pytest.approx(1000.0 + iterations * interval)
    assert not ggpbot._API_RATE_LOCK.locked()
    assert ggpbot._API_RATE_LOCK_OWNER["thread_id"] is None

def test_endpoint_pacing_lanes_do_not_serialize_unrelated_methods(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch, start=2000.0)
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", 0.10)
    ggpbot._API_METHOD_MIN_INTERVALS.update({"probe_a": 1.0, "probe_b": 1.0})
    ggpbot._API_RATE_METHOD_LAST_RESERVED.update({
        "probe_a": clock[0], "probe_b": clock[0],
    })
    starts = []

    def probe_a():
        starts.append(("a", clock[0]))

    def probe_b():
        starts.append(("b", clock[0]))

    ggpbot.rate_limit(probe_a)
    probe_a()
    ggpbot.rate_limit(probe_b)
    probe_b()
    ggpbot.rate_limit(probe_a)
    probe_a()

    assert [name for name, _ in starts] == ["a", "b", "a"]
    assert [value for _, value in starts] == pytest.approx([2001.0, 2001.1, 2002.0])
    assert sleeps == pytest.approx([1.0, 0.1, 0.9])


def test_local_scheduler_contention_is_not_counted_as_exchange_rate_limit(monkeypatch):
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda value: str(value))
    monkeypatch.setattr(ggpbot, "_ohlcv_rate_limit_cooldown_remaining", lambda _symbol: 0.0)
    monkeypatch.setattr(
        ggpbot, "safe_api_call",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ggpbot.ApiRateLockTimeout("API_RATE pacing deadline exceeded for fetch_ohlcv")
        ),
    )
    monkeypatch.setattr(ggpbot, "_mark_ohlcv_rate_limit_cooldown",
                        lambda _symbol: pytest.fail("local contention must not trigger exchange cooldown"))

    assert ggpbot.fetch_ohlcv("BTC/USDT:USDT") is None
    diagnostic = ggpbot.OHLCV_FETCH_DIAGNOSTICS["BTC/USDT:USDT"]
    assert diagnostic["reason"] == "LOCAL_SCHEDULER_CONTENTION"
    assert "pacing deadline exceeded" in diagnostic["detail"]

def test_order_placement_and_cancellation_share_trade_write_quota(monkeypatch):
    clock, sleeps = _install_fake_clock(monkeypatch, start=4000.0)
    monkeypatch.setattr(ggpbot, "MIN_API_INTERVAL", 0.10)
    ggpbot._API_RATE_METHOD_LAST_RESERVED["trade_write"] = clock[0]
    ggpbot._API_METHOD_MIN_INTERVALS.update({
        "create_order": 1.05,
        "cancel_order": 1.05,
    })
    calls = []

    def create_order():
        calls.append(("create", clock[0]))

    def cancel_order():
        calls.append(("cancel", clock[0]))

    ggpbot.rate_limit(create_order)
    create_order()
    ggpbot.rate_limit(cancel_order)
    cancel_order()

    assert [name for name, _ in calls] == ["create", "cancel"]
    assert [value for _, value in calls] == pytest.approx([4001.05, 4002.10])
    assert sleeps == pytest.approx([1.05, 1.05])
    assert not ggpbot._API_RATE_LOCK.locked()

