"""Post-merge 2026-11-24 stability incident regressions.

Offline and paper-safe. These tests cover the four production observations:

1. empty symbol reaching engine recovery from a no-argument watchdog verifier;
2. a main-loop stall with no genuine progress phase on the blocked path;
3. duplicated OHLCV requests and governor bypass during bounded retries;
4. RANGE_CHOP -> PROFIT_DEFENSE rule correctness and symbol/state isolation.

No credentials, network access or live orders are used.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import os

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from naiso_api_resilience import ApiResilience, CircuitOpenError
from naiso_health import HealthWatchdog


SYMBOL = "BTC/USDT:USDT"


def _markets():
    return {
        SYMBOL: {"id": "BTCUSDT", "symbol": SYMBOL, "type": "swap", "swap": True, "active": True},
        "SOL/USDT:USDT": {"id": "SOLUSDT", "symbol": "SOL/USDT:USDT", "type": "swap",
                          "swap": True, "active": True},
    }


class FakeExchange:
    def __init__(self, positions=None, *, fail_positions=None, markets=None, has=None):
        self.markets = _markets() if markets is None else markets
        self.rows = list(positions or [])
        self.fail_positions = fail_positions
        self.has = {"fetchPositions": True} if has is None else has
        self.options = {"defaultType": "swap"}
        self.calls = []

    def market(self, symbol):
        self.calls.append(("market", symbol))
        if symbol not in self.markets:
            raise Exception(f"bingx does not have market symbol {symbol}")
        return self.markets[symbol]

    def fetch_positions(self, symbols=None):
        self.calls.append(("fetch_positions", symbols))
        if self.fail_positions is not None:
            raise self.fail_positions
        requested = symbols if isinstance(symbols, (list, tuple)) else list(symbols or [])
        if requested and any(s not in self.markets for s in requested):
            raise Exception(f"bingx does not have market symbol {requested}")
        if not requested:
            return list(self.rows)
        return [row for row in self.rows if row.get("symbol") in requested]

    def fetch_ohlcv(self, symbol, timeframe, limit=None):
        self.calls.append(("fetch_ohlcv", symbol, timeframe, limit))
        return []


@pytest.fixture
def runtime(monkeypatch):
    exchange = FakeExchange()
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", exchange)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False, "current_symbol": ""})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False, "symbol": ""})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "DASHBOARD_STATE", {})
    monkeypatch.setattr(ggpbot, "CACHE", {})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "_live_manager",
                        SimpleNamespace(lifecycle_state=None, brain=SimpleNamespace(current_state="RANGE_CHOP")))
    ggpbot._OHLCV_RATE_COOLDOWN["until"] = 0.0
    ggpbot._OHLCV_RATE_COOLDOWN["symbol_until"].clear()
    return exchange


def _frame(n=150):
    interval_ms = 900_000
    now_ms = int(time.time() * 1000)
    last_closed_open_ms = (now_ms // interval_ms) * interval_ms - interval_ms
    ts = [last_closed_open_ms - (n - 1 - i) * interval_ms for i in range(n)]
    base = np.linspace(100.0, 101.5, n)
    return pd.DataFrame({
        "timestamp": ts, "open": base * 0.999, "high": base * 1.002,
        "low": base * 0.998, "close": base, "volume": np.full(n, 1000.0),
    })


def _position_row(symbol=SYMBOL, amount="0", side="LONG", contract_type="PERPETUAL"):
    return {
        "symbol": symbol,
        "contracts": amount,
        "side": side,
        "info": {"symbol": symbol, "positionAmt": str(amount),
                 "positionSide": side, "contractType": contract_type},
    }


# --- Incident 1: empty symbol must be rejected before recovery dispatch --- #

def test_no_argument_recovery_flat_without_symbol_uses_symbol_free_verification(runtime, monkeypatch):
    calls = []

    def forbidden_resolve(value, *args, **kwargs):
        calls.append(value)
        raise AssertionError("empty runtime symbol must never reach resolve_position_symbol")

    monkeypatch.setattr(ggpbot, "resolve_position_symbol", forbidden_resolve)
    monkeypatch.setattr(ggpbot, "_recovery_hold", lambda *a, **k: pytest.fail("flat verification must not hold"))

    verified, reason, context = ggpbot._verify_engine_recovery(
        caller="watchdog_stalled", component="main_loop")

    assert calls == []
    assert verified is True
    assert reason == "FLAT_VERIFIED_NO_SYMBOL"
    assert context["flat_verified"] is True
    assert context["symbol_status"] == "MISSING"
    assert context["caller"] == "watchdog_stalled"
    assert context["component"] == "main_loop"
    assert ("fetch_positions", None) in runtime.calls
    assert not any(c[0] == "fetch_positions" and c[1] for c in runtime.calls)


def test_no_argument_recovery_open_without_symbol_is_rejected_not_inferred(runtime, monkeypatch):
    ggpbot.STATE.update({"open": True, "current_symbol": None})
    ggpbot.TRADE_STATE.update({"in_position": True, "symbol": None})

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason == "engine_recovery_symbol_missing_open_exposure"
    assert context["symbol_status"] == "MISSING"
    assert context["symbol"] == ""
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.STATE["recovery_symbol_status"] == "MISSING"
    assert not any(c[0] == "fetch_positions" for c in runtime.calls)


def test_explicit_empty_symbol_still_reports_invalid_base_asset(runtime):
    verified, reason, context = ggpbot._verify_engine_recovery("")

    assert verified is False
    assert "invalid_base_asset" in reason
    assert context["symbol_status"] == "INVALID"
    assert not any(c[0] == "fetch_positions" for c in runtime.calls)


def test_installed_ccxt_fetch_positions_none_is_account_wide_swap_snapshot():
    """CCXT 4.5.92 routes an unsymbolized read to the all-positions endpoint."""
    import inspect

    import ccxt

    source = inspect.getsource(ccxt.bingx.fetch_positions)
    assert "symbols: Strings = None" in source
    assert "symbolsNormalized = self.market_symbols(symbols)" in source
    assert "if symbolsNormalized is not None:" in source
    assert "response = self.swapV2PrivateGetUserPositions(paramsSubType)" in source
    assert "self.parse_positions(positions, symbolsNormalized)" in source


def test_flat_account_snapshot_is_authoritative_for_supported_contract_types(runtime, monkeypatch):
    runtime.rows = [
        _position_row(SYMBOL, "0", "LONG", "PERPETUAL"),
        _position_row("SOL/USDT:USDT", "0.0", "SHORT", "SWAP"),
    ]
    monkeypatch.setattr(ggpbot, "_recovery_hold",
                        lambda *a, **k: pytest.fail("authoritative FLAT must not hold"))

    verified, reason, context = ggpbot._verify_engine_recovery(
        caller="watchdog_stalled", component="main_loop")

    assert verified is True
    assert reason == "FLAT_VERIFIED_NO_SYMBOL"
    assert context["flat_verified"] is True
    assert context["snapshot_reason"] == "FLAT_VERIFIED_ACCOUNT_WIDE"
    assert context["snapshot_diagnostics"]["rows"] == 2
    assert context["snapshot_diagnostics"]["supported_markets"] == 2
    assert ("fetch_positions", None) in runtime.calls
    assert not any(c[0] == "fetch_positions" and c[1] for c in runtime.calls)

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "FLAT"
    assert positions == []
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "FLAT"


def test_account_snapshot_detects_real_open_position_without_inferring_symbol(runtime, monkeypatch):
    runtime.rows = [
        _position_row(SYMBOL, "0", "LONG"),
        _position_row("SOL/USDT:USDT", "2", "SHORT", "SWAP"),
    ]
    holds = []
    monkeypatch.setattr(ggpbot, "_recovery_hold", lambda *a, **k: holds.append((a, k)))

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason == "engine_recovery_unexpected_exchange_position"
    assert context["flat_verified"] is False
    assert context["snapshot_reason"] == "OPEN_POSITIONS_PRESENT"
    assert context["snapshot_diagnostics"]["open_rows"] == 1
    assert holds and holds[0][1]["details"]["symbol_status"] == "UNEXPECTED_EXCHANGE_SYMBOL"
    assert ("fetch_positions", None) in runtime.calls
    assert not any(c[0] == "fetch_positions" and c[1] for c in runtime.calls)

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "OPEN"
    assert len(positions) == 1
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "OPEN"


@pytest.mark.parametrize("rows,reason", [
    ([None], "MALFORMED_POSITION_ROW"),
    ([_position_row("BTC/USDT")], "UNSUPPORTED_POSITION_SYMBOL"),
    ([_position_row("BTC/USD:BTC")], "UNSUPPORTED_POSITION_SYMBOL"),
    ([_position_row("DOGE/USDT:USDT")], "UNKNOWN_POSITION_SYMBOL"),
    ([_position_row(SYMBOL, "0", "LONG", "SPOT")], "UNSUPPORTED_POSITION_CONTRACT_TYPE"),
    ([_position_row(SYMBOL, "nan", "LONG")], "MALFORMED_POSITION_AMOUNT"),
    ([_position_row(SYMBOL, "1", "")], "AMBIGUOUS_POSITION_SIDE"),
    ([_position_row(SYMBOL, "1", "LONG"), _position_row(SYMBOL, "1", "LONG")],
     "DUPLICATE_POSITION_ROW"),
])
def test_malformed_account_snapshot_is_never_flat(runtime, monkeypatch, rows, reason):
    runtime.rows = rows
    monkeypatch.setattr(ggpbot, "_recovery_hold", lambda *a, **k: None)

    verified, verify_reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason in verify_reason
    assert context["snapshot_reason"] == reason
    assert context["flat_verified"] is False
    assert ggpbot.STATE.get("recovery_hold") is not True

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "UNKNOWN"
    assert positions is None
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "UNKNOWN"


@pytest.mark.parametrize("case,reason", [
    ("no_markets", "MARKET_METADATA_UNAVAILABLE"),
    ("no_swap_markets", "NO_SUPPORTED_SWAP_MARKETS"),
    ("unsupported", "FETCH_POSITIONS_UNSUPPORTED"),
    ("standard", "UNSUPPORTED_POSITION_SCOPE_STANDARD"),
    ("inverse", "UNSUPPORTED_POSITION_SCOPE_INVERSE"),
])
def test_unverifiable_account_snapshot_scope_is_unknown(runtime, monkeypatch, case, reason):
    if case == "no_markets":
        runtime.markets = {}
    elif case == "no_swap_markets":
        runtime.markets = {"BTC/USDT": {"symbol": "BTC/USDT", "type": "spot", "spot": True}}
    elif case == "unsupported":
        runtime.has = {"fetchPositions": False}
    elif case == "standard":
        runtime.options["fetchPositions"] = {"standard": True}
    else:
        runtime.options["fetchPositions"] = {"subType": "inverse"}
    runtime.rows = [_position_row(SYMBOL, "0", "LONG")]
    monkeypatch.setattr(ggpbot, "_recovery_hold", lambda *a, **k: None)

    verified, verify_reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason in verify_reason
    assert context["snapshot_reason"] == reason
    assert context["flat_verified"] is False

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "UNKNOWN"
    assert positions is None


def test_failed_or_non_list_account_snapshot_is_unknown(runtime, monkeypatch):
    monkeypatch.setattr(ggpbot, "_recovery_hold", lambda *a, **k: None)

    runtime.fail_positions = TimeoutError("account snapshot timed out")
    verified, reason, context = ggpbot._verify_engine_recovery()
    assert verified is False
    assert reason == "engine_recovery_flat_query_unknown:TimeoutError"
    assert context["flat_verified"] is False

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "UNKNOWN"
    assert positions is None

    runtime.fail_positions = None
    monkeypatch.setattr(runtime, "fetch_positions", lambda *_a, **_k: {"data": []})
    verified, reason, context = ggpbot._verify_engine_recovery()
    assert verified is False
    assert reason == "engine_recovery_flat_query_unknown:NON_LIST_POSITION_SNAPSHOT"
    assert context["flat_verified"] is False

    status, positions = ggpbot.fetch_position_tri_state(
        None, strict_account_snapshot=True)
    assert status == "UNKNOWN"
    assert positions is None


def test_watchdog_does_not_repeat_failed_recovery_verification():
    clock = [100.0]
    calls = []
    watchdog = HealthWatchdog(
        check_interval_sec=1.0, verification_retry_sec=30.0,
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        safety_gate=lambda: (True, "READY"))

    def verifier():
        calls.append(clock[0])
        return False, "engine_recovery_invalid_base_asset", {
            "caller": "watchdog_stalled", "component": "main_loop",
            "symbol_status": "INVALID", "open_position_verified": False,
        }

    watchdog.register("main_loop", interval_sec=1.0, grace_sec=10.0, restartable=True,
                      recovery_verifier=verifier, restart_fn=lambda context=None: False,
                      lifecycle="RUNNING")
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")
    clock[0] = 200.0

    first = watchdog.check()["main_loop"]
    second = watchdog.check()["main_loop"]

    assert calls == [200.0], "a blocked verifier must not run on every stall tick"
    assert first["status"] == second["status"] == "STALLED_SAFE_HOLD"
    assert first["restart"] == "VERIFICATION_HOLD"
    assert second["restart"] == "VERIFICATION_BACKOFF"
    assert first["reason"] == "engine_recovery_invalid_base_asset"
    assert first["caller"] == "watchdog_stalled"
    assert first["component"] == "main_loop"
    assert first["symbol_status"] == "INVALID"
    assert ggpbot.NAISO_WATCHDOG is not watchdog

    clock[0] = 235.0
    watchdog.check()
    assert calls == [200.0, 235.0]


# --- Incident 2: real progress phases on blocking foreground work --- #

def test_flat_sync_emits_bounded_blocked_phase_then_completion(runtime, monkeypatch):
    phases = []
    blocked = []
    monkeypatch.setattr(ggpbot, "_main_loop_progress", lambda phase, **details: phases.append(phase))
    monkeypatch.setattr(
        ggpbot, "_main_loop_blocked_phase",
        lambda phase, reason, **details: blocked.append((phase, reason, details)),
    )
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state",
                        lambda _symbols, **_kwargs: ("FLAT", []))
    monkeypatch.setattr(ggpbot, "get_realized_pnl",
                        lambda *_a, **_k: pytest.fail("no inferred symbol for realized PnL"))

    ggpbot.sync_all_states()

    assert [item[0] for item in blocked] == ["position_sync_start"]
    assert blocked[0][1] == "EXCHANGE_POSITION_SYNC"
    assert blocked[0][2]["symbol"] == "NO_SYMBOL"
    assert phases == ["position_sync_complete"]
    assert ggpbot.MEMORY["position_status"] == "FLAT"


def test_blocked_phase_uses_exchange_timeout_without_changing_watchdog_grace(runtime):
    runtime.timeout = 60000
    deadline = ggpbot._exchange_call_phase_deadline_sec()
    assert deadline == 70.0

    original = ggpbot.NAISO_WATCHDOG

    class Recorder:
        def __init__(self):
            self.beats = []

        def monotonic(self):
            return 500.0

        def beat(self, name, **details):
            self.beats.append(details)

    recorder = Recorder()
    ggpbot.NAISO_WATCHDOG = recorder
    try:
        ggpbot._main_loop_blocked_phase("position_sync_start", "EXCHANGE_POSITION_SYNC")
        ggpbot._main_loop_progress("position_sync_complete")
    finally:
        ggpbot.NAISO_WATCHDOG = original

    assert recorder.beats[0]["state"] == "BLOCKED"
    assert recorder.beats[0]["blocked_until"] == 570.0
    assert recorder.beats[1]["state"] == "RUNNING"
    assert "blocked_until" not in recorder.beats[1]


def test_main_loop_has_progress_after_long_candidate_work():
    import inspect

    source = inspect.getsource(ggpbot._original_main_loop_sniper)
    assert "smart_opportunity_selection()" in source
    assert "watchlist_refresh_complete" in source
    assert "_run_sniper_scan_if_due()" in source
    assert "sniper_scan_complete" in source
    assert "position_sync_complete" in inspect.getsource(ggpbot.sync_all_states)


def test_progress_phase_sequence_is_monotonic():
    seen = []
    original = ggpbot.NAISO_WATCHDOG

    class Recorder:
        def beat(self, name, **details):
            seen.append(details.get("progress_seq", 0))

    ggpbot.NAISO_WATCHDOG = Recorder()
    try:
        ggpbot._main_loop_progress("phase_one")
        ggpbot._main_loop_progress("phase_two")
    finally:
        ggpbot.NAISO_WATCHDOG = original
    assert len(seen) == 2
    assert seen[1] == seen[0] + 1


def test_status_only_beats_never_advance_liveness_or_sequence():
    clock = [500.0]
    watchdog = HealthWatchdog(
        check_interval_sec=1.0, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        safety_gate=lambda: (True, "READY"), thread_alive_fn=lambda _name: True)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=10.0,
                      restartable=False, critical=True)
    watchdog.beat("main_loop", state="BLOCKED", phase="position_sync_start",
                  reason="EXCHANGE_POSITION_SYNC", blocked_until=600.0,
                  progress=False, thread_name="MainThread")
    watchdog.beat("main_loop", state="RUNNING", phase="realized_pnl_failed",
                  reason="EXCHANGE_REALIZED_PNL_FAILED", progress=False,
                  thread_name="MainThread")

    component = watchdog._components["main_loop"]
    assert component.last_progress == 500.0
    assert component.last_beat == 500.0
    assert "progress_seq" not in component.diagnostics

    clock[0] = 511.0
    report = watchdog.check()["main_loop"]
    assert report["status"] == "STALLED"
    assert report["restart"] == "NOT_RESTARTABLE"
    assert report["age_sec"] >= 10.0


def test_blocked_deadline_is_capped_by_existing_grace_window():
    clock = [100.0]
    watchdog = HealthWatchdog(
        check_interval_sec=1.0, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        safety_gate=lambda: (True, "READY"), thread_alive_fn=lambda _name: True)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=10.0, restartable=False)
    watchdog.beat("main_loop", state="BLOCKED", phase="position_sync_start",
                  reason="EXCHANGE_POSITION_SYNC", blocked_until=10_000.0,
                  progress=False, thread_name="MainThread")

    clock[0] = 105.0
    report = watchdog.check()["main_loop"]
    assert report["status"] == "BLOCKED"
    assert report["next_due_sec"] <= 5.0
    assert report["hard_deadline_sec"] <= 5.0

    clock[0] = 111.0
    report = watchdog.check()["main_loop"]
    assert report["status"] == "STALLED"


def test_blocked_phase_with_dead_thread_enters_safety_hold():
    clock = [100.0]
    restarts = []
    alerts = []
    watchdog = HealthWatchdog(
        check_interval_sec=1.0, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        alert_fn=lambda message, level: alerts.append((message, level)),
        safety_gate=lambda: (True, "READY"), thread_alive_fn=lambda _name: False)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=60.0,
                      restartable=True, restart_fn=lambda *_a, **_k: restarts.append(1) or True)
    watchdog.beat("main_loop", state="BLOCKED", phase="position_sync_start",
                  reason="EXCHANGE_POSITION_SYNC", blocked_until=500.0,
                  progress=False, thread_name="dead-main-loop")

    report = watchdog.check()["main_loop"]

    assert report["status"] == "FAILED"
    assert report["restart"] == "SAFETY_HOLD"
    assert report["reason"] == "BLOCKED_THREAD_DEAD"
    assert report["thread_name"] == "dead-main-loop"
    assert restarts == []
    assert any("blocked phase lost its thread" in message for message, _level in alerts)


def test_failed_verification_hold_is_bounded_and_cannot_loop_forever():
    clock = [0.0]
    calls = []
    restarts = []
    watchdog = HealthWatchdog(
        check_interval_sec=1.0, verification_retry_sec=30.0,
        verification_max_attempts=2, verification_hold_max_sec=120.0,
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        safety_gate=lambda: (True, "READY"))

    def verifier():
        calls.append(clock[0])
        return False, "engine_recovery_flat_query_unknown", {"caller": "watchdog_stalled"}

    watchdog.register("main_loop", interval_sec=1.0, grace_sec=5.0,
                      restartable=True, recovery_verifier=verifier,
                      restart_fn=lambda *_a, **_k: restarts.append(clock[0]) or True)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")

    clock[0] = 10.0
    first = watchdog.check()["main_loop"]
    clock[0] = 41.0
    second = watchdog.check()["main_loop"]
    clock[0] = 500.0
    later = watchdog.check()["main_loop"]

    assert calls == [10.0, 41.0]
    assert first["restart"] == "VERIFICATION_HOLD"
    assert second["status"] == later["status"] == "STALLED_SAFE_HOLD"
    assert second["restart"] == later["restart"] == "VERIFICATION_EXHAUSTED"
    assert later["verification_attempts"] == 2
    assert restarts == []

    # A genuine new progress beat must re-arm verification once, not forever.
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")
    clock[0] = 520.0
    watchdog.check()
    assert calls == [10.0, 41.0, 520.0]


def test_completion_beat_is_emitted_only_after_operation_returns(runtime, monkeypatch):
    events = []

    class Recorder:
        def monotonic(self):
            return 700.0

        def beat(self, name, **details):
            events.append((details.get("phase"), details.get("state"), details.get("progress")))

    recorder = Recorder()
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", recorder)

    def failing_fetch(*_a, **_k):
        events.append(("fetch_call", None, None))
        raise TimeoutError("position sync hung")

    monkeypatch.setattr(ggpbot, "fetch_position_tri_state", failing_fetch)
    with pytest.raises(TimeoutError):
        ggpbot.sync_all_states()
    assert events == [("position_sync_start", "BLOCKED", False), ("fetch_call", None, None)]

    events.clear()

    def completed_fetch(*_a, **_k):
        events.append(("fetch_call", None, None))
        return "FLAT", []

    monkeypatch.setattr(ggpbot, "fetch_position_tri_state", completed_fetch)
    ggpbot.sync_all_states()
    assert events == [
        ("position_sync_start", "BLOCKED", False),
        ("fetch_call", None, None),
        ("position_sync_complete", "RUNNING", True),
    ]


def test_failed_realized_pnl_is_not_reported_as_complete(runtime, monkeypatch):
    events = []
    ggpbot.STATE.update({"open": True, "current_symbol": SYMBOL})

    class Recorder:
        def monotonic(self):
            return 800.0

        def beat(self, name, **details):
            events.append((
                details.get("phase"), details.get("state"),
                details.get("progress"), details.get("progress_seq"),
            ))

    recorder = Recorder()
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", recorder)
    monkeypatch.setattr(ggpbot, "sync_position_state", lambda *_a, **_k: None)

    def failed_pnl(*_a, **_k):
        raise TimeoutError("pnl read failed")

    monkeypatch.setattr(ggpbot, "get_realized_pnl", failed_pnl)
    ggpbot.sync_all_states()

    phases = [event[0] for event in events]
    assert "realized_pnl_complete" not in phases
    assert "realized_pnl_failed" in phases
    failed = events[phases.index("realized_pnl_failed")]
    completed = events[phases.index("position_sync_complete")]
    assert failed[1] == "RUNNING"
    assert failed[2] is False
    assert failed[3] is None
    assert completed[2] is True
    assert completed[3] is not None
    assert phases.index("position_sync_complete") < phases.index("realized_pnl_failed")


# --- Incident 3: one governed OHLCV snapshot, bounded eligible retries --- #

def test_canonical_cache_dedupes_symbol_aliases_and_limits(runtime, monkeypatch):
    frame = _frame()
    fetch_calls = []

    def fake_fetch(exchange):
        def fetch(symbol, timeframe, limit=None):
            fetch_calls.append((symbol, timeframe, limit))
            return frame.values.tolist()
        return fetch

    runtime.fetch_ohlcv = fake_fetch(runtime)
    monkeypatch.setattr(runtime, "fetch_ohlcv", runtime.fetch_ohlcv)

    short, src1 = ggpbot.get_ohlcv_safe_meta("BTC/USDT", 50)
    full, src2 = ggpbot.get_ohlcv_safe_meta(SYMBOL, 150)

    assert src1 == "FRESH_REQUEST"
    assert src2 == "CACHE_HIT"
    assert len(fetch_calls) == 1
    assert fetch_calls[0][0] == SYMBOL
    assert fetch_calls[0][2] == ggpbot.OHLCV_CACHE_LIMIT
    assert len(short) == 50
    assert len(full) >= 100


def test_concurrent_ohlcv_misses_coalesce_into_one_fetch(runtime, monkeypatch):
    frame = _frame()
    calls = []

    def fake_fetch(symbol, limit=150):
        calls.append(symbol)
        time.sleep(0.05)
        return frame.copy()

    monkeypatch.setattr(ggpbot, "fetch_ohlcv", fake_fetch)
    results = []
    errors = []

    def worker():
        try:
            results.append(ggpbot.get_ohlcv_safe(SYMBOL, 120))
        except Exception as exc:
            errors.append(exc)

    threads = [__import__("threading").Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert not errors
    assert calls == [SYMBOL], "concurrent misses must share one single-flight fetch"
    assert all(frame is not None for frame in results)


def test_circuit_gate_is_enforced_on_every_retry(monkeypatch):
    api = ApiResilience(
        failure_threshold=2, cooldown_sec=60.0, max_attempts=4,
        base_backoff_sec=0.01, sleep_fn=lambda _s: None)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", api)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)

    calls = []

    def fetch_positions():
        calls.append(1)
        raise TimeoutError("network timeout")

    with pytest.raises(CircuitOpenError):
        ggpbot.safe_api_call(fetch_positions)

    assert len(calls) == 2, "a circuit opened by attempt 2 must fence all later retries"
    assert api.metrics()["counters"]["circuit_opens"] == 1


def test_rate_limited_ohlcv_has_endpoint_cooldown(runtime, monkeypatch):
    calls = []

    def fetch(symbol, timeframe, limit=None):
        calls.append(symbol)
        raise Exception("100410 rate limit")

    monkeypatch.setattr(runtime, "fetch_ohlcv", fetch)
    monkeypatch.setattr(ggpbot, "color_text", lambda text, *_a: text)

    assert ggpbot.fetch_ohlcv(SYMBOL, 150) is None
    attempts_after_first = len(calls)
    assert ggpbot.fetch_ohlcv(SYMBOL, 150) is None

    assert attempts_after_first == 3, "safe_api_call remains bounded at three attempts"
    assert len(calls) == attempts_after_first, "cooldown must stop a second logical kline burst"
    assert ggpbot.OHLCV_FETCH_DIAGNOSTICS[SYMBOL]["reason"] == "OHLCV_RATE_LIMITED"


def test_entry_closed_candle_retry_uses_governed_cache_only_for_stale(monkeypatch):
    stale = _frame()
    fresh = _frame()
    refresh_calls = []
    checks = []

    monkeypatch.setattr(ggpbot, "ENTRY_CANDLE_RETRY_DELAY_SEC", 0.0)
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "_entry_exchange_time_ms", lambda: (int(time.time() * 1000), "TEST"))

    def guarded_refresh(*_a, **_k):
        refresh_calls.append(1)
        return fresh, "FRESH_REQUEST"

    monkeypatch.setattr(ggpbot, "get_ohlcv_safe_meta", guarded_refresh)
    monkeypatch.setattr(ggpbot, "get_ohlcv_safe", lambda *_a, **_k: stale.copy())

    def validator(symbol, frame, context, **kwargs):
        checks.append(1)
        if len(checks) == 1:
            return None, {"reason": "CLOSED_CANDLE_STALE"}
        return frame, {"reason": "VALID", "timeframe": str(ggpbot.INTERVAL),
                       "closed_candles": len(frame), "last_closed_candle_ts": 0,
                       "closed_candle_age_sec": 0.0, "time_source": "TEST",
                       "source": "TEST"}

    monkeypatch.setattr(ggpbot, "_validate_entry_closed_frame", validator)
    frame, diagnostic = ggpbot.resolve_entry_closed_candles(SYMBOL)
    assert frame is not None
    assert diagnostic["reason"] == "VALID"
    assert len(refresh_calls) == 1

    checks.clear()
    refresh_calls.clear()

    def forming_validator(symbol, frame, context, **kwargs):
        checks.append(1)
        return None, {"reason": "ONLY_FORMING_CANDLE"}

    monkeypatch.setattr(ggpbot, "_validate_entry_closed_frame", forming_validator)
    frame, diagnostic = ggpbot.resolve_entry_closed_candles(SYMBOL)
    assert frame is None
    assert diagnostic["reason"] == "ONLY_FORMING_CANDLE"
    assert len(checks) == 1
    assert refresh_calls == [], "non-transient candle reasons must not trigger extra fetches"


# --- Incident 4: existing rules are correct; ownership stays isolated --- #

def test_profit_defense_boundary_matches_existing_rules(monkeypatch):
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    smart = {"banker_pressure": 10, "retailer_pressure": 80,
             "distribution_risk": 70, "institutional_bias": "SELL"}
    regime = "CHOPPY"

    defense = ggpbot.TradeStateMachine().update(
        smart, {"continuation_strength": 29.9, "momentum_decay": False},
        18, regime, side="BUY", source="WATCHLIST_DISCOVERY", symbol="FIL/USDT:USDT")
    distribution = ggpbot.TradeStateMachine().update(
        smart, {"continuation_strength": 30.1, "momentum_decay": False},
        18, regime, side="BUY", source="WATCHLIST_DISCOVERY", symbol="ADA/USDT:USDT")

    assert defense == "PROFIT_DEFENSE"
    assert distribution == "DISTRIBUTION"


def test_watchlist_state_machines_do_not_own_live_trade_state(runtime, monkeypatch):
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    ggpbot.STATE["trade_state"] = "RANGE_CHOP"
    live_machine = ggpbot.TradeStateMachine()

    for symbol in ("FIL/USDT:USDT", "ADA/USDT:USDT", "KAVA/USDT:USDT"):
        machine = ggpbot.TradeStateMachine()
        ggpbot._WATCHLIST_TRADE_STATE_MACHINES[symbol] = machine
        state = machine.update(
            {"banker_pressure": 10, "distribution_risk": 70, "institutional_bias": "SELL"},
            {"continuation_strength": 20, "momentum_decay": False},
            18, "CHOPPY", side="BUY", source="WATCHLIST_DISCOVERY", symbol=symbol)
        assert state == "PROFIT_DEFENSE"

    assert ggpbot.STATE["trade_state"] == "RANGE_CHOP"
    assert live_machine.current_state == "RANGE_CHOP"
    assert all(machine is not live_machine
               for machine in ggpbot._WATCHLIST_TRADE_STATE_MACHINES.values())


def test_start_trade_resets_authoritative_state_machine(runtime, monkeypatch):
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    manager = object.__new__(ggpbot.LiveTradeManager)
    manager.brain = ggpbot.InstitutionalTradeBrain()
    manager.brain.current_state = "PROFIT_DEFENSE"
    manager.exchange_sync = SimpleNamespace(_last_reconcile=123.0)
    manager.event_bus = SimpleNamespace(emit=lambda *_a, **_k: None)
    manager.lifecycle_state = None
    ggpbot.STATE.update({"trade_state": "PROFIT_DEFENSE", "last_trade_state_candle_ts": 999})
    monkeypatch.setattr(ggpbot, "_live_manager", manager)

    manager.start_trade(SYMBOL, "BUY", 100.0, 1.0, 99.0, 101.0, 102.0)

    assert manager.brain.current_state == "RANGE_CHOP"
    assert ggpbot.STATE["trade_state"] == "RANGE_CHOP"
    assert "last_trade_state_candle_ts" not in ggpbot.STATE
    assert manager.last_position_sync_ts == 0.0
    assert manager.last_heavy_calc_ts == 0.0
