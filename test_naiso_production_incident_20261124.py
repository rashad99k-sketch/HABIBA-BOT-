"""Production-incident regressions for NAISO-LIVE v1.1.0 (offline, paper-safe).

These tests cover the 2026-11-24 stability incident only. They use fake
exchanges and injected clocks; no credentials, network calls, live orders or
trading-strategy changes are involved.
"""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from bingx_execution import PositionMode, TradeExecutionService
from bingx_protection import normalize_exchange_symbol
from naiso_api_resilience import ApiResilience, CircuitOpenError, classify_api_error
from naiso_health import HealthWatchdog, RestartPolicy

SYMBOL = "BTC/USDT:USDT"
BAD_SYMBOL_ERROR = "bingx does not have market symbol :USDT"


def _markets():
    return {
        SYMBOL: {"id": "BTCUSDT", "symbol": SYMBOL, "type": "swap", "swap": True, "active": True},
        "ETH/USDT:USDT": {"id": "ETHUSDT", "symbol": "ETH/USDT:USDT", "type": "swap",
                          "swap": True, "active": True},
    }


class StrictExchange:
    """Rejects unconfirmed symbols exactly as BingX/CCXT does."""

    def __init__(self, rows=None):
        self.markets = _markets()
        self.rows = list(rows or [])
        self.calls = []
        self.create_calls = []

    def load_markets(self):
        self.calls.append(("load_markets",))
        return self.markets

    def market(self, symbol):
        self.calls.append(("market", symbol))
        if symbol not in self.markets:
            raise Exception(BAD_SYMBOL_ERROR)
        return self.markets[symbol]

    def price_to_precision(self, symbol, price):
        self.calls.append(("price_to_precision", symbol))
        return f"{float(price):.8f}"

    def fetch_positions(self, symbols=None):
        self.calls.append(("fetch_positions", symbols))
        requested = symbols if isinstance(symbols, (list, tuple)) else [symbols]
        for symbol in requested:
            if symbol not in self.markets:
                raise Exception(f"bingx does not have market symbol {symbol}")
        return [row for row in self.rows if row.get("symbol") in requested]

    def fetch_open_orders(self, *args, **kwargs):
        self.calls.append(("fetch_open_orders", args, kwargs))
        return []

    def swapV2PrivateGetTradeOpenOrders(self, params=None):
        self.calls.append(("swapV2PrivateGetTradeOpenOrders", params))
        if not params or params.get("type") != "STOP_MARKET":
            return {"code": 0, "data": []}
        return {"code": 0, "data": list(self.rows)}

    def create_order(self, symbol, kind, side, quantity, price=None, params=None):
        self.create_calls.append((symbol, kind, side, quantity, params))
        return {"id": "duplicate-stop"}


def _stop_row(price=100.0, qty=2.0, order_id="sl-existing"):
    return {
        "id": order_id, "symbol": SYMBOL, "type": "STOP_MARKET", "side": "sell",
        "positionSide": "LONG", "status": "NEW", "stopPrice": price,
        "origQty": qty, "info": {"type": "STOP_MARKET", "orderId": order_id},
    }


def _runtime(monkeypatch, exchange=None):
    import ggpbot

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False, "symbol": ""})
    monkeypatch.setattr(ggpbot, "STATE", {"open": False, "current_symbol": ""})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "LAST_POSITION_FETCH_STATUS", "UNKNOWN")
    if exchange is not None:
        monkeypatch.setattr(ggpbot, "ex", exchange)
    return ggpbot


# --- A/B: symbol normalization, market validation, circuit classification --- #

def test_symbol_normalization_accepts_all_used_market_formats():
    assert normalize_exchange_symbol("BTC") == SYMBOL
    assert normalize_exchange_symbol("BTC/USDT") == SYMBOL
    assert normalize_exchange_symbol("BTC-USDT") == SYMBOL
    assert normalize_exchange_symbol("BTCUSDT") == SYMBOL
    assert normalize_exchange_symbol("btc/usdt") == SYMBOL
    assert normalize_exchange_symbol(SYMBOL) == SYMBOL
    assert normalize_exchange_symbol(" BTC/USDT ") == SYMBOL


@pytest.mark.parametrize("bad", ["", None, ":USDT", "USDT", "BTC/USD", "/USDT"])
def test_symbol_normalization_rejects_empty_base_and_wrong_quote(bad):
    assert normalize_exchange_symbol(bad) == ""


def test_invalid_symbol_never_reaches_bingx_position_read(monkeypatch):
    exchange = StrictExchange()
    ggpbot = _runtime(monkeypatch, exchange)
    # An empty symbol is invalid only when local state claims an open trade.
    # A genuinely flat account must use account-wide position verification.
    ggpbot.STATE["open"] = True
    ggpbot.TRADE_STATE["in_position"] = True

    verified, reason, context = ggpbot._verify_engine_recovery("")
    assert verified is False
    assert "invalid_base_asset" in reason
    assert context["open_position_verified"] is False
    assert context["flat_verified"] is False

    status, position = ggpbot.fetch_position_tri_state(":USDT")
    assert status == "UNKNOWN"
    assert position is None
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "UNKNOWN"
    assert not any(call[0] == "fetch_positions" for call in exchange.calls)


def test_invalid_symbol_classification_does_not_open_or_extend_circuit():
    clock = [0.0]
    api = ApiResilience(
        failure_threshold=3, cooldown_sec=20.0, max_attempts=3,
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)

    for _ in range(12):
        kind = api.record_failure("fetch_positions", Exception(BAD_SYMBOL_ERROR))
        api.before_call("fetch_positions")

    assert kind.kind == "NON_RETRYABLE_BUSINESS"
    assert kind.retryable is False
    metrics = api.metrics()
    assert metrics["endpoints"]["fetch_positions"]["failures"] == 0
    assert metrics["counters"]["business_rejections"] == 12
    assert metrics["counters"]["circuit_opens"] == 0


def test_invalid_symbol_consumes_only_one_safe_api_call(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    api = ApiResilience(failure_threshold=3, cooldown_sec=20.0, max_attempts=3)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", api)

    calls = []

    def fetch_positions(_symbols):
        calls.append(1)
        raise Exception(BAD_SYMBOL_ERROR)

    with pytest.raises(Exception, match=BAD_SYMBOL_ERROR):
        ggpbot.safe_api_call(fetch_positions, [":USDT"])

    assert len(calls) == 1
    assert api.metrics()["counters"]["circuit_opens"] == 0
    assert api.metrics()["endpoints"]["fetch_positions"]["state"] == "CLOSED"


def test_transport_circuit_opens_and_half_open_success_closes_it():
    clock = [0.0]
    api = ApiResilience(
        failure_threshold=2, cooldown_sec=10.0, max_attempts=3,
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)

    for _ in range(2):
        api.record_failure("fetch_positions", TimeoutError("network timeout"))
    with pytest.raises(CircuitOpenError, match="circuit open"):
        api.before_call("fetch_positions")

    clock[0] = 11.0
    api.before_call("fetch_positions")  # one safe half-open probe
    with pytest.raises(CircuitOpenError, match="probe already in flight"):
        api.before_call("fetch_positions")
    api.record_success("fetch_positions")

    endpoint = api.metrics()["endpoints"]["fetch_positions"]
    assert endpoint["state"] == "CLOSED"
    assert endpoint["failures"] == 0
    assert api.metrics()["counters"]["half_open_successes"] == 1


def test_market_universe_load_uses_central_governor_and_never_falls_back(monkeypatch):
    exchange = StrictExchange()
    ggpbot = _runtime(monkeypatch, exchange)
    governor_calls = []

    def governor(func, *args, **kwargs):
        governor_calls.append(getattr(func, "__name__", "call"))
        return func(*args, **kwargs)

    monkeypatch.setattr(ggpbot, "safe_api_call", governor)

    symbols = ggpbot.get_usdt_perp_symbols()
    assert governor_calls == ["load_markets"]
    assert symbols == [SYMBOL, "ETH/USDT:USDT"]
    assert ggpbot.MEMORY["market_universe"]["state"] == "READY"


# --- B/F: tri-state sync and recovery hold --- #

def test_tri_state_unknown_is_never_flat(monkeypatch):
    exchange = StrictExchange()
    ggpbot = _runtime(monkeypatch, exchange)

    def broken_fetch(_symbols=None):
        raise TimeoutError("simulated timeout")

    monkeypatch.setattr(ggpbot, "safe_api_call", lambda func, *a, **k: broken_fetch(*a, **k))
    status, position = ggpbot.fetch_position_tri_state(SYMBOL)
    assert (status, position) == ("UNKNOWN", None)
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "UNKNOWN"

    monkeypatch.setattr(ggpbot, "safe_api_call", lambda func, *a, **k: [])
    status, position = ggpbot.fetch_position_tri_state(SYMBOL)
    assert (status, position) == ("FLAT", None)
    assert ggpbot.LAST_POSITION_FETCH_STATUS == "FLAT"


def test_recovery_hold_blocks_discovery_but_not_open_position_management(monkeypatch):
    import ggpbot

    calls = []
    monkeypatch.setattr(ggpbot, "STATE", {"open": True, "recovery_hold": False,
                                          "current_symbol": SYMBOL, "recovery_reason": ""})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": True, "symbol": SYMBOL})
    monkeypatch.setattr(ggpbot, "MEMORY", {"recovery_status": ""})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "LAST_POSITION_FETCH_STATUS", "FLAT")
    monkeypatch.setattr(ggpbot, "hourly_cleanup", lambda: None)
    monkeypatch.setattr(ggpbot, "_live_manager",
                        SimpleNamespace(manage_live_trade=lambda: calls.append("manage")))
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", None)

    assert ggpbot._main_loop_manage_open_position() is True
    assert calls == ["manage"]

    ggpbot.STATE["recovery_hold"] = True
    assert ggpbot._scanner_recovery_blocked() == "RECOVERY_HOLD"
    assert ggpbot._verify_scanner_recovery()[0] is False
    assert ggpbot._resume_scanner_after_flat_verification(
        {"flat_verified": True}) is False
    assert ggpbot._main_loop_manage_open_position() is False
    assert calls == ["manage"]  # hold blocks entries, not an already-open trade


def test_scanner_unknown_sync_blocks_unless_fresh_flat_is_verified(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "STATE", {"open": False, "recovery_hold": False,
                                          "execution_unresolved": False})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "LAST_POSITION_FETCH_STATUS", "UNKNOWN")
    monkeypatch.setattr(ggpbot, "MEMORY", {"position_status": "UNKNOWN"})

    assert ggpbot._scanner_recovery_blocked() == "POSITION_SYNC_UNKNOWN"
    fresh_flat = {"flat_verified": True, "symbol": SYMBOL}
    assert ggpbot._scanner_recovery_blocked(fresh_flat) == ""

    ggpbot.STATE["recovery_hold"] = True
    assert ggpbot._scanner_recovery_blocked(fresh_flat) == "RECOVERY_HOLD"


# --- C/D: main loop, scheduled discovery, watchdog lifecycle --- #

def test_main_loop_phase_progress_is_reported(monkeypatch):
    import ggpbot

    phases = []
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG",
                        SimpleNamespace(beat=lambda _name, **details: phases.append(details["phase"])))
    ggpbot._main_loop_progress("iteration_start")
    ggpbot._main_loop_progress("open_trade_management")
    assert phases == ["iteration_start", "open_trade_management"]


def test_main_loop_recovery_distinguishes_verified_recovery_from_liveness(monkeypatch):
    import ggpbot

    context = {"open_position_verified": True, "symbol": SYMBOL}
    component = SimpleNamespace(resume_requires_liveness=True)
    watchdog = SimpleNamespace(_components={"main_loop": component})
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_PROGRESS_SEQ", 10)
    monkeypatch.setattr(ggpbot, "_ENGINE_RECOVERY_GUARD", {"ts": 0.0, "symbol": None})
    monkeypatch.setattr(ggpbot, "startup_trade_recovery", lambda: True)
    monkeypatch.setattr(ggpbot, "_wait_for_main_loop_progress", lambda _baseline: False)
    monkeypatch.setattr(ggpbot, "STATE", {"recovery_hold": False, "current_symbol": SYMBOL})

    assert ggpbot._resume_main_loop_after_verification(context) is True
    assert context["engine_resumed"] is False


def test_watchdog_stall_alert_includes_captured_main_thread_stack():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=5.0,
                      critical=True, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="before_blocking_call")
    watchdog._stack_snapshot = lambda _component: {
        "possible_stacks": {"main": '  File "ggpbot.py", line 123, in main_loop_sniper\n    sync_all_states()\n'}}
    clock[0] = 6.0

    report = watchdog.check()["main_loop"]
    assert report["status"] == "STALLED"
    message = next(message for message, _level in alerts if "stalled for" in message)
    assert "sync_all_states" in message


def _watchdog(clock, alerts):
    return HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=3, monotonic_fn=lambda: clock[0]),
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None,
        alert_fn=lambda message, level: alerts.append((message, level)))


def test_scheduled_worker_stays_idle_for_20_minutes_then_wakes_once(monkeypatch):
    clock = [0.0]
    alerts = []
    starts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("scanner:global_discovery", interval_sec=1200.0, grace_sec=300.0,
                      critical=False, restartable=True,
                      recovery_verifier=lambda: (True, "SCANNER_FLAT_VERIFIED",
                                                 {"flat_verified": True}),
                      restart_fn=lambda context: starts.append(1) or True)
    watchdog.beat("scanner:global_discovery", state="IDLE", idle_until=1200.0)

    clock[0] = 1199.0
    report = watchdog.check()["scanner:global_discovery"]
    assert report["status"] == "IDLE"
    assert starts == []
    assert alerts == []

    clock[0] = 1200.0
    report = watchdog.check()["scanner:global_discovery"]
    assert report["status"] == "RECOVERED"
    assert report["restart"] == "SCHEDULED"
    assert starts == [1]
    assert not any("stalled" in message for message, _ in alerts)

    clock[0] = 1202.0
    report = watchdog.check()["scanner:global_discovery"]
    assert report["status"] == "HEALTHY"
    assert starts == [1]  # scheduled wake must not launch a duplicate cycle


def test_no_false_stall_alert_and_true_stall_is_detected_once():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("scanner:global_discovery", interval_sec=1200.0, grace_sec=180.0,
                      critical=False, restartable=False)
    watchdog.beat("scanner:global_discovery", state="IDLE", idle_until=1200.0)

    clock[0] = 900.0
    assert watchdog.check()["scanner:global_discovery"]["status"] == "IDLE"
    assert alerts == []

    clock[0] = 0.0
    watchdog.beat("scanner:global_discovery", state="RUNNING")
    clock[0] = 181.0
    assert watchdog.check()["scanner:global_discovery"]["status"] == "STALLED"
    clock[0] = 300.0
    watchdog.check()
    stalled = [message for message, _ in alerts if "stalled" in message]
    assert len(stalled) == 1


def test_in_cycle_progress_beat_prevents_stall_during_long_scan():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("scanner:global_discovery", interval_sec=1200.0, grace_sec=180.0,
                      critical=False, restartable=False)
    watchdog.beat("scanner:global_discovery", state="RUNNING", phase="cycle_enter")

    clock[0] = 170.0
    watchdog.beat("scanner:global_discovery", state="RUNNING", phase="in_cycle")
    clock[0] = 340.0
    report = watchdog.check()["scanner:global_discovery"]
    assert report["status"] == "HEALTHY"
    assert alerts == []


def _prepare_dispatcher(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", None)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "_SCANNER_TASKS_LOCK", threading.RLock())
    monkeypatch.setattr(ggpbot, "_SCANNER_TASK_LOCKS", {})
    monkeypatch.setattr(ggpbot, "_SCANNER_TASK_STATUS", {})
    monkeypatch.setattr(ggpbot, "_SCANNER_TASK_THREADS", {})
    monkeypatch.setattr(ggpbot, "_SCANNER_TASK_CYCLES", {})
    return ggpbot


def test_duplicate_discovery_cycle_is_refused(monkeypatch):
    ggpbot = _prepare_dispatcher(monkeypatch)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def scan():
        calls.append(1)
        started.set()
        assert release.wait(2.0)

    assert ggpbot.dispatch_scanner_task("global_discovery", scan) is True
    assert started.wait(1.0)
    assert ggpbot.dispatch_scanner_task("global_discovery", scan) is False
    release.set()
    time.sleep(0.05)
    assert calls == [1]


def test_thread_start_failure_releases_lock_and_does_not_delay_next_due(monkeypatch):
    ggpbot = _prepare_dispatcher(monkeypatch)

    class FailedThread:
        def __init__(self, *_a, **_k):
            pass

        def start(self):
            raise RuntimeError("can't start new thread")

    monkeypatch.setattr(ggpbot.threading, "Thread", FailedThread)

    assert ggpbot.dispatch_scanner_task("global_discovery", lambda: None) is False
    status = ggpbot.MEMORY["scanner_tasks"]["global_discovery"]
    assert status["state"] == "FAILED"
    assert status["reason"] == "START_FAILED"
    assert status["last_success"] is None
    assert status["next_due"] is not None
    assert status["next_due"] - time.time() < 1200.0
    assert not ggpbot._SCANNER_TASK_LOCKS["global_discovery"].locked()

    # A second attempt must be allowed; a stale lock would keep returning False.
    monkeypatch.setattr(ggpbot.threading, "Thread", lambda **_k: FailedThread())
    assert ggpbot.dispatch_scanner_task("global_discovery", lambda: None) is False


def test_successful_cycle_separates_last_attempt_success_and_next_due(monkeypatch):
    ggpbot = _prepare_dispatcher(monkeypatch)
    finished = threading.Event()

    def scan():
        finished.set()

    assert ggpbot.dispatch_scanner_task("global_discovery", scan) is True
    assert finished.wait(1.0)
    time.sleep(0.05)
    status = ggpbot.MEMORY["scanner_tasks"]["global_discovery"]
    assert status["state"] == "IDLE"
    assert status["last_attempt"] > 0
    assert status["last_success"] >= status["last_attempt"]
    assert status["next_due"] > status["last_success"]


# --- E: attributed, race-free state transitions --- #

def test_state_machine_transitions_are_attributed_and_locked(monkeypatch):
    import ggpbot

    logs = []
    monkeypatch.setattr(ggpbot, "log_execution", lambda message, *_a, **_k: logs.append(message))
    machine = ggpbot.TradeStateMachine()
    smart = {"banker_pressure": 70, "distribution_risk": 70,
             "institutional_bias": "BUY"}
    collapse = {"continuation_strength": 10, "momentum_decay": True}
    healthy = {"continuation_strength": 50, "momentum_decay": False}

    errors = []

    def update(source, symbol):
        try:
            for _ in range(25):
                machine.update(smart, collapse, 12, "CHOPPY", side="SELL",
                               source=source, symbol=symbol, candle_ts=1000)
                machine.update(smart, healthy, 12, "CHOPPY", side="SELL",
                               source=source, symbol=symbol, candle_ts=1001)
        except Exception as exc:  # pragma: no cover - failure evidence
            errors.append(exc)

    threads = [threading.Thread(target=update, args=args) for args in
               (("WATCHLIST_DISCOVERY", "BTC"), ("LIVE_TRADE_MANAGER", "ETH"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert errors == []
    assert machine.current_state in ggpbot.TradeStateMachine.STATES
    assert logs
    assert all("[STATE]" in line and "source=" in line and "reason=" in line for line in logs)
    assert any("source=WATCHLIST_DISCOVERY symbol=BTC" in line for line in logs)
    assert any("source=LIVE_TRADE_MANAGER symbol=ETH" in line for line in logs)


# --- F: protective-order idempotency and API governor --- #

def test_existing_native_stop_is_adopted_without_duplicate_order():
    exchange = StrictExchange(rows=[_stop_row()])
    service = TradeExecutionService(exchange)

    first = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.ONE_WAY, position_side="LONG")
    second = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.ONE_WAY, position_side="LONG")

    assert first.outcome == "VERIFIED_ADOPTED"
    assert second.outcome == "VERIFIED_ADOPTED"
    assert exchange.create_calls == []


def test_execution_reads_use_governor_and_reject_empty_symbol():
    exchange = StrictExchange(rows=[{
        "symbol": SYMBOL, "side": "long", "contracts": 2.0, "entryPrice": 100.0,
    }])
    calls = []

    def governor(func, *args, **kwargs):
        calls.append(getattr(func, "__name__", "call"))
        return func(*args, **kwargs)

    service = TradeExecutionService(exchange, api_caller=governor)
    position = service._position(SYMBOL, "LONG", "BUY")
    assert position is not None
    assert "fetch_positions" in calls

    before = len(calls)
    assert service._position(":USDT", "LONG", "BUY") is None
    assert len(calls) == before

    ok, reason = service._precheck_native_stop(
        symbol=":USDT", side="BUY", close_side="SELL", position_side="LONG",
        mode=PositionMode.ONE_WAY, quantity=2.0, new_trigger_price=100.0,
        previous_order_id=None, previous_trigger_price=None)
    assert ok is False
    assert reason == "precheck_invalid_symbol"
    assert len(calls) == before


# --- residual incident hardening: blocked lifecycle, alert re-arm, liveness --- #

def test_startup_blocked_lifecycle_clears_on_real_loop_progress():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0,
                      critical=True, restartable=False, lifecycle="BLOCKED")
    watchdog.beat("main_loop", state="BLOCKED", reason="STARTUP_RECOVERY",
                  blocked_until=300.0)

    clock[0] = 299.0
    assert watchdog.check()["main_loop"]["status"] == "BLOCKED"
    assert alerts == []

    clock[0] = 300.0
    watchdog.beat("main_loop", state="RUNNING", phase="startup_recovery_complete")
    clock[0] = 360.0
    report = watchdog.check()["main_loop"]
    assert report["status"] == "HEALTHY"
    assert alerts == []


def test_stall_alerts_rearm_after_real_progress():
    clock = [0.0]
    alerts = []
    watchdog = _watchdog(clock, alerts)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0,
                      critical=True, restartable=False)
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")

    clock[0] = 61.0
    assert watchdog.check()["main_loop"]["status"] == "STALLED"
    clock[0] = 120.0
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start")
    clock[0] = 181.0
    assert watchdog.check()["main_loop"]["status"] == "STALLED"

    stalls = [message for message, _ in alerts if "stalled for" in message]
    assert len(stalls) == 2


def test_recovery_never_claims_main_loop_restarted_without_progress(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "STATE", {"open": False, "recovery_hold": False})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "LAST_POSITION_FETCH_STATUS", "FLAT")
    monkeypatch.setattr(ggpbot, "_ENGINE_RECOVERY_GUARD", {"ts": 0.0, "symbol": None})
    monkeypatch.setattr(ggpbot, "startup_trade_recovery", lambda: True)
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_PROGRESS_SEQ", 5)

    monkeypatch.setattr(ggpbot, "_wait_for_main_loop_progress", lambda *_a, **_k: False)
    context = {"open_position_verified": True, "symbol": SYMBOL}
    assert ggpbot._resume_main_loop_after_verification(context) is False
    assert context["engine_resumed"] is False

    monkeypatch.setattr(ggpbot, "_wait_for_main_loop_progress", lambda *_a, **_k: True)
    context = {"open_position_verified": True, "symbol": SYMBOL}
    assert ggpbot._resume_main_loop_after_verification(context) is True
    assert context["engine_resumed"] is True
