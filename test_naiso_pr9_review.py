"""Critical PR #9 review regressions (offline and paper-safe).

Every test drives a real production helper with a fake BingX exchange. No
credentials, network calls, live orders or strategy changes are involved.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

SYMBOL = "BTC/USDT:USDT"


def _stop_row(price=100.0, order_id="sl-1", qty=2.0):
    return {
        "id": order_id, "symbol": SYMBOL, "type": "STOP_MARKET", "side": "sell",
        "positionSide": "LONG", "status": "NEW", "stopPrice": price, "origQty": qty,
        "info": {"type": "STOP_MARKET", "orderId": order_id},
    }


def _position(qty=2.0):
    return {"symbol": SYMBOL, "positionSide": "LONG", "side": "long",
            "contracts": qty, "entryPrice": 100.0, "markPrice": 101.0,
            "info": {"positionAmt": str(qty)}}


class Exchange:
    """Deterministic BingX-shaped fake for recovery-only code paths."""

    def __init__(self, *, rows=None, position=None, conditional_ok=True,
                 create_result=None, create_error=None):
        self.rows = list(rows or [])
        self.position = position
        self.conditional_ok = conditional_ok
        self.create_result = create_result or {"id": "new-sl"}
        self.create_error = create_error
        self.create_calls = []
        self.calls = []

    def market(self, symbol):
        self.calls.append("market")
        return {"id": "BTCUSDT"}

    def price_to_precision(self, symbol, price):
        self.calls.append("price_to_precision")
        return f"{float(price):.8f}"

    def fetch_open_orders(self, *args, **kwargs):
        self.calls.append("fetch_open_orders")
        # Empty unified results are intentionally never proof of absence.
        return []

    def swapV2PrivateGetTradeOpenOrders(self, params=None):
        self.calls.append("swapV2PrivateGetTradeOpenOrders")
        if not self.conditional_ok:
            return {"code": 100, "data": [], "msg": "conditional query unavailable"}
        rows = [r for r in self.rows if r.get("type", "STOP_MARKET") == params.get("type", "STOP_MARKET")]
        return {"code": 0, "data": rows}

    def fetch_positions(self, symbols=None):
        self.calls.append("fetch_positions")
        return [self.position] if self.position is not None else []

    def create_order(self, symbol, kind, side, quantity, price=None, params=None):
        self.calls.append("create_order")
        self.create_calls.append((symbol, kind, side, quantity, params))
        if self.create_error is not None:
            raise self.create_error
        order_id = f"new-sl-{len(self.create_calls)}"
        self.rows.append(_stop_row(float(params["stopPrice"]), order_id, float(quantity)))
        return {"id": order_id}


def _safe_runtime(monkeypatch):
    """Disable sleeps/circuits while keeping the real fail-closed governors."""
    import ggpbot

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *_a, **_k: None)
    monkeypatch.setattr(ggpbot, "DASHBOARD_STATE", {})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "_live_manager", SimpleNamespace(lifecycle_state=None))
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_ENGINE_RECOVERY_GUARD", {"ts": 0.0, "symbol": None})
    return ggpbot


def _open_state(monkeypatch, ggpbot, **overrides):
    state = {
        "open": True, "current_symbol": SYMBOL, "side": "BUY", "entry": 100.0,
        "qty": 2.0, "initial_qty": 2.0, "remaining_qty": 2.0, "exchange_qty": 2.0,
        "position_side": "LONG", "trade_id": "T-1", "recovery_hold": False,
        "execution_unresolved": False, "close_reconciliation_required": False,
        "native_sl_last_verified_price": 100.0, "native_sl_original_price": 100.0,
        "native_sl_status": "UNKNOWN", "native_sl_verified": False,
    }
    state.update(overrides)
    monkeypatch.setattr(ggpbot, "STATE", state)
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {
        "in_position": True, "symbol": SYMBOL, "side": "BUY", "qty": 2.0,
    })
    return state


# --------------------------------------------------------------------------- #
# 1. Conditional-order UNKNOWN vs MISSING
# --------------------------------------------------------------------------- #
def test_empty_fetch_open_orders_with_failed_conditional_query_is_unknown(monkeypatch):
    ggpbot = _safe_runtime(monkeypatch)
    exchange = Exchange(conditional_ok=False)
    monkeypatch.setattr(ggpbot, "ex", exchange)

    status, order_id, matched = ggpbot.verify_native_stop_loss_detailed(
        SYMBOL, "BUY", 100.0, 2.0, "LONG")

    assert status == "UNKNOWN"
    assert order_id is None and matched is None
    assert exchange.create_calls == []


def test_conditional_query_unknown_never_creates_a_stop(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    exchange = Exchange(conditional_ok=False)
    service = TradeExecutionService(exchange)

    result = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "HOLD"
    assert result.reason == "CONDITIONAL_QUERY_UNKNOWN"
    assert exchange.create_calls == []


def test_proven_absence_allows_repair_only_with_journal_evidence(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    ggpbot = _safe_runtime(monkeypatch)
    exchange = Exchange(rows=[])
    service = TradeExecutionService(exchange, api_caller=lambda func, *a, **k: func(*a, **k))

    result = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "PLACED_VERIFIED"
    assert len(exchange.create_calls) == 1

    monkeypatch.setenv("NAISO_NATIVE_SL_REPAIR", "true")
    monkeypatch.setattr(ggpbot, "ex", exchange)
    before = len(exchange.create_calls)
    assert ggpbot._repair_native_sl_from_journal(
        SYMBOL, "BUY", 2.0, prices=[], mode="HEDGE", position_side="LONG") == (
            False, "INSUFFICIENT_JOURNAL_EVIDENCE", None)
    assert len(exchange.create_calls) == before


def test_hold_remains_when_existing_stop_is_at_unproven_level(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    exchange = Exchange(rows=[_stop_row(99.0, "moved-sl")])
    service = TradeExecutionService(exchange)

    result = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG", trigger_prices=[99.0])

    assert result.outcome == "VERIFIED_ADOPTED"
    assert result.verified is True
    assert exchange.create_calls == []

    unknown_price = TradeExecutionService(Exchange(rows=[_stop_row(99.0, "moved-sl")]))
    held = unknown_price.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")
    assert held.outcome == "HOLD"
    assert held.reason == "ACTIVE_STOP_AT_UNPROVEN_LEVEL"
    assert unknown_price.exchange.create_calls == []


# --------------------------------------------------------------------------- #
# 2. API execution safety
# --------------------------------------------------------------------------- #
def test_native_sl_repair_never_bypasses_central_rate_limiter(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    ggpbot = _safe_runtime(monkeypatch)
    gated_calls = []
    rate_calls = []

    def governor(func, *args, **kwargs):
        gated_calls.append(func.__name__)
        ggpbot.rate_limit(func)
        return func(*args, **kwargs)

    monkeypatch.setattr(ggpbot, "rate_limit", lambda func: rate_calls.append(func.__name__))
    exchange = Exchange(rows=[])
    service = TradeExecutionService(exchange, api_caller=governor)

    result = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "PLACED_VERIFIED"
    assert gated_calls == rate_calls
    assert {"fetch_open_orders", "swapV2PrivateGetTradeOpenOrders", "create_order",
            "price_to_precision"}.issubset(set(gated_calls))


def test_timed_out_stop_write_is_never_duplicated(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    ggpbot = _safe_runtime(monkeypatch)
    exchange = Exchange(rows=[], create_error=TimeoutError("timed out after submit"))
    service = TradeExecutionService(exchange, api_caller=ggpbot.safe_api_call)

    result = service.ensure_native_stop(
        symbol=SYMBOL, side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "HOLD"
    assert result.reason == "ORDER_SUBMISSION_UNCERTAIN"
    assert len(exchange.create_calls) == 1
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


@pytest.mark.parametrize("error", [
    PermissionError("permission denied for swap trade"),
    Exception("AuthenticationInvalid: bad api key"),
    Exception("Invalid symbol BTC/UNKNOWN"),
])
def test_non_retryable_api_errors_are_not_repeated(monkeypatch, error):
    import ggpbot
    from naiso_api_resilience import ApiResilience, classify_api_error

    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    resilience = ApiResilience(failure_threshold=50, seed=4)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", resilience)
    assert classify_api_error(error, method="fetch_balance").retryable is False

    calls = []

    def read():
        calls.append(1)
        raise error

    with pytest.raises(type(error)):
        ggpbot.safe_api_call(read)
    assert len(calls) == 1


# --------------------------------------------------------------------------- #
# 3. Watchdog / engine recovery separated from Scanner
# --------------------------------------------------------------------------- #
def test_open_position_engine_resume_restores_management_without_scanner(monkeypatch):
    ggpbot = _safe_runtime(monkeypatch)
    state = _open_state(monkeypatch, ggpbot)
    exchange = Exchange(rows=[_stop_row()], position=_position())
    monkeypatch.setattr(ggpbot, "ex", exchange)

    management = []
    scanner = []
    monkeypatch.setattr(ggpbot, "startup_trade_recovery",
                        lambda: management.append("restored") or True)
    monkeypatch.setattr(ggpbot, "dispatch_scanner_task",
                        lambda *a, **k: scanner.append((a, k)) or True)

    verified, reason, context = ggpbot._verify_engine_recovery()
    assert verified is True
    assert reason == "OPEN_POSITION_VERIFIED"
    assert context["open_position_verified"] is True
    assert context["stop_verified"] is True

    assert ggpbot._resume_main_loop_after_verification(context) is True
    assert management == ["restored"]
    assert scanner == []
    assert state["recovery_hold"] is False


def test_flat_engine_resume_restarts_scanner_only(monkeypatch):
    ggpbot = _safe_runtime(monkeypatch)
    monkeypatch.setattr(ggpbot, "STATE", {
        "open": False, "current_symbol": SYMBOL, "recovery_hold": False,
    })
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "ex", Exchange(rows=[], position=None))

    management = []
    scanner = []
    monkeypatch.setattr(ggpbot, "startup_trade_recovery",
                        lambda: management.append("unexpected") or True)
    monkeypatch.setattr(ggpbot, "dispatch_scanner_task",
                        lambda *a, **k: scanner.append(a) or True)

    verified, reason, context = ggpbot._verify_engine_recovery()
    assert verified is True and reason == "FLAT_VERIFIED"
    assert ggpbot._resume_main_loop_after_verification(context) is True
    assert scanner and scanner[0][0] == "global_discovery"
    assert management == []


def test_watchdog_resumes_engine_with_and_without_open_position(monkeypatch):
    from naiso_health import HealthWatchdog, RestartPolicy

    clock = [0.0]
    for open_position in (True, False):
        contexts = []

        def verifier(open_position=open_position):
            return (True, "VERIFIED", {
                "open_position_verified": open_position,
                "flat_verified": not open_position,
            })

        def restart_fn(context):
            contexts.append(context)
            return True

        gate_allowed = not open_position
        watchdog = HealthWatchdog(
            restart_policy=RestartPolicy(monotonic_fn=lambda: clock[0]),
            safety_gate=lambda: (gate_allowed, "OPEN_POSITION" if not gate_allowed else "READY"),
            monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
        watchdog.register("main_loop", interval_sec=1.0, grace_sec=1.0,
                          restartable=True, recovery_verifier=verifier,
                          restart_fn=restart_fn)
        watchdog.beat("main_loop")
        clock[0] += 10.0
        report = watchdog.check()

        assert report["main_loop"]["status"] == "RECOVERED"
        assert len(contexts) == 1
        assert contexts[0]["open_position_verified"] is open_position


def test_watchdog_defers_restart_when_verification_fails(monkeypatch):
    from naiso_health import HealthWatchdog, RestartPolicy

    clock = [0.0]
    calls = []
    watchdog = HealthWatchdog(
        restart_policy=RestartPolicy(monotonic_fn=lambda: clock[0]),
        safety_gate=lambda: (True, "READY"),
        monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=1.0, restartable=True,
                      recovery_verifier=lambda: (False, "EXCHANGE_STATE_UNKNOWN", {}),
                      restart_fn=lambda: calls.append(1) or True)
    watchdog.beat("main_loop")
    clock[0] = 10.0

    report = watchdog.check()
    assert report["main_loop"]["status"] == "STALLED_SAFE_HOLD"
    assert calls == []


@pytest.mark.parametrize("failure", ["position_query", "conditional_query", "stop_missing"])
def test_recovery_hold_persists_when_any_verification_step_fails(monkeypatch, failure):
    ggpbot = _safe_runtime(monkeypatch)
    _open_state(monkeypatch, ggpbot)
    exchange = Exchange(
        rows=[] if failure == "stop_missing" else [_stop_row()],
        position=_position(), conditional_ok=failure != "conditional_query")
    if failure == "position_query":
        def broken_positions(_symbols=None):
            raise TimeoutError("position query timed out")
        exchange.fetch_positions = broken_positions
    monkeypatch.setattr(ggpbot, "ex", exchange)

    management = []
    scanner = []
    monkeypatch.setattr(ggpbot, "startup_trade_recovery",
                        lambda: management.append("unexpected") or True)
    monkeypatch.setattr(ggpbot, "dispatch_scanner_task",
                        lambda *a, **k: scanner.append("unexpected") or True)

    verified, reason, _context = ggpbot._verify_engine_recovery()
    assert verified is False
    assert reason.startswith("engine_recovery_")
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert management == [] and scanner == []
    assert exchange.create_calls == []
