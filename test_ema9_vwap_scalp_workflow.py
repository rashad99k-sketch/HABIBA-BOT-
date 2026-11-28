"""Acceptance tests for the EMA9 + Session VWAP scalp workflow.

Everything here is PAPER/mock-only. The tests cover the confirmed-cross to
READY lifecycle, the closed-candle gate that protects young scalp trades from
indicator churn, the unchanged full-close/TP ladder, and fail-closed execution
verification. No bot process, exchange connection or live order is started.
"""
from __future__ import annotations

import copy
import time
from types import SimpleNamespace

import pandas as pd
import pytest

import bingx_execution as execution
import ggp_cross_event_pipeline as pipeline
import ggp_profit_brain as brain
import ggpbot

from ggpbot import (
    InstitutionalTradeBrain,
    LiveTradeManager,
    STATE,
    TradeDecisionType,
    TradeManagementBoard,
)


SYMBOL = "BTC/USDT:USDT"
NOW_MS = int(time.time() * 1000)
CANDLE_TS = NOW_MS - 900_000
_MISSING = object()


# ---------------------------------------------------------------------------
# Confirmed EMA9/Session-VWAP cross -> Watchlist -> READY
# ---------------------------------------------------------------------------

def _cross_event(side="BUY", *, adx=22.0, candle_ts=CANDLE_TS):
    buy = str(side).upper() == "BUY"
    event = pipeline.build_cross_event(
        symbol=SYMBOL, side=side, candle_timestamp=candle_ts,
        ema9=101.0 if buy else 99.0,
        previous_ema9=99.0 if buy else 101.0,
        vwap=100.0, previous_vwap=100.0, adx=adx,
        cross_status="CONFIRMED", data_freshness="FRESH", now_ms=NOW_MS,
    )
    assert event is not None
    return event


def _zone_row(side="BUY", *, adx=22.0, mode="SCALP"):
    buy = str(side).upper() == "BUY"
    zone = (
        {"low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
         "strength": 6, "direction": "BULLISH",
         "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH"}
        if buy else
        {"low": 100.5, "high": 101.0, "timestamp": CANDLE_TS - 900_000,
         "strength": 6, "direction": "BEARISH",
         "zone_definition": "BEARISH_OPEN_TO_HIGH", "status": "FRESH"}
    )
    return {
        "symbol": SYMBOL, "exchange_symbol": SYMBOL, "side": side,
        "cross_status": "CONFIRMED",
        "cross_direction": "BULLISH" if buy else "BEARISH",
        "cross_candle_timestamp": CANDLE_TS, "adx": adx,
        "ema9": 101.0 if buy else 99.0, "vwap": 100.0, "price": 100.0,
        "atr": 1.0, "structural_stop": 98.8 if buy else 101.2,
        "data_quality": "VALID_CLOSED_CANDLES", "data_freshness": "FRESH",
        "indicator_snapshot": {
            "ema9": 101.0 if buy else 99.0, "vwap": 100.0,
            "previous_ema9": 99.0 if buy else 101.0, "previous_vwap": 100.0,
            "latest_closed_timestamp": CANDLE_TS, "timeframe_seconds": 900,
            "quality": "FRESH", "cross_status": "CONFIRMED",
            "cross_direction": "BULLISH" if buy else "BEARISH",
            "cross_candle_timestamp": CANDLE_TS,
        },
        "trading_mode": mode, "order_block": zone, "fvg": {},
        "support_resistance": {"supports": [], "resistances": []},
        "decision_state": "CROSS_CONFIRMED",
        "closed_mss_pending": True,
        "no_fresh_nearby_order_block_or_fvg": False,
    }


def _authorize(event, row, store, **overrides):
    zone = pipeline.evaluate_cross_zone(row, now_ms=NOW_MS)
    stop = pipeline.evaluate_stop_loss(row, zone)
    kwargs = dict(
        event=event, analysis_row=row, zone=zone, stop_loss=stop,
        market_state={"position_state": "FLAT"}, risk_state={"allowed": True},
        store=store, now_ms=NOW_MS,
    )
    kwargs.update(overrides)
    return pipeline.evaluate_cross_authorization(**kwargs), zone, stop


@pytest.fixture
def cross_store(tmp_path, monkeypatch):
    store = pipeline.CrossEventStore(str(tmp_path / "cross-events.sqlite3"))
    monkeypatch.setattr(pipeline, "_STORE", store)
    return store


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_confirmed_buy_and_sell_scalp_cross_reaches_ready(side, cross_store):
    event = _cross_event(side)
    cross_store.upsert(event)
    decision, zone, stop = _authorize(event, _zone_row(side), cross_store)
    assert decision["authorized"] is True
    assert decision["final_authorization"] == "ENTRY_AUTHORIZED"
    assert decision["watchlist_status"] == "READY"
    assert decision["trade_mode"] == "SCALP"
    assert zone["valid"] is True and stop["valid"] is True
    assert cross_store.get_state(event.event_id)["lifecycle_status"] == "READY"


def test_adx_below_18_blocks_cross_entry(cross_store):
    weak = _cross_event("BUY", adx=17.99)
    decision, _, _ = _authorize(weak, _zone_row("BUY", adx=17.99), cross_store)
    assert decision["authorized"] is False
    assert decision["rejection_reason"].startswith("adx_at_least_18:")


def test_approaching_converging_and_ema50_only_crosses_are_not_entries():
    for status in ("APPROACHING", "CONVERGING", "NEAR_CROSS"):
        assert pipeline.build_cross_event(
            symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS,
            ema9=101.0, previous_ema9=99.0, vwap=100.0, previous_vwap=100.0,
            adx=22.0, cross_status=status, now_ms=NOW_MS,
        ) is None
    # EMA50 alignment is advisory; without the EMA9 cross proof there is no event.
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS,
        ema9=99.0, previous_ema9=99.0, vwap=100.0, previous_vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=NOW_MS,
    ) is None


def test_watchlist_row_carries_cross_identity_zone_and_pending_state():
    event = _cross_event("BUY")
    row = pipeline.watchlist_row_for_event(event)
    assert row["symbol"] == SYMBOL
    assert row["side"] == "BUY"
    assert row["cross_event_id"] == event.event_id
    assert row["cross_candle_timestamp"] == CANDLE_TS
    assert row["ema9"] == 101.0 and row["session_vwap"] == 100.0
    assert row["adx"] == 22.0
    assert row["watchlist_status"] == "CROSS_CONFIRMED"
    assert row["zone_status"] == "PENDING"
    assert row["entry_authorized"] is False


def test_institutional_zone_analysis_cannot_create_opposite_entry(cross_store):
    row = _zone_row("BUY")
    row["side"] = "SELL"
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is True
    assert zone["selected_zone"]["evaluation_side"] == "BUY"
    decision, _, _ = _authorize(_cross_event("BUY"), row, cross_store)
    assert decision["authorized"] is False
    assert "direction_preserved" in decision["rejection_reason"]


def test_duplicate_event_cannot_be_consumed_twice(cross_store):
    event = _cross_event("BUY")
    cross_store.upsert(event)
    first, _, _ = _authorize(event, _zone_row("BUY"), cross_store)
    assert first["authorized"] is True
    consumed = cross_store.consume_exact(event.event_id, "trade-1")
    assert consumed.status == pipeline.CONSUMPTION_CONSUMED and consumed.ok

    duplicate, _, _ = _authorize(event, _zone_row("BUY"), cross_store)
    assert duplicate["authorized"] is False
    assert "duplicate_event" in duplicate["rejection_reason"]
    repeat = cross_store.consume_exact(event.event_id, "trade-1")
    assert repeat.status == pipeline.CONSUMPTION_IDEMPOTENT and repeat.idempotent
    conflict = cross_store.consume_exact(event.event_id, "trade-2")
    assert conflict.status == pipeline.CONSUMPTION_CONFLICT and not conflict.ok


# ---------------------------------------------------------------------------
# Closed-candle exit gate and target policy
# ---------------------------------------------------------------------------

_STATE_KEYS = (
    "exit_risk_snapshot", "failed_breakout_thesis_credit_applied",
    "last_trade_state_candle_ts", "trade_state", "entry_closed_candle_ts",
    "post_entry_closed_candle_timestamps", "critical_market_risk_verified",
    "exchange_safety_exit",
)


@pytest.fixture(autouse=True)
def _isolate_state(monkeypatch):
    saved = {key: copy.deepcopy(STATE.get(key, _MISSING)) for key in _STATE_KEYS}
    yield
    for key, value in saved.items():
        if value is _MISSING:
            monkeypatch.delitem(STATE, key, raising=False)
        else:
            monkeypatch.setitem(STATE, key, value)


@pytest.fixture
def board():
    return TradeManagementBoard()


def _ctx(**overrides):
    ctx = {
        "symbol": SYMBOL, "side": "BUY", "entry": 100.0,
        "current_price": 100.2, "atr": 1.0, "roe": 0.2,
        "remaining_qty": 2.0, "initial_qty": 2.0,
        "profit_mode": brain.RANGE_MODE, "trade_state": "TREND_RIDE",
        "continuation_probability": 0.7,
        "entry_closed_candle_ts": 1000.0,
        "last_closed_candle_ts": 1000.0,
        "post_entry_closed_candle_timestamps": [],
        "closed_candles_since_entry": 0,
        "scalp_target_status": "VALID", "scalp_target_price": 100.5,
        "range_target_price": 100.5, "range_target_move_pct": 0.5,
        "native_sl_verified": True, "entry_time": time.time(),
        "emergency_exit": False, "structural_invalidation": False,
        "thesis_failure_score": 0.0, "thesis_failure_reasons": [],
    }
    ctx.update(overrides)
    return ctx


def test_scalp_target_policy_is_only_025_and_050_price_moves():
    assert brain.RANGE_PROFIT_TARGETS_PCT == (0.25, 0.50)
    weak_pct, _ = brain.select_range_target_pct(
        {"side": "BUY", "adx": 19.0, "continuation_probability": 0.50, "volume_ratio": 0.60})
    strong_pct, _ = brain.select_range_target_pct(
        {"side": "BUY", "adx": 24.0, "continuation_probability": 0.80,
         "volume_ratio": 1.50, "di_plus": 30.0, "di_minus": 15.0,
         "aligned_closed_candles": 3})
    assert weak_pct == 0.25 and strong_pct == 0.50


def test_scalp_entry_planning_uses_025_050_price_moves(monkeypatch):
    frame = pd.DataFrame({
        "close": [100.0] * 30, "high": [100.1] * 30, "low": [99.9] * 30,
    })
    monkeypatch.setattr(
        ggpbot.PrecisionSafety, "normalize_price",
        staticmethod(lambda _symbol, price: price),
    )
    monkeypatch.setattr(ggpbot, "_market_price_tick_size", lambda _symbol, _price: 1e-8)
    _sl, tp1, tp2 = ggpbot.compute_sl_tp(
        100.0, "BUY", "SCALP", 0.1, frame, symbol=SYMBOL)
    assert tp1 == pytest.approx(100.25, abs=1e-6)
    assert tp2 == pytest.approx(100.50, abs=1e-6)


def test_transient_momentum_does_not_close_young_scalp_before_closed_candles(board):
    ctx = _ctx(
        trade_state="MOMENTUM_COLLAPSE", momentum_decay=True,
        flow_bias="SELL", smart_money={"institutional_bias": "SELL"},
        continuation_probability=0.30, roe=1.0,
        closed_candles_since_entry=0,
    )
    risk = brain.evaluate_exit_risk(ctx)
    assert risk["action"] == "HARD_EXIT"

    deferred = board.evaluate(ctx)
    assert deferred.action is TradeDecisionType.HOLD
    assert "range_scalp_waiting_for_single_target" in deferred.reason
    assert "ordinary_exit_deferred" in STATE["exit_risk_snapshot"]["exit_gate_reason"]

    confirmed = board.evaluate(_ctx(**{**ctx, "closed_candles_since_entry": 2}))
    assert confirmed.action is TradeDecisionType.THESIS_FAILURE_EXIT


def test_one_opposing_indicator_is_not_hard_exit(board):
    ctx = _ctx(
        closed_candles_since_entry=2,
        smart_money={"institutional_bias": "SELL"},
    )
    risk = brain.evaluate_exit_risk(ctx)
    assert risk["action"] != "HARD_EXIT"
    assert "institutional_bias_opposed" in risk["reasons"]

    decision = board.evaluate(ctx)
    assert decision.action not in {
        TradeDecisionType.THESIS_FAILURE_EXIT, TradeDecisionType.FULL_CLOSE
    }
    assert decision.action is TradeDecisionType.HOLD


def test_synthetic_stop_bypasses_ordinary_exit_gate(board):
    ctx = _ctx(
        trade_state="MOMENTUM_COLLAPSE", momentum_decay=True,
        synthetic_sl=100.30, current_price=100.20,
        closed_candles_since_entry=0,
    )
    decision = board.evaluate(ctx)
    assert decision.action is TradeDecisionType.FULL_CLOSE
    assert decision.reason == "synthetic_structural_stop_triggered"


def test_verified_structural_invalidation_bypasses_ordinary_exit_gate(board):
    ctx = _ctx(structural_invalidation=True, closed_candles_since_entry=0)
    decision = board.evaluate(ctx)
    assert decision.action is TradeDecisionType.THESIS_FAILURE_EXIT
    assert decision.params["close_reason"] == "PROFIT_DANGER_HARD_EXIT"


def test_verified_emergency_and_critical_risk_bypass_ordinary_exit_gate(board):
    assert "verified_emergency" in brain.urgent_exit_reasons(
        {"side": "BUY", "emergency_exit": True})
    assert "verified_critical_risk" in brain.urgent_exit_reasons(
        {"side": "BUY", "critical_market_risk_verified": True})
    assert "verified_critical_risk" in brain.urgent_exit_reasons(
        {"side": "BUY", "exchange_safety_exit": True})

    decision = board.evaluate(_ctx(
        emergency_exit=True, emergency_reason="MANUAL_KILL_SWITCH",
        closed_candles_since_entry=0,
    ))
    assert decision.action is TradeDecisionType.EMERGENCY_EXIT


def test_scalp_target_closes_full_remaining_verified_quantity(board):
    decision = board.evaluate(_ctx(
        current_price=100.60, remaining_qty=1.5, initial_qty=2.0,
        closed_candles_since_entry=2,
    ))
    assert decision.action is TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "SCALP_TARGET"
    assert "single_target_full_exit" in decision.reason
    assert "ratio" not in decision.params
    assert "target_remaining_qty" not in decision.params


def test_trend_tp1_tp2_ladder_is_unchanged_with_no_third_target(board):
    tp1 = board.evaluate(_ctx(
        profit_mode=brain.TREND_MODE, current_price=101.20,
        remaining_qty=2.0, initial_qty=2.0, tp1_price=101.0,
        closed_candles_since_entry=2,
    ))
    assert tp1.action is TradeDecisionType.PARTIAL_CLOSE
    assert tp1.params["ratio"] == pytest.approx(0.5)
    assert tp1.params["target_remaining_qty"] == pytest.approx(1.0)
    assert tp1.params["close_reason"] == "TP1"

    tp2 = board.evaluate(_ctx(
        profit_mode=brain.TREND_MODE, current_price=102.20,
        remaining_qty=1.0, initial_qty=2.0, tp1_hit=True,
        tp1_price=101.0, tp2_price=102.0, closed_candles_since_entry=2,
    ))
    assert tp2.action is TradeDecisionType.FULL_CLOSE
    assert tp2.params["close_reason"] == "TP2"

    final = board.evaluate(_ctx(
        profit_mode=brain.TREND_MODE, current_price=103.0,
        remaining_qty=1.0, initial_qty=2.0, tp1_hit=True, tp2_hit=True,
        be_done=True, tp2_price=102.0, trail_stop=102.0,
        closed_candles_since_entry=2,
    ))
    assert final.action not in {
        TradeDecisionType.PARTIAL_CLOSE, TradeDecisionType.FULL_CLOSE
    }


def test_journal_snapshot_preserves_exit_gate_context_for_restart(monkeypatch):
    monkeypatch.setitem(STATE, "entry_closed_candle_ts", 1000.0)
    monkeypatch.setitem(STATE, "post_entry_closed_candle_timestamps", [1900.0, 2800.0])
    monkeypatch.setitem(STATE, "critical_market_risk_verified", False)
    monkeypatch.setitem(STATE, "exchange_safety_exit", False)
    snapshot = ggpbot._journal_snapshot_state()
    assert snapshot["entry_closed_candle_ts"] == 1000.0
    assert snapshot["post_entry_closed_candle_timestamps"] == [1900.0, 2800.0]
    assert snapshot["critical_market_risk_verified"] is False
    assert snapshot["exchange_safety_exit"] is False


def test_new_trade_clears_stale_hard_state_and_gate_context(monkeypatch):
    manager = object.__new__(LiveTradeManager)
    manager.brain = InstitutionalTradeBrain()
    manager.exchange_sync = SimpleNamespace()
    manager.event_bus = SimpleNamespace(emit=lambda *_args, **_kwargs: None)
    manager.brain.state_machine.current_state = "MOMENTUM_COLLAPSE"
    manager.brain.current_trade_state = "MOMENTUM_COLLAPSE"
    manager.brain.last_update = 999.0
    monkeypatch.setitem(STATE, "last_trade_state_candle_ts", 123.0)
    monkeypatch.setitem(STATE, "trade_state", "MOMENTUM_COLLAPSE")
    monkeypatch.setitem(STATE, "entry_closed_candle_ts", 1000.0)
    monkeypatch.setitem(STATE, "post_entry_closed_candle_timestamps", [1900.0])

    manager.start_trade(SYMBOL, "BUY", 100.0, 1.0, 99.0, 101.0, 102.0)

    assert manager.brain.state_machine.current_state == "RANGE_CHOP"
    assert manager.brain.current_trade_state == "RANGE_CHOP"
    assert manager.brain.last_update == 0.0
    assert STATE["last_trade_state_candle_ts"] == 0.0
    assert STATE["trade_state"] == "RANGE_CHOP"


# ---------------------------------------------------------------------------
# Fail-closed entry verification (mocked exchange)
# ---------------------------------------------------------------------------

class _ExecutionExchange:
    def __init__(self, *, order=None, include_positions=True):
        self.order = order or {
            "id": "ex-1", "status": "closed", "filled": 2.0,
            "remaining": 0.0, "average": 101.5,
        }
        self.position = {
            "symbol": SYMBOL, "side": "long", "contracts": 2.0,
            "entryPrice": 101.5,
        }
        self.include_positions = include_positions
        if not include_positions:
            self.fetch_positions = None

    def fetch_position_mode(self, symbol=None):
        return {"hedged": False}

    def set_leverage(self, leverage, symbol, params=None):
        return None

    def fetch_leverage(self, symbol, params=None):
        return {"leverage": 20}

    def create_order(self, symbol, order_type, side, amount, price, params):
        return self.order

    def fetch_order(self, order_id, symbol, params=None):
        return self.order

    def fetch_my_trades(self, symbol, since, limit, params):
        return []

    def fetch_positions(self, symbols=None, params=None):
        if not self.include_positions:
            raise AssertionError("fetch_positions must not be called in this test")
        return [self.position]


def test_partial_fill_is_not_reported_as_filled():
    exchange = _ExecutionExchange(order={
        "id": "ex-part", "status": "open", "filled": 1.0,
        "remaining": 1.0, "average": 101.5,
    })
    exchange.position["contracts"] = 1.0
    result = execution.TradeExecutionService(exchange).submit_market_entry(
        symbol=SYMBOL, side="BUY", quantity=2.0, trade_id="partial-1")
    assert result.state is execution.ExecutionState.PARTIAL_FILL
    assert result.filled_qty == pytest.approx(1.0)
    assert result.remaining_qty == pytest.approx(1.0)


def test_order_ack_without_exchange_position_query_is_execution_uncertain():
    exchange = _ExecutionExchange(include_positions=False)
    result = execution.TradeExecutionService(exchange).submit_market_entry(
        symbol=SYMBOL, side="BUY", quantity=2.0, trade_id="no-position-query")
    assert result.state is execution.ExecutionState.EXECUTION_UNCERTAIN
