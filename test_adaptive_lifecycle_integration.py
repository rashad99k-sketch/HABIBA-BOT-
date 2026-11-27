"""End-to-end adaptive SCALP/TREND lifecycle proof, fully mocked.

This module drives the real entry, reconciliation, policy classification, board
decision, verified close and journal-recovery paths. The exchange is a fake that
only changes position state after a reduce-only order is submitted and read back,
so every assertion about OPEN/FLAT or remaining quantity comes from an
exchange-derived observation rather than from a local assumption.

No credentials, network calls, live orders or bot launches are used.
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from ggp_profit_brain import (
    BEHAVIOR_DISTRIBUTION, BEHAVIOR_INSUFFICIENT, BEHAVIOR_TREND_CONTINUATION,
    CLOSED_STATE, RANGE_MODE, SCALP_STATE, STANDARD_MODE, STANDARD_STATE,
    TREND_MODE, TREND_STATE,
)
from roro_trade_journal import TradeJournal


SYMBOL = "BTC/USDT:USDT"
RAW_SYMBOL = "BTC-USDT"
SL = 95.0
TP1_PRICE = 105.0
TP2_PRICE = 110.0


class FakeExchange:
    """Minimal deterministic exchange with truthful tri-state position deltas."""

    def __init__(self, *, hedged=True):
        self.hedged = bool(hedged)
        self.mark = 100.0
        self.position = None
        self.orders = []
        self.open_orders = []
        self.create_calls = []
        self._seq = 0

    def _next_id(self, prefix):
        self._seq += 1
        return f"mock-{prefix}-{self._seq}"

    def set_mark(self, price):
        self.mark = float(price)
        if not self.position:
            return
        qty = float(self.position["contracts"])
        entry = float(self.position["entryPrice"])
        pside = str((self.position.get("info") or {}).get("positionSide") or "LONG").upper()
        self.position["markPrice"] = self.mark
        self.position["unrealizedPnl"] = ((entry - self.mark) if pside == "SHORT" else (self.mark - entry)) * qty
        self.position["initialMargin"] = entry * qty / 20.0
        self.position["leverage"] = 20.0

    def market(self, symbol):
        return {"id": RAW_SYMBOL, "symbol": SYMBOL, "contractSize": 1.0}

    def amount_to_precision(self, symbol, amount):
        return float(round(float(amount), 8))

    def fetch_balance(self):
        return {"free": {"USDT": 100000.0}, "total": {"USDT": 100000.0}}

    def fetch_ticker(self, symbol, params=None):
        return {"last": self.mark, "close": self.mark}

    def set_leverage(self, leverage, symbol, params=None):
        return {"leverage": int(leverage)}

    def fetch_leverage(self, symbol, params=None):
        return {"leverage": 20}

    def fetch_position_mode(self, symbol=None):
        return {"hedged": self.hedged}

    def fetch_positions(self, symbols=None):
        if not self.position or float(self.position.get("contracts", 0)) <= 0:
            return []
        return [dict(self.position)]

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params=None):
        return [dict(order) for order in self.open_orders]

    def swapV2PrivateGetTradeOpenOrders(self, params=None):
        return {"code": 0, "data": {"orders": []}}

    def fetch_closed_orders(self, symbol=None, since=None, limit=None, params=None):
        return [dict(order) for order in self.orders if order.get("_kind") == "market"]

    def fetch_order(self, order_id, symbol=None, params=None):
        for order in self.orders:
            if str(order.get("id")) == str(order_id) or str(order.get("clientOrderId")) == str(order_id):
                return dict(order)
        return {"id": str(order_id), "status": "closed"}

    def create_order(self, symbol, kind, side, amount, price=None, params=None):
        params = dict(params or {})
        side = str(side).lower()
        amount = float(amount)
        self.create_calls.append((symbol, str(kind), side, amount, params))
        order_id = self._next_id("m" if str(kind).upper() == "MARKET" else "s")

        if str(kind).upper() == "STOP_MARKET":
            order = {
                "id": order_id, "status": "NEW", "symbol": RAW_SYMBOL,
                "type": "STOP_MARKET", "side": side,
                "positionSide": params.get("positionSide", "LONG"),
                "stopPrice": str(params.get("stopPrice", SL)),
                "origQty": str(amount), "amount": amount,
                "info": {"orderId": order_id, "symbol": RAW_SYMBOL},
                "_kind": "stop",
            }
            self.open_orders.append(order)
            self.orders.append(order)
            return dict(order)

        if self.position is None:
            entry = self.mark
            position_side = params.get("positionSide", "LONG")
            self.position = {
                "symbol": SYMBOL,
                "side": "short" if str(position_side).upper() == "SHORT" else "long",
                "contracts": amount,
                "entryPrice": entry, "markPrice": entry,
                "unrealizedPnl": 0.0, "initialMargin": entry * amount / 20.0,
                "leverage": 20.0, "contractSize": 1.0,
                "info": {"positionSide": position_side, "positionAmt": str(amount),
                         "avgPrice": str(entry), "leverage": 20.0},
            }
            order = {
                "id": order_id, "status": "closed", "symbol": RAW_SYMBOL,
                "side": side, "amount": amount, "filled": amount, "remaining": 0.0,
                "average": entry, "clientOrderId": params.get("clientOrderId"),
                "info": {"orderId": order_id, "executedQty": str(amount),
                         "avgPrice": str(entry), "symbol": RAW_SYMBOL,
                         "positionSide": params.get("positionSide", "LONG"),
                         "realizedPnl": "0"},
                "_kind": "market",
            }
            self.orders.append(order)
            return dict(order)

        before = float(self.position["contracts"])
        entry = float(self.position["entryPrice"])
        pside = str((self.position.get("info") or {}).get("positionSide") or "LONG").upper()
        filled = min(before, amount)
        after = max(0.0, before - filled)
        realized = ((entry - self.mark) if pside == "SHORT" else (self.mark - entry)) * filled
        if after <= 1e-9:
            self.position = None
        else:
            self.position["contracts"] = after
            self.position["info"]["positionAmt"] = str(after)
            self.position["unrealizedPnl"] = (self.mark - entry) * after
            self.position["initialMargin"] = entry * after / 20.0
        order = {
            "id": order_id, "status": "closed", "symbol": RAW_SYMBOL,
            "side": side, "amount": filled, "filled": filled, "remaining": 0.0,
            "average": self.mark,
            "info": {"orderId": order_id, "executedQty": str(filled),
                     "avgPrice": str(self.mark), "symbol": RAW_SYMBOL,
                     "positionSide": params.get("positionSide", "LONG"),
                     "type": "MARKET", "status": "CLOSED",
                     "realizedPnl": str(realized)},
            "_kind": "market",
        }
        self.orders.append(order)
        return dict(order)


def _frame(end_ts, *, volume=100.0, volume_ratio=1.0):
    rows = []
    for i in range(120):
        close = 100.0 + ((i % 5) - 2) * 0.05
        vol = volume * (volume_ratio if i == 119 else 1.0)
        rows.append({
            "timestamp": end_ts - (119 - i) * 900,
            "open": close - 0.02, "high": close + 0.30,
            "low": close - 0.30, "close": close, "volume": vol,
        })
    return pd.DataFrame(rows)


def _fresh_state():
    template = dict(ggpbot.STATE)
    ggpbot.STATE.clear()
    for key in template:
        ggpbot.STATE[key] = None
    for flag in ("open", "recovery_hold", "execution_unresolved",
                 "close_reconciliation_required", "close_verified",
                 "native_sl_verified", "tp1_hit", "tp2_hit", "be_done",
                 "trail_activated", "partial_closed", "roe_verified",
                 "leverage_verified"):
        ggpbot.STATE[flag] = False
    ggpbot.TRADE_STATE.clear()
    ggpbot.TRADE_STATE.update({"in_position": False, "qty": 0.0, "tp1_hit": False,
                               "tp2_hit": False, "trail_on": False})
    ggpbot.RECOVERY_BLOCK_ENTRIES = True
    ggpbot._ACTIVE_TRADE = False
    ggpbot.DASHBOARD_STATE["live_trade_mode"] = False
    ggpbot.STATE["bot_close_order_ids"] = []
    ggpbot.STATE["trade_regime_state"] = None
    ggpbot.STATE["profit_mode"] = None
    ggpbot._live_manager.lifecycle_state = ggpbot.TradeLifecycleState.IDLE


@pytest.fixture
def harness(monkeypatch, tmp_path):
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", True)
    monkeypatch.setattr(ggpbot.time, "sleep", lambda *_args, **_kwargs: None)
    _fresh_state()

    db_path = Path(tmp_path) / "adaptive-lifecycle.sqlite3"
    journal = TradeJournal(str(db_path))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    exchange = FakeExchange()
    monkeypatch.setattr(ggpbot, "ex", exchange)

    scenario = {
        "mark": 100.0, "candle_ts": time.time() - 7200,
        "volume": 100.0, "volume_ratio": 0.5,
        "adx": 14.0, "adx_slope": -1.0, "di_plus": 18.0, "di_minus": 22.0,
        "continuation": 0.30, "pressure": 30.0, "momentum": 30.0,
        "distribution": 60.0, "trend_expansion": False,
        "regime": "RANGE_CHOP", "trade_state": "RANGE_CHOP",
        "behavior": BEHAVIOR_DISTRIBUTION, "behavior_reason": "mock_weak_rejection",
        "behavior_evidence": {"aligned_closes": 0, "counter_candles": 2,
                              "adverse_atr": 0.7, "structural_invalidation": False,
                              "adverse_rejection": True},
        "structure": "none", "ema": "INSUFFICIENT_DATA",
        "pullback": "WEAK_PULLBACK", "trend_health": "WEAK",
    }
    events = []
    manager = ggpbot._live_manager

    for name in ("tg_entry", "tg_close", "tg_sl_hit", "tg_tp_hit", "tg_error",
                 "send_once", "update_stats", "update_position_dashboard",
                 "clear_position_dashboard"):
        monkeypatch.setattr(ggpbot, name, lambda *_args, **_kwargs: None)

    def _record(event_type, status="INFO", **fields):
        events.append((str(event_type), str(status), dict(fields)))
        return {"event_type": event_type, "status": status, **fields}

    monkeypatch.setattr(ggpbot, "record_trade_management_event", _record)

    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda _symbol: scenario["mark"])
    monkeypatch.setattr(ggpbot, "get_spread_bps", lambda _symbol: 0.01)
    monkeypatch.setattr(
        ggpbot, "get_ohlcv_safe",
        lambda _symbol, limit=100: _frame(scenario["candle_ts"] - 900,
                                          volume=scenario["volume"],
                                          volume_ratio=scenario["volume_ratio"]),
    )
    monkeypatch.setattr(
        ggpbot, "get_ohlcv_safe_meta",
        lambda _symbol, limit=120: (_frame(scenario["candle_ts"],
                                           volume=scenario["volume"],
                                           volume_ratio=scenario["volume_ratio"]),
                                    "MOCK_CLOSED"),
    )
    monkeypatch.setattr(ggpbot, "compute_atr", lambda df: pd.Series([1.0] * len(df)))
    monkeypatch.setattr(ggpbot, "compute_rsi", lambda df: pd.Series([55.0] * len(df)))
    monkeypatch.setattr(ggpbot, "compute_vwap", lambda df: pd.Series([100.0] * len(df)))
    monkeypatch.setattr(
        ggpbot, "get_di_components",
        lambda df: (scenario["di_plus"], scenario["di_minus"],
                    scenario["adx"], scenario["adx_slope"]),
    )
    monkeypatch.setattr(ggpbot, "ema_trend_context",
                        lambda df, side, atr: {"state": scenario["ema"]})
    monkeypatch.setattr(ggpbot.trend_engine, "analyze_pullback",
                        lambda df, side, atr: scenario["pullback"])
    monkeypatch.setattr(ggpbot.trend_engine, "get_trend_health",
                        lambda df, side: scenario["trend_health"])
    monkeypatch.setattr(ggpbot, "detect_structure_shift",
                        lambda df: scenario["structure"])
    monkeypatch.setattr(
        ggpbot, "analyze_post_entry_behavior",
        lambda df, side, entry, atr, context=None: (
            scenario["behavior"], scenario["behavior_reason"],
            dict(scenario["behavior_evidence"]),
        ),
    )
    monkeypatch.setattr(ggpbot.SmartMoneyEngine, "analyze_smart_money",
                        lambda df: {"distribution_risk": scenario["distribution"]})
    monkeypatch.setattr(ggpbot.MomentumFlowEngine, "analyze_momentum_flow",
                        lambda df: {"momentum_health": scenario["momentum"],
                                    "trend_expansion": scenario["trend_expansion"]})
    monkeypatch.setattr(ggpbot._continuation_engine, "evaluate",
                        lambda *args, **kwargs: ggpbot.ContinuationEvaluation(
                            continuation_probability=scenario["continuation"],
                            trend_strength=scenario["continuation"],
                            exhaustion_probability=0.0,
                            reclaim_risk=0.0,
                            counter_pressure=0.0,
                            confidence=0.6,
                            reasons=["mock_continuation"],
                            should_hold=scenario["continuation"] >= 0.62,
                            hold_quality="MOCK",
                        ))
    monkeypatch.setattr(ggpbot, "tp2_reassessment",
                        lambda *args, **kwargs: ("HOLD", "mock_continuation_strong"))

    monkeypatch.setattr(manager.regime_classifier, "classify",
                        lambda df, ob: scenario["regime"])
    monkeypatch.setattr(manager.brain, "update",
                        lambda smart, momentum, adx, regime: scenario["trade_state"])
    monkeypatch.setattr(manager.continuation_pressure_engine, "calculate_pressure",
                        lambda *args, **kwargs: (scenario["pressure"], []))
    monkeypatch.setattr(manager.thesis_failure_engine, "evaluate_failure",
                        lambda *args, **kwargs: (False, [], 0.0))
    monkeypatch.setattr(manager.confidence_engine, "update_live_confidence",
                        lambda old, *args, **kwargs: old)
    monkeypatch.setattr(ggpbot.ConfidenceEngine, "apply_institutional_modifiers",
                        lambda confidence, *args, **kwargs: confidence)

    return {
        "monkeypatch": monkeypatch, "tmp_path": tmp_path, "db_path": db_path,
        "journal": journal, "exchange": exchange, "scenario": scenario,
        "events": events, "manager": manager,
    }


def _trend_scenario(scenario):
    scenario.update({
        "mark": 100.0, "volume": 100.0, "volume_ratio": 1.30,
        "adx": 28.0, "adx_slope": 1.0, "di_plus": 30.0, "di_minus": 15.0,
        "continuation": 0.80, "pressure": 80.0, "momentum": 75.0,
        "distribution": 10.0, "trend_expansion": True,
        "regime": "STRONG_TREND", "trade_state": "TREND_RIDE",
        "behavior": BEHAVIOR_TREND_CONTINUATION,
        "behavior_reason": "mock_aligned_closed_candles",
        "behavior_evidence": {"aligned_closes": 3, "counter_candles": 0,
                              "adverse_atr": 0.0, "structural_invalidation": False,
                              "adverse_rejection": False},
        "structure": "bullish_shift", "ema": "BULLISH",
        "pullback": "NO_PULLBACK", "trend_health": "HEALTHY",
    })


def _standard_scenario(scenario):
    """Two weak signals but no proven range/thin liquidity: keep 50/50."""
    scenario.update({
        "mark": 100.0, "volume": 100.0, "volume_ratio": 1.0,
        "adx": 14.0, "adx_slope": -1.0, "di_plus": 30.0, "di_minus": 18.0,
        "continuation": 0.30, "pressure": 30.0, "momentum": 50.0,
        "distribution": 30.0, "trend_expansion": False,
        "regime": "UNKNOWN", "trade_state": "UNKNOWN",
        "behavior": BEHAVIOR_INSUFFICIENT,
        "behavior_reason": "mock_insufficient_evidence",
        "behavior_evidence": {"aligned_closes": 0, "counter_candles": 0,
                              "adverse_atr": 0.0, "structural_invalidation": False,
                              "adverse_rejection": False},
        "structure": "none", "ema": "INSUFFICIENT_DATA",
        "pullback": "NO_PULLBACK", "trend_health": "UNKNOWN",
    })


def _short_trend_scenario(scenario):
    _trend_scenario(scenario)
    scenario.update({
        "di_plus": 15.0, "di_minus": 30.0,
        "structure": "bearish_shift", "ema": "BEARISH",
    })


def _open_trade(h, trade_id, *, side="BUY", sl=SL, tp1=TP1_PRICE, tp2=TP2_PRICE):
    """Real entry executor + real position verification + durable journal start."""
    exchange = h["exchange"]
    expected_leg = "LONG" if side == "BUY" else "SHORT"
    management_state = {
        "trade_id": trade_id, "synthetic_sl": sl, "native_sl_original_price": sl,
        "tp1_price": tp1, "tp2_price": tp2,
        "initial_qty": 2.0, "qty": 2.0, "remaining_qty": 2.0,
    }
    order = ggpbot.open_position(
        side, 2.0, SYMBOL, stop_loss=sl, trade_id=trade_id,
        management_state=management_state,
    )
    assert order is not None
    assert float(order["filled"]) == 2.0
    assert float(order["average"]) == 100.0
    assert order.get("native_sl_verified") is True

    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": side, "trade_id": trade_id,
        "entry": 100.0, "qty": 2.0, "initial_qty": 2.0, "actual_filled_qty": 2.0,
        "exchange_qty": 2.0, "remaining_qty": 2.0, "margin": 10.0,
        "initial_margin": 10.0, "mark_price": 100.0, "roe_pct": 0.0,
        "unrealized_pnl_usdt": 0.0, "leverage": 20.0, "contract_size": 1.0,
        "sl": sl, "synthetic_sl": sl, "native_sl_original_price": sl,
        "tp1_price": tp1, "tp2_price": tp2, "synthetic_tp1": tp1,
        "original_tp1_price": tp1, "original_tp2_price": tp2,
        "position_mode": order.get("positionMode", "HEDGE"),
        "position_side": order.get("positionSide", expected_leg),
        "entry_client_order_id": order.get("clientOrderId"),
        "entry_exchange_order_id": order.get("id"),
        "entry_execution_state": "FILLED",
        "native_sl_order_id": order.get("native_sl_order_id"),
        "native_sl_verified": True, "native_sl_status": "VERIFIED",
        "recovery_hold": False, "execution_unresolved": False,
        "close_reconciliation_required": False, "close_verified": False,
        "tp1_hit": False, "tp2_hit": False, "partial_closed": False,
        "be_done": False, "trail_activated": False, "trail_stop": 0.0,
        "max_price": 100.0, "min_price": 100.0, "peak_roe": 0.0,
        "peak_drawdown_roe": 0.0, "bot_close_order_ids": [],
        "entry_time": time.time(), "trade_lifecycle_state": "LIVE",
        "native_sl_policy": "FIXED_FROM_ENTRY", "be_local": False,
        "tp1_filled_qty": 0.0, "tp2_filled_qty": 0.0,
        "last_close_realized_pnl": 0.0, "last_close_pnl_complete": True,
    })
    ggpbot.TRADE_STATE.update({
        "in_position": True, "symbol": SYMBOL, "side": side,
        "entry": 100.0, "qty": 2.0, "last_update_ts": time.time(),
    })
    ggpbot.RECOVERY_BLOCK_ENTRIES = False
    assert ggpbot._persist_trade_journal_start(setup_type="ADAPTIVE_LIFECYCLE") is True
    assert ggpbot._persist_trade_journal_snapshot(force=True) is True

    ggpbot.sync_position_state(SYMBOL)
    ggpbot._live_manager.start_trade(SYMBOL, side, 100.0, 2.0, sl, tp1, tp2)
    ggpbot._live_manager.lifecycle_state = ggpbot.TradeLifecycleState.LIVE
    ggpbot.DASHBOARD_STATE["live_trade_mode"] = True

    status, position = ggpbot.fetch_position_tri_state(SYMBOL)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 2.0
    assert ggpbot.STATE["leverage_verified"] is True
    assert ggpbot.STATE["leverage_source"] == "exchange_snapshot"
    return order


def _run_cycle(h):
    """One real management decision cycle with deterministic closed-candle inputs."""
    exchange = h["exchange"]
    scenario = h["scenario"]
    manager = h["manager"]
    exchange.set_mark(scenario["mark"])
    ggpbot.sync_position_state(SYMBOL)
    manager.last_heavy_calc_ts = 0.0
    manager.last_management_ts = 0.0
    manager.last_log_ts = 0.0
    manager.lifecycle_state = ggpbot.TradeLifecycleState.LIVE
    manager._apply_management(SYMBOL, time.time())
    scenario["candle_ts"] += 900
    return manager._last_decision


def _status(exchange):
    return ggpbot.fetch_position_tri_state(SYMBOL)


def test_scalp_lifecycle_closes_full_position_only_after_exchange_verification(harness):
    exchange = harness["exchange"]
    _open_trade(harness, "SCALP-LIFECYCLE")
    assert len([c for c in exchange.create_calls if c[1] == "market"]) == 1

    # Below target: classify from evidence and select one ROE target, no close.
    harness["scenario"]["mark"] = 100.10
    classification_decision = _run_cycle(harness)
    classification_state = dict(ggpbot.STATE)
    assert classification_decision is None or classification_decision.action is ggpbot.TradeDecisionType.HOLD
    assert classification_state["trade_regime_state"] == SCALP_STATE
    assert classification_state["profit_mode"] == RANGE_MODE
    assert classification_state["scalp_target_roe_pct"] == 5.0
    assert classification_state["tp1_target_roe_pct"] == 0.0
    assert classification_state["tp2_target_roe_pct"] == 0.0
    assert len([c for c in exchange.create_calls if c[1] == "market"]) == 1

    harness["scenario"]["mark"] = 100.25  # 0.50 USDT PnL on 10 USDT margin = 5% ROE
    decision = _run_cycle(harness)

    state = ggpbot.STATE
    assert state["trade_regime_state"] == CLOSED_STATE
    assert classification_state["trade_regime_reason"].startswith("initial_classification_")
    assert classification_state["profit_mode"] == RANGE_MODE
    assert classification_state["scalp_target_roe_pct"] == 5.0
    assert state["scalp_target_roe_pct"] in (5.0, 10.0, 20.0)
    assert state["tp1_target_roe_pct"] == 0.0
    assert state["tp2_target_roe_pct"] == 0.0
    assert state["roe_verified"] is True
    assert abs(state["computed_roe_pct"] - 5.0) < 1e-6
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "SCALP_TARGET"
    assert decision.action is not ggpbot.TradeDecisionType.PARTIAL_CLOSE

    assert len([c for c in exchange.create_calls if c[1] == "market"]) == 2
    close_call = [c for c in exchange.create_calls if c[1] == "market"][-1]
    assert close_call[4].get("positionSide") == "LONG"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert state["open"] is False
    assert state["trade_regime_state"] == CLOSED_STATE
    assert state["last_exchange_close_status"] == "FLAT"

    verified = [e for e in harness["events"] if e[0] == "SCALP_TARGET_VERIFIED"]
    assert len(verified) == 1
    assert verified[0][1] == "EXCHANGE_VERIFIED"
    assert not any(e[0] in {"TP1_VERIFIED", "TP2_VERIFIED"} for e in harness["events"])

    row = harness["journal"].get("SCALP-LIFECYCLE")
    assert row is not None
    assert int(row["finalized"]) == 1

    calls_before = len(exchange.create_calls)
    assert ggpbot.safe_close_position(SYMBOL, close_reason="DUPLICATE_CHECK") is False
    assert len(exchange.create_calls) == calls_before


def test_trend_lifecycle_tp1_restart_tp2_without_duplicate_close(harness):
    exchange = harness["exchange"]
    _trend_scenario(harness["scenario"])
    _open_trade(harness, "TREND-LIFECYCLE")

    # Confirm TREND from aligned closed-candle evidence before any profit target.
    harness["scenario"]["mark"] = 100.10
    classification_decision = _run_cycle(harness)
    assert classification_decision is None or classification_decision.action is ggpbot.TradeDecisionType.HOLD
    assert ggpbot.STATE["trade_regime_state"] == TREND_STATE
    assert ggpbot.STATE["profit_mode"] == TREND_MODE
    assert ggpbot.STATE["tp1_target_roe_pct"] == 40.0
    assert ggpbot.STATE["tp2_target_roe_pct"] == 60.0
    assert ggpbot.STATE["scalp_target_roe_pct"] == 0.0
    assert ggpbot.STATE["tp1_price"] == 102.0
    assert ggpbot.STATE["tp2_price"] == 103.0

    harness["scenario"]["mark"] = 102.0  # 4 USDT PnL on 10 USDT margin = 40% ROE
    tp1_decision = _run_cycle(harness)
    assert ggpbot.STATE["trade_regime_state"] == TREND_STATE
    assert ggpbot.STATE["profit_mode"] == TREND_MODE
    assert ggpbot.STATE["tp1_target_roe_pct"] == 40.0
    assert ggpbot.STATE["tp2_target_roe_pct"] == 60.0
    assert tp1_decision.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert tp1_decision.params["ratio"] == 0.5
    assert tp1_decision.params["target_remaining_qty"] == 1.0
    assert tp1_decision.params["close_reason"] == "TP1"

    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 1.0
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["exchange_qty"] == 1.0
    assert ggpbot.STATE["tp1_hit"] is True
    assert ggpbot.STATE["tp1_filled_qty"] == 1.0
    assert ggpbot.STATE["last_exchange_close_status"] == "OPEN"
    assert any(e[0] == "TP1_VERIFIED" and e[1] == "EXCHANGE_VERIFIED"
               for e in harness["events"])

    saved = harness["journal"].get("TREND-LIFECYCLE")["state"]
    assert saved["tp1_hit"] is True
    assert saved["remaining_qty"] == 1.0
    assert saved["trade_regime_state"] == TREND_STATE
    assert saved["profit_mode"] == TREND_MODE
    assert saved["tp2_target_roe_pct"] == 60.0
    assert saved["tp2_price"] == 103.0
    assert saved["leverage_verified"] is True
    assert saved["bot_close_order_ids"]

    calls_before_restart = len(exchange.create_calls)

    # Simulated bot restart: clear all live STATE, reopen the same journal and
    # let startup recovery restore from journal + exchange truth.
    _fresh_state()
    reopened = TradeJournal(str(harness["db_path"]))
    harness["monkeypatch"].setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", reopened)
    assert ggpbot.startup_trade_recovery() is True
    assert ggpbot.STATE["tp1_hit"] is True
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["exchange_qty"] == 1.0
    assert ggpbot.STATE["trade_regime_state"] == TREND_STATE
    assert ggpbot.STATE["profit_mode"] == TREND_MODE
    assert ggpbot.STATE["tp1_target_roe_pct"] == 40.0
    assert ggpbot.STATE["tp2_target_roe_pct"] == 60.0
    assert ggpbot.STATE["leverage_verified"] is True
    assert ggpbot.STATE["leverage"] == 20.0
    assert ggpbot.STATE["recovery_hold"] is False
    assert ggpbot.STATE["native_sl_verified"] is True
    assert len(exchange.create_calls) == calls_before_restart

    # Runner reaches the second fixed ROE target after restart.
    harness["scenario"]["mark"] = 103.0  # 3 USDT PnL on 5 USDT margin = 60% ROE
    tp2_decision = _run_cycle(harness)
    assert tp2_decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert tp2_decision.params["close_reason"] == "TP2"

    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.PERF["last_trade"]["close_reason"] == "TP2"
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["trade_regime_state"] == CLOSED_STATE
    assert ggpbot.STATE["last_exchange_close_status"] == "FLAT"
    assert any(e[0] == "TP2_VERIFIED" and e[1] == "EXCHANGE_VERIFIED"
               for e in harness["events"])
    assert int(reopened.get("TREND-LIFECYCLE")["finalized"]) == 1

    calls_after_tp2 = len(exchange.create_calls)
    assert ggpbot.safe_close_position(SYMBOL, close_reason="DUPLICATE_AFTER_TP2") is False
    assert len(exchange.create_calls) == calls_after_tp2
    assert calls_after_tp2 == calls_before_restart + 1


def test_one_way_close_uses_reduce_only_and_verify_quantity_delta(harness):
    """ReduceOnly is preserved where the venue mode supports it."""
    exchange = FakeExchange(hedged=False)
    harness["monkeypatch"].setattr(ggpbot, "ex", exchange)
    exchange.position = {
        "symbol": SYMBOL, "side": "long", "contracts": 2.0,
        "entryPrice": 100.0, "markPrice": 100.0, "unrealizedPnl": 0.0,
        "initialMargin": 10.0, "leverage": 20.0, "contractSize": 1.0,
        "info": {"positionAmt": "2", "avgPrice": "100"},
    }
    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": "BUY", "entry": 100.0,
        "remaining_qty": 2.0, "exchange_qty": 2.0, "initial_qty": 2.0,
        "mark_price": 100.0, "margin": 10.0, "leverage": 20.0,
    })

    assert ggpbot.safe_close_partial(SYMBOL, 0.5, close_reason="TP1",
                                     target_remaining_qty=1.0) is True
    close_calls = [c for c in exchange.create_calls if c[1] == "market"]
    assert len(close_calls) == 1
    assert close_calls[0][4].get("reduceOnly") is True
    assert close_calls[0][4].get("positionSide") == "BOTH"
    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 1.0
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["close_verified"] is False


def test_one_way_short_close_uses_buy_reduce_only_and_verify_quantity_delta(harness):
    """A One-Way short is closed with a buy reduce-only order and verified FLAT."""
    exchange = FakeExchange(hedged=False)
    harness["monkeypatch"].setattr(ggpbot, "ex", exchange)
    exchange.position = {
        "symbol": SYMBOL, "side": "short", "contracts": 2.0,
        "entryPrice": 100.0, "markPrice": 100.0, "unrealizedPnl": 0.0,
        "initialMargin": 10.0, "leverage": 20.0, "contractSize": 1.0,
        "info": {"positionAmt": "-2", "avgPrice": "100", "positionSide": "BOTH"},
    }
    ggpbot.STATE.update({
        "open": True, "current_symbol": SYMBOL, "side": "SELL", "entry": 100.0,
        "remaining_qty": 2.0, "exchange_qty": 2.0, "initial_qty": 2.0,
        "mark_price": 100.0, "margin": 10.0, "leverage": 20.0,
    })

    assert ggpbot.safe_close_position(SYMBOL, close_reason="SHORT_TEST") is True
    close_calls = [c for c in exchange.create_calls if c[1] == "market"]
    assert len(close_calls) == 1
    assert close_calls[0][2] == "buy"
    assert close_calls[0][4].get("reduceOnly") is True
    assert close_calls[0][4].get("positionSide") == "BOTH"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["close_verified"] is True


def test_pending_close_is_journaled_blocks_duplicate_and_clears_only_after_proof(harness):
    """An unproven MARKET close stays pending, blocks retries, and clears on proof."""
    exchange = harness["exchange"]
    _open_trade(harness, "PENDING-CLOSE")
    original_tri_state = ggpbot.fetch_position_tri_state
    pending_order = {"id": None}

    def _stuck_tri_state(symbol):
        status, position = original_tri_state(symbol)
        if ggpbot.STATE.get("pending_close_order_id"):
            if status != "OPEN" or position is None:
                position = {"symbol": SYMBOL, "contracts": 2.0,
                            "info": {"positionAmt": "2", "positionSide": "LONG"}}
            else:
                position = dict(position)
                position["contracts"] = 2.0
                position = dict(position, info=dict(position.get("info") or {}, positionAmt="2"))
            status = "OPEN"
        return status, position

    def _new_order(order_id, symbol=None, params=None):
        pending_order["id"] = str(order_id)
        return {"id": str(order_id), "status": "NEW"}

    harness["monkeypatch"].setattr(ggpbot, "fetch_position_tri_state", _stuck_tri_state)
    harness["monkeypatch"].setattr(exchange, "fetch_order", _new_order)

    assert ggpbot.safe_close_position(SYMBOL, close_reason="PENDING_TEST") is False
    order_id = ggpbot.STATE["pending_close_order_id"]
    assert order_id == pending_order["id"]
    assert ggpbot.STATE["pending_close_reason"] == "PENDING_TEST"
    assert ggpbot.STATE["pending_close_before_qty"] == 2.0
    assert ggpbot.STATE["pending_close_requested_qty"] == 2.0
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.STATE["close_reconciliation_required"] is True
    assert ggpbot.STATE["close_verified"] is False
    assert ggpbot.STATE["remaining_qty"] == 2.0
    assert ggpbot.STATE["trade_regime_state"] != CLOSED_STATE

    calls_after_first = len(exchange.create_calls)
    row = harness["journal"].get("PENDING-CLOSE")
    assert row["state"]["pending_close_order_id"] == order_id

    assert ggpbot.safe_close_position(SYMBOL, close_reason="PENDING_RETRY") is False
    assert len(exchange.create_calls) == calls_after_first

    # A terminal order read plus a real position delta is the only proof that
    # clears the pending close and adopts the smaller remaining quantity.
    harness["monkeypatch"].setattr(ggpbot, "fetch_position_tri_state", original_tri_state)
    harness["monkeypatch"].setattr(
        exchange, "fetch_order",
        lambda order_id, symbol=None, params=None: {"id": str(order_id), "status": "CLOSED"})
    exchange.position = {
        "symbol": SYMBOL, "side": "long", "contracts": 1.0,
        "entryPrice": 100.0, "markPrice": 100.0, "unrealizedPnl": 0.0,
        "initialMargin": 5.0, "leverage": 20.0, "contractSize": 1.0,
        "info": {"positionAmt": "1", "positionSide": "LONG", "avgPrice": "100"},
    }
    snap = ggpbot._snapshot_from_position(SYMBOL, exchange.position)
    assert ggpbot._resolve_pending_close_from_snapshot(SYMBOL, snap) is True
    assert ggpbot.STATE["pending_close_order_id"] is None
    assert ggpbot.STATE["execution_unresolved"] is False
    assert ggpbot.STATE["close_reconciliation_required"] is False
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["last_close_filled_qty"] == 1.0


def test_concurrent_close_is_suppressed_by_nonblocking_close_lock(harness):
    """A second close cannot submit a duplicate non-idempotent MARKET order."""
    exchange = harness["exchange"]
    _open_trade(harness, "CLOSE-LOCK")
    market_calls_before = len([c for c in exchange.create_calls if c[1] == "market"])
    assert ggpbot._CLOSE_ORDER_LOCK.acquire(blocking=False) is True
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(ggpbot.safe_close_position, SYMBOL, 2.0,
                                 close_reason="DUPLICATE_LOCK_TEST")
            assert future.result(timeout=5) is False
    finally:
        ggpbot._CLOSE_ORDER_LOCK.release()
    assert len([c for c in exchange.create_calls if c[1] == "market"]) == market_calls_before
    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 2.0
    assert ggpbot.STATE["close_verified"] is False


def test_runtime_reconcile_holds_on_external_quantity_increase(harness):
    """An exchange quantity increase must not silently rewrite managed exposure."""
    exchange = harness["exchange"]
    _open_trade(harness, "RUNTIME-QTY-HOLD")
    before_calls = len(exchange.create_calls)

    exchange.position["contracts"] = 3.0
    exchange.set_mark(100.0)
    result = ggpbot.sync_position_state(SYMBOL)

    assert result == (None, None, None, None)
    assert ggpbot.STATE["recovery_hold"] is True
    assert "quantity_exceeds_original_fill" in ggpbot.STATE["recovery_reason"]
    assert "quantity_increased_since_last_sync" in ggpbot.STATE["recovery_reason"]
    assert ggpbot.STATE["remaining_qty"] == 2.0
    assert ggpbot.STATE["exchange_qty"] == 2.0
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert len(exchange.create_calls) == before_calls


def test_runtime_reconcile_holds_on_position_identity_drift(harness):
    """A hedge-leg flip must hold management instead of adopting the wrong leg."""
    exchange = harness["exchange"]
    _open_trade(harness, "RUNTIME-SIDE-HOLD")
    before_calls = len(exchange.create_calls)

    exchange.position["info"]["positionSide"] = "SHORT"
    exchange.set_mark(100.0)
    result = ggpbot.sync_position_state(SYMBOL)

    assert result == (None, None, None, None)
    assert ggpbot.STATE["recovery_hold"] is True
    assert "position_side_drift" in ggpbot.STATE["recovery_reason"]
    assert "hedge_leg_mismatch" in ggpbot.STATE["recovery_reason"]
    assert ggpbot.STATE["side"] == "BUY"
    assert ggpbot.STATE["remaining_qty"] == 2.0
    assert len(exchange.create_calls) == before_calls


def test_exchange_reported_roe_mismatch_blocks_roe_targets(harness):
    """Only an independent venue ROE value may cross-check the computed basis."""
    exchange = harness["exchange"]
    _open_trade(harness, "ROE-MISMATCH-HOLD")

    harness["scenario"]["mark"] = 100.10
    _run_cycle(harness)  # classify and select a SCALP target
    closes_before = len([c for c in exchange.create_calls if c[1] == "market"])

    exchange.set_mark(100.25)
    exchange.position["roe"] = 20.0  # venue disagrees with computed 5%
    decision = _run_cycle(harness)

    assert ggpbot.STATE["roe_verified"] is False
    assert "reported_roe_mismatch" in ggpbot.STATE["roe_diagnostic"]
    assert decision is None or decision.action is ggpbot.TradeDecisionType.HOLD
    assert len([c for c in exchange.create_calls if c[1] == "market"]) == closes_before


def test_venue_fees_and_funding_are_deducted_before_targets(harness):
    """Net ROE, not gross unrealized PnL, drives ROE target decisions."""
    exchange = harness["exchange"]
    _open_trade(harness, "NET-ROE-FEES")
    exchange.position["fee"] = 0.5
    exchange.position["fundingFee"] = 0.25
    closes_before = len([c for c in exchange.create_calls if c[1] == "market"])

    harness["scenario"]["mark"] = 100.25  # 0.50 gross PnL, -0.25 net on 10 margin
    decision = _run_cycle(harness)

    assert ggpbot.STATE["roe_verified"] is True
    assert abs(ggpbot.STATE["computed_roe_pct"] + 2.5) < 1e-6
    assert abs(ggpbot.STATE["net_roe_pct"] + 2.5) < 1e-6
    assert decision is None or decision.action is ggpbot.TradeDecisionType.HOLD
    assert len([c for c in exchange.create_calls if c[1] == "market"]) == closes_before


def test_standard_lifecycle_keeps_original_price_targets_50_50(harness):
    """Insufficient evidence keeps the ORIGINAL 50/50 plan, never SCALP."""
    exchange = harness["exchange"]
    _standard_scenario(harness["scenario"])
    _open_trade(harness, "STANDARD-LIFECYCLE")

    harness["scenario"]["mark"] = 100.10
    classification = _run_cycle(harness)
    assert classification is None or classification.action is ggpbot.TradeDecisionType.HOLD
    assert ggpbot.STATE["trade_regime_state"] == STANDARD_STATE
    assert ggpbot.STATE["profit_mode"] == STANDARD_MODE
    assert ggpbot.STATE["scalp_target_roe_pct"] == 0.0
    assert ggpbot.STATE["tp1_target_roe_pct"] == 0.0
    assert ggpbot.STATE["tp2_target_roe_pct"] == 0.0
    assert ggpbot.STATE["original_tp1_price"] == TP1_PRICE
    assert ggpbot.STATE["original_tp2_price"] == TP2_PRICE

    harness["scenario"]["mark"] = TP1_PRICE
    tp1 = _run_cycle(harness)
    assert tp1.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert tp1.params["ratio"] == 0.5
    assert tp1.params["target_remaining_qty"] == 1.0
    assert tp1.params["close_reason"] == "TP1"
    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 1.0
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["tp1_hit"] is True
    assert ggpbot.STATE["trade_regime_state"] == STANDARD_STATE

    harness["scenario"]["mark"] = TP2_PRICE
    tp2 = _run_cycle(harness)
    assert tp2.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert tp2.params["close_reason"] == "TP2"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["trade_regime_state"] == CLOSED_STATE
    assert any(e[0] == "TP2_VERIFIED" and e[1] == "EXCHANGE_VERIFIED"
               for e in harness["events"])


def test_sell_standard_lifecycle_keeps_original_price_targets_50_50(harness):
    """SELL STANDARD must use the original 50/50 prices and verified closes."""
    exchange = harness["exchange"]
    _standard_scenario(harness["scenario"])
    harness["scenario"].update({"di_plus": 18.0, "di_minus": 30.0})
    _open_trade(harness, "SELL-STANDARD-LIFECYCLE", side="SELL",
                sl=105.0, tp1=95.0, tp2=90.0)

    harness["scenario"]["mark"] = 99.90
    classification = _run_cycle(harness)
    assert classification is None or classification.action is ggpbot.TradeDecisionType.HOLD
    assert ggpbot.STATE["trade_regime_state"] == STANDARD_STATE
    assert ggpbot.STATE["profit_mode"] == STANDARD_MODE
    assert ggpbot.STATE["original_tp1_price"] == 95.0
    assert ggpbot.STATE["original_tp2_price"] == 90.0

    harness["scenario"]["mark"] = 95.0
    tp1 = _run_cycle(harness)
    assert tp1.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert tp1.params["ratio"] == 0.5
    assert tp1.params["target_remaining_qty"] == 1.0
    assert tp1.params["close_reason"] == "TP1"
    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 1.0
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["tp1_hit"] is True
    assert ggpbot.STATE["trade_regime_state"] == STANDARD_STATE

    harness["scenario"]["mark"] = 90.0
    tp2 = _run_cycle(harness)
    assert tp2.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert tp2.params["close_reason"] == "TP2"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["trade_regime_state"] == CLOSED_STATE
    assert any(e[0] == "TP2_VERIFIED" and e[1] == "EXCHANGE_VERIFIED"
               for e in harness["events"])


def test_standard_regime_after_tp1_survives_restart(harness):
    exchange = harness["exchange"]
    _standard_scenario(harness["scenario"])
    _open_trade(harness, "STANDARD-RESTART")

    harness["scenario"]["mark"] = 100.10
    _run_cycle(harness)
    harness["scenario"]["mark"] = TP1_PRICE
    tp1 = _run_cycle(harness)
    assert tp1.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert ggpbot._persist_trade_journal_snapshot(force=True) is True
    calls_before = len(exchange.create_calls)

    _fresh_state()
    reopened = TradeJournal(str(harness["db_path"]))
    harness["monkeypatch"].setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", reopened)
    assert ggpbot.startup_trade_recovery() is True
    assert ggpbot.STATE["trade_regime_state"] == STANDARD_STATE
    assert ggpbot.STATE["profit_mode"] == STANDARD_MODE
    assert ggpbot.STATE["original_tp1_price"] == TP1_PRICE
    assert ggpbot.STATE["original_tp2_price"] == TP2_PRICE
    assert ggpbot.STATE["tp1_hit"] is True
    assert ggpbot.STATE["remaining_qty"] == 1.0
    assert ggpbot.STATE["exchange_qty"] == 1.0
    assert ggpbot.STATE["scalp_target_roe_pct"] == 0.0
    assert ggpbot.STATE["recovery_hold"] is False
    assert len(exchange.create_calls) == calls_before

    harness["scenario"]["mark"] = TP2_PRICE
    tp2 = _run_cycle(harness)
    assert tp2.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert tp2.params["close_reason"] == "TP2"
    assert len(exchange.create_calls) == calls_before + 1


def test_short_scalp_full_close_only_at_selected_target(harness):
    """SELL SCALP uses the same single-target/full-close policy as BUY."""
    exchange = harness["exchange"]
    _open_trade(harness, "SHORT-SCALP", side="SELL", sl=105.0, tp1=95.0, tp2=94.0)

    harness["scenario"]["mark"] = 100.10
    classification = _run_cycle(harness)
    assert classification is None or classification.action is ggpbot.TradeDecisionType.HOLD
    assert ggpbot.STATE["trade_regime_state"] == SCALP_STATE
    assert ggpbot.STATE["profit_mode"] == RANGE_MODE
    assert ggpbot.STATE["scalp_target_roe_pct"] == 5.0
    assert ggpbot.STATE["tp1_target_roe_pct"] == 0.0

    closes_before = len([c for c in exchange.create_calls if c[1] == "market"])
    harness["scenario"]["mark"] = 99.75  # 0.25% favorable = 5% on verified 20x
    decision = _run_cycle(harness)
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "SCALP_TARGET"
    assert decision.action is not ggpbot.TradeDecisionType.PARTIAL_CLOSE
    close_calls = [c for c in exchange.create_calls if c[1] == "market"]
    assert len(close_calls) == closes_before + 1
    assert close_calls[-1][2] == "buy"
    assert close_calls[-1][4].get("positionSide") == "SHORT"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["trade_regime_state"] == CLOSED_STATE


def test_short_trend_tp1_tp2_use_verified_roe_and_final_quantity(harness):
    """SELL TREND closes 50% at 40% ROE and the runner at 60% ROE."""
    exchange = harness["exchange"]
    _short_trend_scenario(harness["scenario"])
    _open_trade(harness, "SHORT-TREND", side="SELL", sl=105.0, tp1=98.0, tp2=97.0)

    harness["scenario"]["mark"] = 100.10
    classification = _run_cycle(harness)
    assert classification is None or classification.action is ggpbot.TradeDecisionType.HOLD
    assert ggpbot.STATE["trade_regime_state"] == TREND_STATE
    assert ggpbot.STATE["profit_mode"] == TREND_MODE
    assert ggpbot.STATE["tp1_price"] == 98.0
    assert ggpbot.STATE["tp2_price"] == 97.0

    harness["scenario"]["mark"] = 98.0
    tp1 = _run_cycle(harness)
    assert tp1.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert tp1.params["ratio"] == 0.5
    status, position = _status(exchange)
    assert status == "OPEN"
    assert ggpbot._position_contracts(position) == 1.0
    assert ggpbot.STATE["tp1_hit"] is True

    harness["scenario"]["mark"] = 97.0
    tp2 = _run_cycle(harness)
    assert tp2.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert tp2.params["close_reason"] == "TP2"
    status, position = _status(exchange)
    assert status == "FLAT"
    assert position is None
    assert ggpbot.STATE["remaining_qty"] == 0.0
    assert ggpbot.STATE["trade_regime_state"] == CLOSED_STATE


def test_scalp_regime_and_target_survive_restart_without_new_order(harness):
    exchange = harness["exchange"]
    _open_trade(harness, "SCALP-RESTART")

    harness["scenario"]["mark"] = 100.10
    _run_cycle(harness)
    assert ggpbot.STATE["trade_regime_state"] == SCALP_STATE
    assert ggpbot.STATE["scalp_target_roe_pct"] == 5.0
    assert ggpbot._persist_trade_journal_snapshot(force=True) is True
    calls_before = len(exchange.create_calls)

    _fresh_state()
    reopened = TradeJournal(str(harness["db_path"]))
    harness["monkeypatch"].setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", reopened)
    assert ggpbot.startup_trade_recovery() is True
    assert ggpbot.STATE["trade_regime_state"] == SCALP_STATE
    assert ggpbot.STATE["profit_mode"] == RANGE_MODE
    assert ggpbot.STATE["scalp_target_roe_pct"] == 5.0
    assert ggpbot.STATE["tp1_target_roe_pct"] == 0.0
    assert ggpbot.STATE["remaining_qty"] == 2.0
    assert ggpbot.STATE["exchange_qty"] == 2.0
    assert ggpbot.STATE["recovery_hold"] is False
    assert len(exchange.create_calls) == calls_before

    harness["scenario"]["mark"] = 100.25
    decision = _run_cycle(harness)
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "SCALP_TARGET"
    assert len(exchange.create_calls) == calls_before + 1


def test_exceptions_are_never_success_and_never_update_state_before_proof(harness, monkeypatch):
    exchange = harness["exchange"]
    _open_trade(harness, "CLOSE-EXCEPTION")
    harness["scenario"]["mark"] = 100.10
    _run_cycle(harness)

    before_calls = list(exchange.create_calls)
    before_remaining = ggpbot.STATE["remaining_qty"]
    original_create = exchange.create_order

    def _raise_on_market(symbol, kind, side, amount, price=None, params=None):
        if str(kind).upper() == "MARKET":
            raise RuntimeError("simulated transport failure after submission")
        return original_create(symbol, kind, side, amount, price, params)

    monkeypatch.setattr(exchange, "create_order", _raise_on_market)
    assert ggpbot.safe_close_position(SYMBOL, close_reason="EXCEPTION_TEST") is False
    assert exchange.create_calls == before_calls
    assert ggpbot.STATE["remaining_qty"] == before_remaining
    assert ggpbot.STATE["exchange_qty"] == before_remaining
    assert ggpbot.STATE["close_verified"] is False
    assert ggpbot.STATE["last_close_order_id"] is None
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.STATE["close_reconciliation_required"] is True
