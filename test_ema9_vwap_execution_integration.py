"""Integration coverage for EMA9/VWAP authorization -> original execution -> Brain handoff.

All exchange-facing calls are mocked. These tests only prove routing, fail-closed
guards, and that the existing safety/sizing path remains the sole order sender.
"""
import os
import time
from dataclasses import replace

import pandas as pd
import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("ENABLE_LIVE_TRADING", "false")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from ggp_management_events import TradeManagementEventLog
from test_unified_ema_vwap_strategy import TF_MS, crossover_frame
from unified_ema_vwap_strategy import Setup, UnifiedEMAVWAPPullbackEngine


PRICE = 99.85
SL = 99.20
TP1 = 101.0
TP2 = 102.0
ATR = 2.0
ZONE_LOW = 99.5
ZONE_HIGH = 100.5


def symbols():
    exchange = "BTC/USDT:USDT"
    return ggpbot.roro_canonical_symbol(exchange), exchange


def make_setup(canonical, side="BUY", status="ENTRY_READY", entry_status="READY"):
    label = "DEMAND_ZONE" if side == "BUY" else "SUPPLY_ZONE"
    ob = "BULLISH_ORDER_BLOCK" if side == "BUY" else "BEARISH_ORDER_BLOCK"
    return Setup(
        symbol=canonical, status=status, entry_status=entry_status, direction=side,
        crossover_ts=1_800_062_100_000, crossover_index=69, crossover_price=PRICE,
        ema9=100.025, ema15=100.03, ema50=100.0, vwap=99.4083, atr14=ATR,
        causal_zone_low=ZONE_LOW, causal_zone_high=ZONE_HIGH,
        causal_zone_type="FVG", zone_label=label, zone_quality="TESTED_ZONE",
        order_block_type=ob, last_candle_ts=1_800_062_100_000,
        trend_evidence={"vwap_slope_atr": 0.12, "ema9_slope_atr": 0.10},
    )


class FakeEntryEngine:
    """Minimal stateful engine double; no persistence and no exchange calls."""

    def __init__(self, setup):
        self.setup = setup
        self.calls = []

    def get(self, symbol):
        return self.setup if symbol == self.setup.symbol else None

    def authorize(self, symbol, price, tp1, *, side=None, now_ms=None):
        self.calls.append(("authorize", symbol, side, price, tp1))
        return {
            "allowed": True, "reason": "AUTHORIZED", "reason_code": "ENTRY_AUTHORIZED",
            "status": "ENTRY_READY", "symbol": self.setup.symbol,
            "side": self.setup.direction, "entry_price": price, "tp1": tp1,
            "zone_type": self.setup.causal_zone_type,
            "zone_label": self.setup.zone_label,
            "zone_quality": self.setup.zone_quality,
        }

    def mark_executing(self, symbol):
        self.calls.append(("mark_executing", symbol))
        if self.setup.status != "ENTRY_READY":
            return False
        self.setup.status = "ENTRY_EXECUTING"
        self.setup.entry_status = "ENTRY_EXECUTING"
        return True

    def mark_open(self, symbol):
        self.calls.append(("mark_open", symbol))
        if self.setup.status != "ENTRY_EXECUTING":
            return False
        self.setup.status = "TRADE_OPEN"
        self.setup.entry_status = "TRADE_OPEN"
        return True

    def mark_rejected_execution(self, symbol, code, reason):
        self.calls.append(("mark_rejected_execution", symbol, code))
        if self.setup.handoff_id or self.setup.status not in {"ENTRY_EXECUTING", "TRADE_OPEN"}:
            return False
        self.setup.status = "REJECTED"
        self.setup.entry_status = "BLOCKED"
        self.setup.last_error_code = code
        return True

    def mark_handoff(self, symbol, handoff_id):
        self.calls.append(("mark_handoff", symbol, handoff_id))
        if (self.setup.status != "TRADE_OPEN" or self.setup.handoff_id
                or not handoff_id):
            return False
        self.setup.handoff_id = str(handoff_id)
        self.setup.entry_status = "HANDED_OFF"
        return True


def authorized_context(canonical, exchange, setup):
    return {
        "setup_type": "EMA_VWAP_CAUSAL_ZONE",
        "strategy_id": ggpbot.ENTRY_STRATEGY_ID,
        "ema_vwap_authorized": True,
        "canonical_symbol": canonical,
        "exchange_symbol": exchange,
        "side": setup.direction,
        "entry_price": PRICE,
        "tp1": TP1,
        "causal_zone_type": setup.causal_zone_type,
        "zone_label": setup.zone_label,
        "ob_type": setup.order_block_type,
        "zone_quality": setup.zone_quality,
        "causal_zone_low": ZONE_LOW,
        "causal_zone_high": ZONE_HIGH,
        "causal_zone_boundaries": {"low": ZONE_LOW, "high": ZONE_HIGH},
        "atr": ATR,
        "ema9": setup.ema9,
        "vwap": setup.vwap,
        "regime": "UNKNOWN",
        "entry_trend_evidence": dict(setup.trend_evidence or {}),
    }


def confirmed_state(monkeypatch, *, side="BUY", fill=True):
    canonical, _ = symbols()
    state = {
        "open": True, "current_symbol": canonical, "side": side,
        "trade_id": "trade-123", "entry": PRICE,
        "initial_qty": 1.0 if fill else 0.0, "qty": 1.0 if fill else 0.0,
        "native_sl_original_price": SL, "synthetic_sl": SL, "sl": SL,
        "tp1_price": TP1, "tp2_price": TP2,
        "entry_exchange_order_id": "exchange-order-1",
        "entry_execution_state": "FILLED", "native_sl_verified": True,
        "native_sl_status": "PAPER_SIMULATED", "recovery_hold": False,
        "execution_unresolved": False, "market_regime": "UNKNOWN",
    }
    monkeypatch.setattr(ggpbot, "STATE", state)
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": True})
    monkeypatch.setattr(ggpbot, "MEMORY", {"watchlist": {}, "position_status": "OPEN"})
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    return state


def capture_diagnostics(monkeypatch):
    rows = []
    monkeypatch.setattr(
        ggpbot, "_record_entry_diagnostic",
        lambda tag, **fields: rows.append((tag, dict(fields))),
    )
    return rows


# --------------------------------------------------------------------------- #
# Shared execute_entry boundary: original execution is reached only with a token.
# --------------------------------------------------------------------------- #
def test_final_execute_entry_calls_original_once_with_matching_authorization(monkeypatch):
    canonical, exchange = symbols()
    setup = make_setup(canonical, status="ENTRY_EXECUTING", entry_status="ENTRY_EXECUTING")
    engine = FakeEntryEngine(setup)
    ctx = authorized_context(canonical, exchange, setup)
    original_calls = []

    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", ctx)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_start", lambda *a, **k: None)
    diagnostics = capture_diagnostics(monkeypatch)

    def fake_original(*args, **kwargs):
        original_calls.append((args, kwargs))
        return True

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", fake_original)
    result = ggpbot.execute_entry(
        "BUY", exchange, PRICE, SL, TP1, TP2, 80, "authorized", ATR,
        "TREND", "RORO_INSTITUTIONAL", "TREND",
    )

    assert result is True
    assert len(original_calls) == 1
    assert original_calls[0][0] == (
        "BUY", exchange, PRICE, SL, TP1, TP2, 80, "authorized", ATR,
        "TREND", "RORO_INSTITUTIONAL", "TREND",
    )
    assert all(tag != "[ENTRY_REJECTED]" for tag, _ in diagnostics)


def test_final_execute_entry_blocks_original_without_matching_token(monkeypatch):
    canonical, exchange = symbols()
    setup = make_setup(canonical, status="ENTRY_EXECUTING", entry_status="ENTRY_EXECUTING")
    engine = FakeEntryEngine(setup)
    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", None)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False})
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    diagnostics = capture_diagnostics(monkeypatch)

    def must_not_run(*args, **kwargs):
        raise AssertionError("original execute_entry must not be called without authorization")

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", must_not_run)
    assert ggpbot.execute_entry(
        "BUY", exchange, PRICE, SL, TP1, TP2, 80, "legacy", ATR,
        "TREND", "TEST", "TREND",
    ) is False
    assert any(tag == "[ENTRY_REJECTED]" and fields.get("reason_code") == "EXECUTION_REJECTED"
               for tag, fields in diagnostics)


def test_open_position_lower_guard_blocks_second_order_without_token(monkeypatch):
    called = []

    class Exchange:
        def market(self, symbol):
            called.append(symbol)
            return {"contractSize": 1.0}

    monkeypatch.setattr(ggpbot, "ex", Exchange())
    monkeypatch.setattr(ggpbot, "_ACTIVE_TRADE", False)
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    assert ggpbot.open_position("BUY", 1.0, "BTC/USDT:USDT", stop_loss=SL) is None
    assert called == []


# --------------------------------------------------------------------------- #
# Full orchestration: authorization -> original path -> confirmed handoff.
# --------------------------------------------------------------------------- #
def _patch_smart_inputs(monkeypatch, *, original_result, set_confirmed_state):
    canonical, exchange = symbols()
    setup = make_setup(canonical)
    engine = FakeEntryEngine(setup)
    item = {
        "exchange_symbol": exchange, "score": 90.0, "decision_state": "ENTRY_READY",
        "strategy_status": "ENTRY_READY",
        "ema_vwap_strategy": {"status": "ENTRY_READY", "direction": "BUY"},
        "evidence": ["ema9_vwap_causal_zone"],
    }
    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", None)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {canonical: item}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "TRADE_MANAGEMENT_LOG", TradeManagementEventLog())
    monkeypatch.setattr(ggpbot, "refresh_unified_institutional_watchlist", lambda *a, **k: None)
    monkeypatch.setattr(
        ggpbot, "_integration_fetch_frame",
        lambda symbol: pd.DataFrame({"close": [PRICE] * 100}),
    )
    monkeypatch.setattr(
        ggpbot, "roro_validate_closed_frame",
        lambda frame, **kwargs: frame,
    )
    monkeypatch.setattr(ggpbot, "validate_dataframe", lambda *a, **k: True)
    monkeypatch.setattr(
        ggpbot, "roro_analyze_symbol_both",
        lambda *a, **k: {"decision_state": "WAITING_FOR_CONFIRMATION",
                         "evidence": ["context_only"]},
    )
    monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *a, **k: {})
    monkeypatch.setattr(ggpbot, "compute_atr", lambda df: pd.Series([ATR] * len(df)))
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda symbol: PRICE)
    monkeypatch.setattr(
        ggpbot, "build_liquidity_pools",
        lambda df: {"high_pools": [], "low_pools": []},
    )
    monkeypatch.setattr(ggpbot, "compute_sl_tp", lambda *a, **k: (SL, TP1, TP2))
    monkeypatch.setattr(ggpbot, "is_strong_candidate", lambda item: False)
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_start", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    diagnostics = capture_diagnostics(monkeypatch)

    original_calls = []
    captured_context = {}

    def fake_original(*args, **kwargs):
        original_calls.append((args, kwargs))
        captured_context["ctx"] = dict(ggpbot._INSTITUTIONAL_ENTRY_CONTEXT or {})
        if set_confirmed_state:
            state = ggpbot.STATE
            state.update({
                "open": True, "current_symbol": canonical, "side": "BUY",
                "trade_id": "trade-123", "entry": PRICE,
                "initial_qty": 1.0, "qty": 1.0, "native_sl_original_price": SL,
                "native_sl_status": "PAPER_SIMULATED", "entry_exchange_order_id": "exchange-1",
                "execution_unresolved": False, "recovery_hold": False,
                "tp1_price": TP1, "tp2_price": TP2, "market_regime": "UNKNOWN",
            })
            ggpbot.MEMORY["position_status"] = "OPEN"
        return original_result

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", fake_original)
    return engine, item, original_calls, captured_context, diagnostics


def test_smart_selection_uses_original_execution_and_hands_off_confirmed_position_once(monkeypatch):
    engine, item, original_calls, captured_context, diagnostics = _patch_smart_inputs(
        monkeypatch, original_result=True, set_confirmed_state=True,
    )

    assert ggpbot.smart_opportunity_selection() is True
    assert len(original_calls) == 1
    args = original_calls[0][0]
    assert args[0] == "BUY"
    assert args[2] == pytest.approx(PRICE)
    assert args[3] == pytest.approx(ZONE_LOW - 0.15 * ATR)  # causal-zone SL preserved
    assert args[4] == pytest.approx(TP1)
    assert args[5] == pytest.approx(TP2)
    assert ("mark_executing", "BTC") in engine.calls
    assert engine.setup.status == "TRADE_OPEN"
    assert engine.setup.handoff_id
    assert item["trade_handoff"] == "CONFIRMED"
    assert item["entry_status"] == "EXECUTED_VIA_GGP_EXECUTE_ENTRY"

    tags = [tag for tag, _ in diagnostics]
    assert tags.count("[TRADE_HANDOFF]") == 1
    assert tags.count("[TRADE_CLASSIFIED]") == 1

    events = ggpbot.TRADE_MANAGEMENT_LOG.snapshot(limit=50)
    handoff = next(event for event in events if event["event_type"] == "TRADE_HANDOFF")
    for field in (
        "symbol", "exchange_position_id", "side", "actual_entry", "filled_qty",
        "initial_sl", "initial_tp1", "initial_tp2", "atr", "ema9", "vwap",
        "zone_type", "ob_type", "zone_boundaries", "timestamp", "strategy_id",
        "regime", "execution_confirmation_status",
    ):
        assert field in handoff
    assert handoff["symbol"] == "BTC"
    assert handoff["execution_confirmation_status"] == "PAPER_POSITION_CONFIRMED"
    assert handoff["strategy_id"] == ggpbot.ENTRY_STRATEGY_ID
    assert handoff["management_authority"] == "TradeManagementBoard / LiveTradeManager"
    assert any(event["event_type"] == "TRADE_CLASSIFIED"
               and event["authoritative"] is False for event in events)

    # A duplicate dispatch must not create a second handoff or change the setup.
    ctx = captured_context["ctx"]
    assert ggpbot._dispatch_confirmed_trade_handoff(
        "BTC", "BUY", SL, TP1, TP2, ctx) is False
    assert sum(1 for event in ggpbot.TRADE_MANAGEMENT_LOG.snapshot(limit=50)
               if event["event_type"] == "TRADE_HANDOFF") == 1


def test_exchange_rejection_blocks_handoff_and_marks_execution_rejected(monkeypatch):
    engine, item, original_calls, _, diagnostics = _patch_smart_inputs(
        monkeypatch, original_result=False, set_confirmed_state=False,
    )

    assert ggpbot.smart_opportunity_selection() is False
    assert len(original_calls) == 1
    assert engine.setup.status == "REJECTED"
    assert engine.setup.last_error_code == "EXECUTION_REJECTED"
    assert engine.setup.handoff_id is None
    assert item["decision_state"] == "REJECTED"
    assert "trade_handoff" not in item
    assert any(tag == "[ENTRY_REJECTED]" and fields.get("reason_code") == "EXECUTION_REJECTED"
               for tag, fields in diagnostics)
    assert all(tag != "[TRADE_HANDOFF]" for tag, _ in diagnostics)
    assert all(event["event_type"] != "TRADE_HANDOFF"
               for event in ggpbot.TRADE_MANAGEMENT_LOG.snapshot(limit=50))


def test_missing_fill_or_position_is_position_not_confirmed(monkeypatch):
    canonical, exchange = symbols()
    setup = make_setup(canonical, status="TRADE_OPEN", entry_status="TRADE_OPEN")
    engine = FakeEntryEngine(setup)
    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)
    confirmed_state(monkeypatch, fill=False)
    monkeypatch.setattr(ggpbot, "TRADE_MANAGEMENT_LOG", TradeManagementEventLog())
    diagnostics = capture_diagnostics(monkeypatch)

    assert ggpbot._dispatch_confirmed_trade_handoff(
        canonical, "BUY", SL, TP1, TP2,
        authorized_context(canonical, exchange, setup),
    ) is False
    assert any(tag == "[ENTRY_REJECTED]"
               and fields.get("reason_code") == "POSITION_NOT_CONFIRMED"
               for tag, fields in diagnostics)
    assert engine.setup.handoff_id is None
    assert all(event["event_type"] != "TRADE_HANDOFF"
               for event in ggpbot.TRADE_MANAGEMENT_LOG.snapshot(limit=50))


def test_confirmed_exchange_position_payload_is_recorded_once(monkeypatch):
    canonical, exchange = symbols()
    setup = make_setup(canonical, status="TRADE_OPEN", entry_status="TRADE_OPEN")
    engine = FakeEntryEngine(setup)
    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)
    confirmed_state(monkeypatch)
    monkeypatch.setattr(ggpbot, "TRADE_MANAGEMENT_LOG", TradeManagementEventLog())
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    diagnostics = capture_diagnostics(monkeypatch)

    assert ggpbot._dispatch_confirmed_trade_handoff(
        canonical, "BUY", SL, TP1, TP2,
        authorized_context(canonical, exchange, setup),
    ) is True
    event = next(event for event in ggpbot.TRADE_MANAGEMENT_LOG.snapshot(limit=50)
                 if event["event_type"] == "TRADE_HANDOFF")
    assert event["exchange_position_id"] == "exchange-order-1"
    assert event["actual_entry"] == pytest.approx(PRICE)
    assert event["filled_qty"] == pytest.approx(1.0)
    assert event["initial_sl"] == pytest.approx(SL)
    assert event["initial_tp1"] == pytest.approx(TP1)
    assert event["initial_tp2"] == pytest.approx(TP2)
    assert event["zone_boundaries"] == {"low": ZONE_LOW, "high": ZONE_HIGH}
    assert event["brain_regime_label"] in ggpbot.classify_management_regime({"side": "BUY"})["label"]
    assert [tag for tag, _ in diagnostics].count("[TRADE_HANDOFF]") == 1


# --------------------------------------------------------------------------- #
# Unchanged scanner/cache behaviour and legacy safety boundaries.
# --------------------------------------------------------------------------- #
def test_refresh_uses_cached_scanner_frame_without_new_ohlcv_request(tmp_path, monkeypatch):
    canonical, exchange = symbols()
    engine_path = str(tmp_path / "strategy.json")
    engine = UnifiedEMAVWAPPullbackEngine(engine_path)
    monkeypatch.setattr(ggpbot, "EMA_VWAP_ENTRY_ENGINE", engine)

    frame = crossover_frame("BUY")
    target_last = int(time.time() * 1000) - TF_MS - 1000
    shift = target_last - int(frame.timestamp.iloc[-1])
    frame["timestamp"] = frame["timestamp"].astype("int64") + shift

    peek_calls = []
    fetch_calls = []

    def peek(symbol):
        peek_calls.append(symbol)
        return frame.copy()

    def fetch(symbol):
        fetch_calls.append(symbol)
        return frame.copy()

    monkeypatch.setattr(ggpbot, "_peek_scanner_frame", peek)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", fetch)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None)
    watchlist = {canonical: {"exchange_symbol": exchange}}

    ggpbot._refresh_ema_vwap_setups(watchlist)

    assert len(peek_calls) == 1
    assert fetch_calls == []
    assert "ema_vwap_strategy" in watchlist[canonical]
    assert watchlist[canonical]["strategy_id"] == ggpbot.ENTRY_STRATEGY_ID
