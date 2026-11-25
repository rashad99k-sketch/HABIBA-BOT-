"""Deterministic integration coverage for the mandatory six-slot portfolio.

These tests drive the real ``execute_entry`` execution boundary with only I/O
isolated. No network, live keys, commits or skips are involved.
"""
from __future__ import annotations

import inspect
import os
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from ggp_institutional_strategy import build_institutional_report
from portfolio_allocator import (
    CLASS_NEWS,
    CLASS_TECHNICAL,
    TOTAL_CAPACITY,
    UnknownClassHintError,
    PortfolioAllocator,
)
from portfolio_runtime import PortfolioRuntime


class _FakeJournal:
    """Minimal durable journal surface used by the execution boundary."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.events = {}
        self.consumed = set()
        self.contexts = {}
        self.runtime_state = {}

    def get_runtime_state(self, key, default=None):
        return self.runtime_state.get(key, default)

    def set_runtime_state(self, key, value):
        self.runtime_state[key] = value
        return True

    def open_trades(self):
        return list(self.rows)

    def record_cross_event(self, event_id, **kwargs):
        self.events[event_id] = {
            "event_id": event_id,
            "status": "ACTIVE",
            "side": kwargs.get("side"),
            "expires_ts": kwargs.get("expires_ts", time.time() + 60),
            "metadata": kwargs.get("metadata") or {},
        }
        return True

    def get_cross_event(self, event_id):
        return self.events.get(event_id)

    def consume_cross_event(self, event_id, _trade_id):
        self.consumed.add(event_id)
        return True

    def is_event_consumed(self, event_id):
        return event_id in self.consumed

    def consume_event(self, event_id, _trade_id):
        self.consumed.add(event_id)
        return True

    def persist_sizing_decision(self, _trade_id, _decision):
        return True

    def persist_entry_context(self, trade_id, context):
        self.contexts[trade_id] = dict(context)
        return True

    def get_active_rf_reentry_block(self, _symbol, _side):
        return None

    def release_rf_reentry_blocks(self, **_kwargs):
        return 0

    def finalize_once(self, *_args, **_kwargs):
        return True

    def get(self, _trade_id):
        return None


def _fresh_allocator(tmp_path, name="portfolio.sqlite3"):
    return PortfolioAllocator(str(tmp_path / name))


def _fresh_runtime(monkeypatch):
    manager = SimpleNamespace(
        start_trade=lambda *a, **k: None,
        manage_live_trade=lambda: None,
        lifecycle_state=None,
    )
    monkeypatch.setattr(ggpbot, "_live_manager", manager, raising=False)
    runtime = PortfolioRuntime(
        ggpbot.STATE, ggpbot.TRADE_STATE, ggpbot.DASHBOARD_STATE,
        manager, lock=ggpbot._TRADE_LOCK,
    )
    monkeypatch.setattr(ggpbot, "PORTFOLIO_RUNTIME", runtime, raising=False)
    return runtime


def _patch_execution(monkeypatch, tmp_path, *, allocator=None):
    allocator = allocator or _fresh_allocator(tmp_path)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True, raising=False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", False, raising=False)
    monkeypatch.setattr(ggpbot, "PORTFOLIO_ALLOCATOR", allocator, raising=False)
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STATE", {
        "until": 0.0, "triggered_at": 0.0, "trigger_balance": None,
        "utc_day": None, "peak_balance": None, "trigger_count": 0,
    }, raising=False)
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STORE_READY", True, raising=False)
    runtime = _fresh_runtime(monkeypatch)

    ggpbot.STATE.clear()
    ggpbot.STATE.update({
        "open": False, "side": None, "entry": 0.0, "qty": 0.0, "remaining_qty": 0.0,
        "execution_unresolved": False, "close_reconciliation_required": False,
        "recovery_hold": False, "close_verified": False, "current_symbol": None,
        "range_target_price": 0.0, "trade_id": None,
    })
    ggpbot.TRADE_STATE.clear()
    ggpbot.TRADE_STATE.update({"in_position": False, "symbol": None})
    ggpbot.MEMORY["position_status"] = "FLAT"
    ggpbot.MEMORY["recovery_status"] = "NOT_STARTED"
    ggpbot.RECOVERY_BLOCK_ENTRIES = False
    ggpbot._ACTIVE_TRADE = False
    ggpbot.paper["balance"] = 1000.0
    ggpbot.paper["position"] = None

    journal = _FakeJournal()
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal, raising=False)
    monkeypatch.setattr(ggpbot, "tg_entry", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(ggpbot, "tg_close", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(ggpbot, "update_position_dashboard", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(ggpbot, "clear_position_dashboard", lambda: None, raising=False)
    monkeypatch.setattr(ggpbot, "update_stats", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(ggpbot, "log_execution", lambda *a, **k: None, raising=False)
    return allocator, runtime, journal


def _frame(index=0):
    n = 120
    ts = 1_700_000_000_000 + index * 86_400_000
    stamps = [ts + i * 900_000 for i in range(n)]
    base = np.linspace(99.0, 100.0, n)
    return pd.DataFrame({
        "timestamp": stamps,
        "open": base * 0.999,
        "high": base * 1.004,
        "low": base * 0.996,
        "close": base,
        "volume": np.full(n, 1000.0),
    })


def _execute(monkeypatch, symbol, index, *, strategy="RORO", caller_label="NEWS_TRADING"):
    frame = _frame(index)
    ts = int(frame["timestamp"].iloc[-1])
    row = {
        "symbol": symbol,
        "exchange_symbol": symbol,
        "side": "BUY",
        "price": 100.0,
        "atr": 1.0,
        "timeframe_ms": 900_000,
        "last_updated": time.time(),
        "last_update": time.time(),
        "data_freshness": "FRESH",
        "data_quality": "VALID_CLOSED_CANDLES",
        "closed_candle_source": "TEST_CLOSED_FRAME",
        "cross_context": {
            "cross_state": "CONFIRMED_BUY_CROSS", "durable_event": True,
            "authoritative": True,
        },
        "market_regime": "STRONG_TREND",
        "golden_zone": {"state": "CONFIRMED", "score": 82.0},
        "market_structure": {"direction": "BULLISH", "mss": "BULLISH_CHOCH", "confirmed": True},
        "liquidity": {"event": "LIQUIDITY_SWEEP", "direction": "BULLISH", "confidence": 72.0},
        "volume_ratio": 1.4,
        "volume_confirmed": True,
        "adx": 28.0,
        "adx_di": {"di_plus": 30.0, "di_minus": 17.0, "adx_slope": 1.2},
        "continuation_probability": 0.74,
        "invalidation_level": 98.0,
        "structural_stop": 99.0,
        "support_resistance": {"status": "VALID", "supports": [], "resistances": []},
    }
    context = {
        "setup_type": "VALIDATION",
        "event_id": f"event-{index}",
        "evidence": ["test"],
        "structural_stop": 99.5,
        "entry_behavior": "PULLBACK",
        "entry_behavior_confidence": 0.7,
        "entry_behavior_reasons": ["test_closed_candle_reason"],
        "market_regime": row["market_regime"],
        "liquidity": row["liquidity"],
        "cross_event_id": "",
        "candle_timeframe": "15m",
        "closed_candle_frame": frame,
        "closed_candle_source": "TEST_CLOSED_FRAME",
        "closed_candle_timestamp": ts,
        "strategy": strategy,
        "source": strategy,
        # Hostile caller labels: never an authority for slot classification.
        "classification": caller_label,
        "slot_class": caller_label,
        "class_label": caller_label,
        "label": caller_label,
        "institutional_report": build_institutional_report(row, now_ts=time.time()),
        "data_freshness": "FRESH",
        "data_quality": "VALID_CLOSED_CANDLES",
        "last_updated": time.time(),
        "timeframe_ms": 900_000,
    }
    monkeypatch.setattr(ggpbot, "get_ohlcv_safe", lambda *a, **k: frame, raising=False)
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda *a, **k: 100.0, raising=False)
    monkeypatch.setattr(
        ggpbot, "_validate_dual_ema_vwap_entry_gate",
        lambda *a, **k: {
            "ready": True, "reason": "test_valid_dual_cross",
            "ema9_cross": "BULLISH", "ema50_cross": "BULLISH",
            "ema9_cross_timestamp": ts - 900_000, "ema50_cross_timestamp": ts - 900_000,
            "ema9": 100.1, "ema50": 100.0, "vwap": 99.9,
            "latest_closed_timestamp": ts, "signal_candle_id": f"sig-{index}",
        },
        raising=False,
    )
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", context, raising=False)
    return ggpbot.execute_entry(
        "BUY", symbol, 100.0, 99.5, 100.25, 101.0, 80,
        "six_slot_integration_test", 0.5, "TREND", "TEST", caller_label,
    )


def test_five_technical_plus_one_news_through_real_execute_entry(monkeypatch, tmp_path):
    allocator, runtime, _ = _patch_execution(monkeypatch, tmp_path)
    for index in range(5):
        assert _execute(monkeypatch, f"TECH{index}/USDT:USDT", index) is True
    assert _execute(
        monkeypatch, "NEWS1/USDT:USDT", 5, strategy="NEWS_TRADING", caller_label="RORO"
    ) is True
    counts = allocator.counts()
    assert counts[CLASS_TECHNICAL] == 5
    assert counts[CLASS_NEWS] == 1
    assert counts["TOTAL"] == TOTAL_CAPACITY
    assert len(runtime.status()["active"]) == TOTAL_CAPACITY
    assert ggpbot.STATE["current_symbol"] == "NEWS1/USDT:USDT"


def test_sixth_technical_and_seventh_total_rejected_before_order(monkeypatch, tmp_path):
    allocator, runtime, _ = _patch_execution(monkeypatch, tmp_path)
    for index in range(5):
        assert _execute(monkeypatch, f"T{index}/USDT:USDT", index) is True
    assert _execute(monkeypatch, "T5/USDT:USDT", 5) is False
    assert allocator.reservation("T5/USDT:USDT") is None
    assert len(runtime.status()["active"]) == 5

    assert _execute(
        monkeypatch, "N1/USDT:USDT", 6, strategy="NEWS_TRADING"
    ) is True
    assert _execute(monkeypatch, "T6/USDT:USDT", 7) is False
    assert allocator.counts()["TOTAL"] == TOTAL_CAPACITY
    assert len(runtime.status()["active"]) == TOTAL_CAPACITY
    assert allocator.reservation("T6/USDT:USDT") is None


def test_central_classification_ignores_caller_labels_and_conflicting_hints(monkeypatch, tmp_path):
    allocator, _, _ = _patch_execution(monkeypatch, tmp_path)
    hostiles = {
        "classification": "NEWS_TRADING", "slot_class": "NEWS_TRADING",
        "class_label": "NEWS", "label": "NEWS", "slot": "NEWS", "class_hint": "NEWS",
        "strategy": "RORO",
    }
    assert ggpbot._portfolio_trusted_hint(hostiles) == {"strategy": "RORO"}
    with pytest.raises(UnknownClassHintError):
        ggpbot._portfolio_trusted_hint({"strategy": "RORO", "source": "NEWS_TRADING"})

    # Even though the caller insists this is NEWS, the trusted source is RORO.
    assert _execute(
        monkeypatch, "CALLERNEWS/USDT:USDT", 20, strategy="RORO", caller_label="NEWS_TRADING"
    ) is True
    assert allocator.counts()[CLASS_TECHNICAL] == 1
    assert allocator.counts()[CLASS_NEWS] == 0


def test_news_cap_and_verified_flat_slot_reuse(monkeypatch, tmp_path):
    allocator, runtime, _ = _patch_execution(monkeypatch, tmp_path)
    assert _execute(
        monkeypatch, "NEWSX/USDT:USDT", 30, strategy="NEWS_TRADING"
    ) is True
    assert _execute(
        monkeypatch, "NEWSY/USDT:USDT", 31, strategy="NEWS_TRADING"
    ) is False
    assert allocator.counts()[CLASS_NEWS] == 1

    active = allocator.active_reservations()
    assert len(active) == 1
    reservation = active[0]
    assert ggpbot._portfolio_release_reservation(
        reservation.trade_id, reservation.symbol, "TEST_VERIFIED_FLAT"
    ) is True
    with runtime.focus(reservation.trade_id):
        assert runtime.release(
            reservation.trade_id,
            verified_flat=ggpbot._portfolio_flat_evidence(reservation.symbol, "TEST_VERIFIED_FLAT"),
        ).ok
    ggpbot._recompute_portfolio_globals()
    assert allocator.counts()[CLASS_NEWS] == 0

    assert _execute(
        monkeypatch, "NEWSZ/USDT:USDT", 32, strategy="NEWS_TRADING"
    ) is True
    assert allocator.counts()[CLASS_NEWS] == 1


def test_finalizer_releases_exactly_once_and_preserves_other_positions(monkeypatch, tmp_path):
    allocator, runtime, _ = _patch_execution(monkeypatch, tmp_path)
    assert _execute(monkeypatch, "KEEP/USDT:USDT", 40) is True
    assert _execute(monkeypatch, "CLOSE/USDT:USDT", 41) is True
    assert allocator.counts()["TOTAL"] == 2
    close_id = ggpbot.STATE.get("trade_id")

    close_fields = {
        "open": True, "close_verified": True, "remaining_qty": 0.0,
        "exchange_qty": 0.0, "last_close_fill_price": 101.0,
        "last_close_realized_pnl": 1.0,
        "last_close_pnl_source": "PAPER_MARK_ESTIMATE",
        "last_close_pnl_complete": True, "initial_margin": 5.0,
        "execution_unresolved": False, "close_reconciliation_required": False,
        "recovery_hold": False,
    }
    # The wrapper focuses the trade, so the close evidence must live in that
    # trade's isolated snapshot rather than only in the currently focused globals.
    runtime._positions[close_id].state.update(close_fields)
    ggpbot.STATE.update(close_fields)
    result = ggpbot.finalize_trade_with_reality(
        "CLOSE/USDT:USDT", "TEST_VERIFIED_FLAT", trade_id=close_id
    )
    assert result[0] == pytest.approx(1.0)
    assert allocator.counts()["TOTAL"] == 1
    released = allocator.reservation(close_id)
    assert released is not None and released.status == "RELEASED"
    assert close_id not in {item.trade_id for item in allocator.active_reservations()}
    assert close_id not in runtime._positions
    remaining = allocator.active_reservations()
    assert len(remaining) == 1
    assert remaining[0].symbol == "KEEP/USDT:USDT"
    assert any(item["symbol"] == "KEEP/USDT:USDT" for item in runtime.status()["active"])

    # A second finalization is an idempotent no-op, never a second slot release.
    ggpbot.finalize_trade_with_reality(
        "CLOSE/USDT:USDT", "TEST_VERIFIED_FLAT", trade_id=close_id
    )
    assert allocator.counts()["TOTAL"] == 1
    assert len(runtime.status()["active"]) == 1


def test_malformed_flat_evidence_cannot_free_a_slot(monkeypatch, tmp_path):
    allocator, _, _ = _patch_execution(monkeypatch, tmp_path)
    result = allocator.reserve("BTC/USDT:USDT", "malformed-evidence", "TECHNICAL")
    assert result.ok
    malformed = {"verified_flat": True, "qty": 1.0, "source": "test"}
    released = allocator.release("malformed-evidence", require_flat=True, verified_flat=malformed)
    assert not released.ok
    assert allocator.counts()["TOTAL"] == 1
    assert allocator.reservation("malformed-evidence") is not None


def test_runtime_focus_isolates_management_and_contains_failures(monkeypatch, tmp_path):
    _, runtime, _ = _patch_execution(monkeypatch, tmp_path)
    for index in range(2):
        trade_id = f"focus-{index}"
        symbol = f"FOCUS{index}/USDT:USDT"
        reservation = ggpbot.PORTFOLIO_ALLOCATOR.reserve(symbol, trade_id, "TECHNICAL")
        assert reservation.ok
        ggpbot.STATE.update({
            "open": True, "trade_id": trade_id, "current_symbol": symbol,
            "side": "BUY", "entry": 100.0 + index, "qty": 1.0 + index,
            "remaining_qty": 1.0 + index, "execution_unresolved": index == 1,
        })
        ggpbot.TRADE_STATE.update({"in_position": True, "symbol": symbol})
        assert runtime.adopt(trade_id, symbol, class_hint="TECHNICAL").ok

    seen = []
    def manager():
        trade_id = ggpbot.STATE.get("trade_id")
        seen.append((trade_id, ggpbot.STATE.get("entry")))
        if trade_id == "focus-1":
            raise RuntimeError("bounded management failure")

    summary = runtime.manage_all(manager)
    assert summary.processed == ("focus-0", "focus-1")
    assert len(summary.failed) == 1
    assert summary.failed[0][0] == "focus-1"
    assert seen[0] == ("focus-0", 100.0)
    assert seen[1] == ("focus-1", 101.0)
    # The failed cycle cannot leak into the other position's restored snapshot.
    with runtime.focus("focus-0"):
        assert ggpbot.STATE.get("trade_id") == "focus-0"
        assert ggpbot.STATE.get("execution_unresolved") is False
    assert len(runtime.status()["active"]) == 2


def test_unresolved_order_retains_reservation(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from bingx_execution import ExecutionState

    allocator, _, _ = _patch_execution(monkeypatch, tmp_path)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False, raising=False)

    class Exchange:
        def market(self, _symbol):
            return {"contractSize": 1.0}
        def amount_to_precision(self, _symbol, amount):
            return str(amount)
        def fetch_balance(self):
            return {"free": {"USDT": 1000.0}}
        def fetch_ticker(self, _symbol):
            return {"last": 100.0}

    class PartialFillService:
        def __init__(self, *_args, **_kwargs):
            pass
        def submit_market_entry(self, **_kwargs):
            return SimpleNamespace(
                state=ExecutionState.PARTIAL_FILL,
                trade_id="unresolved-trade",
                client_order_id="unresolved-client",
                exchange_order_id="unresolved-order",
                requested_qty=1.0, filled_qty=0.4, remaining_qty=0.6,
                average_fill_price=100.0, position_mode="ONE_WAY",
                position_side="BOTH", raw_position={"contracts": 0.4},
                reason="partially filled",
            )
        def place_and_verify_native_stop(self, **_kwargs):
            return "unresolved-native-stop"

    monkeypatch.setattr(ggpbot, "ex", Exchange(), raising=False)
    monkeypatch.setattr(ggpbot, "TradeExecutionService", PartialFillService, raising=False)
    monkeypatch.setattr(ggpbot, "safe_api_call", lambda func, *a, **k: func(*a, **k), raising=False)
    monkeypatch.setattr(ggpbot, "normalize_symbol", lambda value: value, raising=False)
    monkeypatch.setattr(ggpbot, "dynamic_spread_tolerance", lambda _symbol: 10.0, raising=False)
    monkeypatch.setattr(ggpbot, "get_spread_bps", lambda _symbol: 1.0, raising=False)
    ggpbot.STATE.update({
        "open": False, "execution_unresolved": False, "recovery_hold": False,
        "close_reconciliation_required": False,
    })
    ggpbot.TRADE_STATE.update({"in_position": False})
    ggpbot.MEMORY["position_status"] = "FLAT"
    ggpbot.RECOVERY_BLOCK_ENTRIES = False

    assert ggpbot.open_position(
        "BUY", 1.0, "BTC/USDT:USDT", stop_loss=90.0, trade_id="unresolved-trade"
    ) is None
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.STATE["recovery_hold"] is True
    assert allocator.reservation("unresolved-trade") is not None
    assert allocator.counts()["TOTAL"] == 1


def test_startup_recovery_restores_six_positions(monkeypatch, tmp_path):
    allocator, runtime, journal = _patch_execution(monkeypatch, tmp_path)
    rows = []
    for index in range(TOTAL_CAPACITY):
        strategy = "NEWS_TRADING" if index == 5 else "RORO"
        trade_id = f"recover-{index}"
        symbol = f"R{index}/USDT:USDT"
        rows.append({
            "trade_id": trade_id,
            "symbol": symbol,
            "side": "BUY",
            "entry_price": 100.0 + index,
            "entry_qty": 1.0,
            "remaining_qty": 1.0,
            "state": {
                "trade_id": trade_id,
                "current_symbol": symbol,
                "side": "BUY",
                "entry": 100.0 + index,
                "initial_qty": 1.0,
                "remaining_qty": 1.0,
                "synthetic_sl": 99.0 + index,
                "sl": 99.0 + index,
                "tp1_price": 100.25 + index,
                "tp2_price": 101.0 + index,
                "strategy": strategy,
                "source": strategy,
            },
        })
    journal.rows = rows
    ggpbot.STATE.clear()
    ggpbot.STATE.update({"recovery_hold": False, "execution_unresolved": False})
    ggpbot.TRADE_STATE.clear()
    ggpbot.TRADE_STATE.update({"in_position": False})

    assert ggpbot.startup_trade_recovery() is True
    counts = allocator.counts()
    assert counts[CLASS_TECHNICAL] == 5
    assert counts[CLASS_NEWS] == 1
    assert counts["TOTAL"] == TOTAL_CAPACITY
    assert len(runtime.status()["active"]) == TOTAL_CAPACITY
    assert ggpbot.MEMORY["position_status"] == "OPEN"
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is False


def test_required_source_assertions():
    execute_source = inspect.getsource(ggpbot._original_execute_entry_for_journal)
    reserve_at = execute_source.index("PORTFOLIO_ALLOCATOR.reserve")
    assert reserve_at < execute_source.index('paper["position"] =')
    assert reserve_at < execute_source.index("open_position(")
    assert "require_flat=True" in inspect.getsource(ggpbot)
    assert "manage_all(" in inspect.getsource(ggpbot._main_loop_manage_open_position)
    assert "_portfolio_trusted_hint" in execute_source
    assert "classification" not in inspect.getsource(ggpbot._portfolio_trusted_hint).split("\"\"\"")[1].split("\"\"\"")[0].replace(
        "Raw caller labels (``classification``", ""
    )
