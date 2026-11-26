"""End-to-end proof for durable cross discovery -> Watchlist -> entry context.

All market data is deterministic and synthetic. No exchange credentials, live
orders, network calls, or real account state are used. The strict gate, journal,
watchlist registry, context builder, profit classifier and TradeManagementBoard
are the real implementations; only synthetic data/exchange boundaries are
controlled by the tests.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

import numpy as np
import pandas as pd
import pytest

import ggpbot
import ggp_institutional_bridge as bridge
import naiso_unified_decision as proj
from ggp_profit_brain import (
    RANGE_MODE, STANDARD_MODE, TREND_MODE, classify_profit_mode,
)
from ggpbot import TradeDecisionType
from roro_confluence import (
    AUTH_EMA9_EMA15_TRIGGER_VALID, evaluate_post_cross_rejection_breakout,
    validate_dual_ema_vwap_entry_gate,
)
from roro_trade_journal import TradeJournal


SYMBOL = "BTC/USDT:USDT"
CANONICAL = "BTC"
TF_MS = 900_000
START_MS = 1_795_132_800_000  # 2026-11-20 00:00 UTC
CROSS_IDX = 79
BASE_BARS = 80
NOW_MS = START_MS + BASE_BARS * TF_MS + 1_000
NOW_SEC = NOW_MS / 1000.0
LIFECYCLE_NOW_MS = START_MS + (BASE_BARS + 3) * TF_MS + 1_000


def _ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def _cross_frame(side="BUY"):
    """A real closed-candle EMA9/EMA15 cross with Session VWAP confirmation."""
    n = BASE_BARS
    closes = 99.0 + (-0.01 if side == "BUY" else 0.01) * np.arange(n)
    closes[-1] = closes[-2] + (4.0 if side == "BUY" else -4.0)
    opens = np.concatenate([closes[:-1], [closes[-2]]])
    return pd.DataFrame({
        "timestamp": START_MS + np.arange(n) * TF_MS,
        "open": opens,
        "high": np.maximum(opens, closes) + 0.2,
        "low": np.minimum(opens, closes) - 0.2,
        "close": closes,
        "volume": np.full(n, 1000.0),
    })


def _lifecycle_frame(side="BUY"):
    """Cross frame plus a closed pullback, rejection wick and later breakout."""
    frame = _cross_frame(side)
    if side == "BUY":
        extra = [
            (101.5, 100.8, 101.7, 98.60),
            (100.8, 101.1, 101.3, 98.70),
            (101.1, 102.0, 102.2, 100.9),
        ]
    else:
        extra = [
            (96.5, 97.2, 100.3, 96.3),
            (98.0, 97.6, 100.25, 97.4),
            (97.6, 96.9, 97.8, 96.7),
        ]
    rows = []
    for offset, (open_price, close_price, high, low) in enumerate(extra):
        rows.append({
            "timestamp": START_MS + (BASE_BARS + offset) * TF_MS,
            "open": open_price, "high": high, "low": low,
            "close": close_price, "volume": 1000.0,
        })
    return pd.concat([frame, pd.DataFrame(rows)], ignore_index=True)


def _gate(frame, side="BUY", now_ms=None):
    return validate_dual_ema_vwap_entry_gate(
        frame, side, _ema,
        timeframe_seconds=900, now_ms=NOW_MS if now_ms is None else now_ms,
        lookback_candles=2,
    )


def _lifecycle_event(side="BUY", *, phase="WAITING_FOR_PULLBACK"):
    frame = _cross_frame(side)
    gate = _gate(frame, side)
    assert gate["ready"] is True
    event_id = ggpbot._confirmed_cross_event_id(SYMBOL, str(ggpbot.INTERVAL), gate)
    direction = "BULLISH" if side == "BUY" else "BEARISH"
    return {
        "event_id": event_id,
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": side,
        "direction": direction,
        "source": ggpbot.CONFIRMED_CROSS_EVENT_SOURCE,
        "timeframe": str(ggpbot.INTERVAL),
        "signal_candle_timestamp": int(gate["signal_candle_timestamp"]),
        "closed_candle_timestamp": int(gate["latest_closed_timestamp"]),
        "expires_ts": time.time() + 14_400.0,
        "metadata": {
            **gate,
            "lifecycle_schema": ggpbot.CROSS_LIFECYCLE_SCHEMA,
            "phase": phase,
            "entry_ready": False,
            "cross_price": float(frame["close"].iloc[-1]),
            "max_chase_atr": ggpbot.CROSS_REJECTION_MAX_CHASE_ATR,
        },
    }


def _payload(event_id="BTC:15m:BUY:1795203900000", expires_ts=None):
    return {
        "event_id": event_id,
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": "BUY",
        "direction": "BULLISH",
        "source": ggpbot.CONFIRMED_CROSS_EVENT_SOURCE,
        "timeframe": str(ggpbot.INTERVAL),
        "signal_candle_timestamp": START_MS + 79 * TF_MS,
        "closed_candle_timestamp": START_MS + 79 * TF_MS,
        "expires_ts": expires_ts if expires_ts is not None else time.time() + 14_400.0,
        "metadata": {
            "ready": True,
            "authorization_state": AUTH_EMA9_EMA15_TRIGGER_VALID,
            "detection_method": "validate_dual_ema_vwap_entry_gate",
            "lookback_candles": 2,
        },
    }


def _record(journal, *, event_id=None, expires_ts=None, side="BUY"):
    payload = _payload(event_id=event_id or f"{CANONICAL}:{ggpbot.INTERVAL}:BUY:{int(time.time() * 1000)}",
                       expires_ts=expires_ts)
    payload["side"] = side
    payload["direction"] = "BULLISH" if side == "BUY" else "BEARISH"
    assert journal.record_cross_event(**payload) is True
    return payload


def _watchlist_cross_item(event):
    return {
        "symbol": event["symbol"],
        "exchange_symbol": event["exchange_symbol"],
        "side": event["side"],
        "cross_event_id": event["event_id"],
        "cross_event_source": event["source"],
        "cross_event_direction": event["direction"],
        "cross_event_timestamp": event["signal_candle_timestamp"],
        "cross_event_status": event["status"],
        "cross_event_timeframe": event["timeframe"],
        "discovery_source": "confirmed_cross_event",
        "sources": ["confirmed_cross_event"],
        "role": "CONFIRMED_CROSS_EVENT",
        "score": 100.0,
    }


# --------------------------------------------------------------------------- #
# 1) Cross-event persistence is unique, restart-safe, and single-consumption.
# --------------------------------------------------------------------------- #
def test_cross_event_journal_lifecycle_survives_restart(tmp_path):
    path = str(tmp_path / "cross-events.sqlite3")
    journal = TradeJournal(path)
    payload = _record(journal)

    assert journal.record_cross_event(**payload) is False, "event identity must be unique"
    restarted = TradeJournal(path)
    row = restarted.get_cross_event(payload["event_id"])
    assert row is not None
    assert row["symbol"] == CANONICAL
    assert row["exchange_symbol"] == SYMBOL
    assert row["side"] == "BUY"
    assert row["direction"] == "BULLISH"
    assert row["source"] == ggpbot.CONFIRMED_CROSS_EVENT_SOURCE
    assert row["timeframe"] == str(ggpbot.INTERVAL)
    assert row["signal_candle_timestamp"] == payload["signal_candle_timestamp"]
    assert row["metadata"]["detection_method"] == "validate_ema9_vwap_entry_gate"
    assert row["metadata"]["lookback_candles"] == 2
    assert [item["event_id"] for item in restarted.list_cross_events(("ACTIVE",))] == [payload["event_id"]]

    assert restarted.consume_cross_event(payload["event_id"], "trade-1") is True
    assert restarted.consume_cross_event(payload["event_id"], "trade-2") is False
    consumed = TradeJournal(path).get_cross_event(payload["event_id"])
    assert consumed["status"] == "ENTERED"
    assert consumed["consumed_trade_id"] == "trade-1"

    invalid = _record(journal, event_id=f"{CANONICAL}:{ggpbot.INTERVAL}:BUY:invalidate")
    assert journal.invalidate_cross_event(invalid["event_id"], "opposite_confirmed_cross") is True
    assert journal.get_cross_event(invalid["event_id"])["status"] == "INVALIDATED"
    assert "opposite_confirmed_cross" in journal.get_cross_event(invalid["event_id"])["metadata"]["invalidation_reason"]

    expired = _record(journal, event_id=f"{CANONICAL}:{ggpbot.INTERVAL}:BUY:expired",
                      expires_ts=time.time() - 10.0)
    assert journal.expire_cross_events() >= 1
    expired_row = journal.get_cross_event(expired["event_id"])
    assert expired_row["status"] == "EXPIRED"
    assert [item["event_id"] for item in journal.list_cross_events(("ACTIVE",))] == []
    assert expired["event_id"] in [
        item["event_id"] for item in journal.list_cross_events(("ACTIVE", "EXPIRED"), include_expired=True)
    ]

    context = {
        "schema_version": 1,
        "cross_event_id": payload["event_id"],
        "cross_event_direction": "BULLISH",
        "entry_zone": {"low": 98.0, "high": 99.0, "quality": 70},
        "adx_di": {"adx": 26.0, "di_plus": 30.0, "di_minus": 12.0},
    }
    assert journal.persist_entry_context("trade-1", context) is True
    assert journal.persist_entry_context("trade-1", context) is False
    with sqlite3.connect(path) as db:
        rows = db.execute(
            "SELECT event_key, event_type, payload FROM trade_events WHERE trade_id=?", ("trade-1",)
        ).fetchall()
    entry_context_events = [row for row in rows if row[1] == "ENTRY_CONTEXT"]
    assert len(entry_context_events) == 1
    assert entry_context_events[0][0] == "entry-context:trade-1"
    assert json.loads(entry_context_events[0][2]) == context


# --------------------------------------------------------------------------- #
# 1b) Post-cross lifecycle: pullback -> rejection candle -> later breakout.
# --------------------------------------------------------------------------- #
def _evaluate(event, frame, *, now_ms=LIFECYCLE_NOW_MS, max_chase_atr=1.5):
    return evaluate_post_cross_rejection_breakout(
        frame, event["side"], event, ema_fn=_ema, timeframe_seconds=900,
        now_ms=now_ms, max_chase_atr=max_chase_atr,
    )


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_post_cross_rejection_breakout_buy_and_sell(side):
    event = _lifecycle_event(side)
    result = _evaluate(event, _lifecycle_frame(side))
    assert result["reason"] == "closed_rejection_high_low_breakout_confirmed"
    assert result["phase"] == "BREAKOUT_CONFIRMED"
    assert result["entry_ready"] is True
    assert result["authorization_state"] == "POST_CROSS_REJECTION_BREAKOUT_VALID"
    assert result["pullback_candle_timestamp"] > event["signal_candle_timestamp"]
    assert result["rejection_candle_timestamp"] > result["pullback_candle_timestamp"]
    assert result["breakout_candle_timestamp"] > result["rejection_candle_timestamp"]
    assert result["rejection_high"] > result["rejection_low"]
    assert result["chase_atr"] <= ggpbot.CROSS_REJECTION_MAX_CHASE_ATR
    if side == "BUY":
        assert result["breakout_close"] > result["rejection_high"]
        assert result["ema9"] > result["vwap"]
    else:
        assert result["breakout_close"] < result["rejection_low"]
        assert result["ema9"] < result["vwap"]


def test_cross_lifecycle_is_durable_and_never_rewinds(tmp_path):
    journal = TradeJournal(str(tmp_path / "lifecycle.sqlite3"))
    event = _lifecycle_event("BUY")
    event["expires_ts"] = LIFECYCLE_NOW_MS / 1000.0 + 3600.0
    assert journal.record_cross_event(**event) is True
    row = journal.get_cross_event(event["event_id"])

    first = _evaluate(row, _lifecycle_frame("BUY"))
    assert first["phase"] in {"REJECTION_RECORDED", "BREAKOUT_CONFIRMED"}
    assert journal.update_cross_event_lifecycle(event["event_id"], first) is True
    # Repeated identical evaluations must not generate redundant journal writes.
    assert journal.update_cross_event_lifecycle(event["event_id"], first) is False

    confirmed = journal.get_cross_event(event["event_id"])
    assert confirmed["metadata"]["phase"] == "BREAKOUT_CONFIRMED"
    assert confirmed["metadata"]["entry_ready"] is True

    # A stale lower phase and a later frame that lost the breakout cannot revoke it.
    assert journal.update_cross_event_lifecycle(event["event_id"], {
        "phase": "WAITING_FOR_PULLBACK", "entry_ready": False,
    }) is False
    retained = journal.get_cross_event(event["event_id"])
    assert retained["metadata"]["phase"] == "BREAKOUT_CONFIRMED"
    assert retained["metadata"]["entry_ready"] is True
    assert _evaluate(retained, _lifecycle_frame("BUY"))["entry_ready"] is True


def test_selection_rejects_cross_event_before_breakout_confirmed(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "selection.sqlite3"))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    event = _lifecycle_event("BUY")
    assert journal.record_cross_event(**event) is True

    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(ggpbot, "refresh_unified_institutional_watchlist", lambda: None)
    monkeypatch.setattr(ggpbot.time, "time", lambda: LIFECYCLE_NOW_MS / 1000.0)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda *a, **k: _lifecycle_frame("BUY"))
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda *a, **k: 102.0)
    monkeypatch.setattr(ggpbot, "roro_analyze_symbol_both", lambda *a, **k: {
        "decision_state": "ENTRY_READY", "side": "BUY",
    })
    monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *a, **k: {})
    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.MEMORY, "position_status", "FLAT")
    monkeypatch.setitem(ggpbot.MEMORY, "_unified_watchlist_refresh_ok", True)
    if hasattr(ggpbot, "RECOVERY_BLOCK_ENTRIES"):
        monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    item = {
        "decision_state": "ENTRY_READY", "side": "BUY", "symbol": CANONICAL,
        "exchange_symbol": SYMBOL, "event_id": "institution:pending",
        "cross_event_id": event["event_id"], "score": 100.0,
    }
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {CANONICAL: item})
    executed = []
    monkeypatch.setattr(ggpbot, "execute_entry", lambda *a, **k: executed.append((a, k)))

    assert ggpbot.smart_opportunity_selection() is False
    assert executed == []
    assert item["decision_state"] == "REJECTED"
    assert item["rejection_reason"] == "confirmed_cross_event_not_breakout_confirmed"


def test_post_cross_entry_requires_closed_rejection_and_later_breakout():
    event = _lifecycle_event("SELL")
    frame = _cross_frame("SELL")

    # A cross plus a direct breakout without a closed pullback/rejection is not
    # an entry; ranking-only directional context cannot substitute.
    direct = frame.copy()
    last = direct.iloc[-1].copy()
    direct = pd.concat([direct, pd.DataFrame([last])], ignore_index=True)
    direct.iloc[-1, direct.columns.get_loc("close")] = 90.0
    direct.iloc[-1, direct.columns.get_loc("low")] = 89.8
    direct.iloc[-1, direct.columns.get_loc("timestamp")] = START_MS + BASE_BARS * TF_MS
    assert _evaluate(event, direct)["entry_ready"] is False

    # A breakout candle that has not closed cannot authorize an entry.
    unfinished = _lifecycle_frame("SELL")
    unfinished.iloc[-1, unfinished.columns.get_loc("timestamp")] = START_MS + (BASE_BARS + 3) * TF_MS
    blocked = _evaluate(event, unfinished)
    assert blocked["entry_ready"] is False
    assert blocked["phase"] != "BREAKOUT_CONFIRMED"

    # Chasing beyond the ATR cap is rejected even with a structurally valid breakout.
    chased = _evaluate(event, _lifecycle_frame("SELL"), max_chase_atr=0.25)
    assert chased["entry_ready"] is False
    assert chased["reason"] == "breakout_exceeds_atr_chase_limit"


# --------------------------------------------------------------------------- #
# 2) Scanner rows are ranking-only; only the strict gate creates cross events.
# --------------------------------------------------------------------------- #
def test_supplementary_scanner_rows_cannot_create_cross_events(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "scanner.sqlite3"))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: None)
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])

    memory = {
        "rf_watchlist": [{"symbol": SYMBOL, "side": "BUY", "score": 99, "ema9_vwap_proximity": {
            "state": "NEAR_CROSS", "cross_any": "BULLISH", "cross_age_seconds": 10,
            "timeframe_seconds": 900, "updated_at": NOW_SEC, "cross_fresh": True}}],
        "scanner_v2_buy": [{"symbol": "ETHUSDT", "side": "BUY", "score": 80}],
        "professional_scan_candidates": [{"symbol": "SOLUSDT", "side_hint": "SELL", "score": 95}],
        "radar_top5": [{"symbol": "XRPUSDT", "score": 70}],
        "watchlist": {},
    }
    merged = bridge.merge_candidates(memory)
    assert set(merged) >= {"BTC", "ETH", "SOL", "XRP"}
    assert all(not item.get("cross_event_id") for item in merged.values())
    assert all(item.get("discovery_source") != "confirmed_cross_event" for item in merged.values())

    assert ggpbot._detect_confirmed_cross_events([SYMBOL]) == 0
    assert journal.list_cross_events(("ACTIVE",)) == []
    assert ggpbot.MEMORY["confirmed_cross_events"] == []

    # Real gate + real detector now create one durable event.
    frame = _cross_frame("BUY")
    gate = _gate(frame, "BUY")
    assert gate["ready"] is True
    assert gate["trigger"] == "EMA9_SESSION_VWAP"
    assert gate["ema9_vwap_cross"] == "BULLISH"
    assert gate["ema9_vwap_cross_timestamp"] == gate["signal_candle_timestamp"]
    assert gate["authorization_state"] == AUTH_EMA9_VWAP_TRIGGER_VALID
    monkeypatch.setattr(ggpbot.time, "time", lambda: NOW_SEC)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: frame)

    assert ggpbot._detect_confirmed_cross_events([SYMBOL]) == 1
    rows = journal.list_cross_events(("ACTIVE",))
    assert len(rows) == 1
    row = rows[0]
    assert row["side"] == "BUY"
    assert row["direction"] == "BULLISH"
    assert row["source"] == ggpbot.CONFIRMED_CROSS_EVENT_SOURCE
    assert row["event_id"] == f"{SYMBOL}:{ggpbot.INTERVAL}:{gate['signal_candle_id']}"
    assert row["metadata"]["detection_method"] == "validate_dual_ema_vwap_entry_gate"
    assert row["metadata"]["lookback_candles"] == 2

    feed = ggpbot.MEMORY["confirmed_cross_events"]
    assert len(feed) == 1
    assert feed[0]["cross_event_id"] == row["event_id"]
    assert feed[0]["cross_event_status"] == "ACTIVE"
    assert feed[0]["role"] == "CONFIRMED_CROSS_EVENT"
    assert ggpbot._detect_confirmed_cross_events([SYMBOL]) == 0
    assert len(journal.list_cross_events(("ACTIVE",))) == 1


# --------------------------------------------------------------------------- #
# 2b) A discovered cross reaches the existing Active Watchlist in the same cycle.
# --------------------------------------------------------------------------- #
def _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls):
    monkeypatch.setattr(ggpbot.time, "time", lambda: NOW_SEC)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_scanner_recovery_blocked", lambda: "")
    monkeypatch.setattr(ggpbot, "get_usdt_perp_symbols",
                        lambda: calls.append("universe") or [SYMBOL])
    monkeypatch.setattr(ggpbot, "run_scanner_v2", lambda **kwargs: calls.append("v2"))
    monkeypatch.setattr(ggpbot, "_professional_lightweight_scan",
                        lambda **kwargs: ([], {"total": 0, "valid_data": 0}))
    monkeypatch.setattr(ggpbot, "scan_market_rf", lambda **kwargs: calls.append("rf") or [])
    monkeypatch.setattr(ggpbot, "radar_score", lambda _frame: 0)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: frame.copy())
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame",
                        lambda symbol: fetch_calls.append(symbol) or frame.copy())

    def refresh_spy(**kwargs):
        # Execute the real owned-worker body synchronously so scanner-cycle
        # delivery is proven end to end, while keeping the test off real clocks
        # and exchange APIs. Nothing is published until the real worker runs.
        refresh_calls.append(kwargs)
        state = {
            "lock": threading.Lock(), "in_progress": True, "generation": 1,
            "timed_out": False, "restart_requested": False,
            "last_attempt": NOW_SEC, "last_attempt_monotonic": 0.0,
            "started_monotonic": 0.0, "last_progress_monotonic": 0.0,
            "last_completed_monotonic": 0.0, "phase": "IDLE",
            "current_symbol": "", "timed_out": False, "restart_requested": False,
            "last_error": "", "worker_name": "unified-institutional-watchlist",
        }
        monkeypatch.setattr(ggpbot, "_UNIFIED_WATCHLIST_REFRESH_STATE", state)
        monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_BATCH_SIZE", 4)
        monkeypatch.setattr(ggpbot, "INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS", 30)
        monkeypatch.setattr(ggpbot, "INSTITUTIONAL_RESOURCE_MANAGER", None)
        monkeypatch.setitem(
            ggpbot.MEMORY, "_unified_watchlist_refresh_ok", False)

        def publish_cross_rows(memory_target, _fetch_frame, **_kwargs):
            for event in memory_target.get("confirmed_cross_events", []):
                key = ggpbot.roro_canonical_symbol(
                    event.get("exchange_symbol") or event.get("symbol"))
                if not key:
                    continue
                memory_target.setdefault("watchlist", {})[key] = {
                    "symbol": event.get("exchange_symbol") or event.get("symbol"),
                    "side": event.get("side"),
                    "score": 100.0,
                    "decision_state": "WAITING_FOR_CONFIRMATION",
                    "cross_event_id": event.get("cross_event_id"),
                    "cross_event_status": event.get("cross_event_status", "ACTIVE"),
                    "cross_event_analysis_required": True,
                    "cross_event_lifecycle_status": event.get(
                        "cross_event_lifecycle_status", ggpbot.CROSS_WATCHLIST_LIFECYCLE),
                    "last_update": NOW_SEC,
                    "source": "confirmed_cross_event",
                }
            return memory_target.setdefault("watchlist", {})

        monkeypatch.setattr(ggpbot, "update_unified_watchlist", publish_cross_rows)
        monkeypatch.setattr(ggpbot, "_enrich_with_existing_gpp_engines", lambda *_a, **_k: {})
        ggpbot._run_unified_institutional_watchlist_refresh()
        return ggpbot.MEMORY.setdefault("watchlist", {})
    monkeypatch.setattr(ggpbot, "refresh_unified_institutional_watchlist", refresh_spy)

    for key in ("confirmed_cross_events", "scanner_v2_buy", "scanner_v2_sell",
                "rf_watchlist", "radar_watchlist", "top_candidates"):
        monkeypatch.setitem(ggpbot.MEMORY, key, [])
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {})
    monkeypatch.setitem(ggpbot.MEMORY, "_last_unified_watchlist_refresh", NOW_SEC)
    monkeypatch.setitem(ggpbot.STATE, "open", None)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)


def test_confirmed_cross_is_delivered_to_active_watchlist_in_same_scanner_cycle(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "same-cycle.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)

    ggpbot.run_global_discovery_scanner()

    events = journal.list_cross_events(("ACTIVE",))
    assert len(events) == 1
    event_id = events[0]["event_id"]
    feed = ggpbot.MEMORY["confirmed_cross_events"]
    watch = ggpbot.MEMORY["watchlist"]

    assert len(feed) == 1 and feed[0]["cross_event_id"] == event_id
    assert watch[CANONICAL]["cross_event_id"] == event_id
    assert watch[CANONICAL]["cross_event_status"] == "ACTIVE"
    assert watch[CANONICAL]["cross_event_analysis_required"] is True
    assert refresh_calls == [{"force": True}]
    assert calls.count("universe") == 1
    assert calls.count("v2") == 1
    assert calls.count("rf") == 1
    assert len(fetch_calls) <= ggpbot.INSTITUTIONAL_WATCHLIST_BATCH_SIZE

    delivery = ggpbot.MEMORY["_cross_watchlist_delivery"]
    assert delivery["new_events"] == 1
    assert delivery["same_cycle"] is True
    assert delivery["delivered"] is True
    assert delivery["reason"] == "ACTIVE_WATCHLIST_DELIVERED"
    assert delivery["detected_at"] == pytest.approx(NOW_SEC)
    assert delivery["delivered_at"] >= delivery["detected_at"]
    assert ggpbot.MEMORY["discovery_scanner"]["cross_watchlist_delivery"] == delivery


def test_duplicate_scanner_cycle_keeps_event_without_second_delivery(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "duplicate-cycle.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)

    ggpbot.run_global_discovery_scanner()
    first_event = journal.list_cross_events(("ACTIVE",))[0]["event_id"]
    refresh_calls.clear()

    ggpbot.run_global_discovery_scanner()

    assert [row["event_id"] for row in journal.list_cross_events(("ACTIVE",))] == [first_event]
    assert ggpbot.MEMORY["watchlist"][CANONICAL]["cross_event_id"] == first_event
    assert refresh_calls == []
    assert ggpbot.MEMORY["_cross_watchlist_delivery"]["new_events"] == 0
    assert ggpbot.MEMORY["_cross_watchlist_delivery"]["reason"] == "NO_NEW_CROSS_EVENT"


def test_new_cross_respects_open_position_discovery_pause(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "cross-position-pause.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)

    real_detect = ggpbot._detect_confirmed_cross_events
    def detect_then_open(symbols):
        created = real_detect(symbols)
        monkeypatch.setitem(ggpbot.STATE, "open", True)
        return created
    monkeypatch.setattr(ggpbot, "_detect_confirmed_cross_events", detect_then_open)

    ggpbot.run_global_discovery_scanner()

    assert len(journal.list_cross_events(("ACTIVE",))) == 1
    assert ggpbot.MEMORY["confirmed_cross_events"]
    assert refresh_calls == []
    delivery = ggpbot.MEMORY["_cross_watchlist_delivery"]
    assert delivery["new_events"] == 1
    assert delivery["delivered"] is False
    assert delivery["same_cycle"] is False
    assert delivery["reason"] == "POSITION_OPEN_DISCOVERY_PAUSED"


# --------------------------------------------------------------------------- #
# 3) Active cross events are forced into bounded Watchlist analysis every cycle.
# --------------------------------------------------------------------------- #
def test_active_cross_event_is_forced_into_each_watchlist_batch(monkeypatch):
    seen = []

    def fake_analyze(symbol, side, frame, now_ms=None):
        seen.append((bridge.canonical_symbol(symbol), side))
        return {
            "symbol": bridge.canonical_symbol(symbol),
            "exchange_symbol": symbol,
            "side": side,
            "decision_state": "WAITING_FOR_CONFIRMATION",
            "entry_status": "WAITING_FOR_CONFIRMATION",
            "decision_reason": "synthetic_analysis",
            "rejection_reasons": [],
            "score": 10.0,
            "event_id": None,
            "structural_stop": None,
            "data_quality": "VALID_CLOSED_CANDLES",
            "golden_zone": {"score": 10},
            "evidence": [],
            "last_update": time.time(),
            "icons": {"status": "🟡"},
        }

    monkeypatch.setattr(bridge, "analyze_symbol", fake_analyze)
    frame = _cross_frame("BUY")
    event = _payload(expires_ts=time.time() + 3600.0)
    memory = {
        "confirmed_cross_events": [_watchlist_cross_item({**event, "status": "ACTIVE"})],
        "scanner_v2_buy": [{"symbol": "ETHUSDT", "side": "BUY", "score": 5}],
        "watchlist": {},
    }

    first = bridge.update_unified_watchlist(memory, lambda _: frame, max_items=10, batch_size=1)
    second = bridge.update_unified_watchlist(memory, lambda _: frame, max_items=10, batch_size=1)
    assert seen.count(("BTC", "BUY")) == 2
    # A durable cross event is analyzed only in its immutable confirmed side.
    # Scanner hints cannot create a second hypothesis for the same event.
    assert seen == [("BTC", "BUY"), ("BTC", "BUY")]
    assert first["BTC"]["cross_event_id"] == event["event_id"]
    assert second["BTC"]["cross_event_analysis_required"] is True
    assert second["BTC"]["cross_event_direction"] == "BULLISH"
    assert second["BTC"]["cross_event_status"] == "ACTIVE"

    # Active cross rows are protected from the 1800s Watchlist TTL even if no
    # fresh analysis is produced for them.
    second["BTC"]["last_update"] = time.time() - 3600.0
    third = bridge.update_unified_watchlist(memory, lambda _: frame, max_items=10, batch_size=1)
    assert "BTC" in third

    # Once the event is no longer active, it stops being forced and stale
    # Watchlist metadata can no longer protect it from the normal TTL.
    third["BTC"]["decision_state"] = "REJECTED"
    third["BTC"]["last_update"] = time.time() - 3600.0
    memory["confirmed_cross_events"] = []
    fourth = bridge.update_unified_watchlist(memory, lambda _: frame, max_items=10, batch_size=10)
    assert "BTC" not in fourth


# --------------------------------------------------------------------------- #
# 4) Persisted context is complete and feeds the one classifier + board path.
# --------------------------------------------------------------------------- #
def _full_entry_context(gate, event, *, trend=True):
    frame = _cross_frame("BUY")
    source = {
        "cross_event_id": event["event_id"],
        "cross_event_source": ggpbot.CONFIRMED_CROSS_EVENT_SOURCE,
        "cross_event_direction": "BULLISH",
        "cross_event_timestamp": event["signal_candle_timestamp"],
        "event_id": event["event_id"],
        "evidence": ["closed_ema9_vwap_cross", "fresh_institutional_zone"],
        "structural_stop": 96.0,
        "entry_zone_low": 98.50,
        "entry_zone_high": 99.20,
        "zone_quality": 72.0,
        "zone_type": "DEMAND",
        "order_block": {
            "type": "ORDER_BLOCK", "status": "FRESH", "valid": True,
            "low": 98.50, "high": 99.20, "strength": 8.0, "distance_atr": 0.30,
        },
        "liquidity": {"event": "SWEEP", "direction": "BULLISH", "level": 98.20, "confidence": 80},
        "market_structure": {"confirmed": True, "mss": "BULLISH_CHOCH"},
        "volume_ratio": 1.20,
        "adx": 26.0,
        "di_plus": 30.0,
        "di_minus": 12.0,
        "market_regime": "STRONG_TREND" if trend else "RANGE_CHOP",
        "trading_mode": "TREND" if trend else "SCALP",
        "entry_behavior": "ENTER_NOW" if trend else "WAIT_FOR_STRUCTURE",
        "closed_candle_frame": frame,
        "candle_timeframe": str(ggpbot.INTERVAL),
        "closed_candle_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
    }
    return ggpbot._build_persistent_entry_context(source, gate, "BUY", SYMBOL, "trade-ctx", 0.69)


def test_persisted_entry_context_drives_classifier_and_single_board(tmp_path):
    frame = _cross_frame("BUY")
    gate = _gate(frame, "BUY")
    journal = TradeJournal(str(tmp_path / "ctx.sqlite3"))
    event = _payload(expires_ts=time.time() + 3600.0)
    assert journal.record_cross_event(**event) is True

    context = _full_entry_context(gate, event)
    required = {
        "trade_id", "symbol", "exchange_symbol", "side", "cross_event_id",
        "cross_event_source", "cross_event_direction", "cross_event_timestamp",
        "entry_zone", "order_block", "supply_demand_zone", "liquidity",
        "market_structure", "relative_volume", "adx_di", "atr", "ema9", "ema50",
        "vwap", "entry_regime", "closed_candle_timestamp", "frame_source",
    }
    assert required <= set(context)
    assert context["cross_event_id"] == event["event_id"]
    assert context["cross_event_direction"] == "BULLISH"
    assert context["cross_event_source"] == ggpbot.CONFIRMED_CROSS_EVENT_SOURCE
    assert context["order_block"]["freshness"] == "FRESH"
    assert context["order_block"]["valid"] is True
    assert context["adx_di"] == {"adx": 26.0, "di_plus": 30.0, "di_minus": 12.0}
    assert context["ema9"] == pytest.approx(gate["ema9"])
    assert context["vwap"] == pytest.approx(gate["vwap"])
    assert journal.persist_entry_context("trade-ctx", context) is True

    current = {
        "side": "BUY", "regime": "STRONG_TREND", "trade_state": "TREND_RIDE",
        "adx": 28.0, "adx_slope": 1.2, "continuation_probability": 0.82,
        "di_plus": 32.0, "di_minus": 10.0, "volume_ratio": 1.15,
        "entry_context": context,
    }
    mode, reason, evidence = classify_profit_mode(current)
    assert mode == TREND_MODE
    assert "entry_confirmed_directional_cross" in evidence
    assert "entry_context_trend_support" not in reason  # first confirmed-trend path is authoritative

    weak = {
        "side": "BUY", "regime": "CHOPPY", "trade_state": "RANGE_CHOP",
        "adx": 15.0, "adx_slope": -1.0, "continuation_probability": 0.38,
        "di_plus": 18.0, "di_minus": 17.0, "volume_ratio": 0.62,
        "momentum_strength": 20.0, "entry_context": context,
    }
    assert classify_profit_mode(weak)[0] == STANDARD_MODE

    board = ggpbot.TradeManagementBoard()
    base = {
        "symbol": "BTC", "side": "BUY", "entry": 99.0, "current_price": 105.0,
        "atr": 0.70, "initial_qty": 1.0, "remaining_qty": 0.5,
        "profit_mode": mode, "entry_context": context,
        "indicator_decisions_allowed": False, "synthetic_sl": 0.0,
        "trail_stop": 0.0, "trail_active": False, "tp1_hit": True,
        "tp2_hit": False, "tp1_price": 100.0, "tp2_price": 110.0,
        "be_done": True, "max_price": 105.5, "adx": 28.0,
        "continuation_probability": 0.82,
    }
    trail = board.evaluate(base)
    assert trail.action == TradeDecisionType.TRAIL
    assert trail.params["new_stop"] > base["entry"]
    assert board._decision_history[-1]["entry_cross_event_id"] == event["event_id"]

    be = board.evaluate({**base, "be_done": False})
    assert be.action == TradeDecisionType.BREAKEVEN
    assert be.params["new_sl"] > base["entry"]

    tp1 = board.evaluate({**base, "tp1_hit": False, "remaining_qty": 1.0,
                          "current_price": 100.5, "tp1_price": 100.0})
    assert tp1.action == TradeDecisionType.PARTIAL_CLOSE
    assert tp1.params["ratio"] == pytest.approx(0.5)

    scalp = board.evaluate({**base, "profit_mode": RANGE_MODE, "remaining_qty": 1.0,
                            "tp1_hit": False, "range_target_price": 99.5,
                            "current_price": 99.6})
    assert scalp.action == TradeDecisionType.FULL_CLOSE


# --------------------------------------------------------------------------- #
# 5) Full durable path: confirmed cross -> Watchlist -> entry -> management.
# --------------------------------------------------------------------------- #
def test_confirmed_cross_to_watchlist_to_entry_to_adaptive_management(tmp_path, monkeypatch):
    frame = _cross_frame("BUY")
    gate = _gate(frame, "BUY")
    assert gate["ready"] is True

    clock = [NOW_SEC]
    frames = {"current": frame}
    monkeypatch.setattr(ggpbot.time, "time", lambda: clock[0])
    journal = TradeJournal(str(tmp_path / "e2e.sqlite3"))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: frames["current"])
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", False)
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {})
    monkeypatch.setitem(ggpbot.MEMORY, "position_status", "FLAT")
    monkeypatch.setitem(ggpbot.MEMORY, "_unified_watchlist_refresh_ok", True)
    for key in ("open", "in_position", "execution_unresolved", "recovery_hold",
                "entry_context_persistence_failed", "cross_event_persistence_failed"):
        monkeypatch.setitem(ggpbot.STATE, key, False)
    monkeypatch.setitem(ggpbot.STATE, "trade_id", "")
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.paper, "balance", 1000.0)
    monkeypatch.setitem(ggpbot.paper, "position", None)
    monkeypatch.setattr(ggpbot, "get_ohlcv_safe", lambda *a, **k: frames["current"])
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda *a, **k: float(frames["current"]["close"].iloc[-1]))
    monkeypatch.setattr(ggpbot, "update_position_dashboard", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "tg_entry", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "_live_manager", type(
        "PaperManager", (), {"start_trade": lambda *a, **k: None}
    )())

    # 1. Strict gate discovers the durable cross event.
    assert ggpbot._detect_confirmed_cross_events([SYMBOL]) == 1
    event = journal.list_cross_events(("ACTIVE",))[0]
    assert event["event_id"] == f"{SYMBOL}:{ggpbot.INTERVAL}:{gate['signal_candle_id']}"

    # 2. The persisted event is independently merged into the Watchlist and
    #    receives real institutional analysis, even with RF/V2 rows present.
    memory = ggpbot.MEMORY
    memory["scanner_v2_buy"] = [{"symbol": "ETHUSDT", "side": "BUY", "score": 70}]
    watchlist = bridge.update_unified_watchlist(
        memory, lambda _: frame, max_items=20, batch_size=20,
    )
    item = watchlist[CANONICAL]
    assert item["cross_event_id"] == event["event_id"]
    assert item["cross_event_direction"] == "BULLISH"
    assert item["cross_event_status"] == "ACTIVE"
    assert item["cross_event_analysis_required"] is True
    assert item["data_quality"] == "VALID_CLOSED_CANDLES"
    assert item["side_analyses"]["BUY"]["state"] in {
        "ENTRY_READY", "WAITING_FOR_CONFIRMATION", "WAITING_FOR_RETEST", "REJECTED"
    }

    # 3. Later closed candles supply the pullback/rejection and the strictly
    #    later breakout. The gate is now stale by design, so only the durable
    #    lifecycle may authorize this delayed entry.
    lifecycle_frame = _lifecycle_frame("BUY")
    frames["current"] = lifecycle_frame
    clock[0] = LIFECYCLE_NOW_MS / 1000.0
    ggpbot._sync_confirmed_cross_events()
    event = journal.get_cross_event(event["event_id"])
    assert event["metadata"]["phase"] == "BREAKOUT_CONFIRMED"
    # Breakout is a candidate; the Watchlist has not selected an entry zone yet.
    assert event["metadata"]["entry_ready"] is False
    assert event["metadata"]["structural_zone_gate"]["allowed"] is False
    assert event["metadata"]["structural_zone_gate"]["reason"] == "missing_selected_entry_zone"
    assert event["metadata"]["rejection_high"] > event["metadata"]["rejection_low"]
    assert ggpbot._cross_event_is_usable(event["event_id"], "BUY") is False

    # The breakout did not become structurally authorized. No entry may be
    # opened, and no trade-management snapshot can exist for this candidate.
    price = float(lifecycle_frame["close"].iloc[-1])
    assert ggpbot.execute_entry(
        "BUY", SYMBOL, price, price - 2.0, price * 1.008, price * 1.02, 80,
        "CROSS_DRIVEN_E2E_WITHOUT_WATCHLIST_ZONE", 0.69, "TREND", "TREND", "TREND",
    ) is False
    assert ggpbot.paper.get("position") is None
    unchanged = journal.get_cross_event(event["event_id"])
    assert unchanged["status"] == "ACTIVE"
    assert unchanged["metadata"]["entry_ready"] is False


# --------------------------------------------------------------------------- #
# 8) The same-cycle handoff is bounded, idempotent and lifecycle-aware.
# --------------------------------------------------------------------------- #
def test_no_new_cross_does_not_force_watchlist_refresh(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "no-new-cross.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)
    monkeypatch.setattr(ggpbot, "_detect_confirmed_cross_events", lambda symbols: 0)

    ggpbot.run_global_discovery_scanner()

    assert refresh_calls == []
    assert ggpbot.MEMORY["_cross_watchlist_delivery"]["new_events"] == 0
    assert ggpbot.MEMORY["_cross_watchlist_delivery"]["lifecycle_status"] == "CROSS_DETECTED"


def test_no_journal_never_attempts_watchlist_delivery(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "no-journal.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)

    ggpbot.run_global_discovery_scanner()

    assert refresh_calls == []
    assert ggpbot.MEMORY["confirmed_cross_events"] == []
    assert ggpbot.MEMORY["_cross_watchlist_delivery"]["reason"] == "NO_NEW_CROSS_EVENT"


def test_same_cycle_delivery_marks_durable_active_watchlist_lifecycle(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "lifecycle-delivery.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)

    ggpbot.run_global_discovery_scanner()

    event_id = journal.list_cross_events(("ACTIVE",))[0]["event_id"]
    durable = journal.get_cross_event(event_id)
    assert durable["metadata"]["lifecycle_status"] == "ACTIVE_WATCHLIST"
    assert durable["metadata"]["watchlist_delivered_at"] == pytest.approx(NOW_SEC)
    assert ggpbot.MEMORY["confirmed_cross_events"][0]["cross_event_lifecycle_status"] == "ACTIVE_WATCHLIST"
    assert ggpbot.MEMORY["watchlist"][CANONICAL]["cross_event_lifecycle_status"] == "ACTIVE_WATCHLIST"
    delivery = ggpbot.MEMORY["_cross_watchlist_delivery"]
    assert delivery["lifecycle_marked"] == [event_id]
    assert delivery["lifecycle_status"] == "ACTIVE_WATCHLIST"
    assert delivery["event_ids"] == [event_id]


def test_cross_lifecycle_survives_watchlist_snapshot_disappearance(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "snapshot-gone.sqlite3"))
    event = _lifecycle_event("BUY")
    event["metadata"]["lifecycle_status"] = "ACTIVE_WATCHLIST"
    assert journal.record_cross_event(**event) is True
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: None)
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {})

    ggpbot._sync_confirmed_cross_events()

    feed = ggpbot.MEMORY["confirmed_cross_events"]
    assert len(feed) == 1
    assert feed[0]["cross_event_lifecycle_status"] == "ACTIVE_WATCHLIST"
    projected = proj.project_watchlist_row(feed[0])
    assert projected["lifecycle_status"] == "ACTIVE_WATCHLIST"
    assert projected["decision_context"]["lifecycle_status"] == "ACTIVE_WATCHLIST"
    assert projected["cross_context"]["durable_event"] is True
    assert projected["cross_context"]["authoritative"] is True


def test_two_cross_symbols_keep_distinct_events_sides_and_stops(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "two-cross.sqlite3"))
    frame = _cross_frame("BUY")
    calls, fetch_calls, refresh_calls = [], [], []
    _prepare_scanner_cycle(monkeypatch, journal, frame, calls, fetch_calls, refresh_calls)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda symbol: _cross_frame("BUY"))

    symbols = [SYMBOL, "ETH/USDT:USDT"]
    assert ggpbot._detect_confirmed_cross_events(symbols) == 2

    rows = journal.list_cross_events(("ACTIVE",))
    assert len(rows) == 2
    assert len({row["event_id"] for row in rows}) == 2
    assert {row["symbol"] for row in rows} == {CANONICAL, "ETH"}
    assert {row["exchange_symbol"] for row in rows} == set(symbols)

    bridge.update_unified_watchlist(
        ggpbot.MEMORY, lambda symbol: _cross_frame("BUY"),
        max_items=10, batch_size=10,
    )
    watch = ggpbot.MEMORY["watchlist"]
    assert set(watch) == {CANONICAL, "ETH"}
    assert watch[CANONICAL]["cross_event_id"] != watch["ETH"]["cross_event_id"]
    assert watch[CANONICAL]["source_snapshot_id"] != watch["ETH"]["source_snapshot_id"]
    for symbol, row in watch.items():
        assert row["cross_event_status"] == "ACTIVE"
        assert row["decision_context"]["authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"
        assert row["entry_behavior_context"]["classification_source"] in {"GGP_MARKET_INTELLIGENCE", "NONE"}
