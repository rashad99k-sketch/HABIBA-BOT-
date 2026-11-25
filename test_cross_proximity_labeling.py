"""Regression proof for ranking-only cross labels in the existing Scanner feed.

The proximity observation is intentionally not a durable cross event. This test
only proves that the already-projected non-durable label is published on the
existing Scanner row; no detector, entry gate, journal or execution path is
changed.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import ggpbot
import naiso_unified_decision as proj
from roro_confluence import ema9_vwap_proximity, validate_dual_ema_vwap_entry_gate
from roro_trade_journal import TradeJournal
from test_dual_scanner_dashboard import _sample_session_frame


EXPECTED_DISPLAY = "RANKING_ONLY_CONFIRMED_BUY_CROSS_OBSERVATION"
CROSS_SYMBOL = "IOTX/USDT:USDT"
START_MS = 1_795_132_800_000
TF_MS = 900_000


def _ema50_only_frame():
    """A real closed-candle EMA50/VWAP cross with no EMA9/VWAP cross."""
    closes = []
    price = 100.0
    for _ in range(30):
        closes.append(price)
        price -= 0.12
    for _ in range(37):
        closes.append(price)
        price += 0.05
    closes = np.asarray(closes, dtype=float)
    opens = np.r_[closes[0], closes[:-1]]
    frame = pd.DataFrame({
        "timestamp": START_MS + np.arange(len(closes)) * TF_MS,
        "open": opens,
        "high": np.maximum(opens, closes) + 0.2,
        "low": np.minimum(opens, closes) - 0.2,
        "close": closes,
        "volume": np.full(len(closes), 1000.0),
    })
    return frame, int(frame["timestamp"].iloc[-1]) + TF_MS + 1_000


def _ema(values, period):
    return values.ewm(span=period, adjust=False).mean()


def _ranking_payload(candle_ts):
    return {
        "direction": "BUY",
        "state": "CROSS_CONFIRMED",
        "cross": "BULLISH",
        "cross_ema9": "BULLISH",
        "cross_ema50": None,
        "cross_any": "BULLISH",
        "cross_trigger": "EMA9_VWAP",
        "score": 80.0,
        "ema9": 101.0,
        "ema50": 100.5,
        "vwap": 100.0,
        "gap_atr": 0.1,
        "candle_timestamp": candle_ts,
        "latest_closed_timestamp": candle_ts,
        "data_freshness": "FRESH",
        "cross_age_seconds": 10.0,
    }


def test_proximity_writer_publishes_explicit_ranking_display_state(monkeypatch):
    frame, now_ms = _sample_session_frame(80)
    closed_ts = int(frame.timestamp.iloc[-1])
    monkeypatch.setattr(ggpbot.time, "time", lambda: now_ms / 1000.0)
    monkeypatch.setattr(
        ggpbot, "_roro_ema9_vwap_proximity",
        lambda *args, **kwargs: _ranking_payload(closed_ts),
    )
    monkeypatch.setitem(ggpbot.MEMORY, "_proximity_last_seen_by_symbol", {})
    monkeypatch.setitem(ggpbot.MEMORY, "_proximity_batch_updates", 0)
    monkeypatch.setitem(ggpbot.MEMORY, "ema9_vwap_proximity_scanner", [])
    analysis = {"symbol": "IOTX", "exchange_symbol": "IOTX/USDT:USDT", "icons": {}}

    ggpbot._enrich_with_existing_gpp_engines(frame, "BUY", analysis)

    rows = ggpbot.MEMORY["ema9_vwap_proximity_scanner"]
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "IOTX"
    # Raw indicator state is preserved for icon/ordering logic.
    assert row["state"] == "CROSS_CONFIRMED"
    # The published label is explicitly non-durable.
    assert row["cross_state_display"] == EXPECTED_DISPLAY
    assert row["cross_context"]["display_cross_state"] == EXPECTED_DISPLAY
    assert row["cross_context"]["cross_state"] == "CONFIRMED_BUY_CROSS"
    assert row["cross_state_display"] == row["cross_context"]["display_cross_state"]
    assert row["cross_context"]["durable_event"] is False
    assert row["cross_context"]["authoritative"] is False
    assert row["cross_context"]["role"] == proj.RANKING_ONLY_ROLE
    assert "cross_event_id" not in row


def test_projection_display_label_demotes_ranking_cross():
    row = {
        "symbol": "IOTX",
        "exchange_symbol": "IOTX/USDT:USDT",
        "direction": "BUY",
        "state": "CROSS_CONFIRMED",
        "cross_any": "BULLISH",
        "cross_ema9": "BULLISH",
        "role": proj.RANKING_ONLY_ROLE,
    }
    projected = proj.project_watchlist_row(row, ranking_only=True)
    assert projected["cross_state_display"] == EXPECTED_DISPLAY
    assert projected["cross_context"]["display_cross_state"] == EXPECTED_DISPLAY
    assert projected["cross_state_display"] == projected["cross_context"]["display_cross_state"]
    assert projected["cross_context"]["durable_event"] is False
    assert projected["cross_context"]["authoritative"] is False


def test_ema50_only_cross_is_ranking_and_never_creates_durable_event(tmp_path, monkeypatch):
    frame, now_ms = _ema50_only_frame()

    gate = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", _ema, timeframe_seconds=900, now_ms=now_ms, lookback_candles=2,
    )
    proximity = ema9_vwap_proximity(
        frame, _ema, ema_len=9, timeframe_seconds=900, now_ms=now_ms,
    )
    assert gate["ema9_cross"] is None
    assert gate["ema50_cross"] == "BULLISH"
    assert gate["ready"] is False
    assert gate.get("authorization_state") != "EMA9_TRIGGER_VALID"
    assert gate["reason"] == "missing_fresh_directional_ema9_vwap_cross"
    assert proximity["state"] == "CROSS_CONFIRMED"
    assert proximity["cross_ema9"] is None
    assert proximity["cross_ema50"] == "BULLISH"
    assert proximity["cross_any"] == "BULLISH"
    assert proximity["cross_trigger"] == "EMA50_VWAP"
    assert "entry_ready" not in proximity
    assert "allow_entry" not in proximity

    monkeypatch.setattr(ggpbot.time, "time", lambda: now_ms / 1000.0)
    monkeypatch.setitem(ggpbot.MEMORY, "_proximity_last_seen_by_symbol", {})
    monkeypatch.setitem(ggpbot.MEMORY, "_proximity_batch_updates", 0)
    monkeypatch.setitem(ggpbot.MEMORY, "ema9_vwap_proximity_scanner", [])
    analysis = {"symbol": "IOTX", "exchange_symbol": CROSS_SYMBOL, "icons": {}}
    ggpbot._enrich_with_existing_gpp_engines(frame, "BUY", analysis)

    row = ggpbot.MEMORY["ema9_vwap_proximity_scanner"][0]
    assert row["state"] == "CROSS_CONFIRMED"
    assert row["cross_ema9"] is None
    assert row["cross_ema50"] == "BULLISH"
    assert row["cross_trigger"] == "EMA50_VWAP"
    assert row["cross_state_display"] == EXPECTED_DISPLAY
    assert row["cross_context"]["durable_event"] is False
    assert row["cross_context"]["authoritative"] is False
    assert "cross_event_id" not in row

    journal = TradeJournal(str(tmp_path / "ema50_only.sqlite3"))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: frame)
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])

    assert ggpbot._detect_confirmed_cross_events([CROSS_SYMBOL]) == 0
    assert journal.list_cross_events(("ACTIVE",)) == []
    assert ggpbot.MEMORY["confirmed_cross_events"] == []
