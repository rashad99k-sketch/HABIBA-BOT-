"""Regression proof for ranking-only cross labels in the existing Scanner feed.

The proximity observation is intentionally not a durable cross event. This test
only proves that the already-projected non-durable label is published on the
existing Scanner row; no detector, entry gate, journal or execution path is
changed.
"""
from __future__ import annotations

import ggpbot
import naiso_unified_decision as proj
from test_dual_scanner_dashboard import _sample_session_frame


EXPECTED_DISPLAY = "RANKING_ONLY_CONFIRMED_BUY_CROSS_OBSERVATION"


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
    assert projected["cross_context"]["durable_event"] is False
    assert projected["cross_context"]["authoritative"] is False
