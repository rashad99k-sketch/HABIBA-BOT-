import pandas as pd

from ggp_institutional_bridge import _indicator_snapshot, cross_event_id
from roro_confluence import compute_session_vwap


def _frame(closes):
    start = 0
    rows = []
    for i, close in enumerate(closes):
        ts = start + i * 900_000
        rows.append({
            "timestamp": ts,
            "open": float(close),
            "high": float(close) + 0.2,
            "low": float(close) - 0.2,
            "close": float(close),
            "volume": 100.0,
        })
    return pd.DataFrame(rows)


def test_bridge_indicator_snapshot_uses_canonical_session_vwap():
    frame = _frame([100.0] * 60)
    now_ms = 60 * 900_000
    expected = compute_session_vwap(frame, timeframe_seconds=900, now_ms=now_ms)
    snapshot = _indicator_snapshot(frame, now_ms=now_ms)
    assert snapshot["vwap_source"] == "roro_confluence.compute_session_vwap"
    assert snapshot["vwap"] == expected.iloc[-1]
    assert snapshot["quality"] == "FRESH"


def test_bridge_rejects_truncated_latest_utc_session():
    frame = _frame([100.0] * 60)
    # Remove the UTC session-opening candles from the latest session.
    frame["timestamp"] += 86_400_000
    frame = frame.iloc[4:].reset_index(drop=True)
    snapshot = _indicator_snapshot(frame, now_ms=86_400_000 + 60 * 900_000)
    assert snapshot["quality"] == "DATA_UNAVAILABLE"
    assert "invalid_or_truncated_session_vwap" in snapshot["missing"]


def test_cross_event_identity_is_stable_and_directional():
    first = cross_event_id("BTC/USDT:USDT", "bullish", 1_700_000_000_000)
    second = cross_event_id("BTCUSDT", "BULLISH", 1_700_000_000_000)
    assert first == second == "EMA9VWAP:BTC:BULLISH:1700000000000"
    assert cross_event_id("BTCUSDT", "SELL", 1_700_000_000_000) is None
    assert cross_event_id("BTCUSDT", "BEARISH", 1_700_000_000_000) == "EMA9VWAP:BTC:BEARISH:1700000000000"


def test_indicator_snapshot_exposes_cross_lifecycle_fields():
    # A final bearish displacement from a flat sequence produces a closed-candle
    # EMA9/VWAP cross while retaining a valid canonical session snapshot.
    frame = _frame([100.0] * 59 + [90.0])
    snapshot = _indicator_snapshot(frame, now_ms=60 * 900_000)
    assert snapshot["cross_status"] == "CONFIRMED"
    assert snapshot["cross_direction"] == "BEARISH"
    assert snapshot["cross_candle_timestamp"] == frame.iloc[-1]["timestamp"]
    assert snapshot["cross_distance"] > 0
