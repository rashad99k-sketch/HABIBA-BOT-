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
    frame = _frame([100.0] * 58 + [102.0, 90.0])
    snapshot = _indicator_snapshot(frame, now_ms=60 * 900_000)
    assert snapshot["cross_status"] == "CONFIRMED"
    assert snapshot["cross_direction"] == "BEARISH"
    assert snapshot["cross_candle_timestamp"] == frame.iloc[-1]["timestamp"]
    assert snapshot["cross_distance"] > 0


def test_location_quality_does_not_require_sweep_mss_or_retest():
    from ggp_institutional_bridge import _evaluate_cross_location

    frame = _frame([100.0] * 60)
    decision = _evaluate_cross_location(frame, "BUY", {
        "price": 100.0,
        "atr": 1.0,
        "order_block": {
            "valid": True, "status": "FRESH", "low": 99.2, "high": 99.8,
            "distance_atr": 0.2,
        },
        "fvg": None,
        "support_resistance": {"supports": [], "resistances": []},
        "golden_zone": {"value_zone": "UNKNOWN"},
        "indicator_snapshot": {"price_vwap_distance_atr": 0.6},
        # No liquidity event, sweep, MSS or retest data is provided.
    })
    assert decision["status"] in {"GOOD", "MODERATE"}
    assert decision["structural_stop"] is not None
    assert decision["universal_sweep_mss_retest_gate"] is False


def test_trade_mode_uses_scalp_by_default_and_trend_only_with_aligned_strength():
    from types import SimpleNamespace
    from ggp_institutional_bridge import _classify_cross_trade_mode

    indicators = {
        "adx": 24.9, "di_plus": 32.0, "di_minus": 12.0,
        "ema9_slope_atr": 0.4,
    }
    assert _classify_cross_trade_mode("BUY", SimpleNamespace(direction="BULLISH"), indicators, 1.1)[0] == "SCALP"
    indicators["adx"] = 25.0
    assert _classify_cross_trade_mode("BUY", SimpleNamespace(direction="BULLISH"), indicators, 1.1)[0] == "TREND"
    assert _classify_cross_trade_mode("BUY", SimpleNamespace(direction="BEARISH"), indicators, 1.1)[0] == "SCALP"
    assert _classify_cross_trade_mode("BUY", SimpleNamespace(direction="BULLISH"), indicators, 0.89)[0] == "SCALP"


def _install_analyzer_stubs(monkeypatch, adx_value):
    from types import SimpleNamespace
    import ggp_institutional_bridge as bridge

    liquidity = SimpleNamespace(event="UNKNOWN", direction="UNKNOWN", level=None, confidence=0, reasons=[])
    context = {
        "price": 100.0, "atr": 1.0,
        "regime": SimpleNamespace(state="BULLISH", direction="BULLISH"),
        "liquidity": liquidity,
        "structure": {"confirmed": False},
        "volume_ratio": 0.9, "volume_confirmed": False,
        "indicator_snapshot": {
            "quality": "FRESH", "cross_status": "CONFIRMED", "cross_direction": "BULLISH",
            "cross_candle_timestamp": 59 * 900_000, "cross_age_bars": 0,
            "cross_age_seconds": 0, "cross_distance": 0.1,
            "ema9": 100.2, "ema50": 99.8, "vwap": 100.0,
            "ema9_slope_atr": 0.3, "price_vwap_distance_atr": 0.2,
            "volume_ratio": 0.9, "vwap_position": "ABOVE",
        },
        "golden_zone": {"score": 20, "value_zone": "UNKNOWN"},
        "order_block": None, "fvg": None,
        "support_resistance": {"status": "UNKNOWN", "supports": [], "resistances": []},
    }

    class _Decision:
        def to_dict(self):
            return {"status": "LOCATION_ONLY", "evidence": []}

    monkeypatch.setattr(bridge, "_build_context", lambda *a, **k: context)
    monkeypatch.setattr(bridge, "_evaluate_cross_location", lambda *a, **k: {
        "status": "GOOD", "distance_atr": 0.2, "structural_stop": 98.8,
        "reason": "confirmed_directional_location", "evidence": ["OB_FRESH"],
        "factor_count": 2, "zone_low": 99.0, "zone_high": 99.8,
    })
    monkeypatch.setattr(bridge, "_dmi_components", lambda *a, **k: {
        "adx": adx_value, "di_plus": 28.0, "di_minus": 12.0, "adx_slope": 1.0,
    })
    monkeypatch.setattr(bridge, "build_intelligence_decision", lambda **k: _Decision())
    monkeypatch.setattr(bridge, "classify_price_pattern", lambda *a, **k: {"stage": "TEST"})


def test_confirmed_cross_can_be_entry_ready_without_sweep_mss_or_retest(monkeypatch):
    from ggp_institutional_bridge import analyze_symbol

    _install_analyzer_stubs(monkeypatch, 18.0)
    frame = _frame([100.0] * 60)
    now_ms = int(frame.timestamp.iloc[-1]) + 900_000
    result = analyze_symbol("ETH/USDT:USDT", "BUY", frame, now_ms=now_ms)
    assert result["decision_state"] == "ENTRY_READY"
    assert result["setup_type"] == "EMA9_VWAP_CROSS_LOCATION"
    assert result["trade_mode"] == "SCALP"
    assert result["event_id"].startswith("EMA9VWAP:ETH:BULLISH:")
    assert "SWEEP_MSS_RETEST_ARE_NOT_UNIVERSAL_ENTRY_GATES" in result["evidence"]


def test_base_adx_below_18_rejects_even_when_location_is_good(monkeypatch):
    from ggp_institutional_bridge import analyze_symbol

    _install_analyzer_stubs(monkeypatch, 17.99)
    frame = _frame([100.0] * 60)
    now_ms = int(frame.timestamp.iloc[-1]) + 900_000
    result = analyze_symbol("ETH/USDT:USDT", "BUY", frame, now_ms=now_ms)
    assert result["decision_state"] == "REJECTED"
    assert result["decision_reason"].startswith("BASE_ADX_BELOW_18:")


def test_entry_time_cross_mode_is_latched_over_legacy_looser_trend_heuristic():
    from ggp_profit_brain import RANGE_MODE, TREND_MODE, classify_profit_mode

    strong_looking = {
        "side": "BUY", "regime": "STRONG_TREND", "trade_state": "TREND_RIDE",
        "adx": 24.0, "adx_slope": 1.0, "continuation_probability": 0.80,
        "di_plus": 35.0, "di_minus": 10.0, "volume_ratio": 1.4,
        "momentum_strength": 80.0,
    }
    scalp_mode, _, _ = classify_profit_mode({**strong_looking, "authorized_cross_mode": "SCALP"})
    trend_mode, _, _ = classify_profit_mode({**strong_looking, "authorized_cross_mode": "TREND"})
    assert scalp_mode == RANGE_MODE
    assert trend_mode == TREND_MODE


def test_bridge_adx_and_di_values_follow_gGP_rma_calculation():
    import numpy as np
    from ggp_institutional_bridge import _dmi_components

    frame = _frame(np.linspace(100.0, 113.0, 80))
    high, low, close = frame.high, frame.low, frame.close
    period = 14
    tr = pd.concat([
        (high - low).rename("hl"),
        (high - close.shift(1)).abs().rename("hc"),
        (low - close.shift(1)).abs().rename("lc"),
    ], axis=1).max(axis=1)
    rma = lambda values: values.ewm(alpha=1.0 / period, adjust=False).mean()
    atr = rma(tr) + 1e-9
    up_move, down_move = high.diff(), -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=frame.index)
    plus_di = 100.0 * rma(plus_dm) / (atr + 1e-9)
    minus_di = 100.0 * rma(minus_dm) / (atr + 1e-9)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100.0
    expected_adx = rma(dx).bfill().ffill().fillna(0.0).clip(0.0, 100.0)

    actual = _dmi_components(frame)
    assert actual["adx"] == expected_adx.iloc[-1]
    assert actual["di_plus"] == plus_di.iloc[-1]
    assert actual["di_minus"] == minus_di.iloc[-1]


def test_exact_touch_then_separation_is_not_a_confirmed_cross(monkeypatch):
    import ggp_institutional_bridge as bridge

    frame = _frame([100.0] * 60)
    # EMA9 is 100; set prior VWAP exactly equal to EMA9, then below it.
    # Equality followed by separation is not a strict negative-to-positive cross.
    monkeypatch.setattr(
        bridge,
        "compute_session_vwap",
        lambda closed, timeframe_seconds, now_ms=None: pd.Series([101.0] * 58 + [100.0, 99.0], index=closed.index),
    )
    snapshot = _indicator_snapshot(frame, now_ms=60 * 900_000)
    assert snapshot["cross_status"] != "CONFIRMED"
    assert snapshot["cross_direction"] is None
