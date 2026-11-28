import numpy as np
import pandas as pd

from ggp_market_intelligence import (
    EntryBehavior,
    classify_dynamic_trade_classification,
    classify_entry_behavior,
)
from ggp_institutional_bridge import _indicator_snapshot


def test_adaptive_immediate_entry_is_evidence_based_and_symmetric():
    common = dict(
        setup_type="TREND_CONTINUATION", causal_zone_valid=True,
        displacement_atr=1.4, continuation_probability=0.80,
        volatility_atr=0.5, zone_quality=85, order_block_fresh=True,
        ema9_slope=0.8, adx=31, rsi=58, momentum_strength=82,
        volume_ratio=1.35, liquidity_confidence=78,
        opposing_zone_distance_atr=2.2,
    )
    buy = classify_entry_behavior(vwap_position="ABOVE", **common)
    sell = classify_entry_behavior(vwap_position="BELOW", ema9_slope=-0.8, rsi=42, **{k: v for k, v in common.items() if k not in {"ema9_slope", "rsi"}})
    assert buy[0] == EntryBehavior.IMMEDIATE.value
    assert sell[0] == EntryBehavior.IMMEDIATE.value
    assert "strong_fresh_zone" in buy[2]
    assert "strong_fresh_zone" in sell[2]


def test_adaptive_classifier_keeps_micro_pullback_for_extension_without_immediate_proof():
    result = classify_entry_behavior(
        setup_type="TREND_CONTINUATION", causal_zone_valid=True,
        displacement_atr=1.3, continuation_probability=0.75,
        volatility_atr=1.0,
    )
    assert result[0] == EntryBehavior.MICRO_PULLBACK.value


def test_dynamic_trade_classification_reclassifies_weak_and_reversal_states():
    weak = classify_dynamic_trade_classification(
        adx=16, momentum_strength=32, volume_ratio=0.7,
        continuation_probability=0.35, momentum_decay=True,
    )
    reversal = classify_dynamic_trade_classification(
        adx=28, momentum_strength=70, volume_ratio=1.2,
        continuation_probability=0.70, structure_aligned=True,
        structure_broken=True, counter_pressure=75,
    )
    trend = classify_dynamic_trade_classification(
        adx=30, momentum_strength=75, volume_ratio=1.3,
        continuation_probability=0.75, structure_aligned=True,
        liquidity_supportive=True,
    )
    assert weak[0] == "WEAK_MOMENTUM"
    assert reversal[0] == "REVERSAL_RISK"
    assert trend[0] == "TREND_CONTINUATION"


def test_closed_candle_indicator_snapshot_exposes_real_values_and_stale_quality():
    n = 80
    # The canonical Session VWAP intentionally rejects truncated UTC sessions.
    # Start this synthetic frame at a UTC session boundary so the fixture is
    # valid under the production data contract.
    ts = np.arange(n) * 900_000 + 1_699_920_000_000
    close = np.linspace(100.0, 104.0, n)
    frame = pd.DataFrame({
        "timestamp": ts, "open": close - 0.1, "high": close + 0.3,
        "low": close - 0.3, "close": close, "volume": np.full(n, 1000.0),
    })
    # The latest candle must also be closed before canonical VWAP has a value
    # at the final row.
    fresh = _indicator_snapshot(frame, now_ms=int(ts[-1] + 900_000))
    stale = _indicator_snapshot(frame, now_ms=int(ts[-1] + 3_000_000))
    assert fresh["quality"] == "FRESH"
    assert stale["quality"] == "STALE_DATA"
    assert fresh["ema9"] is not None
    assert fresh["vwap"] is not None
    assert fresh["atr"] is not None
    assert fresh["source"] == "CLOSED_CANDLE_FRAME"
