"""Strict micro-pullback continuation tests for both directions."""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

import roro_institutional as institutional


def _micro_frame(side):
    count = 60
    timestamps = 1_800_000_000_000 + np.arange(count) * 900_000
    if side == "BUY":
        close = np.linspace(100.0, 118.0, count)
        open_ = close - 0.10
        high = close + 0.15
        low = close - 0.15
        open_[57], close[57], high[57], low[57] = 116.5, 117.5, 117.6, 116.4
        open_[58], close[58], high[58], low[58] = 117.5, 117.3, 117.5, 117.2
        open_[59], close[59], high[59], low[59] = 117.4, 118.2, 118.3, 117.3
        level = 117.0
        zone = (116.7, 116.9, "ORDER_BLOCK", int(timestamps[50]))
    else:
        close = np.linspace(120.0, 102.0, count)
        open_ = close + 0.10
        high = close + 0.15
        low = close - 0.15
        open_[57], close[57], high[57], low[57] = 103.5, 102.5, 103.6, 102.4
        open_[58], close[58], high[58], low[58] = 102.5, 102.7, 102.7, 102.3
        open_[59], close[59], high[59], low[59] = 102.5, 101.8, 102.6, 101.7
        level = 103.0
        zone = (103.1, 103.3, "ORDER_BLOCK", int(timestamps[50]))
    frame = pd.DataFrame({
        "timestamp": timestamps, "open": open_, "high": high,
        "low": low, "close": close, "volume": np.ones(count),
    })
    frame.loc[57, "volume"] = 2.0
    return frame, level, zone


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_strict_micro_pullback_can_confirm_without_full_zone_retest(side):
    frame, level, zone = _micro_frame(side)
    pivots = ([(10, level)], []) if side == "BUY" else ([], [(10, level)])
    metadata = {
        "kind": "ORDER_BLOCK", "zone_direction": "DEMAND" if side == "BUY" else "SUPPLY",
        "low": zone[0], "high": zone[1], "available_timestamp": zone[3],
    }
    context = {
        "location_first_required": True,
        "latest_timestamp": int(frame.timestamp.iloc[-1]),
        "timeframe_ms": 900_000,
        "reference_price": float(frame.close.iloc[-1]),
        "max_location_distance_atr": 1.5,
        "volume_confirmed": True,
        "order_block": dict(metadata),
    }

    with (
        patch.object(institutional, "_closed", side_effect=lambda data, now_ms=None: data.copy()),
        patch.object(institutional, "_atr", return_value=1.0),
        patch.object(institutional, "_confirmed_pivots", return_value=pivots),
        patch.object(institutional, "evaluate_early_trend_timing",
                     return_value={"valid": True, "phase": "CONFIRMED", "adx": 30.0,
                                   "ema9_extension_atr": 0.2, "vwap_extension_atr": 0.2}),
        patch.object(institutional, "_zone_retest_candidates", return_value=[zone]),
        patch.object(institutional, "_zone_metadata_for_retest", return_value=dict(metadata)),
    ):
        decision = institutional.ContinuationEngine().analyze(
            "TEST/USDT:USDT", side, frame, context=context,
        )

    assert decision.state == institutional.DecisionState.ENTRY_READY.value
    assert any(item == "MICRO_PULLBACK:MICRO_STRUCTURE" for item in decision.evidence)
    assert decision.entry_zone["entry_pattern"] == "MICRO_PULLBACK"
    assert decision.entry_zone["retest_confirmed"] is False
    assert 0 < decision.entry_zone["pullback_depth_atr"] <= 0.65
    assert any(item.startswith("entry_location_context:") for item in decision.evidence)
