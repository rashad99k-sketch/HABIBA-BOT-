"""Regression coverage for the continuation entry-zone trace.

The success path must retain the exact zone that was retested. This test
exercises the full ContinuationEngine decision path in both directions without
exchange access or order execution.
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

import roro_institutional as institutional


def _continuation_frame(side):
    count = 60
    timestamps = 1_800_000_000_000 + np.arange(count) * 900_000
    if side == "BUY":
        close = np.linspace(100.0, 118.0, count)
        open_ = close - 0.10
        high = close + 0.15
        low = close - 0.15
        open_[57], close[57], high[57], low[57] = 116.5, 117.5, 117.6, 116.4
        open_[58], close[58], high[58], low[58] = 117.3, 117.4, 117.7, 116.95
        open_[59], close[59], high[59], low[59] = 117.5, 118.2, 118.3, 117.4
        broken_level = 117.0
        zone = (117.0, 117.5, "ORDER_BLOCK", int(timestamps[50]))
    else:
        close = np.linspace(120.0, 102.0, count)
        open_ = close + 0.10
        high = close + 0.15
        low = close - 0.15
        open_[57], close[57], high[57], low[57] = 103.5, 102.5, 103.6, 102.4
        open_[58], close[58], high[58], low[58] = 102.8, 102.6, 103.1, 102.3
        open_[59], close[59], high[59], low[59] = 102.5, 101.8, 102.6, 101.7
        broken_level = 103.0
        zone = (102.5, 103.0, "ORDER_BLOCK", int(timestamps[50]))
    frame = pd.DataFrame({
        "timestamp": timestamps, "open": open_, "high": high,
        "low": low, "close": close, "volume": np.ones(count),
    })
    frame.loc[57, "volume"] = 2.0
    return frame, broken_level, zone


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_continuation_entry_ready_preserves_selected_zone_without_name_error(side):
    frame, broken_level, zone = _continuation_frame(side)
    pivots = ([(10, broken_level)], []) if side == "BUY" else ([], [(10, broken_level)])
    zone_metadata = {
        "kind": "ORDER_BLOCK",
        "side": side,
        "zone_direction": "DEMAND" if side == "BUY" else "SUPPLY",
        "low": zone[0],
        "high": zone[1],
        "available_timestamp": zone[3],
    }
    context = {
        "location_first_required": True,
        "latest_timestamp": int(frame.timestamp.iloc[-1]),
        "timeframe_ms": 900_000,
        "reference_price": float(frame.close.iloc[-1]),
        "max_location_distance_atr": 1.5,
        "volume_confirmed": True,
        "order_block": dict(zone_metadata),
    }

    with (
        patch.object(institutional, "_closed", side_effect=lambda data, now_ms=None: data.copy()),
        patch.object(institutional, "_atr", return_value=1.0),
        patch.object(institutional, "_confirmed_pivots", return_value=pivots),
        patch.object(institutional, "evaluate_early_trend_timing",
                     return_value={"valid": True, "phase": "CONFIRMED", "adx": 30.0,
                                   "ema9_extension_atr": 0.2, "vwap_extension_atr": 0.2}),
        patch.object(institutional, "_zone_retest_candidates", return_value=[zone]),
        patch.object(institutional, "_zone_metadata_for_retest", return_value=zone_metadata),
    ):
        decision = institutional.ContinuationEngine().analyze(
            "TEST/USDT:USDT", side, frame, context=context,
        )

    assert decision.state == institutional.DecisionState.ENTRY_READY.value
    assert decision.reason == "continuation_sequence_confirmed"
    assert decision.entry_zone == zone_metadata
    assert any(item.startswith("entry_zone:") for item in decision.evidence)
    assert decision.structural_stop is not None
    assert decision.structural_stop < float(frame.close.iloc[-1]) if side == "BUY" else decision.structural_stop > float(frame.close.iloc[-1])
