import numpy as np
import pandas as pd

from ggp_institutional_bridge import canonical_symbol, merge_candidates, analyze_symbol, update_unified_watchlist


def frame(n=80):
    ts = np.arange(n) * 900_000 + 1_700_000_000_000
    close = np.linspace(100, 120, n)
    open_ = close - 0.3
    high = close + 1
    low = close - 1
    volume = np.ones(n) * 100
    return pd.DataFrame({"timestamp": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume})


def test_canonical_symbol_and_source_merge():
    assert canonical_symbol("BTC/USDT:USDT") == "BTC"
    merged = merge_candidates({
        "rf_watchlist": [{"symbol": "BTC/USDT:USDT", "side": "BUY", "score": 4}],
        "radar_top5": [{"symbol": "BTCUSDT", "score": 2}],
    })
    assert list(merged) == ["BTC"]
    assert merged["BTC"]["sides"] == ["BUY"]
    assert set(merged["BTC"]["sources"]) == {"rf_watchlist", "radar_top5"}


def test_analysis_returns_evidence_and_fails_closed_shape():
    result = analyze_symbol("BTC", "BUY", frame(), now_ms=1_700_000_000_000 + 80 * 900_000)
    assert result["decision_state"] in {"ENTRY_READY", "WAITING_FOR_CONFIRMATION", "WAITING_FOR_RETEST", "REJECTED"}
    assert "evidence" in result or "rejection_reason" in result
    assert "market_regime" in result


def test_watchlist_does_not_require_rf_only():
    memory = {"scanner_v2_buy": [{"symbol": "ETHUSDT", "side": "BUY", "score": 5}], "watchlist": {}}
    result = update_unified_watchlist(memory, lambda symbol: frame(), max_items=10)
    assert "ETH" in result
    assert result["ETH"]["source"] == "scanner_v2_buy"
    assert result["ETH"]["data_quality"] == "VALID_CLOSED_CANDLES"


def test_professional_scan_source_is_merged_without_directional_authority():
    merged = merge_candidates({
        "professional_scan_candidates": [{"symbol": "SOL/USDT:USDT", "side": "NEUTRAL", "score": 78,
                                           "side_hint": "SELL", "side_hint_is_soft": True}],
        "scanner_v2_buy": [{"symbol": "SOLUSDT", "side": "BUY", "score": 6}],
    })
    assert list(merged) == ["SOL"]
    assert set(merged["SOL"]["sources"]) == {"professional_scan_candidates", "scanner_v2_buy"}
    # Conflicting scanner hints are retained as evidence, not used as trade authorization.
    assert merged["SOL"]["sides"] == ["BUY", "SELL"]


def test_update_watchlist_checks_both_sides_even_when_discovery_hint_is_one_sided(monkeypatch):
    import ggp_institutional_bridge as bridge

    seen = []
    def fake_analyze(symbol, side, source_frame, now_ms=None):
        seen.append(side)
        state = "ENTRY_READY" if side == "SELL" else "WAITING_FOR_CONFIRMATION"
        return {
            "symbol": canonical_symbol(symbol), "exchange_symbol": "ETH/USDT:USDT", "side": side,
            "decision_state": state, "entry_status": state, "score": 82 if side == "SELL" else 35,
            "decision_reason": "fake", "rejection_reasons": [], "event_id": "sell-event" if side == "SELL" else None,
            "structural_stop": 99.0 if side == "SELL" else None,
            "data_quality": "VALID_CLOSED_CANDLES", "icons": {"status": "✅" if side == "SELL" else "🟡"},
            "golden_zone": {"score": 65}, "evidence": [],
        }
    monkeypatch.setattr(bridge, "analyze_symbol", fake_analyze)
    memory = {"scanner_v2_buy": [{"symbol": "ETH/USDT:USDT", "side": "BUY", "score": 8}], "watchlist": {}}
    out = bridge.update_unified_watchlist(memory, lambda _: frame(), max_items=10, batch_size=10)
    assert seen == ["BUY", "SELL"]
    assert out["ETH"]["side"] == "SELL"
    assert out["ETH"]["decision_state"] == "ENTRY_READY"
    assert out["ETH"]["side_hint_used_as_gate"] is False


def test_analyze_symbol_both_rejects_opposing_ready_hypotheses(monkeypatch):
    import ggp_institutional_bridge as bridge
    def fake_analyze(symbol, side, source_frame, now_ms=None):
        return {"symbol": "BTC", "side": side, "decision_state": "ENTRY_READY",
                "entry_status": "ENTRY_READY", "score": 80, "golden_zone": {"score": 70},
                "decision_setup_type": "TEST", "decision_reason": "ready", "event_id": f"{side}-event",
                "structural_stop": 99 if side == "BUY" else 121, "icons": {"status": "✅"}}
    monkeypatch.setattr(bridge, "analyze_symbol", fake_analyze)
    result = bridge.analyze_symbol_both("BTC/USDT:USDT", frame(), side_hints=["BUY"])
    assert result["decision_state"] == "REJECTED"
    assert result["decision_reason"] == "conflicting_opposing_setups_require_resolution"
    assert result["side_analyses"]["BUY"]["state"] == "ENTRY_READY"
    assert result["side_analyses"]["SELL"]["state"] == "ENTRY_READY"


def test_fvg_fill_is_cumulative_not_last_candle_only():
    import ggp_institutional_bridge as bridge
    n = 80
    ts = np.arange(n) * 900_000 + 1_700_000_000_000
    df = pd.DataFrame({"timestamp": ts, "open": 105.0, "high": 106.0,
                       "low": 104.0, "close": 105.0, "volume": 100.0})
    df.loc[70, ["open", "high", "low", "close"]] = [99.0, 100.0, 98.0, 99.5]
    df.loc[71, ["open", "high", "low", "close"]] = [100.5, 103.0, 100.5, 102.5]
    df.loc[72, ["open", "high", "low", "close"]] = [103.0, 105.0, 102.0, 104.0]
    df.loc[73, ["open", "high", "low", "close"]] = [103.0, 104.0, 100.8, 103.5]
    # Subsequent bars recover above the gap, but historical penetration must remain recorded.
    result = bridge.detect_fvg(df, side="BUY", atr=1.0)
    assert result is not None
    assert result["status"] == "PARTIALLY_FILLED"
    assert result["fill_pct"] >= 59.0
    assert result["fill_pct"] <= 61.0
    assert result["last_touch_timestamp"] == int(ts[73])


def test_watchlist_priority_prefers_actual_golden_zone_and_retest_state(tmp_path):
    import ggp_institutional_bridge as bridge
    memory = {
        "professional_scan_candidates": [
            {"symbol": "AAAUSDT", "score": 99, "side_hint": "BUY"},
            {"symbol": "BBB/USDT:USDT", "score": 72, "side_hint": "SELL"},
        ],
        "watchlist": {
            "BBB": {"decision_state": "WAITING_FOR_RETEST", "analysis_priority_score": 82,
                    "golden_zone": {"score": 88},
                    "order_block": {"distance_atr": 0.25},
                    "liquidity": {"confidence": 80}, "last_update": __import__("time").time()},
        },
    }
    # Only the first item in the rotation batch is analyzed. Structural proximity
    # and a real pending retest must outrank a larger raw discovery score.
    seen = []
    out = bridge.update_unified_watchlist(memory, lambda symbol: seen.append(symbol) or frame(),
                                          max_items=10, batch_size=1)
    assert seen == ["BBB/USDT:USDT"]
    assert "BBB" in out
    assert out["BBB"]["decision_state"] in {"ENTRY_READY", "WAITING_FOR_CONFIRMATION", "WAITING_FOR_RETEST", "REJECTED"}


def test_decision_serializer_flattens_tuple_evidence():
    import ggp_institutional_bridge as bridge
    from roro_institutional import InstitutionalDecision

    decision = InstitutionalDecision(
        "WAITING_FOR_CONFIRMATION", "TEST", "BUY", "test",
        evidence=("sweep", ("reclaim", "closed_mss")),
        rejection_reasons=(("trigger_pending",),),
    )
    result = bridge._decision_result(decision)
    assert result["evidence"] == ["sweep", "reclaim", "closed_mss"]
    assert result["rejection_reasons"] == ["trigger_pending"]
    assert all(isinstance(value, str) for value in result["evidence"] + result["rejection_reasons"])


def test_confirmed_cross_is_persisted_as_independent_watchlist_event_and_survives_neutral_scan(monkeypatch):
    import time
    import ggp_institutional_bridge as bridge

    cross_ts = int(time.time() * 1000) - 60_000
    enabled = [True]

    def fake_analyze(symbol, side, source_frame, now_ms=None):
        is_match = enabled[0] and side == "BUY"
        return {
            "symbol": canonical_symbol(symbol), "exchange_symbol": "ETH/USDT:USDT", "side": side,
            "decision_state": "ENTRY_READY" if is_match else "WAITING_FOR_CONFIRMATION",
            "entry_status": "ENTRY_READY" if is_match else "WAITING_FOR_CONFIRMATION",
            "score": 1.0, "decision_reason": "confirmed_cross_location_ready" if is_match else "no_current_cross",
            "event_id": "EMA9VWAP:ETH:BULLISH:" + str(cross_ts) if is_match else None,
            "cross_event_id": "EMA9VWAP:ETH:BULLISH:" + str(cross_ts) if is_match else None,
            "cross_status": "CONFIRMED" if is_match else "NONE",
            "cross_direction": "BULLISH" if is_match else None,
            "cross_candle_timestamp": cross_ts if is_match else None,
            "structural_stop": 98.5 if is_match else None,
            "location_status": "GOOD" if is_match else "WAITING",
            "location_distance_atr": 0.4 if is_match else None,
            "location_factor_count": 2 if is_match else 0,
            "trade_mode": "SCALP" if is_match else None,
            "data_quality": "VALID_CLOSED_CANDLES", "icons": {"status": "✅" if is_match else "🟡"},
            "golden_zone": {"score": 20}, "evidence": ["location_only_no_sweep_required"] if is_match else [],
        }

    monkeypatch.setattr(bridge, "analyze_symbol", fake_analyze)
    memory = {
        "market_universe": {"state": "READY", "symbols": ["ETH/USDT:USDT"]},
        "watchlist": {},
    }
    bridge.update_unified_watchlist(memory, lambda _: frame(), max_items=10, batch_size=10)
    event_id = "EMA9VWAP:ETH:BULLISH:" + str(cross_ts)
    assert event_id in memory["cross_event_watchlist"]
    assert memory["cross_event_watchlist"][event_id]["side"] == "BUY"
    assert memory["cross_event_watchlist"][event_id]["event_lifecycle_state"] == "DISCOVERED"

    enabled[0] = False
    bridge.update_unified_watchlist(memory, lambda _: frame(), max_items=10, batch_size=10)
    assert event_id in memory["cross_event_watchlist"]
    assert memory["cross_event_watchlist"][event_id]["cross_status"] == "CONFIRMED"


def test_confirmed_cross_below_adx_is_queued_as_rejected_event_not_lost_to_opposite_side(monkeypatch):
    import time
    import ggp_institutional_bridge as bridge

    cross_ts = int(time.time() * 1000) - 60_000
    event_id = f"EMA9VWAP:ETH:BULLISH:{cross_ts}"

    def fake_analyze(symbol, side, source_frame, now_ms=None):
        if side == "BUY":
            return {
                "symbol": "ETH", "exchange_symbol": "ETH/USDT:USDT", "side": "BUY",
                "decision_state": "REJECTED", "entry_status": "REJECTED",
                "decision_reason": "BASE_ADX_BELOW_18:17.99", "rejection_reason": "BASE_ADX_BELOW_18:17.99",
                "event_id": event_id, "cross_event_id": event_id,
                "cross_status": "CONFIRMED", "cross_direction": "BULLISH",
                "cross_candle_timestamp": cross_ts, "adx": 17.99,
                "location_status": "EXCELLENT", "location_distance_atr": 0.2,
                "structural_stop": 98.0, "data_quality": "VALID_CLOSED_CANDLES",
                "golden_zone": {"score": 99}, "evidence": ["BASE_ADX_BELOW_18"],
            }
        return {
            "symbol": "ETH", "exchange_symbol": "ETH/USDT:USDT", "side": "SELL",
            "decision_state": "WAITING_FOR_CONFIRMATION", "entry_status": "WAITING_FOR_CONFIRMATION",
            "decision_reason": "no_current_cross", "event_id": None,
            "cross_event_id": None, "cross_status": "NONE", "cross_direction": None,
            "data_quality": "VALID_CLOSED_CANDLES", "golden_zone": {"score": 0}, "evidence": [],
        }

    monkeypatch.setattr(bridge, "analyze_symbol", fake_analyze)
    memory = {
        "market_universe": {"state": "READY", "symbols": ["ETH/USDT:USDT"]},
        "watchlist": {},
    }
    bridge.update_unified_watchlist(memory, lambda _: frame(), max_items=10, batch_size=10)
    assert event_id in memory["cross_event_watchlist"]
    queued = memory["cross_event_watchlist"][event_id]
    assert queued["decision_state"] == "REJECTED"
    assert queued["rejection_reason"].startswith("BASE_ADX_BELOW_18")
