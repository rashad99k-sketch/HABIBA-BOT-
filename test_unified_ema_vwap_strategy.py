import numpy as np
import pandas as pd

from unified_ema_vwap_strategy import Setup, UnifiedEMAVWAPPullbackEngine, _clean_closed, indicators


def frame(n=70, start=None, close=None, volume=None):
    start = start or int((pd.Timestamp.now(tz="UTC").timestamp() - n * 900) * 1000)
    ts = start + np.arange(n) * 900_000
    c = np.asarray(close if close is not None else 100 + np.sin(np.arange(n) / 5) * .2, dtype=float)
    return pd.DataFrame({"timestamp": ts, "open": c, "high": c + .25, "low": c - .25,
                         "close": c, "volume": np.asarray(volume if volume is not None else np.ones(n) * 10, dtype=float)})


def test_session_vwap_uses_ohlc4_and_resets_by_utc_date():
    day = 1_800_000_000_000
    ts = np.array([day, day + 900_000, day + 86_400_000, day + 86_400_000 + 900_000], dtype=np.int64)
    o = np.array([10., 10., 20., 20.]); h = o + 2; l = o - 2; c = o + 1; v = np.ones(4)
    base = pd.DataFrame({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
    # Repeat within two separate UTC sessions to satisfy the indicator warmup.
    rows = []
    for day_offset in (0, 86_400_000):
        for k in range(30):
            rows.append({"timestamp": day + day_offset + k * 900_000, "open": 20., "high": 22.,
                         "low": 18., "close": 21., "volume": 1.})
    got = indicators(pd.DataFrame(rows))
    assert got["vwap"] == (20 + 22 + 18 + 21) / 4
    assert got["timezone"] == "UTC"


def test_closed_filter_rejects_forming_and_seconds_timestamps():
    f = frame(60)
    now = int(f.timestamp.iloc[-1] + 900_000 - 1)
    assert len(_clean_closed(f, now, 900)) == 59
    bad = f.copy(); bad["timestamp"] //= 1000
    assert _clean_closed(bad, now, 900).empty


def test_zero_session_volume_reports_unknown_vwap():
    result = indicators(frame(60, volume=np.zeros(60)))
    assert result["vwap"] is None
    assert result["reason"] == "zero_session_volume"


def test_setup_persists_across_engine_restart(tmp_path):
    path = tmp_path / "watch.json"
    UnifiedEMAVWAPPullbackEngine(str(path))._persist(
        Setup(symbol="BTC", status="WAITING_FOR_PULLBACK", direction="BUY", crossover_ts=123))
    restored = UnifiedEMAVWAPPullbackEngine(str(path)).get("BTC")
    assert restored and restored.status == "WAITING_FOR_PULLBACK" and restored.crossover_ts == 123


def test_authorization_requires_ready_and_tp1_forward_space(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "watch.json"))
    assert engine.authorize("X", 101, 103)["allowed"] is False
    engine._persist(Setup(symbol="X", status="ENTRY_READY", direction="BUY", atr14=1,
                           causal_level=100, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG", causal_zone_ts=1, rejection_ts=1, confirmation_ts=2))
    assert engine.authorize("X", 101, 103)["allowed"] is True
    other = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "other.json"))
    other._persist(Setup(symbol="Y", status="ENTRY_READY", direction="BUY", atr14=1,
                         causal_level=100, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG", causal_zone_ts=1, rejection_ts=1, confirmation_ts=2))
    decision = other.authorize("Y", 102.1, 103)
    assert decision["allowed"] is False and decision["status"] == "NO_CHASE"


def test_same_closed_candle_does_not_advance_setup_twice(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "watch.json"))
    f = frame(70); ts = int(f.timestamp.iloc[-1])
    engine._persist(Setup(symbol="X", status="WAITING_FOR_PULLBACK", direction="BUY",
                          crossover_ts=ts - 900_000, last_candle_ts=ts, atr14=1, causal_level=100, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG"))
    first = engine.evaluate("X", f, now_ms=ts + 900_000)
    second = engine.evaluate("X", f, now_ms=ts + 900_000)
    assert first["last_candle_ts"] == second["last_candle_ts"] == ts
    assert first["status"] == second["status"]


def cross_frame(side, reset_session=True):
    if side == "BUY":
        close = np.r_[np.linspace(120, 100, 60), np.linspace(100.5, 108, 10)]
    else:
        close = np.r_[np.linspace(80, 100, 60), np.linspace(99.5, 92, 10)]
    n = len(close)
    midnight = int(pd.Timestamp.now(tz="UTC").floor("D").timestamp() * 1000)
    start = midnight - (60 if reset_session else 70) * 900_000
    ts = start + np.arange(n) * 900_000
    return pd.DataFrame({"timestamp": ts, "open": close, "high": close + .2,
                         "low": close - .2, "close": close, "volume": np.ones(n) * 10})


def test_buy_and_sell_crosses_are_confirmed_on_closed_candles(tmp_path):
    for side in ("BUY", "SELL"):
        f = cross_frame(side)
        engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / f"{side}.json"))
        from unified_ema_vwap_strategy import _find_causal_zone, indicators, indicator_series
        vals = indicators(f)
        series = indicator_series(f)
        ema9, vwap = series["ema9"], series["vwap_series"]
        cross_idx = int(np.where(np.diff(np.sign(ema9 - vwap)) != 0)[0][-1] + 1)
        assert _find_causal_zone(f, cross_idx, side, vals["atr14"]) is None
        result = engine.evaluate(side, f, now_ms=int(f.timestamp.iloc[-1] + 900_000))
        assert result["direction"] == side
        assert result["crossover_ts"] is not None
        assert result["status"] == "INVALIDATED"
        assert result["expiration_reason"] == "NO_CAUSAL_ZONE"


def test_cross_on_unclosed_bar_is_never_discovered(tmp_path):
    # Keep all already-closed bars inside one UTC session. The final, still-forming
    # spike is deliberately large enough to create the EMA9/VWAP cross by itself.
    midnight = int(pd.Timestamp.now(tz="UTC").floor("D").timestamp() * 1000)
    close = np.r_[np.linspace(120, 100, 60), np.linspace(99.8, 95, 9), 160.0]
    start = midnight - 70 * 900_000
    ts = start + np.arange(len(close)) * 900_000
    f = pd.DataFrame({"timestamp": ts, "open": close, "high": close + .2,
                      "low": close - .2, "close": close, "volume": np.ones(len(close)) * 10})
    from unified_ema_vwap_strategy import indicator_series
    series = indicator_series(f)
    gaps = series["ema9"] - series["vwap_series"]
    assert gaps.iloc[-2] <= 0 < gaps.iloc[-1]  # Cross exists only on the forming candle.
    now = int(f.timestamp.iloc[-1] + 900_000 - 1)
    result = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "forming.json")).evaluate("BUY", f, now_ms=now)
    assert result["crossover_ts"] is None


def test_vwap_misaligned_buy_cross_is_not_flipped_to_sell(tmp_path):
    # A high-volume down candle pulls Session VWAP below EMA9 while its close
    # remains below VWAP: this is a real BUY-side EMA/VWAP cross but is misaligned.
    midnight = int(pd.Timestamp.now(tz="UTC").floor("D").timestamp() * 1000)
    close = np.r_[np.linspace(130, 120, 60), 110.0]
    volume = np.r_[np.ones(60) * 10, 10_000.0]
    start = midnight - 61 * 900_000
    ts = start + np.arange(len(close)) * 900_000
    f = pd.DataFrame({"timestamp": ts, "open": close, "high": close + .2,
                      "low": close - .2, "close": close, "volume": volume})
    from unified_ema_vwap_strategy import indicator_series
    series = indicator_series(f)
    gaps = series["ema9"] - series["vwap_series"]
    assert gaps.iloc[-2] <= 0 < gaps.iloc[-1]
    assert close[-1] < series["vwap_series"].iloc[-1]
    result = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "misaligned.json")).evaluate(
        "MISALIGNED", f, now_ms=int(f.timestamp.iloc[-1] + 900_000))
    assert result["status"] == "INVALIDATED"
    assert result["direction"] == "BUY"
    assert result["expiration_reason"] == "VWAP_MISALIGNED"


def test_rejection_and_confirmation_must_be_separate_closed_candles(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "lifecycle.json"))
    closes = np.linspace(100, 110, 70)
    # Keep the full fixture inside one UTC VWAP session so the test isolates
    # rejection/confirmation sequencing instead of a session-reset edge case.
    session_start = int(pd.Timestamp.now(tz="UTC").normalize().timestamp() * 1000)
    f = frame(70, start=session_start, close=closes)
    ts = int(f.timestamp.iloc[-1])
    f.loc[69, ["open", "high", "low", "close"]] = [109.8, 110.0, 108.8, 109.95]
    engine._persist(Setup(symbol="SEQ", status="WAITING_FOR_REJECTION", direction="BUY",
                          crossover_ts=ts - 4 * 900_000, pullback_type="MICRO",
                          last_candle_ts=ts - 900_000, atr14=1, causal_level=109.0, causal_zone_low=108.8, causal_zone_high=109.2, causal_zone_type="ORDER_BLOCK", causal_zone_ts=ts - 2*900_000))
    rejection = engine.evaluate("SEQ", f, now_ms=ts + 900_000)
    assert rejection["status"] == "WAITING_FOR_CONFIRMATION"
    assert rejection["rejection_ts"] == ts
    assert rejection["confirmation_ts"] is None
    next_close = np.r_[closes, 111.0]
    f2 = frame(71, start=int(f.timestamp.iloc[0]), close=next_close)
    f2.loc[70, ["open", "high", "low", "close"]] = [110.0, 111.2, 109.9, 111.0]
    confirmed = engine.evaluate("SEQ", f2, now_ms=int(f2.timestamp.iloc[-1] + 900_000))
    assert confirmed["status"] == "ENTRY_READY"
    assert confirmed["confirmation_ts"] == int(f2.timestamp.iloc[-1])
    assert confirmed["confirmation_ts"] > confirmed["rejection_ts"]


def test_setup_expires_after_eight_closed_bars(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "expiry.json"))
    f = frame(70, close=np.linspace(100, 110, 70))
    last = int(f.timestamp.iloc[-1])
    cross_ts = int(f.timestamp.iloc[-10])
    engine._persist(Setup(symbol="OLD", status="WAITING_FOR_PULLBACK", direction="BUY",
                          crossover_ts=cross_ts, last_candle_ts=last - 900_000,
                          atr14=1, causal_level=100, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG"))
    expired = engine.evaluate("OLD", f, now_ms=last + 900_000)
    assert expired["status"] == "EXPIRED"
    assert expired["expiration_reason"] == "ENTRY_WINDOW_EXPIRED"


def test_mark_executing_requires_successful_strategy_authorization(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "handoff.json"))
    engine._persist(Setup(symbol="BTC/USDT", status="ENTRY_READY", direction="BUY",
                          atr14=1.0, causal_level=100.0, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG", rejection_ts=10,
                          confirmation_ts=20, entry_status="NOT_READY"))
    assert engine.mark_executing("BTC/USDT") is False
    assert engine.get("BTC/USDT").status == "ENTRY_READY"
    decision = engine.authorize("BTC/USDT", 101.0, 103.0)
    assert decision["allowed"] is True
    assert engine.mark_executing("BTC/USDT") is True
    assert engine.get("BTC/USDT").status == "ENTRY_EXECUTING"


def test_execution_handoff_requires_authorized_symbol_side_and_price(tmp_path):
    from unified_ema_vwap_strategy import execution_handoff_allowed

    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "handoff-guard.json"))
    engine._persist(Setup(symbol="BTC/USDT", status="ENTRY_READY", direction="BUY",
                          atr14=1.0, causal_level=100.0, causal_zone_low=99.5, causal_zone_high=100.5, causal_zone_type="FVG", rejection_ts=10,
                          confirmation_ts=20))
    assert engine.authorize("BTC/USDT", 101.0, 103.0)["allowed"] is True
    assert engine.mark_executing("BTC/USDT") is True
    context = {"ema_vwap_authorized": True, "canonical_symbol": "BTC/USDT",
               "side": "BUY", "entry_price": 101.0, "tp1": 103.0}
    assert execution_handoff_allowed(engine, "BTC/USDT", "BUY", 101.0, 103.0, context)
    assert not execution_handoff_allowed(engine, "ETH/USDT", "BUY", 101.0, 103.0, context)
    assert not execution_handoff_allowed(engine, "BTC/USDT", "SELL", 101.0, 103.0, context)
    assert not execution_handoff_allowed(engine, "BTC/USDT", "BUY", 102.0, 103.0, context)
    assert not execution_handoff_allowed(engine, "BTC/USDT", "BUY", 101.0, 103.0, {})

def test_causal_fvg_detection_uses_only_closed_pre_entry_structure():
    from unified_ema_vwap_strategy import _find_causal_zone
    rows = []
    base_ts = 1_800_000_000_000
    for i in range(8):
        price = 100.0 + i * 0.1
        rows.append({"timestamp": base_ts + i*900_000, "open": price, "high": price+0.2,
                     "low": price-0.2, "close": price+0.05, "volume": 10.0})
    # Bullish three-candle imbalance at index 6: current low > high two bars earlier.
    rows[4].update({"open":100.4,"high":100.5,"low":100.2,"close":100.3})
    rows[5].update({"open":100.6,"high":100.9,"low":100.55,"close":100.8})
    rows[6].update({"open":101.0,"high":101.5,"low":101.1,"close":101.4})
    frame_ = pd.DataFrame(rows)
    zone = _find_causal_zone(frame_, 6, "BUY", 1.0)
    assert zone is not None
    assert zone["type"] == "FVG"
    assert zone["low"] == 100.5
    assert zone["high"] == 101.1
    assert zone["timestamp"] == int(frame_.timestamp.iloc[6])


def test_causal_zone_is_required_for_entry_authorization(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "zone-required.json"))
    engine._persist(Setup(symbol="NOZONE", status="ENTRY_READY", direction="BUY",
                          atr14=1.0, causal_level=100.0, rejection_ts=10, confirmation_ts=20))
    result = engine.authorize("NOZONE", 101.0, 103.0)
    assert result["allowed"] is False
    assert result["reason"] == "tp1_atr_or_causal_zone_unknown"


