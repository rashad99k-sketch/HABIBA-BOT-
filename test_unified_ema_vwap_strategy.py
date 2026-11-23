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
                           causal_level=100, rejection_ts=1, confirmation_ts=2))
    assert engine.authorize("X", 101, 103)["allowed"] is True
    other = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "other.json"))
    other._persist(Setup(symbol="Y", status="ENTRY_READY", direction="BUY", atr14=1,
                         causal_level=100, rejection_ts=1, confirmation_ts=2))
    decision = other.authorize("Y", 102, 103)
    assert decision["allowed"] is False and decision["status"] == "NO_CHASE"


def test_same_closed_candle_does_not_advance_setup_twice(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "watch.json"))
    f = frame(70); ts = int(f.timestamp.iloc[-1])
    engine._persist(Setup(symbol="X", status="WAITING_FOR_PULLBACK", direction="BUY",
                          crossover_ts=ts - 900_000, last_candle_ts=ts, atr14=1, causal_level=100))
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
        result = engine.evaluate(side, f, now_ms=int(f.timestamp.iloc[-1] + 900_000))
        assert result["direction"] == side
        assert result["status"] in {"WAITING_FOR_PULLBACK", "WAITING_FOR_REJECTION"}
        assert result["crossover_ts"] is not None


def test_cross_on_unclosed_bar_is_never_discovered(tmp_path):
    f = cross_frame("BUY")
    close = np.r_[np.linspace(120, 100, 60), np.linspace(99.8, 95, 9), 120.0]
    f["open"] = close; f["close"] = close; f["high"] = close + .2; f["low"] = close - .2
    # The only EMA cross is on the latest bar, which remains unclosed at now_ms.
    now = int(f.timestamp.iloc[-1] + 900_000 - 1)
    result = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "forming.json")).evaluate("BUY", f, now_ms=now)
    assert result["crossover_ts"] is None


def test_vwap_misaligned_buy_cross_is_not_flipped_to_sell(tmp_path):
    f = cross_frame("BUY", reset_session=False)
    result = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "misaligned.json")).evaluate(
        "MISALIGNED", f, now_ms=int(f.timestamp.iloc[-1] + 900_000))
    assert result["status"] == "INVALIDATED"
    assert result["direction"] == "BUY"
    assert result["expiration_reason"] == "VWAP_MISALIGNED"


def test_rejection_and_confirmation_must_be_separate_closed_candles(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "lifecycle.json"))
    closes = np.linspace(100, 110, 70)
    f = frame(70, close=closes)
    ts = int(f.timestamp.iloc[-1])
    f.loc[69, ["open", "high", "low", "close"]] = [109.8, 110.0, 108.8, 109.95]
    engine._persist(Setup(symbol="SEQ", status="WAITING_FOR_REJECTION", direction="BUY",
                          crossover_ts=ts - 4 * 900_000, pullback_type="MICRO",
                          last_candle_ts=ts - 900_000, atr14=1, causal_level=109.0))
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
                          atr14=1, causal_level=100))
    expired = engine.evaluate("OLD", f, now_ms=last + 900_000)
    assert expired["status"] == "EXPIRED"
    assert expired["expiration_reason"] == "ENTRY_WINDOW_EXPIRED"


def test_mark_executing_requires_successful_strategy_authorization(tmp_path):
    engine = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "handoff.json"))
    engine._persist(Setup(symbol="BTC/USDT", status="ENTRY_READY", direction="BUY",
                          atr14=1.0, causal_level=100.0, rejection_ts=10,
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
                          atr14=1.0, causal_level=100.0, rejection_ts=10,
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
