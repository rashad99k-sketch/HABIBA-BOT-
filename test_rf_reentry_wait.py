"""Deterministic RF opposite-signal re-entry wait coverage.

Every block is backed by the durable trade journal. RF is evaluated only on
closed candles, external closes are never promoted to RF closes, consumed
signals are never reused, and durable read failures fail closed.
"""
from __future__ import annotations

import inspect
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

os.environ.setdefault("PAPER_MODE", "true")
os.environ.setdefault("BINGX_API_KEY", "")
os.environ.setdefault("BINGX_API_SECRET", "")

import ggpbot
from roro_trade_journal import TradeJournal


SYMBOL = "BTC/USDT:USDT"
TF_MS = 900_000
RF_TS = 1_795_132_800_000  # closed-candle open timestamp (ms)
RF_SIGNAL = "SELL"


def _journal(tmp_path, name="rf-reentry.sqlite3"):
    return TradeJournal(str(Path(tmp_path) / name))


def _frame(timestamps):
    rows = []
    for index, ts in enumerate(timestamps):
        price = 100.0 + index * 0.01
        rows.append({
            "timestamp": int(ts), "open": price - 0.02, "high": price + 0.05,
            "low": price - 0.05, "close": price, "volume": 1000.0 + index,
        })
    return pd.DataFrame(rows)


def _closed_timestamps(count=40):
    return [RF_TS + i * TF_MS for i in range(count)]


def _gate(signal_ts, *, ready=True, event_id=""):
    return {
        "ready": ready,
        "signal_candle_timestamp": int(signal_ts),
        "ema9_cross_timestamp": int(signal_ts),
        "latest_closed_timestamp": int(signal_ts),
        "durable_event_id": event_id,
        "reason": "TEST_CLOSED_CANDLE_SIGNAL",
    }


def _rf_context(*, signal_ts=RF_TS, close_ts=None):
    return {
        "symbol": SYMBOL, "position_side": "BUY", "rf_signal": RF_SIGNAL,
        "rf_signal_candle_timestamp": int(signal_ts),
        "closed_candle_timestamp": int(signal_ts),
        "close_ts": float(close_ts if close_ts is not None else time.time()),
        "source": ggpbot.RF_REENTRY_CLOSED_SOURCE,
        "trade_id": "T-RF",
    }


@pytest.fixture
def clean_state():
    """Isolate the module-level live STATE across these scenarios."""
    template = dict(ggpbot.STATE)
    for key in template:
        ggpbot.STATE[key] = None
    for key in ("open", "recovery_hold", "execution_unresolved", "close_reconciliation_required",
                "close_verified", "native_sl_verified", "tp1_hit", "tp2_hit", "be_done",
                "trail_activated", "partial_closed", "recovery_block_entries"):
        ggpbot.STATE[key] = False
    yield
    ggpbot.STATE.clear()
    ggpbot.STATE.update(template)


class _ScriptedRFEngine:
    """Deterministic RF signal without depending on indicator tuning."""

    def __init__(self, *args, **kwargs):
        pass

    def compute(self, frame):
        last_ts = int(frame["timestamp"].iloc[-1])
        return {
            "signal": RF_SIGNAL, "triggered": True, "distance": 0.01,
            "filt": 100.0, "h_band": 101.0, "l_band": 99.0,
            "evaluated_last_timestamp": last_ts,
        }


def test_rf_opposite_close_writes_durable_block(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "RFEngine", _ScriptedRFEngine)
    monkeypatch.setattr(ggpbot, "tg_close", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "update_stats", lambda *a, **k: None)

    def _verified_close(symbol, qty=None, *, close_reason=""):
        assert close_reason == ggpbot.RF_OPPOSITE_CLOSE_REASON
        ggpbot.STATE["close_verified"] = True
        ggpbot.STATE["last_close_reason"] = close_reason
        ggpbot.STATE["last_close_fill_price"] = 99.0
        return True

    monkeypatch.setattr(ggpbot, "safe_close_position", _verified_close)
    journal.start_trade("T-RF", SYMBOL, "BUY", "TEST", 1.0, 100.0, 95.0)
    ggpbot.STATE.update({
        "open": True, "side": "BUY", "current_symbol": SYMBOL, "trade_id": "T-RF",
        "entry": 100.0, "initial_qty": 1.0, "qty": 1.0,
        "last_close_pnl_source": "PAPER_MARK_ESTIMATE", "last_close_pnl_complete": True,
        "last_close_realized_pnl": -1.0, "close_verified": False,
    })
    closed = _closed_timestamps()

    assert ggpbot.close_position_on_opposite_rf(
        SYMBOL, df=_frame(closed), now_ms=closed[-1] + TF_MS,
    ) is True

    block = journal.get_active_rf_reentry_block(SYMBOL, "BUY")
    assert block is not None
    assert block["direction"] == "BUY"
    assert block["rf_signal_timestamp"] == closed[-1]
    assert block["close_ts"] > 0
    assert block["metadata"]["rf_signal"] == RF_SIGNAL
    assert block["metadata"]["closed_candle_timestamp"] == closed[-1]
    assert journal.get_active_rf_reentry_block(SYMBOL, "SELL") is None
    assert ggpbot.STATE.get("rf_opposite_close") is None


def test_older_and_same_signal_candles_stay_blocked(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    journal.record_rf_reentry_block(
        symbol=SYMBOL, direction="BUY", rf_signal_timestamp=RF_TS, close_ts=time.time(),
    )

    for candidate_ts in (RF_TS - TF_MS, RF_TS):
        allowed, reason = ggpbot._rf_reentry_block_permission(
            SYMBOL, "BUY", _gate(candidate_ts))
        assert allowed is False
        assert "rf_wait_signal_not_later" in reason

    # The blocked direction is directional; the RF signal side is not held back.
    allowed, reason = ggpbot._rf_reentry_block_permission(
        SYMBOL, "SELL", _gate(RF_TS - TF_MS))
    assert allowed is True
    assert reason == "no_active_rf_reentry_block"


def test_delayed_entry_uses_original_cross_signal_timestamp(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    newer_rf_ts = RF_TS + 2 * TF_MS
    journal.record_rf_reentry_block(
        symbol=SYMBOL, direction="BUY", rf_signal_timestamp=newer_rf_ts, close_ts=time.time(),
    )
    journal.record_cross_event(
        event_id="cross-original", symbol=SYMBOL, exchange_symbol=SYMBOL, side="BUY",
        direction="BULLISH", source=ggpbot.CONFIRMED_CROSS_EVENT_SOURCE,
        timeframe=str(ggpbot.INTERVAL), signal_candle_timestamp=RF_TS,
        closed_candle_timestamp=RF_TS, expires_ts=time.time() + 3600.0,
    )
    # A delayed breakout gate carries a later timestamp, but the durable cross
    # event still points at the original (older) signal candle.
    gate = _gate(newer_rf_ts + TF_MS, event_id="cross-original")

    assert ggpbot._rf_reentry_signal_timestamp(gate, "cross-original") == RF_TS
    allowed, reason = ggpbot._rf_reentry_block_permission(
        SYMBOL, "BUY", gate, cross_event_id="cross-original")
    assert allowed is False
    assert "rf_wait_signal_not_later" in reason


def test_strictly_later_signal_unblocks_and_releases_after_fill(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    journal.record_rf_reentry_block(
        symbol=SYMBOL, direction="BUY", rf_signal_timestamp=RF_TS, close_ts=time.time(),
    )
    later_ts = RF_TS + TF_MS
    gate = _gate(later_ts)
    gate["rf_reentry_signal_timestamp"] = later_ts

    allowed, reason = ggpbot._rf_reentry_block_permission(SYMBOL, "BUY", gate)
    assert allowed is True
    assert reason == "rf_wait_satisfied_by_later_closed_signal"

    assert ggpbot._release_rf_reentry_block_after_fill(journal, SYMBOL, "BUY", gate) == 1
    assert journal.get_active_rf_reentry_block(SYMBOL, "BUY") is None
    restarted = TradeJournal(journal.path)
    assert restarted.get_active_rf_reentry_block(SYMBOL, "BUY") is None


def test_restart_preserves_rf_reentry_block(tmp_path, monkeypatch, clean_state):
    path = str(Path(tmp_path) / "rf-reentry.sqlite3")
    first = TradeJournal(path)
    first.record_rf_reentry_block(
        symbol=SYMBOL, direction="BUY", rf_signal_timestamp=RF_TS, close_ts=123.5,
        metadata={"rf_signal": RF_SIGNAL},
    )
    restarted = TradeJournal(path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", restarted)

    block = restarted.get_active_rf_reentry_block(SYMBOL, "BUY")
    assert block["rf_signal_timestamp"] == RF_TS
    assert block["close_ts"] == pytest.approx(123.5)
    assert block["metadata"]["rf_signal"] == RF_SIGNAL
    allowed, reason = ggpbot._rf_reentry_block_permission(SYMBOL, "BUY", _gate(RF_TS))
    assert allowed is False
    assert "rf_wait_signal_not_later" in reason


def test_external_close_is_not_relabelled_as_rf_close(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", True)
    monkeypatch.setattr(ggpbot, "_fetch_external_close_diagnostic", lambda *a, **k: {
        "reason": "EXTERNAL_CLOSE_UNKNOWN", "confidence": "UNKNOWN", "close_price": 99.0,
        "realized_pnl": -1.0, "filled_qty": 1.0, "order_id": None, "evidence": [],
    })
    monkeypatch.setattr(ggpbot, "record_trade_management_event", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "clear_position_dashboard", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "tg_sl_hit", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "send_once", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "_live_manager", SimpleNamespace(lifecycle_state=None))
    finalized = []
    monkeypatch.setattr(
        ggpbot, "finalize_trade_with_reality",
        lambda symbol, close_reason=None: finalized.append(close_reason),
    )
    ggpbot.STATE.update({
        "open": True, "side": "BUY", "current_symbol": SYMBOL, "trade_id": "T-EXT",
        "rf_opposite_close": _rf_context(),
    })

    assert ggpbot._clear_confirmed_flat_unlocked(SYMBOL, "EXCHANGE_FLAT_CONFIRMED") is True
    assert finalized == ["EXTERNAL_CLOSE_UNKNOWN"]
    assert journal.get_active_rf_reentry_block(SYMBOL, "BUY") is None
    assert ggpbot.STATE.get("rf_opposite_close") is None


def test_consumed_signal_is_never_reused_or_released(tmp_path, monkeypatch, clean_state):
    journal = _journal(tmp_path)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    journal.record_rf_reentry_block(
        symbol=SYMBOL, direction="BUY", rf_signal_timestamp=RF_TS, close_ts=time.time(),
    )
    later_ts = RF_TS + TF_MS
    event_id = f"BUY:{later_ts}"

    assert journal.consume_event(event_id, "trade-1") is True
    assert journal.consume_event(event_id, "trade-2") is False
    gate = _gate(later_ts, event_id=event_id)
    gate["rf_reentry_signal_timestamp"] = later_ts

    # A later candle alone satisfies the wait, but consumption still blocks reuse
    # and therefore no verified fill exists that could release the durable block.
    allowed, reason = ggpbot._rf_reentry_block_permission(SYMBOL, "BUY", gate)
    assert allowed is True
    assert reason == "rf_wait_satisfied_by_later_closed_signal"
    assert journal.is_event_consumed(event_id) is True
    released = 0 if journal.is_event_consumed(event_id) else ggpbot._release_rf_reentry_block_after_fill(
        journal, SYMBOL, "BUY", gate)
    assert released == 0

    restarted = TradeJournal(journal.path)
    assert restarted.is_event_consumed(event_id) is True
    assert restarted.get_active_rf_reentry_block(SYMBOL, "BUY") is not None

    wrapper_source = inspect.getsource(ggpbot.execute_entry)
    assert wrapper_source.index("not journal.consume_event(event_id, trade_id)") < wrapper_source.index(
        "_release_rf_reentry_block_after_fill(")


def test_rf_is_computed_only_from_closed_candles(tmp_path, monkeypatch, clean_state):
    closed = _closed_timestamps()
    open_ts = closed[-1] + TF_MS

    class _OpenCandleAwareRFEngine(_ScriptedRFEngine):
        def compute(self, frame):
            last_ts = int(frame["timestamp"].iloc[-1])
            # Any leaked open candle has a timestamp beyond the last closed bar.
            signal = RF_SIGNAL if last_ts == closed[-1] else "BUY"
            return {**super().compute(frame), "signal": signal}

    monkeypatch.setattr(ggpbot, "RFEngine", _OpenCandleAwareRFEngine)

    rf = ggpbot._rf_signal_from_closed_candles(
        SYMBOL, df=_frame(closed + [open_ts]), timeframe="15m", now_ms=open_ts + 1,
    )
    assert rf is not None
    assert rf["signal"] == RF_SIGNAL
    assert rf["signal_candle_timestamp"] == closed[-1]
    assert rf["source"] == ggpbot.RF_REENTRY_CLOSED_SOURCE


def test_unreadable_rf_durable_state_fails_closed(tmp_path, monkeypatch, clean_state):
    class _BrokenJournal:
        def get_active_rf_reentry_block(self, *args, **kwargs):
            raise RuntimeError("simulated unreadable RF state")

    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", _BrokenJournal())
    allowed, reason = ggpbot._rf_reentry_block_permission(
        SYMBOL, "BUY", _gate(RF_TS + TF_MS))
    assert allowed is False
    assert "rf_reentry_state_unreadable" in reason

    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    ggpbot.STATE["rf_opposite_close"] = _rf_context()
    assert ggpbot._persist_rf_reentry_block_from_close(
        SYMBOL, ggpbot.RF_OPPOSITE_CLOSE_REASON, ggpbot.STATE) is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
