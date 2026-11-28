"""Acceptance tests for EMA9/Session-VWAP cross -> Watchlist -> entry lifecycle.

All exchange calls are mocked. PAPER mode is forced by conftest; no live order
is submitted and no BingX connection is made.
"""
from __future__ import annotations

import math
import time
from dataclasses import FrozenInstanceError

import numpy as np
import pandas as pd
import pytest

import ggp_cross_event_pipeline as pipeline
import ggpbot


SYMBOL = "BTC/USDT:USDT"
NOW_MS = int(time.time() * 1000)
CANDLE_TS = NOW_MS - 900_000


def _event(side="BUY", *, adx=22.0, ts=CANDLE_TS, ema9=101.0, vwap=100.0):
    event = pipeline.build_cross_event(
        symbol=SYMBOL, side=side, candle_timestamp=ts, ema9=ema9, vwap=vwap,
        previous_ema9=99.0 if side == "BUY" else 101.0,
        previous_vwap=100.0, adx=adx, cross_status="CONFIRMED",
        data_freshness="FRESH", now_ms=NOW_MS,
    )
    assert event is not None
    return event


def _able_zone_row(side="BUY", *, adx=22.0):
    if side == "BUY":
        zone = {"low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
                "strength": 6, "direction": "BULLISH",
                "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH"}
        stop = 98.8
    else:
        zone = {"low": 100.5, "high": 101.0, "timestamp": CANDLE_TS - 900_000,
                "strength": 6, "direction": "BEARISH",
                "zone_definition": "BEARISH_OPEN_TO_HIGH", "status": "FRESH"}
        stop = 101.2
    return {
        "symbol": SYMBOL, "exchange_symbol": SYMBOL, "side": side,
        "cross_status": "CONFIRMED", "cross_direction": "BULLISH" if side == "BUY" else "BEARISH",
        "cross_candle_timestamp": CANDLE_TS, "adx": adx, "ema9": 101.0 if side == "BUY" else 99.0,
        "vwap": 100.0, "price": 100.0, "atr": 1.0, "structural_stop": stop,
        "data_quality": "VALID_CLOSED_CANDLES", "data_freshness": "FRESH",
        "trading_mode": "SCALP", "order_block": zone, "fvg": {},
        "support_resistance": {"supports": [], "resistances": []},
        "decision_state": "WAITING_FOR_CONFIRMATION",
        "closed_mss_pending": True, "no_fresh_nearby_order_block_or_fvg": False,
    }


def _authorize(event, row, store, **overrides):
    zone = pipeline.evaluate_cross_zone(row, now_ms=NOW_MS)
    stop = pipeline.evaluate_stop_loss(row, zone)
    kwargs = dict(
        event=event, analysis_row=row, zone=zone, stop_loss=stop,
        market_state={"position_state": "FLAT"}, risk_state={"allowed": True},
        store=store, now_ms=NOW_MS,
    )
    kwargs.update(overrides)
    return pipeline.evaluate_cross_authorization(**kwargs), zone, stop


@pytest.fixture(autouse=True)
def _safe_account_state(monkeypatch):
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(ggpbot, "get_account_balance_snapshot",
                        lambda: (1000.0, "TEST_ACCOUNT_BALANCE", time.time()))


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "cross_events.sqlite3"
    monkeypatch.setattr(pipeline, "_STORE", pipeline.CrossEventStore(str(path)))
    return pipeline._STORE


# --------------------------------------------------------------------------
# Strategy validation and event integrity
# --------------------------------------------------------------------------

def test_confirmed_buy_and_sell_crossovers_create_unique_immutable_events():
    buy = _event("BUY")
    sell = _event("SELL")
    assert buy.event_id == f"EMA9VWAP:BTC:BULLISH:{CANDLE_TS}"
    assert sell.event_id == f"EMA9VWAP:BTC:BEARISH:{CANDLE_TS}"
    assert buy.side == "BUY" and sell.side == "SELL"
    assert buy.closed_confirmed and sell.closed_confirmed
    with pytest.raises(FrozenInstanceError):
        buy.side = "SELL"


def test_approaching_ema50_and_unclosed_candles_never_create_events():
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        adx=22.0, cross_status="APPROACHING", now_ms=NOW_MS,
    ) is None
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=CANDLE_TS,
    ) is None
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=NOW_MS,
    ).cross_direction == "BULLISH"


def test_adx_below_18_blocks_entry_and_adx_18_passes(store):
    weak = _event(adx=17.99)
    row = _able_zone_row(adx=17.99)
    decision, _, _ = _authorize(weak, row, store)
    assert decision["authorized"] is False
    assert decision["rejection_reason"].startswith("adx_at_least_18:")

    strong = _event(adx=18.0)
    row = _able_zone_row(adx=18.0)
    decision, _, _ = _authorize(strong, row, store)
    assert decision["authorized"] is True
    assert decision["final_authorization"] == "ENTRY_AUTHORIZED"


def test_optional_smc_conditions_do_not_block_valid_crossover_zone(store):
    event = _event()
    row = _able_zone_row()
    decision, zone, stop = _authorize(event, row, store)
    assert zone["valid"] is True
    assert stop["valid"] is True
    assert decision["authorized"] is True
    assert decision["watchlist_status"] == "READY"
    assert decision["watchlist_icon"] == "🟢"
    assert all(pipeline.is_universal_blocker(k) is False
               for k in ("closed_mss_pending", "post_mss_retest_pending",
                         "no_fresh_nearby_order_block_or_fvg"))
    assert pipeline.is_universal_blocker("adx_below_18") is True
    assert len(decision["mandatory_gates"]) == 10
    assert "adx_at_least_18" in decision["mandatory_gates"]
    assert "closed_mss_pending" not in decision["mandatory_gates"]


def test_invalid_zone_and_missing_stop_loss_block_entry(store):
    event = _event()
    row = _able_zone_row()
    row["order_block"] = {"low": 80.0, "high": 80.5, "timestamp": CANDLE_TS - 900_000,
                          "strength": 6, "direction": "BULLISH"}
    row["structural_stop"] = None
    decision, zone, stop = _authorize(event, row, store)
    assert zone["valid"] is False
    assert stop["valid"] is False
    assert stop["status"] == "MISSING"
    assert decision["authorized"] is False


def test_valid_scalp_and_trend_zones_qualify_without_strong_regime(store):
    for mode in ("SCALP", "TREND"):
        row = _able_zone_row()
        row["trading_mode"] = mode
        row["market_regime"] = "UNKNOWN"
        decision, _, _ = _authorize(_event(), row, store)
        assert decision["authorized"] is True
        assert decision["trade_mode"] == mode


def test_opposite_institutional_hypothesis_cannot_overwrite_crossover_direction(store):
    event = _event("BUY")
    row = _able_zone_row("SELL")
    row["side"] = "SELL"
    decision, _, _ = _authorize(event, row, store)
    assert decision["authorized"] is False
    assert "direction_preserved" in decision["rejection_reason"]


def test_duplicate_consumed_event_cannot_authorize_twice(store):
    event = _event()
    store.upsert(event)
    decision, _, _ = _authorize(event, _able_zone_row(), store)
    assert decision["authorized"] is True
    store.consume(event.event_id, "trade-1")
    decision, _, _ = _authorize(event, _able_zone_row(), store)
    assert decision["authorized"] is False
    assert "duplicate_event" in decision["rejection_reason"]
    assert store.consume(event.event_id, "trade-2") is False


def test_persisted_event_survives_later_candle_without_cross(store):
    event = _event()
    watchlist = {}
    pipeline.ingest_cross_event(event, watchlist, store)
    key = SYMBOL
    assert watchlist[key]["watchlist_status"] == "CROSS_CONFIRMED"
    assert watchlist[key]["watchlist_icon"] == "🔵"
    merged = pipeline.merge_cross_fields(watchlist[key], {"cross_status": "NONE", "price": 101.0})
    assert merged["cross_event_id"] == event.event_id
    assert merged["cross_status"] == "CONFIRMED"
    assert merged["price"] == 101.0


def test_repeated_scanner_cycles_do_not_duplicate_events(store):
    event = _event()
    watchlist = {}
    pipeline.ingest_cross_event(event, watchlist, store)
    pipeline.ingest_cross_event(event, watchlist, store)
    assert len(store.all()) == 1
    assert len(watchlist) == 1


def test_newer_event_is_not_blocked_by_an_older_expired_event(store):
    old = _event(ts=CANDLE_TS - 3 * 900_000)
    store.upsert(old)
    store.mark_status(old.event_id, "EXPIRED", "event_window_expired")
    store.expire_stale(now_ms=NOW_MS)
    new = _event()
    decision, _, _ = _authorize(new, _able_zone_row(), store)
    assert decision["authorized"] is True
    assert decision["event_id"] == new.event_id


def test_sync_builds_watchlist_lifecycle_and_decision_record(store, monkeypatch):
    event = _event()
    row = _able_zone_row()
    row.update({
        "cross_event_id": event.event_id, "cross_status": "CONFIRMED",
        "cross_direction": event.cross_direction, "cross_candle_timestamp": event.candle_timestamp,
        "ema9": event.ema9, "vwap": event.vwap, "adx": event.adx,
    })
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {SYMBOL: row}, "cross_events": {}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", False)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    authorized = ggpbot._sync_cross_events_from_watchlist(now_ms=NOW_MS)
    assert authorized == 1
    synced = ggpbot.MEMORY["watchlist"][SYMBOL]
    assert synced["watchlist_status"] == "READY"
    assert synced["watchlist_icon"] == "🟢"
    assert synced["entry_authorized"] is True
    assert synced["decision_record"]["final_authorization"] == "ENTRY_AUTHORIZED"
    assert synced["decision_record"]["event_id"] == event.event_id
    assert synced["decision_state"] == "READY"
    assert synced["entry_readiness"] == "READY"
    assert ggpbot.MEMORY["cross_events"][event.event_id]["side"] == "BUY"


def test_unauthorized_cross_never_displays_entry_ready(store, monkeypatch):
    row = _able_zone_row(adx=17.5)
    row["adx"] = 17.5
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {SYMBOL: row}, "cross_events": {}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", False)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    assert ggpbot._sync_cross_events_from_watchlist(now_ms=NOW_MS) == 0
    synced = ggpbot.MEMORY["watchlist"][SYMBOL]
    assert synced["watchlist_status"] == "REJECTED"
    assert synced["watchlist_icon"] == "🔴"
    assert synced["decision_state"] != "ENTRY_READY"
    assert synced["entry_readiness"] != "ENTRY_READY"
    assert synced["entry_authorized"] is False
    assert synced["authorization_reason"].startswith("adx_at_least_18:")


def test_unavailable_account_balance_blocks_ready(store, monkeypatch):
    row = _able_zone_row()
    monkeypatch.setattr(ggpbot, "get_account_balance_snapshot", lambda: (0.0, "UNAVAILABLE", time.time()))
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {SYMBOL: row}, "cross_events": {}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", False)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    assert ggpbot._sync_cross_events_from_watchlist(now_ms=NOW_MS) == 0
    synced = ggpbot.MEMORY["watchlist"][SYMBOL]
    assert synced["entry_authorized"] is False
    assert synced["watchlist_status"] == "REJECTED"
    assert synced["authorization_reason"] == "risk_and_exchange_constraints_ok:account_balance_unavailable"


def test_approaching_cross_gets_yellow_watchlist_icon(store, monkeypatch):
    row = {"symbol": SYMBOL, "exchange_symbol": SYMBOL, "side": "BUY",
           "cross_status": "APPROACHING", "decision_state": "WAITING_FOR_CONFIRMATION"}
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {SYMBOL: row}, "cross_events": {}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    assert ggpbot._sync_cross_events_from_watchlist(now_ms=NOW_MS) == 0
    assert row["watchlist_status"] == "CROSS_APPROACHING"
    assert row["watchlist_icon"] == "🟡"
    assert row["entry_authorized"] is False


def test_confirmed_cross_identity_survives_institutional_row_refresh(store):
    event = _event()
    watchlist = {}
    pipeline.ingest_cross_event(event, watchlist, store)
    watchlist[SYMBOL].update({"watchlist_status": "READY", "decision_state": "READY",
                              "entry_authorized": True, "authorization_reason": "all_mandatory_gates_passed"})
    refreshed = pipeline.merge_cross_fields(watchlist[SYMBOL], {
        "cross_status": "NONE", "decision_state": "ENTRY_READY", "price": 101.0,
    })
    assert refreshed["cross_event_id"] == event.event_id
    assert refreshed["cross_status"] == "CONFIRMED"
    assert refreshed["watchlist_status"] == "READY"
    assert refreshed["decision_state"] == "READY"
    assert refreshed["entry_authorized"] is True
    assert refreshed["price"] == 101.0


# --------------------------------------------------------------------------
# Authorization -> real execute_entry handoff (mocked exchange)
# --------------------------------------------------------------------------

def _frame(n=100):
    t = np.arange(n)
    close = 100.0 + np.sin(t / 7.0) * 0.35 + t * 0.002
    start = 1_700_000_000_000
    return pd.DataFrame({
        "timestamp": start + t * 900_000,
        "open": close - 0.05, "high": close + 0.25, "low": close - 0.25,
        "close": close, "volume": np.full(n, 1000.0),
    })


def test_ready_cross_event_calls_execute_entry_once_and_consumes_event(tmp_path, monkeypatch):
    store_path = tmp_path / "cross.sqlite3"
    monkeypatch.setattr(pipeline, "_STORE", pipeline.CrossEventStore(str(store_path)))
    store = pipeline.get_cross_event_store()

    event = _event()
    row = _able_zone_row()
    ready, _, _ = _authorize(event, row, store)
    assert ready["authorized"] is True
    store.upsert(event)
    row.update({
        "cross_event_id": event.event_id, "cross_status": "CONFIRMED",
        "cross_direction": event.cross_direction, "cross_candle_timestamp": event.candle_timestamp,
        "ema9": event.ema9, "vwap": event.vwap, "adx": event.adx,
        "watchlist_status": "READY", "watchlist_icon": "🟢", "entry_authorized": True,
    })

    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", False)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    monkeypatch.setattr(ggpbot, "MEMORY", {
        "watchlist": {SYMBOL: row}, "cross_events": {}, "position_status": "FLAT",
    })
    monkeypatch.setattr(ggpbot, "_validated_scanner_symbol", lambda _: SYMBOL)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda _: _frame())
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda _: 100.0)
    monkeypatch.setattr(ggpbot, "validate_dataframe", lambda *_a, **_k: True)
    monkeypatch.setattr(ggpbot, "compute_atr", lambda _: pd.Series([1.0]))
    monkeypatch.setattr(ggpbot, "compute_sl_tp", lambda *_a, **_k: (None, 100.5, 101.0))
    monkeypatch.setattr(ggpbot, "is_strong_candidate", lambda _: False)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)

    calls = []
    monkeypatch.setattr(ggpbot, "execute_entry", lambda *a, **k: calls.append((a, k)) or True)
    assert ggpbot._try_cross_event_entries() is True
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[0] == "BUY" and args[1] == SYMBOL
    assert math.isclose(float(args[2]), 100.0)
    assert math.isclose(float(args[3]), 98.8)
    assert kwargs["entry_type"] == "EMA9_VWAP_CROSS"
    assert ggpbot.MEMORY["watchlist"][SYMBOL]["watchlist_status"] == "POSITION_OPEN"
    assert store.is_consumed(event.event_id) is True

    calls.clear()
    ggpbot.MEMORY["watchlist"][SYMBOL]["entry_authorized"] = True
    assert ggpbot._try_cross_event_entries() is False
    assert calls == []


def test_execute_entry_rejects_consumed_or_unauthorized_cross_context(tmp_path, monkeypatch):
    event = _event()
    store = pipeline.CrossEventStore(str(tmp_path / "guard.sqlite3"))
    store.upsert(event)
    store.consume(event.event_id, "trade-1")
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(
        ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT",
        {"entry_source": "EMA9_VWAP_CROSS_AUTHORIZED", "cross_event": event.to_dict(),
         "cross_authorization": {"authorized": True}},
        raising=False,
    )
    assert ggpbot._original_execute_entry_for_journal(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 99.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is False

    monkeypatch.setattr(
        ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT",
        {"entry_source": "EMA9_VWAP_CROSS_AUTHORIZED", "cross_event": event.to_dict(),
         "cross_authorization": {"authorized": False}},
        raising=False,
    )
    assert ggpbot._original_execute_entry_for_journal(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 99.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is False
