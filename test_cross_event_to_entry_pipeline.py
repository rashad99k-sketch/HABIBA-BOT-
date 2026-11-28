"""Acceptance tests for EMA9/Session-VWAP cross -> Watchlist -> entry lifecycle.

All exchange calls are mocked. PAPER mode is forced by conftest; no live order
is submitted and no BingX connection is made.
"""
from __future__ import annotations

import math
import time
from copy import deepcopy
from dataclasses import FrozenInstanceError

import numpy as np
import pandas as pd
import pytest

import ggp_cross_event_pipeline as pipeline
import ggpbot


SYMBOL = "BTC/USDT:USDT"
NOW_MS = int(time.time() * 1000)
CANDLE_TS = NOW_MS - 900_000


def _event(side="BUY", *, adx=22.0, ts=CANDLE_TS, ema9=None, vwap=100.0):
    buy = str(side).upper() == "BUY"
    ema9 = (101.0 if buy else 99.0) if ema9 is None else ema9
    event = pipeline.build_cross_event(
        symbol=SYMBOL, side=side, candle_timestamp=ts, ema9=ema9, vwap=vwap,
        previous_ema9=99.0 if buy else 101.0,
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
        "indicator_snapshot": {
            "ema9": 101.0 if side == "BUY" else 99.0, "vwap": 100.0,
            "previous_ema9": 99.0 if side == "BUY" else 101.0, "previous_vwap": 100.0,
            "latest_closed_timestamp": CANDLE_TS, "timeframe_seconds": 900,
            "quality": "FRESH", "cross_status": "CONFIRMED",
            "cross_direction": "BULLISH" if side == "BUY" else "BEARISH",
            "cross_candle_timestamp": CANDLE_TS,
        },
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
        previous_ema9=99.0, previous_vwap=100.0,
        adx=22.0, cross_status="APPROACHING", now_ms=NOW_MS,
    ) is None
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        previous_ema9=99.0, previous_vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=CANDLE_TS,
    ) is None
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        previous_ema9=99.0, previous_vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=NOW_MS,
    ).cross_direction == "BULLISH"
    # A CONFIRMED label without the previous closed-candle proof is never enough.
    assert pipeline.build_cross_event(
        symbol=SYMBOL, side="BUY", candle_timestamp=CANDLE_TS, ema9=101.0, vwap=100.0,
        adx=22.0, cross_status="CONFIRMED", now_ms=NOW_MS,
    ) is None


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
    assert len(decision["mandatory_gates"]) == 11
    assert "adx_at_least_18" in decision["mandatory_gates"]
    assert "cross_math_proof" in decision["mandatory_gates"]
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


def test_buy_bullish_zone_is_eligible_and_producer_metadata_is_preserved():
    row = _able_zone_row("BUY")
    ob = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
        "impulse_timestamp": CANDLE_TS - 450_000, "strength": 6,
        "direction": "BULLISH", "zone_definition": "BULLISH_LOW_TO_OPEN",
        "status": "FRESH", "source": "CAUSAL_ORDER_BLOCK", "fill_pct": 0.0,
    }
    row["order_block"] = ob
    before = deepcopy(ob)
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is True
    selected = zone["selected_zone"]
    assert selected["direction"] == "BULLISH"
    assert selected["direction_status"] == pipeline._ZONE_DIRECTION_SOURCE
    assert selected["source_direction"] == "BULLISH"
    assert selected["source"] == "CAUSAL_ORDER_BLOCK"
    assert selected["impulse_timestamp"] == ob["impulse_timestamp"]
    assert selected["available_timestamp"] == ob["impulse_timestamp"]
    assert row["order_block"] == before


def test_buy_bearish_zone_is_rejected_without_direction_flip():
    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
        "strength": 6, "direction": "BEARISH",
        "zone_definition": "BEARISH_OPEN_TO_HIGH", "status": "FRESH",
    }
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is False
    assert zone["reason"] == "zone_direction_mismatch"
    assert zone["zones"][0]["direction"] == "BEARISH"


def test_sell_bearish_zone_is_eligible_and_bullish_zone_is_rejected():
    row = _able_zone_row("SELL")
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is True
    assert zone["selected_zone"]["direction"] == "BEARISH"

    row = _able_zone_row("SELL")
    row["order_block"] = {
        "low": 100.5, "high": 101.0, "timestamp": CANDLE_TS - 900_000,
        "strength": 6, "direction": "BULLISH",
        "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH",
    }
    rejected = pipeline.evaluate_cross_zone(row)
    assert rejected["valid"] is False
    assert rejected["reason"] == "zone_direction_mismatch"


def test_missing_or_contradictory_zone_direction_fails_closed():
    row = _able_zone_row("BUY")
    row["order_block"] = {"low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
                          "strength": 6, "status": "FRESH"}
    assert pipeline.evaluate_cross_zone(row)["reason"] == "zone_direction_unknown"

    row = _able_zone_row("BUY")
    row["order_block"] = {}
    row["fvg"] = {"lower": 99.2, "upper": 99.7, "timestamp": CANDLE_TS - 900_000,
                  "status": "UNFILLED", "size_atr": 0.3}
    assert pipeline.evaluate_cross_zone(row)["reason"] == "zone_direction_unknown"

    row = _able_zone_row("BUY")
    row["order_block"] = {}
    row["support_resistance"] = {"supports": [{
        "low": 99.2, "high": 99.7, "strength": 6, "status": "CONFIRMED",
        "confirmed_at_timestamp": CANDLE_TS - 900_000,
    }], "resistances": []}
    assert pipeline.evaluate_cross_zone(row)["reason"] == "zone_direction_unknown"

    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000, "strength": 6,
        "direction": "BULLISH", "zone_definition": "BEARISH_OPEN_TO_HIGH", "status": "FRESH",
    }
    assert pipeline.evaluate_cross_zone(row)["reason"] == "zone_direction_contradictory"


def test_structural_derivation_uses_only_producer_definition():
    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000,
        "strength": 6, "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH",
    }
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is True
    selected = zone["selected_zone"]
    assert selected["direction"] == "BULLISH"
    assert selected["direction_status"] == pipeline._ZONE_DIRECTION_DERIVED
    assert selected["derived_direction"] == "BULLISH"
    assert selected["source_direction"] is None

    row = _able_zone_row("BUY")
    row["order_block"] = {}
    row["fvg"] = {"lower": 99.2, "upper": 99.7, "timestamp": CANDLE_TS - 900_000,
                  "status": "UNFILLED", "size_atr": 0.3}
    assert pipeline.evaluate_cross_zone(row)["reason"] == "zone_direction_unknown"


def test_invalid_stale_and_mitigated_zones_are_rejected():
    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 100.5, "high": 99.5, "timestamp": CANDLE_TS - 900_000, "strength": 6,
        "direction": "BULLISH", "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH",
    }
    assert pipeline.evaluate_cross_zone(row)["reason"] == "invalid_zone_geometry_or_market_reference"

    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 49 * 900_000, "strength": 6,
        "direction": "BULLISH", "zone_definition": "BULLISH_LOW_TO_OPEN", "status": "FRESH",
    }
    assert pipeline.evaluate_cross_zone(row)["reason"] == "stale_zone"

    row = _able_zone_row("BUY")
    row["order_block"] = {
        "low": 99.0, "high": 99.5, "timestamp": CANDLE_TS - 900_000, "strength": 6,
        "direction": "BULLISH", "zone_definition": "BULLISH_LOW_TO_OPEN",
        "status": "FULLY_MITIGATED",
    }
    assert pipeline.evaluate_cross_zone(row)["reason"] == "order_block_mitigated_or_invalidated"


def test_crossover_direction_not_latest_row_side_controls_zone_validation():
    row = _able_zone_row("BUY")
    row["side"] = "SELL"
    zone = pipeline.evaluate_cross_zone(row)
    assert zone["valid"] is True
    assert zone["selected_zone"]["direction"] == "BULLISH"
    assert zone["selected_zone"]["evaluation_side"] == "BUY"


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

    def _fake_verified_execution(*a, **k):
        calls.append((a, k))
        trade_id = "trade-cross-handoff"
        monkeypatch.setitem(ggpbot.STATE, "trade_id", trade_id)
        monkeypatch.setitem(ggpbot.STATE, "open", True)
        monkeypatch.setitem(ggpbot.STATE, "entry_execution_state", "PAPER_SIMULATED")
        monkeypatch.setitem(ggpbot.STATE, "qty", 1.0)
        monkeypatch.setitem(ggpbot.STATE, "entry", 100.0)
        monkeypatch.setitem(ggpbot.STATE, "entry_event_consumption_pending", False)
        monkeypatch.setitem(ggpbot.STATE, "entry_event_persistence_failed", False)
        monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
        assert store.consume_exact(event.event_id, trade_id).ok is True
        return True

    monkeypatch.setattr(ggpbot, "execute_entry", _fake_verified_execution)
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


# --------------------------------------------------------------------------
# Issue B/C acceptance: verified execution, event consumption and recovery
# --------------------------------------------------------------------------

class _FakeTradeJournal:
    """Minimal durable-event journal; no trade rows or exchange access."""

    def __init__(self):
        self.events = {}

    def get_event_trade_id(self, event_id):
        return self.events.get(str(event_id))

    def is_event_consumed(self, event_id):
        return str(event_id) in self.events

    def consume_event(self, event_id, trade_id):
        if not event_id or not trade_id or str(event_id) in self.events:
            return False
        self.events[str(event_id)] = str(trade_id)
        return True


def _patch_verified_wrapper(monkeypatch, store, event, *, paper=True,
                            unresolved=False, native_sl_status=None):
    durable = f"EMA9VWAP:900:{event.event_id}"
    trade_id = "trade-verified-cross"
    journal = _FakeTradeJournal()
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal, raising=False)
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_start", lambda *a, **k: True)
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_snapshot", lambda *a, **k: True)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", paper, raising=False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", not paper, raising=False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    monkeypatch.setattr(ggpbot, "_LAST_EXECUTION_GATE_RESULT", {}, raising=False)
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", {
        "entry_source": "EMA9_VWAP_CROSS_AUTHORIZED",
        "setup_type": "EMA9_VWAP_CROSS",
        "event_id": event.event_id,
        "cross_event_id": event.event_id,
        "cross_event": event.to_dict(),
        "cross_authorization": {"authorized": True},
    }, raising=False)
    for key, value in (
        ("open", True), ("trade_id", trade_id), ("current_symbol", SYMBOL),
        ("side", event.side), ("qty", 1.0), ("entry", 100.0),
        ("durable_event_id", durable), ("cross_event_id", event.event_id),
        ("entry_execution_state", "PAPER_SIMULATED" if paper else "FILLED"),
        ("entry_exchange_order_id", None if paper else "EXCHANGE-ORDER-1"),
        ("native_sl_status", native_sl_status or ("PAPER_SIMULATED" if paper else "VERIFIED")),
        ("execution_unresolved", unresolved), ("recovery_hold", False),
        ("entry_event_persistence_failed", False), ("entry_event_consumption_pending", False),
    ):
        monkeypatch.setitem(ggpbot.STATE, key, value)

    def _fake_original(*_a, **_k):
        ggpbot._LAST_EXECUTION_GATE_RESULT = {"durable_event_id": durable}
        return True

    monkeypatch.setattr(ggpbot, "_original_execute_entry_for_journal", _fake_original)
    return journal, durable, trade_id


def test_verified_execution_consumes_events_and_records_lifecycle(store, monkeypatch):
    event = _event()
    store.upsert(event)
    store.mark_status(event.event_id, pipeline.STATUS_AUTHORIZED, "all_mandatory_gates_passed")
    store.mark_execution(event.event_id, pipeline.STATUS_EXECUTING)
    journal, durable, trade_id = _patch_verified_wrapper(monkeypatch, store, event)

    assert ggpbot.execute_entry(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 90.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is True
    assert store.is_consumed(event.event_id) is True
    assert store.get_state(event.event_id)["consumed_trade_id"] == trade_id
    assert journal.events[durable] == trade_id
    statuses = [row["status"] for row in store.history(event.event_id)]
    for status in (
        pipeline.STATUS_AUTHORIZED, pipeline.STATUS_EXECUTING,
        pipeline.STATUS_ORDER_SUBMITTED, pipeline.STATUS_FILL_VERIFIED,
        pipeline.STATUS_POSITION_VERIFIED, pipeline.STATUS_EVENT_CONSUMED,
        pipeline.STATUS_OPEN,
    ):
        assert status in statuses
    assert ggpbot.STATE["event_consumption_status"] == "CONSUMED"
    assert ggpbot.STATE["entry_event_consumption_pending"] is False


def test_unverified_execution_never_consumes_events_and_blocks_entries(store, monkeypatch):
    event = _event()
    store.upsert(event)
    journal, _, _ = _patch_verified_wrapper(
        monkeypatch, store, event, paper=False, unresolved=True,
        native_sl_status="UNVERIFIED",
    )
    assert ggpbot.execute_entry(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 90.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is True
    assert store.is_consumed(event.event_id) is False
    assert journal.events == {}
    assert ggpbot.STATE["entry_event_consumption_pending"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    statuses = [row["status"] for row in store.history(event.event_id)]
    assert pipeline.STATUS_EVENT_CONSUMED not in statuses
    assert pipeline.STATUS_OPEN not in statuses


def test_cross_store_persistence_failure_blocks_without_journal_consumption(store, monkeypatch):
    event = _event()
    store.upsert(event)
    journal, _, trade_id = _patch_verified_wrapper(monkeypatch, store, event)
    monkeypatch.setattr(
        store, "consume_exact",
        lambda *_a, **_k: pipeline.ConsumptionResult(
            pipeline.CONSUMPTION_PERSISTENCE_ERROR, trade_id, False,
        ),
    )
    ggpbot.execute_entry(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 90.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    )
    assert journal.events == {}
    assert ggpbot.STATE["entry_event_persistence_failed"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


def test_same_trade_idempotency_is_not_treated_as_persistence_failure(store, monkeypatch):
    event = _event()
    store.upsert(event)
    journal, durable, trade_id = _patch_verified_wrapper(monkeypatch, store, event)
    assert store.consume_exact(event.event_id, trade_id).ok is True
    journal.events[durable] = trade_id

    assert ggpbot.execute_entry(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 90.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is True
    assert store.get_state(event.event_id)["consumed_trade_id"] == trade_id
    assert journal.events[durable] == trade_id
    assert ggpbot.STATE["entry_event_persistence_failed"] is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is False


def test_restart_reconciles_verified_fill_before_consuming_event(store, monkeypatch):
    event = _event()
    store.upsert(event)
    journal = _FakeTradeJournal()
    trade_id = "trade-recovered-cross"
    durable = f"EMA9VWAP:900:{event.event_id}"
    store.mark_execution(event.event_id, pipeline.STATUS_ORDER_SUBMITTED,
                         trade_id=trade_id, execution_state="FILLED")
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal, raising=False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    monkeypatch.setitem(ggpbot.STATE, "entry_event_consumption_pending", True)
    monkeypatch.setitem(ggpbot.STATE, "entry_event_persistence_failed", False)
    saved = {
        "entry_type": "EMA9_VWAP_CROSS", "cross_event_id": event.event_id,
        "durable_event_id": durable, "trade_id": trade_id,
    }
    assert ggpbot._reconcile_cross_event_consumption(
        trade_id, saved_state=saved, verified=True,
    ) is True
    assert store.is_consumed(event.event_id) is True
    assert journal.events[durable] == trade_id
    assert ggpbot.STATE["entry_event_consumption_pending"] is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is False
    assert pipeline.STATUS_OPEN in [row["status"] for row in store.history(event.event_id)]


def test_restart_conflict_blocks_cross_event_consumption(store, monkeypatch):
    event = _event()
    store.upsert(event)
    trade_id = "trade-recovered-cross"
    store.consume_exact(event.event_id, "different-trade")
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", _FakeTradeJournal(), raising=False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    saved = {"entry_type": "EMA9_VWAP_CROSS", "cross_event_id": event.event_id,
             "durable_event_id": "durable-conflict"}
    assert ggpbot._reconcile_cross_event_consumption(
        trade_id, saved_state=saved, verified=True,
    ) is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert ggpbot.STATE["entry_event_consumption_pending"] is True


def test_unresolved_execution_attempt_blocks_startup_until_consumed(store, monkeypatch):
    event = _event()
    store.upsert(event)
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    assert ggpbot._has_unresolved_cross_execution_events() is False

    store.mark_execution(event.event_id, pipeline.STATUS_ORDER_SUBMITTED,
                         trade_id="trade-unresolved", execution_state="FILLED")
    assert ggpbot._has_unresolved_cross_execution_events() is True

    assert store.consume_exact(event.event_id, "trade-unresolved").ok is True
    assert ggpbot._has_unresolved_cross_execution_events() is False


def test_restart_consumes_non_cross_durable_signal_without_cross_event(store, monkeypatch):
    journal = _FakeTradeJournal()
    durable = "BTCUSDT:15m:BUY:900"
    trade_id = "trade-non-cross-recovered"
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal, raising=False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False, raising=False)
    monkeypatch.setitem(ggpbot.STATE, "entry_event_consumption_pending", True)
    saved = {"entry_type": "INSTITUTIONAL", "durable_event_id": durable}

    assert ggpbot._reconcile_cross_event_consumption(
        trade_id, saved_state=saved, verified=True,
    ) is True
    assert journal.events[durable] == trade_id
    assert ggpbot.STATE["entry_event_consumption_pending"] is False


def test_execution_boundary_rejects_ema50_only_gate(store, monkeypatch):
    event = _event()
    store.upsert(event)
    monkeypatch.setattr(ggpbot, "get_cross_event_store", lambda: store)
    monkeypatch.setattr(ggpbot, "_validated_scanner_symbol", lambda _: SYMBOL)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", lambda *_a, **_k: _frame())
    monkeypatch.setattr(ggpbot, "_validate_dual_ema_vwap_entry_gate", lambda *_a, **_k: {
        "ready": True, "ema9_cross": "BEARISH", "ema9_cross_timestamp": CANDLE_TS,
        "ema50_cross": "BULLISH", "ema50_cross_timestamp": CANDLE_TS,
        "signal_candle_id": f"BUY:{CANDLE_TS}",
    })
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", {
        "entry_source": "EMA9_VWAP_CROSS_AUTHORIZED",
        "cross_event": event.to_dict(),
        "cross_authorization": {"authorized": True},
        "candle_timeframe": "15m",
        "closed_candle_frame": _frame(),
    }, raising=False)
    assert ggpbot._original_execute_entry_for_journal(
        "BUY", SYMBOL, 100.0, 98.8, 100.5, 101.0, 90.0, "test", 1.0,
        "SCALP", "EMA9_VWAP_CROSS", "SCALP",
    ) is False


def test_ema50_or_non_ema9_proof_never_creates_executable_event():
    row = _able_zone_row()
    row["indicator_snapshot"] = {
        **row["indicator_snapshot"],
        "previous_ema9": 101.0, "previous_vwap": 100.0,
        "latest_closed_timestamp": CANDLE_TS, "timeframe_seconds": 900,
    }
    assert pipeline.build_cross_event_from_analysis(row, now_ms=NOW_MS) is None
    valid = _able_zone_row()
    valid["cross_trigger"] = "EMA50_VWAP"
    assert pipeline.build_cross_event_from_analysis(valid, now_ms=NOW_MS) is None


def test_refreshed_watchlist_row_preserves_crossover_proof():
    event = _event()
    row = pipeline.watchlist_row_for_event(event)
    refreshed = pipeline.merge_cross_fields(row, {
        "cross_status": "NONE", "decision_state": "ENTRY_READY", "price": 101.0,
    })
    rebuilt = pipeline.build_cross_event_from_analysis(refreshed, now_ms=NOW_MS)
    assert rebuilt is not None
    assert rebuilt.event_id == event.event_id
    assert rebuilt.cross_direction == event.cross_direction
    assert rebuilt.candle_timestamp == event.candle_timestamp
    assert rebuilt.previous_ema9 == event.previous_ema9
    assert rebuilt.previous_vwap == event.previous_vwap
    assert refreshed["price"] == 101.0
