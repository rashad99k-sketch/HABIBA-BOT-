"""Targeted proof for the additive unified decision projection.

The projection is read-only. These tests verify that it makes existing evidence
explicit without creating a second cross detector, indicator, API call, journal,
or order path. The full Scanner -> Watchlist -> Entry test runs in PAPER mode and
records an execution trace for every stage.
"""
from __future__ import annotations

import time

import pytest

import ggpbot
import ggp_institutional_bridge as bridge
import naiso_unified_decision as proj
from roro_trade_journal import TradeJournal
from test_cross_driven_discovery_integration import (
    CANONICAL,
    LIFECYCLE_NOW_MS,
    NOW_SEC,
    SYMBOL,
    _cross_frame,
    _gate,
    _lifecycle_event,
    _lifecycle_frame,
)


FIXED_NOW = 1_800_000_000.0


def _durable_record(side="BUY", *, phase="WAITING_FOR_PULLBACK", entry_ready=False,
                    status="ACTIVE", event_id=None, expires_ts=None, **extra):
    return {
        "event_id": event_id or f"{CANONICAL}:15m:{side}:1",
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": side,
        "direction": "BULLISH" if side == "BUY" else "BEARISH",
        "status": status,
        "timeframe": "15m",
        "signal_candle_timestamp": 1_790_000_000_000,
        "closed_candle_timestamp": 1_790_000_000_000,
        "expires_ts": FIXED_NOW + 3600.0 if expires_ts is None else expires_ts,
        "metadata": {"phase": phase, "entry_ready": entry_ready},
        **extra,
    }


def _ranking(**extra):
    return {
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "direction": "BUY",
        "role": proj.RANKING_ONLY_ROLE,
        "state": "ALIGNED_NOT_CROSSING",
        "candle_timestamp": 1_790_000_000_000,
        **extra,
    }


# --------------------------------------------------------------------------- #
# 1) Cross state names, lifecycle and ranking isolation.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("side,expected", [
    ("BUY", "CONFIRMED_BUY_CROSS"),
    ("SELL", "CONFIRMED_SELL_CROSS"),
])
def test_confirmed_buy_and_sell_cross_states(side, expected):
    record = _durable_record(side)
    out = proj.classify_cross_state(record, now_ts=FIXED_NOW)
    assert out["cross_state"] == expected
    assert out["cross_direction"] == side
    assert out["cross_event_phase"] == "WAITING_FOR_PULLBACK"
    assert out["cross_lifecycle_status"] == "ACTIVE"
    assert out["durable_event"] is True
    assert out["authoritative"] is True
    assert out["role"] == proj.CONFIRMED_CROSS_ROLE
    assert out["event_timestamp"] == record["signal_candle_timestamp"]
    assert out["source_snapshot_id"] == proj.snapshot_id(record, side=side)


@pytest.mark.parametrize("record,proximity,expected,authoritative,stage", [
    (_ranking(state="NEAR_CROSS", converging=True, gap=0.1), {}, "CONVERGING", False, "ACTIVE"),
    (_ranking(state="ALIGNED_NOT_CROSSING", gap=1.25, direction="SELL"), {}, "ALIGNED_NOT_CROSSING", False, "ACTIVE"),
    (_ranking(reason="forming intrabar candle"), {}, "INVALID_DATA", False, "ACTIVE"),
    (_ranking(state="CROSS_CONFIRMED", cross_any="BULLISH", cross_ema9="BULLISH"),
     {}, "CONFIRMED_BUY_CROSS", False, "ACTIVE"),
    (_durable_record(status="ENTERED", consumed_trade_id="T1"), {}, "CROSS_ALREADY_CONSUMED", False, "ENTERED"),
    (_durable_record(status="INVALIDATED"), {}, "NO_CROSS", False, "INVALIDATED"),
    (_durable_record(status="INACTIVE"), {}, "NO_CROSS", False, "EXPIRED"),
    (_durable_record(expires_ts=FIXED_NOW - 1.0), {}, "NO_CROSS", False, "EXPIRED"),
    ({**_durable_record(), "expires_ts": None, "cross_event_expires_ts": FIXED_NOW - 1.0},
     {}, "NO_CROSS", False, "EXPIRED"),
    ({}, {}, "NO_CROSS", False, "ACTIVE"),
])
def test_cross_state_matrix(record, proximity, expected, authoritative, stage):
    out = proj.classify_cross_state(record, proximity=proximity, now_ts=FIXED_NOW)
    assert out["cross_state"] == expected
    assert out["authoritative"] is authoritative
    assert out["cross_lifecycle_status"] == stage
    if out["role"] == proj.RANKING_ONLY_ROLE:
        assert out["durable_event"] is False
        assert out["authoritative"] is False


def test_expired_synced_memory_cross_is_not_authoritative():
    stale_memory_row = {
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": "BUY",
        "cross_event_id": "BTC:15m:BUY:expired-memory",
        "cross_event_status": "ACTIVE",
        "cross_event_direction": "BUY",
        "cross_event_timestamp": 1_790_000_000_000,
        "cross_event_timeframe": "15m",
        "cross_event_expires_ts": FIXED_NOW - 10.0,
        "cross_context": {"cross_state": "CONFIRMED_BUY_CROSS", "authoritative": True},
        "role": "CONFIRMED_CROSS_EVENT",
    }
    classified = proj.classify_cross_state(stale_memory_row, now_ts=FIXED_NOW)
    assert classified["cross_state"] == "NO_CROSS"
    assert classified["cross_lifecycle_status"] == "EXPIRED"
    assert classified["authoritative"] is False
    sections = proj.project_dashboard_sections(
        confirmed_crosses=[stale_memory_row], now_ts=FIXED_NOW,
    )
    assert sections["confirmed_crosses"] == []


def test_cross_event_is_unique_and_never_reusable(tmp_path, monkeypatch):
    journal = TradeJournal(str(tmp_path / "cross-unique.sqlite3"))
    payload = _lifecycle_event("BUY")
    assert journal.record_cross_event(**payload) is True
    assert journal.record_cross_event(**payload) is False
    rows = journal.list_cross_events(("ACTIVE",))
    assert len(rows) == 1

    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    assert ggpbot._cross_event_is_usable(payload["event_id"], "BUY") is False  # no breakout yet
    assert journal.consume_cross_event(payload["event_id"], "trade-1") is True
    assert journal.consume_cross_event(payload["event_id"], "trade-2") is False
    consumed = journal.get_cross_event(payload["event_id"])
    out = proj.classify_cross_state(consumed, now_ts=FIXED_NOW)
    assert out["cross_state"] == "CROSS_ALREADY_CONSUMED"
    assert out["authoritative"] is False
    assert out["consumed"] is True
    assert ggpbot._cross_event_is_usable(payload["event_id"], "BUY") is False


# --------------------------------------------------------------------------- #
# 2) Context separation, liquidity stages and explicit decision states.
# --------------------------------------------------------------------------- #
def _context_row(**overrides):
    row = {
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": "BUY",
        "decision_state": "WAITING_FOR_CONFIRMATION",
        "decision_reason": "awaiting_closed_structure",
        "market_regime": "STRONG_TREND",
        "trend_direction": "BULLISH",
        "trend_strength": 72.0,
        "trade_state": "POST_CROSS",
        "higher_timeframe_bias": "SUPPORTIVE",
        "ema50_direction": "BUY",
        "vwap_position": "ABOVE",
        "adx": 27.0,
        "adx_di": {"di_plus": 31.0, "di_minus": 12.0, "di_dominant": "PLUS"},
        "momentum": {"flow_bias": "BULLISH", "continuation_strength": 68.0},
        "order_block": {"valid": True, "status": "FRESH", "fill_pct": 5.0,
                        "distance_atr": 0.4, "low": 100.0, "high": 101.0},
        "fvg": {"status": "PARTIALLY_FILLED", "fill_pct": 20.0,
                "lower": 100.2, "upper": 101.1, "direction": "BULLISH"},
        "golden_zone": {"score": 85, "state": "GOLDEN_CONFLUENCE", "ob_fvg_overlap": True},
        "zone_quality": 85.0,
        "liquidity": {"event": "SWEEP", "direction": "BULLISH", "level": 99.5,
                      "confidence": 0.72, "candle_timestamp": 1_790_000_000_000,
                      "reasons": ["low_liquidity_sweep_confirmed"]},
        "market_structure": {"confirmed": True, "mss": "BULLISH_CHOCH", "pattern": "MSS",
                             "direction": "BULLISH", "swing_high": 101.0, "swing_low": 98.0},
        "engine_candle_timestamp": 1_790_000_000_000,
        "structural_stop": 98.0,
        "data_freshness": "FRESH",
        "data_quality": "VALID_CLOSED_CANDLES",
    }
    row.update(overrides)
    return row


def test_context_includes_all_required_fields_and_separates_quality_scores():
    out = proj.project_decision_context(_context_row(), now_ts=FIXED_NOW)
    mc = out["market_context"]
    assert mc["ema50_direction"] == "BUY"
    assert mc["vwap_position"] == "ABOVE"
    assert mc["higher_timeframe_bias"] == "SUPPORTIVE"
    assert mc["local_structure"] == "CONFIRMED"
    assert mc["market_regime"] == "STRONG_TREND"
    assert mc["adx"] == 27.0
    assert mc["di_plus"] == 31.0 and mc["di_minus"] == 12.0
    assert mc["momentum"]["flow_bias"] == "BULLISH"
    assert mc["order_block"]["valid"] is True
    assert mc["order_block"]["freshness"] == "FRESH"
    assert mc["order_block"]["mitigation_pct"] == 5.0
    assert mc["order_block"]["distance_atr"] == 0.4
    assert mc["order_block"]["inside_zone"] is True
    assert mc["fvg"]["lower"] == 100.2 and mc["fvg"]["upper"] == 101.1
    assert mc["fvg"]["fill_pct"] == 20.0
    assert mc["fvg"]["freshness"] == "FRESH"
    assert mc["fvg"]["overlap_with_order_block"] is True
    assert mc["supply_demand"]["zone_type"] == "DEMAND"
    assert out["structure_context"]["break_level"] == 101.0
    assert out["structure_context"]["previous_swing"] == 98.0
    assert out["structure_context"]["confirmation_status"] == "CONFIRMED"
    assert out["liquidity_context"]["state"] == "LIQUIDITY_SWEEP"
    assert out["liquidity_context"]["stage"] == "CONFIRMED"
    assert out["zone_quality_score"] == 85.0
    assert out["context_quality_score"] != out["zone_quality_score"]
    assert "not_entry_authorization" in out["zone_context"]["meaning"]
    assert out["timeframe_view"]["timeframe_conflict"] == "NONE"

    opposed = proj.project_decision_context(
        _context_row(higher_timeframe_bias="OPPOSED"), now_ts=FIXED_NOW,
    )
    assert opposed["timeframe_view"]["timeframe_conflict"] == "PRESENT"
    assert any("TIMEFRAME_CONFLICT" in reason
               for reason in opposed["timeframe_view"]["conflict_reasons"])


@pytest.mark.parametrize("liquidity,setup,expected,stage", [
    ({"event": "SWEEP", "direction": "BULLISH", "candle_timestamp": 1,
      "confidence": 0.72, "reasons": ["low_liquidity_sweep_confirmed"]}, "", "LIQUIDITY_SWEEP", "CONFIRMED"),
    ({"event": "GRAB", "direction": "BULLISH", "candle_timestamp": 1,
      "confidence": 0.2, "reasons": ["touch_only"]}, "", "LIQUIDITY_GRAB", "DETECTED"),
    ({"event": "NONE", "direction": "NEUTRAL", "candle_timestamp": None,
      "confidence": 0.0, "reasons": []}, "FAILED_AUCTION_REVERSAL", "FAILED_AUCTION", "DETECTED"),
    ({"event": "NONE", "direction": "BULLISH", "candle_timestamp": 2,
      "confidence": 0.0, "reasons": ["reclaimed_equal_low"]}, "", "RECLAIM", "RECLAIMED"),
    ({"event": "NONE", "direction": "NEUTRAL", "candle_timestamp": None,
      "confidence": 0.0, "reasons": ["valid_data_no_recent_event"]}, "", "NO_LIQUIDITY_EVENT", "PENDING"),
])
def test_liquidity_states_distinguish_sweep_grab_failed_auction_and_reclaim(
    liquidity, setup, expected, stage,
):
    out = proj.classify_liquidity({"liquidity": liquidity, "decision_setup_type": setup})
    assert out["state"] == expected
    assert out["stage"] == stage
    if expected == "LIQUIDITY_GRAB":
        assert out["pending_reason"] == "touch_or_grab_not_sweep_confirmation"
    if expected == "NO_LIQUIDITY_EVENT":
        assert out["pending_reason"] == "valid_data_no_recent_event"


def test_generic_liquidity_context_gap_is_data_incomplete_not_fake_wait():
    row = _context_row(decision_reason="liquidity_context_unavailable")
    out = proj.project_decision_context(row, now_ts=FIXED_NOW)
    assert out["analysis_status"] == "DATA_INCOMPLETE"
    assert out["decision_state"] == "DATA_INCOMPLETE"
    assert out["required_next_event"] == "COMPLETE_CLOSED_CANDLE_FRAME_REQUIRED"
    assert out["entry_readiness"]["conditions_complete"] is False


def test_confirmed_reclaim_is_not_a_fake_wait():
    row = _context_row(
        decision_reason="reclaimed_equal_low",
        liquidity={"event": "NONE", "direction": "BULLISH", "candle_timestamp": 2,
                   "confidence": 0.0, "reasons": ["reclaimed_equal_low"]},
    )
    out = proj.project_decision_context(row, now_ts=FIXED_NOW)
    assert out["liquidity_context"]["state"] == "RECLAIM"
    assert out["liquidity_context"]["stage"] == "RECLAIMED"
    assert out["decision_state"] != "WAITING_FOR_LIQUIDITY_CONFIRMATION"


def _decision_case(state):
    base = _context_row()
    cases = {
        "ENTRY_READY": dict(
            _durable_record(phase="BREAKOUT_CONFIRMED", entry_ready=True),
            decision_state="ENTRY_READY",
            decision_reason="closed_rejection_breakout_confirmed",
            structural_stop=98.0,
        ),
        "WAITING_FOR_CROSS": {"decision_state": "WAITING_FOR_CONFIRMATION"},
        "WAITING_FOR_PULLBACK": _durable_record(phase="WAITING_FOR_PULLBACK"),
        "WAITING_FOR_RETEST": {"decision_reason": "awaiting_retest"},
        "WAITING_FOR_LIQUIDITY_CONFIRMATION": {"decision_reason": "waiting_liquidity_sweep_confirmation"},
        "WAITING_FOR_MSS": {"decision_reason": "requires_mss_choch"},
        "WAITING_FOR_REJECTION_CANDLE": _durable_record(phase="PULLBACK_CONFIRMED"),
        "WAITING_FOR_BREAK_CONFIRMATION": _durable_record(phase="REJECTION_RECORDED"),
        "BLOCKED_INVALID_ZONE": {"decision_reason": "no_actionable_zone"},
        "BLOCKED_MARKET_CHOP": {"market_regime": "CHOP", "decision_reason": "market_chop"},
        "BLOCKED_CONFLICTING_CONTEXT": {"conflicting_directions": ["BUY", "SELL"]},
        "REJECTED_INVALIDATED_SETUP": {"decision_reason": "post_sweep_invalidated"},
        "EXPIRED": {"decision_state": "EXPIRED", "decision_reason": "cross_event_expired"},
        "DATA_STALE": {"data_freshness": "STALE_DATA"},
        "DATA_INCOMPLETE": {"data_quality": "INSUFFICIENT_CLOSED_CANDLES"},
    }
    if state == "ENTRY_READY":
        return cases[state]
    row = dict(base)
    row.update(cases[state])
    return row


@pytest.mark.parametrize("state", proj.DECISION_STATES)
def test_all_required_decision_states_are_explicit(state):
    row = _decision_case(state)
    out = proj.project_decision_context(row, now_ts=FIXED_NOW)
    assert out["decision_state"] == state
    assert out["setup_stage"] == proj.SETUP_STAGE[state]
    assert out["required_next_event"] == proj.REQUIRED_NEXT_EVENT[state]
    assert out["primary_reason"]
    assert out["authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"
    if state in {"DATA_STALE", "DATA_INCOMPLETE"}:
        assert out["analysis_status"] == state
        assert out["entry_readiness"]["conditions_complete"] is False


def test_entry_readiness_never_presents_unknown_sl_as_ready():
    row = _decision_case("ENTRY_READY")
    row.pop("structural_stop", None)
    out = proj.project_decision_context(row, now_ts=FIXED_NOW)
    assert out["decision_state"] != "ENTRY_READY"
    assert out["entry_readiness"]["state"] == "NOT_READY"
    assert out["entry_readiness"]["conditions_complete"] is False
    assert out["entry_readiness"]["sl_valid"] is False
    assert "STRUCTURAL_STOP_UNKNOWN" in out["entry_readiness"]["missing"]


def test_projection_sections_are_separated_and_ranking_never_becomes_an_event():
    durable = _durable_record(phase="BREAKOUT_CONFIRMED", entry_ready=True)
    ranking = _ranking(symbol="ETH", exchange_symbol="ETH/USDT:USDT",
                       state="CROSS_CONFIRMED", cross_any="BULLISH", cross_ema9="BULLISH")
    out = proj.project_dashboard_sections(
        watchlist={"BTC": dict(_context_row(), **durable, decision_state="ENTRY_READY")},
        confirmed_crosses=[durable],
        ranking=[ranking],
        now_ts=FIXED_NOW,
    )
    assert set(out) >= {"confirmed_crosses", "watchlist_ranking", "institutional_analysis", "execution_readiness"}
    assert len(out["confirmed_crosses"]) == 1
    assert out["confirmed_crosses"][0]["cross_context"]["durable_event"] is True
    assert len(out["watchlist_ranking"]) == 1
    ranking_cross = out["watchlist_ranking"][0]["cross_context"]
    assert ranking_cross["cross_state"] == "CONFIRMED_BUY_CROSS"
    assert ranking_cross["display_cross_state"] == "RANKING_ONLY_CONFIRMED_BUY_CROSS_OBSERVATION"
    assert out["watchlist_ranking"][0]["cross_state_display"] == "RANKING_ONLY_CONFIRMED_BUY_CROSS_OBSERVATION"
    assert out["watchlist_ranking"][0]["decision_context"]["cross_context"]["display_cross_state"] == \
        "RANKING_ONLY_CONFIRMED_BUY_CROSS_OBSERVATION"
    assert ranking_cross["durable_event"] is False
    assert ranking_cross["authoritative"] is False
    assert ranking_cross["role"] == proj.RANKING_ONLY_ROLE
    assert out["confirmed_crosses"][0]["cross_context"]["display_cross_state"] == "CONFIRMED_BUY_CROSS"
    assert out["confirmed_crosses"][0]["decision_context"]["cross_context"]["display_cross_state"] == "CONFIRMED_BUY_CROSS"
    assert all(row["symbol"] != "ETH" for row in out["execution_readiness"])
    assert any(row["conditions_complete"] is True for row in out["execution_readiness"])


# --------------------------------------------------------------------------- #
# 3) Full PAPER integration trace with no premature order path.
# --------------------------------------------------------------------------- #
def test_full_scanner_to_watchlist_to_entry_projection_paper_trace(tmp_path, monkeypatch):
    frame = _cross_frame("BUY")
    gate = _gate(frame, "BUY")
    assert gate["ready"] is True

    clock = [NOW_SEC]
    frames = {"current": frame}
    monkeypatch.setattr(ggpbot.time, "time", lambda: clock[0])
    journal = TradeJournal(str(tmp_path / "unified-decision.sqlite3"))
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_get_scanner_frame", lambda *a, **k: frames["current"])
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", False)
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {})
    monkeypatch.setitem(ggpbot.MEMORY, "position_status", "FLAT")
    monkeypatch.setitem(ggpbot.MEMORY, "_unified_watchlist_refresh_ok", True)
    for key in ("open", "in_position", "execution_unresolved", "recovery_hold",
                "entry_context_persistence_failed", "cross_event_persistence_failed"):
        monkeypatch.setitem(ggpbot.STATE, key, False)
    monkeypatch.setitem(ggpbot.STATE, "trade_id", "")
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.paper, "balance", 1000.0)
    monkeypatch.setitem(ggpbot.paper, "position", None)
    monkeypatch.setattr(ggpbot, "get_ohlcv_safe", lambda *a, **k: frames["current"])
    monkeypatch.setattr(ggpbot, "get_ticker_safe", lambda *a, **k: float(frames["current"]["close"].iloc[-1]))
    monkeypatch.setattr(ggpbot, "update_position_dashboard", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "tg_entry", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "_live_manager", type(
        "PaperManager", (), {"start_trade": lambda *a, **k: None}
    )())

    def forbidden_api(*args, **kwargs):
        raise AssertionError("projection or premature entry attempted a live/exchange API call")

    monkeypatch.setattr(ggpbot, "safe_api_call", forbidden_api)
    monkeypatch.setattr(ggpbot.TradeExecutionService, "submit_market_entry", forbidden_api)
    monkeypatch.setattr(ggpbot.TradeExecutionService, "place_and_verify_native_stop", forbidden_api)

    trace = []

    def step(stage, **evidence):
        trace.append({"stage": stage, **evidence})

    # 1) Scanner strict gate creates one durable event.
    assert ggpbot._detect_confirmed_cross_events([SYMBOL]) == 1
    event = journal.list_cross_events(("ACTIVE",))[0]
    step("SCANNER_STRICT_GATE", event_id=event["event_id"], source=event["source"])

    # 2) Confirmed-cross projection is durable and not yet entry ready.
    ggpbot._sync_confirmed_cross_events()
    confirmed = ggpbot.MEMORY["confirmed_cross_events"][0]
    assert confirmed["cross_state"] == "CONFIRMED_BUY_CROSS"
    assert confirmed["cross_context"]["durable_event"] is True
    assert confirmed["cross_context"]["authoritative"] is True
    assert confirmed["cross_context"]["cross_event_phase"] == "WAITING_FOR_PULLBACK"
    assert confirmed["cross_context"]["cross_event_entry_ready"] is False
    step("CROSS_PROJECTION_ACTIVE", cross_state=confirmed["cross_state"],
         phase=confirmed["cross_context"]["cross_event_phase"])

    # 3) Watchlist carries one explicit projected decision; it is not ready.
    watchlist = bridge.update_unified_watchlist(
        ggpbot.MEMORY, lambda _: frame, max_items=20, batch_size=20,
    )
    item = watchlist[CANONICAL]
    assert item["cross_event_id"] == event["event_id"]
    assert item["decision_context"]["decision_state"] != "ENTRY_READY"
    assert item["entry_readiness_context"]["conditions_complete"] is False
    assert item["source_snapshot_id"]
    step("WATCHLIST_DECISION_PROJECTION", decision_state=item["decision_context"]["decision_state"],
         next_event=item["required_next_event"], snapshot_id=item["source_snapshot_id"])

    # 4) Entry gate must refuse before the closed rejection breakout exists.
    price = float(frame["close"].iloc[-1])
    ctx = {
        "setup_type": "TREND_CONTINUATION_ENGINE",
        "event_id": f"institution:{event['event_id']}",
        "evidence": ["closed_ema9_vwap_cross"], "structural_stop": price - 2.0,
        "trading_mode": "TREND", "entry_behavior": "ENTER_NOW",
        "entry_zone_low": price - 0.5, "entry_zone_high": price + 0.2,
        "support_resistance": {}, "order_block": {}, "fvg": {},
        "liquidity": {}, "golden_zone": {}, "market_intelligence": {},
        "zone_quality": 70.0, "zone_type": "DEMAND", "market_structure": {},
        "volume_ratio": 1.2, "adx": 25.0, "adx_di": {}, "ema9": gate["ema9"],
        "ema50": gate["ema50"], "vwap": gate["vwap"], "market_regime": "STRONG_TREND",
        "cross_event_id": event["event_id"], "cross_event_source": event["source"],
        "cross_event_direction": event["direction"],
        "cross_event_timestamp": event["signal_candle_timestamp"],
        "exchange_symbol": SYMBOL, "candle_timeframe": str(ggpbot.INTERVAL),
        "closed_candle_frame": frame.copy(),
        "closed_candle_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
        "closed_candle_timestamp": int(frame["timestamp"].iloc[-1]),
    }
    # A deliberately spoofed projection claiming ENTRY_READY must not change the
    # unchanged authority: the durable gate is still WAITING_FOR_PULLBACK.
    ctx["unified_decision_context"] = {
        "schema": proj.SCHEMA_VERSION,
        "decision_state": "ENTRY_READY",
        "entry_readiness": {"conditions_complete": True, "sl_valid": True},
        "authority": "SPOOFED_PROJECTION_MUST_BE_IGNORED",
    }
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", ctx)
    premature = ggpbot.execute_entry(
        "BUY", SYMBOL, price, price - 2.0, price * 1.008, price * 1.02, 80,
        "UNIFIED_PREMATURE_PROOF", 0.69, "TREND", "TREND", "TREND",
    )
    assert premature is False
    assert ggpbot.paper.get("position") is None
    assert journal.get_cross_event(event["event_id"])["status"] == "ACTIVE"
    step("ENTRY_BLOCKED_BEFORE_BREAKOUT", reason="cross_phase_waiting_for_pullback",
         spoofed_projection_ready=True, journal_status="ACTIVE", paper_position=None)

    # 5) Later closed candles complete the durable rejection lifecycle.
    frames["current"] = _lifecycle_frame("BUY")
    clock[0] = LIFECYCLE_NOW_MS / 1000.0
    ggpbot._sync_confirmed_cross_events()
    completed = journal.get_cross_event(event["event_id"])
    assert completed["metadata"]["phase"] == "BREAKOUT_CONFIRMED"
    assert completed["metadata"]["entry_ready"] is True
    completed_projection = ggpbot._project_confirmed_cross_row(completed)
    assert completed_projection["cross_state"] == "CONFIRMED_BUY_CROSS"
    assert completed_projection["cross_event_phase"] == "BREAKOUT_CONFIRMED"
    assert completed_projection["cross_event_entry_ready"] is True
    assert ggpbot._cross_event_is_usable(event["event_id"], "BUY") is True
    step("CLOSED_LIFECYCLE_BREAKOUT", phase=completed_projection["cross_event_phase"],
         entry_ready=completed_projection["cross_event_entry_ready"])

    # 6) A complete authoritative setup is projected ENTRY_READY with all context.
    cross_view = proj.classify_cross_state(completed, now_ts=clock[0])
    authorized_row = dict(item)
    authorized_row.update({
        "decision_state": "ENTRY_READY", "side": "BUY",
        "event_id": f"institution:{event['event_id']}",
        "decision_setup_type": "TREND_CONTINUATION_ENGINE",
        "structural_stop": price - 2.0,
        "order_block": {"valid": True, "status": "FRESH", "low": price - 0.5,
                        "high": price + 0.2, "strength": 8.0, "distance_atr": 0.25},
        "fvg": {"status": "UNFILLED", "fill_pct": 0.0, "lower": price - 0.5,
                "upper": price + 0.2, "direction": "BULLISH"},
        "liquidity": {"event": "SWEEP", "direction": "BULLISH", "level": price - 0.8,
                      "confidence": 0.8, "candle_timestamp": int(frames["current"]["timestamp"].iloc[-1]),
                      "reasons": ["low_liquidity_sweep_confirmed"]},
        "market_structure": {"confirmed": True, "mss": "BULLISH_CHOCH"},
        "golden_zone": {"score": 72, "state": "GOLDEN_CONFLUENCE", "ob_fvg_overlap": True},
        "zone_quality": 72.0, "zone_type": "DEMAND", "volume_ratio": 1.2,
        "adx": 26.0, "adx_di": {"di_plus": 30.0, "di_minus": 12.0},
        "ema9": gate["ema9"], "ema50": gate["ema50"], "vwap": gate["vwap"],
        "market_regime": "STRONG_TREND", "trading_mode": "TREND",
        "entry_behavior": "ENTER_NOW", "entry_zone_low": price - 0.5,
        "entry_zone_high": price + 0.2, "atr": 0.69,
        "evidence": ["closed_ema9_vwap_cross", "fresh_institutional_zone"],
    })
    decision = proj.project_decision_context(authorized_row, cross=cross_view, now_ts=clock[0])
    assert decision["decision_state"] == "ENTRY_READY"
    assert decision["entry_readiness"]["conditions_complete"] is True
    assert decision["entry_readiness"]["sl_valid"] is True
    assert decision["required_next_event"] == "NONE_ALL_CONDITIONS_COMPLETE"
    step("DECISION_ENTRY_READY", decision_state=decision["decision_state"],
         snapshot_id=decision["source_snapshot_id"])

    # 7) The projection is frozen into the existing persistent entry context.
    ctx["unified_decision_context"] = decision
    live_price = float(frames["current"]["close"].iloc[-1])
    live_stop = live_price - 2.0
    persistent = ggpbot._build_persistent_entry_context(
        ctx, dict(gate), "BUY", SYMBOL, "trace-trade-1", 0.69,
    )
    frozen = persistent.get("unified_decision_context") or {}
    assert frozen.get("decision_state") == "ENTRY_READY"
    step("FROZEN_ENTRY_CONTEXT", decision_state=frozen.get("decision_state"),
         closed_candle_timestamp=persistent.get("closed_candle_timestamp"))

    # 8) PAPER execution succeeds only after the conditions are complete.
    ctx.update({
        "closed_candle_frame": frames["current"].copy(),
        "closed_candle_timestamp": int(frames["current"]["timestamp"].iloc[-1]),
        "structural_stop": live_stop,
        "entry_zone_low": live_price - 0.5,
        "entry_zone_high": live_price + 0.2,
        "unified_decision_context": decision,
    })
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", ctx)
    ok = ggpbot.execute_entry(
        "BUY", SYMBOL, live_price, live_stop, live_price * 1.008, live_price * 1.02, 80,
        "UNIFIED_DECISION_E2E", 0.69, "TREND", "TREND", "TREND",
    )
    assert ok is True
    trade_id = str(ggpbot.STATE["trade_id"])
    assert trade_id
    assert ggpbot.paper.get("position") is not None
    consumed = journal.get_cross_event(event["event_id"])
    assert consumed["status"] == "ENTERED"
    step("PAPER_ENTRY_EXECUTED", trade_id=trade_id, journal_status=consumed["status"])

    # 9) The consumed cross cannot be reused.
    consumed_view = proj.classify_cross_state(consumed, now_ts=clock[0])
    assert consumed_view["cross_state"] == "CROSS_ALREADY_CONSUMED"
    assert consumed_view["authoritative"] is False
    assert ggpbot._cross_event_is_usable(event["event_id"], "BUY") is False
    step("CROSS_CONSUMED_NO_REUSE", cross_state=consumed_view["cross_state"],
         authoritative=consumed_view["authoritative"])

    assert [row["stage"] for row in trace] == [
        "SCANNER_STRICT_GATE", "CROSS_PROJECTION_ACTIVE", "WATCHLIST_DECISION_PROJECTION",
        "ENTRY_BLOCKED_BEFORE_BREAKOUT", "CLOSED_LIFECYCLE_BREAKOUT", "DECISION_ENTRY_READY",
        "FROZEN_ENTRY_CONTEXT", "PAPER_ENTRY_EXECUTED", "CROSS_CONSUMED_NO_REUSE",
    ]
    assert trace[3]["paper_position"] is None
    assert trace[5]["decision_state"] == "ENTRY_READY"
    assert trace[7]["journal_status"] == "ENTERED"


def _watchlist_card_row():
    """One original Watchlist row carrying its already-produced diagnostics."""
    row = _context_row()
    row.update({
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": "BUY",
        "cross_event_id": "BTC:15m:BUY:card-test",
        "cross_event_status": "ACTIVE",
        "decision_state": "WAITING_FOR_RETEST",
        "structural_stop": 98.0,
        "rejection_reasons": ["closed_retest_not_confirmed"],
        "decision_context": {
            "decision_state": "WAITING_FOR_RETEST",
            "setup_stage": "AWAIT_ZONE_RETEST",
            "blocking_reasons": ["closed_retest_not_confirmed"],
            "required_next_event": "CLOSED_INSTITUTIONAL_ZONE_RETEST",
            "authority": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED",
            "cross_context": {
                "cross_state": "CONFIRMED_BUY_CROSS",
                "display_cross_state": "CONFIRMED_BUY_CROSS",
                "durable_event": True,
                "authoritative": True,
                "role": proj.CONFIRMED_CROSS_ROLE,
                "source_snapshot_id": "snap-btc-card",
            },
            "zone_context": {
                "zone_quality_score": 82.0,
                "supply_demand": {"valid": True},
            },
            "liquidity_context": {"state": "LIQUIDITY_SWEEP", "stage": "CONFIRMED"},
            "structure_context": {"confirmation_status": "PENDING"},
            "timeframe_view": {"market_regime": "STRONG_TREND", "trade_state": "POST_CROSS"},
            "entry_readiness": {
                "conditions_complete": False,
                "sl_valid": True,
                "missing": ["WAITING_FOR_RETEST"],
                "authoritative_gate": "UNCHANGED_EXECUTE_ENTRY",
            },
        },
    })
    return row


def _patch_dashboard_runtime(monkeypatch):
    monkeypatch.setitem(ggpbot.DASHBOARD_STATE, "live_trade_mode", False)
    for key in ("golden_buy", "golden_sell", "scanner_v2_buy", "scanner_v2_sell",
                "rf_dashboard", "no_entry_feed"):
        monkeypatch.setitem(ggpbot.MEMORY, key, [])
    monkeypatch.setattr(ggpbot, "get_balance_safe", lambda: 1000.0)
    monkeypatch.setattr(ggpbot, "get_free_balance_safe", lambda: 1000.0)
    monkeypatch.setattr(ggpbot, "get_dashboard_metrics", lambda: {
        "total_pnl": "0.00", "total_pnl_usdt": 0.0, "last_trade": "-",
    })
    monkeypatch.setattr(ggpbot, "cleanup_watchlist", lambda: None)
    monkeypatch.setattr(ggpbot, "naiso_health_snapshot", lambda: {"status": "TEST"})


def test_dashboard_renders_one_watchlist_with_card_diagnostics(monkeypatch):
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {
        "BTC": _watchlist_card_row(),
        "ETH": dict(_context_row(), symbol="ETH", exchange_symbol="ETH/USDT:USDT", side="SELL"),
    })
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [_durable_record()])
    monkeypatch.setitem(ggpbot.MEMORY, "ema9_vwap_proximity_scanner", [
        _ranking(symbol="ETH", exchange_symbol="ETH/USDT:USDT",
                 state="CROSS_CONFIRMED", cross_any="BULLISH", cross_ema9="BULLISH"),
    ])
    _patch_dashboard_runtime(monkeypatch)

    client = ggpbot.app.test_client()
    html = client.get("/").get_data(as_text=True)
    assert html.count("WATCHLIST / ACTIVE CANDIDATES") == 1
    assert "watchlist-panel" in html
    assert "NAISO UNIFIED DECISION VIEW" not in html
    for element in ("uni-confirmed-crosses", "uni-watchlist-ranking",
                    "uni-institutional-analysis", "uni-execution-readiness"):
        assert element not in html
    for token in ("Decision projection (read-only)", "Cross:", "durable=", "role=",
                  "Zone: quality=", "Liquidity:", "Confirmation:", "Pullback/Retest:",
                  "Structural Stop:", "STOP_INVALID_NOT_READY", "Blocking:", "Next:",
                  "UNCHANGED_EXECUTE_ENTRY"):
        assert token in html
    assert "cardDecision = w.decision_context" in html
    assert 'if (!cardCrossDurable && String(cardCrossState).startsWith("CONFIRMED_"))' in html
    assert "SL: UNKNOWN" not in html
    assert 'const slText = x.sl_valid ? "VALID" : "UNKNOWN";' not in html

    response = client.get("/data")
    assert response.status_code == 200
    payload = response.get_json()
    assert set(payload["watchlist"]) == {"BTC", "ETH"}
    for key in ("confirmed_crosses", "watchlist_ranking", "institutional_analysis",
                "execution_readiness", "unified_decision_schema"):
        assert key not in payload
    decision = payload["watchlist"]["BTC"]["decision_context"]
    assert decision["cross_context"]["durable_event"] is True
    assert decision["cross_context"]["authoritative"] is True
    assert decision["blocking_reasons"] == ["closed_retest_not_confirmed"]
    assert decision["required_next_event"] == "CLOSED_INSTITUTIONAL_ZONE_RETEST"
    assert decision["entry_readiness"]["sl_valid"] is True
    assert decision["entry_readiness"]["conditions_complete"] is False
    assert decision["entry_readiness"]["authoritative_gate"] == "UNCHANGED_EXECUTE_ENTRY"


def test_dashboard_projection_adds_no_exchange_or_scanner_calls(monkeypatch):
    monkeypatch.setitem(ggpbot.MEMORY, "watchlist", {"BTC": _watchlist_card_row()})
    monkeypatch.setitem(ggpbot.MEMORY, "confirmed_cross_events", [])
    monkeypatch.setitem(ggpbot.MEMORY, "ema9_vwap_proximity_scanner", [])
    _patch_dashboard_runtime(monkeypatch)

    def forbidden(*args, **kwargs):
        raise AssertionError("dashboard projection attempted a new exchange/scanner call")

    for name in ("safe_api_call", "get_ohlcv_safe", "get_ticker_safe",
                 "_integration_fetch_frame", "_detect_confirmed_cross_events",
                 "refresh_unified_institutional_watchlist", "execute_entry"):
        monkeypatch.setattr(ggpbot, name, forbidden)

    client = ggpbot.app.test_client()
    html = client.get("/").get_data(as_text=True)
    payload = client.get("/data").get_json()
    assert "WATCHLIST / ACTIVE CANDIDATES" in html
    assert set(payload["watchlist"]) == {"BTC"}


def test_dashboard_source_has_no_entry_or_scanner_path():
    names = set(ggpbot.dashboard.__code__.co_names)
    forbidden = {
        "execute_entry", "TradeExecutionService", "place_order", "safe_api_call",
        "fetch_ohlcv", "get_ohlcv_safe", "get_ticker_safe", "_integration_fetch_frame",
        "_detect_confirmed_cross_events", "refresh_unified_institutional_watchlist",
        "update_unified_watchlist", "record_watchlist_entry", "_project_dashboard_sections",
    }
    assert not names & forbidden


def test_bridge_projection_preserves_legacy_protected_fields():
    legacy = {
        "symbol": CANONICAL,
        "exchange_symbol": SYMBOL,
        "side": "BUY",
        "decision_state": "ENTRY_READY",
        "entry_status": "ENTRY_READY",
        "decision_reason": "closed_rejection_breakout_confirmed",
        "structural_stop": 98.0,
        "entry_readiness": {"conditions_complete": True, "sl_valid": True},
        "position_size_pct": 20.0,
        "leverage": 20,
        "risk_pct": 0.2,
        "tp1_pct": 5.0,
        "tp2_pct": 10.0,
        "golden_zone": {"score": 85},
    }
    before = dict(legacy)
    before["entry_readiness"] = dict(legacy["entry_readiness"])
    before["golden_zone"] = dict(legacy["golden_zone"])

    out = bridge._with_projected_decision(legacy)
    for key, value in before.items():
        assert out[key] == value, key
    assert out["entry_readiness"] is legacy["entry_readiness"]
    assert out["decision_context"]["schema"] == proj.SCHEMA_VERSION
    assert out["projected_decision_state"] == out["decision_context"]["decision_state"]
    assert out["required_next_event"] == out["decision_context"]["required_next_event"]
    assert out["decision_context"]["authority"] == "UNIFIED_ENTRY_AUTHORITY_UNCHANGED"


def test_projection_adds_no_exchange_or_snapshot_calls(monkeypatch):
    frame = _cross_frame("BUY")

    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected exchange/snapshot API call")

    monkeypatch.setattr(ggpbot, "get_ohlcv_safe", forbidden)
    monkeypatch.setattr(ggpbot, "safe_api_call", forbidden)
    monkeypatch.setattr(ggpbot, "_integration_fetch_frame", forbidden)

    watchlist = bridge.update_unified_watchlist(
        {"scanner_v2_buy": [{"symbol": SYMBOL, "side": "BUY", "score": 80}]},
        lambda _: frame, max_items=3, batch_size=3,
    )
    row = watchlist[CANONICAL]
    assert row["decision_context"]["schema"] == proj.SCHEMA_VERSION
    assert row["exchange_symbol"] == SYMBOL
    assert row["source_snapshot_id"]
    assert row["decision_context"]["timeframe_view"]["source"] == "EXISTING_FIELDS_NO_EXTRA_FETCH"
    assert "unified" in row or "decision_context" in row
    assert ggpbot.normalize_symbol(SYMBOL) == SYMBOL
