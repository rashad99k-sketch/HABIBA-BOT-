"""Tests for verified exchange-native stop moves from the management Brain.

No real order or exchange request is made: TradeExecutionService is replaced by
a deterministic fake that records the exact replace call and returns the three
contract outcomes (MOVED / RETAINED / LOST).
"""
from __future__ import annotations

import copy

import pytest

import ggpbot
from ggpbot import TradeDecision, TradeDecisionType


class MoveResult:
    def __init__(self, outcome, order_id=None, reason=""):
        self.outcome = outcome
        self.order_id = order_id
        self.reason = reason


class FakeExecutionService:
    def __init__(self, outcome="MOVED", order_id="new-2", reason=""):
        self.outcome = outcome
        self.order_id = order_id
        self.result_reason = reason
        self.calls = []

    def replace_native_stop(self, **kwargs):
        self.calls.append(kwargs)
        return MoveResult(self.outcome, self.order_id, self.result_reason)


@pytest.fixture
def management_state(monkeypatch):
    state = copy.deepcopy(ggpbot.STATE)
    state.update({
        "open": True,
        "side": "BUY",
        "current_symbol": "BTC/USDT:USDT",
        "entry": 100.0,
        "qty": 2.0,
        "initial_qty": 2.0,
        "remaining_qty": 2.0,
        "atr": 1.0,
        "position_mode": "HEDGE",
        "position_side": "LONG",
        "synthetic_sl": 95.0,
        "native_sl_original_price": 95.0,
        "native_sl_last_verified_price": 95.0,
        "native_sl_order_id": "old-1",
        "native_sl_verified": True,
        "native_sl_status": "VERIFIED",
        "native_sl_last_requested_price": 0.0,
        "native_sl_last_update_ts": 0.0,
        "recovery_hold": False,
        "execution_unresolved": False,
        "close_reconciliation_required": False,
        "external_close_detected": False,
    })
    monkeypatch.setattr(ggpbot, "STATE", state)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", object(), raising=False)
    return state


def _install_service(monkeypatch, fake):
    monkeypatch.setattr(ggpbot, "TradeExecutionService", lambda *a, **k: fake)
    return fake


def _decision(action, **params):
    return TradeDecision(action=action, params=params, reason="test_reason")


def _execute(monkeypatch, decision, symbol="BTC/USDT:USDT"):
    monkeypatch.setattr(ggpbot, "_persist_trade_journal_snapshot",
                        lambda *a, **k: None, raising=False)
    ggpbot._live_manager._execute_decision(decision, symbol)


def test_paper_mode_never_calls_exchange_but_records_local_target(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService())
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)

    ok, status, _ = ggpbot._move_native_stop_verified("BTC/USDT:USDT", 97.0)

    assert ok is True
    assert status == "PAPER_SIMULATED"
    assert fake.calls == []
    assert management_state["native_sl_policy"] == "PAPER_SYNTHETIC_ONLY"
    assert management_state["native_sl_local_target"] == 97.0
    assert management_state["native_sl_verified"] is True


def test_live_move_sl_sends_replace_and_adopts_only_verified_new_stop(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "new-2"))

    _execute(monkeypatch, _decision(TradeDecisionType.MOVE_SL, new_sl=97.0))

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["symbol"] == "BTC/USDT:USDT"
    assert call["side"] == "BUY"
    assert call["quantity"] == 2.0
    assert call["new_trigger_price"] == 97.0
    assert call["mode"].value == "HEDGE"
    assert call["position_side"] == "LONG"
    assert call["previous_order_id"] == "old-1"
    assert management_state["native_sl_order_id"] == "new-2"
    assert management_state["native_sl_last_verified_price"] == 97.0
    assert management_state["native_sl_verified"] is True
    assert management_state["native_sl_status"] == "VERIFIED"
    assert management_state["native_sl_policy"] == "EXCHANGE_VERIFIED_MOVED"
    assert management_state["recovery_hold"] is False


def test_retained_move_keeps_previous_verified_stop_and_does_not_claim_success(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("RETAINED", "old-1", "cancel_ambiguous"))

    _execute(monkeypatch, _decision(TradeDecisionType.BREAKEVEN, new_sl=97.0))

    assert len(fake.calls) == 1
    assert management_state["native_sl_order_id"] == "old-1"
    assert management_state["native_sl_verified"] is True
    assert management_state["native_sl_status"] == "VERIFIED_RETAINED"
    assert management_state["native_sl_policy"] == "EXCHANGE_STOP_RETAINED_SYNTHETIC_ADVANCED"
    assert management_state["recovery_hold"] is False


def test_lost_move_enters_existing_recovery_hold_without_false_verification(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("LOST", None, "no_verified_stop"))

    _execute(monkeypatch, _decision(TradeDecisionType.MOVE_SL, new_sl=97.0))

    assert len(fake.calls) == 1
    assert management_state["native_sl_verified"] is False
    assert management_state["native_sl_status"] == "UNVERIFIED"
    assert management_state["native_sl_policy"] == "EXCHANGE_STOP_UNVERIFIED_HOLD"
    assert management_state["recovery_hold"] is True
    assert "native_sl_move" in str(management_state["recovery_reason"])


def test_duplicate_stop_move_is_not_submitted_twice(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "new-2"))

    first = ggpbot._move_native_stop_verified("BTC/USDT:USDT", 97.0, action="TRAIL")
    second = ggpbot._move_native_stop_verified("BTC/USDT:USDT", 97.0, action="TRAIL")

    assert first[1] == "MOVED"
    assert second[1] == "DUPLICATE_SKIPPED"
    assert len(fake.calls) == 1


def test_micro_ratchet_does_not_submit_a_venue_cancel_replace(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "new-2"))

    ok, status, _ = ggpbot._move_native_stop_verified("BTC/USDT:USDT", 95.01, action="TRAIL")

    assert ok is False
    assert status == "NOT_MATERIAL"
    assert fake.calls == []


def test_trail_decision_moves_native_stop_after_verification(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "trail-2"))
    management_state["trail_stop"] = 95.0

    _execute(monkeypatch, _decision(TradeDecisionType.TRAIL, new_stop=98.0, multiplier=1.5))

    assert len(fake.calls) == 1
    assert fake.calls[0]["new_trigger_price"] == 98.0
    assert management_state["native_sl_order_id"] == "trail-2"
    assert management_state["native_sl_last_verified_price"] == 98.0
    assert management_state["synthetic_sl"] == 98.0
    assert management_state["trail_stop"] == 98.0


def test_tp1_breakeven_moves_native_stop_for_remaining_runner(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "be-2"))
    management_state["remaining_qty"] = 1.0
    management_state["last_close_filled_qty"] = 1.0
    management_state["last_close_order_id"] = "tp1-order"
    management_state["last_close_order_pnl_source"] = "EXCHANGE_VERIFIED"

    def fake_partial(symbol, ratio, *, close_reason="TP1", target_remaining_qty=None):
        management_state["remaining_qty"] = 1.0
        return True

    monkeypatch.setattr(ggpbot, "safe_close_partial", fake_partial)
    _execute(monkeypatch, _decision(TradeDecisionType.PARTIAL_CLOSE, ratio=0.5,
                                    target_remaining_qty=1.0, close_reason="TP1"))

    assert len(fake.calls) == 1
    assert fake.calls[0]["quantity"] == 1.0
    assert fake.calls[0]["new_trigger_price"] == pytest.approx(100.1)
    assert management_state["native_sl_order_id"] == "be-2"
    assert management_state["be_done"] is True
    assert management_state["be_local"] is False


def test_unresolved_management_state_blocks_stop_move_before_any_api_call(monkeypatch, management_state):
    fake = _install_service(monkeypatch, FakeExecutionService("MOVED", "new-2"))
    management_state["recovery_hold"] = True

    ok, status, _ = ggpbot._move_native_stop_verified("BTC/USDT:USDT", 97.0)

    assert ok is False
    assert status == "BLOCKED_UNRESOLVED"
    assert fake.calls == []
