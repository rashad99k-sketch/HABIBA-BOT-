"""NAISO-LIVE v1.1.0 long-run hardening regressions.

All tests are offline and paper-safe: no credentials, no real orders, no network.
They cover the failure modes reported from the long run: sticky recovery hold on
native-SL mismatch, duplicate protective orders, unbounded retry/stall handling,
unsafe maintenance during open positions and untested daily-loss pausing.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest


# --------------------------------------------------------------------------- #
# API resilience
# --------------------------------------------------------------------------- #
def test_api_error_classification_never_retries_order_writes():
    from naiso_api_resilience import classify_api_error

    assert classify_api_error(TimeoutError("timed out"), method="create_order").kind == "ORDER_EXECUTION_UNKNOWN"
    assert classify_api_error(TimeoutError("timed out"), method="create_order").retryable is False
    assert classify_api_error(Exception("HTTP 429"), method="fetch_positions").kind == "RATE_LIMITED"
    assert classify_api_error(Exception("100410 rate limit"), method="fetch_ohlcv").retryable is True
    assert classify_api_error(Exception("network unavailable"), method="fetch_balance").kind == "NETWORK"


def test_api_circuit_breaker_opens_fences_then_half_opens():
    from naiso_api_resilience import ApiResilience, CircuitOpenError

    clock = [0.0]

    def now():
        return clock[0]

    resilience = ApiResilience(
        failure_threshold=3, cooldown_sec=10.0, base_backoff_sec=0.01,
        jitter_fraction=0.0, seed=7, monotonic_fn=now, sleep_fn=lambda _s: None)

    for _ in range(3):
        resilience.before_call("fetch_positions")
        resilience.record_failure("fetch_positions", TimeoutError("timed out"))

    with pytest.raises(CircuitOpenError):
        resilience.before_call("fetch_positions")

    clock[0] = 11.0
    resilience.before_call("fetch_positions")  # single half-open probe
    with pytest.raises(CircuitOpenError):
        resilience.before_call("fetch_positions")

    resilience.record_success("fetch_positions")
    metrics = resilience.metrics()
    assert metrics["counters"]["circuit_opens"] == 1
    assert metrics["counters"]["half_open_successes"] == 1
    assert metrics["endpoints"]["fetch_positions"]["state"] == "CLOSED"
    assert metrics["endpoints"]["fetch_positions"]["failures"] == 0


def test_failed_half_open_probe_restarts_full_cooldown():
    from naiso_api_resilience import ApiResilience, CircuitOpenError

    clock = [0.0]
    resilience = ApiResilience(
        failure_threshold=2, cooldown_sec=10.0, monotonic_fn=lambda: clock[0],
        sleep_fn=lambda _s: None)
    for _ in range(2):
        resilience.before_call("fetch_balance")
        resilience.record_failure("fetch_balance", TimeoutError("timed out"))
    clock[0] = 11.0
    resilience.before_call("fetch_balance")
    resilience.record_failure("fetch_balance", TimeoutError("still down"))

    with pytest.raises(CircuitOpenError):
        resilience.before_call("fetch_balance")
    assert resilience.metrics()["endpoints"]["fetch_balance"]["state"] == "OPEN"


def test_safe_api_call_retries_reads_bounded_and_is_idempotent(monkeypatch):
    import ggpbot
    from naiso_api_resilience import ApiResilience

    sleeps = []
    resilience = ApiResilience(
        failure_threshold=50, base_backoff_sec=0.02, max_backoff_sec=0.04,
        jitter_fraction=0.0, seed=1, sleep_fn=lambda s: sleeps.append(s), monotonic_fn=lambda: 0.0)
    monkeypatch.setattr(ggpbot, "API_RESILIENCE", resilience)
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)

    calls = []

    def fetch_positions():
        calls.append(1)
        if len(calls) < 3:
            raise Exception("HTTP 429 Too Many Requests")
        return ["ok"]

    assert ggpbot.safe_api_call(fetch_positions) == ["ok"]
    assert len(calls) == 3
    assert len(sleeps) == 2
    assert resilience.metrics()["counters"]["retries"] == 2


def test_safe_api_call_never_retries_order_submission_uncertainty(monkeypatch):
    import ggpbot
    from naiso_api_resilience import ApiResilience

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", ApiResilience(seed=2))
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)

    calls = []

    def create_order():
        calls.append(1)
        raise TimeoutError("timed out")

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(create_order)

    assert len(calls) == 1, "a timed-out order submission must never be retried automatically"
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


def test_safe_api_call_never_retries_cancel_uncertainty(monkeypatch):
    import ggpbot
    from naiso_api_resilience import ApiResilience

    monkeypatch.setattr(ggpbot, "API_RESILIENCE", ApiResilience(seed=3))
    monkeypatch.setattr(ggpbot, "rate_limit", lambda *_a, **_k: None)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)

    calls = []

    def cancel_order():
        calls.append(1)
        raise TimeoutError("timed out")

    with pytest.raises(TimeoutError):
        ggpbot.safe_api_call(cancel_order)

    assert len(calls) == 1, "a timed-out cancel must be reconciled, not retried"
    assert ggpbot.STATE["execution_unresolved"] is True
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


# --------------------------------------------------------------------------- #
# Watchdog / restart policy
# --------------------------------------------------------------------------- #
def test_restart_policy_is_bounded_with_exponential_backoff():
    from naiso_health import RestartPolicy

    clock = [0.0]
    policy = RestartPolicy(max_attempts=2, window_sec=10.0, base_delay_sec=1.0,
                           jitter_fraction=0.0, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
    first = policy.next_delay("main_loop")
    record = policy.begin("main_loop", "stalled", first)
    policy.finish(record, "RESTARTED")
    clock[0] = 1.0
    second = policy.next_delay("main_loop")
    record = policy.begin("main_loop", "stalled", second)
    policy.finish(record, "RESTART_FAILED")

    assert first == pytest.approx(1.0)
    assert second == pytest.approx(2.0)
    assert policy.next_delay("main_loop") is None


def test_restart_history_is_bounded_during_long_run():
    from naiso_health import RestartPolicy

    clock = [0.0]
    policy = RestartPolicy(max_attempts=1, window_sec=1.0, monotonic_fn=lambda: clock[0])
    for cycle in range(500):
        clock[0] = float(cycle)
        record = policy.begin("worker", f"cycle_{cycle}", 0.01)
        policy.finish(record, "RESTARTED")

    assert len(policy.records("worker", limit=1000)) == RestartPolicy.MAX_RECORDS_PER_COMPONENT
    assert policy.records("worker", limit=1)[0]["reason"] == "cycle_499"


def test_watchdog_respects_safety_gate_before_restarting():
    from naiso_health import HealthWatchdog, RestartPolicy

    clock = [0.0]
    alerts = []
    calls = []
    gate = {"allow": False, "reason": "OPEN_POSITION"}
    watchdog = HealthWatchdog(
        restart_policy=RestartPolicy(max_attempts=3, monotonic_fn=lambda: clock[0]),
        alert_fn=lambda message, level: alerts.append((message, level)),
        safety_gate=lambda: (gate["allow"], gate["reason"]),
        check_interval_sec=1.0, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
    watchdog.register("main_loop", interval_sec=1.0, grace_sec=1.0, critical=True,
                      restartable=True, restart_fn=lambda: calls.append(1) or True)
    watchdog.beat("main_loop")
    clock[0] = 10.0

    report = watchdog.check()
    assert report["main_loop"]["status"] == "STALLED_SAFE_HOLD"
    assert calls == []
    assert any("restart of main_loop deferred: OPEN_POSITION" in m for m, _ in alerts)

    gate.update({"allow": True, "reason": "READY"})
    report = watchdog.check()
    assert report["main_loop"]["status"] == "RECOVERED"
    assert len(calls) == 1
    assert watchdog.snapshot()["components"]["main_loop"]["status"] == "RECOVERED"


# --------------------------------------------------------------------------- #
# Maintenance mode
# --------------------------------------------------------------------------- #
class _FakeResourceManager:
    def __init__(self):
        self.calls = 0

    def cleanup(self, preserve_keys=()):
        self.calls += 1
        return {"removed": self.calls}


class _FakeJournal:
    def __init__(self):
        self.checkpoints = 0
        self.prunes = 0

    def checkpoint_wal(self, mode):
        self.checkpoints += 1
        return {"mode": mode, "complete": True}

    def integrity_check(self):
        return {"ok": True, "result": "ok"}

    def prune_runtime_state(self, *, ttl_days, protected_keys):
        self.prunes += 1
        return {"removed_runtime_keys": 0, "protected_keys": sorted(protected_keys)}


def test_maintenance_only_runs_heavy_work_when_flat(monkeypatch):
    import naiso_maintenance

    state = {"open": True, "recovery_hold": False, "native_sl_verified": True}
    resources = _FakeResourceManager()
    journal = _FakeJournal()
    gc_calls = []
    monkeypatch.setattr(naiso_maintenance.gc, "collect", lambda: gc_calls.append(1) or 7)
    manager = naiso_maintenance.MaintenanceManager(
        state_provider=lambda: state, journal_provider=lambda: journal,
        resource_provider=lambda: resources, monotonic_fn=lambda: 0.0, min_interval_sec=1.0)

    result = manager.run(force=True)
    assert result["mode"] == "LIGHT"
    assert result["flags"]["position_open"] is True
    assert journal.checkpoints == 0
    assert journal.prunes == 0
    assert gc_calls == []

    state.clear()
    state.update({"open": False, "native_sl_verified": True})
    result = manager.run(force=True)
    assert result["mode"] == "HEAVY"
    assert result["journal_checkpoint"]["complete"] is True
    assert result["integrity"]["ok"] is True
    assert journal.prunes == 1
    assert gc_calls == [1]


def test_runtime_state_prune_protects_recovery_and_loss_keys(tmp_path):
    from roro_trade_journal import TradeJournal

    journal = TradeJournal(str(tmp_path / "journal.sqlite3"))
    protected = "daily_drawdown_cooldown_v1"
    journal.set_runtime_state(protected, {"until": 1.0})
    journal.set_runtime_state("stale_cache_key", {"x": 1})
    with journal._db() as db:
        db.execute("UPDATE runtime_state SET updated_ts=? WHERE state_key IN (?,?)",
                   (time.time() - 90 * 86400, protected, "stale_cache_key"))

    result = journal.prune_runtime_state(ttl_days=30, protected_keys=[protected, "recovery_state_v1"])
    assert result["removed_runtime_keys"] == 1
    assert journal.get_runtime_state(protected) == {"until": 1.0}
    assert journal.get_runtime_state("stale_cache_key", "gone") == "gone"
    assert journal.integrity_check()["ok"] is True


# --------------------------------------------------------------------------- #
# Native stop recovery repair / duplicate prevention
# --------------------------------------------------------------------------- #
def _stop_row(price, order_id="sl-1"):
    return {
        "id": order_id, "symbol": "BTC/USDT:USDT", "type": "STOP_MARKET",
        "side": "sell", "positionSide": "LONG", "status": "NEW",
        "stopPrice": price, "origQty": 2.0,
        "info": {"type": "STOP_MARKET", "orderId": order_id},
    }


class _StopExchange:
    def __init__(self, rows=None, *, query_ok=True, append_on_create=False):
        self.rows = list(rows or [])
        self.query_ok = query_ok
        self.append_on_create = append_on_create
        self.create_calls = 0

    def market(self, symbol):
        return {"id": "BTCUSDT"}

    def price_to_precision(self, symbol, price):
        return f"{float(price):.8f}"

    def fetch_open_orders(self, *args, **kwargs):
        if not self.query_ok:
            raise RuntimeError("conditional order query unavailable")
        return list(self.rows)

    def swapV2PrivateGetTradeOpenOrders(self, params):
        if not self.query_ok:
            raise RuntimeError("conditional order query unavailable")
        return {"code": 0, "data": list(self.rows)}

    def create_order(self, symbol, order_type, side, quantity, price, params):
        self.create_calls += 1
        order_id = f"new-sl-{self.create_calls}"
        if self.append_on_create:
            self.rows.append(_stop_row(float(params["stopPrice"]), order_id))
        return {"id": order_id}


def test_ensure_native_stop_adopts_verified_stop_without_duplicate(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    monkeypatch.setattr("bingx_execution.time.sleep", lambda _s: None)
    exchange = _StopExchange(rows=[_stop_row(100.0)])
    service = TradeExecutionService(exchange)

    result = service.ensure_native_stop(
        symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "VERIFIED_ADOPTED"
    assert result.verified is True
    assert exchange.create_calls == 0


def test_ensure_native_stop_holds_at_unproven_or_unknown_level(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    monkeypatch.setattr("bingx_execution.time.sleep", lambda _s: None)
    wrong_level = TradeExecutionService(_StopExchange(rows=[_stop_row(99.0)]))
    result = wrong_level.ensure_native_stop(
        symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")
    assert result.outcome == "HOLD"
    assert result.reason == "ACTIVE_STOP_AT_UNPROVEN_LEVEL"
    assert wrong_level.exchange.create_calls == 0

    unknown = TradeExecutionService(_StopExchange(query_ok=False))
    result = unknown.ensure_native_stop(
        symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")
    assert result.outcome == "HOLD"
    assert result.reason == "CONDITIONAL_QUERY_UNKNOWN"
    assert unknown.exchange.create_calls == 0


def test_ensure_native_stop_places_once_and_verifies_by_readback(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    monkeypatch.setattr("bingx_execution.time.sleep", lambda _s: None)
    exchange = _StopExchange(rows=[], append_on_create=True)
    service = TradeExecutionService(exchange)

    result = service.ensure_native_stop(
        symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "PLACED_VERIFIED"
    assert exchange.create_calls == 1
    assert result.order_id == "new-sl-1"


def test_ensure_native_stop_keeps_hold_when_placement_is_unverified(monkeypatch):
    from bingx_execution import PositionMode, TradeExecutionService

    monkeypatch.setattr("bingx_execution.time.sleep", lambda _s: None)
    exchange = _StopExchange(rows=[], append_on_create=False)
    service = TradeExecutionService(exchange)

    result = service.ensure_native_stop(
        symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
        mode=PositionMode.HEDGE, position_side="LONG")

    assert result.outcome == "HOLD"
    assert result.reason == "PLACED_READBACK_UNVERIFIED"
    assert result.verified is False


def test_verify_native_stop_accepts_journaled_moved_trigger(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "ex", _StopExchange(rows=[_stop_row(99.0, "moved-sl")]))
    status, order_id, matched = ggpbot.verify_native_stop_loss_detailed(
        "BTC/USDT:USDT", "BUY", 100.0, 2.0, "LONG", trigger_prices=[99.0])

    assert status == "VERIFIED"
    assert order_id == "moved-sl"
    assert matched == 99.0


def test_journal_native_stop_prices_use_only_persisted_candidates():
    import ggpbot

    prices = ggpbot._journal_native_stop_prices(
        {"native_sl_last_verified_price": 99.0, "native_sl_local_target": 98.0,
         "native_sl_original_price": 97.0, "sl": 96.0},
        primary=100.0)
    assert prices == [100.0, 99.0, 98.0, 97.0, 96.0]


def test_native_stop_repair_is_opt_in_and_never_clears_hold_on_unverified(monkeypatch):
    import ggpbot

    monkeypatch.delenv("NAISO_NATIVE_SL_REPAIR", raising=False)
    monkeypatch.setattr(ggpbot, "STATE", {"recovery_hold": True})
    assert ggpbot._repair_native_sl_from_journal(
        "BTC/USDT:USDT", "BUY", 2.0, prices=[100.0], mode="HEDGE", position_side="LONG") == (
            False, "REPAIR_DISABLED", None)
    assert ggpbot.STATE["recovery_hold"] is True


def test_recovery_hold_log_is_fail_closed_and_sets_both_gates(monkeypatch):
    import ggpbot

    messages = []
    monkeypatch.setattr(ggpbot, "STATE", {"native_sl_status": "MISSING"})
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    monkeypatch.setattr(ggpbot, "DASHBOARD_STATE", {})
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_live_manager", type("M", (), {"lifecycle_state": None})())
    monkeypatch.setattr(ggpbot, "log_execution", lambda message, *a, **k: messages.append(str(message)))

    assert ggpbot._recovery_hold("native_sl_missing", symbol="BTC/USDT:USDT", trade_id="T") is False
    assert any(
        "[RECOVERY_HOLD] native_sl_missing; exposure remains tracked; no new entries or TP/SL fabrication"
        in message for message in messages)
    assert ggpbot.STATE["recovery_hold"] is True
    assert ggpbot.STATE["recovery_reason"] == "native_sl_missing"
    assert ggpbot.STATE["native_sl_verified"] is False
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True


def test_finalizer_retains_lock_with_exact_combined_gate_message(monkeypatch):
    import ggpbot

    messages = []
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", True)
    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(ggpbot, "tg_close", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "clear_position_dashboard", lambda: None)
    monkeypatch.setattr(ggpbot, "update_stats", lambda *a, **k: None)
    monkeypatch.setattr(ggpbot, "log_execution", lambda message, *a, **k: messages.append(str(message)))
    monkeypatch.setattr(ggpbot, "_ACTIVE_TRADE", True)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", True)
    monkeypatch.setitem(ggpbot.MEMORY, "position_status", "FLAT")
    monkeypatch.setitem(ggpbot.STATE, "open", True)
    monkeypatch.setitem(ggpbot.STATE, "close_verified", True)
    monkeypatch.setitem(ggpbot.STATE, "current_symbol", "BTC/USDT:USDT")
    monkeypatch.setitem(ggpbot.STATE, "side", "BUY")
    monkeypatch.setitem(ggpbot.STATE, "entry", 100.0)
    monkeypatch.setitem(ggpbot.STATE, "qty", 0.0)
    monkeypatch.setitem(ggpbot.STATE, "initial_qty", 1.0)
    monkeypatch.setitem(ggpbot.STATE, "remaining_qty", 0.0)
    monkeypatch.setitem(ggpbot.STATE, "exchange_qty", 0.0)
    monkeypatch.setitem(ggpbot.STATE, "trade_id", None)
    monkeypatch.setitem(ggpbot.STATE, "entry_time", time.time())
    monkeypatch.setitem(ggpbot.STATE, "last_close_fill_price", 101.0)
    monkeypatch.setitem(ggpbot.STATE, "last_close_realized_pnl", 1.0)
    monkeypatch.setitem(ggpbot.STATE, "last_close_pnl_source", "BINGX_ORDER_REALIZED_PNL")
    monkeypatch.setitem(ggpbot.STATE, "last_close_pnl_complete", True)
    monkeypatch.setitem(ggpbot.STATE, "last_close_reason", "TEST_VERIFIED_CLOSE")
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setitem(ggpbot.STATE, "close_reconciliation_required", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", True)
    monkeypatch.setitem(ggpbot.STATE, "initial_margin", 5.0)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", True)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "symbol", "BTC/USDT:USDT")

    ggpbot.finalize_trade_with_reality("BTC/USDT:USDT", "TEST_VERIFIED_CLOSE")

    assert any(
        "[ENTRY_LOCK_RETAINED] BTC/USDT:USDT: unresolved safety gates: "
        "RECOVERY_HOLD,RECOVERY_BLOCK_ENTRIES" in message for message in messages)
    assert ggpbot._ACTIVE_TRADE is True


# --------------------------------------------------------------------------- #
# Daily loss protection / observability
# --------------------------------------------------------------------------- #
def _reset_loss_state(monkeypatch, journal, clock):
    import ggpbot

    monkeypatch.setattr(ggpbot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STORE_READY", False)
    monkeypatch.setattr(ggpbot, "LOSS_COOLDOWN_SECONDS", 600)
    monkeypatch.setattr(ggpbot.time, "time", lambda: clock[0])
    monkeypatch.setitem(ggpbot.STATE, "open", False)
    monkeypatch.setitem(ggpbot.STATE, "recovery_hold", False)
    monkeypatch.setitem(ggpbot.STATE, "execution_unresolved", False)
    monkeypatch.setitem(ggpbot.STATE, "close_reconciliation_required", False)
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setitem(ggpbot.TRADE_STATE, "in_position", False)
    monkeypatch.setitem(ggpbot.MEMORY, "position_status", "FLAT")
    ggpbot._LOSS_COOLDOWN_STATE.clear()
    ggpbot._LOSS_COOLDOWN_STATE.update({
        "until": 0.0, "triggered_at": 0.0, "trigger_balance": None,
        "utc_day": None, "peak_balance": None, "trigger_count": 0,
    })


def test_daily_loss_pauses_entries_for_ten_minutes_and_persists(tmp_path, monkeypatch):
    import ggpbot
    from roro_trade_journal import TradeJournal

    clock = [time.time()]
    journal = TradeJournal(str(tmp_path / "journal.sqlite3"))
    _reset_loss_state(monkeypatch, journal, clock)
    balances = {"value": 100.0}
    monkeypatch.setattr(ggpbot, "get_balance_safe", lambda: balances["value"])

    assert ggpbot.daily_loss_cooldown_active() is False
    assert ggpbot._LOSS_COOLDOWN_STATE["peak_balance"] == 100.0

    balances["value"] = 94.0
    assert ggpbot.daily_loss_cooldown_active() is True
    assert ggpbot._LOSS_COOLDOWN_STATE["until"] == clock[0] + 600
    assert ggpbot._LOSS_COOLDOWN_STATE["trigger_count"] == 1

    # Restart: persisted pause survives process reload and is not re-triggered.
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STORE_READY", False)
    ggpbot._LOSS_COOLDOWN_STATE.update({
        "until": 0.0, "triggered_at": 0.0, "trigger_balance": None,
        "utc_day": None, "peak_balance": None, "trigger_count": 0,
    })
    assert ggpbot.daily_loss_cooldown_active() is True
    assert ggpbot._LOSS_COOLDOWN_STATE["trigger_count"] == 1

    clock[0] += 601
    assert ggpbot.daily_loss_cooldown_active() is False
    assert ggpbot._LOSS_COOLDOWN_STATE["until"] == 0.0
    assert ggpbot._LOSS_COOLDOWN_STATE["peak_balance"] == 94.0


def test_elapsed_loss_pause_stays_blocked_while_position_open(tmp_path, monkeypatch):
    import ggpbot
    from roro_trade_journal import TradeJournal

    clock = [time.time()]
    journal = TradeJournal(str(tmp_path / "journal.sqlite3"))
    _reset_loss_state(monkeypatch, journal, clock)
    balances = {"value": 100.0}
    monkeypatch.setattr(ggpbot, "get_balance_safe", lambda: balances["value"])
    ggpbot.daily_loss_cooldown_active()

    balances["value"] = 94.0
    assert ggpbot.daily_loss_cooldown_active() is True
    clock[0] += 601
    monkeypatch.setitem(ggpbot.STATE, "open", True)

    assert ggpbot.daily_loss_cooldown_active() is True
    assert ggpbot._LOSS_COOLDOWN_STATE["until"] > 0

    monkeypatch.setitem(ggpbot.STATE, "open", False)
    assert ggpbot.daily_loss_cooldown_active() is False


def _install_healthy_main_loop(monkeypatch):
    """Install a real supervised, genuinely progressing main_loop observation."""
    import ggpbot
    from naiso_health import HealthWatchdog

    watchdog = HealthWatchdog(monotonic_fn=time.monotonic)
    watchdog.register("main_loop", interval_sec=5.0, grace_sec=60.0,
                      critical=True, restartable=True)
    monkeypatch.setattr(ggpbot, "NAISO_WATCHDOG", watchdog)
    thread = threading.current_thread()
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_OWNER", {
        "thread_id": thread.ident, "thread_name": thread.name, "registered": True})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_PROGRESS_SEQ", 1)
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_LAST_OWNER_PROGRESS", {
        "seq": 1, "ts": watchdog.monotonic(), "thread_id": thread.ident,
        "thread_name": thread.name})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "_LOSS_COOLDOWN_STATE", {"until": 0.0})
    monkeypatch.setattr(ggpbot, "_MAIN_LOOP_RESTART_STATE", {
        "in_progress": False, "attempts": 0, "last_reason": "",
        "supervisor_exhausted": False})
    for key in ("open", "recovery_hold", "execution_unresolved", "close_reconciliation_required"):
        monkeypatch.setitem(ggpbot.STATE, key, False)
    monkeypatch.setitem(ggpbot.MEMORY, "recovery_status", "READY")
    watchdog.beat("main_loop", state="RUNNING", phase="iteration_start", progress=True)
    return watchdog


def test_naiso_health_endpoint_exposes_recovery_and_loss_state(monkeypatch):
    import ggpbot

    _install_healthy_main_loop(monkeypatch)
    response = ggpbot.app.test_client().get("/naiso/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["version"] == "NAISO-LIVE-v1.1.0-longrun"
    for key in ("maintenance", "threads", "api", "positions", "recovery", "daily_loss", "reconciliation"):
        assert key in payload
    assert payload["daily_loss"]["limit_pct"] == ggpbot.MAX_DAILY_LOSS_PCT
    assert "block_entries" in payload["recovery"]
