"""Offline deterministic soak harness for NAISO-LIVE v1.1.0 long-run guards.

This is intentionally independent of exchange credentials, the network and the
Flask runtime. It repeatedly exercises the same bounded state machines used by
the live bot so memory/history growth and fail-closed behaviour can be observed
without claiming anything about real one-year uptime.

Run: ``python soak_naiso_longrun.py``
"""
from __future__ import annotations

import tempfile
import time
from pathlib import Path

from bingx_execution import PositionMode, TradeExecutionService
from naiso_api_resilience import ApiResilience, CircuitOpenError
from naiso_health import HealthWatchdog, RestartPolicy
from naiso_maintenance import MaintenanceManager
from roro_trade_journal import TradeJournal


CYCLES = 20_000
SL_CYCLES = 5_000


def soak_api_resilience():
    clock = [0.0]
    api = ApiResilience(
        failure_threshold=3, cooldown_sec=10.0, max_attempts=3,
        base_backoff_sec=0.001, max_backoff_sec=0.002, jitter_fraction=0.0,
        seed=11, monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
    sent = 0
    rejected = 0
    for cycle in range(CYCLES):
        clock[0] = cycle * 0.25
        try:
            api.before_call("fetch_positions")
        except CircuitOpenError:
            rejected += 1
            continue
        sent += 1
        # Three consecutive failures open the circuit; successes between
        # bursts reset it, matching real endpoint behaviour.
        if cycle % 97 in (0, 1, 2):
            api.record_failure("fetch_positions", TimeoutError("simulated timeout"))
        else:
            api.record_success("fetch_positions")
    metrics = api.metrics()
    endpoint = metrics["endpoints"]["fetch_positions"]
    assert sent > 0 and rejected > 0
    assert endpoint["state"] in {"CLOSED", "OPEN", "HALF_OPEN"}
    assert 0.0 <= endpoint["open_remaining_sec"] <= api.cooldown_sec
    assert metrics["counters"]["circuit_opens"] > 0
    assert len(metrics["endpoints"]) == 1
    return {"sent": sent, "fenced": rejected, "circuit_opens": metrics["counters"]["circuit_opens"],
            "final_state": endpoint["state"], "history_bounded": True}


def soak_restart_policy():
    clock = [0.0]
    policy = RestartPolicy(max_attempts=3, window_sec=300.0, base_delay_sec=1.0,
                           max_delay_sec=4.0, jitter_fraction=0.0, seed=12,
                           monotonic_fn=lambda: clock[0], sleep_fn=lambda _s: None)
    exhausted = 0
    restarts = 0
    last_restart_cycle = -1
    for cycle in range(CYCLES):
        clock[0] = cycle * 1.0
        delay = policy.next_delay("main_loop")
        if delay is None:
            exhausted += 1
            continue
        record = policy.begin("main_loop", f"cycle_{cycle}", delay)
        policy.finish(record, "RESTARTED")
        restarts += 1
        last_restart_cycle = cycle
    history = policy.records("main_loop", limit=CYCLES)
    assert exhausted > 0 and restarts > 0
    assert len(history) == RestartPolicy.MAX_RECORDS_PER_COMPONENT
    assert history[0]["reason"] == f"cycle_{last_restart_cycle}"
    return {"restarts": restarts, "budget_exhausted_cycles": exhausted,
            "history_retained": len(history), "last_restart_cycle": last_restart_cycle}


class _Resources:
    def cleanup(self, preserve_keys=()):
        return {"removed": 1}


class _Journal:
    def __init__(self):
        self.checkpoints = 0
        self.integrity = 0
        self.prunes = 0

    def checkpoint_wal(self, mode):
        self.checkpoints += 1
        return {"mode": mode, "complete": True}

    def integrity_check(self):
        self.integrity += 1
        return {"ok": True, "result": "ok"}

    def prune_runtime_state(self, *, ttl_days, protected_keys):
        self.prunes += 1
        return {"removed_runtime_keys": 0, "protected_keys": sorted(protected_keys)}


def soak_maintenance():
    clock = [0.0]
    state = {"open": False, "recovery_hold": False, "native_sl_verified": True}
    journal = _Journal()
    manager = MaintenanceManager(
        state_provider=lambda: dict(state), journal_provider=lambda: journal,
        resource_provider=_Resources, monotonic_fn=lambda: clock[0],
        min_interval_sec=300.0)
    light = 0
    heavy = 0
    skipped = 0
    for cycle in range(CYCLES):
        clock[0] = cycle * 1.0
        # Alternate unsafe and safe windows to prove heavy work never runs with
        # open exposure or an unresolved gate.
        state["open"] = bool(cycle % 2)
        result = manager.run()
        if result.get("mode") == "LIGHT":
            light += 1
            assert result["flags"]["position_open"] is True
        elif result.get("mode") == "HEAVY":
            heavy += 1
            assert result["flags"]["position_open"] is False
        else:
            skipped += 1
    assert journal.checkpoints == heavy
    assert journal.integrity == heavy
    assert journal.prunes == heavy
    return {"light": light, "heavy": heavy, "throttled": skipped,
            "journal_checkpoints": journal.checkpoints}


def _stop_row(price, order_id):
    return {"id": order_id, "symbol": "BTC/USDT:USDT", "type": "STOP_MARKET",
            "side": "sell", "positionSide": "LONG", "status": "NEW",
            "stopPrice": price, "origQty": 2.0,
            "info": {"type": "STOP_MARKET", "orderId": order_id}}


class _StopExchange:
    def __init__(self):
        self.rows = [_stop_row(100.0, "existing-sl")]
        self.create_calls = 0

    def market(self, symbol):
        return {"id": "BTCUSDT"}

    def price_to_precision(self, symbol, price):
        return f"{float(price):.8f}"

    def fetch_open_orders(self, *args, **kwargs):
        return list(self.rows)

    def swapV2PrivateGetTradeOpenOrders(self, params):
        return {"code": 0, "data": list(self.rows)}

    def create_order(self, *args, **kwargs):
        self.create_calls += 1
        raise AssertionError("a verified existing stop must never be duplicated")


def soak_native_stop_idempotency():
    exchange = _StopExchange()
    service = TradeExecutionService(exchange)
    for _ in range(SL_CYCLES):
        result = service.ensure_native_stop(
            symbol="BTC/USDT:USDT", side="BUY", quantity=2.0, trigger_price=100.0,
            mode=PositionMode.HEDGE, position_side="LONG")
        assert result.outcome == "VERIFIED_ADOPTED"
        assert result.verified is True
    assert exchange.create_calls == 0
    return {"cycles": SL_CYCLES, "duplicate_orders": exchange.create_calls}


def soak_journal_protection():
    with tempfile.TemporaryDirectory(prefix="naiso-soak-journal-") as directory:
        journal = TradeJournal(str(Path(directory) / "journal.sqlite3"))
        protected = "daily_drawdown_cooldown_v1"
        protected_value = {"until": time.time() + 600}
        journal.set_runtime_state(protected, protected_value)
        for cycle in range(1_000):
            journal.set_runtime_state(f"helper_{cycle}", {"cycle": cycle})
        with journal._db() as db:
            db.execute("UPDATE runtime_state SET updated_ts=?", (time.time() - 31 * 86400,))
        result = journal.prune_runtime_state(ttl_days=30, protected_keys=[protected])
        assert result["removed_runtime_keys"] == 1_000
        assert journal.get_runtime_state(protected) == protected_value
        assert journal.integrity_check()["ok"] is True
        return {"pruned_helper_keys": result["removed_runtime_keys"],
                "protected_key_preserved": True, "integrity": "ok"}


def main():
    results = {
        "api_resilience": soak_api_resilience(),
        "restart_policy": soak_restart_policy(),
        "maintenance": soak_maintenance(),
        "native_stop": soak_native_stop_idempotency(),
        "journal": soak_journal_protection(),
    }
    print("SOAK_OK deterministic offline long-run guards")
    for name, result in results.items():
        print(f"  {name}: {result}")


if __name__ == "__main__":
    main()
