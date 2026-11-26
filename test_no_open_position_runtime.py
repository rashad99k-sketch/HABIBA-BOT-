"""Runtime regressions for the zero-open-position scanner incident."""
from __future__ import annotations

import pandas as pd
import pytest


def _load(monkeypatch):
    import ggpbot

    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", False)
    monkeypatch.setattr(ggpbot, "STATE", {"open": False, "recovery_hold": False})
    monkeypatch.setattr(ggpbot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(ggpbot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(ggpbot, "MEMORY", {})
    return ggpbot


def test_invalid_market_data_symbol_is_rejected_before_exchange_call(monkeypatch):
    ggpbot = _load(monkeypatch)

    class Exchange:
        def __init__(self):
            self.calls = []
            self.markets = {"BTC/USDT:USDT": {"swap": True, "active": True}}

        def fetch_ohlcv(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return []

    exchange = Exchange()
    monkeypatch.setattr(ggpbot, "ex", exchange)
    assert ggpbot.get_ohlcv_safe(":USDT", 100) is None
    assert exchange.calls == []


def test_one_bad_symbol_does_not_interrupt_valid_scanner_symbols(monkeypatch):
    ggpbot = _load(monkeypatch)
    requested = []
    monkeypatch.setattr(ggpbot, "_get_scanner_frame",
                        lambda symbol, limit: requested.append(symbol) or None)
    result = ggpbot.scan_market_rf(
        top_n=10,
        symbols=["", ":USDT", "BTC/USDT", "BTC/USDT:USDT"],
    )
    assert result == []
    assert requested == ["BTC/USDT:USDT"]


def test_no_position_main_loop_dispatches_blocking_scanners_and_makes_progress(monkeypatch):
    ggpbot = _load(monkeypatch)
    dispatched = []
    progress = []

    monkeypatch.setattr(ggpbot, "safe_api_call", lambda *args, **kwargs: {})
    monkeypatch.setattr(ggpbot, "get_balance_safe", lambda: 100.0)
    monkeypatch.setattr(ggpbot, "tg_start", lambda *args, **kwargs: None)

    class NoopThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            return None

    monkeypatch.setattr(ggpbot.threading, "Thread", NoopThread)
    monkeypatch.setattr(ggpbot, "sync_all_states", lambda: None)
    monkeypatch.setattr(ggpbot, "sync_position_state", lambda: None)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(ggpbot, "hourly_cleanup", lambda: None)
    monkeypatch.setattr(ggpbot, "print_snapshot", lambda: None)
    monkeypatch.setattr(ggpbot, "_main_loop_progress",
                        lambda phase, **details: progress.append(phase))
    monkeypatch.setattr(ggpbot, "dispatch_scanner_task",
                        lambda name, function, *args, **kwargs: dispatched.append(name) or True)
    monkeypatch.setattr(ggpbot, "GLOBAL_SCAN_INTERVAL", 0)
    monkeypatch.setattr(ggpbot, "CANDIDATE_SCAN_INTERVAL", 0)
    monkeypatch.setattr(ggpbot, "SNIPER_V2_ENABLED", True)
    monkeypatch.setattr(ggpbot, "_SNIPER_V2_LAST_SCAN", 0.0)
    monkeypatch.setattr(ggpbot.time, "sleep", lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt()))

    with pytest.raises(KeyboardInterrupt):
        ggpbot._original_main_loop_sniper()

    assert ggpbot.STATE["open"] is False
    assert ggpbot.TRADE_STATE["in_position"] is False
    assert {"global_discovery", "institutional_flow", "watchlist_analysis", "sniper_scan"}.issubset(set(dispatched))
    assert "iteration_start" in progress
    assert "exchange_sync_complete" in progress
    assert not any(name == "execute_entry" for name in dispatched)


def test_watchdog_recovery_flat_paper_mode_never_validates_empty_symbol(monkeypatch):
    ggpbot = _load(monkeypatch)
    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda *_args, **_kwargs: pytest.fail("flat PAPER recovery must not validate a trade symbol"),
    )

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is True
    assert reason == "PAPER_MODE_RECOVERY"
    assert context["flat_verified"] is True
    assert context["local_open"] is False


def test_watchdog_recovery_live_flat_checks_all_positions_without_empty_symbol(monkeypatch):
    ggpbot = _load(monkeypatch)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    calls = []
    monkeypatch.setattr(
        ggpbot, "fetch_position_tri_state",
        lambda symbol=None: calls.append(symbol) or ("FLAT", []),
    )
    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda *_args, **_kwargs: pytest.fail("account-wide flat verification must not validate a trade symbol"),
    )

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is True
    assert reason == "FLAT_VERIFIED"
    assert context["flat_verified"] is True
    assert context["position_query_scope"] == "ALL_MARKETS"
    assert calls == [None]


def test_watchdog_recovery_live_unknown_fails_closed_without_empty_symbol(monkeypatch):
    ggpbot = _load(monkeypatch)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state", lambda symbol=None: ("UNKNOWN", None))
    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda *_args, **_kwargs: pytest.fail("unknown flat-state recovery must not validate a trade symbol"),
    )

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason == "engine_recovery_exchange_state_unknown"
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
    assert context["flat_verified"] is False


def test_watchdog_recovery_live_unexpected_position_fails_closed(monkeypatch):
    ggpbot = _load(monkeypatch)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "fetch_position_tri_state", lambda symbol=None: ("OPEN", [{"symbol": "BTC/USDT:USDT"}]))
    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda *_args, **_kwargs: pytest.fail("account-wide exposure verification must not validate a trade symbol"),
    )

    verified, reason, context = ggpbot._verify_engine_recovery()

    assert verified is False
    assert reason == "engine_recovery_unexpected_exchange_exposure"
    assert context["unexpected_open_positions"] == 1
    assert ggpbot.RECOVERY_BLOCK_ENTRIES is True
