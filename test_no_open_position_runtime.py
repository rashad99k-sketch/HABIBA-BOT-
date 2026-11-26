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
    calls = []

    monkeypatch.setattr(
        ggpbot, "resolve_position_symbol",
        lambda symbol: ("", "INVALID_BASE_ASSET", {
            "original_symbol": symbol, "normalized_symbol": "", "source": "NONE",
        }),
    )

    def forbidden_fetch(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("exchange must not be called with an invalid symbol")

    monkeypatch.setattr(ggpbot, "fetch_ohlcv", forbidden_fetch)
    frame, source = ggpbot.get_ohlcv_safe_meta(":USDT", 100)
    assert frame is None
    assert source == "INVALID_SYMBOL"
    assert calls == []


def test_one_bad_symbol_does_not_interrupt_valid_scanner_symbols(monkeypatch):
    ggpbot = _load(monkeypatch)
    requested = []
    raw_symbols = ["", ":USDT", "BTC/USDT", "BTC/USDT:USDT"]

    def validate(symbol, exchange=None):
        mapping = {
            "": ("", "INVALID_BASE_ASSET", {"source": "NONE"}),
            ":USDT": ("", "INVALID_BASE_ASSET", {"source": "NONE"}),
            "BTC/USDT": ("BTC/USDT:USDT", "", {"source": "MARKETS"}),
            "BTC/USDT:USDT": ("BTC/USDT:USDT", "", {"source": "MARKETS"}),
        }
        return mapping.get(symbol, ("", "MARKET_NOT_FOUND", {"source": "NONE"}))

    monkeypatch.setattr(ggpbot, "resolve_position_symbol", validate)
    monkeypatch.setattr(
        ggpbot, "_get_scanner_frame",
        lambda symbol, limit: requested.append(symbol) or None,
    )

    accepted = []
    seen = set()
    for raw in raw_symbols:
        normalized, reason, _ = ggpbot.resolve_position_symbol(raw)
        if normalized and normalized not in seen:
            seen.add(normalized)
            accepted.append(normalized)

    result = ggpbot.scan_market_rf(top_n=10, symbols=accepted)
    assert result == []
    assert requested == ["BTC/USDT:USDT"]


def test_no_position_main_loop_continues_without_open_trade_and_keeps_loop_progress(monkeypatch):
    ggpbot = _load(monkeypatch)
    progress = []
    loop_calls = []

    class FakeExchange:
        markets = {"BTC/USDT:USDT": {"swap": True, "active": True}}
        def load_markets(self):
            return self.markets
        def fetch_positions(self, symbols=None):
            return []

    fake_exchange = FakeExchange()
    monkeypatch.setattr(ggpbot, "ex", fake_exchange)
    monkeypatch.setattr(ggpbot, "safe_api_call", lambda fn, *args, **kwargs: fn(*args, **kwargs))
    monkeypatch.setattr(ggpbot, "get_balance_safe", lambda: 100.0)
    monkeypatch.setattr(ggpbot, "tg_start", lambda *args, **kwargs: None)
    monkeypatch.setattr(ggpbot, "dispatch_scanner_task",
                        lambda name, function, *args, **kwargs: loop_calls.append(name) or True)
    monkeypatch.setattr(ggpbot, "_run_global_discovery_scanner_with_heartbeat", lambda: None)
    monkeypatch.setattr(ggpbot, "update_institutional_flow_scanner",
                        lambda: loop_calls.append("institutional_flow"))
    monkeypatch.setattr(ggpbot, "smart_opportunity_selection",
                        lambda: loop_calls.append("watchlist_analysis"))
    monkeypatch.setattr(ggpbot, "_run_sniper_scan_if_due",
                        lambda: loop_calls.append("sniper_scan"))
    monkeypatch.setattr(ggpbot, "sync_all_states", lambda: None)
    monkeypatch.setattr(ggpbot, "sync_position_state", lambda: None)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(ggpbot, "hourly_cleanup", lambda: None)
    monkeypatch.setattr(ggpbot, "print_snapshot", lambda: None)
    monkeypatch.setattr(ggpbot, "_main_loop_progress",
                        lambda phase, **details: progress.append(phase))
    monkeypatch.setattr(ggpbot, "GLOBAL_SCAN_INTERVAL", 0)
    monkeypatch.setattr(ggpbot, "CANDIDATE_SCAN_INTERVAL", 0)
    monkeypatch.setattr(ggpbot, "SNIPER_V2_ENABLED", True)
    monkeypatch.setattr(ggpbot, "_SNIPER_V2_LAST_SCAN", 0.0)
    monkeypatch.setattr(ggpbot, "PAPER_MODE", True)

    def stop_after_one_iteration(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(ggpbot.time, "sleep", stop_after_one_iteration)
    with pytest.raises(KeyboardInterrupt):
        ggpbot.main_loop_sniper()

    assert ggpbot.STATE["open"] is False
    assert ggpbot.TRADE_STATE["in_position"] is False
    assert "global_discovery" in loop_calls
    assert "watchlist_analysis" in loop_calls
    assert "sniper_scan" in loop_calls
    assert "iteration_start" in progress
    assert "exchange_sync_complete" in progress
    assert "iteration_complete" not in progress or "cleanup" in progress
    assert not any(name == "execute_entry" for name in loop_calls)


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
