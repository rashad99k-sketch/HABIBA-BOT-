"""Regression proof for cross-event LIVE block log classification.

The durable cross-event entry conditions are unchanged. These tests only prove
that the expected fail-closed branch is a WARN, not a System Error/Telegram
ERROR, while real ERROR logs still use the existing routing.
"""
from __future__ import annotations

import pandas as pd

import ggpbot


SYMBOL = "ICP/USDT:USDT"
MESSAGE = (
    f"[CROSS_EVENT] BLOCKED LIVE {SYMBOL} SELL: "
    "entry requires a durable confirmed cross event"
)


def _ctx_without_cross_event():
    frame = pd.DataFrame({"timestamp": [1_800_000_000_000]})
    return {
        "closed_candle_frame": frame,
        "closed_candle_source": "TEST_FRAME",
        "candle_timeframe": "15m",
        "cross_event_id": "",
    }


def test_live_missing_cross_event_is_warn_and_not_system_error(monkeypatch):
    monkeypatch.setattr(ggpbot, "PAPER_MODE", False)
    monkeypatch.setattr(ggpbot, "MODE_LIVE", True)
    monkeypatch.setattr(ggpbot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(
        ggpbot, "_validate_dual_ema_vwap_entry_gate",
        lambda *a, **k: {"ready": True, "durable_event_id": "", "reason": "TEST_GATE"},
    )
    monkeypatch.setitem(ggpbot.DASHBOARD_STATE, "logs", [])
    monkeypatch.setitem(ggpbot.DASHBOARD_STATE, "errors", [])
    alerts = []
    monkeypatch.setattr(ggpbot, "tg_error", lambda msg, level: alerts.append((msg, level)))
    monkeypatch.setattr(ggpbot, "_INSTITUTIONAL_ENTRY_CONTEXT", _ctx_without_cross_event(), raising=False)

    # Two evaluations must stay in normal logs only; repetition is expected
    # re-evaluation, not a new System Error or Telegram ERROR alert.
    for _ in range(2):
        assert ggpbot.execute_entry(
            "SELL", SYMBOL, 1.0, 1.1, 0.9, 0.8, 50,
            "TEST_LIVE_BLOCK", 0.1, "TREND", "TREND", "TREND",
        ) is False

    logs = ggpbot.DASHBOARD_STATE["logs"]
    assert sum(MESSAGE in line for line in logs) == 2
    assert ggpbot.DASHBOARD_STATE["errors"] == []
    assert alerts == []


def test_real_error_log_still_routes_to_system_errors_and_alert(monkeypatch):
    monkeypatch.setitem(ggpbot.DASHBOARD_STATE, "logs", [])
    monkeypatch.setitem(ggpbot.DASHBOARD_STATE, "errors", [])
    alerts = []
    monkeypatch.setattr(ggpbot, "tg_error", lambda msg, level: alerts.append((msg, level)))

    ggpbot.log_execution("TEST_REAL_RUNTIME_FAILURE", "ERROR")

    assert ggpbot.DASHBOARD_STATE["errors"][0].endswith("TEST_REAL_RUNTIME_FAILURE")
    assert alerts == [("TEST_REAL_RUNTIME_FAILURE", "ERROR")]
