import time

import pandas as pd

from entry_path_diagnostics import (
    clear_entry_path_diagnostics,
    get_entry_path_events,
    get_entry_path_diagnostics,
    record_entry_path_event,
)
from roro_confluence import validate_dual_ema_vwap_entry_gate


def _synthetic_cross_frame():
    start = int((time.time() - 80 * 900) * 1000)
    rows = []
    for i in range(80):
        rows.append({
            "timestamp": start + i * 900_000,
            "open": 100.0,
            "high": 100.2,
            "low": 99.8,
            "close": 100.0,
            "volume": 1000.0,
        })
    frame = pd.DataFrame(rows)
    now_ms = int(frame.timestamp.iloc[-1] + 900_000 + 1_000)

    def ema(values, period):
        out = pd.Series(99.8, index=values.index, dtype=float)
        if period == 9:
            out.iloc[-1] = 100.2
        else:
            out[:] = 100.2
        return out

    return frame, now_ms, ema


def test_synthetic_cross_retest_entry_ready_then_execution_rejection_is_traceable(monkeypatch, tmp_path):
    from roro_institutional import InstitutionalDecision, DecisionState
    import roro_opportunity_pipeline as pipeline_module
    from roro_opportunity_pipeline import OpportunityPipeline, WatchlistStore

    clear_entry_path_diagnostics()

    class ReadyReversal:
        def analyze(self, symbol, side, frame, **kwargs):
            if side == "BUY":
                return InstitutionalDecision(
                    DecisionState.ENTRY_READY.value, "FAILED_AUCTION_REVERSAL", side,
                    "confirmed_failed_auction",
                    ("failed_auction", "closed_mss", "post_mss_retest:synthetic"),
                    (), "synthetic-event-1", int(frame.timestamp.iloc[-5]),
                    100.0, 98.5, 82.0,
                )
            return InstitutionalDecision(
                DecisionState.REJECTED.value, "UNKNOWN", side, "no_structural_setup",
                rejection_reasons=("no_structural_setup",),
            )

    class NoContinuation:
        def analyze(self, symbol, side, frame, **kwargs):
            return InstitutionalDecision(
                DecisionState.REJECTED.value, "UNKNOWN", side, "no_continuation",
                rejection_reasons=("no_continuation",),
            )

    frame, now_ms, _ema = _synthetic_cross_frame()
    cross_ts = int(frame.timestamp.iloc[-3])
    monkeypatch.setattr(pipeline_module, "pre_cross_metrics", lambda *a, **k: {
        "state": "CROSS_CONFIRMED", "cross": "BULLISH",
        "candle_ts": cross_ts, "gap_atr": 0.05,
        "direction": "BUY", "reason": "synthetic_closed_cross",
    })
    pipeline = OpportunityPipeline(WatchlistStore(str(tmp_path / "watchlist.json")))
    pipeline.reversal_engine = ReadyReversal()
    pipeline.continuation_engine = NoContinuation()
    setup = pipeline.analyze("BTC/USDT", frame, now_ms=now_ms)
    assert setup.decision_state == DecisionState.ENTRY_READY.value
    assert setup.pullback_type == "post_mss_retest"

    record_entry_path_event(
        "BTC/USDT", "EMA9_VWAP_GATE", side="BUY",
        decision_state="REJECTED", rejection_reason="stale_ema9_vwap_cross",
        cross_timestamp=setup.crossover_ts,
    )
    events = get_entry_path_events("BTC/USDT")
    stages = [event["stage"] for event in events]
    assert "EMA9_VWAP_CROSS_OBSERVATION" in stages
    assert "INSTITUTIONAL_RETEST_DECISION" in stages
    latest = get_entry_path_diagnostics("BTC/USDT")
    assert latest["decision_state"] == "REJECTED"
    assert latest["rejection_reason"] == "stale_ema9_vwap_cross"
    assert latest["cross_timestamp"] is not None
    assert latest["cross_age"] is not None


def test_synthetic_cross_retest_entry_ready_and_bingx_fill_are_traceable():
    clear_entry_path_diagnostics()
    frame, now_ms, ema = _synthetic_cross_frame()
    gate = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", ema, timeframe_seconds=900, now_ms=now_ms,
    )
    assert gate["ready"] is True
    cross_ts = gate.get("ema9_cross_timestamp") or gate.get("latest_closed_timestamp")

    for stage, state in (
        ("CROSS_CONFIRMED", "CROSS_CONFIRMED"),
        ("RETEST_CONFIRMED", "RETEST_CONFIRMED"),
        ("INSTITUTIONAL_DECISION", "ENTRY_READY"),
        ("LOCATION_GATE", "PASSED"),
        ("EMA9_VWAP_GATE", "PASSED"),
        ("ADX_GATE", "PASSED"),
        ("EXECUTE_ENTRY_ENTERED", "ENTERED"),
        ("OPEN_POSITION_DISPATCH", "SUBMITTED"),
        ("OPEN_POSITION_RESULT", "RETURNED_ORDER"),
        ("BINGX_RESULT", "FILLED"),
    ):
        record_entry_path_event(
            "ETH/USDT", stage, side="BUY", decision_state=state,
            cross_timestamp=cross_ts,
            details={"synthetic": True},
        )

    events = get_entry_path_events("ETH/USDT")
    assert len(events) == 10
    assert events[-1]["stage"] == "BINGX_RESULT"
    assert events[-1]["decision_state"] == "FILLED"
    assert all(event["cross_timestamp"] is not None for event in events)
    assert all(event["cross_age"] is not None for event in events)


def test_diagnostic_failures_never_raise_or_change_caller_value():
    sentinel = {"entry_decision": False}
    observed = record_entry_path_event(
        "SOL/USDT", "TEST", decision_state="REJECTED",
        details={"not_json_native": object()},
    )
    assert sentinel == {"entry_decision": False}
    assert observed["stage"] == "TEST"
