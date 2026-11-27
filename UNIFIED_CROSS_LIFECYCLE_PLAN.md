# Unified EMA9 + Session VWAP cross lifecycle — implementation plan

Scope: connect the **two existing** scanners, the unified Watchlist, the existing institutional
engines, the existing `execute_entry` / BingX path, and the existing `TradeManagementBoard`
into one continuous lifecycle. No third scanner, no second executor, no parallel profit engine,
no strategy change (EMA50 stays diagnostic, not an alternate cross).

## Traced before-flow

1. Scanner 1 `ggpbot.run_global_discovery_scanner()` → `run_scanner_v2()` +
   `ggp_discovery_scanner.professional_lightweight_scan()` + `scan_market_rf()` + `radar_score()`
   → `MEMORY["top_candidates"]`.
2. Scanner 2 `ggpbot._enrich_with_existing_gpp_engines()` → `roro_confluence.ema9_vwap_proximity()`
   → `MEMORY["ema9_vwap_proximity_scanner"]`; institutional bridge merge/rank/rotate in
   `ggp_institutional_bridge.update_unified_watchlist()` → `MEMORY["watchlist"]`.
3. `ggpbot.smart_opportunity_selection()` filters `decision_state == ENTRY_READY`, re-analyzes
   with `roro_analyze_symbol_both()`, validates location, calls `execute_entry()`.
4. `execute_entry()` enforces fresh EMA9/VWAP gate + ADX >= 18, then `open_position()` →
   `TradeExecutionService.submit_market_entry()` → `exchange.create_order()` (BingX) →
   position/fill verification → native stop verification.
5. `LiveTradeManager` → `TradeManagementBoard.evaluate()` → `safe_close_position()` /
   `safe_close_partial()` → verified close → `_clear_confirmed_flat_unlocked()` releases the lock
   and scanners resume.

Knot found in the audit: cross proximity was ranking metadata in the Watchlist but a hard gate
inside `execute_entry`, with no cross Event ID, no explicit lifecycle states, no cross-location
quality class, and `OpportunityPipeline` unwired in production.

## File/function-linked changes

| File / function | Change |
|---|---|
| `ggp_cross_lifecycle.py` (new, execution-free) | `CrossLifecycleState`, `normalize_cross_event`, `make_cross_event_id`, `merge_cross_events`, `transition_state`, `adx_gate`, `build_location_features`, `evaluate_cross_location`, `rank_cross_candidates`, `execution_readiness`. |
| `ggpbot._enrich_with_existing_gpp_engines()` | Build cross candidate from the existing proximity result; merge into `MEMORY["cross_events"]`; retain APPROACHING/CONFIRMED candidates across cycles; attach Event ID + candidate fields to the Watchlist row. |
| `ggp_institutional_bridge.analyze_symbol()` | Accept optional `cross_event`; evaluate the cross's own location using context already built by `_build_context`; emit quality class, evidence, `institutional_analysis_status`, `final_rejection_reason`. Never creates an independent opportunity. |
| `ggp_institutional_bridge.analyze_symbol_both()` / `update_unified_watchlist()` | Pass each cross only to its matching direction, so BUY never blocks SELL; include cross quality in ranking; preserve cross identity on the Watchlist row. |
| `ggpbot.smart_opportunity_selection()` | Rank by cross quality/location/ADX/freshness/R:R; require `INSTITUTIONAL_APPROVED`; transition `EXECUTION_READY`; pass `cross_event_id` into `_INSTITUTIONAL_ENTRY_CONTEXT`; log every selection/rejection; transition submitted/filled/rejected from the execution result. |
| `ggpbot.execute_entry()` (core + journal wrapper) | Use the supplied cross Event ID as the durable event identity (same ID from scanner → Watchlist → institutional analysis → entry), keep all existing gates, log reach/send/fill/quantity/verification, and consume the ID only after a verified fill. |
| `ggp_profit_brain.select_range_target_pct()` + `ggpbot._apply_management()` | Explicit low-volatility 0.25% and higher-volatility 0.50% price-move bands using existing ATR evidence; existing `RANGE_SCALP` full-close and `TREND_RIDE` TP1 50%/TP2 structural stay the only management paths. |
| `test_cross_lifecycle.py` (new) | All mandatory focused scenarios plus AST wiring checks. |
| `UNIFIED_CROSS_LIFECYCLE_REPORT.md` (new) | Final engineering report with before/after, tests, risks and commit/PR status. |

## Guardrails preserved

- Protected BingX native SL/trailing/BE, `TradeExecutionService`, exchange sync, startup recovery,
  reconciliation, verified close, TradeManagementBoard ownership, sizing, 20X policy, scanner
  single-flight locks, dashboard/Telegram/Flask contracts, logging and health endpoints remain in
  place. The one-position lock continues to enforce exposure safety; the requested 5+1/6 portfolio
  cap is documented as a remaining risk rather than silently invented as a parallel system.
- ADX comes from the same closed-candle computation in `execute_entry`; 17.99 never passes.
- No Timeout increases, no skipped or xfailed tests, no live exchange calls, no orders sent.

## Verification

1. Focused: `pytest -q test_cross_lifecycle.py` plus existing entry/management/scanner suites.
2. Full offline regression: `pytest -q` (must remain at least 618 passing).
3. Commit on an independent branch; PR only if GitHub permissions allow.
4. LIVE readiness is **not** claimed without exchange/demo verification and a soak run.
