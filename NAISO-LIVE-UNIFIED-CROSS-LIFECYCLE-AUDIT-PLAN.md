# NAISO-LIVE Unified EMA9 + Session VWAP Cross Lifecycle Audit and Plan

## Current verified path

The two existing scanner paths are retained. Broad discovery is dispatched by `global_discovery` and feeds `MEMORY` discovery sources. The rotating institutional path is dispatched by `_run_watchlist_analysis_with_progress()` and calls the later `smart_opportunity_selection()` definition, which refreshes `MEMORY["watchlist"]` through `refresh_unified_institutional_watchlist()` and `ggp_institutional_bridge.update_unified_watchlist()`.

The unified watchlist analyzes both BUY and SELL hypotheses through `analyze_symbol_both()`, retains prior entries in the existing `MEMORY["watchlist"]`, ranks by institutional location/confluence, and revalidates the selected candidate immediately before authorization. The actual entry gateway is `execute_entry()`, which applies the closed-candle EMA9 + canonical Session VWAP gate, durable journal event guard, mandatory ADX gate, sizing, execution, fill verification, and native-stop verification. `TradeManagementBoard` remains the sole profit/exit authority and `finalize_trade_with_reality()` releases the lifecycle only after verified flat reconciliation.

## Confirmed gaps against the new directive

1. `ggp_institutional_bridge._indicator_snapshot()` manually reproduces OHLC4 UTC-session VWAP instead of calling the canonical `roro_confluence.compute_session_vwap()`. It receives a closed frame but does not itself reject a truncated latest UTC session.
2. Cross proximity is currently represented as ranking-only metadata. The durable `event_id` returned by the institutional engines is structural/setup-derived, not explicitly the EMA9/VWAP cross event identity required by the new contract.
3. Watchlist records do not consistently expose a first-class cross status (`APPROACHING`/`CONFIRMED`), cross direction, cross candle timestamp, cross distance, or explicit institutional analysis lifecycle status. Much of this information exists nested under `ema9_vwap_proximity` and decision fields.
4. The entry path revalidates the EMA9/VWAP event at the execution boundary, but the candidate-to-entry context should carry the cross identity and lifecycle state explicitly so logs, dashboard records, and journal events cannot lose it.
5. Rejection/selection diagnostics are present but need explicit cross lifecycle fields and reason propagation for every selected/rejected candidate.

## Minimal implementation plan

- Keep both scanners, the unified watchlist, the existing institutional bridge, `execute_entry()`, TradeManagementBoard, journal, and execution service. Do not add a parallel engine.
- Replace the bridge's duplicate VWAP calculation with `compute_session_vwap()` and fail closed on invalid/truncated snapshots.
- Add deterministic cross-event identity derived from canonical symbol, side, trigger, and closed cross candle timestamp. Preserve structural `event_id` separately for compatibility.
- Normalize cross metadata into watchlist/result fields: `cross_status`, `cross_direction`, `cross_event_id`, `cross_candle_timestamp`, `ema9`, `ema50`, `session_vwap`, `cross_distance`, `data_freshness`, `institutional_analysis_status`, and `final_rejection_reason`.
- Carry those fields into `_INSTITUTIONAL_ENTRY_CONTEXT` and emit explicit lifecycle logs without changing execution contracts.
- Keep `ENTRY_READY` distinct from `EXECUTION_READY`; only the final revalidation, event journal guard, location checks, ADX gate, risk gates, and protection verification may reach execution.
- Add focused tests for canonical bridge VWAP, approaching/confirmed metadata, BUY/SELL cross identity stability, event persistence/duplicate prevention, selection/ranking, and rejected-location cases. Then rerun the existing focused and full suites.

## Protected scope

Native BingX stop/trailing/breakeven protection, TradeExecutionService, exchange synchronization, recovery, verified close pipeline, position sizing, 20x leverage, scanner single-flight controls, dashboard/API/Telegram contracts, and TradeManagementBoard ownership remain unchanged.
