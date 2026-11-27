# Unified EMA9 + Session VWAP cross lifecycle — engineering report

**Status:** offline regression verified; **not LIVE ready**. No exchange call, order,
balance query, API-key change or demo/LIVE trade was performed. BingX paths were
changed only by reusing the existing order path and identity plumbing, never by
bypassing a gate.

## 1. Modified files and functions

| File | Functions / additions |
|---|---|
| `ggp_cross_lifecycle.py` (new) | `CrossLifecycleState`, `adx_gate`, `make_cross_event_id`, `normalize_cross_event`, `merge_cross_events`, `transition_state`, `_zone_quality`, `evaluate_cross_location`, `build_location_features`, `cross_rank`, `rank_cross_candidates`, `execution_readiness`. Execution-free: no data fetch and no order call. |
| `ggpbot.py` | Imports the lifecycle module; `_enrich_with_existing_gpp_engines()` converts existing Scanner-2 proximity into retained cross events and logs them; `smart_opportunity_selection()` adds unresolved-state guard, cross-only ready filter, cross ranking, side-specific re-analysis, location re-evaluation, ADX readiness gate, context Event ID and lifecycle transitions; `execute_entry()` uses the cross Event ID as the durable event identity and logs reaching the final boundary; the serialized `execute_entry()` wrapper logs order sent / filled quantity / position verified / protection verified; `LiveTradeManager._apply_management()` supplies ATR% and volatility band context. |
| `ggp_institutional_bridge.py` | `analyze_symbol()` accepts `cross_event`/`cross_context`, evaluates the cross's own location, and returns quality, evidence, analysis status and rejection reason; insufficient-data rows carry the same cross fields; `analyze_symbol_both()` selects the cross event by side without changing the legacy call shape; `update_unified_watchlist()` matches cross events to the candidate and adds cross quality / approval to ranking. |
| `ggp_profit_brain.py` | `select_range_target_pct()` explicitly maps `LOW_VOL -> 0.25%` and `HIGHER_VOL -> 0.50%` price moves. |
| `test_cross_lifecycle.py` (new) | 18 offline focused tests for all mandatory scenarios plus AST checks that protected execution/management functions remain in place. |
| `UNIFIED_CROSS_LIFECYCLE_PLAN.md` (new) | File/function-linked implementation plan written before the edits. |
| `UNIFIED_CROSS_LIFECYCLE_DIFF.txt` (new) | Exact staged Git diff for this change set. |
| `UNIFIED_CROSS_LIFECYCLE_REPORT.md` (new) | This report. |

## 2. Before / after data flow

**Before.** `global_discovery` and `watchlist_analysis` both ran, but EMA9/Session-VWAP
proximity was ranking metadata in the Watchlist and a hard gate only inside
`execute_entry()`. There was no cross Event ID, no retained cross state, no
cross-location quality class and no single identity across Scanner → Watchlist →
Institutional Engine → Entry. `OpportunityPipeline` existed but was not wired into
production.

**After.**

1. Existing Scanner 2 calls `_enrich_with_existing_gpp_engines()`, which calls the
   unchanged `roro_confluence.ema9_vwap_proximity()` on closed candles.
2. `normalize_cross_event()` creates APPROACHING or CONFIRMED candidates. Confirmed
   crosses get a stable `cross_event_id` from symbol + timeframe + direction +
   closed-candle timestamp. `merge_cross_events()` retains candidates after the visual
   cross disappears until their own expiry/invalidation.
3. `update_unified_watchlist()` attaches matching cross evidence to each Watchlist
   row. The Watchlist remains the cross-event list; it is not a generic institutional
   opportunity list.
4. `analyze_symbol()` annotates the existing institutional hypothesis with
   cross-location analysis. It does not create a second opportunity and does not
   bypass `UnifiedEntryAuthority`.
5. `smart_opportunity_selection()` only considers confirmed, institutionally approved
   crosses, re-evaluates BUY and SELL independently, checks the same closed-candle ADX
   source used by the Entry Engine, transitions `EXECUTION_READY`, and supplies the
   cross Event ID to the existing `execute_entry()`.
6. `execute_entry()` keeps every existing final gate. When a cross Event ID is
   present and the EMA9/VWAP gate is ready, that ID becomes the durable journal event
   identity, so the same confirmed cross cannot submit two orders even after restart.
7. `open_position()` → `TradeExecutionService.submit_market_entry()` →
   `exchange.create_order()` (BingX) → fill/position verification → native stop
   verification is unchanged. The lifecycle logs `execute_entry_reached`, order sent,
   filled quantity, position verified and protection verified.
8. Existing `TradeManagementBoard` owns all exits. After verified close, existing
   reconciliation releases the one-position lock and scanning resumes.

## 3. How cross-location analysis works

`build_location_features()` maps the institutional context already built by
`ggp_institutional_bridge._build_context()` and the enrichment engines into explicit
boolean/evidence features:

- demand/supply zone proximity from golden-zone alignment;
- direction-valid, non-mitigated Order Block and direction-valid FVG;
- liquidity sweep + reclaim for BUY, buy-side sweep + rejection for SELL;
- confirmed Market Structure / CHOCH and premium-discount alignment;
- real value area versus mid-range chop (zone evidence must exist);
- room to opposing liquidity / resistance / support / previous swing high-low;
- Smart Money bias, Momentum flow, volume confirmation, institutional trend support;
- available price space in ATR units.

`evaluate_cross_location()` returns one of four explainable classes:
`HIGH QUALITY CROSS`, `GOOD CROSS`, `WEAK CROSS`, `INVALID CROSS`. HIGH/GOOD are
executable candidates only when ADX >= 18 and no critical location condition fails.
A high Score alone never approves a cross. All booleans and numeric inputs are kept in
`evidence` and all failures in `rejection_reasons`.

ADX is a hard gate: `adx_gate(17.99)` rejects; `adx_gate(18.00)` passes only the ADX
gate. The same `compute_adx(closed_df).iloc[-1]` value used by `execute_entry()` is
passed into `execution_readiness()` immediately before authorization.

## 4. Ranking

`rank_cross_candidates()` ranks by cross quality class, proximity
(`cross_distance_atr`), OB validity, FVG validity, liquidity confirmation, Market
Structure, ADX, Momentum, Institutional Flow, available price space, R:R, freshness,
institutional approval and Event-ID uniqueness. Duplicate Event IDs are demoted with
`final_rejection_reason = duplicate_cross_event_id`. The prior location-first rank is
retained as a deterministic tie-breaker, and `update_unified_watchlist()` adds cross
quality / approval bonuses so better crosses are analyzed first.

## 5. BingX entry path

No protected file was replaced. The exact existing path remains:

`smart_opportunity_selection()` → `execute_entry()` → `open_position()` →
`TradeExecutionService.submit_market_entry()` → `bingx_execution.py` order send →
fill/order readback → `_position()` direction proof →
`place_and_verify_native_stop()` → existing `TradeManagementBoard`.

The only change at this boundary is identity and observability: the cross Event ID is
used as `durable_event_id`, and the serialized wrapper logs the actual accepted flag,
order id presence, filled quantity, verified position and verified protection.
Durable `is_event_consumed()` / `consume_event()` semantics are unchanged.

## 6. Scalp / Trend selection

`LiveTradeManager._apply_management()` computes ATR% from closed-frame ATR divided by
mark price and labels `LOW_VOL` (<0.40%), `HIGHER_VOL` (>=0.80%) or `NORMAL`.
`ggp_profit_brain.select_range_target_pct()` turns these bands into explicit price-move
targets of 0.25% and 0.50%. These are price movements, not leveraged ROE.

- `RANGE_SCALP`: one target, full remaining close, no TP1/TP2 and no splitting.
- `TREND_RIDE`: existing TP1 at 50% of original filled quantity near the liquidity /
  structural target, then structural / liquidity TP2 for the runner; no TP3.
- All decisions remain inside `TradeManagementBoard`; no Profit Engine or parallel
  manager was added.

## 7. Actual test results

| Command | Result |
|---|---|
| `python -m py_compile ggp_cross_lifecycle.py ggp_institutional_bridge.py ggp_profit_brain.py ggpbot.py test_cross_lifecycle.py` | clean |
| `python -m pytest -q test_cross_lifecycle.py` | **18 passed in 5.38s** |
| Focused + integration subset (`test_cross_lifecycle.py`, `test_entry_event_idempotency.py`, `test_ema9_vwap_soft_gate.py`, `test_profit_brain.py`, `test_profit_target_ladder.py`, `test_dual_scanner_dashboard.py`, `test_ggp_roro_integration.py`) | **87 passed in 23.67s** |
| Full offline regression `python -m pytest -q` | **636 passed, 1 warning in 126.25s** |

Baseline before edits was `618 passed, 1 warning in 177.13s`. The delta is the 18 new
focused tests. No Timeout was raised, no test was skipped or marked xfail, and no
failing test was ignored. The single warning is a pre-existing Pandas deprecation in
`test_professional_discovery_scanner.py`.

## 8. Remaining risks

1. **No live/demo verification.** Fill quantity, native protection, close verification
   and restart recovery are proven only by offline tests and existing regression
   coverage. LIVE readiness is not claimed.
2. **Portfolio cap policy not present.** The requested `5 technical/institutional + 1
   news, 6 total` cap does not exist in the codebase. Runtime still uses the existing
   one-open-position / one-active-execution lock. It is documented here rather than
   invented as a parallel system.
3. **Pre-existing native SL contradiction.** One helper labels native SL immutable
   while the manager repairs it through `replace_native_stop()`. This change does not
   alter that protected behavior.
4. **Pre-existing `sync_position_state()` UNKNOWN contradiction.** `fetch_live_snapshot()`
   raises on UNKNOWN before the wrapper's UNKNOWN branch. Not changed here.
5. **Post-fill event consumption failure** blocks new entries but cannot unwind an
   already filled position; inherited fail-closed behavior.
6. **Same-symbol opposing crosses** are both analyzed, but the Watchlist currently
   stores one best row and the existing conflict logic may reject both when both
   structural hypotheses are `ENTRY_READY`. This was not broadened beyond preserving
   cross identity.
7. **Volatility bands** use simple ATR% thresholds; regime stabilization and hysteresis
   remain the responsibility of the existing engines.
8. Stale `RELEASE_MANIFEST.txt` in the installed ZIP is intentionally untouched to
   preserve ZIP fidelity.

## 9. Git

The implementation is committed independently on branch
`unified-cross-lifecycle` from clean `main` at
`908f29c836b417b0dac7560611cc5c6d7097efbb`:

- Implementation commit: `1f80d691ff74ebe71a728e82b27ac014c6145899`
  (`Unify EMA9/VWAP cross lifecycle from scanner to verified close`).
- Pull request: https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/44
- The exact implementation patch is in `UNIFIED_CROSS_LIFECYCLE_DIFF.txt`.
- This report's final metadata is a small follow-up commit on the same branch.

No commit or PR claims LIVE readiness; both are for offline-verified code only.
