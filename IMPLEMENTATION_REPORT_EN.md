# NAISO-LIVE EMA/VWAP Strategy Integration — Engineering Report

## Status

**Implementation status: partial / not production accepted.** A persistent closed-candle EMA9/EMA15 + UTC-session VWAP pullback/rejection/confirmation engine has been added and connected as a required strategy gate immediately before the existing automated GGP `execute_entry()` path. Existing sizing, recovery, execution, fill verification, native stop, and trade-management code were not replaced.

This does **not** yet meet the prompt’s final objective that EMA/VWAP be the sole primary entry authority and institutional analysis be only a location/risk filter. The current automated selector still requires the pre-existing institutional watchlist to mark a candidate `ENTRY_READY` before the new strategy gate is reached. That is a meaningful remaining architectural blocker, so this project must not be described as production-ready or live-ready.

No exchange was connected, and no order, paper trade, or BingX Demo transaction was run.

## Forensic entry-flow map

The inspected active path is:

```text
Global Discovery / Scanner
  → candidate memories
  → refresh_unified_institutional_watchlist()
  → update_unified_watchlist() in ggp_institutional_bridge.py
  → closed-candle institutional reversal/continuation analysis
  → smart_opportunity_selection() (active later definition in ggpbot.py)
  → fresh closed-OHLCV revalidation and two-sided institutional analysis
  → existing event/ATR/setup/structural-stop/TP checks
  → EMA/VWAP strategy authorization added in this change
  → existing execute_entry()
  → original GGP sizing/recovery/exchange path
  → verified fill and native-stop checks
```

`run_global_discovery_scanner()` is discovery/ranking, not an order sender. The automated selector calls the original `execute_entry()`; no substitute execution function was introduced. Separate legacy/manual/dashboard entry call sites exist and were not unified by this change. In particular, the dashboard’s manual `/trade` route remains a separate entry route and is not governed by this automated strategy gate.

## Key audit findings

- EMA9/VWAP proximity was already computed for context/ranking, but was not the active selector’s EMA9/EMA15 entry gate. The old institutional pipeline had independent reversal/continuation entry authority; the separate confluence engine was not proven to drive the active `execute_entry()` path.
- `roro_opportunity_pipeline.py` has atomic JSON watchlist persistence and closed-candle validation, but its pullback helper is not called from that module’s main `analyze()` route, and its handoff method is a predicate rather than an execution call.
- `ggp_discovery_scanner.py` accepts a caller-supplied frame fetcher and filters forming bars; the scanner module itself did not prove shared cache use. `ggp_market_data.py` is an injected-calculator snapshot helper and is not the active session-VWAP implementation.
- GGP’s original execution path performs sizing/risk/geometry checks, and the trade lifecycle uses recovery holds, fill/position verification and native-stop checks. Existing management and protection paths were left in place.
- Existing tests are broad for isolated helpers and fake-exchange behavior, but do not prove a complete scanner-to-exchange run or a real Flask dashboard session.

## Files changed

1. `unified_ema_vwap_strategy.py` — new execution-free engine. It stores one setup per symbol in an atomically replaced JSON file, uses confirmed OHLCV only, scans the newest eight closed candles for the latest actual EMA9/EMA15 cross, and uses OHLC4 × supplied volume for VWAP. Its session convention is the UTC calendar day (00:00 UTC reset), explicitly documented in code. It advances pullback → rejection → later confirmation as separate states, checks the eight-bar expiry, and applies 1.5 ATR no-chase and forward TP1-space checks.
2. `ggpbot.py` — existing integration-frame retrieval now uses the scanner frame cache. The strategy lifecycle refresh reads **only** fresh cache entries and does not fetch OHLCV itself. The active automated selector rechecks the strategy authorization using the existing TP1 calculation, then calls the original `execute_entry()`. It marks the strategy state `ENTRY_EXECUTING` before that call and `TRADE_OPEN` only when the existing call reports success. The dashboard `/data` response gains an additive `ema_vwap_strategy_watchlist` field, and the existing watchlist panel displays strategy status and reason.
3. `test_unified_ema_vwap_strategy.py` — 11 focused offline tests for BUY/SELL crosses, closed-bar filtering, session VWAP and zero volume, persistence across restart, pullback/rejection/confirmation sequencing, duplicate candle processing, expiry, TP1 space and no-chase.

No sizing, leverage, execution, position management, native-stop, recovery, or public Flask route behavior was intentionally rewritten.

## Test results

Final complete offline suite command:

```text
python -m pytest -q test_*.py
315 passed, 7 failed, 1 warning
```

The seven failing tests are:

- `test_position_sizing_policy.py::test_doge_raw_quantity_6666_666666`
- `test_position_sizing_policy.py::test_contract_size_and_precision_applied`
- `test_position_sizing_policy.py::test_doge_example_100_buy_allocation_binding`
- `test_position_sizing_policy.py::test_doge_example_33_35_sell_allocation_binding`
- `test_recovery_lifecycle.py::test_native_sl_move_failure_retains_previous_protection`
- `test_recovery_lifecycle.py::test_native_sl_move_failure_holds_only_when_protection_is_truly_lost`
- `test_recovery_lifecycle.py::test_native_sl_move_success_updates_state_and_journal`

I ran those same seven tests against an untouched extraction of the uploaded archive: all seven also failed there. The four sizing tests fail on the archive’s existing allocation/risk sizing expectations; the native-stop tests fail on the archive’s existing fixed-native-stop behavior/expectations. These failures are not caused by the strategy integration. The complete baseline suite was not rerun; only those seven failing tests were baseline-checked.

Focused tests after final changes:

```text
python -m pytest -q test_unified_ema_vwap_strategy.py test_professional_discovery_scanner.py test_ggp_roro_integration.py test_dual_scanner_dashboard.py
35 passed, 1 pre-existing pandas deprecation warning
```

An earlier focused run including the opportunity-pipeline tests passed 45 tests. Python compilation checks passed for `ggpbot.py`, `unified_ema_vwap_strategy.py`, and the new strategy tests.

## API/runtime validation

- The strategy lifecycle polling path uses `_peek_scanner_frame()` and therefore makes **zero OHLCV exchange requests by design**; it waits for a fresh frame already present in the shared scanner cache. This is a code-path expectation, not an exchange-call measurement.
- The existing integration fetch helper now uses the shared scanner cache, which can reduce repeated requests while a cache entry is fresh. No before/after network-call instrumentation was run, so a numeric measured API-call delta is unavailable.
- No dashboard HTTP request was made; dashboard verification is code/compile/unit-test level, not a live HTTP integration test.
- No paper runtime, BingX Demo, or BingX Live test was performed. No credential or exchange action was used.

## Remaining acceptance blockers

1. **Entry authority is not fully unified.** The strategy gate is additional and required, but old institutional `ENTRY_READY` remains a prerequisite. This means missing Sweep/BOS/MSS or another old setup requirement can still prevent an otherwise valid EMA/VWAP strategy setup from reaching execution.
2. **Institutional analysis has not been reduced to a location-only validator.** The requested opposing-zone distance/TP1-consumption policy and `UNKNOWN` handling are not implemented as an independent location filter in the active route.
3. **The new engine advances only symbols represented in the existing unified watchlist and present in the cache.** It does not yet create a complete strategy-owned candidate universe independent of institutional eligibility.
4. TP1 distance uses the existing GGP target output and a minimum 0.35 ATR gross-space threshold; fees, slippage and the project’s approved minimum net-space policy have not been established or applied.
5. Acceptance requirements for full recovery/timeout idempotency, partial-fill/native-stop behavior, per-symbol synchronization, API-rate-limit instrumentation, and production dashboard rendering still need end-to-end integration tests.
6. Session reset is UTC 00:00 by explicit convention; this is not a claim that UTC midnight matches any separately defined trading session.

**Recommendation:** review and complete the authority/location-filter refactor, then add isolated paper-mode integration tests and a controlled BingX Demo validation under the project owner’s operational process. Do not enable Live based on this report.
