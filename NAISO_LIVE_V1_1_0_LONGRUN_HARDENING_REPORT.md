# NAISO-LIVE v1.1.0 Long-Run Stability & Self-Healing Hardening Report

Date: 2026-11-24  
Branch: `fix/naiso-live-v1.1.0-longrun-hardening`  
Base: `origin/main` at `a2d446c`  
Status: implementation complete on an isolated branch; **not merged to `main` and not approved for deployment**.

## 1. Safety boundary

This work was performed in a local clone with:

- `PAPER_MODE=true` by default in tests and local execution.
- No real BingX API keys.
- No live orders submitted.
- No `LIVE_TRADING` enablement.
- No changes to `main`.
- No claim of a guaranteed one-year uptime. The tests are offline simulations plus repeated full-suite runs; they cannot prove live exchange availability or venue behavior.

The automatic native-stop repair path is **opt-in** via `NAISO_NATIVE_SL_REPAIR=true|1|yes`. It remains disabled by default so startup recovery continues to fail closed unless an operator explicitly authorizes repair. Even when enabled, it can only clear a hold after a fresh exchange read-back proves a correctly scoped stop.

## 2. Reported symptoms and confirmed root causes

| Reported symptom | Confirmed source | Root cause |
|---|---|---|
| `[RECOVERY_HOLD] native_sl_missing \| exposure tracked; retrying journal/exchange reconciliation` | `ggpbot.py` main loop + `_restore_trade_from_journal()` | Restore verified only `native_sl_original_price`. A stop legitimately moved after entry (for example to `native_sl_last_verified_price`) was treated as `MISSING`. `MISSING` means queries succeeded but no candidate strictly matched; it does not prove absence. No bounded repair path existed. |
| `exposure tracked; retrying journal/exchange reconciliation` | Recovery-hold branch in `main_loop_sniper()` | The loop only ran `hourly_cleanup()` and slept. The reconciliation supervisor only called `reconcile(..., force=True)`, which validates position state but never verifies or repairs the native stop, so the hold could persist indefinitely. |
| `[ENTRY_LOCK_RETAINED] ... unresolved safety gates: RECOVERY_HOLD,RECOVERY_BLOCK_ENTRIES` | `finalize_trade_with_reality()` | `_recovery_hold()` sets both gates together. The finalizer correctly snapshots and preserves them before local cleanup; the message is a fail-closed assertion, not evidence that a duplicate order was submitted. |
| Sticky recovery after crash | Startup/runtime recovery | Native stop verification had a single trigger-price candidate and no idempotent adoption/repair flow. Conditional-order visibility differences, hedge/one-way `positionSide`, and trigger quantization could yield `MISSING`. |
| No watchdog/heartbeat/deadlock handling | Thread/loop structure | Existing threads had no liveness registry, stall detection, restart budget, stack snapshot, or reason-coded restart log. `safe_main_loop()` used a fixed five-second retry. |
| Unbounded/slow API failures | `safe_api_call()` | Three attempts with fixed waits, no endpoint-scoped circuit breaker, no failure classification, and no jitter. `create_order` was already non-retryable, but cancel/cancel-replace calls still entered the generic retry loop despite being order mutations. |
| Unsafe heavy maintenance during open exposure | `hourly_cleanup()` | Explicit GC and journal checkpoints were not gated by trade/recovery state. |
| Daily-loss rule untested in isolation | Daily-loss code | 5%/10-minute logic existed and persisted state, but had no direct regression test or health dashboard field. |
| Repeated local test runs could fail | Shared default SQLite journal | A persisted daily-loss cooldown from one pytest run leaked into the next run, incorrectly blocking entry tests. |

## 3. Implemented fixes

### 3.1 Native-stop recovery: verify all journaled levels, then optionally repair idempotently

Files: `ggpbot.py`, `bingx_execution.py`

- Added `verify_native_stop_loss_detailed()`, which verifies the original price plus `native_sl_last_verified_price`, `native_sl_local_target`, `native_sl_original_price`, `synthetic_sl`, and `sl`. Every candidate still uses the existing strict matcher: native `STOP_MARKET`/`TRIGGER_MARKET`, canonical symbol, close side, exact `positionSide`, active status, exact trigger/quantization delta, and quantity coverage.
- `verify_native_stop_loss()` is retained as a two-value compatibility wrapper.
- `UNKNOWN` (query failure) is now explicitly held as `native_sl_unknown`; it is never converted to `MISSING` and never triggers a placement.
- Added `TradeExecutionService.ensure_native_stop()` returning tri-state `StopEnsureResult`:
  - `VERIFIED_ADOPTED`: an existing strict-matching stop is adopted.
  - `PLACED_VERIFIED`: no active stop was proven, one was placed, and fresh read-back succeeded.
  - `HOLD`: conditional query failed, an active stop exists at an unproven level, submission outcome was uncertain, read-back failed, or inputs were insufficient.
- Duplicate prevention is structural: any active conditional stop in the same symbol/side/position scope at a non-matching price returns `HOLD` before any `create_order`.
- Added `_repair_native_sl_from_journal()`. It is opt-in, uses only journal-proven prices, requires exchange confirmation, persists the repair, and otherwise preserves `RECOVERY_HOLD` and `RECOVERY_BLOCK_ENTRIES`.

### 3.2 API resilience

New file: `naiso_api_resilience.py`; wired into `safe_api_call()` in `ggpbot.py`.

- Endpoint-scoped circuit breaker with failure threshold, cooldown, single half-open probe, and full cooldown restart after a failed probe.
- Error classification (`RATE_LIMITED`, `TIMEOUT`, `NETWORK`, `API_FAILURE`, `ORDER_EXECUTION_UNKNOWN`).
- Bounded exponential backoff with small jitter for read calls.
- Order-mutating calls (`create_order`, `create_market_order`, `create_limit_order`, `cancel_order`, `cancel_all_orders`, `swapV1PrivatePostTradeCancelReplace`) are never automatically retried; timeout/uncertainty still sets `execution_unresolved`, `RECOVERY_BLOCK_ENTRIES`, and `ORDER_SUBMISSION_UNCERTAIN`.
- Circuit breaker is disabled in paper mode and can be disabled explicitly with `NAISO_API_CIRCUIT=false`.

### 3.3 Watchdog, heartbeat and bounded restart policy

New file: `naiso_health.py`; wired into `ggpbot.py`.

- `HealthWatchdog` registers core engines (main loop, reconciliation, institutional updater, keep-alive, scanner workers), tracks heartbeats/age/status, and records recent restart history.
- Stalled components produce one alert per incident with a redacted stack snapshot where available.
- `RestartPolicy` enforces a bounded attempt budget with exponential backoff and jitter, and caps diagnostic restart history at 200 records per component so the watchdog cannot grow memory without bound. Restart callbacks are optional; current core components are registered as non-restartable to avoid unsafe blind restarts.
- `safe_main_loop()` now logs a reason-coded `[SUPERVISOR]` restart with bounded jittered backoff capped at 60 seconds instead of a fixed five-second retry.
- Watchdog restart is gated by `_naiso_safety_gate()`: open position, recovery hold, unresolved execution/close, or daily-loss cooldown prevents automatic restart action.

### 3.4 Maintenance mode

New file: `naiso_maintenance.py`; wired into `hourly_cleanup()`; journal helpers in `roro_trade_journal.py`.

- Heavy work (explicit GC, WAL checkpoint, integrity check, runtime-state pruning) runs only when flat and no recovery/execution gate is active.
- During open position or unresolved recovery, only bounded cache cleanup is allowed; exposure, journal rows, and safety state are untouched.
- Added read-only `TradeJournal.integrity_check()`, `size_bytes()`, and protected-key `prune_runtime_state()`. Recovery and daily-loss keys are explicitly protected from pruning.
- Existing resource-manager cleanup no longer forces a blind `gc.collect()` on every cycle.

### 3.5 Observability and daily-loss visibility

- New `/naiso/health` endpoint and expanded `/health` payload expose resource usage, watchdog component status, API counters/circuit state, recovery gates, native-stop status, daily-loss state, reconciliation freshness, scanner task state, and supervisor restart history.
- The main dashboard payload includes the same snapshot as `naiso_health`.
- Telegram alerts are emitted only for `ERROR`/`CRITICAL` watchdog events through the existing notifier; no credentials are logged.

### 3.6 Test isolation

New file: `conftest.py`.

- Pins `TRADE_JOURNAL_PATH` to a session-private temporary directory before `ggpbot` import, preventing persisted daily-loss state from leaking between local/soak runs.

New file: `soak_naiso_longrun.py`.

- Repeats API, restart-budget, maintenance, native-stop and journal-protection cycles offline with injected clocks/fakes.
- No credentials, network calls, orders or Flask runtime are involved.

## 4. Configuration added

| Variable | Default | Purpose |
|---|---:|---|
| `NAISO_NATIVE_SL_REPAIR` | `false` | Opt in to exchange-verified, idempotent native-stop recovery repair. |
| `NAISO_API_CIRCUIT` | `true` | Disable API circuit breaker explicitly (`false`); ignored in paper mode. |
| `API_CIRCUIT_FAILURE_THRESHOLD` | `10` | Endpoint failures before circuit opens (bounded 3-50). |
| `API_CIRCUIT_COOLDOWN_SEC` | `20` | Circuit cooldown in seconds (bounded 5-300). |
| `WATCHDOG_RESTART_MAX_ATTEMPTS` | `3` | Maximum restart attempts per component within the window. |
| `WATCHDOG_RESTART_WINDOW_SEC` | `300` | Restart budget window. |
| `MAINTENANCE_MIN_INTERVAL_SEC` | `300` | Minimum heavy-maintenance interval. |
| `JOURNAL_RUNTIME_TTL_DAYS` | `30` | TTL for stale runtime helper keys only; protected keys are never pruned. |
| `MAINTENANCE_ENABLE_GC` | `true` | Allow explicit GC during safe flat maintenance. |

## 5. Verification performed

| Check | Result |
|---|---:|
| Baseline before changes | `545 passed in 130.75s` |
| Targeted recovery/execution/resource suites after initial fix | `117 passed in 48.50s` |
| Full suite after initial fix, run 1 | `566 passed in 110.16s` |
| Full suite after initial fix, run 2 | `566 passed in 110.16s` |
| Deterministic offline soak (`python soak_naiso_longrun.py`) | `SOAK_OK` (details below) |
| New regression suite after final safety fixes | `23 passed in 7.90s` |
| Full suite after final safety fixes | `568 passed in 113.68s` |
| `python -m compileall -q ...` | clean |
| Added-line secret / `LIVE_TRADING` scan | clean |

Soak output:

```text
SOAK_OK deterministic offline long-run guards
  api_resilience: {'sent': 11951, 'fenced': 8049, 'circuit_opens': 207, 'final_state': 'OPEN', 'history_bounded': True}
  restart_policy: {'restarts': 201, 'budget_exhausted_cycles': 19799, 'history_retained': 200, 'last_restart_cycle': 19868}
  maintenance: {'light': 66, 'heavy': 66, 'throttled': 19868, 'journal_checkpoints': 66}
  native_stop: {'cycles': 5000, 'duplicate_orders': 0}
  journal: {'pruned_helper_keys': 1000, 'protected_key_preserved': True, 'integrity': 'ok'}
```

This is staged soak evidence for the bounded offline state machines only. A true long-duration paper soak (24-72 hours) with `/naiso/health` sampled periodically is recommended before any live deployment; it was not run here because the requested safety boundary forbids live keys/orders and this environment is not a hosted paper deployment.

## 6. New regression coverage

`test_naiso_longrun_hardening.py` adds 23 offline tests covering:

- API error classification, including non-retryable order writes.
- Circuit open, fence, half-open probe, successful reset, and failed-probe cooldown restart.
- Bounded jittered read retries and no automatic retry after order-submission or cancel uncertainty.
- Restart budget boundaries, bounded restart history, and watchdog safety-gate deference.
- Maintenance light/heavy gating, protected runtime keys, journal integrity.
- Native-stop adoption without duplicate creation, hold at unproven/visible-unknown levels, verified placement, and failed read-back hold.
- Verification against a journaled moved trigger price.
- Exact fail-closed `[RECOVERY_HOLD]` and `[ENTRY_LOCK_RETAINED]` log assertions.
- Daily-loss 5% trigger, 10-minute persistence across simulated reload, rebase only when flat, and continued block while an open position remains.
- `/naiso/health` schema exposure.

## 7. Rollback plan

1. Do not merge to `main` until the owner approves.
2. To roll back the commit after merge: `git revert <commit-sha>` on `main` and redeploy the previous image/commit.
3. To roll back before merge: delete the feature branch; no base branch files were changed.
4. The changes are additive at the SQLite level (new methods only; no destructive migration). `prune_runtime_state()` protects `daily_drawdown_cooldown_v1`, `recovery_state_v1`, and `recovery_hold_v1`.
5. If native-stop repair causes concern, set `NAISO_NATIVE_SL_REPAIR=false` (the default). Recovery then remains fully fail-closed while the other observability/resilience fixes stay active.

## 8. Deployment checklist before owner approval

- Confirm `PAPER_MODE=true` and no `LIVE_TRADING` environment override.
- Confirm no real API keys are present in the deployment environment.
- Run `python -m pytest -q --disable-warnings` and `python soak_naiso_longrun.py` from a clean checkout.
- Monitor `/naiso/health` for `recovery.hold`, `recovery.block_entries`, `positions.native_sl_verified`, `api.counters`, `threads.components`, `daily_loss`, and `restart_count`.
- Keep `NAISO_NATIVE_SL_REPAIR=false` until the owner explicitly accepts exchange-verified runtime repair policy.
