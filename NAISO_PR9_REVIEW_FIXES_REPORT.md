# PR #9 Review Fixes — NAISO-LIVE v1.1.0

Date: 2026-11-24  
Base of this work: PR #9 head `58159bd` (`fix/naiso-live-v1.1.0-longrun-hardening`)  
New branch: `fix/naiso-pr9-review-hardening`  
Safety: **no merge to `main`, no `LIVE_TRADING`, no real BingX keys, no live orders.**

The GitHub API returned no issue, review or inline comments on PR #9, so the
eight critical notes supplied in the task were treated as the review comments
and implemented directly.

## 1. Files modified

| File | Change |
|---|---|
| `bingx_protection.py` | Added authoritative conditional-order evidence helper. A successful unified `fetch_open_orders=[]` is no longer absence proof; both BingX `STOP_MARKET` and `TRIGGER_MARKET` raw queries must succeed with success envelopes. De-duplicates the same native order seen through several endpoints. |
| `bingx_execution.py` | `TradeExecutionService` accepts an injected API governor. Native-stop repair routes every read/write through it, adopts only journal-proven levels, detects duplicate active stops, and HOLDs when conditional evidence is `UNKNOWN`. The placement write is never retried. |
| `ggpbot.py` | `safe_api_call` now stops retrying confirmed non-retryable business errors. `verify_native_stop_loss_detailed` returns `UNKNOWN` unless both conditional queries succeed. Added exchange-verified engine recovery, separate open-management and flat-scanner resume paths, idempotency guard, restartable reconciliation supervisor, and watchdog callbacks for core engines. Native-stop repair injects `safe_api_call` and all journal-proven trigger levels. |
| `naiso_api_resilience.py` | Explicit retry policy: only rate-limit, timeout, network and confirmed transient server errors are retryable. Authentication, permission, invalid symbol, invalid parameters, configuration and unknown business failures are not retried. |
| `naiso_health.py` | Watchdog supports per-component recovery verification and passes verification context to restart callbacks. Verified open-trade recovery may bypass only the `OPEN_POSITION` gate; recovery-hold, unresolved-execution and daily-loss gates still block. Non-trading engines are explicitly marked as such. |
| `soak_naiso_longrun.py` | Added long-run offline engine-recovery phase with alternating open/flat, failed verification and bounded restart history. |
| `test_naiso_pr9_review.py` | New 16-case offline regression suite covering all eight critical notes. |

No EMA9/Session-VWAP entry logic, BUY/SELL conditions, pullback/retest rules,
Unified Trade Management Brain, TP1/TP2, trailing, smart exit, position sizing,
leverage, SL/TP prices, Scanner, watchlist, dashboard, Telegram or Flask routes
were changed.

## 2. Behaviour proven by tests

- `fetch_open_orders=[]` plus a failed conditional query returns `UNKNOWN`; no stop is created.
- Authoritative successful empty conditional queries permit exactly one journal-proven repair and verify it by read-back.
- A stop at a known journaled moved level is adopted; an unproven level is HOLD with no duplicate order.
- Every native-stop repair call passes through the central governor/rate limiter.
- A timed-out stop write is called once, sets `execution_unresolved`, blocks entries and keeps HOLD.
- Authentication, permission and invalid-symbol errors are called once and classified non-retryable.
- Watchdog recovery works with and without an open position; open-position recovery restores management only and never starts Scanner.
- Failed position, conditional-order or native-stop verification keeps Recovery Hold and performs no management/scanner resume.

## 3. Test results

| Check | Result |
|---|---:|
| New PR #9 review suite (`python -m pytest -q test_naiso_pr9_review.py --disable-warnings`) | `16 passed in 8.74s` |
| Previously added long-run suite (`test_naiso_longrun_hardening.py`) | passes within full suite |
| Full suite (`python -m pytest -q --disable-warnings`) | `584 passed in 120.20s` |
| Offline soak (`python soak_naiso_longrun.py`) | `SOAK_OK` |
| `python -m compileall -q bingx_execution.py bingx_protection.py ggpbot.py naiso_api_resilience.py naiso_health.py naiso_maintenance.py roro_trade_journal.py conftest.py test_naiso_longrun_hardening.py test_naiso_pr9_review.py soak_naiso_longrun.py` | clean |
| Added-line secret / `LIVE_TRADING` scan | clean |
| `git diff --check` | clean |

Soak output:

```text
SOAK_OK deterministic offline long-run guards
  api_resilience: {'sent': 11951, 'fenced': 8049, 'circuit_opens': 207, 'final_state': 'OPEN', 'history_bounded': True}
  restart_policy: {'restarts': 201, 'budget_exhausted_cycles': 19799, 'history_retained': 200, 'last_restart_cycle': 19868}
  maintenance: {'light': 66, 'heavy': 66, 'throttled': 19868, 'journal_checkpoints': 66}
  native_stop: {'cycles': 5000, 'duplicate_orders': 0}
  engine_recovery: {'management': 257, 'scanner': 226, 'holds': 385, 'recovered': 483, 'history_bounded': True}
  journal: {'pruned_helper_keys': 1000, 'protected_key_preserved': True, 'integrity': 'ok'}
```

## 4. Limits of the offline paper soak

This soak uses injected clocks, in-memory exchange fakes and temporary SQLite
files. It proves bounded state, fail-closed gating and idempotency in the local
code paths. It does **not** prove BingX uptime, real conditional-order endpoint
semantics, real latency, real rate-limit headers or a guaranteed one-year
uptime. Before deployment approval, run a hosted 24–72 hour paper soak with
`/naiso/health` sampled periodically and review native-stop diagnostics.

## 5. Remaining issues / operational caveats

- A genuinely hung OS thread cannot be killed safely in-process. The watchdog
  records diagnostics and holds rather than forcing an unsafe restart.
- Non-trading engines (reconciliation monitor, institutional updater, keep-alive)
  may resume while a position is open because they never create or cancel orders.
  Trade management and Scanner remain strictly separated and verified.
- Retry classification is conservative and text/signature based. A hosted paper
  soak should confirm the venue's exact transient error codes before any live
  deployment is considered.
- The automatic native-stop repair remains opt-in via
  `NAISO_NATIVE_SL_REPAIR`; default `false` preserves fail-closed recovery.

## 6. Rollback

This is a stacked PR against PR #9, not a merge into `main`. Rollback is done by
closing/reverting this single commit on `fix/naiso-pr9-review-hardening`; PR #9
and `main` remain untouched.
