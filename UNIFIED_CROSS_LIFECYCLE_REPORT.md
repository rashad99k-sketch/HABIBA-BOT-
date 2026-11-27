# Unified EMA9 + Session VWAP cross lifecycle — final engineering report

**Status:** merged into `main` after offline regression verification and independent review;
**not LIVE ready**.
No exchange call, order, balance query, API-key change, demo order or LIVE trade was
performed. BingX execution paths were only reused through the existing order path and
identity plumbing; no gate was bypassed.

## 1. Target repository verification

| Check | Result |
|---|---|
| Repository URL | `https://github.com/rashad99k-sketch/HABIBA-BOT-.git` |
| GitHub full name | `rashad99k-sketch/HABIBA-BOT-` (GitHub API) |
| Local workdir | `C:\Users\rasha\AppData\Local\Temp\opencode\habiba-adaptive-management` |
| `origin` URL | `https://github.com/rashad99k-sketch/HABIBA-BOT-.git` |
| Branch before merge work | `unified-cross-lifecycle` |
| HEAD before fix commits | `0a66ddcb3233a5149db77edc1ad83e421b1307e5` |
| Clean base before work | `908f29c836b417b0dac7560611cc5c6d7097efbb` |
| Remote `main` after owner commit | `59eeffd09ac7ea0d81ea3bb800c0633547acf0d6` |
| Merge base PR → main | `908f29c836b417b0dac7560611cc5c6d7097efbb` |
| Final PR head | `f9be4e91172f875958dbb71c45eb010aa324625a` |
| Merge commit on `main` | `1f342414dbba45e4edb3a94eac067d260ea93270` (GitHub-verified) |
| PR result | merged, not closed unmerged |
| PR | https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/44 |

The owner's newer `main` commit changed only `.env.example`. It was preserved by a
clean merge; this change set does not modify that file. The current template now has
`PAPER_MODE=false` and `ENABLE_LIVE_TRADING=true` with empty credentials, which is a
deployment risk documented below and is not a claim that LIVE trading was tested.

## 2. Architecture audit result

Three independent read-only audits were run: full call-path architecture trace, Git
diff code review, and remaining-risk assessment. They confirmed:

- Both existing Scanners remain: `global_discovery` and `watchlist_analysis`.
- No third Scanner, second Profit Engine, second executor or parallel trade manager was
  added. `TradeManagementBoard`, `LiveTradeManager`, `TradeExecutionService`, BingX
  execution, native protection, recovery, sync, reconciliation, verified close, sizing,
  20X policy, dashboard/Telegram/Flask/API/health and single-flight controls remain the
  existing implementations.
- EMA9 + Session VWAP remains the core cross. EMA50 stays diagnostic only in
  `roro_confluence`; it was never substituted as a cross signal.
- The first review found a merge-blocking defect: the final readiness gate inspected
  the raw cross-event object, which never received institutional approval, so the
  unified path could not reach `execute_entry()`. It also found stale Event-ID
  selection, shallow Watchlist copies, overstated price space, a weak-market target
  override and inaccurate verification logs. All were fixed and covered by tests.

## 3. Proven issues and fixes

| Issue | Fix |
|---|---|
| `execution_readiness()` received a stale `CROSS_DETECTED` event while approval was written to a local copy | `smart_opportunity_selection()` now passes the enriched candidate to the gate; `analyze_symbol()` and `_sync_cross_event()` write quality/status/evidence back into `MEMORY["cross_events"]`. |
| Event selection could pick the first retained cross for a symbol/side and reuse a consumed ID | Selection prefers the Watchlist row's own `cross_event_id`, otherwise the newest unconsumed confirmed event. Canonical symbol matching handles `BTCUSDT` vs `BTC/USDT:USDT`. Bridge selection follows the same rule. |
| Duplicate Event IDs were annotated but not excluded and could be re-approved | Duplicates are removed before analysis; `cross_rank` stays `-1.0`; permanent rejection reasons block re-approval; both structural and cross Event IDs are checked against the journal. |
| Ranking worked on shallow copies, leaving stale Watchlist state | The original Watchlist dicts are now mutated with rank, decision state and rejection reason. |
| Available price space used the EMA9/VWAP gap and `max()` over distant levels | It now uses the nearest opposing boundary: resistance `low` for BUY, support `high` for SELL, plus opposing swing/liquidity levels. |
| `HIGHER_VOL` bypassed the inherited weak/thin/ADX guard | Volatility bands are applied only after the mandatory weak-market guard. |
| Verification log read a non-existent `STATE["order"]` key | The wrapper logs `entry_exchange_order_id`, actual filled quantity (`actual_filled_qty`/`qty`), `entry_execution_state` and `native_sl_verified`. |
| Focused tests did not exercise the production authorization path | A production-shaped offline test drives `smart_opportunity_selection()` through frame validation, cross selection, location analysis, readiness and a monkeypatched `execute_entry()`. |
| Independent re-review: bridge could relabel consumed/APPROACHING events as approved | `cross_event_authorization_block()` now requires CONFIRMED, unconsumed and non-permanently-rejected state before bridge approval; `_select_cross_event()` returns `None` when no valid event exists. |
| Independent re-review: preferred expired/rejected Event ID could block a newer cross | Both bridge and Watchlist selectors apply the same validity filter to the preferred ID and fall back to the newest valid confirmed event. |
| Independent re-review: pre-authorization bridge filter used raw string equality | The pre-authorization event filter now canonicalizes both Watchlist keys and exchange-qualified event symbols; the production-shaped test asserts the bridge receives both events. |

## 4. Modified files and functions

| File | Functions / additions |
|---|---|
| `ggp_cross_lifecycle.py` (new) | `CrossLifecycleState`, `adx_gate`, `make_cross_event_id`, `normalize_cross_event`, `merge_cross_events`, `transition_state`, `_zone_quality`, `evaluate_cross_location`, `build_location_features`, `cross_rank`, `rank_cross_candidates`, `execution_readiness`. Execution-free: no data fetch and no order call. Fixed permanent-rejection handling, duplicate rank and nearest-boundary price space. |
| `ggpbot.py` | Imports the lifecycle module; `_enrich_with_existing_gpp_engines()` converts existing Scanner-2 proximity into retained cross events; `smart_opportunity_selection()` adds unresolved-state guard, cross ranking, duplicate exclusion, canonical event selection, state write-back, side re-analysis, enriched readiness and ADX gate; `execute_entry()` uses the cross Event ID as durable identity; the serialized wrapper logs authoritative fill/protection state; `LiveTradeManager._apply_management()` supplies ATR% and volatility band context. |
| `ggp_institutional_bridge.py` | `analyze_symbol()` evaluates the cross's own location and mutates the supplied cross event with the result; `cross_event_authorization_block()` guards approval; `_select_cross_event()` chooses the exact/newest valid confirmed event; `analyze_symbol_both()` and `update_unified_watchlist()` use canonical symbol matching and the same selection rule. |
| `ggp_profit_brain.py` | `select_range_target_pct()` maps `LOW_VOL -> 0.25%` and `HIGHER_VOL -> 0.50%` after the weak-market guard. |
| `test_cross_lifecycle.py` (new) | 25 offline tests: mandatory scenarios, AST protected-area checks, enriched-readiness proof, duplicate blocking, bridge Event-ID selection and authorization block, nearest-boundary space, production-shaped authorization path with stale preferred ID / canonical filter and verification-log fields. |
| `UNIFIED_CROSS_LIFECYCLE_PLAN.md` (new) | File/function-linked implementation plan written before edits. |
| `UNIFIED_CROSS_LIFECYCLE_DIFF.txt` (new) | Exact Git diff for the change set. |
| `UNIFIED_CROSS_LIFECYCLE_REPORT.md` (new) | This report. |

## 5. Before / after data flow

**Before.** Both Scanners ran, but EMA9/Session-VWAP proximity was ranking metadata and
an `execute_entry()` hard gate. There was no cross Event ID, retained cross state,
cross-location quality class or single identity across Scanner → Watchlist →
Institutional Engine → Entry.

**After.**

1. Existing Scanner 2 calls `_enrich_with_existing_gpp_engines()`, which uses unchanged
   `roro_confluence.ema9_vwap_proximity()` on closed candles.
2. `normalize_cross_event()` creates APPROACHING/CONFIRMED candidates. Confirmed crosses
   get a stable ID from symbol + timeframe + direction + closed-candle timestamp.
   `merge_cross_events()` retains them after the visual cross disappears until expiry.
3. `update_unified_watchlist()` attaches matching cross evidence to Watchlist rows using
   canonical symbol matching. The Watchlist remains the cross-event list.
4. `analyze_symbol()` annotates the existing institutional hypothesis with cross-location
   analysis and writes the analysis result back to the supplied cross event. It does not
   create another opportunity or bypass `UnifiedEntryAuthority`.
5. `smart_opportunity_selection()` ranks confirmed/approved crosses, excludes duplicate
   IDs, re-evaluates BUY/SELL independently, re-evaluates location with Smart Money /
   Momentum / trend evidence, syncs state into `MEMORY["cross_events"]`, checks the same
   closed-candle ADX used by Entry Engine, transitions `EXECUTION_READY`, and forwards the
   cross Event ID to the existing `execute_entry()`.
6. `execute_entry()` keeps every existing final gate. When the EMA9/VWAP gate is ready,
   the cross Event ID becomes the durable journal identity.
7. `open_position()` → `TradeExecutionService.submit_market_entry()` →
   `bingx_execution.py` order send → fill/position readback → native stop verification is
   unchanged. The wrapper logs reaching the final boundary, order acceptance, actual
   filled quantity, position verification and protection verification.
8. `TradeManagementBoard` owns all exits. After verified close, existing reconciliation
   releases the one-position lock and scanning resumes.

## 6. Cross-location analysis and ranking

`build_location_features()` maps the existing institutional context into explicit
evidence: demand/supply zone proximity, direction-valid non-mitigated Order Block and
FVG, sweep + reclaim or buy-side sweep + rejection, Market Structure / CHOCH, premium/
discount alignment, real value area vs mid-range chop, nearest opposing liquidity /
resistance / support / swing level, Smart Money bias, Momentum, volume, institutional
trend and available price space in ATR. Available space is the **minimum** distance to
the nearest opposing boundary; it is never inflated by the EMA9/VWAP gap.

`evaluate_cross_location()` returns `HIGH QUALITY CROSS`, `GOOD CROSS`, `WEAK CROSS` or
`INVALID CROSS`, with all booleans and numeric inputs retained in `evidence` and all
failures in `rejection_reasons`. HIGH/GOOD are executable candidates only when ADX >= 18
and no critical condition fails; a high Score alone never approves a cross.

`rank_cross_candidates()` ranks by quality class, cross proximity, OB/FVG validity,
liquidity confirmation, Market Structure, ADX, Momentum, Institutional Flow, available
space, R:R, freshness, approval and Event-ID uniqueness. Duplicate IDs are permanently
rejected with `cross_rank = -1.0`.

## 7. BingX entry path

No protected file was replaced. The exact path remains:

`smart_opportunity_selection()` → `execute_entry()` → `open_position()` →
`TradeExecutionService.submit_market_entry()` → `bingx_execution.py` order send →
fill/order readback → `_position()` direction proof → `place_and_verify_native_stop()` →
existing `TradeManagementBoard`.

The only changes are identity and observability: the cross Event ID becomes the durable
event identity when the existing gate is ready, and the wrapper logs authoritative
execution state. Durable `is_event_consumed()` / `consume_event()` semantics are unchanged.

## 8. Scalp / Trend selection

`LiveTradeManager._apply_management()` computes ATR% from closed-frame ATR / mark price
and labels `LOW_VOL` (<0.40%), `HIGHER_VOL` (>=0.80%) or `NORMAL`.
`ggp_profit_brain.select_range_target_pct()` applies the weak-market guard first, then
maps the bands to 0.25% and 0.50% price moves. These are price movements, not leveraged
ROE.

- `RANGE_SCALP`: one target, full remaining close, no TP1/TP2 and no splitting.
- `TREND_RIDE`: existing TP1 at 50% of original filled quantity near the liquidity /
  structural target, then structural / liquidity TP2 for the runner; no TP3.
- All exits remain inside `TradeManagementBoard`; no Profit Engine was added.

## 9. Actual test results

| Command | Result |
|---|---|
| `python -m py_compile ggp_cross_lifecycle.py ggp_institutional_bridge.py ggp_profit_brain.py ggpbot.py test_cross_lifecycle.py` | clean |
| `python -m pytest -q test_cross_lifecycle.py` | **25 passed in 11.29s** |
| Focused + integration subset (`test_cross_lifecycle.py`, `test_entry_event_idempotency.py`, `test_ema9_vwap_soft_gate.py`, `test_profit_brain.py`, `test_profit_target_ladder.py`, `test_dual_scanner_dashboard.py`, `test_ggp_roro_integration.py`) | **94 passed in 28.28s** |
| Full offline regression before merging `origin/main` | **642 passed, 1 warning in 124.51s** |
| Full offline regression after merging owner commit `59eeffd` | **642 passed, 1 warning in 126.45s** |
| Final full offline regression after independent re-review fixes | **643 passed, 1 warning in 130.83s** |
| GitHub Actions `python-tests` on final PR head `f9be4e9` | **success** (run 46064388234) |
| Independent initial re-review | Prior blockers fixed; safe as offline-verified code, not LIVE-ready |
| Independent targeted re-review of follow-up fixes | **no actionable findings**; safe as offline-verified code, not LIVE-ready |

Baseline before edits was `618 passed, 1 warning in 177.13s`. No Timeout was raised, no
test was skipped or marked xfail, and no failing test was ignored. The single warning is
a pre-existing Pandas `Timestamp.utcnow` deprecation in
`test_professional_discovery_scanner.py`.

## 10. Remaining risks and explicit non-coverage

1. **No live/demo verification.** No order submission, actual fill, position readback,
   native SL verification, TradeManagementBoard exit, verified close or restart recovery
   was tested against BingX Demo or LIVE. LIVE readiness is not claimed.
2. **Owner `.env.example` defaults are LIVE-shaped.** Remote `main` commit `59eeffd`
   sets `PAPER_MODE=false` and `ENABLE_LIVE_TRADING=true`. Credentials are empty, and no
   environment or API setting was changed by this work. This must be reviewed before
   deployment.
3. **Portfolio cap not implemented.** The requested `5 technical/institutional + 1 news,
   6 total` cap does not exist. There is no real News Engine or NEWS category. Runtime is
   structurally single-position across global state, sync, recovery, reconciliation and
   management; enabling 5+1 safely would require a redesign. It was not faked.
4. **Pre-existing native SL contradiction.** One helper labels native SL immutable while
   the manager repairs it through `replace_native_stop()`. This change does not alter it.
5. **Pre-existing UNKNOWN contradiction.** The original class method appears to raise on
   UNKNOWN before the wrapper branch, but the effective runtime monkey-patch preserves
   UNKNOWN and blocks entries. Duplicate implementations remain an audit hazard.
6. **Post-fill event consumption failure** blocks new entries but cannot unwind an
   already filled position; inherited fail-closed behavior.
7. **Same-symbol opposing crosses** are both analyzed, but the Watchlist stores one best
   row and existing conflict logic may reject both when both hypotheses are
   `ENTRY_READY`.
8. **Volatility bands** use simple ATR% thresholds; regime stabilization/hysteresis stay
   with existing engines.
9. No 24-hour soak and no exchange outage simulation under LIVE credentials. GitHub
   Actions `python-tests` did run and passed on the final PR head; it is not a substitute
   for demo/LIVE verification.

## 11. Git history and PR

| Event | Commit / result |
|---|---|
| Clean base before edits | `908f29c836b417b0dac7560611cc5c6d7097efbb` |
| Owner `.env.example` commit on `main` (preserved) | `59eeffd09ac7ea0d81ea3bb800c0633547acf0d6` |
| Implementation | `1f80d691ff74ebe71a728e82b27ac014c6145899` |
| Initial report metadata | `0a66ddcb3233a5149db77edc1ad83e421b1307e5` |
| First independent-review fixes | `6270c5a3faaf212e963e14389a93672600b01df3` |
| Integration of `origin/main` into branch | `877b0e6a737a5b94e4858e4d1451a8b022303671` |
| Final documentation / generated diff | `12b0dd7d4346e700a2eecef0bff2ad66884728bb` |
| Final PR head (follow-up validity/canonical fixes) | `f9be4e91172f875958dbb71c45eb010aa324625a` |
| PR #44 merge into `main` | `1f342414dbba45e4edb3a94eac067d260ea93270` |

- PR: https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/44
- Merge method: standard GitHub merge commit; no force push, rebase or history rewrite.
- GitHub API confirmed `merged: true` with the merge commit above; subsequent report-only
  documentation commits advanced `main` without changing implementation code.
- GitHub Actions `python-tests`: success on `f9be4e9`.
- Exact patch: `UNIFIED_CROSS_LIFECYCLE_DIFF.txt`.

No commit or PR claims LIVE readiness; they cover offline-verified code only.

## 12. Follow-up hardening round — `institutional-cross-lifecycle-hardening`

This section supersedes the earlier status only for this follow-up branch. Work started from
the latest verified `origin/main` at `a7d72f225523e3fdb3026ade30f710e3b3dab569` on
`institutional-cross-lifecycle-hardening`. No history rewrite, force push or rebase was used.

### 12.1 Root causes found by the follow-up audits

1. **Taxonomy and badges were not state-driven.** The lifecycle module still used
   `HIGH/GOOD/WEAK/INVALID CROSS` labels; the required taxonomy is
   `EXCELLENT / GOOD / MODERATE / WEAK / INVALID`, and Watchlist icons were not derived
   from the retained lifecycle state.
2. **ADX and cross direction were lost in ranking.** The bridge's institutional context
   did not expose ADX for cross-location evaluation, and the watchlist ranker compared a
   BUY/SELL cross direction against BULLISH/BEARISH expected signals, so a valid same-side
   EMA9 cross could not earn its cross priority.
3. **Cross age was scoped incorrectly.** `_proximity_cross_age` was assigned only inside
   the first-observation branch but read on every scan, making freshness/rank inputs
   depend on scan order.
4. **One mixed queue and no best picks.** The Watchlist had a single BUY/SELL ordering,
   APPROACHING/unapproved/expired rows could occupy the bounded `ready[:8]` revalidation
   slots, and there were no `BEST BUY OPPORTUNITY` / `BEST SELL OPPORTUNITY` outputs.
5. **SCALP still had a third target.** `RANGE_PROFIT_TARGETS_PCT` was `(0.25, 0.50, 1.00)`
   and a strong-follow-through branch returned 1.00%.
6. **API governor was not universal.** Only the journal-repair `TradeExecutionService`
   constructor injected `api_caller=safe_api_call`; the main entry and native-stop-move
   constructors did not.
7. **Entry journal persistence failure was ignored.** A verified entry could continue
   without a durable journal start, creating a recovery hazard.
8. **Categorical evidence was dropped.** `evaluate_cross_location()` retained booleans
   and numbers but discarded strings such as `indicator_quality`, hiding stale/missing
   indicator evidence.

### 12.2 Fixes and exact file/function map

| File | Function / area | Fix |
|---|---|---|
| `ggp_cross_lifecycle.py` | constants, `CrossLifecycleState`, transitions | Required EXCELLENT/GOOD/MODERATE/WEAK/INVALID taxonomy, `APPROVABLE_QUALITIES`, explicit `ENTRY_CANDIDATE → INSTITUTIONAL_APPROVED` transition, consumed/expired/waiting badges. |
| `ggp_cross_lifecycle.py` | `cross_lifecycle_badge()`, `cross_lifecycle_identity()`, `opportunity_summary()` | State-driven six-icon identity and explainable best-pick payload fields. |
| `ggp_cross_lifecycle.py` | `rank_cross_candidates_by_side()._eligible()` | Best picks require CONFIRMED event, non-permanent-rejection state, event ID, approved quality, ADX >= 18 and an approved/candidate/ready lifecycle state. |
| `ggp_cross_lifecycle.py` | `evaluate_cross_location()` | Stale/unavailable indicators fail the location; string evidence is retained. |
| `ggp_institutional_bridge.py` | `_indicator_snapshot()`, `_build_context()`, `analyze_symbol()` | ADX is exposed from the closed-candle frame and written into the cross lifecycle identity. |
| `ggp_institutional_bridge.py` | `update_unified_watchlist()` | BUY/SELL directions are normalized before comparison; only EMA9 cross direction earns cross priority; identity is written back to rows and best selection. |
| `ggpbot.py` | `_enrich_with_existing_gpp_engines()` | Cross age initialized unconditionally; side-matched EMA9-only cross event, distance, age and R:R ranking inputs; lifecycle identity/badge written back. |
| `ggpbot.py` | `smart_opportunity_selection()` | Filtering, side-independent ranking, interleaved bounded queues, strict final readiness, full state progression through SUBMITTED/FILLED/CONSUMED, journal-start failure guard. |
| `ggpbot.py` | `data()` / dashboard renderer | Publishes `cross_opportunity_ranking` and BEST BUY/SELL blocks; renders Event ID, candle, EMA9/VWAP, ADX, zone, quality, structure, liquidity, retest, volume, classification, rank and rejection reason. |
| `ggpbot.py` | `open_position()`, `_move_native_stop_verified()`, `_repair_native_sl_from_journal()` | All three `TradeExecutionService` constructors inject `safe_api_call`. |
| `ggp_profit_brain.py` | `RANGE_PROFIT_TARGETS_PCT`, `select_range_target_pct()` | SCALP is exactly 0.25% / 0.50%; strong follow-through is capped at 0.50%. |
| `.gitattributes`, `UNIFIED_CROSS_LIFECYCLE_REPORT.md`, `UNIFIED_CROSS_LIFECYCLE_DIFF.txt` | Reporting artifacts | Regenerated the exact follow-up patch and marked only the embedded-diff artifact whitespace-exempt. |

### 12.3 One integrated workflow after the hardening

`existing discovery / watchlist scanners` → `EMA9+Session-VWAP closed-candle cross event`
→ `single unified Watchlist row` → `existing institutional bridge (BUY and SELL)`
→ `cross-location analysis + side-independent ranked queues` → `existing execution-side
gates + one final cross readiness gate` → `single execute_entry/open_position/BingX path`
→ `fill and native protection verification` → `existing TradeManagementBoard` →
`verified close/reconciliation` → `same Watchlist and cross-event store produce the next
opportunity`.

No third scanner, second executor, second manager, second profit engine, second watchlist
or second cross-event store was added. `roro_confluence.py`, `bingx_execution.py`,
`roro_trade_journal.py`, `ggp_position_sizing.py` and `TradeManagementBoard` remain the
single source of truth for their existing responsibilities.

### 12.4 Required icon states

| State | Badge |
|---|---|
| `cross_status=APPROACHING` | 🔵 APPROACHING CROSS |
| `cross_status=CONFIRMED`, analysis not started | 🟡 CROSS CONFIRMED — ANALYZING |
| `INSTITUTIONAL_ANALYSIS_PENDING` / `ZONE_ANALYSIS_COMPLETE` | 🟣 INSTITUTIONAL ZONE ANALYSIS |
| `ENTRY_CANDIDATE` / `INSTITUTIONAL_APPROVED` | 🟢 ENTRY CANDIDATE |
| `EXECUTION_READY` | 🚀 EXECUTION READY |
| `INSTITUTIONAL_REJECTED` / `decision_state=REJECTED` | 🔴 REJECTED |
| `CONSUMED` / expired / waiting | ⚪ EXPIRED / WAITING |

### 12.5 BUY/SELL ranking and authorization

`rank_cross_candidates_by_side()` produces separate BUY and SELL queues and independent
`best_buy` / `best_sell` summaries. A best pick must be a confirmed EMA9/Session-VWAP
cross with a stable event ID, approved EXCELLENT/GOOD location, ADX >= 18, no permanent
rejection and a non-consumed approved/candidate/ready lifecycle state. `smart_opportunity_selection()`
interleaves the two queues before the bounded `ready[:8]` work slice, so neither side can
starve the other, and permanently rejected or expired identities are removed rather than
blocking newer valid events.

The final authorization remains strict: closed candles only; EMA9 cross only; EMA50 is
diagnostic; BUY and SELL are independently re-analyzed; structural `UnifiedEntryAuthority`
and location-window checks still apply; the same closed-candle ADX must be >= 18; the
enriched candidate — not the raw event — must pass `execution_readiness()`; state must
reach `EXECUTION_READY` before the single existing `execute_entry()` call. Submission,
verified fill, native protection, journal consume and cross-event consume remain on the
existing one-position path.

### 12.6 SCALP / TREND policy after hardening

- SCALP: only 0.25% and 0.50% **price-move** targets, always 100% remaining close, no
  TP1/TP2 and no 1.00% tier.
- TREND: existing TP1 at 50% of the original filled quantity, TP2 for the remaining
  verified quantity, no TP3.
- REVERSAL 0.50%/1.00% entry geometry and SNIPER_V2 1.00% constants are pre-existing
  non-SCALP behavior and were deliberately left unchanged.

### 12.7 Verification

| Check | Result |
|---|---|
| `py_compile` on all changed Python files | clean |
| Focused lifecycle/scanner/profit suite | **98 passed** |
| Full offline regression with isolated venv | **656 passed, 1 warning in 128.09s** |
| Skipped / xfail / weakened timeout tests | none |
| Real exchange order, balance query, API setting change | none |
| Demo / LIVE verification | **not performed** |
| GitHub REST API (credential-helper auth) | repository and `main` SHA verified; `rate_limit.remaining=4982` at check time |

The single warning remains the pre-existing Pandas `Timestamp.utcnow` deprecation in
`test_professional_discovery_scanner.py`. The regenerated patch artifact is marked
`-whitespace` in `.gitattributes` because it embeds unified diff context lines; all source
and test files remain clean under `git diff --check`. `api_caller=safe_api_call` is present at all three
runtime `TradeExecutionService` constructors and covered by an AST test; no API/rate-limit
behavior was exercised against BingX.

### 12.8 Remaining risks

The earlier report's single-position architecture, unresolved exchange-state handling,
post-fill journal consumption failure, pre-existing native-SL/UNKNOWN contradictions and
same-symbol conflict behavior remain applicable. This hardening did not attempt the
requested 5+1 portfolio redesign or add a News Engine. No 24-hour soak, exchange outage,
demo order, LIVE trade, native-stop recovery soak or verified-close exchange test was
performed. **The system is not demo-verified, not LIVE-verified and not LIVE-ready.**

### 12.9 Independent review disposition and PR status

An independent read-only architecture/code review returned **no BLOCKER** and four CONCERNs.
All four were dispositioned before merge:

| Concern | Disposition |
|---|---|
| Ranking published before reanalysis could make the UI stale | `/data` now recomputes BEST BUY/SELL from current Watchlist rows on every request; `smart_opportunity_selection()` still publishes its snapshot for non-UI consumers. |
| Display eligibility broader than final readiness | BEST picks now require `cross_location_approved=True` in addition to confirmed EMA9 cross, approved quality, ADX >= 18 and an approved/candidate/ready lifecycle. |
| `or` fallbacks could discard legitimate fresh zero geometry/ADX | `cross_lifecycle_identity()`, `_sync_cross_event()` and cross-location feature construction now use explicit `is None` fallbacks. |
| Several assertions were static source/AST checks | Added a behavioral Flask `/data` test plus best-pick and zero-value behavior tests; AST checks remain as architecture guards only. |

PR: https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/45 .
The first branch commit `5f8f151a7703042fbca0f0964fd00e4c6446bc9b` had GitHub Actions
`python-tests: success`. The review-fix commit is rechecked by CI before merge.
No GitHub review or check reported a blocker.

### 12.10 Final merge record

| Item | Value |
|---|---|
| Starting commit | `a7d72f225523e3fdb3026ade30f710e3b3dab569` |
| Branch | `institutional-cross-lifecycle-hardening` |
| Branch commits | `5f8f151a7703042fbca0f0964fd00e4c6446bc9b`, `66ba9f307ee631ccbc9611f18e21d077fc36c230` |
| PR | https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/45 |
| PR final head | `66ba9f307ee631ccbc9611f18e21d077fc36c230` |
| PR state | closed / merged at `2026-11-27T21:52:26Z` |
| Merge commit on `main` | `073a3cc21bb8ad362f3d90ef7524eb1c22dd9812` |
| GitHub Actions | run `46072784258`, `Sniper V2 integration CI`, completed `success` |
| Final local regression | `656 passed, 1 pre-existing warning in 128.09s` |
| GitHub REST API | authenticated via git credential helper; `rate_limit.remaining=4982` at verification time |
| Exchange API/rate-limit behavior | not exercised |
| Demo result | **not performed** |
| LIVE result | **not performed** |

No force push, rebase or history rewrite was used. The merged workflow is still the single
scanner → Watchlist → institutional analysis → ranked authorization → existing BingX execution
→ verified management/close path. **No demo or LIVE verification was performed; this build is
not LIVE-ready.**
