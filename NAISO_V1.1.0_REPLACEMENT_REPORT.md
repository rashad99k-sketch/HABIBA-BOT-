# NAISO-LIVE v1.1.0 — Complete Replacement Report

## Source and safety

- Source archive: `C:\Users\rasha\Downloads\NAISO-LIVE-ema9-vwap-profit-brain-v1.1.0.zip`
- Archive SHA-256: `badf2048509ec52993ba0962a464ebe0a2551e3055dab1017515bfbfe4c15db6`
- Archive contents: 123 files under `NAISO-LIVE-ema9-vwap-profit-brain-v1.1.0/`
- Replacement branch: `release/naiso-live-v1.1.0` (created from `origin/main`)
- Backups created before replacement:
  - Branch/tag `backup/pre-naiso-v1.1.0-adaptive` / `pre-naiso-v1.1.0-adaptive` — preserves the previous uncommitted EMA9/VWAP adaptive working tree.
  - Tag `pre-naiso-v1.1.0-main` — preserves the previous production `main` commit.
- No repository deletion, history rewrite, credential change, real order, or live API call was performed.
- The local ignored `.env`, SQLite journals, and logs were not deleted or committed. `logs/` was added to `.gitignore`.

## Legacy files removed

Complete application replacement removed every tracked file present in `main` but absent from v1.1.0:

- `.github/workflows/python-tests.yml` (superseded by `sniper-v2-ci.yml`)
- `EMA_VWAP_CENTRAL_GATE_PATCH_NOTES_AR.md`
- `EXTRACTION_REPORT.md`
- `IMPLEMENTATION_REPORT_EN.md`
- `README.md`
- `archives/GGP_RELEASE_9_LONG_RUN_STABILITY_HARDENED.zip`
- `archives/NAISO-LIVE-main-EMA-VWAP-CENTRAL-GATE-PATCHED.zip`
- `test_central_entry_gate.py`
- `test_unified_ema_vwap_strategy.py`
- `unified_ema_vwap_strategy.py`

The v1.1.0 source contains no import of `unified_ema_vwap_strategy`, `ggp_evidence`, or `ggp_smart_entry`; there is no duplicate engine or legacy run path.

## New v1.1.0 files added

Key additions from the supplied archive include:

- `MASTER_DIRECTIVE_IMPLEMENTATION_V1.1.0.md`, `RELEASE_NOTES_EMA9_VWAP_V1.1.0.md`, `TEST_REPORT_EMA9_VWAP_V1.1.0.md`
- `ENGINEERING_AUDIT_EMA9_VWAP_V1.0.0.md`, `IMPLEMENTATION_REPORT_EMA9_VWAP_V1.0.0.md`, and related v1.0.0 deployment/rollback docs
- `ggp_market_intelligence.py`, `location_authority.py`, `roro_trend_timing.py`
- `test_neon_adaptive_entry.py`, `test_ema9_vwap_soft_gate.py`, `test_ema50_vwap_integration.py`, `test_profit_target_ladder.py`, `test_sniper_v2_integration.py`
- `tests/test_resource_manager.py`, `tests/test_runtime_state_journal.py`, `tests/test_trade_journal_maintenance.py`
- `.github/workflows/sniper-v2-ci.yml`, `docs/MARKET_INTELLIGENCE_DESIGN_AR.md`, and the decision-flow diagram/report assets

All 93 common files were overwritten with the v1.1.0 versions.

## Fixes applied on top of the supplied v1.1.0 source

1. **Verified native stop-loss integration (critical requirement).**
   - `ggpbot.py` now calls `TradeExecutionService.replace_native_stop()` from the existing management path for `MOVE_SL`, `BREAKEVEN`, `LOCK_PROFITS`, `TRAIL`, and TP1 breakeven.
   - The new level is adopted only when the service returns `MOVED` with a fresh read-back. `RETAINED` keeps the previously verified venue stop; `LOST` enters the existing recovery hold; API exceptions never claim success.
   - Material-move gating, 300-second duplicate suppression, a dedicated lock, and resolution-state blocking prevent repeated or unsafe cancel/replace orders.
   - Paper mode keeps synthetic-only behavior and records `PAPER_SIMULATED`.
   - New tests: `test_native_sl_management_integration.py` (9 tests).

2. **Resource manager TTL correctness.**
   - `roro_resource_manager.py` now treats `ttl_s=0` as immediately expired (`<=` instead of `<`), fixing the one failing delivered test without hiding it.

3. **Entry-guard ordering.**
   - Exposure, recovery-hold, and unknown-position gates now run before balance/market-data reads, preventing unnecessary exchange calls when entries are already blocked.

4. **CI / preflight.**
   - `preflight.py` checks the v1.1.0 modules and `_move_native_stop_verified`.
   - CI includes `release/**` pushes, empty BingX credentials, a focused native-stop/adaptive test step, and the full suite.

5. **Runtime hygiene.**
   - `logs/` is ignored so runtime diagnostics cannot enter the release commit.

## Validation performed

| Check | Result |
|---|---|
| `python -m compileall -q .` | passed |
| `python preflight.py` (PAPER, empty keys) | passed |
| Focused native-stop, adaptive-entry, EMA/VWAP, profit-target, execution and hardening tests | **114 passed** |
| Full `python -m pytest -q` with isolated journal | **545 passed, 0 failed** |
| Flask `/health` via test client | HTTP **200**, `mode=PAPER`, `ok=true` |

The supplied v1.1.0 test report documented 536 passing tests. The final repository has 545 because of the nine new native-stop integration tests; the delivered resource-manager failure was fixed.

## Strategy and configuration verification

- Primary signal: closed-candle EMA9 × session OHLC4 VWAP, with BUY/SELL symmetry covered by `test_neon_adaptive_entry.py` and the EMA/VWAP integration tests.
- Entry behaviors: `IMMEDIATE`, `MICRO_PULLBACK`, `PULLBACK`, `RETEST`, and fail-closed `WAIT_FOR_STRUCTURE`.
- Management labels: `SCALP`, `TREND`, `TREND_CONTINUATION`, `WEAK_MOMENTUM`, `REVERSAL_RISK`; `TradeManagementBoard / LiveTradeManager` remains the sole routine exit authority.
- Watchlist indicator continuity, zone/order-block state, source/age/freshness, exchange sync, reconciliation, recovery holds, idempotency, caching, rate limits, locks, Flask Dashboard, Telegram, and journal behavior remain on the existing single-authority paths.
- `.env.example` now uses safe defaults (`PAPER_MODE=true`, `ENABLE_LIVE_TRADING=false`, empty credentials). No real secret file was read, copied, committed, or logged.
- Render readiness: `PORT` selects the hosted port and binds `0.0.0.0`; `/health` responds 200; `requirements.txt` includes `ccxt==4.5.92`. The supplied archive contains no `render.yaml`, so no deployment declaration was invented.

## Limitations

No live BingX order, credential use, production deployment, 24-hour stability run, or live API-load measurement was performed. The PR is prepared for review only and has not been merged into `main`.
