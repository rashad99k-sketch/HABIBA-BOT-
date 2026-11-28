# NAISO-LIVE Complete Upgrade Delivery

## Baseline and scope

This delivery is based on `main` commit `7c370b395d6311d627d7e4e8602b079b133a1829`. The uploaded ZIP was treated as historical reference only and was not applied over the current repository. Existing execution, sizing, leverage, native-stop, journal, dashboard, Telegram, recovery, and trade-management authorities were preserved.

The current implementation provides the coordinated path:

`EMA9 + Session VWAP closed-candle evidence → Global Discovery → Unified Watchlist → Institutional Analysis → Unified Entry Authority → existing execution gateway → Unified Trade Management / Adaptive Profit Brain`

The implementation includes closed-candle cross evidence, event identity and idempotency, watchlist retention and rotation, institutional location analysis, pullback/retest and no-chase gates, adaptive `RANGE_SCALP` versus `TREND_RIDE` management, verified TP1/TP2 handling, adaptive trailing, native-stop verification, recovery holds, exchange reconciliation, bounded API resilience, watchdog heartbeats, bounded histories, protected journal state, health endpoints, and offline long-run guards.

## Changes in this delivery branch

The functional implementation is the latest `main` baseline. This branch adds the paper-safe `render.yaml`, hosted deployment instructions in `DEPLOYMENT_RENDER.md`, and this delivery record. No leverage, risk allocation, position sizing, or live execution behavior was changed.

## Validation

The full latest-main regression suite completed with **612 passed, 0 failed, and 1 warning** in paper-safe offline mode. The deterministic long-run guard completed successfully with bounded API/restart histories, zero duplicate native-stop orders, journal integrity `ok`, and protected runtime keys preserved. Compileall and `git diff --check` are required release gates and are run during packaging.

These results do not prove exchange availability, real-order correctness, or guaranteed six-month uptime. Staged offline, hosted-paper, and monitored operation remains required.

## Safety boundary

No real BingX credentials are included. No live orders are submitted. The Render descriptor defaults to paper mode, disables live trading, disables native-stop repair, and uses `autoDeploy: false` so deployment remains an explicit operator action.
