# NAISO-LIVE/GGP BingX Futures — Engineering Audit

**Release:** `ema9-vwap-profit-brain-v1.0.0`
**Scope:** supplied archive audited before modification; no live exchange connection or deployment.

## Executive conclusion

The supplied system already had the required safety spine: shared market-data/cache utilities, bounded discovery components, a unified institutional watchlist bridge, `TradeManagementBoard`/`LiveTradeManager` as the management authority, `TradeExecutionService` as the execution gateway, actual-fill state, exchange reconciliation, native stop verification, recovery holds, and journal-backed idempotency. The implementation therefore extends those authorities rather than introducing a second scanner, profit manager, or order path.

## Authority map

| Concern | Authoritative component | Integration decision |
|---|---|---|
| Market data | `ggp_market_data.py`, shared cache path in `ggpbot.py` | Reused; no uncontrolled per-symbol fetch loop added |
| Discovery/ranking | `ggp_discovery_scanner.py`, `roro_confluence.py`, institutional bridge | Enriched EMA9/VWAP/EMA50 evidence; scanner remains ranking-only |
| Entry authorization | institutional/closed-candle gate in `ggpbot.py` | Preserved as final execution-boundary gate |
| Trade management | `TradeManagementBoard` + `LiveTradeManager` | Added explainability/protection metadata only |
| Profit policy | `ggp_profit_brain.py` planner | Reused; no order authority added |
| Order execution | `bingx_execution.py::TradeExecutionService` | Preserved as the only exchange gateway |
| Recovery | `ggp_recovery.py` and reconciliation paths | Preserved; UNKNOWN/recovery hold remains fail-closed |
| Persistence/audit | `roro_trade_journal.py`, management events | Used for durable event identity and traceability |

## Requirements-to-design mapping

- EMA9/VWAP is primary. EMA50 is secondary evidence and does not independently block a valid EMA9/VWAP opportunity.
- Near-cross/convergence states are represented before a completed cross; completed-cross age and freshness are carried as evidence.
- Closed-candle timestamps, ATR-normalized distance, slopes, relationship state, volume, candle body and wick context are emitted for ranking and diagnosis.
- Symbol canonicalization and bounded universe handling occur at the scanner boundary. The scanner has no execution imports or order calls.
- Profit management starts from actual-fill state; planner output is distinct from exchange submission.
- Management records now carry MFE/current ROE, time in trade, evidence used/missing, risk state, protection status, execution result, and exchange-verification result.
- TP1/TP2 quantity handling, protected runners, reduce-only verification, native SL checks, idempotency, and recovery logic were not replaced.

## Risk assessment

| Risk | Status | Mitigation |
|---|---|---|
| Duplicate execution authority | Controlled | One execution service and one management board remain in use |
| Stale discovery data | Controlled | Closed-candle validation plus `STALE_DATA` classification and freshness metadata |
| EMA50 over-blocking | Controlled | EMA50 is secondary/ranking evidence |
| Scanner API burst | Controlled | Shared cache, bounded symbols, deduplication and existing rotation |
| Partial-fill ambiguity | Controlled | Existing actual-fill/reconciliation and UNKNOWN handling retained |
| Missing native SL | Controlled | Existing verification/recovery hold retained |
| Silent target widening | Fixed | Normal reversal fallback is 0.50% TP1 / 1.00% TP2; wide-ATR safety remains |
| Runtime-state test contamination | Operational note | Tests must use a clean temporary journal; release excludes runtime DB |

## Audit boundary

No claim of 24/7 stability, live profitability, exchange latency, or live API load performance is made. Those require a controlled exchange/sandbox run and long-duration observation.
