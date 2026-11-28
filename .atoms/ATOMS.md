# HABIBA / GGP Project Context

## Section: Project Scope
The repository is the Python GGP / HABIBA trading bot. It contains both the independent Demo lifecycle preview (`/lifecycle`) and the live EMA9 + Session VWAP Cross strategy integration. The Demo engine remains isolated and cannot submit exchange orders. The live strategy uses the existing GGP execution, protection, recovery, journal, sizing and position-management authorities.

## Section: Protected Live Architecture
- `execute_entry()` remains the only order-entry boundary.
- `TradeManagementBoard / LiveTradeManager` remains the sole live position-management authority.
- Preserve BingX synchronization, Native SL verification, fill reconciliation, recovery holds, trade journal, API resilience, locks, health endpoints, leverage and sizing constraints.
- Never let Demo state mutate `MEMORY`, `STATE`, live orders, balances, or exchange positions.
- A live Cross Event must pass durable event identity, closed-candle freshness, ADX, location, SL geometry, order-idempotency, actual fill and Native SL verification.

## Section: Strategy and Authorization Semantics
- Primary trigger: EMA9 crossing canonical Session VWAP on a validated closed M15 candle (timeframe remains configurable via `INTERVAL`). EMA50 is context only, not a trigger or general veto.
- Cross identity: `EMA9VWAP:<symbol>:<BULLISH|BEARISH>:<candle_timestamp>`; each event is independent of symbol ranking and is retained through subsequent neutral scans until consumed, rejected or expired.
- `BASE ADX >= 18` is absolute. Missing/invalid ADX or ADX below 18 blocks entry.
- Institutional analysis grades entry location only (directional OB, FVG, confirmed S/R, value area and optional liquidity evidence). Sweep, MSS and retest are not universal Cross-strategy entry gates.
- Location must be actionable and within the configured 1.5 ATR acceptance distance; a structural Stop Loss must be valid before entry.
- Trade mode is latched at entry: SCALP unless strong side-aligned trend conditions are all confirmed (ADX >= 25, matching regime, DI, EMA9 slope and relative volume). SCALP is not later upgraded by generic legacy heuristics.
- SCALP profit target is an underlying price move of 0.25% or 0.50%; 1.00% is reserved for exceptional closed-candle follow-through. SCALP closes the full position at its target.
- TREND uses the existing trend target resolver and manager; absent a closer valid structural/liquidity plan its fallback references are +2% TP1 and +3% TP2. TP1 closes 50% and TP2 closes the remaining 50%.
- Leverage remains 20x. Cross entries use standard 20% account-margin allocation; the unrelated 30% strong-allocation tier is not granted by a Cross score alone.
- READY is derived from authorization, never from score alone.

## Section: Scanner and 24/7 Runtime
- The active USDT perpetual universe is bounded by `SCANNER_UNIVERSE_MAX` (default 80), refreshed by global discovery every `GLOBAL_SCAN_INTERVAL` (default 20 minutes).
- Watchlist analysis runs on the short candidate schedule (default 15 seconds); the unified location/cross refresh uses a 20-second refresh interval and 12-symbol batches by default.
- Cross events are queued immediately when discovered, including explicit rejected events such as ADX < 18; symbol ranking cannot erase their identity.
- The existing one-position safety architecture is preserved: discovery pauses while a position is open or exchange state is ambiguous; open-position management and reconciliation continue independently. This is continuous 24/7 process operation, not simultaneous multi-position scanning/trading.
- Keep data fresh, use closed candles, and fail closed on missing market data or uncertain exchange state.

## Section: Demo Runtime Boundaries
- `habiba_demo.py` owns synthetic Demo events, lifecycle states, simulated fills, positions, PnL and reconciliation.
- `habiba_demo_web.py` exposes only `/lifecycle` and `/lifecycle/api/*` routes.
- `static/habiba_lifecycle_demo.html` is the Arabic RTL Demo interface.
- Every simulated order and position is tagged `DEMO_SIMULATION_ONLY` / `SIM-*`.
- Demo code must not import CCXT or invoke live execution/protection functions.

## Section: Validation Requirements
- Test strict closed-candle cross detection and rejection of an exact-touch-without-cross.
- Test unique Cross Event identity, persistence, neutral-scan survival and retention of rejected ADX events.
- Test ADX < 18 rejection, ADX = 18 acceptance at the base gate, invalid location rejection and location analysis without universal Sweep/MSS/Retest.
- Test SCALP/TREND classification and mode latching; test 0.25%, 0.50%, exceptional 1.00% scalp targets and 50/50 TREND targets.
- Preserve tests for duplicate order blocking, SL geometry, real fills, Native SL, recovery and trade reconciliation.
- Run the full repository suite in an environment with the dependencies in `requirements.txt`; distinguish collection/environment errors from assertion failures.
- Do not claim LIVE production readiness without dependency-complete tests and a separate BingX Demo/VST runtime smoke test.
