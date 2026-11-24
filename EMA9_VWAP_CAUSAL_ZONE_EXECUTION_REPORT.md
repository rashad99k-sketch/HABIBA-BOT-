# EMA9 × Session-VWAP Causal-Zone Entry — Execution Flow Report

Branch: `feat/ema9-vwap-causal-zone-entry`  
Base: `befe173 fix: unify entry authority and immediate trade management`  
Scope: entry decision + original execution handoff only. **No live orders were placed and no merge was performed.**

## 1. Audit result

The full flow was audited before edits:

1. `safe_main_loop()`
2. `main_loop_sniper()` startup recovery wrapper
3. `smart_opportunity_selection()`
4. `refresh_unified_institutional_watchlist()`
5. `_refresh_ema_vwap_setups()` using cached scanner frames only
6. `EMA_VWAP_ENTRY_ENGINE.evaluate()/authorize()`
7. `mark_executing()` one-time token
8. final fail-closed `execute_entry()` wrapper
9. original `execute_entry()` → `open_position()` → `TradeExecutionService`
10. exchange fill/native-stop verification and state/journal registration
11. `_dispatch_confirmed_trade_handoff()`
12. `TradeManagementBoard` / `LiveTradeManager` as the only routine exit authority

Findings that shaped the change:

- The previous strategy lifecycle conflated rejection/confirmation candles with the requested immediate causal-zone crossover model.
- The original execution path already contains the exchange safety checks (fill quantity, average price, native SL verification, recovery hold, journaling, sync). Those were preserved rather than replaced.
- `execute_entry()` is overridden late in `ggpbot.py`; the final wrapper is the effective boundary for all callers.
- `scaling_logic()` can call `open_position()` directly, so a second, lower-level guard was added there.
- `.env.example` still ships `PAPER_MODE=true` with `ENABLE_LIVE_TRADING=true`. This is documented as a pre-existing safety concern and was intentionally not changed in this feature branch.

## 2. Implemented decision model

`unified_ema_vwap_strategy.py` now implements:

- EMA9 from closed `close` values only.
- Session VWAP from OHLC4, cumulative within a UTC day.
- Cross detection on the newest **closed** candle only.
- `NO_CROSS`, `INCOMPLETE_CANDLE`, and duplicate-candle suppression.
- Causal FVG/displacement-order-block detection using history up to the cross candle only.
- Zone states: `VALID_ZONE`, `FRESH_ZONE`, `TESTED_ZONE`, `WEAK_ZONE`, `INVALIDATED_ZONE`, `ENTRY_WINDOW_MISSED`.
- ATR-normalized zone distance and inside-zone enforcement; no chasing.
- BUY only on EMA9 crossing above VWAP inside a bullish causal zone.
- SELL only on EMA9 crossing below VWAP inside a bearish causal zone.
- EMA15/50 are observational only and never gate the entry.
- Pullback/retest/rejection fields are diagnostic history only; they are not entry requirements.

Reason codes are restricted to the exact whitelist:

`NO_CROSS`, `INVALID_ZONE`, `WRONG_ZONE_DIRECTION`, `STALE_MARKET_DATA`, `INCOMPLETE_CANDLE`, `DUPLICATE_SIGNAL`, `ENTRY_WINDOW_MISSED`, `EXECUTION_REJECTED`, `POSITION_NOT_CONFIRMED`, `TRADE_HANDOFF_FAILED`.

Exact diagnostics emitted:

`[EMA9_VWAP_CROSS]`, `[ZONE_DETECTED]`, `[ZONE_QUALITY]`, `[ENTRY_VALIDATION]`, `[ENTRY_AUTHORIZED]`, `[ENTRY_REJECTED]`, `[ENTRY_EXECUTION_STARTED]`, `[ENTRY_EXECUTION_CONFIRMED]`, `[TRADE_HANDOFF]`, `[TRADE_CLASSIFIED]`.

## 3. Execution and authorization boundary

The strategy engine never sends orders. It only produces an authorization token:

- `authorize()` binds symbol, side, entry price, TP1, ATR, zone quality, zone direction, zone boundaries, and data age.
- `mark_executing()` consumes the authorization exactly once.
- `execution_handoff_allowed()` is checked in the final `execute_entry()` wrapper before `_original_execute_entry_for_journal()` is called.
- `open_position()` separately refuses any call without a matching `_INSTITUTIONAL_ENTRY_CONTEXT` token.

This keeps legacy callers, scanners, and `scaling_logic()` from submitting a second order without an EMA9 × VWAP causal-zone authorization.

## 4. Brain handoff

`_dispatch_confirmed_trade_handoff()` is called only after the original execution path returns success and the platform position is confirmed.

Before handoff it requires:

- local position open and no recovery/execution hold;
- registered symbol and side matching the authorized setup;
- trade ID, actual entry, filled quantity, initial stop, TP1 and TP2;
- finite and directionally valid SL/TP geometry;
- PAPER confirmation, or for LIVE: exchange order ID, `FILLED` state, and a verified native stop;
- finite causal-zone boundaries, ATR, EMA9 and VWAP evidence.

The recorded payload includes symbol, exchange position ID, side, actual entry, filled quantity, initial SL/TP1/TP2, ATR, EMA9, VWAP, zone type, OB type, zone boundaries, causal timestamp, strategy ID, regime, and execution confirmation status.

`classify_management_regime()` in `ggp_profit_brain.py` is pure and non-authoritative. It returns one of:

`SCALP`, `TREND`, `CONTINUATION`, `HIGH MOMENTUM`, `WEAK MOMENTUM`.

It does not set `profit_mode`, authorize entries, change sizing, or send orders. `TradeManagementBoard` / `LiveTradeManager` remain the sole routine management authority.

## 5. Scanner/cache/rate-limit preservation

- `_refresh_ema_vwap_setups()` reads only `_peek_scanner_frame()`.
- No new OHLCV request is issued for cached symbols.
- Existing watchlist caps, batch size, refresh interval, scanner task locks, cache TTL, and state locks are unchanged.
- Integration coverage asserts the strategy refresh performs exactly one cache peek and zero fetch calls.

## 6. Verification

All commands were run locally on the branch:

| Check | Result |
| --- | --- |
| `python -m py_compile unified_ema_vwap_strategy.py ggp_profit_brain.py ggpbot.py` | pass |
| `python preflight.py` | `PREFLIGHT OK: files, syntax, imports, and required GGP entry points verified` |
| `python -m compileall -q .` | pass |
| `python -m pytest -q test_unified_ema_vwap_strategy.py` | 16 passed |
| Integration + brain + gate + sizing suites | 44 passed |
| `python -m pytest -q` | **342 passed in 129.19s** |

The 16 focused scenarios cover:

1. UTC OHLC4 session VWAP.
2. Closed-close EMA9 source.
3. Forming-candle cross rejection.
4. BUY demand-zone readiness.
5. SELL supply-zone readiness.
6. EMA15 non-gating.
7. No extra pullback/confirmation gate.
8. Causal zone history.
9. All zone-quality states.
10. Stale data rejection.
11. Duplicate-candle safety across restart.
12. No-chase enforcement.
13. Authorization binding.
14. Exact success diagnostics and once-only handoff.
15. Exact reject-code whitelist.
16. Exact causal FVG boundaries.

Integration coverage adds: final wrapper dispatch, blocked legacy calls, lower-level `open_position()` guard, confirmed-position once-only handoff, exchange rejection blocking handoff, missing-fill rejection, full payload contents, and unchanged scanner-cache behavior.

## 7. Known limitations / safety notes

- No live or paper runtime trading was attempted; all execution tests are mocked.
- This branch is not proof of production readiness. Repository documentation already describes the broader integration as partial/not production-accepted.
- `.env.example` retains `ENABLE_LIVE_TRADING=true`; review before any deployment.
- PR [#7](https://github.com/rashad99k-sketch/HABIBA-BOT-/pull/7) is open from `feat/ema9-vwap-causal-zone-entry` into `main`. It has not been merged.
