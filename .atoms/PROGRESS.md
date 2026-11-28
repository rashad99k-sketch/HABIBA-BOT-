# HABIBA / GGP Implementation Progress

## Section: Current Revision
Unified the live EMA9 + Session VWAP Cross lifecycle with the existing institutional Watchlist and GGP entry/position-management path. The earlier `/lifecycle` Demo preview remains isolated and unchanged as a presentation/simulation tool.

## Section: Completed
- Added strict EMA9 / canonical Session VWAP cross detection from validated closed candles, with unique event identity per symbol, direction and candle timestamp.
- Made confirmed cross events the primary Watchlist lifecycle object, independent of symbol-ranking rows; later neutral scans do not overwrite or erase an active event.
- Retained confirmed rejected events (including ADX below 18) in the event queue so they remain visible and auditable rather than disappearing behind the opposite-side hypothesis.
- Aligned institutional DMI/ADX smoothing with the live GGP ADX/RMA implementation so the analyzer and final entry gate use the same calculation at the hard threshold.
- Replaced universal institutional setup gating for Cross events with location-only evaluation: directional OB, FVG, confirmed support/resistance, value-area location and optional liquidity evidence.
- Explicitly removed universal Sweep/MSS/Retest requirements from the Cross setup. Legacy Reversal / Continuation setups retain their own setup-specific checks.
- Added `EMA9_VWAP_CROSS_LOCATION` as an explicit setup type through `smart_opportunity_selection()` and `execute_entry()`; final cross identity, direction, location status/distance, ADX and structural SL are revalidated before the existing order path.
- Kept `execute_entry()` as the sole exchange order boundary and `TradeManagementBoard / LiveTradeManager` as the sole position exit authority.
- Added entry-time SCALP/TREND classification and latched mode through position management. TREND requires ADX >= 25 plus matching regime, DI, EMA9 slope and relative-volume confirmation; otherwise use SCALP.
- Added SCALP price-move targets of 0.25% / 0.50%, with 1.00% reserved for exceptional closed-candle follow-through. Existing full-close SCALP behavior remains.
- Preserved TREND two-stage management: 50% at TP1 and remaining 50% at TP2; existing structural target resolver, trailing, Native SL and recovery authorities remain the execution/management owners.
- Cross entries retain 20x leverage and standard 20% account-margin allocation; Cross scores cannot grant the independent strong 30% allocation tier.
- Included the bounded active USDT perpetual universe in candidate merging so the rotating Watchlist can analyze the whole configured universe without starving low-ranked symbols.
- Updated `ATOMS.md`, `PROGRESS.md`, README and engineering report to reflect live integration separately from Demo.

## Section: Validation
- Focused cross-lifecycle, Watchlist integration, profit-brain, TP ladder, institutional market-intelligence and EMA50/VWAP compatibility tests: 150 passed, 1 deselected.
- The deselected test requires importing `ggpbot.py`; `ccxt` is absent from the current validation container.
- Re-ran the full suite after updating the stale architecture assertion: `444 passed, 47 failed, 1 skipped, 6 errors`. All 47 failures and all 6 collection errors terminate on `ModuleNotFoundError: ccxt` while importing `ggpbot.py`; no functional assertion failure appeared in that run. The focused run explicitly deselected one test that also imports `ggpbot.py` because `ccxt` is unavailable. `requirements.txt` includes `ccxt==4.5.92`, but CCXT and Flask are absent from this validation container.
- Python `py_compile` / `compileall` passed. Demo lifecycle suite: `11 passed, 1 skipped`. Runtime-independent strategy tests: `150 passed, 1 deselected`.
- No BingX account/API order was submitted during validation. No real exchange execution, fills, Native SL or 24/7 hosted process was verified in this environment.

## Section: Not Yet Verified
- Full repository pytest run with all `requirements.txt` dependencies installed.
- Runtime import and Flask dashboard smoke test in a dependency-complete environment.
- PAPER-mode end-to-end entry/exit smoke test using current exchange metadata.
- BingX Demo/VST verification of order fill, Native SL attachment and close reconciliation.
- Render hosted restart/soak test and operational 24/7 uptime monitoring.

## Section: Live Trading Safety
- The existing one-position safety gate is preserved: scanner/discovery pauses while a position is open or exchange state is ambiguous; the position manager and reconciliation continue independently.
- No weakening of duplicate-order, balance, position-sync, reduce-only close, fill-verification, Native SL, recovery-hold or exchange API safeguards is intended.
- This delivery is code-integrated but is not certified LIVE READY. Do not enable live trading until dependency-complete tests, PAPER validation and BingX Demo/VST runtime checks pass.
