# Release Notes — EMA9/VWAP Adaptive Entry v1.1.0

> Superseded on `strategy/ema15-rf-reentry-six-slots-20261125`: the authoritative
> entry trigger there is a closed-candle EMA9/EMA15 cross with Session VWAP
> direction confirmation, governed by
> `STRATEGY_SPEC_EMA15_RF_SIX_SLOTS.md`. EMA50 remains secondary context and never
> vetoes. This document remains the historical v1.1.0 note for `main`.

This release adds real adaptive-entry classification and indicator-integrity continuity to the existing NAISO-LIVE safety architecture. The primary signal in v1.1.0 was EMA9 crossing session OHLC4 VWAP on a closed candle. EMA15 is not used as an entry requirement in v1.1.0, and EMA50 remains secondary context.

The watchlist now carries a complete closed-candle snapshot with EMA9, VWAP, RSI, ATR, slope, volume ratio, VWAP position, trend direction, zone quality, order-block status, signal direction, readiness, source and age. Freshness is explicit, and missing or stale inputs are represented as `DATA_UNAVAILABLE` or `STALE_DATA`.

The adaptive classifier supports `IMMEDIATE`, `MICRO_PULLBACK`, `PULLBACK`, `RETEST`, and fail-closed `WAIT_FOR_STRUCTURE`. The management layer records dynamic classification changes including `SCALP`, `TREND`, `TREND_CONTINUATION`, `WEAK_MOMENTUM`, and `REVERSAL_RISK`.

The release remains paper-mode validated only. Live deployment, real orders, credential changes, 24-hour stability testing, and live API-load measurements were not performed.
