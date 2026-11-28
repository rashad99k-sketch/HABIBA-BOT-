# Implementation Report — EMA9/VWAP Discovery + Unified Profit Explainability

## Modified source files

- `roro_confluence.py`: enriched closed-candle EMA9/VWAP/EMA50 proximity evidence; added ATR normalization, slopes, relationship/cross age, volume/body/wick context, freshness metadata, and `STALE_DATA` classification while retaining compatible states such as `AT_VWAP`.
- `ggp_discovery_scanner.py`: canonical symbol deduplication and bounded symbol enforcement at the scanner boundary.
- `ggpbot.py`: corrected normal REVERSAL TP geometry to 0.50%/1.00%; fixed the `adjust_sl_tp` result assignment so minimum-distance TP1 protection is applied; extended authoritative management decision records and live context with explainability and protection fields.

## Important non-changes

No credentials, `.env` values, exchange keys, live-mode activation, deployment configuration, execution service, recovery service, or original archive were modified. No parallel management brain or execution path was created.

## Functional behavior

The scanner discovers aligned, converging, near-cross and fresh-cross candidates using closed candles, freshness, and bounded ranking. It returns evidence only. The final execution boundary remains responsible for authorization. The management board remains the single decision authority, and the existing execution service remains the only order gateway.

## Regression correction

The baseline suite exposed a normal-reversal target regression (`98.0` instead of `99.5` for a representative BUY). The release restores the intended two-stage fallback geometry and preserves wide-ATR safety adjustment. The corrected assignment also ensures the existing minimum-distance protection applies to TP1 rather than discarding the adjusted value.

## Release identity

`NAISO-LIVE-ema9-vwap-profit-brain-v1.0.0`
