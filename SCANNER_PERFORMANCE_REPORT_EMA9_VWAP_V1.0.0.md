# Scanner Performance / Load Report

## Method

A deterministic synthetic OHLCV benchmark used 45 requested symbols containing five duplicates, 180 candles per unique symbol, a caller-provided in-memory frame fetcher, and `max_symbols=40`. No exchange/API request was made.

## Result

- Requested entries including duplicates: **45**
- Bounded unique symbols processed: **40**
- Valid data rows: **40**
- Data errors: **0**
- Selected rows: **0** at the configured synthetic score threshold (ranking logic still processed all valid frames)
- Elapsed wall time: **0.878528 s**
- Throughput: **45.53 bounded symbols/s**
- Deduplication: **5 duplicate entries excluded before processing**

## Interpretation

This is a local deterministic CPU measurement, not an exchange latency or production capacity claim. It demonstrates bounded work and deduplication. Production performance depends on exchange response time, cache hit ratio, network conditions, pandas workload, symbol universe, and scheduler cadence.

## Load protections preserved

The implementation uses the existing shared market-data/cache path, bounded symbol count, canonicalization, closed-candle validation, watchlist rotation, and ranking-only discovery. The scanner does not place, cancel, amend, or authorize orders.
