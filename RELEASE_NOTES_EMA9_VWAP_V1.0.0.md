# Release Notes and Known Limitations

## Included

This release integrates faster EMA9/VWAP/EMA50 discovery evidence and strengthens one existing institutional management authority with traceable decision metadata. It includes audit, implementation, performance, decision-flow, test, deployment and rollback documentation.

## Known limitations

- No 24-hour or longer stability run was performed.
- No live BingX request, order, fill, latency, slippage, or production load measurement was performed.
- Synthetic scanner throughput is not a production capacity guarantee.
- The full suite emits one non-blocking pandas deprecation warning.
- Persistent journal state intentionally affects cooldown behavior; test runs must use an isolated temporary journal or a deliberately prepared test database.
- EMA50 is secondary evidence; it is not a promise that every market regime will produce a valid trade.
- The bot remains a complex Python application with exchange, network, clock, and dependency risks that require staged operational validation.

## Safety status

Source changes preserve fail-closed entry gates, actual-fill initialization, native stop verification, reduce-only quantity checks, recovery handling, idempotency, and single-authority execution/management. Live trading was not activated.
