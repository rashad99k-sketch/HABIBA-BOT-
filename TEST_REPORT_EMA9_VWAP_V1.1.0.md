# v1.1.0 Test Report

## Exact results

The command below was run with an isolated temporary journal:

```bash
TRADE_JOURNAL_PATH=/tmp/naiso-v1.1-test.sqlite3 python3 -m pytest -q
```

Result: **536 passed, 1 warning, 0 failed in 76.48s**.

Static compilation also passed:

```bash
python3 -m compileall -q .
```

Targeted results were **89 passed** for the new adaptive-entry/indicator tests combined with market-intelligence tests, and **114 passed** for existing market-intelligence, institutional bridge, dashboard, and EMA50/VWAP integration regressions.

A paper-mode import and Flask test-client health check returned HTTP 200 with `mode=PAPER`.

The one warning is the existing pandas deprecation warning for `Timestamp.utcnow` in `test_professional_discovery_scanner.py`. It did not cause a failure.

## Limitations

No live BingX request, order, fill, credential use, deployment, 24-hour stability run, or production API-load test was performed. Runtime journals must be isolated for deterministic tests because persisted drawdown cooldown state is intentionally fail-closed.
