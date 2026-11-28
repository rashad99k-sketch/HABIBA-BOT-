# Test and Validation Report

## Commands executed

```bash
python3 -m compileall -q .
TRADE_JOURNAL_PATH=/tmp/naiso-clean-test.sqlite3 python3 -m pytest -q
```

## Results

- Static compilation: **PASS**
- Clean-journal full suite: **532 passed, 1 warning, 0 failed**
- Duration: **76.40 seconds**
- Warning: pandas deprecation warning for `Timestamp.utcnow` in `test_professional_discovery_scanner.py`; non-failing and unrelated to release behavior.
- Targeted post-change suite: **128 passed, 1 warning**.

## Runtime-journal isolation note

A prior full run using the working directory's persisted `data/ggp_trade_journal.sqlite3` produced 529 passed and 3 failures in `test_position_sizing_policy.py`. The journal contained an active daily drawdown cooldown created by the paper balance sequence; `execute_entry` correctly failed closed. Re-running the complete suite with a clean temporary `TRADE_JOURNAL_PATH` produced the authoritative result above. Runtime state is not a source release artifact.

## Coverage areas exercised

Scanner ranking and closed candles; EMA9/VWAP/EMA50 integration; profit brain; sniper integration; execution gateway; native protection; recovery/reconciliation; duplicate-event/order guards; position sizing; management event logging; and runtime safety mocks.

## Explicit limitations

No live exchange connection, real order, credential use, deployment, 24-hour test, or production API-load test was performed. Therefore no 24/7 stability or live profitability claim is made.
