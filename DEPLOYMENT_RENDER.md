# NAISO-LIVE Render Deployment

This project runs as a single-process Python web service. The same process owns the dashboard, scanner, exchange reconciliation, trade journal, recovery logic, and trade-management supervisor. Do not run multiple instances against the same journal or exchange account.

## Safe first deployment

Use the included `render.yaml` or create a Render Web Service with:

```text
Build command: pip install -r requirements.txt
Start command: python ggpbot.py
Health check: /naiso/health
```

The descriptor deliberately sets `PAPER_MODE=true` and `ENABLE_LIVE_TRADING=false`. Leave those values unchanged for the initial deployment. Do not provide real BingX credentials during offline validation or the first hosted paper run. Telegram variables are optional.

Render must provide a persistent storage strategy if the trade journal is expected to survive restarts. Without persistent storage, the service can start safely but cannot guarantee recovery of runtime journal state after an instance replacement. Before any Demo or live consideration, verify the SQLite journal backup and restore process independently.

## Environment requirements

The required Python dependencies are pinned or declared in `requirements.txt`. The service binds to Render's `PORT` through the existing hosted dashboard startup logic. `DASHBOARD_BIND_HOST=0.0.0.0` is required for hosted access. Keep `NAISO_NATIVE_SL_REPAIR=false` until an owner explicitly accepts an exchange-verified repair policy.

## Operational checks

After deployment, check `/health` and `/naiso/health`. Confirm paper mode, recovery gates, scanner task status, native-stop status, journal integrity, API counters, and resource metrics. A healthy endpoint is not proof of exchange connectivity, order safety, or six-month uptime.

Use staged evidence before considering any extended operation: 24-hour offline soak, 72-hour hosted paper soak, seven-day monitored paper run, and then longer staged operation. Do not enable live trading based only on unit tests or a healthy dashboard.

## Shutdown and rollback

Stop the service gracefully before replacing the release. Preserve the journal and logs needed for recovery. If exchange state is unknown, keep the recovery hold and block new entries; never treat an unknown account state as flat. Roll back the application code only after reconciling the exchange state and journal.
