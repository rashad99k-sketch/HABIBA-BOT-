# HABIBA-BOT — extracted GGP / NAISO-LIVE source

This repository now contains the normalized contents of the source ZIP that was previously the repository's only file. The original archives are preserved under [`archives/`](archives/) and are not part of the runtime path.

> **Safety status: PAPER / DEMO candidate only. Not live-ready.** No exchange connection, order, or trade is performed by the extraction or the offline tests.

## Entry point

- `ggpbot.py` — main application (Windows launchers: `RUN.bat` / `RUN_WINDOWS.bat`).
- `preflight.py` — required-file, syntax, import, and entry-point verification.
- `RUN_TESTS.bat` or `python -m pytest -q test_*.py` — offline test suite.

## Safe start

1. Create a virtual environment and run `pip install -r requirements.txt` (pinned: `ccxt==4.5.92`).
2. Copy `.env.example` to a local `.env` and keep `PAPER_MODE=true`, `ENABLE_LIVE_TRADING=false`, and empty credentials until review is complete. Never commit `.env`.
3. Run `python preflight.py`, then the tests.
4. Do not enable LIVE: the project documentation explicitly states it is not accepted for production and needs PAPER + BingX Demo validation first.

## Documentation

- [`README_AR.md`](README_AR.md) — original Arabic project README.
- [`IMPLEMENTATION_REPORT_EN.md`](IMPLEMENTATION_REPORT_EN.md) — EMA/VWAP integration report and remaining blockers.
- [`EMA_VWAP_CENTRAL_GATE_PATCH_NOTES_AR.md`](EMA_VWAP_CENTRAL_GATE_PATCH_NOTES_AR.md) — notes for the newest patch.
- [`EXTRACTION_REPORT.md`](EXTRACTION_REPORT.md) — repository audit, extraction decisions, and validation results.
