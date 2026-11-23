# Repository Extraction, Normalization & Validation Report

Branch: `chore/professional-repository-extraction` (created from `main`; `main` untouched)
Date: 2026-11-23

## A. Repository Audit

| Item | Finding |
|---|---|
| Visibility / access | Public repo, read OK; write access confirmed with `git push --dry-run` |
| Default branch | `main` |
| Head before changes | `195b4be28f0c375c0df63d699f93288b237bb044` — "Add files via upload" (signed) |
| Tree before changes | One file only: `NAISO-LIVE-main-EMA-VWAP-CENTRAL-GATE-PATCHED.zip` (738,355 bytes) |
| Other config found before | None (no README, requirements, Docker, Actions, render.yaml, package.json, pyproject.toml) |

### Archives discovered

| Archive | SHA-256 | Entries | Root folder |
|---|---|---|---|
| `NAISO-LIVE-main-EMA-VWAP-CENTRAL-GATE-PATCHED.zip` | `53942d5ebdff6fa068a9ec292e2d83738add2afef543eae0b9cb52cdb5566a99` | 99 | `NAISO-LIVE-main/` |
| `GGP_RELEASE_9_LONG_RUN_STABILITY_HARDENED.zip` (nested in the first) | `bf8a32f7f7bf320908b3b05c2bd060c0230898aa88213f354163111c7754304a` | 98 | `ggp_env_patch/` |

Both archives were inspected in memory before any write. No absolute paths, `..` traversal, backslash paths, symlinks, or unusual modes were found.

## B. Extraction Report

- **Archives inspected:** 2
- **Archives merged into the runtime tree:** 1 (the newer outer snapshot)
- **Archives preserved as source artifacts only:** 2 (both now in `archives/`, outside the runtime path)
- **Files extracted to repository root:** 99 (including hidden `.env.example` and `.gitignore`)
- **Files created by the nested archive:** 0 — see conflict policy below.

### Conflict policy applied

| Situation | Decision | Reason |
|---|---|---|
| 92 files byte-identical in both snapshots | Kept the outer copy only; no duplicate created | Identical content |
| `ggpbot.py` differs (outer 534,194 B / inner 526,448 B) | **Kept the outer version**, never overwritten by inner | Outer is newer (2026-11-23 20:03) and contains the central EMA/VWAP fail-closed gate; inner is the older Release 9 build |
| 5 files only in outer (`unified_ema_vwap_strategy.py`, `test_unified_ema_vwap_strategy.py`, `test_central_entry_gate.py`, `EMA_VWAP_CENTRAL_GATE_PATCH_NOTES_AR.md`, `IMPLEMENTATION_REPORT_EN.md`) | Extracted | Newest patch content, absent from inner |
| 5 files only in inner (`.pytest_cache/**`) | **Not extracted** | Generated pytest cache, explicitly ignored by project `.gitignore`; not source |
| `RELEASE_MANIFEST.txt` | Preserved unchanged, but treated as stale | Its `ggpbot.py` hash matches neither snapshot and it lists `tests/` + SQLite files absent from both archives |
| Root duplicate of the nested ZIP created during the first extraction pass | Removed after verifying byte-identity with `archives/` copy | Avoid placing a ZIP in the runtime path; documented here |

### Moved files

- `NAISO-LIVE-main-EMA-VWAP-CENTRAL-GATE-PATCHED.zip` → `archives/` (byte-identical).
- Nested ZIP bytes → `archives/GGP_RELEASE_9_LONG_RUN_STABILITY_HARDENED.zip` (byte-identical).
- `.gitignore`: added `.gitignore` exception `!archives/*.zip` so the preserved sources are tracked, and `logs/` for runtime artifacts created by verification.

### Files not extracted / not kept in the runtime path

- `ggp_env_patch/.pytest_cache/**` (5 generated files).
- Both source ZIPs (kept under `archives/` instead of the runtime root).
- Generated verification artifacts: `logs/ggp_runtime.log`, `data/ggp_trade_journal.sqlite3`, `__pycache__/`, `.pytest_cache/` — all gitignored; the log was removed from the working tree.

No original project source file was deleted. No README, launcher, or deployment setting was replaced; the added `README.md` is an index that links to the original documents.

## C. Final Repository Structure

```text
HABIBA-BOT-/
├── .env.example                 # safe template (PAPER by default, empty credentials)
├── .gitignore                   # original + archive/runtime-artifact exceptions
├── README.md                    # added index (does not replace README_AR.md)
├── EXTRACTION_REPORT.md         # this report
├── ggpbot.py                    # MAIN ENTRY POINT (534,194 B, newest patched build)
├── preflight.py                 # required-file / syntax / import verification
├── requirements.txt             # ccxt==4.5.92, pandas, numpy, flask, requests,
│                                # psutil, pytest, python-dotenv
├── RUN.bat / RUN_WINDOWS.bat / RUN_TESTS.bat
├── unified_ema_vwap_strategy.py  # new central gate engine
├── ggp_*.py, bingx_*.py, roro_*.py   # 17 runtime modules (flat layout, as shipped)
├── test_*.py                    # 25 offline test modules (flat layout, as shipped)
├── *.md, RELEASE_MANIFEST.txt   # 87 original reports + manifest (preserved)
├── data/README_AR.md            # original data-folder documentation
├── archives/                    # source ZIPs, audit only, not runtime
│   ├── NAISO-LIVE-main-EMA-VWAP-CENTRAL-GATE-PATCHED.zip
│   └── GGP_RELEASE_9_LONG_RUN_STABILITY_HARDENED.zip
└── .git/
```

There is no `NAISO-LIVE-main/`, `ggp_env_patch/`, or `project/project/project/` nesting. Tests remain flat because their imports and `RUN_TESTS.bat` assume the root-level layout.

## D. Validation Report

Environment: Python 3.11.6; all eight `requirements.txt` packages installed. All commands ran with `PAPER_MODE=true`, `ENABLE_LIVE_TRADING=false`, and empty BingX credentials. No exchange connection, order, or trade was made.

| Check | Result |
|---|---|
| `python preflight.py` | **PASS** — files, syntax, imports, required GGP entry points verified; printed `[MODE] PAPER` |
| `python -m compileall -q .` | **PASS** — all Python files compile |
| `python -m pytest -q test_unified_ema_vwap_strategy.py test_central_entry_gate.py` | **14 passed** |
| `python -m pytest -q` (full suite) | **315 passed, 10 failed** (110.80 s) |
| Same full suite on a pristine extraction of the original ZIP | **315 passed, 10 failed** (100.41 s) — identical |

### The 10 failures (all pre-existing in the source archive)

- `test_position_sizing_policy.py`: 4 allocation/risk-gate expectation failures and 3 `execute_entry` tests now blocked by the new `EMA_VWAP_ENTRY_GATE` (`legacy_or_unconfirmed_entry`).
- `test_recovery_lifecycle.py`: 3 native-stop move tests expecting behavior that the shipped fixed-native-stop policy deliberately does not perform.

The three `execute_entry` failures are **new relative to the report in `IMPLEMENTATION_REPORT_EN.md` (which recorded 7 failures)** because that report predates the final central-gate patch; they are present in the untouched archive too, so extraction introduced no regression.

### Dependency / CI notes

- No `pyproject.toml`, lockfile, Dockerfile, GitHub Actions workflow, or deployment manifest exists in the source archive; there is no CI pipeline to run. `requirements.txt` pins only `ccxt`; all other packages are unpinned.
- `RELEASE_MANIFEST.txt` is stale (wrong hash and layout) and should not be used as an integrity check until regenerated.
- A deprecation warning from pandas may appear in tests; it does not affect results.

## E. Security & Operational Risks

- No private keys, non-empty API credentials, or Telegram tokens were found in either archive; `.env.example` contains only empty values and PAPER defaults.
- `SECURITY_ROTATION_AR.md` states that an **earlier** delivery contained real credentials. Those credentials must be considered compromised and rotated; they were not present in the supplied archive and were not copied anywhere.
- `resolve_trading_mode()` requires `PAPER_MODE=false`, `ENABLE_LIVE_TRADING=true`, and both BingX credentials for LIVE. Runtime stayed PAPER throughout.
- The project documentation explicitly says it is **not production/live accepted**; the EMA/VWAP gate still depends on the old institutional `ENTRY_READY` state, and the dashboard manual `/trade` route remains outside the gate.
- Do not deploy, do not enable live trading, and do not merge into `main` until the failing tests and acceptance blockers are reviewed by the project owner.

## F. Delivery

- Branch: `chore/professional-repository-extraction` (commits listed in the Git history; `main` unchanged).
- Pull request and CI status: recorded in the PR description / final handoff after push.
- No merge to `main` and no production deployment were performed.
