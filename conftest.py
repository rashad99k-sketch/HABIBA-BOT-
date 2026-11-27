"""Test-session isolation for persisted runtime safety state.

Several modules open the default SQLite trade journal at import time. Without
isolation, a daily-loss cooldown written by one test run can leak into the next
run and make unrelated entry tests fail closed. CI starts from a clean checkout,
but local/soak runs repeat in the same workspace, so pin the journal to a
session-private temporary file before any test module imports ``ggpbot``.
"""
from __future__ import annotations

import atexit
import os
import shutil
import tempfile


_JOURNAL_DIR = tempfile.mkdtemp(prefix="naiso-pytest-journal-")
os.environ.setdefault("TRADE_JOURNAL_PATH", os.path.join(_JOURNAL_DIR, "ggp_trade_journal.sqlite3"))
os.environ.setdefault("PAPER_MODE", "true")
atexit.register(shutil.rmtree, _JOURNAL_DIR, True)
