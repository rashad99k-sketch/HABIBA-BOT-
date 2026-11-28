# Deployment and Rollback Runbook

## Preconditions

1. Review the audit and test report.
2. Keep the original archive as the immutable rollback baseline.
3. Install exactly `requirements.txt` in an isolated environment.
4. Configure credentials outside the archive; never copy secrets into source control or the ZIP.
5. Run preflight and the clean-journal test command before any exchange connection.

## Staged deployment

1. Unpack the versioned ZIP into a new directory; do not overwrite the original installation.
2. Start in paper/sandbox mode with a new journal path and verify scanner freshness, watchlist rotation, management records, native-stop verification behavior, and recovery hold behavior.
3. Confirm that the scanner only produces ranked evidence and that all order actions pass through `TradeExecutionService`.
4. Only an authorized operator may separately enable live mode after exchange-side checks; this delivery does not enable it.

## Rollback

1. Stop the bot gracefully and confirm no unresolved exchange operation is in flight.
2. If live, reconcile positions and native stops directly against BingX before switching code.
3. Preserve the journal and logs for investigation; do not delete evidence.
4. Point the launcher to the prior known-good installation/archive.
5. Restart in paper/sandbox mode, run preflight and the clean-journal test suite, then reconcile state again.
6. Never restore credentials from an archive. Reapply them through the operator's existing secret mechanism.

## Emergency safety

If exchange state is UNKNOWN, do not force a new entry. Keep the recovery hold active, reconcile fills/positions/orders, verify native protection, and only then resume.
