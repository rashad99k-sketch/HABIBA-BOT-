# Six-slot portfolio integration plan (working note)

Authoritative requirement: `STRATEGY_SPEC_EMA15_RF_SIX_SLOTS.md`.
Authoritative modules: `portfolio_allocator.py` and `portfolio_runtime.py`.

## Mandatory execution-boundary changes

1. Import `portfolio_allocator` and `PortfolioRuntime`; initialize `PORTFOLIO_ALLOCATOR`
   and `PORTFOLIO_RUNTIME(STATE, TRADE_STATE, DASHBOARD_STATE, _live_manager, _TRADE_LOCK)`
   after all globals and `_live_manager` exist.
2. Derive slot class centrally from trusted strategy/source hints only. Never pass
   caller `classification`, `slot_class`, `class_label`, or `label` as the authority.
3. Reserve before every PAPER and LIVE order submission. No globals may be written
   for the candidate before reserve succeeds. On pre-order failure with no exposure,
   release with explicit verified-flat evidence. On uncertain/partial exposure, retain
   the reservation and enter recovery hold.
4. Replace `_ACTIVE_TRADE` as the one-position authority with allocator counts plus
   registry state. `MEMORY["position_status"]` and dashboard flags become aggregate views.
5. Focus each position through `PORTFOLIO_RUNTIME.focus()` for management, sync,
   reconciliation, and finalization. No entry may overwrite another trade's state.
6. `finalize_trade_with_reality(symbol, close_reason=None, trade_id=None)` must focus
   the trade, release the durable allocator reservation only with strict verified-flat
   evidence, release the runtime entry, and recompute global flags from remaining slots.
7. `_main_loop_manage_open_position` must call `PORTFOLIO_RUNTIME.manage_all(...)`.
8. Startup recovery, exit verification, clear-flat paths, and scanners must be
   capacity-aware. Multiple exchange positions are normal only when they match the
   expected registry set; unmatched, duplicate, malformed, or unprotected exposure
   fails closed.
9. Dashboard/health may remain focused views, but must expose portfolio counts and
   active trade ids without changing execution authority.

## Required new integration tests

Create `test_six_slot_portfolio_integration.py` with deterministic tests for:
- five technical + one news through real `execute_entry`;
- sixth technical and seventh total rejected before order submission;
- central classification rejects caller labels and conflicting hints;
- one news cap and slot reuse after verified flat;
- exactly-once release and preservation of other positions;
- malformed flat evidence cannot free a slot;
- focus isolation across management cycles and failure containment;
- unresolved order retains reservation;
- startup recovery with six positions;
- source assertions for reserve-before-order, `require_flat=True`, and `manage_all`.

No skips, xfails, weakened assertions, or CI timeout increase. This was the
working note used for the strategy-branch implementation and verification.
