# Strategy Specification — EMA9/EMA15 Confirmation, RF Re-entry Wait, Six-Slot Portfolio

Status: authoritative for the strategy changes on branch
`strategy/ema15-rf-reentry-six-slots-20261125`. It supersedes older documents only
for the three requirements below; reliability, reconciliation, and fail-closed
execution rules remain unchanged.

## 1. Entry confirmation

- Entry authorization requires a **confirmed EMA9/EMA15 cross on closed candles**.
- The same signal must have **Session VWAP direction confirmation** on a closed
  candle. EMA50 remains advisory context and must never be an automatic veto.
- The existing closed-candle freshness, durable event identity, consumed-event,
  and post-verified-fill consumption protections remain mandatory.
- No scanner, RF, V2, radar, proximity, or legacy setup may bypass this gate.

## 2. Range Filter opposite-signal re-entry wait

- When an open position is closed because of an opposite Range Filter (RF)
  signal, the finalizer must persist a durable RF re-entry block.
- The block must record the symbol, blocked direction, RF signal candle
  timestamp, and close timestamp.
- Re-entry is blocked until a **strictly later closed candle** produces a valid
  BUY/SELL entry signal. An older signal, the same signal candle, or an already
  consumed event can never satisfy the wait.
- Existing consumed-event and signal-timestamp protections are preserved, not
  replaced or weakened.

## 3. Six-slot portfolio capacity

- The portfolio supports **exactly six total positions**:
  - at most **five** Technical/Institutional positions;
  - at most **one** independent News Trading position.
- Slot classification is derived centrally and cannot be trusted from a caller
  label. Allocation and release must be race-safe and durable.
- A slot is reserved before order submission and released only after the
  position is verified flat. Unknown, malformed, duplicate, or over-capacity
  exchange snapshots must fail closed and never free a slot.
- All six positions require verified native protection and independent exit
  management; no entry may overwrite another position's state.

## Non-goals for this specification

- Do not change CI timeout (`timeout-minutes: 15`).
- Do not add skips, xfails, weakened assertions, or early merges.
- Do not claim production readiness from unit tests alone.
- Do not mix these strategy changes into the reliability PR #28.
