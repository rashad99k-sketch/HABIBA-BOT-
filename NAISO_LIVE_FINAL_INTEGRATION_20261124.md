# NAISO-LIVE final production integration (2026-11-24)

This note is the authoritative summary for the final integration on `main`.
Older reports that mention 40% allocation, 10x leverage, or a 1% wallet-risk model
are historical and are superseded by `CAPITAL_ALLOCATION_POLICY_AR.md` and this
document.

## Single authorities

| Concern | Single authority | Notes |
| --- | --- | --- |
| Position size | `ggp_position_sizing.calculate_position_size()` | One calculation. Exchange precision only rounds the result; no downstream resize. |
| Trade management | `TradeManagementBoard` / `LiveTradeManager` | No second manager. TP1 is exactly 50% of the verified filled quantity. |
| Entry trigger | `roro_confluence.validate_dual_ema_vwap_entry_gate()` | M15 closed-candle EMA9 / Session VWAP cross only. RF/V2/radar/proximity are ranking-only. |
| Post-cross entry | `roro_confluence.evaluate_post_cross_rejection_breakout()` | Pullback/retest → closed rejection candle → strictly later closed breakout. |
| Durable state | `TradeJournal` | Cross events, immutable sizing decisions, entry context, fills and recovery state. |

## Sizing policy

- Basis: available (free) USDT balance from one snapshot, timestamped in the decision.
- STANDARD: 20% target margin.
- STRONG: 30% target margin, only with independent institutional authorization/evidence.
- Leverage: 20x, set once and verified by the existing execution path.
- The structural stop/risk gate may reduce the size once. That reduction is recorded,
  never silently repeated.
- One `POSITION_SIZING_DECISION` event is persisted per trade before order submission.
  It records balance basis/source/time, available balance, requested and effective
  classification, target margin, leverage, target notional, stop distance, risk budget,
  maximum quantity, final exchange quantity, estimated margin, estimated stop loss,
  conservative fee/slippage assumptions, and the reduction reason.

These are margin-allocation percentages. They are **not** the weak-market profit
targets below and must never be replaced by 0.25% / 0.50% / 1.00% account allocation.

## Profit convention

`RANGE_PROFIT_TARGETS_PCT = (0.25, 0.50, 1.00)` and trend TP1/TP2 are
**unleveraged price-move percentages from the verified entry price**. They are not
account allocations and not exchange ROE percentages.

At 20x, before fees, funding and slippage:

| Price move | Gross ROE | Estimated cost model |
| --- | --- | --- |
| 0.25% | 5% | 0.05% taker per side + 0.02% slippage per side |
| 0.50% | 10% | same |
| 1.00% | 20% | same |
| TP1 2.00% | 40% | same |
| TP2 3.00% | 60% | same |

`ggp_profit_brain.exchange_pnl_at_move()` exposes a pre-trade gross-to-net estimate.
It is explicitly labelled `PRE_TRADE_ESTIMATE_NOT_REALIZED_PNL`; realized PnL remains
exchange-reported. Gross ROE/PnL is never presented as net.

## Entry lifecycle

1. Strict M15 closed-candle cross creates one durable `confirmed_cross_events` row.
2. The event is projected into the unified Watchlist with ranking-only sources kept distinct.
3. Later closed candles must show a pullback/retest of the cross zone.
4. A later **closed** rejection candle must record its high and low.
5. A strictly later **closed** candle must break that high (BUY) or low (SELL).
6. An ATR chase cap rejects extended breakouts. Unfinished candles can never authorize an entry.
7. Delayed entries keep the original cross event ID; execution binds the durable
   rejection/breakout context into the immutable entry handoff.

The handoff includes the cross event identity, direction/timestamp, zone/quality,
Order Block, supply/demand, liquidity, structure, relative volume, ADX/DI, ATR,
EMA9/EMA50/VWAP, regime, rejection high/low, planned and actual entry price, and
actual filled quantity.

## Safety invariants

UNKNOWN is never FLAT. Recovery Hold blocks entries but never stops management of an
open trade. Native stops are verified by readback and never duplicated. API Governor,
bounded retries, no uncertain order-write retries, watchdog/recovery, main-loop
progress, Global Discovery lifecycle, shared OHLCV cache, strict close verification,
reconciliation, dashboard/Telegram, and Flask/Render health checks remain unchanged.

## Verification

Run from the repository root:

```bash
python -m pytest -q --tb=short
python -m compileall -q .
git diff --check
python soak_naiso_longrun.py
```
