"""EMA9 / Session VWAP cross-event lifecycle.

This module is the missing link between crossover discovery and entry
authorization:

    scan -> immutable CrossEvent -> persistence -> Watchlist ingestion
         -> crossover-zone analysis -> READY/ENTRY_AUTHORIZED -> execution

The institutional engines remain the single source of location evidence; they
are no longer a second entry strategy.  Every gate here is explicit, auditable
and fails closed.  No exchange call and no order submission lives in this file.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from location_authority import validate_location_zone
from ggp_position_sizing import entry_adx_allowed

MIN_ENTRY_ADX = 18.0
DEFAULT_EVENT_TTL_SECONDS = 3 * 900  # three closed 15-minute candles
MAX_ZONE_DISTANCE_ATR = 1.5
MAX_ZONE_AGE_BARS = 48

# Actual backend states. These are never score-derived.
STATUS_APPROACHING = "CROSS_APPROACHING"
STATUS_CONFIRMED = "CROSS_CONFIRMED"
STATUS_ANALYZING = "INSTITUTIONAL_ANALYSIS"
STATUS_READY = "READY"
STATUS_EXECUTING = "EXECUTING"
STATUS_OPEN = "POSITION_OPEN"
STATUS_REJECTED = "REJECTED"
STATUS_EXPIRED = "EXPIRED"

ICON_BY_STATUS = {
    STATUS_APPROACHING: "\U0001F7E1",
    STATUS_CONFIRMED: "\U0001F535",
    STATUS_ANALYZING: "\U0001F7E3",
    STATUS_READY: "\U0001F7E2",
    STATUS_EXECUTING: "\u26A1",
    STATUS_OPEN: "\u2705",
    STATUS_REJECTED: "\U0001F534",
    STATUS_EXPIRED: "\u26AA",
}

# Mandatory safety (A), setup-specific confirmation (B), optional evidence (C).
SMC_CONDITION_CLASSIFICATION = {
    "closed_mss_pending": "B",
    "post_liquidity_window_not_ready": "B",
    "post_sweep_window_not_ready": "B",
    "valid_sweep_or_failed_auction_not_confirmed": "B",
    "post_mss_retest_pending": "B",
    "no_fresh_nearby_order_block_or_fvg": "C",
    "bos_volume_weak": "C",
    "directional_trigger_pending": "C",
    "continuation_trigger_pending": "C",
    "adx_below_18": "A",
    "missing_stop_loss": "A",
    "invalid_zone": "A",
    "stale_event": "A",
    "duplicate_event": "A",
    "position_state_unknown": "A",
    "pending_order_conflict": "A",
    "risk_gate_failed": "A",
    # Explicit mandatory gate names used by evaluate_cross_authorization.
    "cross_confirmed_closed_candle": "A",
    "event_fresh_unconsumed": "A",
    "direction_preserved": "A",
    "adx_at_least_18": "A",
    "market_data_valid_fresh": "A",
    "institutional_zone_approved": "A",
    "stop_loss_verified": "A",
    "position_state_known": "A",
    "no_pending_or_conflicting_order": "A",
    "risk_and_exchange_constraints_ok": "A",
}


def classify_smc_condition(name: Any) -> str:
    """Classify a gate as A (mandatory), B (setup-specific) or C (optional)."""
    key = str(name or "").strip().lower()
    return SMC_CONDITION_CLASSIFICATION.get(key, "C")


def is_universal_blocker(name: Any) -> bool:
    """Only class A conditions may block every EMA9/VWAP crossover."""
    return classify_smc_condition(name) == "A"


PENDING_ZONE_REASONS = {
    "no_fresh_nearby_order_block_or_fvg",
    "zone_unknown",
}


def watchlist_status_for_decision(decision: Mapping[str, Any]) -> str:
    """Map one explicit authorization decision to a real backend Watchlist state."""
    if decision.get("authorized"):
        return STATUS_READY
    rejection = str(decision.get("rejection_reason") or "")
    if any(rejection.endswith(f":{reason}") for reason in PENDING_ZONE_REASONS):
        return STATUS_ANALYZING
    return STATUS_REJECTED


def cross_direction_for_side(side: Any) -> Optional[str]:
    value = str(side or "").strip().upper()
    if value in {"BUY", "BULLISH", "LONG"}:
        return "BULLISH"
    if value in {"SELL", "BEARISH", "SHORT"}:
        return "BEARISH"
    return None


def side_for_cross_direction(direction: Any) -> Optional[str]:
    value = str(direction or "").strip().upper()
    if value == "BULLISH":
        return "BUY"
    if value == "BEARISH":
        return "SELL"
    return None


def build_cross_event_id(symbol: Any, direction: Any, candle_timestamp: Any) -> Optional[str]:
    """Stable identity for one confirmed EMA9/Session-VWAP crossover candle."""
    key = str(symbol or "").strip().upper().replace(":USDT", "").replace("/USDT", "")
    cross_direction = cross_direction_for_side(direction)
    try:
        timestamp = int(candle_timestamp)
    except (TypeError, ValueError, OverflowError):
        timestamp = 0
    if not key or cross_direction is None or timestamp <= 0:
        return None
    return f"EMA9VWAP:{key}:{cross_direction}:{timestamp}"


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


@dataclass(frozen=True)
class CrossEvent:
    """Immutable, uniquely identified EMA9/Session-VWAP crossover event."""

    event_id: str
    symbol: str
    side: str
    cross_direction: str
    candle_timestamp: int
    detection_timestamp: float
    ema9: float
    previous_ema9: Optional[float]
    vwap: float
    previous_vwap: Optional[float]
    adx: float
    cross_status: str
    closed_confirmed: bool
    data_freshness: str
    lifecycle_status: str = STATUS_CONFIRMED

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_cross_event(
    *,
    symbol: Any,
    side: Any,
    candle_timestamp: Any,
    ema9: Any,
    vwap: Any,
    adx: Any,
    previous_ema9: Any = None,
    previous_vwap: Any = None,
    cross_status: Any = "CONFIRMED",
    data_freshness: Any = "FRESH",
    detection_timestamp: Any = None,
    now_ms: Any = None,
) -> Optional[CrossEvent]:
    """Build a validated immutable event. Returns None for non-confirmed crosses."""
    status = str(cross_status or "").strip().upper()
    if status not in {"CONFIRMED", "CROSS_CONFIRMED"}:
        return None
    direction = cross_direction_for_side(side)
    trade_side = side_for_cross_direction(direction)
    if direction is None or trade_side is None:
        return None
    try:
        candle_ts = int(candle_timestamp)
    except (TypeError, ValueError, OverflowError):
        return None
    if candle_ts <= 0:
        return None
    event_id = build_cross_event_id(symbol, direction, candle_ts)
    if not event_id:
        return None
    ema_value, vwap_value, adx_value = _as_float(ema9), _as_float(vwap), _as_float(adx)
    if ema_value is None or vwap_value is None or adx_value is None:
        return None
    now_value = int(now_ms if now_ms is not None else time.time() * 1000)
    detected = float(detection_timestamp if detection_timestamp is not None else time.time())
    closed = candle_ts < now_value
    if not closed:
        return None
    return CrossEvent(
        event_id=event_id,
        symbol=str(symbol),
        side=trade_side,
        cross_direction=direction,
        candle_timestamp=candle_ts,
        detection_timestamp=detected,
        ema9=ema_value,
        previous_ema9=_as_float(previous_ema9),
        vwap=vwap_value,
        previous_vwap=_as_float(previous_vwap),
        adx=adx_value,
        cross_status="CONFIRMED",
        closed_confirmed=True,
        data_freshness=str(data_freshness or "UNKNOWN"),
    )


def build_cross_event_from_analysis(row: Mapping[str, Any], *, now_ms: Any = None) -> Optional[CrossEvent]:
    """Convert an institutional-bridge analysis row into a CrossEvent."""
    if str(row.get("cross_status") or "").strip().upper() not in {"CONFIRMED", "CROSS_CONFIRMED"}:
        return None
    snapshot = row.get("indicator_snapshot") if isinstance(row.get("indicator_snapshot"), Mapping) else {}
    side = side_for_cross_direction(row.get("cross_direction")) or str(row.get("side") or "").upper()
    return build_cross_event(
        symbol=row.get("exchange_symbol") or row.get("symbol"),
        side=side,
        candle_timestamp=row.get("cross_candle_timestamp"),
        ema9=row.get("ema9", snapshot.get("ema9")),
        previous_ema9=snapshot.get("previous_ema9"),
        vwap=row.get("vwap", snapshot.get("vwap")),
        previous_vwap=snapshot.get("previous_vwap"),
        adx=row.get("adx"),
        cross_status=row.get("cross_status"),
        data_freshness=row.get("data_freshness", snapshot.get("quality")),
        now_ms=now_ms,
    )


CROSS_FIELDS = (
    "cross_event_id", "cross_status", "cross_direction", "cross_candle_timestamp",
    "cross_distance", "watchlist_status", "watchlist_icon", "watchlist_reason",
    "institutional_zone_decision", "zone_status", "zone_distance_atr", "stop_loss_status",
    "stop_loss_price", "entry_authorized", "authorization_reason", "decision_record",
    "decision_state", "entry_readiness", "trade_mode", "side_source",
)


def cross_status_is_confirmed(row: Optional[Mapping[str, Any]]) -> bool:
    return bool(row) and str(row.get("cross_status") or "").strip().upper() in {"CONFIRMED", "CROSS_CONFIRMED"}


def merge_cross_fields(previous: Mapping[str, Any], latest: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge a fresh institutional row without erasing confirmed cross identity.

    A later candle with no cross must never silently delete an unconsumed
    confirmed event. Newer confirmed events replace older ones because the
    caller writes them explicitly through ``ingest_cross_event``.
    """
    merged = dict(previous)
    merged.update(latest)
    if cross_status_is_confirmed(previous) and not cross_status_is_confirmed(latest):
        for key in CROSS_FIELDS:
            if key in previous:
                merged[key] = previous[key]
        merged["cross_status"] = previous["cross_status"]
        merged["cross_direction"] = previous.get("cross_direction")
        merged["cross_candle_timestamp"] = previous.get("cross_candle_timestamp")
    return merged


def watchlist_row_for_event(event: CrossEvent) -> Dict[str, Any]:
    """Create the watchlist row owned by the crossover lifecycle."""
    return {
        "symbol": event.symbol,
        "exchange_symbol": event.symbol,
        "side": event.side,
        "side_source": "EMA9_VWAP_CROSS",
        "cross_event_id": event.event_id,
        "cross_status": event.cross_status,
        "cross_direction": event.cross_direction,
        "cross_candle_timestamp": event.candle_timestamp,
        "ema9": event.ema9,
        "session_vwap": event.vwap,
        "vwap": event.vwap,
        "adx": event.adx,
        "data_freshness": event.data_freshness,
        "watchlist_status": STATUS_CONFIRMED,
        "watchlist_icon": ICON_BY_STATUS[STATUS_CONFIRMED],
        "watchlist_reason": "Confirmed EMA9/Session VWAP cross on a closed candle; zone analysis pending",
        "institutional_zone_decision": "PENDING",
        "zone_status": "PENDING",
        "stop_loss_status": "PENDING",
        "entry_authorized": False,
        "authorization_reason": "INSTITUTIONAL_ZONE_ANALYSIS_PENDING",
        "decision_state": STATUS_CONFIRMED,
        "entry_readiness": STATUS_CONFIRMED,
        "score": 0.0,
        "icons": {"cross": ICON_BY_STATUS[STATUS_CONFIRMED], "status": ICON_BY_STATUS[STATUS_CONFIRMED]},
        "last_updated": time.time(),
    }


class CrossEventStore:
    """Small SQLite store for immutable cross events and their consumption."""

    def __init__(self, path: Optional[str] = None):
        self.path = str(path or os.environ.get("CROSS_EVENT_DB_PATH", "data/ggp_cross_events.sqlite3"))
        self._lock = threading.RLock()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS cross_events (
                event_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                cross_direction TEXT NOT NULL,
                candle_timestamp INTEGER NOT NULL,
                detection_timestamp REAL NOT NULL,
                adx REAL NOT NULL,
                cross_status TEXT NOT NULL,
                lifecycle_status TEXT NOT NULL,
                consumed_trade_id TEXT,
                payload TEXT NOT NULL,
                created_ts REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_cross_events_symbol ON cross_events(symbol, candle_timestamp);
            CREATE TABLE IF NOT EXISTS cross_event_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                created_ts REAL NOT NULL
            );
            """)

    def _db(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=5000")
        return db

    def upsert(self, event: CrossEvent) -> None:
        payload = json.dumps(event.to_dict(), sort_keys=True)
        with self._lock, self._db() as db:
            db.execute(
                "INSERT INTO cross_events (event_id, symbol, side, cross_direction, candle_timestamp, "
                "detection_timestamp, adx, cross_status, lifecycle_status, payload, created_ts) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                "lifecycle_status=excluded.lifecycle_status, payload=excluded.payload",
                (event.event_id, event.symbol, event.side, event.cross_direction,
                 event.candle_timestamp, event.detection_timestamp, event.adx,
                 event.cross_status, event.lifecycle_status, payload, time.time()),
            )
            db.execute(
                "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                (event.event_id, event.lifecycle_status, "ingested", time.time()),
            )

    def get(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._db() as db:
            row = db.execute("SELECT payload FROM cross_events WHERE event_id=?", (event_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def all(self, *, symbol: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
        with self._lock, self._db() as db:
            if symbol:
                rows = db.execute(
                    "SELECT payload FROM cross_events WHERE symbol=? ORDER BY candle_timestamp DESC LIMIT ?",
                    (symbol, limit),
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT payload FROM cross_events ORDER BY candle_timestamp DESC LIMIT ?", (limit,)
                ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def mark_status(self, event_id: str, status: str, reason: str = "") -> None:
        with self._lock, self._db() as db:
            db.execute("UPDATE cross_events SET lifecycle_status=? WHERE event_id=?", (status, event_id))
            db.execute(
                "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                (event_id, status, reason, time.time()),
            )

    def is_consumed(self, event_id: str) -> bool:
        with self._lock, self._db() as db:
            row = db.execute(
                "SELECT consumed_trade_id FROM cross_events WHERE event_id=?", (event_id,)
            ).fetchone()
        return bool(row and row[0] is not None and str(row[0]) != "")

    def consume(self, event_id: str, trade_id: str) -> bool:
        marker = str(trade_id or "").strip() or f"CONSUMED:{event_id}"
        with self._lock, self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT consumed_trade_id FROM cross_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None:
                db.execute("ROLLBACK")
                return False
            if row[0] is not None and str(row[0]) != "":
                db.execute("ROLLBACK")
                # Same trade re-confirming consumption after execution is safe.
                return str(row[0]) == marker
            db.execute(
                "UPDATE cross_events SET consumed_trade_id=?, lifecycle_status=? WHERE event_id=?",
                (marker, STATUS_OPEN, event_id),
            )
            db.execute(
                "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                (event_id, STATUS_OPEN, f"consumed_by:{marker}", time.time()),
            )
            db.execute("COMMIT")
            return True

    def expire_stale(self, *, now_ms: Any = None, ttl_seconds: int = DEFAULT_EVENT_TTL_SECONDS) -> int:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        cutoff = now - int(ttl_seconds) * 1000
        with self._lock, self._db() as db:
            rows = db.execute(
                "SELECT event_id FROM cross_events WHERE consumed_trade_id IS NULL AND candle_timestamp < ? "
                "AND lifecycle_status NOT IN (?,?)",
                (cutoff, STATUS_REJECTED, STATUS_EXPIRED),
            ).fetchall()
            for (event_id,) in rows:
                db.execute("UPDATE cross_events SET lifecycle_status=? WHERE event_id=?", (STATUS_EXPIRED, event_id))
                db.execute(
                    "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                    (event_id, STATUS_EXPIRED, "event_window_expired", time.time()),
                )
        return len(rows)


_STORE: Optional[CrossEventStore] = None
_STORE_LOCK = threading.Lock()


def get_cross_event_store() -> CrossEventStore:
    global _STORE
    with _STORE_LOCK:
        if _STORE is None:
            _STORE = CrossEventStore()
        return _STORE


def ingest_cross_event(
    event: CrossEvent,
    watchlist: Dict[str, Any],
    store: Optional[CrossEventStore] = None,
) -> Dict[str, Any]:
    """Persist a confirmed cross and place it in the Watchlist immediately."""
    (store or get_cross_event_store()).upsert(event)
    key = event.symbol
    previous = watchlist.get(key) if isinstance(watchlist.get(key), Mapping) else {}
    row = watchlist_row_for_event(event)
    watchlist[key] = merge_cross_fields(previous, row)
    return watchlist[key]


def _normalize_zone(zone: Any, *, kind: str, side: str) -> Optional[Dict[str, Any]]:
    if not isinstance(zone, Mapping):
        return None
    low = _as_float(zone.get("low", zone.get("lower")))
    high = _as_float(zone.get("high", zone.get("upper")))
    if low is None or high is None:
        return None
    timestamp = zone.get("available_timestamp", zone.get("timestamp"))
    return {
        "kind": kind,
        "low": low,
        "high": high,
        "direction": "BULLISH" if str(side).upper() == "BUY" else "BEARISH",
        "status": str(zone.get("status") or "").upper(),
        "strength": zone.get("strength"),
        "zone_definition": str(zone.get("zone_definition") or "").upper(),
        "size_atr": zone.get("size_atr"),
        "available_timestamp": timestamp,
    }


def evaluate_cross_zone(row: Mapping[str, Any], *, now_ms: Any = None) -> Dict[str, Any]:
    """Evaluate only the location of the confirmed crossover.

    Reversal/MSS/sweep/retest evidence is not required here; it is classified
    as optional or setup-specific evidence and cannot block a valid cross zone.
    """
    side = str(row.get("side") or "").upper()
    price = _as_float(row.get("price"))
    atr = _as_float(row.get("atr"))
    candle_ts = int(row.get("cross_candle_timestamp") or 0)
    if price is None or atr is None or atr <= 0 or candle_ts <= 0:
        return {"decision": "REJECTED", "valid": False, "reason": "invalid_zone_market_reference", "zones": []}

    candidates: List[Dict[str, Any]] = []
    ob = _normalize_zone(row.get("order_block"), kind="ORDER_BLOCK", side=side)
    if ob:
        ob["zone_definition"] = ob["zone_definition"] or (
            "BULLISH_LOW_TO_OPEN" if side == "BUY" else "BEARISH_OPEN_TO_HIGH"
        )
        ob["strength"] = ob["strength"] if ob["strength"] is not None else 5
        candidates.append(ob)
    fvg = _normalize_zone(row.get("fvg"), kind="FVG", side=side)
    if fvg:
        fvg["status"] = fvg["status"] or "UNFILLED"
        fvg["size_atr"] = fvg["size_atr"] if fvg["size_atr"] is not None else 0.08
        candidates.append(fvg)
    sr = row.get("support_resistance") if isinstance(row.get("support_resistance"), Mapping) else {}
    sr_kind = "SUPPORT" if side == "BUY" else "RESISTANCE"
    sr_level = None
    levels = sr.get("supports") if side == "BUY" else sr.get("resistances")
    if isinstance(levels, (list, tuple)) and levels:
        first = levels[0]
        sr_level = _as_float(first) if not isinstance(first, Mapping) else _as_float(first.get("price"))
    if sr_level is not None and sr_level > 0:
        width = max(atr * 0.25, price * 0.0002)
        candidates.append({
            "kind": sr_kind, "low": sr_level - width / 2, "high": sr_level + width / 2,
            "direction": "BULLISH" if side == "BUY" else "BEARISH",
            "status": "CONFIRMED", "strength": 5, "available_timestamp": candle_ts,
        })

    timeframe_ms = 900_000
    checked = []
    for zone in candidates:
        verdict = validate_location_zone(
            zone, side, price, atr, candle_ts, timeframe_ms,
            max_age_bars=MAX_ZONE_AGE_BARS, max_distance_atr=MAX_ZONE_DISTANCE_ATR,
        )
        checked.append({**zone, **verdict})
    valid_zones = [z for z in checked if z.get("valid")]
    if not valid_zones:
        reason = checked[0]["reason"] if checked else "no_fresh_nearby_order_block_or_fvg"
        return {
            "decision": "REJECTED" if not checked else "WAITING",
            "valid": False,
            "reason": reason,
            "zones": checked,
        }
    best = min(valid_zones, key=lambda z: z.get("distance_atr", 99))
    zone_count = len(valid_zones)
    decision = "EXCELLENT" if zone_count >= 2 else "GOOD" if best["distance_atr"] <= 0.75 else "MODERATE"
    return {
        "decision": decision,
        "valid": True,
        "reason": "crossover_zone_valid",
        "zones": checked,
        "selected_zone": best,
        "zone_distance_atr": best.get("distance_atr"),
    }


def evaluate_stop_loss(row: Mapping[str, Any], zone: Mapping[str, Any]) -> Dict[str, Any]:
    """Use the existing structural stop; never invent one from an unknown zone."""
    side = str(row.get("side") or "").upper()
    price = _as_float(row.get("price"))
    stop = _as_float(row.get("structural_stop"))
    if stop is None and isinstance(zone.get("selected_zone"), Mapping):
        selected = zone["selected_zone"]
        if side == "BUY":
            stop = _as_float(selected.get("low"))
        elif side == "SELL":
            stop = _as_float(selected.get("high"))
    if price is None or stop is None or stop <= 0:
        return {"valid": False, "status": "MISSING", "price": None, "reason": "structural_stop_missing"}
    if side == "BUY" and not (0 < stop < price):
        return {"valid": False, "status": "INVALID", "price": stop, "reason": "stop_wrong_side"}
    if side == "SELL" and not (stop > price > 0):
        return {"valid": False, "status": "INVALID", "price": stop, "reason": "stop_wrong_side"}
    return {"valid": True, "status": "VERIFIED", "price": stop, "reason": "structural_stop_valid"}


classify_trade_mode = None  # assigned lazily by ggpbot to avoid import cycles


def evaluate_cross_authorization(
    *,
    event: CrossEvent,
    analysis_row: Mapping[str, Any],
    zone: Mapping[str, Any],
    stop_loss: Mapping[str, Any],
    market_state: Optional[Mapping[str, Any]] = None,
    risk_state: Optional[Mapping[str, Any]] = None,
    store: Optional[CrossEventStore] = None,
    now_ms: Any = None,
) -> Dict[str, Any]:
    """Single explicit ENTRY_AUTHORIZED decision for a confirmed crossover."""
    market = market_state or {}
    risk = risk_state or {}
    gates: Dict[str, bool] = {}
    reasons: Dict[str, str] = {}
    now = int(now_ms if now_ms is not None else time.time() * 1000)

    gates["cross_confirmed_closed_candle"] = bool(event.closed_confirmed and event.cross_status == "CONFIRMED")
    reasons["cross_confirmed_closed_candle"] = "confirmed" if gates["cross_confirmed_closed_candle"] else "cross_not_confirmed"

    fresh = now - event.candle_timestamp <= DEFAULT_EVENT_TTL_SECONDS * 1000
    gates["event_fresh_unconsumed"] = bool(fresh and not (store or get_cross_event_store()).is_consumed(event.event_id))
    reasons["event_fresh_unconsumed"] = "fresh_and_unconsumed" if gates["event_fresh_unconsumed"] else (
        "duplicate_event" if (store or get_cross_event_store()).is_consumed(event.event_id) else "stale_event"
    )

    row_side = str(analysis_row.get("side") or "").upper()
    cross_any = str(analysis_row.get("cross_direction") or "").upper()
    gates["direction_preserved"] = bool(
        row_side == event.side and (not cross_any or cross_any == event.cross_direction)
    )
    reasons["direction_preserved"] = "side_preserved" if gates["direction_preserved"] else "opposite_direction_hypothesis"

    gates["adx_at_least_18"] = bool(entry_adx_allowed(event.adx, MIN_ENTRY_ADX))
    reasons["adx_at_least_18"] = "adx_ok" if gates["adx_at_least_18"] else "adx_below_18"

    data_ok = str(analysis_row.get("data_quality") or "VALID_CLOSED_CANDLES") == "VALID_CLOSED_CANDLES"
    gates["market_data_valid_fresh"] = bool(data_ok and str(event.data_freshness).upper() in {"FRESH", "VALID_CLOSED_CANDLES"})
    reasons["market_data_valid_fresh"] = "data_fresh" if gates["market_data_valid_fresh"] else "data_unavailable_or_stale"

    gates["institutional_zone_approved"] = bool(zone.get("valid"))
    reasons["institutional_zone_approved"] = str(zone.get("reason") or "zone_unknown")

    gates["stop_loss_verified"] = bool(stop_loss.get("valid") and stop_loss.get("status") == "VERIFIED")
    reasons["stop_loss_verified"] = str(stop_loss.get("reason") or "stop_loss_unknown")

    position_state = str(market.get("position_state") or "UNKNOWN").upper()
    gates["position_state_known"] = position_state in {"FLAT", "KNOWN", "RECONCILED"}
    reasons["position_state_known"] = position_state.lower()

    gates["no_pending_or_conflicting_order"] = not bool(market.get("pending_order") or market.get("conflicting_order"))
    reasons["no_pending_or_conflicting_order"] = "clear" if gates["no_pending_or_conflicting_order"] else "pending_order_conflict"

    gates["risk_and_exchange_constraints_ok"] = bool(risk.get("allowed", True))
    reasons["risk_and_exchange_constraints_ok"] = str(risk.get("reason") or "risk_ok")

    authorized = all(gates.values())
    mode = str(analysis_row.get("trading_mode") or "UNCLASSIFIED").upper()
    mode = mode if mode in {"SCALP", "TREND"} else "SCALP"
    rejection = "" if authorized else next((f"{k}:{v}" for k, v in reasons.items() if not gates[k]), "unknown_gate_failure")
    record = {
        "symbol": event.symbol,
        "side": event.side,
        "cross_direction": event.cross_direction,
        "event_id": event.event_id,
        "candle_timestamp": event.candle_timestamp,
        "detection_timestamp": event.detection_timestamp,
        "cross_status": event.cross_status,
        "data_freshness": event.data_freshness,
        "adx": event.adx,
        "watchlist_status": watchlist_status_for_decision({"authorized": authorized, "rejection_reason": rejection}),
        "zone_status": zone.get("decision", "UNKNOWN"),
        "zone_reason": zone.get("reason", "unknown"),
        "zone_distance_atr": zone.get("zone_distance_atr"),
        "trade_mode": mode,
        "stop_loss_status": stop_loss.get("status", "MISSING"),
        "stop_loss_price": stop_loss.get("price"),
        "market_state": dict(market),
        "risk_state": dict(risk),
        "mandatory_gates": sorted(g for g, cls in (
            (name, classify_smc_condition(name)) for name in gates
        ) if cls == "A"),
        "setup_specific_or_optional_conditions": sorted(name for name in gates
            if classify_smc_condition(name) != "A"),
        "every_gate_result": {k: {"passed": v, "reason": reasons[k]} for k, v in gates.items()},
        "final_authorization": "ENTRY_AUTHORIZED" if authorized else "BLOCKED",
        "rejection_reason": rejection,
    }
    if authorized and store is not None:
        store.mark_status(event.event_id, STATUS_READY, "entry_authorized")
    return {
        **record,
        "authorized": authorized,
        "watchlist_icon": ICON_BY_STATUS[record["watchlist_status"]],
        "authorization_reason": "all_mandatory_gates_passed" if authorized else rejection,
    }


def log_decision_record(record: Mapping[str, Any]) -> None:
    """Emit one structured, machine-readable decision line."""
    print("[CROSS_ENTRY_DECISION] " + json.dumps(dict(record), sort_keys=True, default=str))
