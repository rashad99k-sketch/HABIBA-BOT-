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
CROSS_TIMEFRAME_SECONDS = 900  # ggp_institutional_bridge.TIMEFRAME_SECONDS

# Actual backend states. These are never score-derived.
STATUS_APPROACHING = "CROSS_APPROACHING"
STATUS_CONFIRMED = "CROSS_CONFIRMED"
STATUS_ANALYZING = "INSTITUTIONAL_ANALYSIS"
STATUS_READY = "READY"
STATUS_AUTHORIZED = "ENTRY_AUTHORIZED"
STATUS_EXECUTING = "EXECUTING"
STATUS_ORDER_SUBMITTED = "ORDER_SUBMITTED"
STATUS_FILL_VERIFIED = "FILL_VERIFIED"
STATUS_POSITION_VERIFIED = "POSITION_VERIFIED"
STATUS_EVENT_CONSUMED = "EVENT_CONSUMED"
STATUS_OPEN = "POSITION_OPEN"
STATUS_REJECTED = "REJECTED"
STATUS_EXPIRED = "EXPIRED"
STATUS_PERSISTENCE_EXCEPTION = "PERSISTENCE_RECOVERY_EXCEPTION"

ICON_BY_STATUS = {
    STATUS_APPROACHING: "\U0001F7E1",
    STATUS_CONFIRMED: "\U0001F535",
    STATUS_ANALYZING: "\U0001F7E3",
    STATUS_READY: "\U0001F7E2",
    STATUS_AUTHORIZED: "\U0001F7E2",
    STATUS_EXECUTING: "\u26A1",
    STATUS_ORDER_SUBMITTED: "\u26A1",
    STATUS_FILL_VERIFIED: "\u26A1",
    STATUS_POSITION_VERIFIED: "\u26A1",
    STATUS_EVENT_CONSUMED: "\u2705",
    STATUS_OPEN: "\u2705",
    STATUS_REJECTED: "\U0001F534",
    STATUS_EXPIRED: "\u26AA",
    STATUS_PERSISTENCE_EXCEPTION: "\U0001F534",
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
    "cross_math_proof": "A",
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

# Exact outcomes of an event-consumption attempt. A `False` from consume() is
# deliberately ambiguous; recovery must distinguish idempotency from failure.
CONSUMPTION_CONSUMED = "EVENT_CONSUMED"
CONSUMPTION_IDEMPOTENT = "ALREADY_CONSUMED_SAME_TRADE"
CONSUMPTION_CONFLICT = "CONSUMED_BY_DIFFERENT_TRADE"
CONSUMPTION_MISSING_EVENT = "EVENT_MISSING"
CONSUMPTION_MISSING_TRADE_ID = "MISSING_VERIFIED_TRADE_ID"
CONSUMPTION_PERSISTENCE_ERROR = "PERSISTENCE_FAILURE"


@dataclass(frozen=True)
class ConsumptionResult:
    status: str
    trade_id: str = ""
    ok: bool = False
    idempotent: bool = False


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


def cross_math_proof(event: Mapping[str, Any]) -> bool:
    """Re-validate the crossover inequality from the upstream closed-candle snapshot.

    This does not recompute EMA9 or Session VWAP. The bridge already computes
    both from validated closed candles; this gate proves that the values carried
    into the immutable event really form the claimed mathematical crossover.
    """
    current_ema = _as_float(event.get("ema9"))
    current_vwap = _as_float(event.get("vwap"))
    previous_ema = _as_float(event.get("previous_ema9"))
    previous_vwap = _as_float(event.get("previous_vwap"))
    if current_ema is None or current_vwap is None or previous_ema is None or previous_vwap is None:
        return False
    direction = str(event.get("cross_direction") or "").strip().upper()
    if direction == "BULLISH":
        return previous_ema <= previous_vwap and current_ema > current_vwap
    if direction == "BEARISH":
        return previous_ema >= previous_vwap and current_ema < current_vwap
    return False


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
    proof_timestamp: Any = None,
    timeframe_seconds: Any = None,
) -> Optional[CrossEvent]:
    """Build a validated immutable event. Returns None for non-confirmed crosses.

    The supplied EMA9/Session-VWAP values must prove the claimed transition from
    the previous closed candle to the current closed candle. No indicator is
    recalculated here; the bridge remains the single calculation authority.
    """
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
    tf_seconds = _as_float(timeframe_seconds)
    tf_seconds = int(tf_seconds) if tf_seconds is not None else CROSS_TIMEFRAME_SECONDS
    if tf_seconds != CROSS_TIMEFRAME_SECONDS:
        return None
    closed = candle_ts + tf_seconds * 1000 <= now_value
    if not closed:
        return None
    proof = {
        "ema9": ema_value, "vwap": vwap_value,
        "previous_ema9": _as_float(previous_ema9), "previous_vwap": _as_float(previous_vwap),
        "cross_direction": direction,
    }
    if not cross_math_proof(proof):
        return None
    if proof_timestamp is not None:
        proof_ts = _as_float(proof_timestamp)
        if proof_ts is None or int(proof_ts) != candle_ts:
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
    """Convert an institutional-bridge analysis row into a CrossEvent.

    The bridge derives `cross_status` only from its EMA9/Session-VWAP crossover
    proof. Rows carrying another trigger (for example EMA50/VWAP proximity) are
    rejected here before they can become executable cross events.
    """
    if str(row.get("cross_status") or "").strip().upper() not in {"CONFIRMED", "CROSS_CONFIRMED"}:
        return None
    trigger = str(row.get("cross_trigger") or row.get("cross_source") or "EMA9_VWAP").strip().upper()
    if trigger not in {"EMA9_VWAP", "EMA9VWAP", "EMA9_VWAP_CROSS"}:
        return None
    snapshot = row.get("indicator_snapshot") if isinstance(row.get("indicator_snapshot"), Mapping) else {}
    side = side_for_cross_direction(row.get("cross_direction")) or str(row.get("side") or "").upper()
    proof_timestamp = snapshot.get("latest_closed_timestamp", snapshot.get("cross_candle_timestamp"))
    timeframe_seconds = snapshot.get("timeframe_seconds", CROSS_TIMEFRAME_SECONDS)
    try:
        tf_value = int(float(timeframe_seconds)) if timeframe_seconds is not None else CROSS_TIMEFRAME_SECONDS
    except (TypeError, ValueError, OverflowError):
        return None
    if tf_value != CROSS_TIMEFRAME_SECONDS:
        return None
    return build_cross_event(
        symbol=row.get("exchange_symbol") or row.get("symbol"),
        side=side,
        candle_timestamp=row.get("cross_candle_timestamp", proof_timestamp),
        ema9=row.get("ema9", snapshot.get("ema9")),
        previous_ema9=row.get("previous_ema9", snapshot.get("previous_ema9")),
        vwap=row.get("vwap", snapshot.get("vwap")),
        previous_vwap=row.get("previous_vwap", snapshot.get("previous_vwap")),
        adx=row.get("adx"),
        cross_status=row.get("cross_status"),
        data_freshness=row.get("data_freshness", snapshot.get("quality")),
        now_ms=now_ms,
        proof_timestamp=proof_timestamp,
        timeframe_seconds=tf_value,
    )


CROSS_FIELDS = (
    "cross_event_id", "cross_status", "cross_direction", "cross_candle_timestamp",
    "cross_trigger", "cross_source", "cross_distance", "indicator_snapshot",
    "previous_ema9", "previous_vwap",
    "ema9", "vwap", "adx", "data_freshness",
    "watchlist_status", "watchlist_icon", "watchlist_reason",
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
        "cross_trigger": "EMA9_VWAP",
        "ema9": event.ema9,
        "previous_ema9": event.previous_ema9,
        "session_vwap": event.vwap,
        "vwap": event.vwap,
        "previous_vwap": event.previous_vwap,
        "indicator_snapshot": {
            "ema9": event.ema9, "vwap": event.vwap,
            "previous_ema9": event.previous_ema9, "previous_vwap": event.previous_vwap,
            "latest_closed_timestamp": event.candle_timestamp,
            "cross_candle_timestamp": event.candle_timestamp,
            "timeframe_seconds": CROSS_TIMEFRAME_SECONDS,
            "cross_status": event.cross_status, "cross_direction": event.cross_direction,
            "quality": event.data_freshness,
        },
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
                execution_trade_id TEXT,
                execution_state TEXT,
                execution_status TEXT,
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
            columns = {row[1] for row in db.execute("PRAGMA table_info(cross_events)")}
            for name, ddl in (
                ("execution_trade_id", "TEXT"), ("execution_state", "TEXT"),
                ("execution_status", "TEXT"),
            ):
                if name not in columns:
                    db.execute(f"ALTER TABLE cross_events ADD COLUMN {name} {ddl}")

    def _db(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=5000")
        return db

    def upsert(self, event: CrossEvent) -> None:
        payload = json.dumps(event.to_dict(), sort_keys=True)
        with self._lock, self._db() as db:
            # A refresh must never roll an authorized/executing/consumed event
            # back to CONFIRMED. Identity is stable, so only the original,
            # pre-execution event payload may be replaced.
            db.execute(
                "INSERT INTO cross_events (event_id, symbol, side, cross_direction, candle_timestamp, "
                "detection_timestamp, adx, cross_status, lifecycle_status, payload, created_ts) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                "symbol=excluded.symbol, side=excluded.side, "
                "cross_direction=excluded.cross_direction, "
                "candle_timestamp=excluded.candle_timestamp, "
                "lifecycle_status=CASE WHEN cross_events.consumed_trade_id IS NULL "
                "AND cross_events.execution_status IS NULL THEN excluded.lifecycle_status "
                "ELSE cross_events.lifecycle_status END, "
                "payload=CASE WHEN cross_events.consumed_trade_id IS NULL "
                "AND cross_events.execution_status IS NULL THEN excluded.payload "
                "ELSE cross_events.payload END",
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

    @staticmethod
    def _payload_with_status(payload: Any, status: str) -> Optional[str]:
        if not payload:
            return None
        try:
            parsed = json.loads(payload)
        except (TypeError, ValueError):
            return None
        if not isinstance(parsed, dict):
            return None
        parsed["lifecycle_status"] = status
        return json.dumps(parsed, sort_keys=True)

    def _set_lifecycle(self, db, event_id: str, status: str) -> None:
        """Update both the indexed status and immutable-event payload."""
        row = db.execute("SELECT payload FROM cross_events WHERE event_id=?", (event_id,)).fetchone()
        payload = self._payload_with_status(row[0] if row else None, status)
        if payload is None:
            db.execute("UPDATE cross_events SET lifecycle_status=? WHERE event_id=?", (status, event_id))
        else:
            db.execute(
                "UPDATE cross_events SET lifecycle_status=?, payload=? WHERE event_id=?",
                (status, payload, event_id),
            )

    def mark_status(self, event_id: str, status: str, reason: str = "") -> None:
        """Persist one lifecycle transition, including inside the event payload."""
        with self._lock, self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._set_lifecycle(db, event_id, status)
            db.execute(
                "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                (event_id, status, reason, time.time()),
            )
            db.execute("COMMIT")

    def mark_execution(self, event_id: str, status: str, *, trade_id: Any = None,
                       execution_state: Any = None, reason: str = "") -> bool:
        """Record a durable execution stage before attempting order submission."""
        marker = str(trade_id or "").strip()
        state = str(execution_state or "").strip().upper() or None
        with self._lock, self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            cur = db.execute(
                "UPDATE cross_events SET execution_status=?, execution_state=COALESCE(?, execution_state), "
                "execution_trade_id=COALESCE(?, execution_trade_id) WHERE event_id=?",
                (status, state, marker or None, event_id),
            )
            if not cur.rowcount:
                db.execute("ROLLBACK")
                return False
            self._set_lifecycle(db, event_id, status)
            db.execute(
                "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                (event_id, status, reason or state or "", time.time()),
            )
            db.execute("COMMIT")
            return True

    def history(self, event_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._db() as db:
            rows = db.execute(
                "SELECT status, reason, created_ts FROM cross_event_history "
                "WHERE event_id=? ORDER BY id", (event_id,)
            ).fetchall()
        return [{"status": r[0], "reason": r[1] or "", "created_ts": r[2]} for r in rows]

    def get_state(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._db() as db:
            row = db.execute(
                "SELECT lifecycle_status, consumed_trade_id, execution_trade_id, execution_state, "
                "execution_status FROM cross_events WHERE event_id=?", (event_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "lifecycle_status": row[0], "consumed_trade_id": row[1],
            "execution_trade_id": row[2], "execution_state": row[3],
            "execution_status": row[4],
        }

    def execution_recovery_candidates(self) -> List[Dict[str, Any]]:
        """Events with an attempted execution but no confirmed consumption."""
        with self._lock, self._db() as db:
            rows = db.execute(
                "SELECT event_id FROM cross_events WHERE consumed_trade_id IS NULL "
                "AND (execution_trade_id IS NOT NULL OR execution_status IS NOT NULL) "
                "ORDER BY candle_timestamp"
            ).fetchall()
        result = []
        for (event_id,) in rows:
            state = self.get_state(event_id) or {}
            result.append({"event_id": event_id, **state})
        return result

    def is_consumed(self, event_id: str) -> bool:
        with self._lock, self._db() as db:
            row = db.execute(
                "SELECT consumed_trade_id FROM cross_events WHERE event_id=?", (event_id,)
            ).fetchone()
        return bool(row and row[0] is not None and str(row[0]) != "")

    def consume_exact(self, event_id: str, trade_id: str) -> ConsumptionResult:
        """Consume one event only with the exact verified trade id.

        Unlike the boolean wrapper, the result distinguishes an idempotent
        repeat from a real persistence failure so the caller can fail closed
        without treating an already-executed position as a new order.
        """
        marker = str(trade_id or "").strip()
        if not marker:
            return ConsumptionResult(CONSUMPTION_MISSING_TRADE_ID, "", False)
        with self._lock, self._db() as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT consumed_trade_id FROM cross_events WHERE event_id=?", (event_id,)
                ).fetchone()
                if row is None:
                    db.execute("ROLLBACK")
                    return ConsumptionResult(CONSUMPTION_MISSING_EVENT, marker, False)
                previous = str(row[0] or "").strip()
                if previous:
                    db.execute("ROLLBACK")
                    if previous == marker:
                        return ConsumptionResult(CONSUMPTION_IDEMPOTENT, marker, True, True)
                    return ConsumptionResult(CONSUMPTION_CONFLICT, marker, False)
                db.execute(
                    "UPDATE cross_events SET consumed_trade_id=?, "
                    "execution_trade_id=?, execution_status=? WHERE event_id=?",
                    (marker, marker, STATUS_EVENT_CONSUMED, event_id),
                )
                self._set_lifecycle(db, event_id, STATUS_EVENT_CONSUMED)
                db.execute(
                    "INSERT INTO cross_event_history (event_id, status, reason, created_ts) VALUES (?,?,?,?)",
                    (event_id, STATUS_EVENT_CONSUMED, f"consumed_by:{marker}", time.time()),
                )
                db.execute("COMMIT")
                return ConsumptionResult(CONSUMPTION_CONSUMED, marker, True)
            except sqlite3.Error:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                return ConsumptionResult(CONSUMPTION_PERSISTENCE_ERROR, marker, False)

    def consume(self, event_id: str, trade_id: str) -> bool:
        """Backward-compatible boolean view of `consume_exact`."""
        return self.consume_exact(event_id, trade_id).ok

    def expire_stale(self, *, now_ms: Any = None, ttl_seconds: int = DEFAULT_EVENT_TTL_SECONDS) -> int:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        cutoff = now - int(ttl_seconds) * 1000
        with self._lock, self._db() as db:
            rows = db.execute(
                "SELECT event_id FROM cross_events WHERE consumed_trade_id IS NULL "
                "AND execution_status IS NULL AND candle_timestamp < ? "
                "AND lifecycle_status NOT IN (?,?,?,?,?,?)",
                (cutoff, STATUS_REJECTED, STATUS_EXPIRED, STATUS_AUTHORIZED,
                 STATUS_EXECUTING, STATUS_ORDER_SUBMITTED, STATUS_PERSISTENCE_EXCEPTION),
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


_ZONE_DIRECTION_SOURCE = "SOURCE"
_ZONE_DIRECTION_DERIVED = "DERIVED_STRUCTURAL"
_ZONE_DIRECTION_UNKNOWN = "UNKNOWN"
_ZONE_DIRECTION_CONTRADICTORY = "CONTRADICTORY"

_OB_DEFINITION_DIRECTION = {
    "BULLISH_LOW_TO_OPEN": "BULLISH",
    "BEARISH_OPEN_TO_HIGH": "BEARISH",
}
_SR_KIND_DIRECTION = {"SUPPORT": "BULLISH", "RESISTANCE": "BEARISH"}
_REJECTED_ORDER_BLOCK_STATUSES = {"INVALIDATED", "FULLY_MITIGATED"}


def _resolve_zone_direction(zone: Mapping[str, Any], *, kind: str) -> tuple:
    """Resolve a producer's real direction without ever inventing one from `side`.

    Returns ``(status, direction, structural_derivation)``. An explicit source
    direction wins; a canonical OB definition or SR type may be used only when
    the source data itself supports the derivation. FVG boundaries alone are not
    directional evidence. Any contradiction fails closed.
    """
    source = str(zone.get("direction") or "").strip().upper()
    source = source if source in {"BULLISH", "BEARISH"} else ""
    definition = str(zone.get("zone_definition") or "").strip().upper()
    source_type = str(zone.get("type") or "").strip().upper()
    source_kind = str(zone.get("kind") or "").strip().upper()
    derived = ""

    if kind == "ORDER_BLOCK":
        derived = _OB_DEFINITION_DIRECTION.get(definition, "")
        if source_kind and source_kind != "ORDER_BLOCK":
            return _ZONE_DIRECTION_CONTRADICTORY, source or derived, derived
    elif kind == "FVG":
        if source_kind and source_kind != "FVG":
            return _ZONE_DIRECTION_CONTRADICTORY, source, derived
    elif kind in _SR_KIND_DIRECTION:
        if source_type and source_type != kind:
            return _ZONE_DIRECTION_CONTRADICTORY, source or _SR_KIND_DIRECTION.get(source_type, ""), derived
        if source_kind and source_kind != kind:
            return _ZONE_DIRECTION_CONTRADICTORY, source or _SR_KIND_DIRECTION.get(source_kind, ""), derived
        derived = _SR_KIND_DIRECTION.get(source_type or source_kind, "")

    if source and derived and source != derived:
        return _ZONE_DIRECTION_CONTRADICTORY, source, derived
    if source:
        return _ZONE_DIRECTION_SOURCE, source, derived
    if derived:
        return _ZONE_DIRECTION_DERIVED, derived, derived
    return _ZONE_DIRECTION_UNKNOWN, "", ""


def _zone_availability_timestamp(zone: Mapping[str, Any], *, kind: str) -> Optional[int]:
    """Use the first timestamp at which the producer could know the zone."""
    keys = {
        "ORDER_BLOCK": ("available_timestamp", "impulse_timestamp", "timestamp", "confirmed_at_timestamp"),
        "FVG": ("available_timestamp", "timestamp", "confirmed_at_timestamp"),
        "SUPPORT": ("available_timestamp", "confirmed_at_timestamp", "timestamp"),
        "RESISTANCE": ("available_timestamp", "confirmed_at_timestamp", "timestamp"),
    }.get(kind, ("available_timestamp", "confirmed_at_timestamp", "impulse_timestamp", "timestamp"))
    for key in keys:
        value = _as_float(zone.get(key))
        if value is not None and math.isfinite(value) and value > 0:
            return int(value)
    return None


def _normalize_zone(zone: Any, *, kind: str, side: str) -> Optional[Dict[str, Any]]:
    """Copy producer zone data and add canonical, non-destructive aliases."""
    if not isinstance(zone, Mapping):
        return None
    normalized = dict(zone)
    source_direction = zone.get("source_direction", zone.get("direction"))
    normalized["source_direction"] = (
        str(source_direction).strip().upper() if source_direction is not None else None
    )
    low = _as_float(zone.get("low", zone.get("lower")))
    high = _as_float(zone.get("high", zone.get("upper")))
    if low is not None:
        normalized["low"] = low
    if high is not None:
        normalized["high"] = high
    status = str(zone.get("status") or "").strip().upper()
    if status:
        normalized["status"] = status
    strength = _as_float(zone.get("strength"))
    if strength is not None:
        normalized["strength"] = strength
    size_atr = _as_float(zone.get("size_atr"))
    if size_atr is not None:
        normalized["size_atr"] = size_atr
    available_timestamp = _zone_availability_timestamp(zone, kind=kind)
    if available_timestamp is not None:
        normalized["available_timestamp"] = available_timestamp

    direction_status, direction, structural = _resolve_zone_direction(normalized, kind=kind)
    normalized["kind"] = kind
    normalized["direction_status"] = direction_status
    normalized["derived_direction"] = structural or None
    normalized["direction"] = direction or _ZONE_DIRECTION_UNKNOWN
    normalized["evaluation_side"] = str(side or "").strip().upper()
    normalized["source_kind"] = str(zone.get("kind") or zone.get("type") or "").strip().upper() or None
    normalized["source_provenance"] = zone.get("source") or zone.get("source_definition") or None
    return normalized


def _zone_direction_precheck(zone: Mapping[str, Any], *, side: str) -> tuple:
    """Fail closed before the central authority if producer direction is unusable."""
    expected = {"BUY": "BULLISH", "SELL": "BEARISH"}.get(str(side or "").strip().upper())
    if not expected:
        return False, "unsupported_zone_or_side"
    if str(zone.get("direction_status") or "") == _ZONE_DIRECTION_CONTRADICTORY:
        return False, "zone_direction_contradictory"
    direction = str(zone.get("direction") or "").strip().upper()
    if direction not in {"BULLISH", "BEARISH"}:
        return False, "zone_direction_unknown"
    if direction != expected:
        return False, "zone_direction_mismatch"
    if zone.get("valid") is False:
        return False, "zone_producer_invalid"
    kind = str(zone.get("kind") or "").strip().upper()
    status = str(zone.get("status") or "").strip().upper()
    if kind == "ORDER_BLOCK" and status in _REJECTED_ORDER_BLOCK_STATUSES:
        return False, "order_block_mitigated_or_invalidated"
    return True, "zone_direction_confirmed"


def evaluate_cross_zone(row: Mapping[str, Any], *, now_ms: Any = None) -> Dict[str, Any]:
    """Evaluate only the location of the confirmed crossover.

    Direction comes from the confirmed crossover, not from the latest
    institutional hypothesis. Producer zone direction/provenance is preserved;
    contradictory, unknown or mitigated zones are excluded. Reversal/MSS/sweep/
    retest evidence is optional and cannot block a valid cross zone.
    """
    side = side_for_cross_direction(row.get("cross_direction")) or str(row.get("side") or "").strip().upper()
    price = _as_float(row.get("price"))
    atr = _as_float(row.get("atr"))
    candle_ts = int(row.get("cross_candle_timestamp") or 0)
    if price is None or atr is None or atr <= 0 or candle_ts <= 0:
        return {"decision": "REJECTED", "valid": False, "reason": "invalid_zone_market_reference", "zones": []}
    if side not in {"BUY", "SELL"}:
        return {"decision": "REJECTED", "valid": False, "reason": "unsupported_zone_or_side", "zones": []}

    candidates: List[Dict[str, Any]] = []
    ob = _normalize_zone(row.get("order_block"), kind="ORDER_BLOCK", side=side)
    if ob:
        candidates.append(ob)
    fvg = _normalize_zone(row.get("fvg"), kind="FVG", side=side)
    if fvg:
        candidates.append(fvg)
    sr = row.get("support_resistance") if isinstance(row.get("support_resistance"), Mapping) else {}
    sr_kind = "SUPPORT" if side == "BUY" else "RESISTANCE"
    levels = sr.get("supports" if side == "BUY" else "resistances")
    if isinstance(levels, (list, tuple)):
        for raw in levels:
            zone = _normalize_zone(raw, kind=sr_kind, side=side)
            if zone:
                candidates.append(zone)

    timeframe_ms = 900_000
    checked = []
    for zone in candidates:
        direction_ok, direction_reason = _zone_direction_precheck(zone, side=side)
        if not direction_ok:
            checked.append({**zone, "valid": False, "reason": direction_reason})
            continue
        verdict = validate_location_zone(
            zone, side, price, atr, candle_ts, timeframe_ms,
            max_age_bars=MAX_ZONE_AGE_BARS, max_distance_atr=MAX_ZONE_DISTANCE_ATR,
        )
        checked.append({**zone, **verdict})
    valid_zones = [z for z in checked if z.get("valid")]
    if not valid_zones:
        reason = checked[0]["reason"] if checked else "no_fresh_nearby_order_block_or_fvg"
        return {
            "decision": "REJECTED" if checked else "WAITING",
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
        "zone_direction": best.get("direction"),
        "zone_direction_status": best.get("direction_status"),
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

    gates["cross_math_proof"] = cross_math_proof(event.to_dict())
    reasons["cross_math_proof"] = "ema9_vwap_inequality_verified" if gates["cross_math_proof"] else "cross_math_proof_missing"

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

    data_ok = str(analysis_row.get("data_quality") or "").strip().upper() == "VALID_CLOSED_CANDLES"
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
