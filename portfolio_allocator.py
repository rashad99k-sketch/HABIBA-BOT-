"""Durable, race-safe six-slot portfolio allocator.

Implements requirement 3 of ``STRATEGY_SPEC_EMA15_RF_SIX_SLOTS.md`` as a
standalone module.  The allocator owns no trading logic: callers reserve a slot
before order submission and release it only after verified-flat evidence.

Design invariants
-----------------
* Exactly six slots exist: five TECHNICAL/INSTITUTIONAL and one NEWS_TRADING.
* Slot class is derived here from a supplied strategy/source hint.  Raw caller
  labels (``slot_class``, ``class_label``, ``label`` ...) are never trusted.
* Every mutation runs inside ``BEGIN IMMEDIATE`` so concurrent callers, even in
  different processes, cannot both claim the last slot.  A partial UNIQUE index
  on ``(slot_class, slot_index) WHERE status='RESERVED'`` is a second barrier.
* State is SQLite-durable.  Unknown, malformed, duplicate or over-capacity
  state fails closed: no slot is claimed and no slot is freed.
* ``release`` requires structured verified-flat evidence.  ``require_flat=False``
  can never bypass that gate.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import json
import math
import os
import re
import sqlite3
import threading
import time
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

__all__ = [
    "CLASS_TECHNICAL",
    "CLASS_NEWS",
    "SLOT_CLASSES",
    "CAPACITY",
    "TOTAL_CAPACITY",
    "SCHEMA_VERSION",
    "PortfolioAllocatorError",
    "UnknownClassHintError",
    "InvalidReservationRequest",
    "FlatEvidenceError",
    "AllocatorStateError",
    "Reservation",
    "ReservationResult",
    "ReleaseResult",
    "PortfolioAllocator",
    "classify_slot",
    "configure_default",
    "get_allocator",
    "reserve",
    "release",
    "status",
    "counts",
]

# Canonical central classification.  Aliases are accepted only through the
# registry below; a caller-supplied class label is never used as authority.
CLASS_TECHNICAL = "TECHNICAL"
CLASS_NEWS = "NEWS_TRADING"
SLOT_CLASSES: Tuple[str, ...] = (CLASS_TECHNICAL, CLASS_NEWS)
CAPACITY: Dict[str, int] = {CLASS_TECHNICAL: 5, CLASS_NEWS: 1}
TOTAL_CAPACITY = 6
SCHEMA_VERSION = 1
_APPLICATION_ID = 0x534958534C4F5401  # "SIXSLOT" marker; guards against foreign DBs

_TRUSTED_HINT_KEYS: Tuple[str, ...] = (
    "strategy", "source", "strategy_name", "source_name", "entry_source",
    "signal_source", "signal_origin", "trade_type", "setup_type", "setup",
    "engine", "origin", "class_hint",
)

# Exact normalized hints.  Deliberately explicit rather than substring based.
_TECHNICAL_HINTS = frozenset({
    "TECHNICAL", "TECHNICALS", "TECH", "TECHNICAL_INSTITUTIONAL",
    "TECHNICAL_AND_INSTITUTIONAL", "INSTITUTIONAL", "INSTITUTIONALS",
    "INSTITUTIONAL_TECHNICAL", "SYSTEMATIC", "SYSTEMATIC_STRATEGY",
    "AUTOMATED", "ALGO", "ALGORITHMIC", "SIGNAL", "SIGNAL_BASED", "STRATEGY",
    "RORO", "GGP", "NAISO", "NEON", "ADAPTIVE", "EMA", "EMA9", "EMA15",
    "EMA9_VWAP", "EMA15_VWAP", "EMA15_RF", "EMA15_RF_REENTRY", "VWAP",
    "RF", "RF_REENTRY", "RANGE_FILTER", "RANGE_FILTER_REENTRY",
    "CONFIRMED_CROSS", "CLOSED_CANDLE", "TREND", "TREND_CONTINUATION",
    "CONTINUATION", "REVERSAL", "REVERSAL_ENGINE", "LIQUIDITY_SWEEP",
    "SWEEP", "BREAKOUT", "PULLBACK", "MICRO_PULLBACK", "CONFLUENCE",
    "SNIPER", "SNIPER_V2", "RADAR", "PROXIMITY", "DISCOVERY", "SCANNER",
    "MOMENTUM", "PATTERN", "STRUCTURAL_STOP", "FAILED_AUCTION_REVERSAL",
    "T", "TECH_SLOT", "TECHNICAL_SLOT", "INSTITUTIONAL_SLOT",
    "TECHNICAL_OR_INSTITUTIONAL", "INSTITUTIONAL_OR_TECHNICAL",
})
_NEWS_HINTS = frozenset({
    "NEWS", "NEWS_TRADING", "NEWS_TRADE", "NEWS_TRADER", "NEWS_TRADERS",
    "NEWS_SIGNAL", "NEWS_ENTRY", "NEWS_SETUP", "NEWS_EVENT",
    "NEWS_RELEASE", "NEWS_CATALYST", "NEWS_DRIVEN", "HEADLINE",
    "HEADLINES", "HEADLINE_TRADING", "MACRO", "MACRO_NEWS",
    "ECONOMIC_CALENDAR", "ECONOMIC_EVENT", "EVENT_TRADING",
    "FOMC", "CPI", "NFP", "PMI", "EARNINGS", "CENTRAL_BANK",
    "N", "NEWS_SLOT", "NEWS_TRADING_SLOT", "NEWSFLOW", "NEWS_IMPACT",
})

_TECHNICAL_TOKENS = frozenset(
    token for hint in _TECHNICAL_HINTS for token in hint.split("_")
) | frozenset({"EMA9", "EMA15", "VWAP", "RF", "FILTER", "CANDLE"})
_NEWS_TOKENS = frozenset(
    token for hint in _NEWS_HINTS for token in hint.split("_")
) - frozenset({"TRADING", "TRADE", "TRADER", "TRADERS", "DRIVEN"})

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-/]{0,127}$")
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]+")
_FLAT_FLAG_KEYS = ("verified_flat", "verified", "flat", "is_flat", "position_flat")
_QTY_KEYS = ("qty", "quantity", "position_qty", "remaining_qty", "positionAmt", "size")
_STATUS_KEYS = ("status", "position_status", "exchange_state")
_SOURCE_KEYS = ("source", "evidence_source", "verified_by", "verifier")
_FLAT_STATUS_VALUES = frozenset({"FLAT", "CLOSED", "NONE", "NO_POSITION", "ZERO", "EMPTY"})


class PortfolioAllocatorError(Exception):
    """Base class for allocator failures."""


class UnknownClassHintError(PortfolioAllocatorError):
    """Raised when a strategy/source hint cannot be classified centrally."""


class InvalidReservationRequest(PortfolioAllocatorError):
    """Raised when reserve/release identifiers or parameters are malformed."""


class FlatEvidenceError(PortfolioAllocatorError):
    """Raised when verified-flat evidence is absent, ambiguous or malformed."""


class AllocatorStateError(PortfolioAllocatorError):
    """Raised when durable allocator state is unknown, corrupt or inconsistent."""


@dataclass(frozen=True)
class Reservation:
    """One durable, active-or-released slot claim."""

    trade_id: str
    symbol: str
    slot_class: str
    slot_index: int
    strategy_hint: str
    created_ts: float
    status: str = "RESERVED"
    released_ts: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "slot_class": self.slot_class,
            "slot_index": self.slot_index,
            "strategy_hint": self.strategy_hint,
            "created_ts": self.created_ts,
            "status": self.status,
            "released_ts": self.released_ts,
        }


@dataclass(frozen=True)
class ReservationResult:
    """Result of ``reserve``; truthy only when the slot was actually claimed."""

    ok: bool
    reason: str
    slot_class: Optional[str] = None
    reservation: Optional[Reservation] = None

    def __bool__(self) -> bool:
        return self.ok

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "slot_class": self.slot_class,
            "reservation": self.reservation.as_dict() if self.reservation else None,
        }


@dataclass(frozen=True)
class ReleaseResult:
    """Result of ``release``; truthy only when the slot was actually freed."""

    ok: bool
    reason: str
    trade_id: str
    slot_class: Optional[str] = None
    released: Optional[Reservation] = None

    def __bool__(self) -> bool:
        return self.ok

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "trade_id": self.trade_id,
            "slot_class": self.slot_class,
            "released": self.released.as_dict() if self.released else None,
        }


def _normalized_hint(value: Any) -> str:
    if not isinstance(value, str):
        raise UnknownClassHintError("class_hint must be a non-empty string or mapping")
    text = _NON_ALNUM_RE.sub("_", value.strip().upper()).strip("_")
    if not text or text == "UNKNOWN":
        raise UnknownClassHintError("class_hint is empty or unknown")
    return text


def _hint_candidates(class_hint: Any) -> Tuple[str, ...]:
    """Extract only trusted strategy/source fields; ignore raw labels."""
    if isinstance(class_hint, Mapping):
        candidates: List[str] = []
        for key in _TRUSTED_HINT_KEYS:
            if key not in class_hint:
                continue
            value = class_hint[key]
            if value is None or value == "":
                continue
            try:
                candidates.append(_normalized_hint(str(value)))
            except UnknownClassHintError:
                continue
        if not candidates:
            raise UnknownClassHintError(
                "class_hint mapping has no trusted strategy/source field")
        return tuple(candidates)
    return (_normalized_hint(class_hint),)


def classify_slot(class_hint: Any) -> str:
    """Derive the slot class centrally from a strategy/source hint.

    Raw caller labels are not parameters of this function by design.  Ambiguous
    or unknown hints raise ``UnknownClassHintError`` (fail closed).
    """
    candidates = _hint_candidates(class_hint)

    # Exact registry matches across ALL supplied fields; conflicting fields
    # fail closed instead of letting one label silently win.
    exact_news = any(candidate in _NEWS_HINTS for candidate in candidates)
    exact_technical = any(candidate in _TECHNICAL_HINTS for candidate in candidates)
    if exact_news and exact_technical:
        raise UnknownClassHintError(
            "ambiguous class_hint mixes news and technical indicators")
    if exact_news:
        return CLASS_NEWS
    if exact_technical:
        return CLASS_TECHNICAL

    news_tokens: set[str] = set()
    technical_tokens: set[str] = set()
    for candidate in candidates:
        tokens = set(candidate.split("_"))
        news_tokens |= tokens & _NEWS_TOKENS
        technical_tokens |= tokens & _TECHNICAL_TOKENS
    if news_tokens and technical_tokens:
        raise UnknownClassHintError(
            "ambiguous class_hint mixes news and technical indicators")
    if news_tokens:
        return CLASS_NEWS
    if technical_tokens:
        return CLASS_TECHNICAL
    raise UnknownClassHintError(f"unrecognized class_hint: {class_hint!r}")


def _validate_identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise InvalidReservationRequest(f"malformed {field}")
    return value


def _serialize_hint(class_hint: Any) -> str:
    if isinstance(class_hint, Mapping):
        trusted = {k: class_hint[k] for k in _TRUSTED_HINT_KEYS if k in class_hint}
        return json.dumps(trusted, sort_keys=True, default=str, separators=(",", ":"))
    return _normalized_hint(class_hint)


def _validate_qty(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FlatEvidenceError("position quantity evidence must be numeric")
    qty = float(value)
    if not math.isfinite(qty) or qty < 0.0:
        raise FlatEvidenceError("position quantity evidence must be finite and non-negative")
    return qty


def _validate_snapshot(snapshot: Any) -> Dict[str, float]:
    """Validate an exchange snapshot; unknown/duplicate/over-cap fails closed."""
    if isinstance(snapshot, Mapping):
        if "positions" in snapshot:
            positions = snapshot["positions"]
        elif "symbols" in snapshot:
            positions = snapshot["symbols"]
        elif "data" in snapshot:
            positions = snapshot["data"]
        else:
            positions = snapshot  # mapping of symbol -> quantity
    elif isinstance(snapshot, (list, tuple)):
        positions = snapshot
    else:
        raise FlatEvidenceError("malformed exchange snapshot")

    if isinstance(positions, Mapping):
        iterator: Any = [(sym, qty) for sym, qty in positions.items()]
    elif isinstance(positions, (list, tuple)):
        iterator = positions
    else:
        raise FlatEvidenceError("malformed exchange snapshot positions")

    try:
        count = len(iterator)
    except TypeError:
        count = -1
    if count < 0 or count > TOTAL_CAPACITY:
        raise FlatEvidenceError("over-capacity or malformed exchange snapshot")

    result: Dict[str, float] = {}
    for item in iterator:
        if isinstance(item, Mapping):
            symbol = item.get("symbol") or item.get("s")
            qty_value = next((item[k] for k in _QTY_KEYS if k in item), None)
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            symbol, qty_value = item
        else:
            raise FlatEvidenceError("malformed exchange snapshot position")
        try:
            symbol = _validate_identifier(symbol, "snapshot symbol")
        except InvalidReservationRequest as exc:
            raise FlatEvidenceError(str(exc)) from None
        key = symbol.upper()
        if key in result:
            raise FlatEvidenceError("duplicate symbol in exchange snapshot")
        result[key] = _validate_qty(qty_value)
    return result


def _collect_evidence(
    verified_flat: Any, evidence: Any, **kwargs: Any
) -> Dict[str, Any]:
    """Normalize strict verified-flat evidence.

    Accepted forms::

        verified_flat={"verified_flat": True, "qty": 0, "source": "exchange"}
        verified_flat=True, evidence={"flat": True, "qty": 0}
        verified_flat=True, qty=0, source="exchange"

    A bare ``verified_flat=True`` is NOT sufficient: explicit flat evidence is
    mandatory.  Malformed snapshots always fail closed.
    """
    if verified_flat is True:
        base = dict(evidence) if isinstance(evidence, Mapping) else {}
        for key in _FLAT_FLAG_KEYS + _QTY_KEYS + _STATUS_KEYS + _SOURCE_KEYS + ("snapshot",):
            if key in kwargs and key not in base:
                base[key] = kwargs[key]
        base["verified_flat"] = True
    elif isinstance(verified_flat, Mapping):
        base = dict(verified_flat)
        if isinstance(evidence, Mapping):
            base.update(evidence)
    else:
        raise FlatEvidenceError("verified_flat evidence is required")

    if not isinstance(base, dict) or not base:
        raise FlatEvidenceError("verified_flat evidence is empty")
    confirmed = any(base.get(key) is True for key in _FLAT_FLAG_KEYS)
    if not confirmed:
        raise FlatEvidenceError("verified_flat evidence lacks an explicit true confirmation")

    for source_key in _SOURCE_KEYS:
        if source_key in base and base[source_key] is not None:
            if not isinstance(base[source_key], str) or not base[source_key].strip():
                raise FlatEvidenceError("malformed evidence source")
            break

    zero_seen = False
    for key in _QTY_KEYS:
        if key in base and base[key] is not None:
            if _validate_qty(base[key]) != 0.0:
                raise FlatEvidenceError("evidence reports a non-zero position")
            zero_seen = True
    for key in _STATUS_KEYS:
        if key in base and base[key] is not None:
            if not isinstance(base[key], str):
                raise FlatEvidenceError("malformed evidence status")
            status_value = base[key].strip().upper()
            if status_value not in _FLAT_STATUS_VALUES:
                raise FlatEvidenceError("evidence status is not FLAT/CLOSED")
            zero_seen = True

    snapshot_qty: Optional[Dict[str, float]] = None
    if "snapshot" in base and base["snapshot"] is not None:
        snapshot_qty = _validate_snapshot(base["snapshot"])
        zero_seen = True

    if not zero_seen:
        raise FlatEvidenceError("verified_flat evidence has no quantity/status proof")

    normalized = dict(base)
    normalized["verified_flat"] = True
    normalized["evidence_validated"] = True
    if snapshot_qty is not None:
        normalized["snapshot_positions"] = snapshot_qty
    return normalized


class PortfolioAllocator:
    """Central allocator backed by a single durable SQLite database."""

    _EXPECTED_COLUMNS = frozenset({
        "trade_id", "symbol", "slot_class", "slot_index", "strategy_hint",
        "status", "created_ts", "released_ts", "release_evidence",
    })

    def __init__(self, path: Any, *, auto_init: bool = True):
        self.path = str(Path(path))
        self._lock = threading.RLock()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        if auto_init:
            self._initialize()

    # --- connection/transaction helpers -----------------------------------
    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            db = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
            db.row_factory = sqlite3.Row
            try:
                db.execute("PRAGMA busy_timeout=30000")
                db.execute("PRAGMA synchronous=FULL")
                yield db
            finally:
                db.close()

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            else:
                db.execute("COMMIT")

    @contextmanager
    def _read_tx(self) -> Iterator[sqlite3.Connection]:
        with self._db() as db:
            db.execute("BEGIN")
            try:
                yield db
            except BaseException:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            else:
                db.execute("COMMIT")

    def _initialize(self) -> None:
        with self._db() as db:
            table = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='allocator_meta'"
            ).fetchone()
            app_id = int(db.execute("PRAGMA application_id").fetchone()[0] or 0)
            if table is None:
                if app_id not in (0, _APPLICATION_ID):
                    raise AllocatorStateError("allocator path points to a foreign database")
                if app_id == 0 and db.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                ).fetchone()[0] > 0:
                    raise AllocatorStateError("allocator path points to an unknown schema")
                # executescript() implicitly commits pending transactions, so
                # create the schema statement-by-statement inside one real tx.
                db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                db.execute("PRAGMA journal_mode=WAL")
                schema_statements = (
                    "CREATE TABLE allocator_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                    "CREATE TABLE slot_reservations ("
                    "trade_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, "
                    "slot_class TEXT NOT NULL CHECK(slot_class IN ('TECHNICAL','NEWS_TRADING')), "
                    "slot_index INTEGER NOT NULL CHECK(slot_index >= 1 AND slot_index <= 5), "
                    "strategy_hint TEXT NOT NULL, "
                    "status TEXT NOT NULL CHECK(status IN ('RESERVED','RELEASED')), "
                    "created_ts REAL NOT NULL CHECK(created_ts > 0), "
                    "released_ts REAL, release_evidence TEXT, "
                    "CHECK ((slot_class='NEWS_TRADING' AND slot_index=1) "
                    "OR (slot_class='TECHNICAL' AND slot_index BETWEEN 1 AND 5)), "
                    "CHECK ((status='RESERVED' AND released_ts IS NULL) "
                    "OR (status='RELEASED' AND released_ts IS NOT NULL "
                    "AND release_evidence IS NOT NULL)))",
                    "CREATE UNIQUE INDEX ux_active_slot ON slot_reservations(slot_class, slot_index) "
                    "WHERE status='RESERVED'",
                    "CREATE INDEX ix_slot_reservations_status ON slot_reservations(status)",
                    "CREATE TABLE allocator_events ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT NOT NULL, "
                    "event_type TEXT NOT NULL, payload TEXT NOT NULL, created_ts REAL NOT NULL)",
                )
                db.execute("BEGIN IMMEDIATE")
                try:
                    for statement in schema_statements:
                        db.execute(statement)
                    db.execute(
                        "INSERT INTO allocator_meta(key,value) VALUES('schema_version',?)",
                        (str(SCHEMA_VERSION),),
                    )
                    db.execute("COMMIT")
                except BaseException:
                    try:
                        db.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise
            else:
                if app_id not in (0, _APPLICATION_ID):
                    raise AllocatorStateError("allocator database application_id mismatch")
                # Validate before touching pragmas: corrupt state must fail
                # closed even at construction/restart time.
                self._validate_state(db)
                if app_id == 0:
                    db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                if db.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                    db.execute("PRAGMA journal_mode=WAL")

    # --- state validation (fail closed) -----------------------------------
    def _validate_state(self, db: sqlite3.Connection) -> None:
        meta_rows = db.execute("SELECT key,value FROM allocator_meta").fetchall()
        meta = {str(row[0]): str(row[1]) for row in meta_rows}
        if meta.get("schema_version") != str(SCHEMA_VERSION) or len(meta) != 1:
            raise AllocatorStateError("unknown allocator schema version")

        columns = {str(row[1]) for row in db.execute("PRAGMA table_info(slot_reservations)")}
        if columns != self._EXPECTED_COLUMNS:
            raise AllocatorStateError("allocator reservation schema is malformed")
        events = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='allocator_events'"
        ).fetchone()
        if events is None:
            raise AllocatorStateError("allocator event log is missing")

        rows = db.execute(
            "SELECT trade_id,symbol,slot_class,slot_index,strategy_hint,status,"
            "created_ts,released_ts,release_evidence FROM slot_reservations"
        ).fetchall()
        active_slots: set[Tuple[str, int]] = set()
        active_count = 0
        class_counts = {cls: 0 for cls in SLOT_CLASSES}
        for row in rows:
            trade_id, symbol, slot_class, slot_index, hint, status, created, released, evidence = row
            if not isinstance(trade_id, str) or not _IDENTIFIER_RE.fullmatch(trade_id):
                raise AllocatorStateError("malformed reservation trade_id")
            if not isinstance(symbol, str) or not _IDENTIFIER_RE.fullmatch(symbol):
                raise AllocatorStateError("malformed reservation symbol")
            if slot_class not in CAPACITY:
                raise AllocatorStateError("unknown reservation slot class")
            if not isinstance(slot_index, int) or isinstance(slot_index, bool):
                raise AllocatorStateError("malformed reservation slot index")
            if slot_index < 1 or slot_index > CAPACITY[slot_class]:
                raise AllocatorStateError("reservation slot index out of range")
            if not isinstance(hint, str) or not hint.strip():
                raise AllocatorStateError("malformed reservation strategy hint")
            if status not in ("RESERVED", "RELEASED"):
                raise AllocatorStateError("unknown reservation status")
            if not isinstance(created, (int, float)) or isinstance(created, bool) or created <= 0:
                raise AllocatorStateError("malformed reservation timestamp")
            if status == "RESERVED":
                if released is not None or evidence is not None:
                    raise AllocatorStateError("active reservation has release fields")
                key = (str(slot_class), int(slot_index))
                if key in active_slots:
                    raise AllocatorStateError("duplicate active slot")
                active_slots.add(key)
                active_count += 1
                class_counts[str(slot_class)] += 1
            else:
                if not isinstance(released, (int, float)) or isinstance(released, bool) or released <= 0:
                    raise AllocatorStateError("released reservation has no valid timestamp")
                if not isinstance(evidence, str) or not evidence.strip():
                    raise AllocatorStateError("released reservation has no evidence")
                try:
                    parsed = json.loads(evidence)
                except (TypeError, ValueError):
                    raise AllocatorStateError("malformed release evidence") from None
                if not isinstance(parsed, dict) or parsed.get("evidence_validated") is not True:
                    raise AllocatorStateError("release evidence was never validated")
        if active_count > TOTAL_CAPACITY:
            raise AllocatorStateError("active reservations exceed total capacity")
        for slot_class, cap in CAPACITY.items():
            if class_counts[slot_class] > cap:
                raise AllocatorStateError(f"active {slot_class} reservations exceed capacity")

    # --- core API ---------------------------------------------------------
    def reserve(
        self, symbol: Any, trade_id: Any, class_hint: Any = None,
        *, metadata: Any = None, **kwargs: Any
    ) -> ReservationResult:
        """Claim one centrally classified slot before order submission.

        ``class_hint`` may be a string strategy/source token or a mapping with
        trusted keys such as ``strategy``/``source``.  If omitted, trusted
        strategy/source keys supplied as keyword arguments are used.  Any raw
        class label (``slot_class``, ``class_label``, ``label``, ...) is ignored
        for classification and can never override the central registry.
        """
        try:
            symbol_value = _validate_identifier(symbol, "symbol")
            trade_value = _validate_identifier(trade_id, "trade_id")
            if class_hint is None:
                fallback = {k: kwargs[k] for k in _TRUSTED_HINT_KEYS if k in kwargs}
                if not fallback:
                    raise UnknownClassHintError("class_hint is required")
                class_hint = fallback
            slot_class = classify_slot(class_hint)
            hint_text = _serialize_hint(class_hint)
        except (InvalidReservationRequest, UnknownClassHintError) as exc:
            return ReservationResult(False, str(exc))

        now = time.time()
        audit = {"symbol": symbol_value, "strategy_hint": hint_text, "slot_class": slot_class}
        if metadata is not None:
            audit["metadata"] = metadata

        try:
            with self._write_tx() as db:
                self._validate_state(db)
                existing = db.execute(
                    "SELECT status FROM slot_reservations WHERE trade_id=?", (trade_value,)
                ).fetchone()
                if existing is not None:
                    return ReservationResult(False, "duplicate_trade_id", slot_class)

                used = {
                    int(row[0]) for row in db.execute(
                        "SELECT slot_index FROM slot_reservations "
                        "WHERE slot_class=? AND status='RESERVED'", (slot_class,)
                    )
                }
                cap = CAPACITY[slot_class]
                total = int(db.execute(
                    "SELECT COUNT(*) FROM slot_reservations WHERE status='RESERVED'"
                ).fetchone()[0])
                # A full portfolio is reported as total capacity even when the
                # class cap is also full; both gates remain enforced.
                if total >= TOTAL_CAPACITY:
                    return ReservationResult(False, "total_capacity_exceeded", slot_class)
                if len(used) >= cap:
                    return ReservationResult(False, "class_capacity_exceeded", slot_class)

                slot_index = next(i for i in range(1, cap + 1) if i not in used)
                evidence_json = json.dumps(audit, sort_keys=True, default=str, separators=(",", ":"))
                try:
                    db.execute(
                        "INSERT INTO slot_reservations(trade_id,symbol,slot_class,slot_index,"
                        "strategy_hint,status,created_ts,released_ts,release_evidence) "
                        "VALUES(?,?,?,?,?,'RESERVED',?,NULL,NULL)",
                        (trade_value, symbol_value, slot_class, slot_index, hint_text, now),
                    )
                except sqlite3.IntegrityError:
                    # The unique slot index / trade id barrier won the race.
                    return ReservationResult(False, "slot_claim_conflict", slot_class)
                db.execute(
                    "INSERT INTO allocator_events(trade_id,event_type,payload,created_ts) "
                    "VALUES(?,?,?,?)", (trade_value, "SLOT_RESERVED", evidence_json, now),
                )
                reservation = Reservation(
                    trade_id=trade_value, symbol=symbol_value, slot_class=slot_class,
                    slot_index=slot_index, strategy_hint=hint_text, created_ts=now,
                )
        except AllocatorStateError:
            return ReservationResult(False, "state_invalid", slot_class)
        except sqlite3.Error as exc:
            return ReservationResult(False, f"allocator_error:{type(exc).__name__}", slot_class)
        return ReservationResult(True, "reserved", slot_class, reservation)

    def release(
        self, trade_id: Any, require_flat: bool = True,
        verified_flat: Any = None, *, evidence: Any = None, **kwargs: Any
    ) -> ReleaseResult:
        """Free a reserved slot only with verified-flat evidence.

        ``require_flat=False`` is rejected: the flat gate cannot be bypassed.
        Evidence may be a mapping passed as ``verified_flat`` or as ``evidence``
        (with ``verified_flat=True``), including a validated exchange snapshot.
        """
        try:
            trade_value = _validate_identifier(trade_id, "trade_id")
        except InvalidReservationRequest as exc:
            return ReleaseResult(False, str(exc), str(trade_id))

        if require_flat is not True:
            return ReleaseResult(False, "require_flat_must_be_true", trade_value)

        try:
            normalized = _collect_evidence(verified_flat, evidence, **kwargs)
        except FlatEvidenceError as exc:
            return ReleaseResult(False, f"flat_evidence_invalid:{exc}", trade_value)

        now = time.time()
        try:
            with self._write_tx() as db:
                self._validate_state(db)
                row = db.execute(
                    "SELECT trade_id,symbol,slot_class,slot_index,strategy_hint,status,"
                    "created_ts,released_ts,release_evidence FROM slot_reservations "
                    "WHERE trade_id=?", (trade_value,)
                ).fetchone()
                if row is None:
                    return ReleaseResult(False, "unknown_trade_id", trade_value)
                if row["status"] != "RESERVED":
                    return ReleaseResult(False, "already_released", trade_value)

                snapshot_positions = normalized.get("snapshot_positions")
                if snapshot_positions is not None:
                    target = row["symbol"].upper()
                    if target not in snapshot_positions or snapshot_positions[target] != 0.0:
                        return ReleaseResult(False, "snapshot_does_not_confirm_flat", trade_value)

                evidence_json = json.dumps(normalized, sort_keys=True, default=str, separators=(",", ":"))
                cur = db.execute(
                    "UPDATE slot_reservations SET status='RELEASED', released_ts=?, "
                    "release_evidence=? WHERE trade_id=? AND status='RESERVED'",
                    (now, evidence_json, trade_value),
                )
                if cur.rowcount != 1:
                    return ReleaseResult(False, "release_conflict", trade_value)
                db.execute(
                    "INSERT INTO allocator_events(trade_id,event_type,payload,created_ts) "
                    "VALUES(?,?,?,?)", (trade_value, "SLOT_RELEASED", evidence_json, now),
                )
                released = Reservation(
                    trade_id=row["trade_id"], symbol=row["symbol"],
                    slot_class=row["slot_class"], slot_index=int(row["slot_index"]),
                    strategy_hint=row["strategy_hint"], created_ts=float(row["created_ts"]),
                    status="RELEASED", released_ts=now,
                )
        except AllocatorStateError:
            return ReleaseResult(False, "state_invalid", trade_value)
        except sqlite3.Error as exc:
            return ReleaseResult(False, f"allocator_error:{type(exc).__name__}", trade_value)
        return ReleaseResult(True, "released", trade_value, released.slot_class, released)

    # --- read/status API --------------------------------------------------
    def active_reservations(self) -> List[Reservation]:
        with self._read_tx() as db:
            self._validate_state(db)
            rows = db.execute(
                "SELECT trade_id,symbol,slot_class,slot_index,strategy_hint,created_ts "
                "FROM slot_reservations WHERE status='RESERVED' ORDER BY slot_class,slot_index"
            ).fetchall()
        return [
            Reservation(
                trade_id=row["trade_id"], symbol=row["symbol"],
                slot_class=row["slot_class"], slot_index=int(row["slot_index"]),
                strategy_hint=row["strategy_hint"], created_ts=float(row["created_ts"]),
            )
            for row in rows
        ]

    def status(self) -> Dict[str, Any]:
        active = self.active_reservations()
        counts = {slot_class: 0 for slot_class in SLOT_CLASSES}
        for item in active:
            counts[item.slot_class] += 1
        counts["TOTAL"] = len(active)
        return {
            "ok": True,
            "schema_version": SCHEMA_VERSION,
            "capacity": dict(CAPACITY),
            "total_capacity": TOTAL_CAPACITY,
            "counts": counts,
            "available": {
                CLASS_TECHNICAL: CAPACITY[CLASS_TECHNICAL] - counts[CLASS_TECHNICAL],
                CLASS_NEWS: CAPACITY[CLASS_NEWS] - counts[CLASS_NEWS],
                "TOTAL": TOTAL_CAPACITY - counts["TOTAL"],
            },
            "active": [item.as_dict() for item in active],
        }

    def counts(self) -> Dict[str, int]:
        return dict(self.status()["counts"])

    def active_count(self) -> int:
        return int(self.counts()["TOTAL"])

    def count(self, slot_class: Any = None) -> int:
        if slot_class is None:
            return self.active_count()
        return int(self.counts()[classify_slot(slot_class)])

    def reservation(self, trade_id: Any) -> Optional[Reservation]:
        trade_value = _validate_identifier(trade_id, "trade_id")
        with self._read_tx() as db:
            self._validate_state(db)
            row = db.execute(
                "SELECT trade_id,symbol,slot_class,slot_index,strategy_hint,status,"
                "created_ts,released_ts FROM slot_reservations WHERE trade_id=?",
                (trade_value,),
            ).fetchone()
        if row is None:
            return None
        return Reservation(
            trade_id=row["trade_id"], symbol=row["symbol"], slot_class=row["slot_class"],
            slot_index=int(row["slot_index"]), strategy_hint=row["strategy_hint"],
            created_ts=float(row["created_ts"]), status=row["status"],
            released_ts=row["released_ts"],
        )

    def is_reserved(self, trade_id: Any) -> bool:
        item = self.reservation(trade_id)
        return item is not None and item.status == "RESERVED"

    def integrity_check(self) -> Dict[str, Any]:
        """Read-only SQLite integrity check plus allocator state validation."""
        try:
            with self._read_tx() as db:
                self._validate_state(db)
                verdict = str(db.execute("PRAGMA integrity_check").fetchone()[0])
        except (AllocatorStateError, sqlite3.Error) as exc:
            return {"ok": False, "result": f"{type(exc).__name__}: {exc}"[:300]}
        return {"ok": verdict.lower() == "ok", "result": verdict[:300]}


# --- module-level convenience API -----------------------------------------
_DEFAULT_LOCK = threading.Lock()
_DEFAULT: Optional[PortfolioAllocator] = None


def configure_default(path: Any) -> PortfolioAllocator:
    """Bind the process-wide allocator instance to a durable SQLite path."""
    global _DEFAULT
    with _DEFAULT_LOCK:
        _DEFAULT = PortfolioAllocator(path)
        return _DEFAULT


def get_allocator(path: Any = None) -> PortfolioAllocator:
    """Lazily return the process-wide allocator (env override supported)."""
    global _DEFAULT
    with _DEFAULT_LOCK:
        if path is not None:
            if _DEFAULT is None or _DEFAULT.path != str(Path(path)):
                _DEFAULT = PortfolioAllocator(path)
            return _DEFAULT
        if _DEFAULT is None:
            default_path = os.environ.get(
                "PORTFOLIO_ALLOCATOR_DB", str(Path.cwd() / "data" / "portfolio_allocator.sqlite3")
            )
            _DEFAULT = PortfolioAllocator(default_path)
        return _DEFAULT


def reserve(symbol: Any, trade_id: Any, class_hint: Any = None, **kwargs: Any) -> ReservationResult:
    return get_allocator().reserve(symbol, trade_id, class_hint, **kwargs)


def release(trade_id: Any, require_flat: bool = True, verified_flat: Any = None,
            **kwargs: Any) -> ReleaseResult:
    return get_allocator().release(trade_id, require_flat, verified_flat, **kwargs)


def status() -> Dict[str, Any]:
    return get_allocator().status()


def counts() -> Dict[str, int]:
    return get_allocator().counts()
