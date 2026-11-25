"""Deterministic tests for the durable six-slot portfolio allocator."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from portfolio_allocator import (
    AllocatorStateError,
    CLASS_NEWS,
    CLASS_TECHNICAL,
    PortfolioAllocator,
    TOTAL_CAPACITY,
    UnknownClassHintError,
    classify_slot,
)

TECHNICAL_HINTS = ("RORO", "EMA15", "INSTITUTIONAL", "TECHNICAL", "RF_REENTRY")
NEWS_HINTS = ("NEWS_TRADING", "NEWS", "HEADLINE", "MACRO_NEWS")


def _reserve(allocator, index, hint="TECHNICAL"):
    return allocator.reserve(f"SYM{index}", f"trade-{index}", hint)


def _flat(qty=0):
    return {"verified_flat": True, "qty": qty, "source": "exchange-reconciliation"}


def _fill_technical(allocator, count=5):
    for index in range(count):
        result = _reserve(allocator, index, TECHNICAL_HINTS[index])
        assert result.ok, result.reason
        assert result.slot_class == CLASS_TECHNICAL


def test_five_technical_plus_one_news_accepted(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    _fill_technical(allocator)

    # Raw caller labels are ignored; the central hint registry decides.
    news = allocator.reserve(
        "NEWSCOIN", "trade-news", class_hint="NEWS_TRADING",
        slot_class=CLASS_TECHNICAL, class_label=CLASS_TECHNICAL, label=CLASS_TECHNICAL,
    )
    assert news.ok, news.reason
    assert news.slot_class == CLASS_NEWS
    assert news.reservation.slot_index == 1

    assert allocator.counts() == {
        CLASS_TECHNICAL: 5, CLASS_NEWS: 1, "TOTAL": 6,
    }
    assert allocator.active_count() == TOTAL_CAPACITY
    active = allocator.active_reservations()
    technical_slots = sorted(i.slot_index for i in active if i.slot_class == CLASS_TECHNICAL)
    news_slots = [i.slot_index for i in active if i.slot_class == CLASS_NEWS]
    assert technical_slots == [1, 2, 3, 4, 5]
    assert news_slots == [1]


def test_sixth_total_or_sixth_technical_rejected(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    _fill_technical(allocator)

    sixth_technical = _reserve(allocator, 5, "CONFLUENCE")
    assert not sixth_technical.ok
    assert sixth_technical.reason == "class_capacity_exceeded"
    assert allocator.counts()[CLASS_TECHNICAL] == 5

    assert allocator.reserve("NEWS1", "news-1", "NEWS_TRADING").ok
    seventh = _reserve(allocator, 6, "TECHNICAL")
    assert not seventh.ok
    assert seventh.reason == "total_capacity_exceeded"
    assert allocator.active_count() == TOTAL_CAPACITY


def test_second_news_rejected_and_freed_news_slot_reusable(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    first = allocator.reserve("NEWS1", "news-1", "NEWS")
    assert first.ok and first.slot_class == CLASS_NEWS

    second = allocator.reserve(
        "NEWS2", "news-2", {"strategy": "HEADLINE", "slot_class": CLASS_TECHNICAL}
    )
    assert not second.ok
    assert second.reason == "class_capacity_exceeded"
    assert second.slot_class == CLASS_NEWS
    assert allocator.counts()[CLASS_NEWS] == 1

    assert allocator.release("news-1", verified_flat=_flat()).ok
    reusable = allocator.reserve("NEWS2", "news-2", "MACRO_NEWS")
    assert reusable.ok and reusable.slot_class == CLASS_NEWS
    assert allocator.counts()[CLASS_NEWS] == 1


def test_concurrent_reservation_race_cannot_exceed_caps(tmp_path):
    path = tmp_path / "race.sqlite3"
    PortfolioAllocator(path)  # initialize before racing

    gate = threading.Barrier(12)

    def claim_technical(index):
        # Separate instances/connections model simultaneous callers/processes.
        allocator = PortfolioAllocator(path)
        gate.wait()
        return allocator.reserve(f"TECH{index}", f"tech-{index}", "TECHNICAL")

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(claim_technical, range(12)))

    winners = [result for result in results if result.ok]
    assert len(winners) == 5
    assert sorted(result.reservation.slot_index for result in winners) == [1, 2, 3, 4, 5]
    assert all(result.slot_class == CLASS_TECHNICAL for result in winners)
    assert PortfolioAllocator(path).counts()[CLASS_TECHNICAL] == 5

    news_gate = threading.Barrier(8)

    def claim_news(index):
        allocator = PortfolioAllocator(path)
        news_gate.wait()
        return allocator.reserve(f"NEWS{index}", f"news-{index}", "NEWS_TRADING")

    with ThreadPoolExecutor(max_workers=8) as pool:
        news_results = list(pool.map(claim_news, range(8)))

    assert sum(bool(result) for result in news_results) == 1
    counts = PortfolioAllocator(path).counts()
    assert counts == {CLASS_TECHNICAL: 5, CLASS_NEWS: 1, "TOTAL": 6}


def test_release_only_after_verified_flat(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    assert allocator.reserve("BTCUSDT", "t1", "TECHNICAL").ok

    missing = allocator.release("t1")
    assert not missing.ok and missing.reason.startswith("flat_evidence_invalid")
    assert allocator.is_reserved("t1")

    bypass = allocator.release("t1", require_flat=False, verified_flat=_flat())
    assert not bypass.ok and bypass.reason == "require_flat_must_be_true"

    bare = allocator.release("t1", verified_flat=True)
    assert not bare.ok and bare.reason.startswith("flat_evidence_invalid")

    non_flat = allocator.release("t1", verified_flat={"verified": True, "qty": 1})
    assert not non_flat.ok and non_flat.reason.startswith("flat_evidence_invalid")
    assert allocator.counts()[CLASS_TECHNICAL] == 1

    released = allocator.release("t1", verified_flat={"verified": True, "qty": 0, "source": "exchange"})
    assert released.ok and released.released.slot_class == CLASS_TECHNICAL
    assert allocator.active_count() == 0
    assert not allocator.release("t1", verified_flat=_flat()).ok

    # Explicit keyword/positional evidence forms remain strict.
    assert allocator.reserve("ETHUSDT", "t2", "TECHNICAL").ok
    assert allocator.release("t2", True, True, qty=0).ok
    assert allocator.active_count() == 0


def test_release_rejects_unknown_duplicate_and_overcapacity_snapshots(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    assert allocator.reserve("BTCUSDT", "t1", "TECHNICAL").ok

    over_capacity = {"verified_flat": True, "qty": 0, "snapshot": [
        {"symbol": "BTCUSDT", "qty": 0},
        *[{"symbol": f"OTHER{i}", "qty": 1} for i in range(6)],
    ]}
    duplicate = {"verified_flat": True, "snapshot": [
        {"symbol": "BTCUSDT", "qty": 0}, {"symbol": "BTCUSDT", "qty": 0},
    ]}
    malformed = {"verified_flat": True, "snapshot": [
        {"symbol": "BTCUSDT", "qty": 0}, {"qty": 1},
    ]}
    unknown_symbol = {"verified_flat": True, "snapshot": [
        {"symbol": "BTCUSDT", "qty": 0}, {"symbol": "!!!", "qty": 1},
    ]}
    malformed_qty = {"verified_flat": True, "snapshot": {"BTCUSDT": 0, "ETHUSDT": "0"}}
    missing_target = {"verified_flat": True, "snapshot": [{"symbol": "ETHUSDT", "qty": 0}]}

    for evidence in (over_capacity, duplicate, malformed, unknown_symbol, malformed_qty, missing_target):
        result = allocator.release("t1", verified_flat=evidence)
        assert not result.ok, evidence
        assert allocator.is_reserved("t1")

    assert allocator.counts()[CLASS_TECHNICAL] == 1
    valid_snapshot = {"verified_flat": True, "snapshot": [
        {"symbol": "BTCUSDT", "qty": 0}, {"symbol": "ETHUSDT", "qty": 1},
    ]}
    assert allocator.release("t1", verified_flat=valid_snapshot).ok
    assert allocator.active_count() == 0


def test_restart_persistence(tmp_path):
    path = tmp_path / "persist.sqlite3"
    allocator = PortfolioAllocator(path)
    _fill_technical(allocator)
    assert allocator.reserve("NEWSCOIN", "news-1", "NEWS").ok

    restarted = PortfolioAllocator(path)
    assert restarted.counts() == {CLASS_TECHNICAL: 5, CLASS_NEWS: 1, "TOTAL": 6}
    assert restarted.is_reserved("trade-0") and restarted.is_reserved("news-1")
    assert restarted.reserve("EXTRA", "extra-1", "TECHNICAL").reason == "total_capacity_exceeded"

    assert restarted.release("trade-0", verified_flat=_flat()).ok
    again = PortfolioAllocator(path)
    assert again.counts()[CLASS_TECHNICAL] == 4
    assert not again.is_reserved("trade-0")
    assert again.reserve("EXTRA", "extra-1", "TECHNICAL").ok
    assert again.active_count() == TOTAL_CAPACITY


def test_unknown_class_hint_fails_closed(tmp_path):
    allocator = PortfolioAllocator(tmp_path / "alloc.sqlite3")
    bad_hints = ("MYSTERY_SOURCE", "", None, 123, {"slot_class": CLASS_NEWS})
    for index, hint in enumerate(bad_hints):
        result = allocator.reserve(f"SYM{index}", f"bad-{index}", hint)
        assert not result.ok

    labelled = allocator.reserve(
        "SYM", "bad-label", "MYSTERY_SOURCE",
        slot_class=CLASS_NEWS, class_label=CLASS_TECHNICAL, label=CLASS_NEWS,
    )
    assert not labelled.ok

    with pytest.raises(UnknownClassHintError):
        classify_slot("MYSTERY_SOURCE")
    with pytest.raises(UnknownClassHintError):
        classify_slot({"strategy": "NEWS", "setup": "TECHNICAL"})
    assert allocator.active_count() == 0


def test_malformed_unknown_state_fails_closed(tmp_path):
    path = tmp_path / "corrupt.sqlite3"
    allocator = PortfolioAllocator(path)
    assert allocator.reserve("BTCUSDT", "t1", "TECHNICAL").ok

    with sqlite3.connect(path) as db:
        db.execute("PRAGMA ignore_check_constraints=ON")
        db.execute(
            "INSERT INTO slot_reservations(trade_id,symbol,slot_class,slot_index,"
            "strategy_hint,status,created_ts,released_ts,release_evidence) "
            "VALUES('alien','ALIEN','ALIEN',1,'TECHNICAL','RESERVED',1.0,NULL,NULL)"
        )
        db.commit()

    with pytest.raises(AllocatorStateError):
        allocator.status()
    with pytest.raises(AllocatorStateError):
        PortfolioAllocator(path)

    reservation = allocator.reserve("ETHUSDT", "t2", "TECHNICAL")
    assert not reservation.ok and reservation.reason == "state_invalid"
    released = allocator.release("t1", verified_flat=_flat())
    assert not released.ok and released.reason == "state_invalid"
    assert allocator.integrity_check()["ok"] is False


def test_unknown_schema_and_schema_version_fail_closed(tmp_path):
    foreign = tmp_path / "foreign.sqlite3"
    with sqlite3.connect(foreign) as db:
        db.execute("CREATE TABLE unrelated(x INTEGER)")
    with pytest.raises(AllocatorStateError):
        PortfolioAllocator(foreign)

    versioned = tmp_path / "version.sqlite3"
    PortfolioAllocator(versioned)
    with sqlite3.connect(versioned) as db:
        db.execute("UPDATE allocator_meta SET value='99' WHERE key='schema_version'")
        db.commit()
    with pytest.raises(AllocatorStateError):
        PortfolioAllocator(versioned)
