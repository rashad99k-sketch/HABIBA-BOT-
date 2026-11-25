"""Regression tests for bounded shared watchlist snapshot/evidence caching."""
import pandas as pd

from roro_watchlist_snapshot_cache import WatchlistSnapshotCache


def test_same_closed_candle_is_computed_once_and_returns_defensive_frames():
    cache = WatchlistSnapshotCache(max_entries=4)
    calls = []
    frame = pd.DataFrame({"close": [1.0, 2.0]})

    def build():
        calls.append(1)
        return frame

    first = cache.get_or_compute(("BTC", "15m", 1), build, ttl_seconds=60)
    first.loc[0, "close"] = 999
    second = cache.get_or_compute(("BTC", "15m", 1), build, ttl_seconds=60)

    assert len(calls) == 1
    assert second.loc[0, "close"] == 1.0
    assert cache.stats()["hits"] == 1


def test_new_candle_gets_new_snapshot():
    cache = WatchlistSnapshotCache()
    calls = []
    for candle in (100, 200):
        cache.get_or_compute(("ETH", "15m", candle),
                             lambda candle=candle: calls.append(candle) or candle)
    assert calls == [100, 200]


def test_cache_is_bounded_and_evicts_oldest_entry():
    cache = WatchlistSnapshotCache(max_entries=2)
    for key in ("a", "b", "c"):
        cache.get_or_compute(key, lambda key=key: key)
    assert cache.stats() == {"entries": 2, "hits": 0, "misses": 3, "evictions": 1}


def test_factory_failure_is_not_cached():
    cache = WatchlistSnapshotCache()
    calls = []

    def fail():
        calls.append(1)
        raise ValueError("bad snapshot")

    for _ in range(2):
        try:
            cache.get_or_compute("bad", fail)
        except ValueError:
            pass
    assert len(calls) == 2
    assert cache.stats()["entries"] == 0
