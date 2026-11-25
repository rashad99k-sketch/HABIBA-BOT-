"""Bounded, thread-safe cache for immutable watchlist market-data snapshots.

This cache is advisory/performance-only. It never authorizes entries and callers
receive defensive copies so side-specific enrichers cannot mutate shared frames.
"""
from __future__ import annotations

import copy
import threading
import time
from collections import OrderedDict
from typing import Any, Callable


class WatchlistSnapshotCache:
    def __init__(self, max_entries: int = 256):
        self.max_entries = max(1, int(max_entries))
        self._lock = threading.RLock()
        self._items: OrderedDict[Any, tuple[float, Any]] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    @staticmethod
    def _clone(value: Any) -> Any:
        if hasattr(value, "copy") and value.__class__.__module__.startswith("pandas"):
            return value.copy(deep=True)
        return copy.deepcopy(value)

    def get_or_compute(self, key: Any, factory: Callable[[], Any],
                       ttl_seconds: float = 30.0) -> Any:
        now = time.monotonic()
        ttl = max(0.0, float(ttl_seconds))
        with self._lock:
            item = self._items.get(key)
            if item is not None and now - item[0] <= ttl:
                self._items.move_to_end(key)
                self.hits += 1
                return self._clone(item[1])
            self.misses += 1
            # Compute under the lock: single-flight for identical keys prevents
            # duplicate pandas/indicator work when BUY and SELL enrich together.
            value = factory()
            self._items[key] = (now, self._clone(value))
            self._items.move_to_end(key)
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)
                self.evictions += 1
            return self._clone(value)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self.hits = self.misses = self.evictions = 0

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"entries": len(self._items), "hits": self.hits,
                    "misses": self.misses, "evictions": self.evictions}


WATCHLIST_SNAPSHOT_CACHE = WatchlistSnapshotCache(max_entries=256)
