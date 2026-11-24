import ast
import time
from pathlib import Path

import numpy as np
import pandas as pd

from roro_confluence import ema9_vwap_proximity


def _project_root():
    """Locate the repository root whether tests live flat or under tests/."""
    here = Path(__file__).resolve()
    for candidate in (here.parent, here.parent.parent):
        if (candidate / "ggpbot.py").is_file():
            return candidate
    return here.parent


ROOT = _project_root()
SOURCE = (ROOT / "ggpbot.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)

def _function(name):
    return next(node for node in TREE.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)

def _sample_session_frame(count=60):
    interval_ms = 900_000
    start_ms = (1_800_000_000_000 // 86_400_000) * 86_400_000  # UTC session midnight
    ts = start_ms + np.arange(count, dtype=np.int64) * interval_ms
    close = 100.0 + np.arange(count) * 0.12 + np.sin(np.arange(count) / 4.0) * 0.04
    open_ = np.r_[close[0] - 0.05, close[:-1]]
    high = np.maximum(open_, close) + 0.15
    low = np.minimum(open_, close) - 0.15
    volume = 1000.0 + np.arange(count) * 3.0
    frame = pd.DataFrame({"timestamp": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume})
    return frame, int(start_ms + count * interval_ms)

def test_roro_proximity_scanner_uses_only_closed_candles_and_is_ranking_only():
    frame, now_ms = _sample_session_frame(60)
    result = ema9_vwap_proximity(
        frame, lambda series, period: series.ewm(span=period, adjust=False).mean(),
        ema_len=9, timeframe_seconds=900, now_ms=now_ms
    )
    assert result is not None
    assert result["state"] in {"CROSS_CONFIRMED", "AT_VWAP", "NEAR_CROSS", "CONVERGING", "ALIGNED_NOT_CROSSING"}
    assert result["candle_timestamp"] == int(frame.timestamp.iloc[-1])
    assert "entry_ready" not in result and "allow_entry" not in result

def test_roro_proximity_scanner_ignores_the_forming_last_candle():
    frame, _ = _sample_session_frame(61)
    now_ms = int(frame.timestamp.iloc[-1])  # last candle has not closed yet
    result = ema9_vwap_proximity(
        frame, lambda series, period: series.ewm(span=period, adjust=False).mean(),
        ema_len=9, timeframe_seconds=900, now_ms=now_ms
    )
    assert result is not None
    assert result["candle_timestamp"] == int(frame.timestamp.iloc[-2])

def test_global_discovery_runs_both_existing_scanners_on_a_shared_universe():
    fn = _function("run_global_discovery_scanner")
    calls = {node.func.id for node in ast.walk(fn) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert "run_scanner_v2" in calls
    assert "scan_market_rf" in calls
    assert "get_usdt_perp_symbols" in calls
    assert "radar_score" in calls

def test_second_scanner_is_integrated_into_unified_watchlist_enrichment():
    fn = _function("_enrich_with_existing_gpp_engines")
    names = {node.id for node in ast.walk(fn) if isinstance(node, ast.Name)}
    assert "_roro_ema9_vwap_proximity" in names
    assert any(isinstance(node, ast.Constant) and node.value == "ema9_vwap_proximity_scanner" for node in ast.walk(fn))

def test_dashboard_server_supports_two_local_ports_and_host_port():
    fn = _function("start_dashboard_servers")
    source = ast.get_source_segment(SOURCE, fn)
    assert "DASHBOARD_LOCAL_PORTS" in source
    assert "8000,8080" in source
    assert 'os.getenv("PORT")' in source
    assert "0.0.0.0" in source
    assert "make_server" in source

def test_dashboard_server_is_started_before_trade_supervisor():
    module_main = next(
        node for node in TREE.body
        if isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name) and node.test.left.id == "__name__"
    )
    calls = [node.func.id for node in ast.walk(module_main) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)]
    assert calls.index("start_dashboard_servers") < calls.index("safe_main_loop")

def test_api_request_start_rate_limit_uses_a_thread_lock():
    fn = _function("rate_limit")
    names = {node.id for node in ast.walk(fn) if isinstance(node, ast.Name)}
    assert "_API_RATE_LOCK" in names
    assert any(isinstance(node, ast.Attribute) and node.attr == "monotonic" for node in ast.walk(fn))


def test_rf_scanner_yields_to_open_position():
    fn = _function("scan_market_rf")
    names = {node.id for node in ast.walk(fn) if isinstance(node, ast.Name)}
    assert "STATE" in names and "TRADE_STATE" in names
    assert any(isinstance(node, ast.Compare) for node in ast.walk(fn))


def test_scanner_config_has_bounded_environment_parsing():
    for name in ("GLOBAL_SCAN_INTERVAL", "SCANNER_UNIVERSE_MAX",
                 "INSTITUTIONAL_WATCHLIST_BATCH_SIZE",
                 "INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS",
                 "EXCHANGE_HTTP_TIMEOUT_MS"):
        assert f'_env_int_range("{name}"' in SOURCE


def test_dashboard_decision_endpoint_exists_and_reads_management_log():
    routes = []
    for node in TREE.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "decision_feed":
            routes.append(node)
    assert len(routes) == 1
    source = ast.get_source_segment(SOURCE, routes[0])
    assert '@app.route("/decision")' in SOURCE
    assert "TRADE_MANAGEMENT_LOG.snapshot" in source

def test_dashboard_renders_unified_tp_management_feed_not_legacy_entry_shape():
    assert "UNIFIED TRADE MANAGEMENT LOG" in SOURCE
    assert "fetch('/decision'" in SOURCE
    assert "TP1_VERIFIED" in SOURCE and "TP2_VERIFIED" in SOURCE

def test_legacy_profit_decision_functions_are_non_executing_shims():
    apply_node = _function("apply_profit_engine")
    council_node = _function("council_exit")
    apply_calls = [n for n in ast.walk(apply_node) if isinstance(n, ast.Call)]
    council_call_names = [n.func.id for n in ast.walk(council_node)
                          if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
    assert not apply_calls
    assert "safe_close_position" not in council_call_names
    assert "finalize_trade_with_reality" not in council_call_names
    assert "USE_PPE = False" in SOURCE


def test_safe_json_uses_mapping_snapshot_during_concurrent_resize():
    import ast
    import threading

    fn_node = next(
        node for node in TREE.body
        if isinstance(node, ast.FunctionDef) and node.name == "safe_json"
    )
    isolated = ast.Module(body=[fn_node], type_ignores=[])
    namespace = {"np": np, "pd": pd}
    exec(compile(isolated, str(ROOT / "ggpbot.py"), "exec"), namespace)
    safe_json = namespace["safe_json"]

    shared = {str(i): {"value": i} for i in range(100)}
    failures = []
    stop = threading.Event()

    def mutate():
        index = 100
        while not stop.is_set():
            key = str(index % 250)
            shared[key] = {"value": index}
            if index % 3 == 0:
                shared.pop(str((index - 50) % 250), None)
            index += 1

    worker = threading.Thread(target=mutate, daemon=True)
    worker.start()
    try:
        for _ in range(1500):
            try:
                result = safe_json(shared)
                assert isinstance(result, dict)
            except Exception as exc:  # the route must survive concurrent writers
                failures.append(exc)
                break
    finally:
        stop.set()
        worker.join(timeout=2)
    assert not failures


def test_effective_watchlist_cleanup_uses_copy_snapshot():
    fn = next(
        node for node in reversed(TREE.body)
        if isinstance(node, ast.FunctionDef) and node.name == "cleanup_watchlist"
    )
    source = ast.get_source_segment(SOURCE, fn)
    assert "wl.copy().items()" in source



def test_scanner_runtime_is_exclusive_while_position_is_open():
    mode = _function("_position_management_mode")
    mode_names = {node.id for node in ast.walk(mode) if isinstance(node, ast.Name)}
    assert {"STATE", "TRADE_STATE"}.issubset(mode_names)

    dispatch = _function("dispatch_scanner_task")
    dispatch_source = ast.get_source_segment(SOURCE, dispatch)
    assert "_position_management_mode()" in dispatch_source
    assert "PAUSED_FOR_POSITION" in dispatch_source

    refresh = _function("_refresh_ema_vwap_setups")
    refresh_source = ast.get_source_segment(SOURCE, refresh)
    assert "_position_management_mode()" in refresh_source

    updater = _function("live_institutional_updater")
    updater_source = ast.get_source_segment(SOURCE, updater)
    assert "_position_management_mode()" in updater_source

    loop = _function("main_loop_sniper")
    loop_source = ast.get_source_segment(SOURCE, loop)
    assert "POSITION_MANAGEMENT_ONLY" in loop_source
    assert "manage_live_trade" in loop_source


def test_scanner_snapshot_cache_uses_canonical_key_and_last_valid_fallback():
    key_fn = _function("_scanner_frame_key")
    key_source = ast.get_source_segment(SOURCE, key_fn)
    assert "roro_canonical_symbol" in key_source
    fetch_fn = _function("_get_scanner_frame")
    fetch_source = ast.get_source_segment(SOURCE, fetch_fn)
    assert "_SCANNER_FRAME_LAST_VALID" in fetch_source
    peek_fn = _function("_peek_scanner_frame")
    peek_source = ast.get_source_segment(SOURCE, peek_fn)
    assert "include_stale" in peek_source
    assert "_SCANNER_FRAME_LAST_VALID" in peek_source



def test_shared_scanner_cache_handles_49_symbols_without_watchlist_refetch():
    import ast
    import threading

    functions = [_function(name) for name in ("_scanner_frame_key", "_get_scanner_frame", "_peek_scanner_frame")]
    isolated = ast.Module(body=functions, type_ignores=[])
    calls = []
    now = [100.0]

    class Clock:
        @staticmethod
        def monotonic():
            return now[0]

    def canonical(symbol):
        return str(symbol).upper().replace(":USDT", "").replace("/", "-")

    def fetch(symbol, limit):
        calls.append(symbol)
        return _sample_session_frame(60)[0]

    namespace = {
        "time": Clock, "threading": threading,
        "_SCANNER_FRAME_CACHE": {}, "_SCANNER_FRAME_LAST_VALID": {},
        "_SCANNER_FRAME_CACHE_LOCK": threading.RLock(),
        "_SCANNER_FRAME_CACHE_TTL": 90.0, "_SCANNER_FRAME_LAST_VALID_MAX": 100,
        "SCANNER_UNIVERSE_MAX": 80, "roro_canonical_symbol": canonical,
        "roro_exchange_symbol": lambda value: value,
        "get_ohlcv_safe": fetch, "validate_dataframe": lambda frame, minimum: len(frame) >= minimum,
    }
    exec(compile(isolated, "ggpbot.py", "exec"), namespace)
    for index in range(49):
        symbol = f"COIN{index:02d}/USDT:USDT"
        assert namespace["_get_scanner_frame"](symbol, 60) is not None
    assert len(calls) == 49
    # Watchlist and strategy read the exact same canonical snapshot without a 50th request.
    assert namespace["_peek_scanner_frame"]("COIN00-USDT", include_stale=True) is not None
    assert len(calls) == 49
    assert len(namespace["_SCANNER_FRAME_LAST_VALID"]) == 49


def test_concurrent_scanner_workers_coalesce_same_symbol_ohlcv_fetch():
    import ast
    import threading

    functions = [_function(name) for name in ("_scanner_frame_key", "_get_scanner_frame")]
    isolated = ast.Module(body=functions, type_ignores=[])
    calls = []
    calls_lock = threading.Lock()
    barrier = threading.Barrier(10)

    def fetch(symbol, limit):
        with calls_lock:
            calls.append((symbol, limit))
        time.sleep(0.05)
        return _sample_session_frame(60)[0]

    namespace = {
        "time": time,
        "threading": threading,
        "_SCANNER_FRAME_CACHE": {},
        "_SCANNER_FRAME_LAST_VALID": {},
        "_SCANNER_FRAME_FETCH_LOCKS": {},
        "_SCANNER_FRAME_CACHE_LOCK": threading.RLock(),
        "_SCANNER_FRAME_CACHE_TTL": 90.0,
        "_SCANNER_FRAME_LAST_VALID_MAX": 100,
        "SCANNER_UNIVERSE_MAX": 80,
        "roro_canonical_symbol": lambda value: str(value).upper().replace("/", "-").split(":")[0],
        "roro_exchange_symbol": lambda value: value,
        "get_ohlcv_safe": fetch,
        "validate_dataframe": lambda frame, minimum: len(frame) >= minimum,
    }
    exec(compile(isolated, "ggpbot.py", "exec"), namespace)

    results = []
    errors = []

    def worker():
        try:
            barrier.wait(timeout=3)
            results.append(namespace["_get_scanner_frame"]("BTC/USDT:USDT", 60))
        except Exception as exc:
            errors.append(exc)

    workers = [threading.Thread(target=worker) for _ in range(10)]
    for worker_thread in workers:
        worker_thread.start()
    for worker_thread in workers:
        worker_thread.join(timeout=4)

    assert not any(worker_thread.is_alive() for worker_thread in workers)
    assert not errors
    assert len(results) == 10
    assert all(result is not None and len(result) == 60 for result in results)
    assert calls == [("BTC/USDT:USDT", 60)]

