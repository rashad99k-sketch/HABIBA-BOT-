#!/usr/bin/env python3
# ====================================================================
# RF LIQUIDITY ENGINE v28.1 - PROFESSIONAL TRADE MANAGEMENT
# [PRODUCTION PATCH] Institutional-Grade Trade Management Refactor
# Leverage: 20x | Risk-Capped Dynamic Sizing | Unified Trade Management
# ====================================================================

import os
import sys
import tempfile
import time
import json
import signal
import threading
import traceback
import math
import gc
import random
import uuid
import copy
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional, Any
from enum import Enum
from collections import deque
import queue
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, field
from html import escape as html_escape

import ccxt
import pandas as pd
import numpy as np
from roro_watchlist_snapshot_cache import WATCHLIST_SNAPSHOT_CACHE
from roro_watchlist_dashboard import build_watchlist_dashboard_payload
from flask import Flask, jsonify, request
import requests

# Load project-local configuration safely. PAPER mode remains the default.
ENV_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE_PATH = os.path.join(ENV_DIR, ".env")
ENV_TEMPLATE_PATH = os.path.join(ENV_DIR, ".env.example")
ENV_FILE_LOADED = False
ENV_SOURCE = "process environment"
try:
    from dotenv import load_dotenv
    # Prefer the user's real .env. If absent, use the template as defaults only;
    # platform-injected values/secrets must never be overwritten by the template.
    _hosted_runtime = any(os.getenv(name) for name in ("RENDER", "RAILWAY_ENVIRONMENT", "DYNO"))
    _override_raw = os.getenv("GGP_LOCAL_ENV_OVERRIDE")
    _dotenv_override = (
        str(_override_raw).strip().lower() in {"1", "true", "yes", "on"}
        if _override_raw is not None else not _hosted_runtime
    )
    if os.path.isfile(ENV_FILE_PATH):
        ENV_FILE_LOADED = bool(load_dotenv(dotenv_path=ENV_FILE_PATH, override=_dotenv_override))
        ENV_SOURCE = ".env"
    elif os.path.isfile(ENV_TEMPLATE_PATH):
        ENV_FILE_LOADED = bool(load_dotenv(dotenv_path=ENV_TEMPLATE_PATH, override=False))
        ENV_SOURCE = ".env.example defaults"
    print(
        f"[ENV] source={ENV_SOURCE} | loaded={ENV_FILE_LOADED} | "
        f"override={_dotenv_override if ENV_SOURCE == '.env' else False} | "
        f"PAPER_MODE={os.getenv('PAPER_MODE', '<unset>')} | "
        f"ENABLE_LIVE_TRADING={os.getenv('ENABLE_LIVE_TRADING', '<unset>')}"
    )
except Exception as _env_exc:
    print(f"[ENV] configuration loader unavailable: {type(_env_exc).__name__}; process environment only")

from ggp_runtime_config import resolve_trading_mode
from ggp_management_events import TradeManagementEventLog
from ggp_telegram import TelegramNotifier
from ggp_position_sizing import (
    calculate_position_size,
    effective_sizing_classification,
    extract_leverage_value,
    is_strong_candidate,
    validate_trade_geometry,
)
from bingx_execution import (TradeExecutionService, BingXPositionModeAdapter,
                              ExecutionState, PositionMode, build_position_order_params,
                              leverage_params)
try:
    from naiso_api_resilience import ORDER_WRITE_METHODS as _ORDER_WRITE_METHODS
except Exception:  # pragma: no cover - fallback keeps the governor self-contained
    _ORDER_WRITE_METHODS = frozenset({
        "create_order", "create_market_order", "create_limit_order",
        "cancel_order", "cancel_all_orders", "swapV1PrivatePostTradeCancelReplace",
    })
from ggp_recovery import resolve_position_identity, build_restored_management_state
from ggp_market_data import (
    REQUIRED_CLOSED_CANDLES,
    MANAGEMENT_OHLCV_LIMIT,
    FETCH_MIN_CANDLES,
    timeframe_seconds as _timeframe_seconds,
)
from ggp_market_intelligence import (
    classify_zone_event, advance_failed_breakout_confirmation,
    is_confirmed_failed_breakout_evidence, build_scalp_target,
    build_support_resistance_zones, classify_dynamic_trade_classification,
)
from ggp_institutional_bridge import build_liquidity_pools
from roro_confluence import classify_liquidity as _classify_live_liquidity
from location_authority import validate_location_zone
import portfolio_allocator
from portfolio_runtime import PortfolioRuntime
from naiso_unified_decision import (classify_cross_state as _project_cross_state,
                                    project_dashboard_sections as _project_dashboard_sections,
                                    project_watchlist_row as _project_watchlist_row)

# Observational only: all routine trade-management decisions still come from
# TradeManagementBoard / LiveTradeManager. This bounded feed never sends orders.
TRADE_MANAGEMENT_LOG = TradeManagementEventLog(max_items=200)
_TRADE_EVENT_DEDUPE = {}
_TRADE_EVENT_DEDUPE_LOCK = threading.RLock()

def record_trade_management_event(event_type, status="INFO", *, log_level=None,
                                  dedupe_key=None, dedupe_seconds=0, **fields):
    now = time.time()
    if dedupe_key:
        with _TRADE_EVENT_DEDUPE_LOCK:
            previous = _TRADE_EVENT_DEDUPE.get(str(dedupe_key), 0.0)
            if now - previous < max(0.0, float(dedupe_seconds)):
                return None
            _TRADE_EVENT_DEDUPE[str(dedupe_key)] = now
            if len(_TRADE_EVENT_DEDUPE) > 500:
                cutoff = now - 900
                for key, ts in list(_TRADE_EVENT_DEDUPE.items()):
                    if ts < cutoff:
                        _TRADE_EVENT_DEDUPE.pop(key, None)
    event = TRADE_MANAGEMENT_LOG.record(event_type, status, **fields)
    logger = globals().get("log_execution")
    if logger and (log_level or event_type not in {"BOARD_DECISION"}):
        details = []
        for key in ("symbol", "side", "price", "roe_pct", "qty_closed", "remaining_qty", "profit_mode", "management_mode", "selected_target_roe_pct", "target_convention_basis", "entry_mode", "entry_behavior", "reason", "order_id"):
            value = event.get(key)
            if value not in (None, ""):
                details.append(f"{key}={value}")
        logger(f"[🧠 PROFIT BRAIN] {event.get('icon','🧠')} {event.get('label',event_type)} | status={status} | " + " | ".join(details),
               log_level or ("WARN" if str(status).upper() in {"UNRESOLVED", "FAILED", "UNKNOWN"} else "INFO"),
               debounce_key=f"trade_event_{dedupe_key}" if dedupe_key else None,
               debounce_sec=max(1, int(dedupe_seconds or 1)))
    return event

def _env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on", "enabled"}:
        return True
    if value in {"0", "false", "no", "off", "disabled"}:
        return False
    print(f"[MODE] Invalid {name}={raw!r}; safe default {bool(default)} applied")
    return bool(default)

def _env_int_range(name, default, minimum, maximum):
    try:
        value = int(os.getenv(name, str(default)))
        return max(minimum, min(maximum, value))
    except (TypeError, ValueError):
        print(f"[CONFIG] Invalid {name}; safe default {default} applied")
        return int(default)

def _env_float_range(name, default, minimum, maximum):
    try:
        value = float(os.getenv(name, str(default)))
        if not math.isfinite(value):
            raise ValueError("non-finite")
        return max(minimum, min(maximum, value))
    except (TypeError, ValueError):
        print(f"[CONFIG] Invalid {name}; safe default {default} applied")
        return float(default)

# Observational only: all routine trade-management decisions still come from
# TradeManagementBoard / LiveTradeManager. This bounded feed never sends orders.
TRADE_MANAGEMENT_LOG = TradeManagementEventLog(max_items=200)
_TRADE_EVENT_DEDUPE = {}
_TRADE_EVENT_DEDUPE_LOCK = threading.RLock()


# ========== INSTITUTIONAL ENGINES (EMBEDDED) ==========
class SmartMoneyEngine:
    @staticmethod
    def _rsi(series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def analyze_smart_money(df):
        if df is None or df.empty:
            return SmartMoneyEngine._default_state()
        required = ["close", "high", "low", "volume"]
        if not all(c in df.columns for c in required):
            return SmartMoneyEngine._default_state()
        if len(df) < 20:
            return SmartMoneyEngine._default_state()
        close = df["close"]
        volume = df["volume"]
        rsi = SmartMoneyEngine._rsi(close, 14)
        vol_ma = volume.rolling(20).mean()
        volume_impulse = volume / vol_ma.replace(0, np.nan)
        vwma = (close * volume).rolling(20).sum() / volume.rolling(20).sum()
        price_distance = ((close - vwma) / vwma.replace(0, np.nan)) * 100
        momentum = close.pct_change(5) * 100
        banker_pressure = (rsi * 0.35) + (volume_impulse * 15) + (momentum * 2) + (price_distance * 1.5)
        banker_pressure = banker_pressure.clip(0, 100)
        retailer_pressure = (100 - banker_pressure).clip(0, 100)
        hot_money_pressure = (abs(momentum) * 5).clip(0, 100)
        smart_money_dominant = (
            banker_pressure.iloc[-1] > 52 and
            banker_pressure.iloc[-1] > retailer_pressure.iloc[-1] + 6
        )
        retail_euphoria = retailer_pressure.iloc[-1] > 75
        distribution_risk = max(0, retailer_pressure.iloc[-1] - banker_pressure.iloc[-1])
        accumulation_strength = banker_pressure.iloc[-1]
        if banker_pressure.iloc[-1] > 60:
            institutional_bias = "BUY"
        elif retailer_pressure.iloc[-1] > 70:
            institutional_bias = "SELL"
        else:
            institutional_bias = "NEUTRAL"
        delta = banker_pressure.iloc[-1] - retailer_pressure.iloc[-1]
        if delta >= 30:
            institutional_bias_detailed = "STRONG_BUY"
        elif delta >= 12:
            institutional_bias_detailed = "BUY"
        elif delta >= 5:
            institutional_bias_detailed = "WEAK_BUY"
        elif delta <= -30:
            institutional_bias_detailed = "STRONG_SELL"
        elif delta <= -12:
            institutional_bias_detailed = "SELL"
        elif delta <= -5:
            institutional_bias_detailed = "WEAK_SELL"
        else:
            institutional_bias_detailed = "NEUTRAL"
        trend_quality = banker_pressure.iloc[-1] - retailer_pressure.iloc[-1]
        flow_alignment = (banker_pressure.iloc[-1] / (retailer_pressure.iloc[-1] + 1)) * 50

        def safe_float_val(v):
            if pd.isna(v) or np.isinf(v):
                return 0.0
            return float(v)

        return {
            "banker_pressure": safe_float_val(banker_pressure.iloc[-1]),
            "retailer_pressure": safe_float_val(retailer_pressure.iloc[-1]),
            "hot_money_pressure": safe_float_val(hot_money_pressure.iloc[-1]),
            "smart_money_dominant": bool(smart_money_dominant),
            "retail_euphoria": bool(retail_euphoria),
            "distribution_risk": safe_float_val(distribution_risk),
            "accumulation_strength": safe_float_val(accumulation_strength),
            "institutional_bias": institutional_bias,
            "institutional_bias_detailed": institutional_bias_detailed,
            "trend_quality": safe_float_val(trend_quality),
            "flow_alignment": safe_float_val(flow_alignment)
        }

    @staticmethod
    def _default_state():
        return {
            "banker_pressure": 50.0,
            "retailer_pressure": 50.0,
            "hot_money_pressure": 50.0,
            "smart_money_dominant": False,
            "retail_euphoria": False,
            "distribution_risk": 0.0,
            "accumulation_strength": 0.0,
            "institutional_bias": "NEUTRAL",
            "institutional_bias_detailed": "NEUTRAL",
            "trend_quality": 0.0,
            "flow_alignment": 25.0
        }


class MomentumFlowEngine:
    @staticmethod
    def _rsi(series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def analyze_momentum_flow(df):
        if df is None or df.empty:
            return MomentumFlowEngine._default_state()
        required = ["close", "high", "low", "volume"]
        if not all(c in df.columns for c in required):
            return MomentumFlowEngine._default_state()
        if len(df) < 20:
            return MomentumFlowEngine._default_state()
        close = df["close"]
        rsi = MomentumFlowEngine._rsi(close, 14)
        ema_fast = close.ewm(span=9).mean()
        ema_slow = close.ewm(span=21).mean()
        momentum_spread = ((ema_fast - ema_slow) / ema_slow.replace(0, np.nan)) * 100
        # Direction-neutral strength; flow_bias below carries the sign.
        # Signed spread previously mislabeled strong bearish trends as weak.
        abs_spread = abs(float(momentum_spread.iloc[-1]))
        norm_strength = min(2.5, abs_spread) / 2.5
        momentum_health = 50 + (norm_strength * 50)
        continuation_strength = min(100, max(0, abs_spread * 20))
        trend_expansion = abs_spread > 0.35
        momentum_decay = abs_spread < 0.05
        rsi_now = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50.0
        climax_risk = max(0.0, rsi_now - 70.0, 30.0 - rsi_now) * 3.0
        exhaustion_risk = max(0, 40 - momentum_health)
        greed_state = (rsi_now > 75) and trend_expansion
        if momentum_spread.iloc[-1] > 0:
            flow_bias = "BUY"
        elif momentum_spread.iloc[-1] < 0:
            flow_bias = "SELL"
        else:
            flow_bias = "NEUTRAL"

        def safe_float_val(v):
            if pd.isna(v) or np.isinf(v):
                return 0.0
            return float(v)

        return {
            "continuation_strength": safe_float_val(continuation_strength),
            "momentum_health": safe_float_val(momentum_health),
            "trend_expansion": bool(trend_expansion),
            "momentum_decay": bool(momentum_decay),
            "climax_risk": safe_float_val(climax_risk),
            "rsi_value": safe_float_val(rsi_now),
            "exhaustion_risk": safe_float_val(exhaustion_risk),
            "greed_state": bool(greed_state),
            "flow_bias": flow_bias
        }

    @staticmethod
    def _default_state():
        return {
            "continuation_strength": 0.0,
            "momentum_health": 50.0,
            "trend_expansion": False,
            "momentum_decay": False,
            "climax_risk": 0.0,
            "rsi_value": 50.0,
            "exhaustion_risk": 0.0,
            "greed_state": False,
            "flow_bias": "NEUTRAL"
        }


# ========== TRADE STATE MACHINE (for context) ==========
class TradeStateMachine:
    STATES = {
        "ACCUMULATION": 0,
        "EXPANSION": 1,
        "TREND_RIDE": 2,
        "DISTRIBUTION": 3,
        "EXHAUSTION": 4,
        "FAKE_BREAKOUT": 5,
        "MOMENTUM_COLLAPSE": 6,
        "PANIC_EXIT": 7,
        "RANGE_CHOP": 8,
        "HEALTHY_PULLBACK": 9,
        "PROFIT_DEFENSE": 10,
        "LIQUIDITY_EXHAUSTION": 11
    }

    def __init__(self):
        self.current_state = "RANGE_CHOP"
        self.last_state_change = 0
        self.state_confidence = 0.0
        self.last_transition_reason = ""
        self._update_lock = threading.RLock()

    def update(self, smart: dict, momentum: dict, adx: float, regime: str, side: str = None,
               *, source: str = "UNSPECIFIED", symbol: str = "", candle_ts=None,
               snapshot: dict = None) -> str:
        """Advance one state machine instance with an attributed transition log.

        The transition rules are unchanged. The lock and attribution prevent
        concurrent watchlist/live updates from being logged as one unexplained
        authoritative state change.
        """
        with self._update_lock:
            return self._update_locked(
                smart, momentum, adx, regime, side=side, source=source,
                symbol=symbol, candle_ts=candle_ts, snapshot=snapshot)

    def _update_locked(self, smart: dict, momentum: dict, adx: float, regime: str, side: str = None,
                       *, source: str = "UNSPECIFIED", symbol: str = "", candle_ts=None,
                       snapshot: dict = None) -> str:
        banker = smart.get("banker_pressure", 50)
        retail = smart.get("retailer_pressure", 50)
        dist_risk = smart.get("distribution_risk", 0)
        accum = smart.get("accumulation_strength", 0)
        mom_health = momentum.get("momentum_health", 50)
        cont_strength = momentum.get("continuation_strength", 50)
        exh_risk = momentum.get("exhaustion_risk", 0)
        climax = momentum.get("climax_risk", 0)
        expansion = momentum.get("trend_expansion", False)
        decay = momentum.get("momentum_decay", False)
        bias_detailed = smart.get("institutional_bias_detailed", "NEUTRAL")
        direction = str(side or "").upper()
        smart_bias = str(smart.get("institutional_bias") or "").upper()
        momentum_bias = str(momentum.get("flow_bias") or "").upper()
        smart_opposed = direction in ("BUY", "SELL") and smart_bias in ("BUY", "SELL") and smart_bias != direction
        momentum_aligned = (direction not in ("BUY", "SELL") or momentum_bias not in ("BUY", "SELL") or momentum_bias == direction) and not smart_opposed
        adverse_smart_flow = smart_opposed and (dist_risk > 60 or banker > 65)

        # PANIC_EXIT
        if (bias_detailed in ("STRONG_SELL", "STRONG_BUY") and
            dist_risk > 75 and cont_strength < 20 and decay
            and (direction not in ("BUY", "SELL") or smart_opposed)):
            new_state = "PANIC_EXIT"
        # MOMENTUM_COLLAPSE uses directional strength/decay, not signed health.
        elif cont_strength < 25 and decay and (direction not in ("BUY", "SELL") or smart_opposed):
            new_state = "MOMENTUM_COLLAPSE"
        # LIQUIDITY_EXHAUSTION
        elif exh_risk > 70 or climax > 75:
            new_state = "LIQUIDITY_EXHAUSTION"
        # PROFIT_DEFENSE
        elif adverse_smart_flow and dist_risk > 50 and cont_strength < 30:
            new_state = "PROFIT_DEFENSE"
        # Distribution is adverse only when institutional flow opposes this position.
        elif adverse_smart_flow and dist_risk > 60:
            new_state = "DISTRIBUTION"
        elif banker > 65 and dist_risk < 25 and mom_health > 40 and direction != "SELL":
            new_state = "ACCUMULATION"
        elif adx > 30 and expansion and cont_strength > 60 and mom_health > 50 and momentum_aligned:
            new_state = "EXPANSION"
        elif cont_strength > 75 and mom_health > 60 and dist_risk < 30 and momentum_aligned:
            new_state = "TREND_RIDE"
        elif 20 <= adx <= 35 and mom_health > 45 and not expansion and not decay and dist_risk < 40 and momentum_aligned:
            new_state = "HEALTHY_PULLBACK"
        # FAKE_BREAKOUT
        elif retail > 70 and banker < 45 and climax > 60:
            new_state = "FAKE_BREAKOUT"
        # RANGE_CHOP
        elif adx < 22 or regime in ("CHOPPY", "COMPRESSION"):
            new_state = "RANGE_CHOP"
        else:
            new_state = self.current_state

        if new_state != self.current_state:
            previous_state = self.current_state
            self.last_state_change = time.time()
            self.state_confidence = 0.5
            self.last_transition_reason = new_state
            detail = snapshot if isinstance(snapshot, dict) else {
                "adx": round(float(adx or 0), 2), "regime": regime,
                "continuation_strength": round(float(cont_strength or 0), 2),
                "momentum_decay": bool(decay), "distribution_risk": round(float(dist_risk or 0), 2),
                "smart_opposed": bool(smart_opposed), "side": direction or "NONE",
            }
            log_execution(
                f"[STATE] source={source} symbol={symbol or 'PORTFOLIO'} "
                f"candle_ts={candle_ts if candle_ts is not None else '-'} "
                f"{previous_state} -> {new_state} reason={new_state} detail={detail}",
                "INFO", debounce_key=f"state_transition:{source}:{symbol}:{previous_state}:{new_state}",
                debounce_sec=1)
        else:
            self.state_confidence = min(1.0, self.state_confidence + 0.05)

        self.current_state = new_state
        return new_state

    def get_trail_multiplier(self) -> float:
        mult_map = {
            "ACCUMULATION": 3.0,
            "EXPANSION": 3.5,
            "TREND_RIDE": 4.0,
            "HEALTHY_PULLBACK": 2.8,
            "PROFIT_DEFENSE": 1.2,
            "DISTRIBUTION": 1.2,
            "EXHAUSTION": 1.0,
            "LIQUIDITY_EXHAUSTION": 0.8,
            "FAKE_BREAKOUT": 0.8,
            "MOMENTUM_COLLAPSE": 0.6,
            "PANIC_EXIT": 0.5,
            "RANGE_CHOP": 1.5
        }
        return mult_map.get(self.current_state, 1.5)

    def should_delay_tp1(self) -> bool:
        return self.current_state in ("ACCUMULATION", "EXPANSION", "TREND_RIDE", "HEALTHY_PULLBACK")

    def should_aggressive_profit_lock(self) -> bool:
        return self.current_state in ("EXHAUSTION", "DISTRIBUTION", "MOMENTUM_COLLAPSE", "PROFIT_DEFENSE", "LIQUIDITY_EXHAUSTION")

    def should_hard_exit(self) -> bool:
        return self.current_state in ("PANIC_EXIT", "MOMENTUM_COLLAPSE", "LIQUIDITY_EXHAUSTION")

    def get_patience_level(self) -> str:
        if self.current_state in ("ACCUMULATION", "EXPANSION", "TREND_RIDE", "HEALTHY_PULLBACK"):
            return "HIGH"
        elif self.current_state in ("DISTRIBUTION", "EXHAUSTION", "PROFIT_DEFENSE"):
            return "LOW"
        else:
            return "MEDIUM"


# ========== TRADE STATE & PERFORMANCE TRACKING ==========
TRADE_STATE = {
    "in_position": False,
    "symbol": None,
    "side": None,
    "entry": 0.0,
    "qty": 0.0,
    "tp1_hit": False,
    "tp2_hit": False,
    "trail_on": False,
    "zone": None,
    "location": None,
    "reason": [],
    "last_update_ts": 0
}

PERF = {
    "total_pnl_pct": 0.0,
    "total_pnl_usdt": 0.0,
    "trades": 0,
    "wins": 0,
    "losses": 0,
    "last_trade": None
}

# ========== ANSI COLOR CODES ==========
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"

def color_pnl(pnl_pct):
    return f"{GREEN}{pnl_pct:.2f}%{RESET}" if pnl_pct >= 0 else f"{RED}{pnl_pct:.2f}%{RESET}"

def color_text(text, color):
    return f"{color}{text}{RESET}"

# ========== SANITIZATION & JSON FIX ==========
def safe_json(obj):
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (pd.Series, pd.DataFrame)):
        return obj.to_dict() if hasattr(obj, 'to_dict') else str(obj)
    if isinstance(obj, dict):
        # Snapshot the live mapping before recursive serialization. Scanner and
        # trade-manager threads may resize shared dictionaries concurrently.
        snapshot = obj.copy()
        return {k: safe_json(v) for k, v in snapshot.items()}
    if isinstance(obj, (list, tuple)):
        return [safe_json(i) for i in obj]
    return obj

def to_json_safe(obj):
    try:
        if obj is None:
            return {}
        if hasattr(obj, "to_dict"):
            return safe_json(obj.to_dict(orient="records"))
        if isinstance(obj, (dict, list, str, int, float, bool)):
            return safe_json(obj)
        return str(obj)
    except:
        return {}

def safe_get(d, key, default=None):
    if d is None:
        return default
    return d.get(key, default)

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except:
        return default

# ========== CACHE & RATE LIMIT ==========
CACHE = {
    "balance": {"value": 0.0, "ts": 0},
    "free_balance": {"value": 0.0, "ts": 0},
    "ohlcv": {},
    "ticker": {},
    "orderbook": {},
}
_last_api_call = 0.0
MIN_API_INTERVAL = _env_float_range("MIN_API_INTERVAL", 0.35, 0.20, 2.0)
# BingX documents a shared public market-data IP budget of 500/10s.
# The current swap endpoint reference separately annotates 1/s for specific
# endpoints (contracts, depth, recent trades, klines, funding history, ticker,
# bookTicker and open interest). Apply 1.05s only to those verified methods;
# do not infer a rate from an IP-group identifier.
BINGX_KLINE_MIN_INTERVAL = _env_float_range("BINGX_KLINE_MIN_INTERVAL", 1.05, 1.0, 2.0)
BINGX_PUBLIC_MARKET_MIN_INTERVAL = _env_float_range(
    "BINGX_PUBLIC_MARKET_MIN_INTERVAL", BINGX_KLINE_MIN_INTERVAL, 1.0, 2.0
)
# BingX's official Futures rate table distinguishes Ordinary and Trader tiers:
# - Place order: 10/s Ordinary, 1/s Trader; cancel order: 5/s Ordinary, 1/s Trader.
# - Account tier is not available from this process, so use the restrictive
#   published Trader ceiling locally (1.05s); this is not an assertion of tier.
# - Leverage/margin/position-mode writes use a conservative local 2/s ceiling.
# The official cancelAllAfter reference is also conservatively paced at 1/s.
BINGX_TRADE_WRITE_MIN_INTERVAL = _env_float_range(
    "BINGX_TRADE_WRITE_MIN_INTERVAL", 1.05, 1.0, 2.0
)
BINGX_CONFIG_WRITE_MIN_INTERVAL = _env_float_range(
    "BINGX_CONFIG_WRITE_MIN_INTERVAL", 0.55, 0.5, 2.0
)
_API_METHOD_MIN_INTERVALS = {
    # Public swap endpoints individually annotated at 1/s per IP.
    "fetch_ohlcv": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_order_book": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_trades": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_ticker": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_tickers": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_bids_asks": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_funding_rate_history": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_markets": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    "fetch_open_interest": BINGX_PUBLIC_MARKET_MIN_INTERVAL,
    # Order writes use a conservative local 1/s cap (not the exchange limit).
    "create_order": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "create_market_order": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "create_limit_order": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "create_orders": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "cancel_order": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "cancel_orders": BINGX_TRADE_WRITE_MIN_INTERVAL,
    # Leverage, margin and position-mode configuration: 2/s.
    "set_leverage": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "fetch_margin_mode": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "set_margin_mode": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "fetch_position_mode": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "set_position_mode": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "add_margin": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    "reduce_margin": BINGX_CONFIG_WRITE_MIN_INTERVAL,
    # BingX cancelAllAfter is documented at 1/s per UID; retain local 1/s cap.
    "cancel_all_orders_after": BINGX_TRADE_WRITE_MIN_INTERVAL,
}
# Endpoints sharing one conservative exchange quota share a pacing lane.
# In particular, order placement and cancellation must not each consume an
# independent 1/s local allowance.
_API_RATE_METHOD_LANES = {
    "create_order": "trade_write",
    "create_market_order": "trade_write",
    "create_limit_order": "trade_write",
    "create_orders": "trade_write",
    "cancel_order": "trade_write",
    "cancel_orders": "trade_write",
    "cancel_all_orders": "trade_write",
    "cancel_all_orders_after": "trade_write",
    "swapV1PrivatePostTradeCancelReplace": "trade_write",
    "set_leverage": "config_write",
    "fetch_margin_mode": "config_write",
    "set_margin_mode": "config_write",
    "fetch_position_mode": "config_write",
    "set_position_mode": "config_write",
    "add_margin": "config_write",
    "reduce_margin": "config_write",
}
_API_RATE_LANE_MIN_INTERVALS = {
    "trade_write": BINGX_TRADE_WRITE_MIN_INTERVAL,
    "config_write": BINGX_CONFIG_WRITE_MIN_INTERVAL,
}
_API_RATE_LOCK = threading.Lock()
# Never let API scheduler contention pin MainThread indefinitely. A timeout is
# fail-closed: the exchange call is not sent and the next loop can try again.
API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC = _env_float_range(
    "API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.75, 0.05, 2.0
)
_API_RATE_LOCK_OWNER = {
    "thread_id": None, "thread_name": "", "since_monotonic": 0.0, "method": ""
}
# Separate the shared request-start floor from endpoint-specific pacing.
# Reservations are made atomically once; callers sleep outside the lock and never
# repeatedly compete for the same slot.
_API_RATE_METHOD_LAST_RESERVED = {}
_API_RATE_METHOD_LAST_START = {}
_API_RATE_LAST_START = 0.0
_LAST_API_RATE_LIMIT = {"ts": 0.0, "method": "", "message": ""}

# Overall pacing budget. The old loop only bounded each lock acquisition, so a
# tiny remaining wait or a wedged clock could spin forever. Both a logical
# (possibly injected) deadline and a captured real-time deadline are enforced.
API_RATE_WAIT_TIMEOUT_SEC = _env_float_range("API_RATE_WAIT_TIMEOUT_SEC", 5.0, 0.05, 30.0)
API_RATE_MIN_SLEEP_SEC = _env_float_range("API_RATE_MIN_SLEEP_SEC", 0.001, 0.000001, 0.05)
API_RATE_MAX_WAIT_ITERATIONS = _env_int_range("API_RATE_MAX_WAIT_ITERATIONS", 4096, 8, 100000)
_HARD_MONOTONIC = time.monotonic


class ApiRateLockTimeout(RuntimeError):
    """Raised when the shared API scheduler cannot reserve a request start."""


def rate_limit(func=None):
    """Reserve a fair, bounded request slot using global and per-endpoint clocks.

    The global floor remains MIN_API_INTERVAL. Endpoint-specific limits are
    independent lanes, so (for example) OHLCV pacing cannot serialize ticker,
    order-book, account, or execution endpoints at the OHLCV interval.
    Reservation is atomic and happens once; waiting never holds the scheduler
    lock and never loops by repeatedly reacquiring the same reservation.
    """
    global _last_api_call, _API_RATE_LAST_START
    method_name = getattr(func, "__name__", "") if func is not None else ""
    method_name = str(method_name or "exchange_call")
    lane_name = str(_API_RATE_METHOD_LANES.get(method_name, method_name))

    def _finite(value):
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError):
            return False

    base_interval = float(MIN_API_INTERVAL)
    endpoint_interval = float(_API_METHOD_MIN_INTERVALS.get(method_name, base_interval))
    if not (_finite(base_interval) and _finite(endpoint_interval)):
        raise ApiRateLockTimeout(
            f"API_RATE rejected non-finite interval for {method_name}; "
            f"base={base_interval!r} endpoint={endpoint_interval!r}"
        )
    if base_interval <= 0.0 or endpoint_interval <= 0.0:
        raise ApiRateLockTimeout(
            f"API_RATE rejected invalid interval for {method_name}; "
            f"base={base_interval!r} endpoint={endpoint_interval!r}"
        )
    lane_interval = float(
        _API_RATE_LANE_MIN_INTERVALS.get(lane_name, endpoint_interval)
    )
    if not _finite(lane_interval) or lane_interval <= 0.0:
        raise ApiRateLockTimeout(
            f"API_RATE rejected invalid lane interval for {lane_name}; "
            f"interval={lane_interval!r}"
        )
    endpoint_interval = max(base_interval, endpoint_interval, lane_interval)
    tolerance = max(1e-12, endpoint_interval * 1e-9)

    real_deadline = _HARD_MONOTONIC() + API_RATE_WAIT_TIMEOUT_SEC
    logical_start = time.monotonic()
    logical_deadline = logical_start + API_RATE_WAIT_TIMEOUT_SEC
    if not (_finite(real_deadline) and _finite(logical_start)
            and _finite(logical_deadline) and _finite(_last_api_call)):
        raise ApiRateLockTimeout(
            f"API_RATE rejected non-finite pacing state for {method_name}; "
            f"real_deadline={real_deadline!r} logical={logical_start!r} "
            f"last_call={_last_api_call!r}"
        )

    def _remaining():
        real_left = real_deadline - _HARD_MONOTONIC()
        logical_now = time.monotonic()
        logical_left = logical_deadline - logical_now
        if not (_finite(real_left) and _finite(logical_now) and _finite(logical_left)):
            raise ApiRateLockTimeout(
                f"API_RATE rejected non-finite clock while scheduling {method_name}"
            )
        if real_left <= 0.0 or logical_left <= 0.0:
            raise ApiRateLockTimeout(
                f"API_RATE pacing deadline exceeded for {method_name}; "
                f"remaining_logical={logical_left:.6f}s remaining_real={real_left:.6f}s"
            )
        return real_left, logical_left, logical_now

    def _acquire_scheduler_lock():
        real_left, logical_left, _ = _remaining()
        acquire_timeout = max(
            0.0,
            min(API_RATE_LOCK_ACQUIRE_TIMEOUT_SEC, real_left, logical_left),
        )
        if not _API_RATE_LOCK.acquire(timeout=acquire_timeout):
            owner = dict(_API_RATE_LOCK_OWNER)
            owner_since = float(owner.get("since_monotonic") or 0.0)
            held_for = max(0.0, time.monotonic() - owner_since) if owner_since > 0 else 0.0
            raise ApiRateLockTimeout(
                f"API_RATE_LOCK acquisition timed out after {acquire_timeout:.2f}s "
                f"for {method_name}; owner_thread={owner.get('thread_name') or 'unknown'} "
                f"owner_id={owner.get('thread_id')} held_for={held_for:.2f}s "
                f"owner_method={owner.get('method') or 'unknown'}"
            )
        _API_RATE_LOCK_OWNER.update({
            "thread_id": threading.get_ident(),
            "thread_name": threading.current_thread().name,
            "since_monotonic": time.monotonic(),
            "method": method_name,
        })

    def _release_scheduler_lock():
        _API_RATE_LOCK_OWNER.update({
            "thread_id": None, "thread_name": "", "since_monotonic": 0.0, "method": ""
        })
        _API_RATE_LOCK.release()

    def _wait_until(target, phase):
        for _iteration in range(API_RATE_MAX_WAIT_ITERATIONS):
            real_left, logical_left, logical_now = _remaining()
            wait_seconds = float(target) - logical_now
            if not _finite(wait_seconds):
                raise ApiRateLockTimeout(
                    f"API_RATE rejected non-finite {phase} wait for {method_name}"
                )
            if wait_seconds <= tolerance:
                return
            if wait_seconds > real_left or wait_seconds > logical_left:
                raise ApiRateLockTimeout(
                    f"API_RATE pacing deadline exceeded for {method_name} during {phase}; "
                    f"required={wait_seconds:.6f}s remaining_logical={logical_left:.6f}s "
                    f"remaining_real={real_left:.6f}s"
                )
            sleep_for = max(float(wait_seconds), API_RATE_MIN_SLEEP_SEC)
            if sleep_for > real_left or sleep_for > logical_left:
                raise ApiRateLockTimeout(
                    f"API_RATE pacing deadline would be exceeded for {method_name} during {phase}"
                )
            time.sleep(sleep_for)
        raise ApiRateLockTimeout(
            f"API_RATE pacing iteration budget ({API_RATE_MAX_WAIT_ITERATIONS}) "
            f"exhausted for {method_name} during {phase}"
        )

    # Reserve one slot under a short critical section. The global floor protects
    # aggregate traffic; each endpoint clock applies only to calls of that API.
    _acquire_scheduler_lock()
    try:
        real_left, logical_left, now = _remaining()
        global_due = float(_last_api_call) + base_interval
        method_previous = _API_RATE_METHOD_LAST_RESERVED.get(lane_name)
        method_due = (
            float(method_previous) + endpoint_interval
            if method_previous is not None else now
        )
        if not (_finite(global_due) and _finite(method_due)):
            raise ApiRateLockTimeout(
                f"API_RATE rejected non-finite reservation deadline for {method_name}"
            )
        reserved_at = max(now, global_due, method_due)
        wait_seconds = reserved_at - now
        if wait_seconds > real_left or wait_seconds > logical_left:
            raise ApiRateLockTimeout(
                f"API_RATE pacing deadline exceeded for {method_name}; "
                f"reservation_wait={wait_seconds:.6f}s "
                f"remaining_logical={logical_left:.6f}s remaining_real={real_left:.6f}s"
            )
        _last_api_call = reserved_at
        _API_RATE_METHOD_LAST_RESERVED[lane_name] = reserved_at
    finally:
        _release_scheduler_lock()

    # No scheduler lock is held during pacing sleep.
    _wait_until(reserved_at, "reserved_slot")

    # Protect actual request starts against OS thread wake-up reordering. A
    # delayed earlier thread cannot let a later thread violate either interval.
    for _iteration in range(API_RATE_MAX_WAIT_ITERATIONS):
        _acquire_scheduler_lock()
        retry_at = None
        try:
            _, _, now = _remaining()
            method_actual = _API_RATE_METHOD_LAST_START.get(lane_name)
            if (not _finite(_API_RATE_LAST_START)
                    or (method_actual is not None and not _finite(method_actual))):
                raise ApiRateLockTimeout(
                    f"API_RATE rejected non-finite actual-start state for {method_name}"
                )
            global_actual_due = (
                _API_RATE_LAST_START + base_interval
                if _API_RATE_LAST_START > 0.0 else now
            )
            method_actual_due = (
                float(method_actual) + endpoint_interval
                if method_actual is not None else now
            )
            actual_due = max(global_actual_due, method_actual_due)
            if now + tolerance >= actual_due:
                _API_RATE_LAST_START = now
                _API_RATE_METHOD_LAST_START[lane_name] = now
                return
            retry_at = actual_due
        finally:
            _release_scheduler_lock()
        _wait_until(retry_at, "actual_start")
    raise ApiRateLockTimeout(
        f"API_RATE actual-start verification budget exhausted for {method_name}"
    )

CACHE_LOCK = threading.RLock()
CACHE_MAX_ITEMS = _env_int_range("CACHE_MAX_ITEMS", 2500, 100, 10000)

# One canonical market-data snapshot per symbol/timeframe. All callers (live
# management, discovery scanners and the institutional Watchlist) share this
# snapshot; caller-specific `limit` values only slice the cached frame.
OHLCV_CACHE_LIMIT = 150
OHLCV_RATE_LIMIT_COOLDOWN_SEC = _env_float_range("OHLCV_RATE_LIMIT_COOLDOWN_SEC", 3.0, 1.0, 15.0)
_OHLCV_INFLIGHT_LOCKS = {}
_OHLCV_INFLIGHT_GUARD = threading.Lock()
_OHLCV_RATE_COOLDOWN = {"until": 0.0, "symbol_until": {}}


def _ohlcv_inflight_lock(key):
    """Return a stable per-key lock so concurrent callers cannot duplicate a fetch."""
    with _OHLCV_INFLIGHT_GUARD:
        lock = _OHLCV_INFLIGHT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _OHLCV_INFLIGHT_LOCKS[key] = lock
        return lock


def _ohlcv_canonical_cache_key(symbol, limit, *, htf=False):
    """Canonicalize symbol aliases and limit buckets into one cache identity."""
    try:
        canonical = normalize_symbol(symbol)
    except Exception:
        canonical = str(symbol or "").strip().upper()
    timeframe = "1h" if htf else str(INTERVAL)
    return canonical, f"ohlcv_v2_{canonical}_{timeframe}_{OHLCV_CACHE_LIMIT}", timeframe


def _ohlcv_cache_get(key, ttl, timeframe):
    """Read one canonical OHLCV snapshot while its bounded TTL is valid.

    Candle-level staleness is evaluated by ``resolve_entry_closed_candles`` where
    it can trigger one governed refresh. The shared cache itself deliberately
    keeps the established TTL semantics so management/scanner callers do not
    issue duplicate requests between candles.
    """
    with CACHE_LOCK:
        item = CACHE.get(key)
        if not isinstance(item, dict) or "value" not in item or "ts" not in item:
            return None
        if time.time() - float(item["ts"]) >= ttl:
            CACHE.pop(key, None)
            return None
        return item["value"]


def _ohlcv_cache_set(key, frame, timeframe):
    with CACHE_LOCK:
        CACHE[key] = {
            "value": frame.copy(),
            "ts": time.time(),
            "timeframe": str(timeframe),
        }
        if len(CACHE) > CACHE_MAX_ITEMS:
            dated = sorted(
                ((float(v.get("ts", 0)), k) for k, v in CACHE.items()
                 if isinstance(v, dict) and "ts" in v and "value" in v),
                key=lambda item: item[0]
            )
            for _, old_key in dated[:max(1, len(CACHE) - CACHE_MAX_ITEMS)]:
                CACHE.pop(old_key, None)


def _ohlcv_rate_limit_cooldown_remaining(symbol):
    now = time.monotonic()
    with _OHLCV_INFLIGHT_GUARD:
        until = max(
            float(_OHLCV_RATE_COOLDOWN.get("until", 0.0)),
            float(_OHLCV_RATE_COOLDOWN.get("symbol_until", {}).get(str(symbol), 0.0)),
        )
    return max(0.0, until - now)


def _mark_ohlcv_rate_limit_cooldown(symbol):
    """Fence repeat kline requests briefly after a confirmed exchange rate-limit."""
    now = time.monotonic()
    until = now + OHLCV_RATE_LIMIT_COOLDOWN_SEC
    with _OHLCV_INFLIGHT_GUARD:
        _OHLCV_RATE_COOLDOWN["until"] = max(float(_OHLCV_RATE_COOLDOWN.get("until", 0.0)), until)
        symbol_until = _OHLCV_RATE_COOLDOWN.setdefault("symbol_until", {})
        symbol_until[str(symbol)] = max(float(symbol_until.get(str(symbol), 0.0)), until)
        if len(symbol_until) > 500:
            for key in [k for k, v in symbol_until.items() if float(v) <= now]:
                symbol_until.pop(key, None)


def cache_get(key, ttl):
    with CACHE_LOCK:
        item = CACHE.get(key)
        if isinstance(item, dict) and "ts" in item and "value" in item:
            if time.time() - float(item["ts"]) < ttl:
                return item["value"]
            CACHE.pop(key, None)
    return None

def cache_set(key, value):
    now = time.time()
    with CACHE_LOCK:
        CACHE[key] = {"value": value, "ts": now}
        if len(CACHE) > CACHE_MAX_ITEMS:
            dated = sorted(
                ((float(v.get("ts", 0)), k) for k, v in CACHE.items()
                 if isinstance(v, dict) and "ts" in v and "value" in v),
                key=lambda item: item[0]
            )
            for _, old_key in dated[:max(1, len(CACHE) - CACHE_MAX_ITEMS)]:
                CACHE.pop(old_key, None)

def safe_api_call(func, *args, **kwargs):
    # Internal callers may lower the retry budget for latency-sensitive reads
    # that run on the foreground trade-control loop. This never increases the
    # configured global retry budget and is not forwarded to CCXT.
    requested_attempts = kwargs.pop("_api_max_attempts", None)
    # Order creation is non-idempotent. Never automatically retry a timed-out
    # create_order call: BingX may have accepted the first request already.
    # functools.wraps wrappers must not hide an order write behind a lambda name.
    unwrapped = func
    for _ in range(8):
        candidate = getattr(unwrapped, "__wrapped__", None)
        if candidate is None:
            break
        unwrapped = candidate
    method_name = getattr(unwrapped, "__name__", getattr(func, "__name__", "exchange_call"))
    # Cancellations and cancel-replace are order mutations too. A timeout may
    # have been applied by BingX, so retrying can duplicate or escalate an
    # uncertain order-state change; reconciliation must decide, not the retry loop.
    is_order_write = method_name in _ORDER_WRITE_METHODS
    resilience = globals().get("API_RESILIENCE")
    if is_order_write and resilience is not None:
        try:
            resilience.before_call(method_name)
        except CircuitOpenError:
            if "MEMORY" in globals():
                MEMORY["api_circuit_state"] = "OPEN"
            log_execution(f"[API_CIRCUIT] {method_name} temporarily fenced; no request sent", "WARN",
                          debounce_key=f"api_circuit_{method_name}", debounce_sec=30)
            raise
    if is_order_write:
        try:
            rate_limit(func)
        except ApiRateLockTimeout as exc:
            if resilience is not None:
                resilience.record_failure(method_name, exc)
            raise
        if resilience is not None:
            resilience.count_request()
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            if resilience is not None:
                resilience.record_failure(method_name, exc)
            try:
                if "STATE" in globals():
                    STATE["execution_unresolved"] = True
                if "MEMORY" in globals():
                    MEMORY["recovery_status"] = "ORDER_SUBMISSION_UNCERTAIN"
                if "RECOVERY_BLOCK_ENTRIES" in globals():
                    globals()["RECOVERY_BLOCK_ENTRIES"] = True
                if "log_execution" in globals():
                    log_execution(f"[ORDER_UNCERTAIN] {method_name} raised {type(exc).__name__}: {exc}; no automatic retry; exchange reconciliation required", "ERROR")
            except Exception:
                pass
            raise
        if resilience is not None:
            resilience.record_success(method_name)
        return result
    configured_attempts = int(getattr(resilience, "max_attempts", 3) or 3) if resilience is not None else 3
    attempts = configured_attempts
    if requested_attempts is not None:
        try:
            # A call-site override may only tighten the shared retry policy.
            attempts = min(configured_attempts, max(1, int(requested_attempts)))
        except (TypeError, ValueError):
            attempts = configured_attempts
    for attempt in range(attempts):
        try:
            # The circuit gate must be enforced for every physical attempt. A
            # retry after a transport failure may open the circuit; the next
            # attempt must not slip past that gate.
            if resilience is not None:
                try:
                    resilience.before_call(method_name)
                except CircuitOpenError:
                    if "MEMORY" in globals():
                        MEMORY["api_circuit_state"] = "OPEN"
                    log_execution(
                        f"[API_CIRCUIT] {method_name} temporarily fenced on retry {attempt + 1}/{attempts}; no request sent",
                        "WARN", debounce_key=f"api_circuit_{method_name}", debounce_sec=30)
                    raise
            # Local scheduler contention is classified once by the outer
            # exception handler; it is not an exchange request and is never retried.
            rate_limit(func)
            if resilience is not None:
                resilience.count_request()
            result = func(*args, **kwargs)
            if resilience is not None:
                resilience.record_success(method_name)
            return result
        except CircuitOpenError:
            raise
        except ApiRateLockTimeout as exc:
            # Local scheduling is never an exchange failure. Even when the
            # optional resilience module is disabled, fail closed without
            # retrying, backing off, or sending a request.
            if resilience is not None:
                resilience.record_failure(method_name, exc)
            raise
        except Exception as e:
            failure_kind = None
            if resilience is not None:
                failure_kind = resilience.record_failure(method_name, e)
            else:
                # Absence of the optional circuit breaker must not turn a
                # business/configuration failure into a retryable read.
                fallback_classifier = globals().get("classify_api_error")
                if callable(fallback_classifier):
                    try:
                        failure_kind = fallback_classifier(e, method=method_name)
                    except Exception:
                        failure_kind = None
            is_rate_limited = (("rate limit" in str(e).lower() or "100410" in str(e))
                               if failure_kind is None else failure_kind.kind == "RATE_LIMITED")
            if is_rate_limited:
                _LAST_API_RATE_LIMIT.update({
                    "ts": time.time(),
                    "method": method_name,
                    "message": str(e)[:180],
                })
                try:
                    log_execution(
                        f"[API_BACKOFF] {method_name} rate-limited; bounded retry {attempt + 1}/{attempts}",
                        "WARN", debounce_key=f"api_backoff_{method_name}", debounce_sec=30
                    )
                except Exception:
                    pass
            if failure_kind is not None and not failure_kind.retryable:
                try:
                    log_execution(
                        f"[API_NO_RETRY] {method_name} failed with non-retryable "
                        f"{failure_kind.kind}: {str(e)[:180]}",
                        "ERROR", debounce_key=f"api_no_retry_{method_name}", debounce_sec=30
                    )
                except Exception:
                    pass
                raise
            if attempt == attempts - 1:
                raise
            if resilience is not None:
                resilience.count_retry()
                resilience.sleep_backoff(attempt, rate_limited=is_rate_limited)
            else:
                time.sleep(1 if not is_rate_limited else min(8, 2 ** attempt))
    return None

# ========== TELEGRAM ==========
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
TELEGRAM_TIMEOUT_SECONDS = _env_float_range("TELEGRAM_TIMEOUT_SECONDS", 8.0, 2.0, 20.0)
_last_tg_msg = {}
_last_tg_lock = threading.RLock()

def _telegram_error(message):
    logger = globals().get("log_execution")
    if logger:
        logger(f"[TELEGRAM] {message}", "WARN", debounce_key="telegram_delivery_error", debounce_sec=60)

TELEGRAM_NOTIFIER = TelegramNotifier(
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    timeout=TELEGRAM_TIMEOUT_SECONDS, max_pending=100,
    on_error=_telegram_error,
)

def _tg_send(text):
    """Queue Telegram delivery off the trading thread; never expose credentials."""
    return TELEGRAM_NOTIFIER.enqueue(text)

def send_once(msg, key, cooldown=60):
    now = time.time()
    cache_key = str(key)
    with _last_tg_lock:
        last = _last_tg_msg.get(cache_key, 0.0)
        if now - last <= cooldown:
            return False
        queued = _tg_send(msg)
        if queued:
            _last_tg_msg[cache_key] = now
            if len(_last_tg_msg) > 1000:
                cutoff = now - 86400
                for old_key, ts in list(_last_tg_msg.items()):
                    if ts < cutoff:
                        _last_tg_msg.pop(old_key, None)
        return queued

def tg_start(balance, mode):
    balance_text = f"{balance:.2f} USDT" if isinstance(balance, (int, float)) and math.isfinite(balance) else "UNAVAILABLE"
    send_once(f"🚀 <b>GGP Institutional</b>\n💼 Balance: {balance_text}\n⚙️ Mode: {html_escape(str(mode))}\n⚖️ Leverage policy: 20x\n🧠 Trade manager: Unified", "startup", 86400)

def tg_entry(side, symbol, entry, sl, tp1, score, reason, entry_type,
             tp2=None, qty=None, leverage=None, trade_id=None):
    side_text = str(side).upper()
    side_emoji = "🟢" if side_text == "BUY" else "🔴" if side_text == "SELL" else "⚪"
    safe_symbol = html_escape(str(symbol))
    safe_type = html_escape(str(entry_type))
    safe_reason = html_escape(str(reason)[:160])
    parts = [
        f"{side_emoji} <b>POSITION OPENED · {html_escape(side_text)} {safe_type}</b>",
        f"📊 {safe_symbol}",
        f"💰 Planned entry: {safe_float(entry):.8f}",
        f"🛑 Structural SL: {safe_float(sl):.8f}",
        f"🥇 TP1: {safe_float(tp1):.8f}",
        f"🥈 TP2: {safe_float(tp2):.8f}" if tp2 is not None and safe_float(tp2) > 0 else "🥈 TP2: NOT SET",
    ]
    if qty is not None: parts.append(f"📦 Position quantity: {safe_float(qty):.8f}")
    if leverage is not None: parts.append(f"⚖️ Leverage: {safe_float(leverage):.2f}x")
    if score is not None: parts.append(f"🧠 Setup score: {html_escape(str(score))}")
    if trade_id: parts.append(f"🔖 Trade ID: {html_escape(str(trade_id))}")
    parts.append(f"📌 Setup: {safe_reason}")
    send_once("\n".join(parts), f"entry_{trade_id or symbol}", 5)

def tg_tp_hit(symbol, tp_level, pnl_pct=None, qty=None, fill_price=None, order_id=None,
              realized_pnl_usdt=None, pnl_source=None, side=None, entry_price=None):
    level = int(tp_level)
    icon = "🥇" if level == 1 else "🥈"
    parts = [f"{icon} <b>TP{level} EXECUTION VERIFIED</b>", f"📊 {html_escape(str(symbol))}"]
    if side: parts.append(f"↕️ Side: {html_escape(str(side))}")
    if entry_price and safe_float(entry_price) > 0: parts.append(f"🎯 Entry: {safe_float(entry_price):.8f}")
    if fill_price and safe_float(fill_price) > 0: parts.append(f"💱 Average fill: {safe_float(fill_price):.8f}")
    if realized_pnl_usdt is not None:
        parts.append(f"💵 Realized PnL: {safe_float(realized_pnl_usdt):+.4f} USDT")
        if pnl_pct is not None: parts.append(f"📈 Realized ROE (allocated margin): {safe_float(pnl_pct):+.2f}%")
    else:
        parts.append("💵 Realized PnL: unavailable from exchange fill data")
    if qty is not None: parts.append(f"📦 Closed quantity: {safe_float(qty):.8f}")
    if pnl_source: parts.append(f"🧾 Source: {html_escape(str(pnl_source))}")
    if order_id: parts.append(f"🔖 Order: {html_escape(str(order_id))}")
    send_once("\n".join(parts), f"tp_{symbol}_{level}_{order_id or 'verified'}", 30)

def tg_sl_hit(symbol, pnl_pct=None, pnl_usdt=None, fill_price=None, order_id=None, pnl_source=None):
    parts = [f"🛑 <b>NATIVE STOP LOSS VERIFIED</b>", f"📊 {html_escape(str(symbol))}"]
    if pnl_usdt is not None:
        parts.append(f"💵 Realized PnL: {safe_float(pnl_usdt):+.4f} USDT")
        if pnl_pct is not None: parts.append(f"📉 Realized ROE: {safe_float(pnl_pct):+.2f}%")
    else:
        parts.append("💵 Realized PnL: unavailable from BingX trade history")
    if fill_price: parts.append(f"💱 Exit fill: {safe_float(fill_price):.8f}")
    if pnl_source: parts.append(f"🧾 Source: {html_escape(str(pnl_source))}")
    if order_id: parts.append(f"🔖 Order: {html_escape(str(order_id))}")
    send_once("\n".join(parts), f"sl_{symbol}_{order_id or 'verified'}", 30)

def tg_close(symbol, pnl_pct, duration_min, side, close_reason=None, pnl_usdt=None,
              entry_price=None, exit_price=None, pnl_source=None,
              total_qty=None, trade_id=None):
    reason = str(close_reason or "CLOSE").upper()
    if reason == "TP2": icon, title = "🥈", "TP2 VERIFIED · POSITION CLOSED"
    elif reason.startswith("RANGE_SCALP"): icon, title = "⚡", "RANGE SCALP · POSITION CLOSED"
    elif "STOP" in reason: icon, title = "🛑", "STOP / PROTECTIVE EXIT · POSITION CLOSED"
    elif "STRICT" in reason or "THESIS" in reason or "EMERGENCY" in reason: icon, title = "⛔", "STRICT EXIT VERIFIED · POSITION CLOSED"
    else: icon, title = "📋", "POSITION CLOSED"
    if pnl_usdt is None:
        outcome = "RESULT: PNL UNAVAILABLE — NOT CLASSIFIED"
    elif pnl_usdt > 0:
        outcome = "RESULT: PROFIT"
    elif pnl_usdt < 0:
        outcome = "RESULT: LOSS"
    else:
        outcome = "RESULT: BREAKEVEN"
    parts = [f"{icon} <b>{title}</b>", f"📊 {html_escape(str(symbol))} ({html_escape(str(side))})",
             f"<b>{outcome}</b>"]
    if pnl_usdt is not None:
        parts.append(f"💵 Total realized PnL: {safe_float(pnl_usdt):+.4f} USDT")
        if pnl_pct is not None: parts.append(f"📈 Total realized ROE: {safe_float(pnl_pct):+.2f}%")
    else: parts.append("💵 Total realized PnL: UNAVAILABLE — exchange data incomplete")
    if entry_price and safe_float(entry_price) > 0: parts.append(f"Entry: {safe_float(entry_price):.8f}")
    if exit_price and safe_float(exit_price) > 0: parts.append(f"Final exit fill: {safe_float(exit_price):.8f}")
    if total_qty is not None: parts.append(f"📦 Original position quantity: {safe_float(total_qty):.8f}")
    parts.extend([f"⏱ Duration: {safe_float(duration_min):.0f} min", f"Reason: {html_escape(reason)}"])
    if pnl_source: parts.append(f"🧾 Source: {html_escape(str(pnl_source))}")
    if trade_id: parts.append(f"🔖 Trade ID: {html_escape(str(trade_id))}")
    close_key = str(trade_id or (str(symbol) + "_" + reason))
    send_once("\n".join(parts), f"close_{close_key}", 1)

def tg_error(err_msg, error_type="EXECUTION"):
    send_once(f"🚨 <b>ERROR</b> [{html_escape(str(error_type))}]\n{html_escape(str(err_msg)[:200])}", f"err_{error_type}_{str(err_msg)[:50]}", 60)

# ========== CONFIGURATION ==========
API_KEY = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")
# Safe default: PAPER. LIVE requires explicit opt-in, explicit acknowledgement and credentials.
PAPER_MODE, MODE_LIVE, _MODE_REASON = resolve_trading_mode(os.environ)
LIVE_REQUESTED = not PAPER_MODE
print(f"[MODE] {'LIVE' if MODE_LIVE else 'PAPER'} | {_MODE_REASON}")

DEFAULT_SYMBOL = (os.getenv("SYMBOL", "BTC/USDT") or "").strip() or "BTC/USDT"
INTERVAL = os.getenv("INTERVAL", "15m")
try:
    LEVERAGE = int(os.getenv("LEVERAGE", "20"))
except (TypeError, ValueError):
    LEVERAGE = 20
if LEVERAGE != 20:
    print(f"[CONFIG] Requested LEVERAGE={LEVERAGE} rejected; GGP policy is fixed at 20x")
    LEVERAGE = 20  # Fixed policy: all LIVE entries require verified 20x on BingX.
try:
    MAX_TRADE_RISK_PCT = float(os.getenv("MAX_TRADE_RISK_PCT", "10.0"))
except (TypeError, ValueError):
    MAX_TRADE_RISK_PCT = 10.0
if not math.isfinite(MAX_TRADE_RISK_PCT) or not (0.1 <= MAX_TRADE_RISK_PCT <= 25.0):
    print("[RISK] Invalid MAX_TRADE_RISK_PCT; safe default 10.0% of target margin applied")
    MAX_TRADE_RISK_PCT = 10.0
try:
    STOP_EXECUTION_BUFFER_PCT = float(os.getenv("STOP_EXECUTION_BUFFER_PCT", "0.05"))
except (TypeError, ValueError):
    STOP_EXECUTION_BUFFER_PCT = 0.05
if not math.isfinite(STOP_EXECUTION_BUFFER_PCT) or not (0.0 <= STOP_EXECUTION_BUFFER_PCT <= 0.5):
    STOP_EXECUTION_BUFFER_PCT = 0.05


USE_PPE = False  # Legacy PPE is disabled; TradeManagementBoard is the only routine profit authority.

# Two-stage scanner architecture ported from RORO:
# 1) broad discovery every 20 minutes; 2) rotating proximity/institutional watchlist.
GLOBAL_SCAN_INTERVAL = _env_int_range("GLOBAL_SCAN_INTERVAL", 1200, 300, 86400)
SCANNER_V2_INTERVAL = GLOBAL_SCAN_INTERVAL  # compatibility alias; the broad scanner runs both RF and V2 once.
MICRO_SCAN_INTERVAL = 5
TOP_LIQUID_COUNT = 80
SCANNER_UNIVERSE_MAX = _env_int_range("SCANNER_UNIVERSE_MAX", 80, 30, 100)
INSTITUTIONAL_WATCHLIST_BATCH_SIZE = _env_int_range("INSTITUTIONAL_WATCHLIST_BATCH_SIZE", 12, 4, 24)
INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS = _env_int_range("INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS", 20, 10, 300)
INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS = _env_int_range("INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS", 120, 30, 150)

MAX_SPREAD_PERCENT_DEFAULT = 0.08
MAX_SPREAD_PERCENT_VOLATILE = 0.15

MAX_SCALE_INS = 0  # Disabled: legacy scale-ins bypass the total-position stop-risk allocation.
SCALE_IN_SIZE_PCT = 0.25
SCALE_IN_PROFIT_PCT = 0.5
RUNNER_PCT = 0.4
TRAIL_ATR_MULT = 1.4
ADVERSE_MOVE_ATR_MULT = 1.8
MAX_DAILY_LOSS_PCT = 5.0
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_MINUTES_LOSS = 10
COOLDOWN_MINUTES_DRAWDOWN = 20

SNAPSHOT_INTERVAL = 15
BASE_SLEEP = 5
KEEP_ALIVE_INTERVAL = 300
BALANCE_SAFETY_FACTOR = 1.0  # Allocation tiers are based on the exchange-reported available balance.
INSUFFICIENT_MARGIN_COOLDOWN_SEC = 60

SCAN_INTERVAL = 900
WATCHLIST_REFRESH = 300
RADAR_COOLDOWN_SEC = 1800
LAST_ENTRY_PER_SYMBOL = {}

INSUFFICIENT_MARGIN_COOLDOWN_UNTIL = None

ex = ccxt.bingx({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "enableRateLimit": True,
    # Keep one-shot foreground reconciliation reads bounded below the
    # watchdog grace window; slow reads become UNKNOWN and retry next cycle.
    "timeout": _env_int_range("EXCHANGE_HTTP_TIMEOUT_MS", 12000, 5000, 15000),
    "options": {"defaultType": "swap"}
})

def normalize_symbol(symbol):
    """Normalize to one USDT-margined swap symbol; invalid inputs return ''."""
    from bingx_protection import normalize_exchange_symbol
    return normalize_exchange_symbol(symbol)


def resolve_position_symbol(symbol, exchange=None):
    """Validate symbol metadata before any position read.

    Returns ``(symbol, reason, diagnostics)``. An invalid or unconfirmed symbol
    must stop the caller before ``fetch_positions`` is invoked.
    """
    from bingx_protection import validate_exchange_symbol
    target = exchange if exchange is not None else globals().get("ex")
    resolved, reason, diagnostics = validate_exchange_symbol(target, symbol)
    if not resolved:
        log_execution(
            f"[SYMBOL_GUARD] {reason}: original={diagnostics.get('original_symbol')!r} "
            f"final={diagnostics.get('normalized_symbol')!r} source={diagnostics.get('source')} "
            f"market_type={diagnostics.get('market_type')}",
            "ERROR", debounce_key=f"symbol_guard_{reason}", debounce_sec=30)
    return resolved, reason, diagnostics

def set_leverage(symbol, leverage, position_side=None):
    """Set and read back exactly 20x using the exchange's actual position mode."""
    if PAPER_MODE:
        return True
    sym = normalize_symbol(symbol)
    try:
        requested = int(leverage)
    except (TypeError, ValueError):
        requested = 0
    if requested != 20 or not callable(getattr(ex, "set_leverage", None)):
        log_execution(f"[LEVERAGE] Refusing LIVE entry: requested={leverage!r}; policy requires 20x and set_leverage support", "ERROR")
        return False
    fetch_method = getattr(ex, "fetch_leverage", None)
    if not callable(fetch_method):
        log_execution("[LEVERAGE] BingX fetch_leverage read-back is unavailable; LIVE entry blocked", "ERROR")
        return False
    try:
        mode_result = BingXPositionModeAdapter(ex).read(sym)
        if mode_result.mode is PositionMode.UNKNOWN:
            log_execution(f"[LEVERAGE] Position mode UNKNOWN for {sym}; LIVE entry blocked", "ERROR")
            return False
        if mode_result.mode is PositionMode.HEDGE and str(position_side or '').upper() not in {"LONG", "SHORT"}:
            log_execution(f"[LEVERAGE] Hedge mode requires LONG/SHORT side for {sym}; LIVE entry blocked", "ERROR")
            return False
        lev_params = leverage_params(mode_result.mode, str(position_side or "LONG"))
        safe_api_call(ex.set_leverage, 20, sym, lev_params)
        try:
            readback = safe_api_call(fetch_method, sym, lev_params)
        except TypeError:
            readback = safe_api_call(fetch_method, sym)
        observed = extract_leverage_value(readback, position_side=(position_side if mode_result.mode is PositionMode.HEDGE else None))
        if observed is None or abs(float(observed) - 20.0) > 1e-9:
            log_execution(f"[LEVERAGE] BingX read-back mismatch for {sym}: requested=20x observed={observed!r}; LIVE entry blocked", "ERROR")
            return False
        log_execution(f"[LEVERAGE] Verified BingX leverage=20x for {sym} ({mode_result.mode.value}/{lev_params['side']})", "SUCCESS")
        return True
    except Exception as exc:
        log_execution(f"[LEVERAGE] Cannot set and verify 20x for {sym}: {type(exc).__name__}: {exc}; LIVE entry blocked", "ERROR")
        return False

# ========== LIVE HYBRID DATAFRAME ==========
_live_high = {}
_live_low = {}
_last_candle_timestamp = {}

def get_live_hybrid_df(symbol, base_df: pd.DataFrame, live_price: float) -> pd.DataFrame:
    """
    Merges the last closed candle with live ticker data.
    Updates high/low/close for the current (incomplete) candle.
    Resets _live_high/_live_low when a new candle starts.
    """
    if base_df is None or base_df.empty or live_price is None or live_price <= 0:
        return base_df

    df = base_df.copy()
    last_idx = df.index[-1]
    if 'timestamp' in df.columns:
        current_ts = df.loc[last_idx, 'timestamp']
    else:
        current_ts = last_idx

    global _last_candle_timestamp, _live_high, _live_low
    prev_ts = _last_candle_timestamp.get(symbol)
    if prev_ts is None or current_ts != prev_ts:
        _last_candle_timestamp[symbol] = current_ts
        _live_high[symbol] = df.loc[last_idx, 'high']
        _live_low[symbol] = df.loc[last_idx, 'low']
    else:
        _live_high[symbol] = max(_live_high.get(symbol, df.loc[last_idx, 'high']), live_price)
        _live_low[symbol] = min(_live_low.get(symbol, df.loc[last_idx, 'low']), live_price)

    df.loc[last_idx, 'high'] = _live_high[symbol]
    df.loc[last_idx, 'low'] = _live_low[symbol]
    df.loc[last_idx, 'close'] = live_price
    return df

# ========== DATA FETCHING (OPTIMIZED CACHE) ==========
OHLCV_FETCH_DIAGNOSTICS = {}

def _record_ohlcv_fetch_diagnostic(symbol, reason, *, received=0, detail=""):
    key = str(symbol or "")
    OHLCV_FETCH_DIAGNOSTICS[key] = {
        "reason": str(reason),
        "received_candles": int(received or 0),
        "detail": str(detail or "")[:180],
        "timestamp": time.time(),
        "timeframe": str(INTERVAL),
    }


def fetch_ohlcv(symbol, limit=150):
    started_at = time.time()
    sym = str(symbol or "")
    try:
        sym = normalize_symbol(symbol)
        cooldown = _ohlcv_rate_limit_cooldown_remaining(sym)
        if cooldown > 0:
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_RATE_LIMITED",
                detail=f"endpoint cooldown active for {cooldown:.2f}s; no request sent",
            )
            return None
        # Position management is a latency-critical foreground responsibility.
        # One physical read per cycle is safer than exhausting the watchdog budget
        # with retries; the next management cycle performs the next reconciliation.
        api_attempts = 1 if (STATE.get("open") or TRADE_STATE.get("in_position")) else None
        data = safe_api_call(ex.fetch_ohlcv, sym, INTERVAL, limit=limit,
                             _api_max_attempts=api_attempts)
        if not data:
            rate = _LAST_API_RATE_LIMIT
            rate_limited = (
                rate.get("method") == "fetch_ohlcv" and float(rate.get("ts", 0.0)) >= started_at
            )
            if rate_limited:
                _mark_ohlcv_rate_limit_cooldown(sym)
            reason = "OHLCV_RATE_LIMITED" if rate_limited else "OHLCV_EMPTY"
            _record_ohlcv_fetch_diagnostic(sym, reason, detail="exchange returned no OHLCV rows")
            return None
        if len(data) < 100:
            _record_ohlcv_fetch_diagnostic(
                sym, "INSUFFICIENT_CLOSED_BARS", received=len(data),
                detail=f"fetcher minimum is 100 rows; received {len(data)}",
            )
            return None
        try:
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        except (TypeError, ValueError):
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(data), detail="invalid OHLCV row shape",
            )
            return None
        for col in ["timestamp", "open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if df.isna().any().any():
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(df), detail="missing or non-numeric OHLCV value",
            )
            return None
        numeric = df[["timestamp", "open", "high", "low", "close", "volume"]].to_numpy(dtype=float)
        if not np.isfinite(numeric).all():
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(df), detail="non-finite OHLCV value",
            )
            return None
        timestamps = df["timestamp"].to_numpy(dtype=float)
        if ((timestamps <= 0).any()
                or not np.equal(timestamps, np.floor(timestamps)).all()
                or not df["timestamp"].is_monotonic_increasing
                or df["timestamp"].duplicated().any()):
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(df),
                detail="timestamps must be positive integer milliseconds, strictly increasing and unique",
            )
            return None
        if (df["volume"] < 0).any() or (df[["open", "high", "low", "close"]] <= 0).any().any():
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(df), detail="non-positive price or negative volume",
            )
            return None
        malformed = (
            (df["high"] < df[["open", "close", "low"]].max(axis=1))
            | (df["low"] > df[["open", "close", "high"]].min(axis=1))
        )
        if bool(malformed.any()):
            _record_ohlcv_fetch_diagnostic(
                sym, "OHLCV_MALFORMED", received=len(df),
                detail=f"{int(malformed.sum())} rows violate OHLC bounds",
            )
            return None
        df = df.reset_index(drop=True)
        _record_ohlcv_fetch_diagnostic(sym, "OK", received=len(df))
        return df
    except Exception as e:
        message = str(e).lower()
        scheduler_contention = isinstance(e, ApiRateLockTimeout) or any(
            base.__name__ == "ApiRateLockTimeout" for base in type(e).__mro__
        )
        rate_limited = "rate limit" in message or "100410" in message
        circuit_open = "circuit open" in message or "probe already in flight" in message
        if rate_limited or circuit_open:
            _mark_ohlcv_rate_limit_cooldown(sym)
        if scheduler_contention:
            reason = "LOCAL_SCHEDULER_CONTENTION"
        else:
            reason = "OHLCV_RATE_LIMITED" if (rate_limited or circuit_open) else "OHLCV_EMPTY"
        _record_ohlcv_fetch_diagnostic(sym, reason, detail=f"{type(e).__name__}: {e}")
        print(color_text(f"fetch_ohlcv error for {symbol}: {e}", YELLOW))
        return None

def fetch_ohlcv_htf(symbol, timeframe='1h', limit=200):
    try:
        sym = normalize_symbol(symbol)
        api_attempts = 1 if (STATE.get("open") or TRADE_STATE.get("in_position")) else None
        data = safe_api_call(ex.fetch_ohlcv, sym, timeframe, limit=limit,
                             _api_max_attempts=api_attempts)
        if not data or len(data) < 30:
            return None
        try:
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        except (TypeError, ValueError):
            return None
        for col in ["timestamp", "open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if df.isna().any().any():
            return None
        numeric = df[["timestamp", "open", "high", "low", "close", "volume"]].to_numpy(dtype=float)
        if not np.isfinite(numeric).all():
            return None
        timestamps = df["timestamp"].to_numpy(dtype=float)
        if ((timestamps <= 0).any()
                or not np.equal(timestamps, np.floor(timestamps)).all()
                or not df["timestamp"].is_monotonic_increasing
                or df["timestamp"].duplicated().any()):
            return None
        if ((df["volume"] < 0).any()
                or (df[["open", "high", "low", "close"]] <= 0).any().any()
                or (df["high"] < df[["open", "close", "low"]].max(axis=1)).any()
                or (df["low"] > df[["open", "close", "high"]].min(axis=1)).any()):
            return None
        return df.reset_index(drop=True)
    except Exception:
        return None

def fetch_ticker(symbol):
    api_attempts = 1 if (STATE.get("open") or TRADE_STATE.get("in_position")) else None
    return safe_api_call(ex.fetch_ticker, normalize_symbol(symbol),
                         _api_max_attempts=api_attempts)

def fetch_orderbook(symbol, limit=20):
    return safe_api_call(ex.fetch_order_book, normalize_symbol(symbol), limit)

def get_balance(_api_max_attempts=None):
    if PAPER_MODE:
        return paper["balance"]
    bal = safe_api_call(ex.fetch_balance, _api_max_attempts=_api_max_attempts)
    if bal:
        return bal.get("total", {}).get("USDT", 0.0)
    return 0.0

def get_free_balance():
    if PAPER_MODE:
        return paper["balance"]
    bal = safe_api_call(ex.fetch_balance)
    if bal:
        return bal.get("free", {}).get("USDT", 0.0)
    return 0.0

def get_spread_bps(symbol):
    try:
        ob = get_orderbook_cached(symbol, 5)
        if ob and ob['asks'] and ob['bids']:
            ask = ob['asks'][0][0]
            bid = ob['bids'][0][0]
            return (ask - bid) / bid * 100
    except:
        pass
    return 100.0

def validate_dataframe(df, min_length=100):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    required = ["open", "high", "low", "close", "volume"]
    if not all(c in df.columns for c in required):
        return False
    if df[required].iloc[-min_length:].isna().any().any():
        return False
    if (df['close'].iloc[-min_length:] == 0).any():
        return False
    if df['close'].iloc[-min_length:].std() < 1e-8:
        return False
    return True

def get_ohlcv_safe(symbol, limit=120, htf=False):
    return get_ohlcv_safe_meta(symbol, limit, htf=htf)[0]


def get_ohlcv_safe_meta(symbol, limit=120, htf=False, *, force_refresh=False):
    """Return (frame, source) from one canonical, single-flight OHLCV snapshot.

    `source` is CACHE_HIT or FRESH_REQUEST (or NONE). A CACHE_HIT means the
    caller served a shared snapshot without sending a new BingX request.
    Caller-specific limits only slice the canonical frame; they never create a
    second exchange request for the same symbol/timeframe.
    """
    ttl = 10 if (STATE.get("open") or TRADE_STATE["in_position"]) else 30
    if htf:
        ttl = max(ttl, 30)
    canonical_symbol, cache_key, timeframe = _ohlcv_canonical_cache_key(symbol, limit, htf=htf)
    if force_refresh:
        with CACHE_LOCK:
            CACHE.pop(cache_key, None)

    cached = _ohlcv_cache_get(cache_key, ttl, timeframe)
    if cached is not None and len(cached) >= 100:
        return cached.tail(max(1, int(limit or OHLCV_CACHE_LIMIT))).copy(), "CACHE_HIT"

    fetch_lock = _ohlcv_inflight_lock(cache_key)
    with fetch_lock:
        # Double-check after waiting so concurrent misses coalesce into one fetch.
        cached = _ohlcv_cache_get(cache_key, ttl, timeframe)
        if cached is not None and len(cached) >= 100:
            return cached.tail(max(1, int(limit or OHLCV_CACHE_LIMIT))).copy(), "CACHE_HIT"
        if htf:
            df = fetch_ohlcv_htf(canonical_symbol, timeframe, OHLCV_CACHE_LIMIT)
        else:
            df = fetch_ohlcv(canonical_symbol, OHLCV_CACHE_LIMIT)
        if df is not None and validate_dataframe(df, min(OHLCV_CACHE_LIMIT, 100)):
            _ohlcv_cache_set(cache_key, df, timeframe)
            return df.tail(max(1, int(limit or OHLCV_CACHE_LIMIT))).copy(), "FRESH_REQUEST"
        return None, "NONE"

def get_ticker_safe(symbol):
    # Use short TTL for live price
    cached = cache_get(f"ticker_{symbol}", 1)
    if cached is not None:
        return cached
    ticker = fetch_ticker(symbol)
    if ticker:
        price = ticker["last"]
        if price and price > 0:
            cache_set(f"ticker_{symbol}", price)
            return price
    return None

def get_balance_safe(*, api_max_attempts=None):
    cached = cache_get("balance", 5)  # 5 seconds
    if cached is not None:
        return cached
    bal = get_balance(_api_max_attempts=api_max_attempts)
    cache_set("balance", bal)
    return bal

def get_free_balance_safe():
    cached = cache_get("free_balance", 5)
    if cached is not None:
        return cached
    bal = get_free_balance()
    cache_set("free_balance", bal)
    return bal


def get_available_balance_snapshot(max_age_sec=15.0):
    """Return (balance, source, timestamp) for the real BingX available balance.

    "free" USDT from BingX Futures is the authoritative available balance. The
    returned timestamp is the moment the value was fetched, so callers can prove
    freshness. A value older than `max_age_sec` is refetched rather than reused.
    In PAPER mode the simulated balance is returned with source "PAPER".
    """
    now = time.time()
    if PAPER_MODE:
        return safe_float(paper.get("balance"), 0.0), "PAPER", now
    cached = cache_get("free_balance", max_age_sec)
    if cached is not None:
        item = CACHE.get("free_balance") if isinstance(CACHE, dict) else None
        ts = float(item.get("ts", now)) if isinstance(item, dict) else now
        return safe_float(cached), "BINGX_FUTURES_FREE_USDT", ts
    bal = get_free_balance()
    cache_set("free_balance", bal)
    return safe_float(bal), "BINGX_FUTURES_FREE_USDT", time.time()

def get_orderbook_cached(symbol, limit=20):
    # During live trade, avoid fetching orderbook to save API calls
    if STATE.get("open") or TRADE_STATE["in_position"]:
        cached = cache_get(f"ob_{symbol}_{limit}", 60)  # long TTL
        if cached is not None:
            return cached
        return None  # don't fetch during trade
    cached = cache_get(f"ob_{symbol}_{limit}", 1)
    if cached is not None:
        return cached
    ob = fetch_orderbook(symbol, limit)
    if ob:
        cache_set(f"ob_{symbol}_{limit}", ob)
    return ob

# ========== EXCHANGE POSITION SYNC ==========
def fetch_position(symbol):
    global LAST_POSITION_FETCH_STATUS
    if PAPER_MODE:
        return None
    try:
        sym, reason, _diagnostics = resolve_position_symbol(symbol)
        if not sym:
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return None
        if hasattr(ex, 'fetch_positions'):
            positions = safe_api_call(ex.fetch_positions, [sym])
        elif hasattr(ex, 'fetch_open_positions'):
            positions = safe_api_call(ex.fetch_open_positions, [sym])
        else:
            return None
        if not positions:
            return None
        for pos in positions:
            pos_sym = pos.get('symbol', '')
            if sym in pos_sym and float(pos.get('contracts', 0)) > 0:
                return pos
        return None
    except Exception as e:
        log_execution(f"[POS_SYNC] fetch_position error: {e}", "ERROR")
        return None

def get_mark_price(symbol):
    if PAPER_MODE:
        return get_ticker_safe(symbol)
    pos = fetch_position(symbol)
    if pos and 'markPrice' in pos and pos['markPrice']:
        return float(pos['markPrice'])
    return get_ticker_safe(symbol)

# ========== TRADE THESIS ENGINE ==========
@dataclass
class TradeThesis:
    thesis_id: str
    symbol: str
    side: str
    trade_type: str
    created_at: float
    entry_reason: List[str] = field(default_factory=list)
    continuation_factors: List[str] = field(default_factory=list)
    invalidation_factors: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    market_context: Dict = field(default_factory=dict)
    confidence: float = 0.0
    continuation_probability: float = 0.5
    exhaustion_probability: float = 0.0
    thesis_strength: float = 0.0
    current_status: str = "ACTIVE"
    last_update: float = field(default_factory=time.time)

class TradeThesisEngine:
    def build_thesis(self, symbol: str, side: str, trade_type: str, market_state: Dict,
                     narrative: Dict, entry_context: Dict) -> TradeThesis:
        reasons = []
        continuation = []
        invalidation = []
        risks = []
        adx = market_state.get("adx", 0)
        regime = market_state.get("regime", "UNKNOWN")
        continuation_probability = 0.5
        if adx > 25:
            reasons.append("strong_trend_environment")
            continuation.append("adx_expansion")
            continuation_probability += 0.1
        if market_state.get("di_dominance", False):
            reasons.append("di_dominance")
            continuation.append("persistent_pressure")
            continuation_probability += 0.1
        if market_state.get("weak_pullback", False):
            reasons.append("weak_pullback")
            continuation.append("counter_move_weakness")
            continuation_probability += 0.1
        if market_state.get("structure_aligned", False):
            reasons.append("market_structure_alignment")
            continuation_probability += 0.1
        narrative_class = narrative.get("classification", "NEUTRAL")
        if narrative_class in ("TREND_CONTINUATION", "INSTITUTIONAL_CONTINUATION"):
            reasons.append("institutional_narrative_alignment")
            continuation_probability += 0.1
        if adx > 45:
            risks.append("trend_exhaustion_risk")
        if market_state.get("counter_displacement", 0) > 1.0:
            risks.append("counter_displacement_risk")
        if regime == "CHOP":
            risks.append("choppy_environment")
        invalidation.extend(["ema_loss", "di_flip", "failed_continuation", "vwap_reclaim", "strong_counter_displacement"])
        confidence = min(continuation_probability, 0.95)
        thesis_strength = (len(reasons) * 1.2 + len(continuation) * 1.5 - len(risks) * 0.8)
        thesis = TradeThesis(
            thesis_id=f"{symbol}_{int(time.time())}",
            symbol=symbol,
            side=side,
            trade_type=trade_type,
            created_at=time.time(),
            entry_reason=reasons,
            continuation_factors=continuation,
            invalidation_factors=invalidation,
            risk_factors=risks,
            market_context=market_state,
            confidence=round(confidence, 2),
            continuation_probability=round(continuation_probability, 2),
            exhaustion_probability=0.0,
            thesis_strength=round(thesis_strength, 2)
        )
        return thesis

    def update_thesis(self, thesis: TradeThesis, market_state: Dict) -> TradeThesis:
        continuation_prob = thesis.continuation_probability
        exhaustion_prob = thesis.exhaustion_probability
        trend_health = market_state.get("trend_health", 5)
        if trend_health >= 7:
            continuation_prob += 0.05
        elif trend_health <= 3:
            continuation_prob -= 0.1
        adx_slope = market_state.get("adx_slope", 0)
        if adx_slope > 0:
            continuation_prob += 0.05
        else:
            continuation_prob -= 0.03
        counter_displacement = market_state.get("counter_displacement", 0)
        if counter_displacement > 1.2:
            continuation_prob -= 0.15
            exhaustion_prob += 0.2
        if market_state.get("weak_pullback", False):
            continuation_prob += 0.08
        continuation_prob = max(0.0, min(1.0, continuation_prob))
        exhaustion_prob = max(0.0, min(1.0, exhaustion_prob))
        thesis.continuation_probability = round(continuation_prob, 2)
        thesis.exhaustion_probability = round(exhaustion_prob, 2)
        thesis.last_update = time.time()
        return thesis

_thesis_engine = TradeThesisEngine()

# ========== REJECTION INTELLIGENCE ENGINE ==========
class RejectionIntelligence:
    @staticmethod
    def is_bearish_rejection(df, atr, zone_price=None):
        if len(df) < 1:
            return False, []
        last = df.iloc[-1]
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        if range_ == 0:
            return False, []
        upper_wick = last['high'] - max(last['open'], last['close'])
        wick_condition = upper_wick >= 1.5 * body
        close_near_low = (last['close'] - last['low']) / range_ <= 0.3
        zone_failure = False
        if zone_price is not None:
            zone_failure = last['close'] < zone_price
        if len(df) >= 3:
            prev2 = df.iloc[-2]
            prev3 = df.iloc[-3]
            weak_continuation = (prev2['close'] < prev2['open'] or prev3['close'] < prev3['open'])
        else:
            weak_continuation = False
        vol_state = classify_volume(df)
        volume_ok = vol_state in ("expansion", "spike") and df['volume'].iloc[-1] < df['volume'].rolling(20).mean().iloc[-1] * 1.2
        di_plus, di_minus, _, _ = get_di_components(df)
        di_ok = (di_minus is not None and di_plus is not None and di_minus > di_plus and (di_minus - di_plus) > 2)
        is_shooting_star = (body / range_ <= 0.3 and upper_wick >= 2 * body and last['close'] < last['open'])
        is_bearish_engulfing = False
        if len(df) >= 2:
            prev = df.iloc[-2]
            is_bearish_engulfing = (prev['close'] > prev['open'] and last['close'] < last['open'] and
                                     last['high'] > prev['high'] and last['low'] < prev['low'])
        reasons = []
        score = 0
        if wick_condition:
            score += 2
            reasons.append("long_upper_wick")
        if close_near_low:
            score += 1
            reasons.append("close_near_low")
        if zone_failure:
            score += 2
            reasons.append("zone_failure")
        if weak_continuation:
            score += 1
            reasons.append("weak_continuation")
        if volume_ok:
            score += 1
            reasons.append("volume_absorption")
        if di_ok:
            score += 2
            reasons.append("di_dominance_sell")
        if is_shooting_star:
            score += 1.5
            reasons.append("shooting_star")
        if is_bearish_engulfing:
            score += 2
            reasons.append("bearish_engulfing")
        is_valid = score >= 5
        return is_valid, reasons

    @staticmethod
    def is_bullish_rejection(df, atr, zone_price=None):
        if len(df) < 1:
            return False, []
        last = df.iloc[-1]
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        if range_ == 0:
            return False, []
        lower_wick = min(last['open'], last['close']) - last['low']
        wick_condition = lower_wick >= 1.5 * body
        close_near_high = (last['high'] - last['close']) / range_ <= 0.3
        zone_failure = False
        if zone_price is not None:
            zone_failure = last['close'] > zone_price
        if len(df) >= 3:
            prev2 = df.iloc[-2]
            prev3 = df.iloc[-3]
            weak_continuation = (prev2['close'] > prev2['open'] or prev3['close'] > prev3['open'])
        else:
            weak_continuation = False
        vol_state = classify_volume(df)
        volume_ok = vol_state in ("expansion", "spike") and df['volume'].iloc[-1] < df['volume'].rolling(20).mean().iloc[-1] * 1.2
        di_plus, di_minus, _, _ = get_di_components(df)
        di_ok = (di_plus is not None and di_minus is not None and di_plus > di_minus and (di_plus - di_minus) > 2)
        is_hammer = (body / range_ <= 0.3 and lower_wick >= 2 * body and last['close'] > last['open'])
        is_bullish_engulfing = False
        if len(df) >= 2:
            prev = df.iloc[-2]
            is_bullish_engulfing = (prev['close'] < prev['open'] and last['close'] > last['open'] and
                                     last['high'] > prev['high'] and last['low'] < prev['low'])
        reasons = []
        score = 0
        if wick_condition:
            score += 2
            reasons.append("long_lower_wick")
        if close_near_high:
            score += 1
            reasons.append("close_near_high")
        if zone_failure:
            score += 2
            reasons.append("zone_failure")
        if weak_continuation:
            score += 1
            reasons.append("weak_continuation")
        if volume_ok:
            score += 1
            reasons.append("volume_absorption")
        if di_ok:
            score += 2
            reasons.append("di_dominance_buy")
        if is_hammer:
            score += 1.5
            reasons.append("hammer")
        if is_bullish_engulfing:
            score += 2
            reasons.append("bullish_engulfing")
        is_valid = score >= 5
        return is_valid, reasons

# ========== MSS / CHOCH VALIDATION ==========
class MSSValidator:
    @staticmethod
    def validate_structure_shift(df, side, atr):
        if len(df) < 10:
            return False, [], 0
        last = df.iloc[-1]
        prev = df.iloc[-2]
        body = abs(last['close'] - last['open'])
        prev_body = abs(prev['close'] - prev['open'])
        displacement_strength = body / (prev_body + 1e-9) if prev_body > 0 else 1.0
        body_expansion = displacement_strength >= 1.5
        follow_through = False
        if len(df) >= 3:
            next_candle = df.iloc[-1]
            if side == "BUY":
                follow_through = next_candle['close'] > next_candle['open'] and body > prev_body
            else:
                follow_through = next_candle['close'] < next_candle['open'] and body > prev_body
        else:
            follow_through = True
        di_plus, di_minus, adx, adx_slope = get_di_components(df)
        di_spread = abs(di_plus - di_minus) if di_plus is not None and di_minus is not None else 0
        vol_state = classify_volume(df)
        volume_ok = vol_state in ("expansion", "spike")
        adx_acc = adx_slope > 0 and adx > 25
        score = 0
        reasons = []
        if body_expansion:
            score += 2
            reasons.append("body_expansion")
        if follow_through:
            score += 2
            reasons.append("follow_through")
        if di_spread > 8:
            score += 2
            reasons.append("di_spread_strong")
        if volume_ok:
            score += 1
            reasons.append("volume_confirm")
        if adx_acc:
            score += 2
            reasons.append("adx_accelerating")
        is_valid = score >= 5
        if adx < 20:
            is_valid = False
            reasons.append("adx_too_low")
        if not body_expansion and not follow_through:
            is_valid = False
            reasons.append("weak_displacement")
        return is_valid, reasons, score

# ========== ADX + DI INTELLIGENCE ==========
class ADXDIIntelligence:
    @staticmethod
    def get_adx_state(df):
        adx_series = compute_adx(df)
        if adx_series is None or len(adx_series) < 3:
            return {"state": "UNKNOWN", "value": 20, "slope": 0, "acceleration": 0}
        adx_val = adx_series.iloc[-1]
        adx_prev = adx_series.iloc[-2]
        adx_prev2 = adx_series.iloc[-3] if len(adx_series) >= 3 else adx_prev
        slope = adx_val - adx_prev
        accel = slope - (adx_prev - adx_prev2)
        if adx_val < 18:
            state = "CHOP"
        elif 18 <= adx_val < 22:
            state = "EMERGING"
        elif 22 <= adx_val < 35:
            state = "STRONG_TREND"
        elif 35 <= adx_val < 45:
            state = "VERY_STRONG"
        else:
            state = "EXHAUSTION"
        return {"state": state, "value": adx_val, "slope": slope, "acceleration": accel}

    @staticmethod
    def get_di_state(df):
        plus_di, minus_di, _, _ = get_di_components(df)
        if plus_di is None or minus_di is None:
            return {"dominant": "NEUTRAL", "spread": 0, "trend": "NEUTRAL"}
        spread = plus_di - minus_di
        if spread > 5:
            dominant = "BUY"
            trend = "BULLISH"
        elif spread < -5:
            dominant = "SELL"
            trend = "BEARISH"
        else:
            dominant = "NEUTRAL"
            trend = "CHOP"
        return {"dominant": dominant, "spread": spread, "trend": trend}

    @staticmethod
    def is_healthy_trend(df, side):
        adx_state = ADXDIIntelligence.get_adx_state(df)
        di_state = ADXDIIntelligence.get_di_state(df)
        if side == "BUY":
            return adx_state["state"] in ("STRONG_TREND", "VERY_STRONG") and di_state["dominant"] == "BUY" and adx_state["slope"] > 0
        else:
            return adx_state["state"] in ("STRONG_TREND", "VERY_STRONG") and di_state["dominant"] == "SELL" and adx_state["slope"] > 0

# ========== CONTINUATION PRESSURE ENGINE ==========
class ContinuationPressureEngine:
    @staticmethod
    def calculate_pressure(df, side, entry_price, atr, entry_time):
        if len(df) < 3:
            return 50, []
        score = 50
        reasons = []
        bodies = [abs(df['close'].iloc[-i] - df['open'].iloc[-i]) for i in range(1, 4)]
        if len(bodies) >= 2:
            growth = bodies[0] / (bodies[1] + 1e-9)
            if growth > 1.2:
                score += 10
                reasons.append("body_expansion")
            elif growth < 0.8:
                score -= 10
                reasons.append("body_contraction")
        adx_state = ADXDIIntelligence.get_adx_state(df)
        di_state = ADXDIIntelligence.get_di_state(df)
        if side == "BUY" and adx_state["slope"] > 0 and di_state["dominant"] == "BUY":
            score += 15
            reasons.append("adx_rising_di_bullish")
        elif side == "SELL" and adx_state["slope"] > 0 and di_state["dominant"] == "SELL":
            score += 15
            reasons.append("adx_rising_di_bearish")
        elif adx_state["slope"] <= 0:
            score -= 10
            reasons.append("adx_falling")
        if di_state["spread"] > 10:
            score += 10
            reasons.append("di_spread_wide")
        elif abs(di_state["spread"]) < 4:
            score -= 10
            reasons.append("di_tangled")
        vol_state = classify_volume(df)
        if vol_state == "expansion":
            score += 15
            reasons.append("volume_expansion")
        elif vol_state == "exhaustion":
            score -= 15
            reasons.append("volume_exhaustion")
        last_close = df['close'].iloc[-1]
        if side == "BUY" and last_close < entry_price:
            score -= 10
            reasons.append("price_below_entry")
        elif side == "SELL" and last_close > entry_price:
            score -= 10
            reasons.append("price_above_entry")
        consecutive = 0
        for i in range(1, min(5, len(df))):
            if side == "BUY" and df['close'].iloc[-i] > df['open'].iloc[-i]:
                consecutive += 1
            elif side == "SELL" and df['close'].iloc[-i] < df['open'].iloc[-i]:
                consecutive += 1
            else:
                break
        if consecutive >= 3:
            score += 10
            reasons.append(f"consecutive_{consecutive}")
        elif consecutive == 0:
            score -= 5
            reasons.append("no_follow_through")
        score = max(0, min(100, score))
        return score, reasons

# ========== THESIS FAILURE ENGINE ==========
class ThesisFailureEngine:
    @staticmethod
    def evaluate_failure(thesis: Dict, market_state: Dict, current_price, entry_price, side):
        if not thesis:
            return False, [], 0
        failure_score = 0
        reasons = []
        if market_state.get("strong_reclaim", False):
            failure_score += 30
            reasons.append("strong_reclaim")
        di_state = ADXDIIntelligence.get_di_state(market_state.get("df", None))
        if side == "BUY" and di_state.get("dominant") == "SELL":
            failure_score += 25
            reasons.append("di_flip_bearish")
        elif side == "SELL" and di_state.get("dominant") == "BUY":
            failure_score += 25
            reasons.append("di_flip_bullish")
        adx_state = ADXDIIntelligence.get_adx_state(market_state.get("df", None))
        if adx_state.get("state") == "CHOP" and adx_state.get("value") < 18:
            failure_score += 20
            reasons.append("adx_collapse")
        last_candle = market_state.get("last_candle", {})
        if side == "SELL" and last_candle.get("close", 0) > last_candle.get("open", 0):
            body = abs(last_candle.get("close",0)-last_candle.get("open",0))
            if body > market_state.get("atr", 0) * 0.6:
                failure_score += 20
                reasons.append("strong_bullish_candle")
        elif side == "BUY" and last_candle.get("close", 0) < last_candle.get("open", 0):
            body = abs(last_candle.get("close",0)-last_candle.get("open",0))
            if body > market_state.get("atr", 0) * 0.6:
                failure_score += 20
                reasons.append("strong_bearish_candle")
        continuation_pressure = market_state.get("continuation_pressure", 50)
        if continuation_pressure < 30:
            failure_score += 25
            reasons.append("low_continuation_pressure")
        sl_distance = abs(current_price - thesis.get("sl", entry_price)) / entry_price
        if sl_distance < 0.005:
            failure_score += 15
            reasons.append("sl_too_close")
        failed = failure_score >= 50
        return failed, reasons, failure_score

# ========== MARKET REGIME CLASSIFIER ==========
class MarketRegimeClassifier:
    @staticmethod
    def classify(df, ob=None):
        if df is None or len(df) < 50:
            return "UNKNOWN"
        adx_state = ADXDIIntelligence.get_adx_state(df)
        di_state = ADXDIIntelligence.get_di_state(df)
        atr = compute_atr(df).iloc[-1]
        price = df['close'].iloc[-1]
        atr_pct = (atr / price) * 100
        vol_state = classify_volume(df)
        range_20 = (df['high'].rolling(20).max() - df['low'].rolling(20).min()).iloc[-1]
        range_pct = (range_20 / price) * 100
        ema20 = ema(df['close'], 20).iloc[-1]
        ema50 = ema(df['close'], 50).iloc[-1]
        price_above_ema = price > ema20 and ema20 > ema50
        price_below_ema = price < ema20 and ema20 < ema50
        bos_up, bos_down = detect_bos(df, lookback=5)
        struct_shift = detect_structure_shift(df)
        if adx_state["state"] in ("STRONG_TREND", "VERY_STRONG") and di_state["dominant"] != "NEUTRAL":
            if (price_above_ema and di_state["dominant"] == "BUY") or (price_below_ema and di_state["dominant"] == "SELL"):
                if atr_pct > 2.0:
                    return "EXPANSION"
                else:
                    return "STRONG_TREND"
        if adx_state["state"] == "EMERGING" and adx_state["slope"] > 0:
            return "WEAK_TREND"
        if adx_state["value"] < 18 or di_state["dominant"] == "NEUTRAL":
            if range_pct < 1.5:
                return "COMPRESSION"
            else:
                return "CHOPPY"
        if vol_state == "expansion" and adx_state["value"] > 25:
            return "EXPANSION"
        if vol_state == "exhaustion" and adx_state["value"] > 30:
            return "DISTRIBUTION"
        if (struct_shift == "bullish_shift" and bos_up) or (struct_shift == "bearish_shift" and bos_down):
            return "TRANSITION"
        if vol_state == "absorption":
            return "ACCUMULATION"
        return "RANGE"

# ========== CONFIDENCE ENGINE ==========
class ConfidenceEngine:
    @staticmethod
    def calculate_initial_confidence(entry_score, narrative_score, regime, adx, di_spread, location_quality):
        base = (entry_score / 10) * 30 + (narrative_score / 10) * 30
        regime_map = {"STRONG_TREND": 20, "WEAK_TREND": 10, "EXPANSION": 25, "COMPRESSION": 5, "CHOPPY": 0, "ACCUMULATION": 15, "DISTRIBUTION": 10, "TRANSITION": 10}
        regime_bonus = regime_map.get(regime, 5)
        adx_bonus = min(20, max(0, (adx - 20) * 2))
        di_bonus = min(15, abs(di_spread))
        location_bonus = {"discount": 10, "premium": 10, "mid": 0}.get(location_quality, 0)
        total = base + regime_bonus + adx_bonus + di_bonus + location_bonus
        return min(100, total)

    @staticmethod
    def update_live_confidence(current_confidence, continuation_pressure, thesis_failure_score, adx_slope, di_spread_change):
        new_conf = current_confidence
        new_conf += (continuation_pressure - 50) * 0.3
        new_conf -= thesis_failure_score * 0.5
        new_conf += adx_slope * 2
        new_conf += di_spread_change * 1.5
        return max(0, min(100, new_conf))

    @staticmethod
    def apply_institutional_modifiers(base_confidence, smart_money, momentum, continuation_strength):
        conf = base_confidence
        if smart_money.get("smart_money_dominant", False):
            conf += 10
            log_execution("[CONF] Smart money dominant: +10", "INFO", debounce_key="conf_smart", debounce_sec=30)
        else:
            conf -= 8
        cont = min(100, max(0, continuation_strength))
        if cont > 20:
            conf += 8
        elif cont < 5:
            conf -= 10
        mom_health = momentum.get("momentum_health", 50)
        if mom_health > 15:
            conf += 6
        elif mom_health < 0:
            conf -= 8
        banker = smart_money.get("banker_pressure", 50)
        retail = smart_money.get("retailer_pressure", 50)
        if banker > retail:
            conf += 5
        else:
            conf -= 6
        dist = smart_money.get("distribution_risk", 0)
        if dist > 45:
            conf -= 12
        climax = momentum.get("climax_risk", 0)
        if climax > 50:
            conf -= 10
        conf = max(0, min(100, conf))
        return conf

# ========== PRECISION SAFETY ==========
class PrecisionSafety:
    @staticmethod
    def normalize_price(symbol, price):
        try:
            market = ex.market(normalize_symbol(symbol))
            prec = market['precision']['price']
            return round(price, prec)
        except:
            return price

    @staticmethod
    def normalize_amount(symbol, amount):
        try:
            market = ex.market(normalize_symbol(symbol))
            prec = market['precision']['amount']
            return math.floor(amount / (10 ** -prec)) * (10 ** -prec)
        except:
            return amount

    @staticmethod
    def adjust_sl_tp(symbol, entry, sl, tp, side, atr):
        min_dist = max(atr * 0.5, entry * 0.002)
        if side == "BUY":
            if entry - sl < min_dist:
                sl = entry - min_dist
            if tp - entry < min_dist:
                tp = entry + min_dist
        else:
            if sl - entry < min_dist:
                sl = entry + min_dist
            if entry - tp < min_dist:
                tp = entry - min_dist
        sl = PrecisionSafety.normalize_price(symbol, sl)
        tp = PrecisionSafety.normalize_price(symbol, tp)
        return sl, tp

# ========== CONTINUATION PROBABILITY ENGINE ==========
@dataclass
class ContinuationEvaluation:
    continuation_probability: float
    trend_strength: float
    exhaustion_probability: float
    reclaim_risk: float
    counter_pressure: float
    confidence: float
    reasons: List[str]
    should_hold: bool
    hold_quality: str

class ContinuationProbabilityEngine:
    HOLD_THRESHOLD = 0.62

    def evaluate(self, side: str, df, market_state: Dict, thesis: Dict) -> ContinuationEvaluation:
        score = 0.0
        reasons = []
        close = df["close"].iloc[-1]
        atr = market_state.get("atr", 0)
        adx = market_state.get("adx", 0)
        adx_slope = market_state.get("adx_slope", 0)
        di_plus = market_state.get("di_plus", 0)
        di_minus = market_state.get("di_minus", 0)
        trend_health = market_state.get("trend_health", 5)
        weak_pullback = market_state.get("weak_pullback", False)
        counter_displacement = market_state.get("counter_displacement", 0)
        volume_ratio = market_state.get("volume_ratio", 1.0)
        ema20 = df["close"].ewm(span=20).mean().iloc[-1]
        ema50 = df["close"].ewm(span=50).mean().iloc[-1]
        exhaustion_probability = 0.0
        reclaim_risk = 0.0
        counter_pressure = 0.0

        if side == "BUY":
            di_spread = di_plus - di_minus
        else:
            di_spread = di_minus - di_plus
        if di_spread > 8:
            score += 2.5
            reasons.append("strong_di_pressure")
        elif di_spread > 4:
            score += 1.5
            reasons.append("moderate_di_pressure")
        else:
            score -= 2.0
            reasons.append("weak_di_pressure")

        if adx > 25:
            score += 2.5
            reasons.append("healthy_adx")
        elif adx > 18:
            score += 1.0
            reasons.append("developing_adx")
        else:
            score -= 2.5
            reasons.append("dead_adx")
        if adx_slope > 0:
            score += 1.5
            reasons.append("adx_expanding")
        else:
            score -= 1.0
            reasons.append("adx_fading")

        if trend_health >= 8:
            score += 3.0
            reasons.append("excellent_trend_health")
        elif trend_health >= 6:
            score += 2.0
            reasons.append("healthy_trend")
        elif trend_health <= 3:
            score -= 3.0
            reasons.append("trend_breakdown")

        if weak_pullback:
            score += 2.0
            reasons.append("weak_pullback_detected")

        if counter_displacement > 1.5:
            counter_pressure += 0.5
            score -= 3.0
            reasons.append("strong_counter_pressure")
        elif counter_displacement > 0.8:
            counter_pressure += 0.25
            score -= 1.5
            reasons.append("moderate_counter_pressure")

        if side == "BUY":
            if close > ema20:
                score += 1.5
                reasons.append("holding_ema20")
            if close > ema50:
                score += 2.0
                reasons.append("holding_ema50")
            if close < ema20:
                reclaim_risk += 0.2
            if close < ema50:
                reclaim_risk += 0.4
        else:
            if close < ema20:
                score += 1.5
                reasons.append("holding_ema20")
            if close < ema50:
                score += 2.0
                reasons.append("holding_ema50")
            if close > ema20:
                reclaim_risk += 0.2
            if close > ema50:
                reclaim_risk += 0.4

        if atr > 0:
            extension = abs(close - ema20) / atr
            if extension > 3:
                exhaustion_probability += 0.5
                score -= 1.5
                reasons.append("overextended")
            elif extension > 2:
                exhaustion_probability += 0.25
                reasons.append("extended_move")

        if volume_ratio > 1.2:
            score += 1.5
            reasons.append("volume_confirmation")
        elif volume_ratio < 0.7:
            score -= 1.5
            reasons.append("weak_volume")

        thesis_strength = thesis.get("thesis_strength", 5)
        score += thesis_strength * 0.3

        smart_money = SmartMoneyEngine.analyze_smart_money(df)
        momentum = MomentumFlowEngine.analyze_momentum_flow(df)

        adx_strength = (adx - 18) / 22 * 100 if adx > 18 else 0
        structure_aligned = 1 if market_state.get("structure_aligned", False) else 0
        continuation_strength = (
            momentum.get("momentum_health", 50) * 0.35 +
            smart_money.get("banker_pressure", 50) * 0.25 +
            adx_strength * 0.25 +
            structure_aligned * 15
        )
        continuation_strength = max(0, min(100, continuation_strength))
        market_state["continuation_strength_scaled"] = continuation_strength
        score += (continuation_strength - 50) / 10

        dominance_weight = 0.7 if smart_money["smart_money_dominant"] else 0.3
        score += (dominance_weight - 0.5) * 6

        if momentum["trend_expansion"]:
            score += 2.0
            reasons.append("momentum_expansion")
        if momentum["momentum_decay"]:
            score -= 2.5
            reasons.append("momentum_decay")
        dist_risk = smart_money["distribution_risk"] / 100.0
        score -= dist_risk * 3.0
        if dist_risk > 0.7:
            reasons.append("high_distribution_risk")
        if smart_money["retail_euphoria"]:
            score -= 1.5
            reasons.append("retail_euphoria")

        probability = (score + 15) / 30
        probability = max(0.0, min(1.0, probability))
        confidence = min(abs(score) / 15, 1.0)
        should_hold = probability >= self.HOLD_THRESHOLD
        if probability >= 0.8:
            hold_quality = "STRONG"
        elif probability >= 0.65:
            hold_quality = "HEALTHY"
        elif probability >= 0.5:
            hold_quality = "NEUTRAL"
        else:
            hold_quality = "WEAK"

        return ContinuationEvaluation(
            continuation_probability=round(probability, 2),
            trend_strength=round(trend_health / 10, 2),
            exhaustion_probability=round(exhaustion_probability, 2),
            reclaim_risk=round(reclaim_risk, 2),
            counter_pressure=round(counter_pressure, 2),
            confidence=round(confidence, 2),
            reasons=reasons,
            should_hold=should_hold,
            hold_quality=hold_quality
        )

_continuation_engine = ContinuationProbabilityEngine()

# ========== LIVE TRADE MANAGEMENT SYSTEM ==========
class TradeLifecycleState(Enum):
    IDLE = "IDLE"
    OPEN_REQUESTED = "OPEN_REQUESTED"
    OPEN_PENDING_CONFIRMATION = "OPEN_PENDING_CONFIRMATION"
    LIVE = "LIVE"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    RECOVERING = "RECOVERING"
    ERROR_DEGRADED = "ERROR_DEGRADED"

class PositionSnapshot:
    def __init__(self):
        self.symbol = None
        self.side = None
        self.qty = 0.0
        self.entry_price = 0.0
        self.mark_price = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        self.roe_pct = 0.0
        self.leverage = LEVERAGE
        self.margin = 0.0
        self.liquidation_price = 0.0
        self.tp1_hit = False
        self.tp2_hit = False
        self.trailing_active = False
        self.trailing_stop = 0.0
        self.sl_price = 0.0
        self.partial_closed = False
        self.stale = False
        self.updated_at = 0.0
        self.source = "unknown"

    def to_dict(self):
        return {
            "symbol": self.symbol,
            "side": self.side,
            "qty": self.qty,
            "entry_price": self.entry_price,
            "mark_price": self.mark_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "roe_pct": self.roe_pct,
            "leverage": self.leverage,
            "margin": self.margin,
            "liquidation_price": self.liquidation_price,
            "tp1_hit": self.tp1_hit,
            "tp2_hit": self.tp2_hit,
            "trailing_active": self.trailing_active,
            "trailing_stop": self.trailing_stop,
            "sl_price": self.sl_price,
            "partial_closed": self.partial_closed,
            "stale": self.stale,
            "updated_at": self.updated_at,
            "source": self.source
        }

class EventBus:
    """Lazy, owned worker: construction never starts a background thread."""

    def __init__(self):
        self._handlers = {}
        self._queue = queue.Queue(maxsize=2000)
        self._stop = threading.Event()
        self._handlers_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._worker = None
        self._worker_started = False

    def start(self):
        """Start exactly one worker; return False when one is already alive."""
        with self._lifecycle_lock:
            if self._worker is not None and self._worker.is_alive():
                return False
            self._stop.clear()
            self._worker = threading.Thread(target=self._process, name="ggp-event-bus", daemon=True)
            self._worker_started = True
            self._worker.start()
            return True

    def stop(self, timeout=5.0):
        """Signal shutdown and join the owned worker with a bounded wait."""
        with self._lifecycle_lock:
            self._stop.set()
            worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=max(0.1, float(timeout)))
        with self._lifecycle_lock:
            self._worker_started = bool(worker is not None and worker.is_alive())

    def status(self):
        worker = self._worker
        return {
            "thread_alive": bool(worker is not None and worker.is_alive()),
            "started": bool(self._worker_started),
        }

    def subscribe(self, event_type, handler):
        with self._handlers_lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def emit(self, event_type, data=None):
        try:
            self._queue.put_nowait((event_type, data))
        except queue.Full:
            log_execution(f"[EVENT_BUS] queue full; rejected event={event_type}", "ERROR")
            return False
        return True

    def _process(self):
        while not self._stop.is_set():
            try:
                event_type, data = self._queue.get(timeout=0.1)
                with self._handlers_lock:
                    handlers = tuple(self._handlers.get(event_type, ()))
                for handler in handlers:
                    try:
                        handler(data)
                    except Exception as e:
                        log_execution(f"[EVENT] handler error: {type(e).__name__}: {e}", "ERROR")
            except queue.Empty:
                continue
            except Exception:
                continue

class ExchangeSyncService:
    def __init__(self, event_bus):
        self.event_bus = event_bus
        self._last_snapshot = PositionSnapshot()
        self._reconcile_count = 0
        self._last_reconcile = 0
        self._reconcile_lock = threading.Lock()

    def fetch_live_snapshot(self, symbol):
        """Return a snapshot, None only for exchange-confirmed FLAT; raise on UNKNOWN."""
        if PAPER_MODE:
            return self._paper_snapshot(symbol)
        status, pos = fetch_position_tri_state(symbol)
        if status == "UNKNOWN":
            raise RuntimeError(f"exchange position state UNKNOWN for {symbol}")
        if status == "FLAT":
            return None
        if not isinstance(pos, dict):
            raise RuntimeError(f"exchange OPEN response malformed for {symbol}")
        snapshot = _snapshot_from_position(symbol, pos)
        self._last_snapshot = snapshot
        return snapshot

    def _paper_snapshot(self, symbol):
        if not STATE.get("open") or STATE.get("current_symbol") != symbol:
            return None
        snap = PositionSnapshot()
        snap.symbol = symbol
        snap.side = STATE["side"]
        snap.qty = STATE["qty"]
        snap.entry_price = STATE["entry"]
        snap.mark_price = get_ticker_safe(symbol) or snap.entry_price
        snap.unrealized_pnl = (snap.mark_price - snap.entry_price) * snap.qty if snap.side == "BUY" else (snap.entry_price - snap.mark_price) * snap.qty
        snap.margin = snap.entry_price * snap.qty / LEVERAGE
        snap.roe_pct = (snap.unrealized_pnl / snap.margin) * 100 if snap.margin else 0
        snap.updated_at = time.time()
        snap.source = "paper"
        return snap

    def reconcile(self, symbol, local_state, *, force=False):
        global RECOVERY_BLOCK_ENTRIES
        now = time.time()
        if not self._reconcile_lock.acquire(blocking=False):
            return "BUSY"
        try:
            if not force and now - self._last_reconcile < 10:
                return "THROTTLED"
            self._last_reconcile = now
            log_execution(f"[RECONCILIATION] Starting for {symbol}", "INFO")
            self._reconcile_count += 1
            try:
                snap = self.fetch_live_snapshot(symbol)
            except Exception as exc:
                # UNKNOWN is never treated as FLAT. Keep exposure/state and block entries.
                if local_state.get("open"):
                    STATE["recovery_hold"] = True
                    STATE["recovery_reason"] = "EXCHANGE_POSITION_STATE_UNKNOWN"
                    RECOVERY_BLOCK_ENTRIES = True
                log_execution(f"[RECONCILIATION] UNKNOWN for {symbol}; local exposure preserved: {type(exc).__name__}: {exc}", "ERROR",
                              debounce_key=f"reconcile_unknown_{symbol}", debounce_sec=30)
                return "UNKNOWN"
            if snap is None:
                if local_state.get("open"):
                    log_execution(f"[RECONCILIATION] BingX confirmed FLAT for {symbol}; reconciling external close", "WARN")
                    _clear_confirmed_flat(symbol, reason="EXCHANGE_FLAT_CONFIRMED")
                return "FLAT"
            with _TRADE_LOCK:
                STATE["entry"] = snap.entry_price
                STATE["qty"] = snap.qty
                STATE["remaining_qty"] = snap.qty
                STATE["side"] = snap.side
                STATE["mark_price"] = snap.mark_price
                STATE["unrealized_pnl_usdt"] = snap.unrealized_pnl
                STATE["roe_pct"] = snap.roe_pct
                STATE["margin"] = snap.margin
                STATE["liquidation_price"] = snap.liquidation_price
                TRADE_STATE.update({
                    "symbol": symbol,
                    "side": snap.side,
                    "entry": snap.entry_price,
                    "qty": snap.qty,
                    "last_update_ts": time.time()
                })
                if not STATE.get("open"):
                    STATE["open"] = True
                    STATE["current_symbol"] = symbol
                    STATE["entry_time"] = time.time()
            log_execution(f"[RECONCILIATION] Completed: ROE={snap.roe_pct:.2f}%, Qty={snap.qty}", "SUCCESS")
            self.event_bus.emit("reconciled", snap)
            return "OPEN"
        finally:
            self._reconcile_lock.release()

class RecoveryGuard:
    def __init__(self, event_bus, exchange_sync):
        self.event_bus = event_bus
        self.exchange_sync = exchange_sync
        self.recovery_attempts = 0
        self.last_recovery = 0

    def check_and_recover(self, symbol):
        now = time.time()
        if self.recovery_attempts > 5 and now - self.last_recovery < 300:
            log_execution("[RECOVERY] Too many attempts, cooling down", "WARN")
            return False
        log_execution(f"[RECOVERY] Entering RECOVERING mode for {symbol}", "WARN")
        self.event_bus.emit("lifecycle_change", TradeLifecycleState.RECOVERING)
        success = False
        for attempt in range(3):
            try:
                snap = self.exchange_sync.fetch_live_snapshot(symbol)
                if snap is not None:
                    self.recovery_attempts = 0
                    self.last_recovery = now
                    self.event_bus.emit("recovery_success", snap)
                    success = True
                    break
                time.sleep(1)
            except:
                continue
        if not success:
            log_execution("[RECOVERY] Failed to recover, staying in degraded", "ERROR")
            self.event_bus.emit("lifecycle_change", TradeLifecycleState.ERROR_DEGRADED)
            self.recovery_attempts += 1
            self.last_recovery = now
        return success

# ========== INSTITUTIONAL TREND ENGINE ==========
class TrendState(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    PROBATION_BULLISH = "PROBATION_BULLISH"
    PROBATION_BEARISH = "PROBATION_BEARISH"
    CHOP = "CHOP"

class InstitutionalTrendEngine:
    def __init__(self):
        self.trend_state = TrendState.CHOP
        self.trend_persistence = 0
        self.last_state_change = 0
        self.state_confidence = 0.0

    def analyze_adx_momentum(self, adx_series):
        if adx_series is None or len(adx_series) < 10:
            return {"value": 20, "slope": 0, "acceleration": 0, "state": "UNKNOWN", "rising": False}
        current = adx_series.iloc[-1]
        slope = adx_series.iloc[-1] - adx_series.iloc[-4] if len(adx_series) >= 4 else 0
        accel = slope - (adx_series.iloc[-4] - adx_series.iloc[-7]) if len(adx_series) >= 7 else 0
        if current < 18:
            state = "CHOP"
        elif 18 <= current < 25:
            state = "EMERGING"
        elif 25 <= current < 35:
            state = "HEALTHY"
        elif 35 <= current < 45:
            state = "STRONG"
        else:
            state = "EXHAUSTION"
        return {"value": current, "slope": slope, "acceleration": accel, "state": state, "rising": slope > 0}

    def analyze_di_pressure(self, df):
        plus_di, minus_di, _, _ = get_di_components(df)
        if plus_di is None or minus_di is None:
            return {"dominant": "NEUTRAL", "spread": 0, "persistent": False}
        spread = plus_di - minus_di
        dominant = "BUY" if plus_di > minus_di else "SELL" if minus_di > plus_di else "NEUTRAL"
        persistent = False
        if len(df) >= 6:
            buy_count = 0
            sell_count = 0
            for i in range(-5, 0):
                p, m, _, _ = get_di_components(df.iloc[:i+1] if i < 0 else df)
                if p is not None and m is not None:
                    if p > m:
                        buy_count += 1
                    elif m > p:
                        sell_count += 1
            if dominant == "BUY" and buy_count >= 4:
                persistent = True
            elif dominant == "SELL" and sell_count >= 4:
                persistent = True
        return {"dominant": dominant, "spread": spread, "persistent": persistent}

    def analyze_pullback(self, df, side, atr):
        if len(df) < 5:
            return "NO_PULLBACK"
        last = df.iloc[-1]
        prev_candles = df.iloc[-5:-1]
        if side == "SELL":
            bullish_candles = [c for i, c in prev_candles.iterrows() if c['close'] > c['open']]
            if not bullish_candles and last['close'] <= last['open']:
                return "NO_PULLBACK"
            avg_body = sum(abs(c['close'] - c['open']) for _, c in prev_candles.iterrows()) / len(prev_candles)
            upper_wicks = sum((c['high'] - max(c['close'], c['open'])) for _, c in prev_candles.iterrows()) / len(prev_candles)
            vol = df['volume'].iloc[-1]
            avg_vol = df['volume'].iloc[-10:-1].mean()
            di = self.analyze_di_pressure(df)
            adx_mom = self.analyze_adx_momentum(compute_adx(df))
            weak_conditions = (avg_body < atr * 0.4 and upper_wicks > avg_body and vol < avg_vol * 0.8 and di["dominant"] == "SELL" and adx_mom["rising"] and adx_mom["state"] in ("HEALTHY", "STRONG"))
            if weak_conditions:
                return "WEAK_PULLBACK"
            if last['close'] > last['open'] and last['close'] > prev_candles['close'].max():
                if vol > avg_vol * 1.5 and di["dominant"] == "BUY" and not adx_mom["rising"]:
                    return "REVERSAL"
            return "STRONG_PULLBACK"
        else:
            bearish_candles = [c for i, c in prev_candles.iterrows() if c['close'] < c['open']]
            if not bearish_candles and last['close'] >= last['open']:
                return "NO_PULLBACK"
            avg_body = sum(abs(c['close'] - c['open']) for _, c in prev_candles.iterrows()) / len(prev_candles)
            lower_wicks = sum((min(c['open'], c['close']) - c['low']) for _, c in prev_candles.iterrows()) / len(prev_candles)
            vol = df['volume'].iloc[-1]
            avg_vol = df['volume'].iloc[-10:-1].mean()
            di = self.analyze_di_pressure(df)
            adx_mom = self.analyze_adx_momentum(compute_adx(df))
            weak_conditions = (avg_body < atr * 0.4 and lower_wicks > avg_body and vol < avg_vol * 0.8 and di["dominant"] == "BUY" and adx_mom["rising"] and adx_mom["state"] in ("HEALTHY", "STRONG"))
            if weak_conditions:
                return "WEAK_PULLBACK"
            if last['close'] < last['open'] and last['close'] < prev_candles['close'].min():
                if vol > avg_vol * 1.5 and di["dominant"] == "SELL" and not adx_mom["rising"]:
                    return "REVERSAL"
            return "STRONG_PULLBACK"

    def calculate_exit_score(self, ctx):
        score = 0
        if ctx.get("di_flip", False): score += 3
        if ctx.get("adx_collapse", False): score += 2
        if ctx.get("strong_reclaim", False): score += 3
        if ctx.get("exhaustion", False): score += 4
        if ctx.get("htf_opposite", False): score += 2
        if ctx.get("momentum_loss", False): score += 2
        if ctx.get("failed_continuation", False): score += 2
        return score

    def is_chop(self, df):
        adx = compute_adx(df)
        if adx is None or len(adx) < 20:
            return True
        adx_val = adx.iloc[-1]
        plus_di, minus_di, _, _ = get_di_components(df)
        if plus_di is None or minus_di is None:
            return True
        di_spread = abs(plus_di - minus_di)
        atr = compute_atr(df).iloc[-1]
        atr_ma = compute_atr(df).rolling(20).mean().iloc[-1] if len(df) >= 20 else atr
        atr_flat = abs(atr - atr_ma) / atr_ma < 0.1 if atr_ma > 0 else True
        vol_state = classify_volume(df)
        low_volume = vol_state in ("exhaustion", "neutral") and df['volume'].iloc[-1] < df['volume'].rolling(20).mean().iloc[-1] * 0.7
        return adx_val < 18 and di_spread < 5 and atr_flat and low_volume

    def update_trend_state(self, df, ob):
        adx_series = compute_adx(df)
        adx_mom = self.analyze_adx_momentum(adx_series)
        di_pressure = self.analyze_di_pressure(df)
        chop = self.is_chop(df)
        now = time.time()
        if chop:
            if self.trend_state != TrendState.CHOP:
                self.trend_state = TrendState.CHOP
                self.last_state_change = now
                self.trend_persistence = 0
                self.state_confidence = 0.0
            return
        bullish = (di_pressure["dominant"] == "BUY" and adx_mom["rising"] and adx_mom["state"] in ("HEALTHY", "STRONG"))
        bearish = (di_pressure["dominant"] == "SELL" and adx_mom["rising"] and adx_mom["state"] in ("HEALTHY", "STRONG"))
        if bullish and not bearish:
            target_state = TrendState.BULLISH
        elif bearish and not bullish:
            target_state = TrendState.BEARISH
        else:
            target_state = TrendState.CHOP
        if target_state != self.trend_state:
            if self.trend_state in (TrendState.BULLISH, TrendState.BEARISH):
                if self.trend_state == TrendState.BULLISH and target_state == TrendState.BEARISH:
                    self.trend_state = TrendState.PROBATION_BEARISH
                elif self.trend_state == TrendState.BEARISH and target_state == TrendState.BULLISH:
                    self.trend_state = TrendState.PROBATION_BULLISH
                else:
                    self.trend_state = target_state
                self.last_state_change = now
                self.trend_persistence = 0
                self.state_confidence = 0.3
            elif self.trend_state in (TrendState.PROBATION_BULLISH, TrendState.PROBATION_BEARISH):
                if now - self.last_state_change > 3600:
                    self.trend_state = target_state
                    self.state_confidence = 0.6
            else:
                self.trend_state = target_state
                self.last_state_change = now
                self.trend_persistence = 0
                self.state_confidence = 0.5
        else:
            self.trend_persistence += 1
            self.state_confidence = min(1.0, self.state_confidence + 0.02)

    def should_enter(self, side, df, ob, atr, price):
        if self.is_chop(df):
            return False, "CHOP market"
        if side == "BUY" and self.trend_state not in (TrendState.BULLISH, TrendState.PROBATION_BULLISH):
            return False, f"Trend not bullish ({self.trend_state.value})"
        if side == "SELL" and self.trend_state not in (TrendState.BEARISH, TrendState.PROBATION_BEARISH):
            return False, f"Trend not bearish ({self.trend_state.value})"
        pullback = self.analyze_pullback(df, side, atr)
        if pullback == "REVERSAL":
            return False, "Strong reversal detected"
        if pullback == "STRONG_PULLBACK":
            return False, "Pullback too strong"
        adx_series = compute_adx(df)
        adx_mom = self.analyze_adx_momentum(adx_series)
        if adx_mom["state"] not in ("HEALTHY", "STRONG", "EMERGING"):
            return False, f"ADX weak ({adx_mom['state']})"
        di = self.analyze_di_pressure(df)
        if not di["persistent"] and di["spread"] < 3:
            return False, "DI not persistent"
        return True, f"Trend {self.trend_state.value}, Pullback={pullback}"

    def get_trend_health(self, df, side):
        adx_mom = self.analyze_adx_momentum(compute_adx(df))
        di = self.analyze_di_pressure(df)
        health = 5
        if adx_mom["rising"]: health += 2
        if adx_mom["state"] == "HEALTHY": health += 1
        elif adx_mom["state"] == "STRONG": health += 2
        elif adx_mom["state"] == "EXHAUSTION": health -= 2
        if di["persistent"]: health += 2
        if side == "BUY" and di["dominant"] == "BUY": health += 1
        elif side == "SELL" and di["dominant"] == "SELL": health += 1
        else: health -= 2
        return max(0, min(10, health))

    def should_hold(self, df, side, atr, current_roe):
        pullback = self.analyze_pullback(df, side, atr)
        adx_mom = self.analyze_adx_momentum(compute_adx(df))
        di = self.analyze_di_pressure(df)
        if pullback == "WEAK_PULLBACK" and adx_mom["rising"] and di["persistent"]:
            return True, "Weak pullback, trend healthy"
        if adx_mom["value"] > 25 and di["dominant"] == side:
            return True, f"ADX {adx_mom['value']:.1f} still strong"
        if current_roe > 3.0 and pullback != "REVERSAL":
            return True, "High profit, allowing pullback"
        return False, "Trend weakening"

    def compute_exit_score_live(self, df, side, entry_price, current_price, atr):
        adx_series = compute_adx(df)
        adx_mom = self.analyze_adx_momentum(adx_series)
        di = self.analyze_di_pressure(df)
        last = df.iloc[-1]
        di_flip = (side == "BUY" and di["dominant"] == "SELL") or (side == "SELL" and di["dominant"] == "BUY")
        adx_collapse = adx_mom["value"] < 18 or (adx_mom["slope"] < -2 and adx_mom["state"] in ("HEALTHY", "STRONG"))
        if side == "SELL":
            strong_reclaim = current_price > entry_price and current_price > last['open']
        else:
            strong_reclaim = current_price < entry_price and current_price < last['open']
        exhaustion = adx_mom["state"] == "EXHAUSTION" and not adx_mom["rising"]
        momentum_loss = adx_mom["slope"] < -1 and adx_mom["acceleration"] < 0
        failed_cont = False
        if side == "SELL" and last['close'] > last['open']:
            if last['high'] - last['low'] > atr * 1.2:
                failed_cont = True
        elif side == "BUY" and last['close'] < last['open']:
            if last['high'] - last['low'] > atr * 1.2:
                failed_cont = True
        ctx = {"di_flip": di_flip, "adx_collapse": adx_collapse, "strong_reclaim": strong_reclaim,
               "exhaustion": exhaustion, "htf_opposite": False, "momentum_loss": momentum_loss,
               "failed_continuation": failed_cont}
        return self.calculate_exit_score(ctx)

trend_engine = InstitutionalTrendEngine()

# ========== INSTITUTIONAL TRADE BRAIN ==========
class InstitutionalTradeBrain:
    def __init__(self):
        self.state_machine = TradeStateMachine()
        self.last_update = 0
        self.current_trade_state = "RANGE_CHOP"

    def update(self, smart: dict, momentum: dict, adx: float, regime: str, side: str = None,
               **attribution):
        self.current_trade_state = self.state_machine.update(
            smart, momentum, adx, regime, side=side, **attribution)
        self.last_update = time.time()
        return self.current_trade_state

    def get_trail_multiplier(self) -> float:
        return self.state_machine.get_trail_multiplier()

    def should_delay_tp1(self) -> bool:
        return self.state_machine.should_delay_tp1()

    def should_aggressive_profit_lock(self) -> bool:
        return self.state_machine.should_aggressive_profit_lock()

    def should_hard_exit(self) -> bool:
        return self.state_machine.should_hard_exit()

    def get_patience_level(self) -> str:
        return self.state_machine.get_patience_level()

# ========== NEW TRADE MANAGEMENT BOARD (INSTITUTIONAL GRADE) ==========
from enum import Enum as PyEnum
from ggp_profit_brain import (classify_profit_mode, range_target_price, target_reached, select_range_target_pct,
                              tp2_reassessment, thesis_failure_action, evaluate_exit_risk,
                              adaptive_trail_multiplier, ema_trend_context, adaptive_range_target_price,
                              trend_target_prices, profit_target_convention,
                              RANGE_MODE, TREND_MODE, STANDARD_MODE)
from ggp_institutional_strategy import (build_institutional_report, validate_institutional_location,
                                        build_management_mode_report, build_entry_risk_plan)

class TradeDecisionType(PyEnum):
    NO_ACTION = "NO_ACTION"
    HOLD = "HOLD"
    MOVE_SL = "MOVE_SL"
    BREAKEVEN = "BREAKEVEN"
    LOCK_PROFITS = "LOCK_PROFITS"
    PARTIAL_CLOSE = "PARTIAL_CLOSE"
    TRAIL = "TRAIL"
    FULL_CLOSE = "FULL_CLOSE"
    THESIS_FAILURE_EXIT = "THESIS_FAILURE_EXIT"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"

@dataclass
class TradeDecision:
    action: TradeDecisionType
    params: Dict = field(default_factory=dict)
    reason: str = ""
    confidence: float = 1.0

class TradeManagementBoard:
    """The only routine authority for TP, runner, trailing and thesis exits."""
    def __init__(self):
        self._last_decision = None
        self._last_decision_time = 0.0
        self._decision_history = deque(maxlen=200)
        self._last_visible_signature = None
        self._last_visible_ts = 0.0

    def evaluate(self, context: Dict) -> TradeDecision:
        ctx = context if isinstance(context, dict) else {}
        side = str(ctx.get("side") or "").upper()
        entry = safe_float(ctx.get("entry"), 0.0)
        current = safe_float(ctx.get("current_price"), 0.0)
        atr = safe_float(ctx.get("atr"), 0.0)
        roe = safe_float(ctx.get("roe"), 0.0)
        remaining = safe_float(ctx.get("remaining_qty"), 0.0)
        initial = safe_float(ctx.get("initial_qty"), remaining)
        smart = ctx.get("smart_money") if isinstance(ctx.get("smart_money"), dict) else {}
        continuation = safe_float(ctx.get("continuation_probability"), 0.5)
        trade_state = str(ctx.get("trade_state") or "UNKNOWN").upper()
        profit_mode = str(ctx.get("profit_mode") or "TWO_STAGE_50_50").upper()
        entry_ctx = ctx.get("entry_context") if isinstance(ctx.get("entry_context"), dict) else {}
        # Additive reporting only: the canonical profit mode still comes from the
        # single profit brain and the TradeManagementBoard remains the authority.
        mode_report = build_management_mode_report(
            ctx, profit_mode=profit_mode,
            reason=ctx.get("profit_mode_reason") or "",
            evidence=ctx.get("profit_mode_evidence") or (),
        )

        def decision(action, reason, **params):
            result = TradeDecision(action, params=params, reason=reason)
            self._last_decision = result
            self._last_decision_time = time.time()
            symbol = str(ctx.get("symbol") or STATE.get("current_symbol") or "")
            meta = {
                "at": self._last_decision_time, "event_type": "BOARD_DECISION",
                "action": action.value, "reason": str(reason), "symbol": symbol,
                "side": side, "price": current, "roe_pct": roe,
                "initial_qty": initial, "remaining_qty": remaining,
                "profit_mode": profit_mode,
                "management_mode": mode_report.get("management_mode"),
                "management_mode_reason": mode_report.get("classification_reason"),
                "selected_target_roe_pct": mode_report.get("selected_target_gross_roe_pct"),
                "selected_target_price_move_pct": mode_report.get("selected_target_price_move_pct"),
                "target_convention_basis": (mode_report.get("target_convention") or {}).get("basis"),
                "actual_exit_reason": (
                    str(params.get("close_reason") or reason)
                    if action in {TradeDecisionType.FULL_CLOSE, TradeDecisionType.THESIS_FAILURE_EXIT,
                                  TradeDecisionType.EMERGENCY_EXIT, TradeDecisionType.PARTIAL_CLOSE}
                    else ""
                ),
                "entry_context_available": bool(entry_ctx),
                "entry_cross_event_id": str(entry_ctx.get("cross_event_id") or ""),
                "entry_mode": str(ctx.get("entry_mode") or "UNCLASSIFIED"),
                "entry_behavior": str(ctx.get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
                "entry_mode_confidence": safe_float(ctx.get("entry_mode_confidence"), 0.0),
                "fresh_ema9_vwap_cross": bool(ctx.get("fresh_ema9_vwap_cross", False)),
                "fresh_ema50_vwap_cross": bool(ctx.get("fresh_ema50_vwap_cross", False)),
                "vwap_cross_direction": str(ctx.get("vwap_cross_direction") or ""),
                "vwap_cross_trigger": str(ctx.get("vwap_cross_trigger") or "NONE"),
                "market_regime": str(ctx.get("regime") or "UNKNOWN"),
                "trade_classification": str(ctx.get("trade_classification") or "UNCLASSIFIED"),
                "trade_classification_reasons": list(ctx.get("trade_classification_reasons") or []),
                "tp1_hit": bool(ctx.get("tp1_hit")),
                "tp2_hit": bool(ctx.get("tp2_hit")),
                "tp1_price": safe_float(ctx.get("tp1_price")),
                "tp2_price": safe_float(ctx.get("tp2_price")),
                "opposing_liquidity_status": str(ctx.get("opposing_liquidity_status") or "UNKNOWN"),
                "opposing_liquidity_level": safe_float(ctx.get("opposing_liquidity_level"), 0.0),
                "scalp_target_status": str(ctx.get("scalp_target_status") or "UNKNOWN"),
                "scalp_target_price": safe_float(ctx.get("scalp_target_price"), 0.0),
                "scalp_target_tp1_price": safe_float(ctx.get("scalp_target_tp1_price"), 0.0),
                "risk_score": safe_float(params.get("risk_score"), 0.0),
                "risk_groups": list(params.get("risk_groups") or []),
                "scalp_target_selected_pct": safe_float(ctx.get("scalp_target_selected_pct"), 0.0),
                "mfe_roe": safe_float(ctx.get("peak_roe"), roe),
                "current_roe": roe,
                "time_in_trade_seconds": max(0.0, time.time() - safe_float(ctx.get("entry_time"), time.time())),
                "evidence_used": list(ctx.get("evidence_used") or params.get("risk_groups") or []),
                "evidence_missing": list(ctx.get("evidence_missing") or []),
                "risk_state": str(ctx.get("risk_state") or params.get("risk_state") or "UNKNOWN"),
                "protection_status": str(ctx.get("protection_status") or ("EXCHANGE_VERIFIED" if ctx.get("native_sl_verified") else "UNKNOWN")),
                "execution_result": str(ctx.get("execution_result") or "NOT_SUBMITTED"),
                "exchange_verification_result": str(ctx.get("exchange_verification_result") or "NOT_REQUIRED"),
            }
            self._decision_history.append(meta)
            signature = (action.value, str(reason), symbol)
            should_publish = action != TradeDecisionType.HOLD or signature != self._last_visible_signature or self._last_decision_time - self._last_visible_ts >= 60
            if should_publish:
                status = "WAITING" if action == TradeDecisionType.HOLD else "DECIDED"
                record_trade_management_event(
                    "BOARD_DECISION", status, log_level="INFO" if action != TradeDecisionType.HOLD else None,
                    dedupe_key=f"board:{symbol}:{action.value}:{reason}", dedupe_seconds=10 if action != TradeDecisionType.HOLD else 1,
                    board_at=meta.get("at"), **{k: v for k, v in meta.items() if k != "event_type"}
                )
                self._last_visible_signature = signature
                self._last_visible_ts = self._last_decision_time
            return result

        if ctx.get("execution_unresolved") or ctx.get("close_reconciliation_required") or ctx.get("recovery_hold"):
            return decision(TradeDecisionType.HOLD, "exchange_execution_or_recovery_unresolved")
        if ctx.get("emergency_exit"):
            return decision(TradeDecisionType.EMERGENCY_EXIT, str(ctx.get("emergency_reason") or "emergency_exit"))

        if ctx.get("indicator_decisions_allowed", True):
            # Unified danger assessment consumes independent evidence from the existing
            # smart-money, momentum, DI/ADX, continuation, EMA structure, liquidity,
            # order-block/FVG and support/resistance engines.
            exit_risk = evaluate_exit_risk(ctx)
            STATE["exit_risk_snapshot"] = dict(exit_risk)
            record_trade_management_event(
                "PROFIT_RISK_ASSESSMENT", exit_risk.get("action", "HOLD"),
                dedupe_key=f"profit_risk:{ctx.get('symbol')}:{ctx.get('side')}:{exit_risk.get('action')}:{exit_risk.get('score')}",
                dedupe_seconds=10, symbol=ctx.get("symbol"), side=side,
                risk_score=exit_risk.get("score"), risk_groups=list(exit_risk.get("groups") or []),
                reasons=list(exit_risk.get("reasons") or ()), roe_pct=roe,
            )
            if exit_risk.get("action") == "HARD_EXIT":
                return decision(
                    TradeDecisionType.THESIS_FAILURE_EXIT,
                    "PROFIT_BRAIN_HARD_RISK:" + ",".join(exit_risk.get("reasons", ())),
                    close_reason="PROFIT_DANGER_HARD_EXIT",
                    risk_score=exit_risk.get("score"), risk_groups=exit_risk.get("groups"),
                )
            if exit_risk.get("action") == "CLOSE_PROFIT":
                return decision(
                    TradeDecisionType.FULL_CLOSE,
                    "PROFIT_DANGER_LOCK:" + ",".join(exit_risk.get("reasons", ())),
                    close_reason="PROFIT_DANGER_LOCK",
                    risk_score=exit_risk.get("score"), risk_groups=exit_risk.get("groups"),
                )
            if exit_risk.get("action") == "LOCK_PROFIT" and entry > 0 and current > 0 and atr > 0:
                fee_buffer = max(entry * 0.0010, atr * 0.10)
                candidate_stop = (
                    max(entry + fee_buffer, current - 0.50 * atr)
                    if side == "BUY" else
                    min(entry - fee_buffer, current + 0.50 * atr)
                )
                if (side == "BUY" and candidate_stop < current) or (side == "SELL" and candidate_stop > current):
                    return decision(
                        TradeDecisionType.LOCK_PROFITS,
                        "PROFIT_DANGER_TIGHTEN:" + ",".join(exit_risk.get("reasons", ())),
                        new_sl=candidate_stop, risk_score=exit_risk.get("score"),
                        risk_groups=exit_risk.get("groups"),
                    )

            # A failed breakout is supporting thesis evidence, not an independent
            # exit signal. Require a fresh same-side event and two adjacent closed
            # candles confirming price remained back inside the causal zone.
            mi = ctx.get("market_intelligence") if isinstance(ctx.get("market_intelligence"), dict) else {}
            zone_evidence_valid = is_confirmed_failed_breakout_evidence(mi, side)
            thesis_reasons = list(ctx.get("thesis_failure_reasons") or [])
            thesis_score = safe_float(ctx.get("thesis_failure_score"), 0.0)
            if zone_evidence_valid:
                thesis_score = min(100.0, thesis_score + 18.0)
                thesis_reasons.append("confirmed_failed_breakout_reentry")
                STATE["failed_breakout_thesis_credit_applied"] = True

            thesis_action, thesis_reason = thesis_failure_action(
                side, roe, thesis_score, thesis_reasons,
                structural_invalidation=bool(ctx.get("structural_invalidation")))
            if thesis_action == "EXIT":
                return decision(TradeDecisionType.THESIS_FAILURE_EXIT,
                                str(ctx.get("thesis_failure_reason") or thesis_reason))
            if thesis_action == "PROTECT" and not ctx.get("be_done"):
                return decision(TradeDecisionType.BREAKEVEN, thesis_reason)
            if trade_state in ("PANIC_EXIT", "MOMENTUM_COLLAPSE", "LIQUIDITY_EXHAUSTION"):
                return decision(TradeDecisionType.FULL_CLOSE, f"hard_exit_state_{trade_state.lower()}")

        sl_price = safe_float(ctx.get("synthetic_sl"), 0.0)
        if sl_price > 0 and current > 0:
            if (side == "BUY" and current <= sl_price) or (side == "SELL" and current >= sl_price):
                return decision(TradeDecisionType.FULL_CLOSE, "synthetic_structural_stop_triggered")
        trail_stop = safe_float(ctx.get("trail_stop"), 0.0)
        if ctx.get("trail_active") and trail_stop > 0 and current > 0:
            if (side == "BUY" and current <= trail_stop) or (side == "SELL" and current >= trail_stop):
                return decision(TradeDecisionType.FULL_CLOSE, "profit_trailing_stop_triggered")

        # RANGE_SCALP is a SINGLE-TARGET mode: no partial TP1 and no runner.
        # On target, close the full verified remaining exposure through the
        # existing strict FULL_CLOSE execution/reconciliation path.
        if profit_mode == "RANGE_SCALP":
            scalp_target_status = str(ctx.get("scalp_target_status") or "UNKNOWN").upper()
            scalp_target = safe_float(ctx.get("scalp_target_price"), 0.0)
            range_target = (
                safe_float(ctx.get("range_target_price"), 0.0)
                or (scalp_target if scalp_target_status == "VALID" else 0.0)
            )
            target_source = ("adaptive_price_target"
                             if safe_float(ctx.get("range_target_price"), 0.0) > 0
                             else "opposing_liquidity_fallback")
            if range_target > 0 and entry > 0 and remaining > 0:
                if target_reached(side, current, range_target):
                    return decision(
                        TradeDecisionType.FULL_CLOSE,
                        f"range_scalp_{target_source}_single_target_full_exit",
                        close_reason="SCALP_TARGET", target_price=range_target,
                        target_move_pct=safe_float(ctx.get("range_target_move_pct"), 0.0),
                        opposing_liquidity_level=safe_float(ctx.get("opposing_liquidity_level"), 0.0),
                    )
            return decision(
                TradeDecisionType.HOLD, "range_scalp_waiting_for_single_target",
                target_price=range_target, target_source=target_source,
                target_move_pct=safe_float(ctx.get("range_target_move_pct"), 0.0),
            )

        # Canonical TP1: reduce the original filled quantity to exactly 50%.
        # This compensates for any previously confirmed partial fill and never
        # changes the policy to 30%/60% based on a score.
        tp1_hit = bool(ctx.get("tp1_hit"))
        tp2_hit = bool(ctx.get("tp2_hit"))
        if not tp1_hit and remaining > 0 and initial > 0:
            tp1_price = safe_float(ctx.get("tp1_price", ctx.get("synthetic_tp1")), 0.0)
            if tp1_price > 0 and target_reached(side, current, tp1_price):
                target_remaining = initial * 0.5
                tolerance = max(initial * 0.002, 1e-10)
                qty_to_close = max(0.0, remaining - target_remaining)
                if qty_to_close <= tolerance:
                    return decision(TradeDecisionType.HOLD, "tp1_quantity_already_at_50pct_wait_for_reconciliation")
                ratio = min(1.0, qty_to_close / remaining)
                return decision(TradeDecisionType.PARTIAL_CLOSE, "TP1_FIXED_50_PERCENT_OF_ORIGINAL_FILLED_QTY",
                                ratio=ratio, target_remaining_qty=target_remaining,
                                close_reason="TP1")

        # TP2 is the final 50% runner exit. If it is objectively remote while
        # continuation/structure has failed, close the remaining runner strictly.
        if tp1_hit and not tp2_hit and remaining > 0:
            tp2_price = safe_float(ctx.get("tp2_price"), 0.0)
            if ctx.get("tp2_decision") == "CLOSE":
                return decision(TradeDecisionType.FULL_CLOSE,
                                str(ctx.get("tp2_decision_reason") or "tp2_remote_and_continuation_weak"),
                                close_reason="TP2_ADAPTIVE_STRICT_EXIT")
            if tp2_price > 0 and target_reached(side, current, tp2_price):
                return decision(TradeDecisionType.FULL_CLOSE, "TP2_FINAL_50_PERCENT_RUNNER_TARGET",
                                close_reason="TP2")

        # BE+ and trailing apply to the verified remaining runner only.
        if tp1_hit and not ctx.get("be_done"):
            be_buffer = max(entry * 0.0010, atr * 0.10)
            be_stop = entry + be_buffer if side == "BUY" else entry - be_buffer
            return decision(TradeDecisionType.BREAKEVEN, "TP1_verified_move_runner_to_BE_plus",
                            new_sl=be_stop)

        if tp1_hit and atr > 0 and entry > 0 and current > 0:
            base_mult = safe_float(ctx.get("smart_trail_mult"), 1.5)
            adx = safe_float(ctx.get("adx"), 20.0)
            trail_mult = adaptive_trail_multiplier(
                base_mult, continuation, adx,
                safe_float(ctx.get("distribution_risk", smart.get("distribution_risk", 0.0))),
                int(safe_float(ctx.get("exit_warning", 0))),
                str(ctx.get("ema_state") or "UNKNOWN"),
                safe_float(ctx.get("peak_drawdown_roe", 0.0)),
                tp1_verified=True,
            )
            favourable = safe_float(ctx.get("max_price"), entry) if side == "BUY" else safe_float(ctx.get("min_price"), entry)
            new_stop = favourable - trail_mult * atr if side == "BUY" else favourable + trail_mult * atr
            be_buffer = max(entry * 0.0010, atr * 0.10)
            floor_stop = entry + be_buffer if side == "BUY" else entry - be_buffer
            if side == "BUY":
                new_stop = max(new_stop, floor_stop, safe_float(ctx.get("trail_stop"), 0.0))
                current_stop = max(sl_price, trail_stop)
                hit = current <= new_stop
                improves = new_stop > current_stop + max(entry * 1e-8, 1e-10)
            else:
                previous = safe_float(ctx.get("trail_stop"), 0.0)
                new_stop = min(new_stop, floor_stop, previous if previous > 0 else float("inf"))
                current_stop = min(sl_price if sl_price > 0 else float("inf"), trail_stop if trail_stop > 0 else float("inf"))
                hit = current >= new_stop
                improves = new_stop < current_stop - max(entry * 1e-8, 1e-10)
            if hit:
                return decision(TradeDecisionType.FULL_CLOSE, "runner_trailing_ratchet_triggered",
                                close_reason="TRAILING_STOP")
            if improves:
                return decision(TradeDecisionType.TRAIL, "ratchet_runner_trailing_stop",
                                new_stop=new_stop, multiplier=trail_mult)

        return decision(TradeDecisionType.HOLD, "no_profit_management_trigger")

# ========== REAL EXCHANGE ORDERS (SAFE) ==========
def _is_definitive_close_rejection(exc):
    """True only for exchange-side validation rejection before order acceptance.

    Transport/timeouts and generic exchange errors remain ambiguous and must
    continue through the strict reconciliation gate.
    """
    return any(cls.__name__ == "InvalidOrder" for cls in type(exc).__mro__)

def _is_min_amount_close_rejection(exc):
    """Recognize only explicit venue minimum-size rejections, not every InvalidOrder.

    BingX documents dedicated minimum-size errors (including 101202, 101485,
    and 110422). Message matching is kept narrow for CCXT versions that wrap
    venue codes differently. Precision, side, mode, and other InvalidOrder
    failures must never trigger an automatic full-position exit.
    """
    message = str(exc or "").lower()
    explicit_codes = ("101202", "101485", "110422")
    if any(code in message for code in explicit_codes):
        return True
    minimum_terms = (
        "minimum order amount", "amount below exchange minimum",
        "below minimum amount", "minimum quantity",
        "closing order size is below the minimum",
        "order size is below the minimum", "minimum nominal value",
        "minsizecoin", "minsizeusd",
        "minimum notional", "min notional", "notional value is too small",
        "order quantity is less than minimum", "quantity is less than minimum",
        "amount is less than minimum", "less than the minimum order size",
        "minimum trade amount", "trade amount is below minimum",
    )
    return (any(cls.__name__ == "InvalidOrder" for cls in type(exc).__mro__)
            and any(term in message for term in minimum_terms))




def _close_failure(message, *, unresolved=True):
    """Fail closed after an ambiguous exchange operation; never fabricate a fill."""
    STATE["execution_unresolved"] = bool(unresolved)
    STATE["close_reconciliation_required"] = bool(unresolved)
    if unresolved:
        globals()["RECOVERY_BLOCK_ENTRIES"] = True
        MEMORY["recovery_status"] = "CLOSE_EXECUTION_UNRESOLVED"
    log_execution(f"[CLOSE_VERIFY] {message}", "ERROR")
    return False


def _verified_market_reduce_impl(symbol, qty=None, *, ratio=None, target_remaining_qty=None,
                            close_reason="BOT_STRICT_CLOSE"):
    """Submit one reduce-only MARKET order and verify the actual BingX position delta.

    No automatic retry is used for non-idempotent MARKET orders. Ambiguous outcomes
    block new entries until exchange reconciliation establishes the actual position.
    """
    if not STATE.get("open"):
        return False
    side = str(STATE.get("side") or "").upper()
    symbol = symbol or STATE.get("current_symbol")
    if side not in ("BUY", "SELL") or not symbol:
        return _close_failure("invalid side or symbol")

    if PAPER_MODE:
        paper_pos = paper.get("position")
        if not isinstance(paper_pos, dict):
            return _close_failure("PAPER position record missing", unresolved=False)
        before = safe_float(paper_pos.get("remaining_qty", STATE.get("remaining_qty")))
        if before <= 0:
            return _close_failure("PAPER quantity is not positive", unresolved=False)
        if target_remaining_qty is not None:
            requested = max(0.0, before - max(0.0, safe_float(target_remaining_qty)))
        elif qty is not None:
            requested = min(before, max(0.0, safe_float(qty)))
        elif ratio is not None:
            requested = before * min(1.0, max(0.0, safe_float(ratio)))
        else:
            requested = before
        if requested <= 0:
            return False
        fill_price = safe_float(get_ticker_safe(symbol), safe_float(STATE.get("mark_price")))
        if fill_price <= 0:
            return _close_failure("PAPER close price unavailable", unresolved=False)
        entry = safe_float(STATE.get("entry"))
        pnl = ((fill_price-entry) if side == "BUY" else (entry-fill_price)) * requested
        after = max(0.0, before-requested)
        paper_pos["remaining_qty"] = after
        STATE.update({"remaining_qty": after, "exchange_qty": after,
                      "last_close_order_id": f"PAPER-{uuid.uuid4().hex[:12]}",
                      "last_close_reason": str(close_reason), "last_close_fill_price": fill_price,
                      "last_close_filled_qty": requested,
                      "last_close_order_realized_pnl": pnl, "last_close_order_pnl_source": "PAPER_MARK_ESTIMATE",
                      "last_close_realized_pnl": safe_float(STATE.get("last_close_realized_pnl"), 0.0)+pnl,
                      "last_close_pnl_source": "PAPER_MARK_ESTIMATE", "last_close_verified": True,
                      "last_close_pnl_complete": True,
                      "execution_unresolved": False, "close_reconciliation_required": False,
                      "close_verified": after <= max(before*0.002, 1e-10)})
        TRADE_STATE["qty"] = after
        if after <= max(before*0.002, 1e-10):
            STATE["remaining_qty"] = 0.0
            paper["position"] = None
        if "_persist_trade_journal_snapshot" in globals():
            _persist_trade_journal_snapshot(force=True)
        log_execution(f"[CLOSE_VERIFY] PAPER {close_reason}: filled={requested:.8f}, remaining={after:.8f}", "SUCCESS")
        return True

    if STATE.get("execution_unresolved") or STATE.get("close_reconciliation_required"):
        return _close_failure("previous close is unresolved; refusing another MARKET order")
    status, position = fetch_position_tri_state(symbol)
    if status == "UNKNOWN":
        return _close_failure("BingX position status UNKNOWN before close")
    if status == "FLAT":
        _clear_confirmed_flat(symbol, reason="EXCHANGE_FLAT_BEFORE_BOT_CLOSE")
        return False
    before = _position_contracts(position)
    if before <= 0:
        return _close_failure("BingX OPEN response has invalid quantity")

    local = safe_float(STATE.get("remaining_qty"), before)
    if local <= 0 or abs(local-before) > max(before*0.01, 1e-10):
        log_execution(f"[CLOSE_VERIFY] Local quantity {local:.10f} differs from BingX {before:.10f}; exchange quantity wins", "WARN")
        STATE["remaining_qty"] = before
        STATE["exchange_qty"] = before
        TRADE_STATE["qty"] = before

    if target_remaining_qty is not None:
        requested = max(0.0, before-max(0.0, safe_float(target_remaining_qty)))
    elif qty is not None:
        requested = min(before, max(0.0, safe_float(qty)))
    elif ratio is not None:
        requested = before*min(1.0, max(0.0, safe_float(ratio)))
    else:
        requested = before
    if requested <= 0:
        return False

    sym = normalize_symbol(symbol)
    try:
        requested = float(ex.amount_to_precision(sym, requested))
        if requested <= 0:
            return _close_failure("close quantity rounded to zero", unresolved=False)
        close_side = "sell" if side == "BUY" else "buy"
        mode_result = BingXPositionModeAdapter(ex).read(sym)
        close_mode_label = getattr(getattr(mode_result, "mode", None), "value", "UNKNOWN")
        if mode_result.mode is PositionMode.UNKNOWN:
            return _close_failure(f"position mode UNKNOWN; close blocked: {mode_result.reason}", unresolved=True)
        close_params = build_position_order_params(
            mode_result.mode, close_side.upper(), opening=False, closing=True,
            reduce_only_required=(mode_result.mode is PositionMode.ONE_WAY),
        )
        # Do not wrap create_order in safe_api_call: an ambiguous MARKET order must not be retried.
        order = safe_api_call(ex.create_order, sym, "market", close_side, requested,
                              params=close_params)
        if not isinstance(order, dict):
            return _close_failure("BingX returned no structured close-order response")
        order_id = order.get("id") or (order.get("info") or {}).get("orderId")
        if not order_id:
            return _close_failure("close response has no order ID; submission outcome is ambiguous")
        order_id = str(order_id)
        STATE.update({"pending_close_order_id": order_id, "pending_close_reason": str(close_reason),
                      "pending_close_before_qty": before, "pending_close_requested_qty": requested,
                      "pending_close_target_remaining_qty": target_remaining_qty,
                      "pending_close_started_at": time.time(), "execution_unresolved": True, "close_blocked_reason": None,
                      "close_reconciliation_required": True})
        bot_ids = STATE.get("bot_close_order_ids")
        if not isinstance(bot_ids, list):
            # A recovered/partially initialized STATE may contain None here.
            # Normalize the journal field before membership checks; never let
            # local metadata shape prevent an already-submitted close from
            # being reconciled against the exchange.
            bot_ids = []
            STATE["bot_close_order_ids"] = bot_ids
        if order_id not in bot_ids:
            bot_ids.append(order_id)
            if len(bot_ids) > 50:
                del bot_ids[:-50]
        _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
    except Exception as exc:
        if _is_definitive_close_rejection(exc):
            # BingX rejected the request at validation time; this is not an
            # accepted-but-unknown MARKET order. Re-read the live exposure and,
            # for TP1 only, fall back once to closing the entire verified
            # remainder rather than leaving a profitable position unmanaged.
            STATE.update({"execution_unresolved": False,
                          "close_reconciliation_required": False,
                          "close_blocked_reason": f"EXCHANGE_INVALID_ORDER: {exc}",
                          "close_blocked_at": time.time()})
            rejected_status, rejected_position = fetch_position_tri_state(symbol)
            log_execution(
                f"[CLOSE_REJECTED] {symbol} {close_reason}: "
                f"{type(exc).__name__}: {exc}; no order accepted "
                f"| requested_qty={safe_float(locals().get('requested'), 0.0):.12g} "
                f"| exchange_before_qty={safe_float(locals().get('before'), 0.0):.12g} "
                f"| live_rejected_qty={safe_float(_position_contracts(rejected_position), 0.0) if rejected_position is not None else 0.0:.12g} "
                f"| position_mode={locals().get('close_mode_label', 'UNKNOWN')} "
                f"| normalized_symbol={locals().get('sym', normalize_symbol(symbol))}",
                "WARN",
            )
            if rejected_status == "FLAT":
                _clear_confirmed_flat(symbol, reason="CLOSE_REJECTED_BUT_EXCHANGE_FLAT")
                return True
            if rejected_status == "OPEN":
                rejected_qty = _position_contracts(rejected_position)
                if (rejected_qty > 0 and str(close_reason).upper() == "TP1"
                        and _is_min_amount_close_rejection(exc)):
                    log_execution(f"[TP1_FALLBACK] {symbol}: venue minimum-size rejection; "
                                  f"attempting one strict full close of verified qty={rejected_qty:.10f}", "WARN")
                    fallback_ok = _verified_market_reduce_impl(
                        symbol, qty=rejected_qty,
                        close_reason="TP1_MIN_AMOUNT_FULL_CLOSE")
                    if fallback_ok and STATE.get("close_verified") and safe_float(STATE.get("remaining_qty")) <= max(rejected_qty * 0.002, 1e-10):
                        finalize = globals().get("finalize_trade_with_reality")
                        if callable(finalize):
                            finalize(symbol, close_reason="TP1_MIN_AMOUNT_FULL_CLOSE")
                        log_execution(f"[TP1_FALLBACK] {symbol}: full remaining exposure closed and finalized", "SUCCESS")
                        return True
                    log_execution(f"[TP1_FALLBACK] {symbol}: full-close attempt did not verify FLAT; "
                                  "position remains active under exchange protection", "ERROR")
                    return False
            # Position unavailable or a non-TP1 action: leave the real position
            # and its exchange-native protection intact, but do not mislabel a
            # definitive validation rejection as an unresolved order outcome.
            log_execution(f"[CLOSE_REJECTED] {symbol}: no safe fallback; "
                          "position remains under normal exchange synchronization", "ERROR")
            return False
        return _close_failure(f"market close submission ambiguous: {type(exc).__name__}: {exc}")

    post_status, post_position, order_state = "UNKNOWN", None, order
    after = before
    can_fetch_order = callable(getattr(ex, "fetch_order", None))
    terminal_statuses = {"CLOSED", "FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
    for _ in range(8):
        try:
            if can_fetch_order:
                fetched = safe_api_call(ex.fetch_order, str(order_id), sym)
                if isinstance(fetched, dict):
                    order_state = fetched
            order_status = str(order_state.get("status") or "UNKNOWN").upper()
            post_status, post_position = fetch_position_tri_state(symbol)
            if post_status == "FLAT":
                after = 0.0
                break
            if post_status == "OPEN":
                after = _position_contracts(post_position)
                has_delta = after < before-max(before*0.001, 1e-10)
                if has_delta and (order_status in terminal_statuses or not can_fetch_order):
                    break
                if order_status in terminal_statuses and not has_delta:
                    break
        except Exception:
            post_status = "UNKNOWN"
        time.sleep(0.35)

    order_status = str(order_state.get("status") or "UNKNOWN").upper()
    if can_fetch_order and order_status not in terminal_statuses and post_status != "FLAT":
        # A market order that remains open after polling must be cancelled before
        # another management action can use the remaining exposure.
        try:
            if callable(getattr(ex, "cancel_order", None)):
                safe_api_call(ex.cancel_order, str(order_id), sym)
                fetched = safe_api_call(ex.fetch_order, str(order_id), sym)
                if isinstance(fetched, dict):
                    order_state = fetched
                    order_status = str(order_state.get("status") or "UNKNOWN").upper()
                post_status, post_position = fetch_position_tri_state(symbol)
                if post_status == "OPEN":
                    after = _position_contracts(post_position)
                elif post_status == "FLAT":
                    after = 0.0
        except Exception as exc:
            log_execution(f"[CLOSE_VERIFY] cancel/readback for pending market order {order_id} unresolved: {exc}", "ERROR")
    if post_status == "UNKNOWN":
        return _close_failure(f"order {order_id} submitted but post-order position is UNKNOWN")
    if can_fetch_order and order_status not in terminal_statuses and post_status != "FLAT":
        return _close_failure(f"order {order_id} is not terminal ({order_status}); waiting for reconciliation")

    filled = max(0.0, before-after)
    tolerance = max(before*0.002, 1e-10)
    if filled <= tolerance:
        if order_status in {"REJECTED", "CANCELED", "CANCELLED", "EXPIRED"} and post_status == "OPEN":
            STATE.update({"pending_close_order_id": None, "pending_close_reason": None,
                          "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
                          "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
                          "execution_unresolved": False, "close_reconciliation_required": False})
            _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
            return False
        return _close_failure(f"order {order_id} returned but no position reduction was confirmed")

    average = safe_float(order_state.get("average") or order_state.get("price") or order.get("average") or order.get("price"))
    realized = None
    for candidate in (order_state.get("info"), order.get("info")):
        if isinstance(candidate, dict):
            for key in ("realizedPnl", "realizedProfit", "realized_pnl", "profit"):
                if candidate.get(key) not in (None, ""):
                    try:
                        realized = float(candidate[key]); break
                    except (TypeError, ValueError):
                        pass
        if realized is not None:
            break
    pnl_source = "BINGX_ORDER_REALIZED_PNL" if realized is not None else "REALIZED_PNL_UNAVAILABLE"
    if average <= 0:
        info = order_state.get("info") if isinstance(order_state.get("info"), dict) else {}
        average = safe_float(info.get("avgPrice"))
    # Do not calculate live futures realized PnL as amount*price: contract size,
    # fees and funding differ by market and exchange accounting mode.
    if realized is None:
        realized = 0.0

    with _TRADE_LOCK:
        STATE.update({"remaining_qty": max(0.0, after), "exchange_qty": max(0.0, after),
                      "last_close_order_id": str(order_id), "last_close_reason": str(close_reason),
                      "last_close_fill_price": average or None, "last_close_filled_qty": filled,
                      "last_close_order_realized_pnl": realized if pnl_source == "BINGX_ORDER_REALIZED_PNL" else None,
                      "last_close_order_pnl_source": pnl_source,
                      "last_close_realized_pnl": safe_float(STATE.get("last_close_realized_pnl"), 0.0)+realized,
                      "last_close_pnl_source": pnl_source, "last_close_verified": True,
                      "last_exchange_close_status": post_status, "execution_unresolved": False,
                      "close_reconciliation_required": False, "close_verified": post_status == "FLAT",
                      "pending_close_order_id": None, "pending_close_reason": None,
                      "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
                      "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
                      "last_close_pnl_complete": bool(STATE.get("last_close_pnl_complete", True) and pnl_source == "BINGX_ORDER_REALIZED_PNL")})
        TRADE_STATE["qty"] = max(0.0, after)
    if close_reason == "TP1":
        STATE["tp1_filled_qty"] = safe_float(STATE.get("tp1_filled_qty"), 0.0)+filled
        STATE["tp1_execution_price"] = average or STATE.get("tp1_execution_price")
    elif close_reason == "TP2":
        STATE["tp2_filled_qty"] = safe_float(STATE.get("tp2_filled_qty"), 0.0)+filled
    if "_persist_trade_journal_snapshot" in globals():
        _persist_trade_journal_snapshot(force=True)
    log_execution(f"[CLOSE_VERIFY] {close_reason} order={order_id} confirmed_delta={filled:.10f} remaining={after:.10f} pnl_source={pnl_source}", "SUCCESS")

    desired_after = max(0.0, before-requested)
    if target_remaining_qty is not None:
        desired_after = max(0.0, safe_float(target_remaining_qty))
    if abs(after-desired_after) > max(before*0.005, tolerance):
        log_execution(f"[CLOSE_VERIFY] Partial fill differs from target; observed remaining={after:.10f}, expected={desired_after:.10f}; no blind retry", "WARN")
        return False
    if qty is None and ratio is None and target_remaining_qty is None and post_status != "FLAT":
        log_execution(f"[CLOSE_VERIFY] Full close left {after:.10f} contracts open; position remains active", "WARN")
        return False
    return True


def _verified_market_reduce(symbol, qty=None, *, ratio=None, target_remaining_qty=None,
                            close_reason="BOT_STRICT_CLOSE"):
    if not _CLOSE_ORDER_LOCK.acquire(blocking=False):
        log_execution("[CLOSE_VERIFY] another close operation is already in progress; duplicate close suppressed", "WARN")
        return False
    try:
        return _verified_market_reduce_impl(symbol, qty, ratio=ratio,
                                            target_remaining_qty=target_remaining_qty,
                                            close_reason=close_reason)
    finally:
        _CLOSE_ORDER_LOCK.release()


def safe_close_position(symbol, qty=None, *, close_reason="BOT_STRICT_CLOSE"):
    """Compatibility wrapper; success requires a verified exchange position delta."""
    return _verified_market_reduce(symbol, qty, close_reason=close_reason)


def safe_close_partial(symbol, ratio, *, close_reason="TP1", target_remaining_qty=None):
    """Close a partial amount only after the resulting position quantity is verified."""
    return _verified_market_reduce(symbol, ratio=ratio, target_remaining_qty=target_remaining_qty,
                                   close_reason=close_reason)


def move_native_stop_on_exchange(symbol, new_trigger_price, quantity, *, reason="BREAKEVEN"):
    """Deprecated compatibility no-op for the old immutable-native-SL policy.

    Active management uses ``_move_native_stop_verified`` and
    ``TradeExecutionService.replace_native_stop``, which adopt a new native stop
    only after a fresh exchange read-back proves MOVED. This wrapper is retained
    only so historical callers cannot mutate the stop without that verification.
    """
    log_execution(
        f"[NATIVE_SL_FIXED] ignored post-entry stop modification for {symbol or STATE.get('current_symbol')}: "
        f"reason={reason}; original_price={safe_float(STATE.get('native_sl_original_price'), safe_float(STATE.get('sl'))):.12g}",
        "INFO",
    )
    return False


def _closed_candle_frame(df, timeframe=None):
    """Return only completed OHLCV candles; retain all rows if timestamps are unavailable."""
    if df is None or getattr(df, "empty", True):
        return df
    frame = df.copy()
    tf = str(timeframe or INTERVAL or "15m").strip().lower()
    try:
        if tf.endswith("m"):
            duration = int(tf[:-1]) * 60
        elif tf.endswith("h"):
            duration = int(tf[:-1]) * 3600
        elif tf.endswith("d"):
            duration = int(tf[:-1]) * 86400
        else:
            duration = 15 * 60
    except Exception:
        duration = 15 * 60
    try:
        raw_ts = frame["timestamp"].iloc[-1] if "timestamp" in frame.columns else frame.index[-1]
        ts = float(raw_ts)
        if ts > 1e12:
            ts /= 1000.0
        # CCXT candle timestamps are candle-open times.
        if time.time() < ts + duration:
            frame = frame.iloc[:-1].copy()
    except Exception:
        # Do not delete data merely because the timestamp format is unknown.
        pass
    return frame


# ========== LIVE TRADE MANAGER WITH NEW BOARD ==========
_NATIVE_STOP_MOVE_LOCK = threading.RLock()
_NATIVE_STOP_MOVE_DEDUPE_SEC = 300.0


def _state_position_mode():
    raw = str(STATE.get("position_mode") or "UNKNOWN").upper().replace("-", "_").replace(" ", "_")
    try:
        return PositionMode(raw)
    except Exception:
        return PositionMode.UNKNOWN


def _move_native_stop_verified(symbol, new_trigger_price, *, action="MOVE_SL", reason=""):
    """Move the venue-native stop and adopt the new level only after read-back.

    The execution service owns all cancel/replace verification. This bridge adds
    management-side materiality, de-duplication and state/journal handling. A
    failed move never claims success: RETAINED keeps the verified previous stop,
    LOST enters the existing recovery hold, and PAPER updates synthetic state only.
    """
    new_price = safe_float(new_trigger_price, 0.0)
    side = str(STATE.get("side") or "").upper()
    if new_price <= 0 or side not in {"BUY", "SELL"}:
        return False, "INVALID_STOP_MOVE_INPUT", {}

    min_step = max(abs(safe_float(STATE.get("entry"), 0.0)) * 0.0005, 1e-12)
    now = time.time()
    with _NATIVE_STOP_MOVE_LOCK:
        last_price = safe_float(STATE.get("native_sl_last_requested_price"), 0.0)
        last_ts = safe_float(STATE.get("native_sl_last_update_ts"), 0.0)
        if last_price > 0 and abs(new_price - last_price) <= min_step \
                and now - last_ts < _NATIVE_STOP_MOVE_DEDUPE_SEC:
            return True, "DUPLICATE_SKIPPED", {"requested_price": new_price}

        current = safe_float(STATE.get("native_sl_last_verified_price"),
                             safe_float(STATE.get("native_sl_original_price"),
                                        safe_float(STATE.get("synthetic_sl"), 0.0)))
        improves = (current <= 0 or new_price > current + min_step) if side == "BUY" \
            else (current <= 0 or new_price < current - min_step)
        if not improves:
            return False, "NOT_MATERIAL", {}

        record = {
            "symbol": symbol, "side": side, "action": action, "reason": reason,
            "requested_stop": new_price, "previous_stop": current or None,
            "previous_order_id": STATE.get("native_sl_order_id"),
        }
        if PAPER_MODE:
            STATE.update({
                "native_sl_policy": "PAPER_SYNTHETIC_ONLY",
                "native_sl_local_target": new_price,
                "native_sl_last_requested_price": new_price,
                "native_sl_last_update_ts": now,
                "native_sl_last_result": "PAPER_SIMULATED",
                "native_sl_last_reason": "paper_mode_no_exchange_stop_move",
            })
            record_trade_management_event(
                "NATIVE_SL_MOVE", "PAPER_SIMULATED", log_level="INFO",
                dedupe_key=f"native_sl:{symbol}:{new_price:.8f}", dedupe_seconds=60,
                **record, order_id=STATE.get("native_sl_order_id"))
            return True, "PAPER_SIMULATED", record

        if (STATE.get("recovery_hold") or STATE.get("external_close_detected")
                or STATE.get("close_reconciliation_required") or STATE.get("execution_unresolved")):
            STATE.update({"native_sl_last_result": "BLOCKED_UNRESOLVED",
                          "native_sl_last_reason": "management_state_unresolved"})
            record_trade_management_event(
                "NATIVE_SL_MOVE", "BLOCKED", log_level="ERROR", **record)
            return False, "BLOCKED_UNRESOLVED", record

        qty = safe_float(STATE.get("remaining_qty"), safe_float(STATE.get("qty"), 0.0))
        previous_qty = safe_float(STATE.get("native_sl_original_qty"),
                                  safe_float(STATE.get("initial_qty"), qty))
        if qty <= 0:
            STATE.update({"native_sl_last_result": "INVALID_QUANTITY",
                          "native_sl_last_reason": "no_remaining_position_quantity"})
            return False, "INVALID_QUANTITY", record
        try:
            service = TradeExecutionService(
                ex, journal=globals().get("INSTITUTIONAL_TRADE_JOURNAL"),
                logger=lambda message: log_execution(f"[NATIVE_SL_MOVE] {message}", "INFO"),
                api_caller=safe_api_call,
            )
            result = service.replace_native_stop(
                symbol=normalize_symbol(symbol), side=side, quantity=qty,
                new_trigger_price=new_price, mode=_state_position_mode(),
                position_side=STATE.get("position_side"),
                previous_order_id=STATE.get("native_sl_order_id"),
                previous_trigger_price=current or None,
                previous_quantity=previous_qty,
            )
        except Exception as exc:
            STATE.update({"native_sl_last_result": "EXCEPTION",
                          "native_sl_last_reason": type(exc).__name__})
            record_trade_management_event(
                "NATIVE_SL_MOVE", "EXCEPTION", log_level="ERROR",
                **record, error=f"{type(exc).__name__}: {exc}")
            return False, "EXCEPTION", record

        outcome = str(getattr(result, "outcome", "") or "").upper()
        result_reason = str(getattr(result, "reason", "") or "")
        order_id = getattr(result, "order_id", None) or STATE.get("native_sl_order_id")
        record.update({"outcome": outcome, "order_id": order_id, "service_reason": result_reason})
        STATE.update({
            "native_sl_last_requested_price": new_price,
            "native_sl_last_update_ts": now,
            "native_sl_last_result": outcome,
            "native_sl_last_reason": result_reason,
        })

        if outcome == "MOVED":
            STATE.update({
                "native_sl_order_id": order_id,
                "native_sl_last_verified_price": new_price,
                "native_sl_original_price": new_price,
                "native_sl_verified": True,
                "native_sl_status": "VERIFIED",
                "native_sl_policy": "EXCHANGE_VERIFIED_MOVED",
            })
            record_trade_management_event(
                "NATIVE_SL_MOVED", "EXCHANGE_VERIFIED", log_level="SUCCESS",
                dedupe_key=f"native_sl:{symbol}:{new_price:.8f}", dedupe_seconds=60,
                **record)
            return True, "MOVED", record

        if outcome == "RETAINED":
            # The previous stop was re-read and proven active. Keep it authoritative.
            STATE.update({
                "native_sl_order_id": order_id,
                "native_sl_verified": True,
                "native_sl_status": "VERIFIED_RETAINED",
                "native_sl_policy": "EXCHANGE_STOP_RETAINED_SYNTHETIC_ADVANCED",
            })
            record_trade_management_event(
                "NATIVE_SL_RETAINED", "EXCHANGE_VERIFIED", log_level="WARN",
                dedupe_key=f"native_sl:{symbol}:{new_price:.8f}", dedupe_seconds=60,
                **record)
            return False, "RETAINED", record

        STATE.update({
            "native_sl_verified": False,
            "native_sl_status": "UNVERIFIED",
            "native_sl_policy": "EXCHANGE_STOP_UNVERIFIED_HOLD",
            "recovery_hold": True,
            "recovery_reason": "native_sl_move_" + (result_reason or "lost"),
        })
        record_trade_management_event(
            "NATIVE_SL_MOVE_LOST", "RECOVERY_HOLD", log_level="ERROR",
            dedupe_key=f"native_sl:{symbol}:{new_price:.8f}", dedupe_seconds=60,
            **record)
        return False, "LOST", record


class LiveTradeManager:
    def __init__(self, event_bus, exchange_sync, recovery_guard):
        self.event_bus = event_bus
        self.exchange_sync = exchange_sync
        self.recovery = recovery_guard
        self.lifecycle_state = TradeLifecycleState.IDLE
        self.current_snapshot = None
        self.last_management_ts = 0
        self.last_log_ts = 0
        self.last_live_debug_ts = 0
        self.last_heavy_calc_ts = 0
        self.last_position_sync_ts = 0
        self.continuation_pressure_engine = ContinuationPressureEngine()
        self.thesis_failure_engine = ThesisFailureEngine()
        self.confidence_engine = ConfidenceEngine()
        self.regime_classifier = MarketRegimeClassifier()
        self.brain = InstitutionalTradeBrain()
        self.board = TradeManagementBoard()
        self._last_decision = None
        self.last_decision_blocked_reason = None
        event_bus.subscribe("reconciled", self._on_reconciled)
        event_bus.subscribe("force_close_local", self._force_close)
        event_bus.subscribe("lifecycle_change", self._set_lifecycle)

    def _set_lifecycle(self, state):
        self.lifecycle_state = state
        log_execution(f"[LIFECYCLE] New state: {state.value}", "INFO")
        DASHBOARD_STATE["lifecycle_state"] = state.value

    def _on_reconciled(self, snapshot):
        """Activate the sole manager only after exposure and native protection agree."""
        self.current_snapshot = snapshot
        if not STATE.get("open") or snapshot is None:
            return
        if str(snapshot.symbol) != str(STATE.get("current_symbol")):
            return
        if str(snapshot.side).upper() != str(STATE.get("side")).upper():
            return
        if (STATE.get("recovery_hold") or STATE.get("external_close_detected") or
                STATE.get("close_reconciliation_required") or STATE.get("execution_unresolved")):
            DASHBOARD_STATE["live_trade_mode"] = False
            return
        if not PAPER_MODE and not STATE.get("native_sl_verified"):
            self.lifecycle_state = TradeLifecycleState.OPEN_PENDING_CONFIRMATION
            DASHBOARD_STATE["live_trade_mode"] = False
            log_execution(f"[LIVE_MANAGER] {snapshot.symbol}: exchange position reconciled but Native SL is not verified; manager remains blocked", "CRITICAL")
            return
        if self.lifecycle_state in (TradeLifecycleState.RECOVERING,
                                    TradeLifecycleState.OPEN_PENDING_CONFIRMATION):
            self.lifecycle_state = TradeLifecycleState.LIVE
            DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.LIVE.value
            DASHBOARD_STATE["live_trade_mode"] = True
            self.event_bus.emit("lifecycle_change", TradeLifecycleState.LIVE)
            log_execution(f"[LIVE_MANAGER] {snapshot.symbol}: exposure, management state and protection confirmed; unified manager ACTIVE", "SUCCESS")

    def _force_close(self, payload):
        if STATE.get("open") and STATE.get("current_symbol"):
            reason = str((payload or {}).get("reason") if isinstance(payload, dict) else "FORCE_CLOSE_EVENT")
            if safe_close_position(STATE["current_symbol"], close_reason=reason) and STATE.get("close_verified"):
                record_trade_management_event("EMERGENCY_CLOSE_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                               log_level="ERROR", symbol=STATE.get("current_symbol"), side=STATE.get("side"),
                                               reason=reason, qty_closed=STATE.get("last_close_filled_qty"),
                                               fill_price=STATE.get("last_close_fill_price"), order_id=STATE.get("last_close_order_id"))
                finalize_trade_with_reality(STATE["current_symbol"], close_reason=reason)
                self.lifecycle_state = TradeLifecycleState.CLOSED
                DASHBOARD_STATE["live_trade_mode"] = False
            else:
                record_trade_management_event("EXECUTION_UNCERTAIN", "UNRESOLVED", log_level="ERROR",
                                               symbol=STATE.get("current_symbol"), side=STATE.get("side"), reason=reason,
                                               order_id=STATE.get("pending_close_order_id"),
                                               remaining_qty=STATE.get("remaining_qty"))

    def _emergency_exit_if_valid(self, symbol, now):
        """Independent emergency-exit path. NEVER uses board.evaluate or indicators.

        A genuine emergency must not depend on indicator snapshots. This handler
        validates the signal (source present, timestamp fresh) and the real
        exchange position state before executing a verified close. It returns True
        only when a close was executed and confirmed.

        The signal is expected to be raised outside the indicator path, e.g.:
            STATE["emergency_exit"] = True
            STATE["emergency_reason"] = "MANUAL_KILL_SWITCH"
            STATE["emergency_source"] = "DASHBOARD"
            STATE["emergency_ts"] = time.time()
        """
        if not bool(STATE.get("emergency_exit", False)):
            return False
        reason = str(STATE.get("emergency_reason") or "EMERGENCY_EXIT")
        source = str(STATE.get("emergency_source") or "")
        ts = safe_float(STATE.get("emergency_ts"), 0.0)
        max_age = safe_float(os.getenv("EMERGENCY_SIGNAL_MAX_AGE_SEC"), 30.0)
        if not source:
            log_execution(f"[EMERGENCY_EXIT_REJECTED] {symbol}: signal has no source; not executed",
                          "ERROR", debounce_key=f"emergency_nosrc_{symbol}", debounce_sec=30)
            return False
        if ts <= 0 or (now - ts) > max_age:
            age = (now - ts) if ts > 0 else -1.0
            log_execution(f"[EMERGENCY_EXIT_REJECTED] {symbol}: signal not fresh (age={age:.1f}s, "
                          f"max={max_age:.0f}s); not executed",
                          "ERROR", debounce_key=f"emergency_stale_{symbol}", debounce_sec=30)
            return False
        # The real position must be confirmed OPEN on the exchange before any close.
        status, position = fetch_position_tri_state(symbol)
        if status != "OPEN":
            log_execution(f"[EMERGENCY_EXIT_REJECTED] {symbol}: exchange position not confirmed OPEN "
                          f"({status}); no close sent",
                          "ERROR", debounce_key=f"emergency_nopos_{symbol}", debounce_sec=30)
            return False
        # Verified close via the existing exchange-verified close path.
        if safe_close_position(symbol, close_reason=reason) and STATE.get("close_verified"):
            record_trade_management_event(
                "EMERGENCY_CLOSE_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                log_level="ERROR", symbol=symbol, side=STATE.get("side"), reason=reason,
                source=source, qty_closed=STATE.get("last_close_filled_qty"),
                fill_price=STATE.get("last_close_fill_price"), order_id=STATE.get("last_close_order_id"))
            finalize_trade_with_reality(symbol, close_reason=reason)
            self.lifecycle_state = TradeLifecycleState.CLOSED
            DASHBOARD_STATE["live_trade_mode"] = False
            STATE["emergency_exit"] = False
            log_execution(f"[EMERGENCY_EXIT_EXECUTED] {symbol}: verified close reason={reason} source={source}",
                          "ERROR")
            return True
        record_trade_management_event("EXECUTION_UNCERTAIN", "UNRESOLVED", log_level="ERROR",
                                       symbol=symbol, side=STATE.get("side"), reason=reason,
                                       order_id=STATE.get("pending_close_order_id"),
                                       remaining_qty=STATE.get("remaining_qty"))
        return False

    def start_trade(self, symbol, side, entry_price, qty, sl, tp1, tp2):
        # Do not inherit a previous position's reconcile throttle, cached regime
        # or state-machine phase. Discovery/watchlist machines are separate.
        self.last_position_sync_ts = 0.0
        self.last_heavy_calc_ts = 0.0
        self.brain.current_state = "RANGE_CHOP"
        STATE["trade_state"] = "RANGE_CHOP"
        STATE.pop("last_trade_state_candle_ts", None)
        if hasattr(self.exchange_sync, "_last_reconcile"):
            self.exchange_sync._last_reconcile = 0.0
        if PAPER_MODE:
            self.lifecycle_state = TradeLifecycleState.LIVE
            self.event_bus.emit("lifecycle_change", TradeLifecycleState.LIVE)
        else:
            self.lifecycle_state = TradeLifecycleState.OPEN_PENDING_CONFIRMATION
            self.event_bus.emit("lifecycle_change", TradeLifecycleState.OPEN_PENDING_CONFIRMATION)
        log_execution(f"[LIFECYCLE] Trade open requested for {symbol} {side}", "INFO")

    def manage_live_trade(self):
        if not (STATE.get("open") and STATE.get("current_symbol")):
            if self.lifecycle_state not in (TradeLifecycleState.IDLE, TradeLifecycleState.CLOSED):
                self.lifecycle_state = TradeLifecycleState.IDLE
                DASHBOARD_STATE["live_trade_mode"] = False
            return
        now = time.time()
        roe = STATE.get("roe_pct", 0.0)
        adx = STATE.get("adx_live", 20.0)
        # Adaptive interval: calm trade -> 5 seconds, volatile -> 2 seconds
        calm_conditions = abs(roe) < 1.5 and 18 < adx < 30
        target_interval = 5 if calm_conditions else 2
        if now - self.last_management_ts < target_interval:
            return
        self.last_management_ts = now
        symbol = STATE["current_symbol"]
        # Emergency close gets first refusal before routine reconciliation and
        # Native-SL/Recovery-Hold gates. The handler independently verifies the
        # fresh signal and exchange OPEN state before submitting any close.
        if bool(STATE.get("emergency_exit", False)):
            with _TRADE_LOCK:
                if self._emergency_exit_if_valid(symbol, now):
                    self._log_live_status()
                    return
        if STATE.get("execution_unresolved"):
            if now - self.last_position_sync_ts >= 2:
                self.exchange_sync.reconcile(symbol, STATE)
                self.last_position_sync_ts = now
            log_execution(f"[EXECUTION_UNRESOLVED] {symbol}: management actions paused until exchange order outcome is reconciled", "ERROR",
                          debounce_key=f"execution_unresolved_{symbol}", debounce_sec=30)
            return
        if not PAPER_MODE and (STATE.get("recovery_hold") or not STATE.get("native_sl_verified")):
            log_execution(f"[MANAGEMENT_BLOCKED] {symbol}: Recovery Hold / Native SL verification is not complete; no TP or synthetic close actions", "CRITICAL",
                          debounce_key=f"management_protection_gate_{symbol}", debounce_sec=30)
            return
        with _TRADE_LOCK:
            # Position reconciliation every 10 seconds
            if now - self.last_position_sync_ts >= 10:
                self.exchange_sync.reconcile(symbol, STATE)
                self.last_position_sync_ts = now
            # Execute management decision
            self._apply_management(symbol, now)
        self._log_live_status()

    def _log_live_status(self):
        now = time.time()
        if now - self.last_log_ts < 15:
            return
        self.last_log_ts = now
        if not STATE.get("open"):
            return
        side = str(STATE.get("side", "?")).upper()
        symbol = str(STATE.get("current_symbol") or "UNKNOWN")
        entry = safe_float(STATE.get("entry"))
        mark = safe_float(STATE.get("mark_price"))
        roe = safe_float(STATE.get("roe_pct"))
        pnl_usdt = safe_float(STATE.get("unrealized_pnl_usdt"))
        margin = safe_float(STATE.get("margin"))
        sl = safe_float(STATE.get("synthetic_sl", STATE.get("sl")))
        tp1 = safe_float(STATE.get("tp1_price", STATE.get("synthetic_tp1")))
        tp2 = safe_float(STATE.get("tp2_price"))
        remaining = safe_float(STATE.get("remaining_qty"), safe_float(STATE.get("qty")))
        mode = str(STATE.get("profit_mode") or STANDARD_MODE)
        if mode == RANGE_MODE:
            phase = f"RANGE_TARGET={safe_float(STATE.get('range_target_price')):.8f}"
        elif STATE.get("tp1_hit"):
            phase = "🥈 RUNNER / TP2"
        else:
            phase = "🥇 WAITING TP1"
        native_sl = "VERIFIED" if STATE.get("native_sl_verified") else str(STATE.get("native_sl_status") or "PENDING")
        state_str = str(STATE.get("trade_state") or self.brain.current_trade_state)
        log_execution(
            f"[🧠 PROFIT BRAIN] 📊 STATUS | {symbol} {side} | phase={phase} | mode={mode} | "
            f"Entry={entry:.8f} Mark={mark:.8f} ROE={roe:+.2f}% PnL={pnl_usdt:+.4f} USDT Margin={margin:.4f} | "
            f"QtyLeft={remaining:.8f} | NativeSL={native_sl} SL(local)={sl:.8f} | "
            f"TP1={tp1:.8f} {'✅' if STATE.get('tp1_hit') else '⏳'} | TP2={tp2:.8f} {'✅' if STATE.get('tp2_hit') else '⏳'} | "
            f"BE={bool(STATE.get('be_done'))} Trail(local)={bool(STATE.get('trail_activated'))} | "
            f"ADX={safe_float(STATE.get('adx_live')):.1f} DI+={safe_float(STATE.get('di_plus_live')):.1f} "
            f"DI-={safe_float(STATE.get('di_minus_live')):.1f} Continuation={safe_float(STATE.get('continuation_probability'),0.5):.2f} | "
            f"VWAPCross={STATE.get('vwap_cross_trigger', 'UNKNOWN')} Direction={STATE.get('vwap_cross_direction', 'UNKNOWN')} | "
            f"State={state_str} RecoveryHold={bool(STATE.get('recovery_hold'))}",
            "INFO", debounce_key=f"profit_brain_status_{symbol}", debounce_sec=15
        )

    def _apply_price_only_management(self, symbol, mark_price, reason):
        """Run verified price/target/stop checks without stale indicator authority."""
        if self.lifecycle_state != TradeLifecycleState.LIVE or not STATE.get("open"):
            return
        context = {
            "symbol": symbol, "side": str(STATE.get("side") or "").upper(),
            "entry": safe_float(STATE.get("entry")), "current_price": safe_float(mark_price),
            "atr": safe_float(STATE.get("atr")), "roe": safe_float(STATE.get("roe_pct")),
            "remaining_qty": safe_float(STATE.get("remaining_qty"), safe_float(STATE.get("qty"))),
            "initial_qty": safe_float(STATE.get("initial_qty"), safe_float(STATE.get("qty"))),
            "synthetic_sl": safe_float(STATE.get("synthetic_sl", STATE.get("sl"))),
            "trail_stop": safe_float(STATE.get("trail_stop")),
            "trail_active": bool(STATE.get("trail_activated")),
            "tp1_hit": bool(STATE.get("tp1_hit")), "tp2_hit": bool(STATE.get("tp2_hit")),
            "tp1_price": safe_float(STATE.get("tp1_price", STATE.get("synthetic_tp1"))),
            "tp2_price": safe_float(STATE.get("tp2_price")),
            "range_target_price": safe_float(STATE.get("range_target_price")),
            "range_target_move_pct": safe_float(STATE.get("range_target_move_pct")),
            "scalp_target_status": str(STATE.get("scalp_target_status") or "UNKNOWN"),
            "scalp_target_price": safe_float(STATE.get("scalp_target_price")),
            "profit_mode": str(STATE.get("profit_mode") or STANDARD_MODE),
            "entry_context": dict(STATE.get("entry_context_snapshot") or {}),
            "trade_state": "UNKNOWN", "continuation_probability": 0.5,
            "adx": 20.0, "smart_money": {}, "momentum": {},
            "be_done": bool(STATE.get("be_done")),
            "execution_unresolved": bool(STATE.get("execution_unresolved")),
            "close_reconciliation_required": bool(STATE.get("close_reconciliation_required")),
            "recovery_hold": bool(STATE.get("recovery_hold")),
            "indicator_decisions_allowed": False,
        }
        decision = self.board.evaluate(context)
        self._last_decision = decision
        self._execute_decision(decision, symbol)
        log_execution(
            f"[PRICE_ONLY_MANAGEMENT] {symbol} reason={reason} decision={decision.action.value} "
            f"detail={decision.reason}",
            "INFO", debounce_key=f"price_only_management:{symbol}:{decision.action.value}:{decision.reason}",
            debounce_sec=10,
        )

    def _apply_management(self, symbol, now):
        if not STATE.get("open"):
            return
        # Emergency exit is deliberately first: it must not depend on ticker,
        # OHLCV, completed candles, indicators, or lifecycle classification.
        if bool(STATE.get("emergency_exit", False)):
            if self._emergency_exit_if_valid(symbol, now):
                return
        if self.lifecycle_state != TradeLifecycleState.LIVE:
            return

        # Always refresh the mark price for live exit checks; cached trade state is only a fallback.
        mark_price = safe_float(get_ticker_safe(symbol), 0.0)
        if mark_price <= 0:
            mark_price = safe_float(STATE.get("mark_price"), 0.0)
        if mark_price <= 0:
            log_execution(f"[PROFIT_BRAIN] {symbol}: no reliable mark price; management held", "WARN")
            return
        STATE["mark_price"] = mark_price

        # Strategy classification must use completed candles only.
        # NOTE: the OHLCV limit MUST be >= the fetcher minimum (100); requesting
        # fewer candles made fetch_ohlcv return None and froze every indicator at
        # its entry value for the whole life of the trade.
        try:
            raw_frame, ohlcv_source = get_ohlcv_safe_meta(symbol, 120)
        except Exception as exc:
            # A market-data failure must never raise out of management: keep the
            # current protection (Native SL / TP) untouched and retry next cycle.
            log_execution(
                f"[MARKET_DATA_REFRESH] {symbol}: fetch error {type(exc).__name__}: {exc}; "
                f"keep current protection (Native SL unchanged)",
                "WARN", debounce_key=f"mkt_fetch_error_{symbol}", debounce_sec=30)
            STATE["last_market_data_freshness"] = "INVALID"
            self._apply_price_only_management(symbol, mark_price, "ohlcv_fetch_error")
            return
        df_live = _closed_candle_frame(raw_frame, INTERVAL)
        received = 0 if raw_frame is None else int(len(raw_frame))
        closed_count = 0 if df_live is None else int(len(df_live))
        last_closed_ts = None
        current_candle_ts = None
        if raw_frame is not None and not getattr(raw_frame, "empty", True):
            try:
                raw_last = float(raw_frame["timestamp"].iloc[-1]) if "timestamp" in raw_frame.columns else float(raw_frame.index[-1])
                raw_last = raw_last / 1000.0 if raw_last > 1e12 else raw_last
                duration = _timeframe_seconds(INTERVAL)
                if time.time() < raw_last + duration:
                    current_candle_ts = raw_last
                    if closed_count > 0:
                        v = float(df_live["timestamp"].iloc[-1]) if "timestamp" in df_live.columns else float(df_live.index[-1])
                        last_closed_ts = v / 1000.0 if v > 1e12 else v
                else:
                    last_closed_ts = raw_last
            except Exception:
                pass
        fetch_status = "OK" if raw_frame is not None else "NONE"
        if df_live is None or len(df_live) < 30:
            log_execution(
                f"[MARKET_DATA_REFRESH]\n"
                f"  Symbol: {symbol}\n"
                f"  Timeframe: {INTERVAL}\n"
                f"  Fetch Time: {now:.3f}\n"
                f"  Fetch Status: {fetch_status}\n"
                f"  Last Closed Candle: {last_closed_ts}\n"
                f"  Current Candle: {current_candle_ts}\n"
                f"  Received Candle Count: {received}\n"
                f"  Required Candle Count: {REQUIRED_CLOSED_CANDLES}\n"
                f"  OHLCV Freshness: INSUFFICIENT\n"
                f"  Data Source: {ohlcv_source}\n"
                f"  Cache Status: {'HIT' if ohlcv_source == 'CACHE_HIT' else 'MISS'}\n"
                f"  -> insufficient completed candles; keep current protection (Native SL unchanged)",
                "WARN", debounce_key=f"mkt_insufficient_{symbol}", debounce_sec=30)
            STATE["last_market_data_freshness"] = "INSUFFICIENT"
            self._apply_price_only_management(symbol, mark_price, "insufficient_closed_candles")
            return
        # Freshness relative to the last closed candle we processed.
        prev_closed = safe_float(STATE.get("last_management_closed_candle_ts"), 0.0)
        if last_closed_ts is not None and prev_closed and last_closed_ts == prev_closed:
            data_freshness = "UNCHANGED_BUT_VALID"
        elif last_closed_ts is not None and prev_closed and last_closed_ts < prev_closed:
            data_freshness = "STALE"
        else:
            data_freshness = "FRESH"
        log_execution(
            f"[MARKET_DATA_REFRESH]\n"
            f"  Symbol: {symbol}\n"
            f"  Timeframe: {INTERVAL}\n"
            f"  Fetch Time: {now:.3f}\n"
            f"  Fetch Status: {fetch_status}\n"
            f"  Last Closed Candle: {last_closed_ts}\n"
            f"  Current Candle: {current_candle_ts}\n"
            f"  Received Candle Count: {received}\n"
            f"  Required Candle Count: {REQUIRED_CLOSED_CANDLES}\n"
            f"  OHLCV Freshness: {data_freshness}\n"
            f"  Data Source: {ohlcv_source}\n"
            f"  Cache Status: {'HIT' if ohlcv_source == 'CACHE_HIT' else 'MISS'}",
            "INFO", debounce_key=f"mkt_refresh_{symbol}", debounce_sec=15)
        STATE["last_market_fetch_ts"] = now
        STATE["last_market_data_freshness"] = data_freshness
        STATE["market_received_candles"] = received
        # Track actual favourable extremes from live mark prices for trailing decisions.
        if str(STATE.get("side")).upper() == "BUY":
            STATE["max_price"] = max(safe_float(STATE.get("max_price"), STATE.get("entry", mark_price)), mark_price)
        else:
            prior_min = safe_float(STATE.get("min_price"), STATE.get("entry", mark_price))
            STATE["min_price"] = min(prior_min, mark_price) if prior_min > 0 else mark_price
        atr = safe_float(compute_atr(df_live).iloc[-1], mark_price * 0.01) if len(df_live) > 14 else mark_price * 0.01
        side = STATE["side"]
        entry = STATE["entry"]
        roe = safe_float(STATE.get("roe_pct"), 0.0)
        peak_roe = max(safe_float(STATE.get("peak_roe"), 0.0), roe)
        STATE["peak_roe"] = peak_roe
        STATE["peak_drawdown_roe"] = max(0.0, peak_roe - roe) if peak_roe > 0 else 0.0
        ema_context_live = ema_trend_context(df_live, side, atr)
        STATE["ema_profit_state"] = ema_context_live.get("state", "INSUFFICIENT_DATA")

        # No orderbook fetching during live trade
        ob = None

        # Heavy calculations every 5 seconds (ADX, DI, SmartMoney, Momentum, StateMachine)
        if now - self.last_heavy_calc_ts >= 5:
            # Preserve the legacy closed-candle EMA9/session-VWAP method and add
            # an independent EMA50/VWAP cross. This is management context, not
            # an order by itself. Open-position protection never waits for a cross.
            try:
                _cross_snapshot = _roro_ema9_vwap_proximity(
                    df_live, ema, ema_len=9,
                    timeframe_seconds=_timeframe_seconds(INTERVAL),
                    now_ms=int(now * 1000),
                )
                if isinstance(_cross_snapshot, dict):
                    _cross_snapshot = dict(_cross_snapshot)
                    _cross_ts_ms = safe_float(_cross_snapshot.get("candle_timestamp"), 0.0)
                    _cross_frame_seconds = float(_timeframe_seconds(INTERVAL))
                    _cross_age = max(0.0, float(now) - (_cross_ts_ms / 1000.0 + _cross_frame_seconds))
                    _cross_max_age = 2.0 * _cross_frame_seconds
                    _cross_event = bool(_cross_snapshot.get("cross_ema9") or _cross_snapshot.get("cross_ema50"))
                    _cross_snapshot.update({
                        "cross_age_seconds": _cross_age if _cross_event else None,
                        "cross_max_age_seconds": _cross_max_age,
                        "timeframe_seconds": _cross_frame_seconds,
                        "cross_fresh": bool(_cross_event and _cross_age <= _cross_max_age),
                    })
                    STATE["vwap_cross_snapshot"] = dict(_cross_snapshot)
                    STATE["fresh_ema9_vwap_cross"] = bool(_cross_snapshot.get("cross_ema9") and _cross_snapshot.get("cross_fresh"))
                    STATE["fresh_ema50_vwap_cross"] = bool(_cross_snapshot.get("cross_ema50") and _cross_snapshot.get("cross_fresh"))
                    STATE["vwap_cross_direction"] = str(_cross_snapshot.get("cross_any") or "")
                    STATE["vwap_cross_trigger"] = str(_cross_snapshot.get("cross_trigger") or "NONE")
                    STATE["vwap_cross_candle_timestamp"] = int(_cross_snapshot.get("candle_timestamp") or 0)
                    STATE["vwap_cross_age_seconds"] = _cross_snapshot.get("cross_age_seconds")
                    STATE["vwap_cross_max_age_seconds"] = _cross_max_age
                else:
                    STATE["vwap_cross_snapshot"] = {}
                    STATE["fresh_ema9_vwap_cross"] = False
                    STATE["fresh_ema50_vwap_cross"] = False
                    STATE["vwap_cross_direction"] = ""
                    STATE["vwap_cross_trigger"] = "UNKNOWN"
                    STATE["vwap_cross_age_seconds"] = None
                    STATE["vwap_cross_max_age_seconds"] = 2.0 * float(_timeframe_seconds(INTERVAL))
            except Exception as _cross_error:
                STATE["vwap_cross_snapshot"] = {}
                STATE["fresh_ema9_vwap_cross"] = False
                STATE["fresh_ema50_vwap_cross"] = False
                STATE["vwap_cross_direction"] = ""
                STATE["vwap_cross_trigger"] = "UNKNOWN"
                log_execution(
                    f"[EMA_VWAP_CROSS] {symbol}: {type(_cross_error).__name__}: {_cross_error}",
                    "WARN", debounce_key=f"ema_vwap_cross_{symbol}", debounce_sec=60
                )
            plus_di, minus_di, adx_now, adx_slope = get_di_components(df_live)
            if plus_di is None: plus_di = 20.0
            if minus_di is None: minus_di = 20.0
            if adx_now is None: adx_now = 20.0
            if adx_slope is None: adx_slope = 0.0

            # Pullback intelligence
            pullback_type = trend_engine.analyze_pullback(df_live, side, atr)
            weak_pullback = (pullback_type == "WEAK_PULLBACK")
            counter_displacement = 0.0
            last_candle = df_live.iloc[-1]
            if side == "SELL" and last_candle['close'] > last_candle['open']:
                body = abs(last_candle['close'] - last_candle['open'])
                if body > atr * 0.6:
                    counter_displacement = body / atr
            elif side == "BUY" and last_candle['close'] < last_candle['open']:
                body = abs(last_candle['close'] - last_candle['open'])
                if body > atr * 0.6:
                    counter_displacement = body / atr
            prior_volume = safe_float(df_live['volume'].iloc[-11:-1].mean()) if len(df_live) >= 11 else 0.0
            volume_ratio = (safe_float(df_live['volume'].iloc[-1]) / prior_volume) if prior_volume > 0 else None
            closed_candle_ts = safe_float(df_live['timestamp'].iloc[-1]) if 'timestamp' in df_live.columns else safe_float(df_live.index[-1])
            if closed_candle_ts > 1e12:
                closed_candle_ts /= 1000.0
            trend_health = trend_engine.get_trend_health(df_live, side)
            struct_shift = detect_structure_shift(df_live)
            structure_aligned = (side == "BUY" and struct_shift == "bullish_shift") or (side == "SELL" and struct_shift == "bearish_shift")
            STATE["last_structural_shift"] = struct_shift
            STATE["structure_aligned"] = bool(structure_aligned)
            market_state = {
                "atr": atr,
                "adx": adx_now,
                "adx_slope": adx_slope,
                "di_plus": plus_di,
                "di_minus": minus_di,
                "trend_health": trend_health,
                "weak_pullback": weak_pullback,
                "counter_displacement": counter_displacement,
                "volume_ratio": volume_ratio,
                "df": df_live,
                "last_candle": last_candle,
                "structure_aligned": structure_aligned,
                "continuation_pressure": 50
            }

            smart_money = SmartMoneyEngine.analyze_smart_money(df_live)
            momentum = MomentumFlowEngine.analyze_momentum_flow(df_live)
            # Refresh liquidity-event evidence from completed candles; the entry snapshot
            # is retained separately for provenance and is never mistaken for live evidence.
            try:
                _live_liq = _classify_live_liquidity(df_live, atr, build_liquidity_pools)
                STATE["management_liquidity"] = {
                    "event": str(getattr(_live_liq, "event", "UNKNOWN") or "UNKNOWN"),
                    "direction": str(getattr(_live_liq, "direction", "UNKNOWN") or "UNKNOWN"),
                    "level": safe_float(getattr(_live_liq, "level", 0.0), 0.0),
                    "confidence": safe_float(getattr(_live_liq, "confidence", 0.0), 0.0),
                    "reasons": list(getattr(_live_liq, "reasons", []) or []),
                    "confirmed_at": safe_float(getattr(_live_liq, "timestamp", 0.0), 0.0),
                    "source": "LIVE_CLOSED_CANDLE_LIQUIDITY_ENGINE",
                }
            except Exception as _live_liq_error:
                STATE["management_liquidity"] = {
                    "event": "UNKNOWN", "direction": "UNKNOWN", "level": 0.0,
                    "confidence": 0.0, "reasons": [type(_live_liq_error).__name__],
                    "source": "LIVE_CLOSED_CANDLE_LIQUIDITY_ENGINE",
                }

            regime = self.regime_classifier.classify(df_live, ob)
            state_candle_ts = safe_float(STATE.get("last_trade_state_candle_ts"), 0.0)
            current_candle_ts = safe_float(closed_candle_ts, 0.0)
            # The authoritative state machine advances once per closed candle,
            # matching watchlist machines. This prevents repeated intra-candle
            # RANGE_CHOP -> defensive chatter without changing any thresholds.
            advance_state = (not current_candle_ts or current_candle_ts != state_candle_ts)
            if advance_state:
                trade_state = self.brain.update(
                    smart_money, momentum, adx_now, regime, side=side,
                    source="LIVE_TRADE_MANAGER", symbol=STATE.get("current_symbol") or symbol,
                    candle_ts=current_candle_ts or None)
                STATE["last_trade_state_candle_ts"] = current_candle_ts
            else:
                trade_state = self.brain.current_trade_state
            STATE["trade_state"] = trade_state
            _liq_snapshot = STATE.get("management_liquidity") or {}
            _trade_class, _trade_class_reasons = classify_dynamic_trade_classification(
                adx=adx_now,
                momentum_strength=momentum.get("momentum_health"),
                volume_ratio=volume_ratio,
                continuation_probability=STATE.get("continuation_probability", 0.5),
                structure_aligned=bool(STATE.get("structure_aligned", False)),
                structure_broken=bool(STATE.get("structure_broken", False)),
                counter_pressure=STATE.get("counter_pressure", 0.0),
                momentum_decay=bool(momentum.get("momentum_decay", False)),
                liquidity_supportive=safe_float(_liq_snapshot.get("confidence"), 0.0) >= 50.0,
            )
            STATE["trade_classification"] = _trade_class
            STATE["trade_classification_reasons"] = list(_trade_class_reasons)
            STATE.setdefault("trade_classification_history", []).append({
                "classification": _trade_class, "reasons": list(_trade_class_reasons),
                "closed_candle_timestamp": closed_candle_ts, "updated_at": time.time(),
            })
            STATE["trade_classification_history"] = STATE["trade_classification_history"][-20:]
            STATE["smart_trail_mult"] = self.brain.get_trail_multiplier()
            STATE["delay_tp1"] = self.brain.should_delay_tp1()
            STATE["adx_live"] = adx_now
            STATE["adx_slope"] = adx_slope
            STATE["volume_ratio"] = volume_ratio
            STATE["last_management_closed_candle_ts"] = closed_candle_ts
            STATE["di_plus_live"] = plus_di
            STATE["di_minus_live"] = minus_di
            STATE["smart_money"] = smart_money
            STATE["momentum_flow"] = momentum
            STATE["market_regime"] = regime
            # Increment the indicator snapshot version only when the closed candle
            # actually changed, so a fresh value is never confused with a repeat.
            if closed_candle_ts and closed_candle_ts != safe_float(STATE.get("last_indicator_calc_closed_ts"), 0.0):
                STATE["indicator_snapshot_version"] = int(safe_float(STATE.get("indicator_snapshot_version"), 0)) + 1
                STATE["last_indicator_calc_closed_ts"] = closed_candle_ts
                STATE["indicator_calc_ts"] = now
            STATE["indicator_snapshot_type"] = "CONFIRMED"
            try:
                rsi_series = compute_rsi(df_live)
                STATE["last_rsi"] = safe_float(rsi_series.iloc[-1], 0.0) if len(rsi_series) > 0 else 0.0
                vwap_series = compute_vwap(df_live)
                STATE["last_vwap"] = safe_float(vwap_series.iloc[-1], 0.0) if len(vwap_series) > 0 else 0.0
            except Exception:
                STATE["last_rsi"] = safe_float(STATE.get("last_rsi"), 0.0)
                STATE["last_vwap"] = safe_float(STATE.get("last_vwap"), 0.0)

            # Thesis and continuation
            thesis_dict = STATE.get("trade_thesis", {})
            cont_pressure_score, cont_pressure_reasons = self.continuation_pressure_engine.calculate_pressure(df_live, side, entry, atr, STATE.get("entry_time", time.time()))
            market_state["continuation_pressure"] = cont_pressure_score
            continuation_eval = _continuation_engine.evaluate(side, df_live, market_state, thesis_dict)
            STATE["continuation_probability"] = continuation_eval.continuation_probability
            STATE["hold_quality"] = continuation_eval.hold_quality
            STATE["counter_pressure"] = continuation_eval.counter_pressure
            STATE["reclaim_risk"] = continuation_eval.reclaim_risk
            STATE["trend_strength"] = continuation_eval.trend_strength
            STATE["continuation_reasons"] = continuation_eval.reasons
            STATE["continuation_pressure"] = cont_pressure_score

            # Unified profit-mode policy: range scalp is allowed only with multiple
            # independent weakness/thin-liquidity observations. Missing data is NOT range.
            mode_context = {
                "side": side, "regime": regime, "trade_state": trade_state,
                "entry": entry,
                "tp1_price": safe_float(STATE.get("tp1_price", STATE.get("synthetic_tp1", 0)), 0.0),
                "tp2_price": safe_float(STATE.get("tp2_price", 0), 0.0),
                "range_target_price": safe_float(STATE.get("scalp_target_price", STATE.get("range_target_price", 0)), 0.0),
                "range_target_move_pct": safe_float(STATE.get("scalp_target_selected_pct", 0), 0.0),
                "entry_context": dict(STATE.get("entry_context_snapshot") or {}),
                "market_intelligence": {
                **(STATE.get("market_intelligence") or {}),
                "zone_side": str(STATE.get("side") or "").upper(),
                "trading_mode": str(STATE.get("trading_mode") or "UNCLASSIFIED"),
                "entry_behavior": str(STATE.get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
                "zone_event_fresh": (
                    safe_float(STATE.get("zone_event_last_candle_ts"), 0.0) > 0
                    and 0 <= time.time() - safe_float(STATE.get("zone_event_last_candle_ts"), 0.0)
                    <= max(2 * _timeframe_seconds(INTERVAL), 300)
                ),
                "failed_breakout_streak": int(safe_float(STATE.get("failed_breakout_streak"), 0)),
                "failed_breakout_last_candle_ts": safe_float(STATE.get("failed_breakout_last_candle_ts"), 0.0),
                "failed_breakout_thesis_credit_applied": bool(STATE.get("failed_breakout_thesis_credit_applied", False)),
                },
                "adx": adx_now, "adx_slope": adx_slope,
                "continuation_probability": continuation_eval.continuation_probability,
                "di_plus": plus_di, "di_minus": minus_di,
                "volume_ratio": volume_ratio,
                "momentum_strength": momentum.get("momentum_health"),
                "spread_pct": STATE.get("entry_spread_pct"),
                "liquidity_quality": STATE.get("liquidity_quality", "UNKNOWN"),
                "orderbook_depth_usdt": STATE.get("orderbook_depth_usdt"),
                "min_orderbook_depth_usdt": STATE.get("min_orderbook_depth_usdt", 0),
                "structure_broken": bool(STATE.get("structure_broken", False)),
                "trend_expansion": bool(momentum.get("trend_expansion", False)),
                "fresh_ema9_vwap_cross": bool(STATE.get("fresh_ema9_vwap_cross", False)),
                "fresh_ema50_vwap_cross": bool(STATE.get("fresh_ema50_vwap_cross", False)),
                "vwap_cross_direction": str(STATE.get("vwap_cross_direction") or ""),
                "vwap_cross_trigger": str(STATE.get("vwap_cross_trigger") or "NONE"),
                "vwap_cross_age_seconds": STATE.get("vwap_cross_age_seconds"),
                "vwap_cross_max_age_seconds": STATE.get("vwap_cross_max_age_seconds"),
            }
            candidate_mode, candidate_reason, candidate_evidence = classify_profit_mode(mode_context)
            current_mode = str(STATE.get("profit_mode") or STANDARD_MODE)
            # Once a confirmed trend runner is established, ordinary weakening
            # cannot demote it into a one-shot scalp. A trend downgrade may move
            # to the standard 50/50 plan; only thesis/structure failure can close it.
            if current_mode == TREND_MODE and candidate_mode == RANGE_MODE:
                _trend_broken = bool(
                    ema_context_live.get("break_confirmed")
                    or STATE.get("structural_invalidation")
                    or safe_float(STATE.get("thesis_failure_score"), 0.0) >= 50.0
                )
                if not _trend_broken:
                    candidate_mode = STANDARD_MODE
                    candidate_reason = "trend_runner_protected_from_range_demotion"
                    candidate_evidence = tuple(list(candidate_evidence) + ["trend_mode_downgrade_guard"])
            last_mode_ts = safe_float(STATE.get("profit_mode_last_candle_ts"), 0.0)
            if closed_candle_ts > last_mode_ts:
                STATE["profit_mode_last_candle_ts"] = closed_candle_ts
                if candidate_mode == current_mode:
                    STATE["profit_mode_candidate"] = candidate_mode
                    STATE["profit_mode_candidate_count"] = 0
                elif not STATE.get("profit_mode"):
                    current_mode = candidate_mode
                    STATE["profit_mode"] = current_mode
                    STATE["profit_mode_candidate"] = candidate_mode
                    STATE["profit_mode_candidate_count"] = 0
                else:
                    if STATE.get("profit_mode_candidate") == candidate_mode:
                        STATE["profit_mode_candidate_count"] = int(STATE.get("profit_mode_candidate_count", 0)) + 1
                    else:
                        STATE["profit_mode_candidate"] = candidate_mode
                        STATE["profit_mode_candidate_count"] = 1
                    # Require two completed candles before changing an established mode.
                    if STATE["profit_mode_candidate_count"] >= 2:
                        current_mode = candidate_mode
                        STATE["profit_mode"] = current_mode
                        STATE["profit_mode_candidate_count"] = 0
                STATE["profit_mode_reason"] = candidate_reason
                STATE["profit_mode_evidence"] = list(candidate_evidence)
                # Additive management-mode trace. The classifier above remains the
                # only mode decision; this only labels and records transitions.
                mode_report = build_management_mode_report(
                    mode_context, profit_mode=current_mode,
                    reason=candidate_reason, evidence=candidate_evidence,
                )
                STATE["management_mode"] = mode_report["management_mode"]
                STATE["management_mode_report"] = mode_report
                previous_mode = ""
                history = STATE.get("management_mode_history")
                if not isinstance(history, list):
                    history = []
                if history:
                    previous_mode = str((history[-1] or {}).get("management_mode") or "")
                if previous_mode != mode_report["management_mode"]:
                    change_record = {
                        "at": time.time(), "candle_ts": closed_candle_ts,
                        "management_mode": mode_report["management_mode"],
                        "canonical_profit_mode": mode_report["canonical_profit_mode"],
                        "reason": mode_report["classification_reason"],
                        "selected_target_gross_roe_pct": mode_report["selected_target_gross_roe_pct"],
                    }
                    history = list(history)
                    history.append(change_record)
                    STATE["management_mode_history"] = history[-20:]
                    STATE["management_mode_last_change_ts"] = closed_candle_ts
                    record_trade_management_event(
                        "MODE_CHANGE", "DECIDED", dedupe_key=f"mode:{STATE.get('current_symbol')}:{closed_candle_ts}",
                        dedupe_seconds=1, symbol=STATE.get("current_symbol"), side=side,
                        profit_mode=current_mode, management_mode=mode_report["management_mode"],
                        reason=mode_report["classification_reason"],
                        selected_target_roe_pct=mode_report["selected_target_gross_roe_pct"],
                        target_convention_basis=(mode_report.get("target_convention") or {}).get("basis"),
                    )
                # Build one opposing-liquidity snapshot from completed candles per candle.
                if closed_candle_ts > safe_float(STATE.get("opposing_liquidity_timestamp"), 0.0):
                    try:
                        _pool_data = build_liquidity_pools(df_live.tail(120).reset_index(drop=True))
                        _pool_levels = _pool_data.get("high_pools", []) if side == "BUY" else _pool_data.get("low_pools", [])
                        _pool = select_opposing_liquidity_pool(_pool_levels, side, mark_price)
                        STATE["opposing_liquidity_status"] = _pool.get("status", "UNKNOWN")
                        STATE["opposing_liquidity_level"] = safe_float(_pool.get("level"), 0.0)
                        STATE["opposing_liquidity_distance_pct"] = safe_float(_pool.get("distance_pct"), 0.0)
                        STATE["opposing_liquidity_timestamp"] = closed_candle_ts
                        _existing_scalp_target = safe_float(STATE.get("scalp_target_price"), 0.0)
                        _existing_scalp_status = str(STATE.get("scalp_target_status") or "UNKNOWN").upper()
                        # Latch the first valid entry-anchored target for this trade.
                        # Later swing-pool changes must never move a profit target farther away.
                        if _existing_scalp_status == "VALID" and _existing_scalp_target > 0:
                            pass
                        elif _pool.get("status") == "VALID":
                            _scalp_plan = build_scalp_target(side, entry, _pool.get("level"))
                            STATE["scalp_target_status"] = _scalp_plan.get("status", "UNKNOWN")
                            STATE["scalp_target_price"] = safe_float(_scalp_plan.get("target_price"), 0.0)
                            STATE["scalp_target_tp1_price"] = safe_float(_scalp_plan.get("tp1_target_price"), 0.0)
                            STATE["scalp_target_selected_pct"] = safe_float(_scalp_plan.get("target_pct"), 0.0)
                            STATE["scalp_target_selection_reason"] = str(_scalp_plan.get("reason") or "target_not_available")
                        else:
                            STATE["scalp_target_status"] = "NO_TARGET"
                            STATE["scalp_target_price"] = 0.0
                            STATE["scalp_target_tp1_price"] = 0.0
                            STATE["scalp_target_selected_pct"] = 0.0
                            STATE["scalp_target_selection_reason"] = "no_verified_opposing_pool"
                        # Rebuild confirmed opposing support/resistance from completed candles.
                        # These levels are exit-risk context only; they never create entries.
                        _sr_snapshot = build_support_resistance_zones(
                            df_live.tail(120).to_dict("records"),
                            price=mark_price, atr=atr, lookback=120,
                            pivot_span=2, cluster_atr=0.22, min_touches=2, max_zones=6,
                        )
                        _sr_key = "resistances" if side == "BUY" else "supports"
                        _sr_candidates = [
                            z for z in (_sr_snapshot.get(_sr_key) or [])
                            if isinstance(z, dict)
                            and str(z.get("status") or "").upper() == "CONFIRMED"
                            and safe_float(z.get("distance_atr"), 999.0) >= 0.0
                        ]
                        _sr_nearest = min(
                            _sr_candidates,
                            key=lambda z: safe_float(z.get("distance_atr"), 999.0),
                            default=None,
                        )
                        STATE["management_support_resistance"] = _sr_snapshot
                        STATE["opposing_zone_distance_atr"] = (
                            safe_float(_sr_nearest.get("distance_atr"), 999.0)
                            if _sr_nearest else None
                        )
                        STATE["opposing_zone_level"] = (
                            safe_float(_sr_nearest.get("level"), 0.0)
                            if _sr_nearest else 0.0
                        )
                        _sr_rejection = False
                        if _sr_nearest and safe_float(_sr_nearest.get("distance_atr"), 999.0) <= 0.50:
                            _last_sr_candle = df_live.iloc[-1]
                            _sr_range = max(
                                safe_float(_last_sr_candle.get("high")) - safe_float(_last_sr_candle.get("low")),
                                1e-12,
                            )
                            if side == "BUY":
                                _upper_wick = safe_float(_last_sr_candle.get("high")) - max(
                                    safe_float(_last_sr_candle.get("open")), safe_float(_last_sr_candle.get("close"))
                                )
                                _sr_rejection = (
                                    safe_float(_last_sr_candle.get("high")) >= safe_float(_sr_nearest.get("low"))
                                    and safe_float(_last_sr_candle.get("close")) < safe_float(_sr_nearest.get("low"))
                                    and _upper_wick / _sr_range >= 0.35
                                )
                            else:
                                _lower_wick = min(
                                    safe_float(_last_sr_candle.get("open")), safe_float(_last_sr_candle.get("close"))
                                ) - safe_float(_last_sr_candle.get("low"))
                                _sr_rejection = (
                                    safe_float(_last_sr_candle.get("low")) <= safe_float(_sr_nearest.get("high"))
                                    and safe_float(_last_sr_candle.get("close")) > safe_float(_sr_nearest.get("high"))
                                    and _lower_wick / _sr_range >= 0.35
                                )
                        STATE["opposing_zone_rejection"] = bool(_sr_rejection)
                        record_trade_management_event(
                            "OPPOSING_LIQUIDITY_REFRESH", "OBSERVED",
                            dedupe_key=f"opposing_pool:{symbol}:{side}:{closed_candle_ts}",
                            dedupe_seconds=0, symbol=symbol, side=side, price=mark_price,
                            reference_level=STATE["opposing_liquidity_level"],
                            distance_pct=STATE["opposing_liquidity_distance_pct"],
                            scalp_target_pct=STATE["scalp_target_selected_pct"],
                            scalp_target_price=STATE.get("scalp_target_price", 0.0),
                            scalp_target_tp1_price=STATE.get("scalp_target_tp1_price", 0.0),
                            scalp_target_status=STATE.get("scalp_target_status", "UNKNOWN"),
                            reason=STATE["scalp_target_selection_reason"],
                        )
                    except Exception as _pool_error:
                        STATE["opposing_liquidity_status"] = "UNKNOWN"
                        STATE["opposing_liquidity_level"] = 0.0
                        STATE["opposing_liquidity_distance_pct"] = 0.0
                        # Keep a previously latched target through transient pool-data errors.
                        if not (str(STATE.get("scalp_target_status") or "").upper() == "VALID"
                                and safe_float(STATE.get("scalp_target_price"), 0.0) > 0):
                            STATE["scalp_target_status"] = "UNKNOWN"
                            STATE["scalp_target_price"] = 0.0
                            STATE["scalp_target_tp1_price"] = 0.0
                            STATE["scalp_target_selected_pct"] = 0.0
                            STATE["scalp_target_selection_reason"] = "pool_analysis_error"
                        log_execution(
                            f"[OPPOSING_LIQUIDITY] {symbol}: {type(_pool_error).__name__}: {_pool_error}",
                            "WARN", debounce_key=f"opposing_pool_{symbol}", debounce_sec=60
                        )
                if current_mode == RANGE_MODE and not safe_float(STATE.get("range_target_price"), 0.0):
                    # Range exits are anchored to the nearest opposing swing-liquidity
                    # pool, not a hard-coded 0.5/1/2% price move. Use completed bars only.
                    swing_levels = []
                    try:
                        recent = df_live.tail(61).reset_index(drop=True)
                        for i in range(2, len(recent) - 2):
                            hi = safe_float(recent.iloc[i].get("high"), 0.0)
                            lo = safe_float(recent.iloc[i].get("low"), 0.0)
                            if hi > 0 and hi >= max(safe_float(x, 0.0) for x in recent["high"].iloc[i-2:i+3]):
                                swing_levels.append(hi)
                            if lo > 0 and lo <= min(safe_float(x, float("inf")) for x in recent["low"].iloc[i-2:i+3]):
                                swing_levels.append(lo)
                    except Exception:
                        swing_levels = []
                    adaptive_target, target_reason = adaptive_range_target_price(
                        side, entry, atr, swing_levels)
                    # Percentage objective is a PRICE move (0.25/0.50/1.00%), not leveraged ROE.
                    # Use the last three completed candles as follow-through evidence.
                    _aligned_closed = 0
                    for _candle_idx in range(max(0, len(df_live) - 4), len(df_live) - 1):
                        _candle = df_live.iloc[_candle_idx]
                        _candle_aligned = (
                            safe_float(_candle.get("close"), 0.0) > safe_float(_candle.get("open"), 0.0)
                            if side == "BUY" else
                            safe_float(_candle.get("close"), 0.0) < safe_float(_candle.get("open"), 0.0)
                        )
                        _aligned_closed += int(bool(_candle_aligned))
                    _target_pct, _target_pct_reason = select_range_target_pct({
                        **mode_context, "side": side,
                        "aligned_closed_candles": _aligned_closed,
                        "adverse_rejection": bool(STATE.get("adverse_rejection", False)),
                    })
                    _percent_target = range_target_price(side, entry, _target_pct)
                    _pool_level = safe_float(STATE.get("opposing_liquidity_level"), 0.0)
                    _scalp_target = safe_float(STATE.get("scalp_target_price"), 0.0)
                    if _pool_level > 0 and str(STATE.get("scalp_target_status") or "").upper() == "VALID" and _scalp_target > 0:
                        _pool_target = _pool_level - atr * 0.10 if side == "BUY" else _pool_level + atr * 0.10
                        _before_pool = _scalp_target < _pool_target if side == "BUY" else _scalp_target > _pool_target
                        if _before_pool and ((side == "BUY" and _scalp_target > entry) or (side == "SELL" and _scalp_target < entry)):
                            adaptive_target = round(_scalp_target, 12)
                            target_reason = str(STATE.get("scalp_target_selection_reason") or "entry_anchored_scalp_target")
                        else:
                            target_reason = "opposing_pool_geometry_rejected_atr_fallback"
                    elif _pool_level > 0:
                        target_reason = str(STATE.get("scalp_target_selection_reason") or "no_valid_scalp_target") + "_atr_fallback"
                    # Apply the percentage cap AFTER liquidity-pool geometry so no
                    # later pool adjustment can turn a scalp into a distant target.
                    if _percent_target > 0:
                        if side == "BUY":
                            adaptive_target = min(adaptive_target, _percent_target) if adaptive_target > 0 else _percent_target
                        else:
                            adaptive_target = max(adaptive_target, _percent_target) if adaptive_target > 0 else _percent_target
                    STATE["range_target_price"] = adaptive_target
                    STATE["range_target_move_pct"] = (abs(adaptive_target-entry) / entry * 100.0) if entry > 0 and adaptive_target > 0 else 0.0
                    STATE["range_target_reason"] = target_reason
                elif current_mode != RANGE_MODE:
                    STATE["range_target_price"] = 0.0
                    STATE["range_target_move_pct"] = 0.0
                log_execution(
                    f"[PROFIT_MODE] {symbol} entry_mode={STATE.get('trading_mode', 'UNCLASSIFIED')} "
                    f"management_mode={current_mode} candidate={candidate_mode} "
                    f"reason={candidate_reason} evidence={','.join(candidate_evidence)}", "INFO")

            # Thesis failure check
            failed, failure_reasons, failure_score = self.thesis_failure_engine.evaluate_failure(thesis_dict, market_state, mark_price, entry, side)
            STATE["thesis_failure_score"] = failure_score
            STATE["thesis_failure_reasons"] = list(failure_reasons)
            if failed:
                log_execution(f"[THESIS_FAILURE] Thesis warning for {symbol}: {failure_reasons}", "WARN")
                # Board will decide based on failure_score

            # Confidence update
            old_conf = safe_float(STATE.get("current_confidence"), 50.0)
            di_spread_change = (plus_di - minus_di) - safe_float(STATE.get("prev_di_spread"), 0.0)
            new_conf = self.confidence_engine.update_live_confidence(old_conf, cont_pressure_score, failure_score, adx_slope, di_spread_change)
            new_conf = ConfidenceEngine.apply_institutional_modifiers(new_conf, smart_money, momentum, continuation_eval.continuation_probability * 100)
            STATE["current_confidence"] = new_conf
            STATE["prev_di_spread"] = plus_di - minus_di

            self.last_heavy_calc_ts = now
        else:
            # Use cached values from STATE; all mode decisions still reference the last closed candle.
            volume_ratio = STATE.get("volume_ratio")
            struct_shift = STATE.get("last_structural_shift")
            adx_now = STATE.get("adx_live", 20.0)
            plus_di = STATE.get("di_plus_live", 20.0)
            minus_di = STATE.get("di_minus_live", 20.0)
            smart_money = STATE.get("smart_money", {})
            momentum = STATE.get("momentum_flow", {})
            trade_state = STATE.get("trade_state", "RANGE_CHOP")
            continuation_eval = ContinuationEvaluation(
                continuation_probability=safe_float(STATE.get("continuation_probability"), 0.5),
                trend_strength=safe_float(STATE.get("trend_strength"), 0.5),
                exhaustion_probability=0.0,
                reclaim_risk=safe_float(STATE.get("reclaim_risk"), 0.0),
                counter_pressure=safe_float(STATE.get("counter_pressure"), 0.0),
                confidence=0.5,
                reasons=STATE.get("continuation_reasons", []),
                should_hold=safe_float(STATE.get("continuation_probability"), 0.5) >= 0.62,
                hold_quality=STATE.get("hold_quality", "UNKNOWN")
            )

        # Detailed live metrics are merged into the bounded 🧠 PROFIT BRAIN status log.

        # ========== INDICATOR REFRESH (confirmed snapshot) ==========
        log_execution(
            f"[INDICATOR_REFRESH]\n"
            f"  Symbol: {symbol}\n"
            f"  ADX: {safe_float(STATE.get('adx_live'), 0.0):.2f}\n"
            f"  DI_PLUS: {safe_float(STATE.get('di_plus_live'), 0.0):.2f}\n"
            f"  DI_MINUS: {safe_float(STATE.get('di_minus_live'), 0.0):.2f}\n"
            f"  RSI: {safe_float(STATE.get('last_rsi'), 0.0):.2f}\n"
            f"  ATR: {atr:.8f}\n"
            f"  EMA: {STATE.get('ema_profit_state', 'n/a')}\n"
            f"  VWAP: {safe_float(STATE.get('last_vwap'), 0.0):.8f}\n"
            f"  Volume: {safe_float(STATE.get('volume_ratio'), 0.0):.4f}\n"
            f"  Continuation: {safe_float(STATE.get('continuation_probability'), 0.5):.2f}\n"
            f"  Market Regime: {STATE.get('market_regime', 'n/a')}\n"
            f"  Snapshot Type: CONFIRMED\n"
            f"  Snapshot Version: {int(safe_float(STATE.get('indicator_snapshot_version'), 0))}\n"
            f"  Last Closed Candle: {safe_float(STATE.get('last_management_closed_candle_ts'), 0.0)}\n"
            f"  Calculation Timestamp: {safe_float(STATE.get('indicator_calc_ts'), now):.3f}\n"
            f"  Freshness: {STATE.get('last_market_data_freshness', 'UNKNOWN')}\n"
            f"  Update Status: UPDATED",
            "INFO", debounce_key=f"ind_refresh_{symbol}", debounce_sec=15)

        # ========== DATA VALIDATION GATE ==========
        # Prove what Profit Brain is actually receiving this cycle, and refuse to
        # treat stale/invalid market data as fresh. Native SL is untouched by this
        # gate: a data problem never disables protection.
        _snap_ts = safe_float(STATE.get("last_management_closed_candle_ts"), 0.0)
        _now_ts = time.time()
        _freshness = str(STATE.get("last_market_data_freshness") or "UNKNOWN")
        _required = {
            "adx": STATE.get("adx_live") is not None,
            "di_plus": STATE.get("di_plus_live") is not None,
            "di_minus": STATE.get("di_minus_live") is not None,
            "continuation": STATE.get("continuation_probability") is not None,
            "regime": bool(STATE.get("market_regime")),
        }
        _missing = [k for k, ok in _required.items() if not ok]
        _stale = [] if _freshness in {"FRESH", "UNCHANGED_BUT_VALID"} else [f"market_data:{_freshness}"]
        _decision_allowed = not _stale and not _missing
        log_execution(
            f"[PROFIT_BRAIN_DATA_VALIDATION]\n"
            f"  Symbol: {symbol}\n"
            f"  Position ID: {STATE.get('trade_id')}\n"
            f"  Indicator Snapshot Version: {int(safe_float(STATE.get('indicator_snapshot_version'), 0))}\n"
            f"  Snapshot Timestamp: {_snap_ts}\n"
            f"  Last Closed Candle: {_snap_ts}\n"
            f"  Data Freshness: {_freshness}\n"
            f"  Required Indicators Available: {[k for k, ok in _required.items() if ok]}\n"
            f"  Stale Indicators: {_stale}\n"
            f"  Missing Indicators: {_missing}\n"
            f"  Decision Allowed: {_decision_allowed}\n"
            f"  Reject Reason: {'' if _decision_allowed else 'stale_or_missing_market_data'}",
            "INFO", debounce_key=f"pb_validation_{symbol}", debounce_sec=15)

        # ========== EXECUTION GATE (enforced, not just logged) ==========
        # When market data is not valid, NO new Profit-Brain decision may be taken
        # and no previously-computed decision may be executed. This is the only
        # routine decision authority, so blocking board.evaluate() blocks all
        # indicator-driven actions: SL move, breakeven, trailing, partial close,
        # full close and thesis exit.
        #
        # Native SL / TP1 / TP2 already placed on the exchange are NEVER touched by
        # this gate. Position monitoring, mark-price refresh, sync and reconciliation
        # run earlier in the cycle and continue normally.
        #
        # An independently-flagged emergency exit is handled by a dedicated path
        # that NEVER uses board.evaluate or any indicator snapshot. It validates
        # the signal and the real exchange position before closing.
        if bool(STATE.get("emergency_exit", False)):
            self._emergency_exit_if_valid(symbol, _now_ts)
            return
        if not _decision_allowed:
            self.last_decision_blocked_reason = "stale_or_missing_market_data"
            log_execution(
                f"[PRICE_ONLY_MANAGEMENT] {symbol}: indicator snapshot {_freshness}; "
                f"indicator-driven risk actions paused, but verified price/target/stop checks continue.",
                "WARN", debounce_key=f"decision_blocked_{symbol}", debounce_sec=30)
        else:
            self.last_decision_blocked_reason = None

        # ========== BUILD CONTEXT FOR BOARD ==========
        # Re-evaluate TP2 only after TP1 has actually filled. A weak continuation
        # closes the runner only when TP2 is objectively remote or structure is lost.
        structure_aligned = ((side == "BUY" and struct_shift == "bullish_shift") or
                             (side == "SELL" and struct_shift == "bearish_shift")) if "struct_shift" in locals() else bool(STATE.get("structure_aligned", False))
        tp2_action, tp2_reason = "HOLD", "tp1_not_filled"
        if STATE.get("tp1_hit"):
            tp2_action, tp2_reason = tp2_reassessment(
                side, mark_price, safe_float(STATE.get("tp2_price")), atr,
                safe_float(STATE.get("continuation_probability"), 0.5),
                safe_float(STATE.get("adx_live"), 20.0),
                safe_float(STATE.get("adx_slope"), 0.0), structure_aligned,
                max_atr_distance=safe_float(os.getenv("TP2_MAX_ATR_DISTANCE", "3.0"), 3.0),
                min_continuation=safe_float(os.getenv("TP2_MIN_CONTINUATION", "0.45"), 0.45))
        context = {
            "symbol": symbol, "side": side, "entry": entry, "current_price": mark_price,
            "indicator_decisions_allowed": bool(_decision_allowed),
            "atr": atr, "roe": roe,
            "remaining_qty": safe_float(STATE.get("remaining_qty"), STATE.get("qty", 0.0)),
            "initial_qty": safe_float(STATE.get("initial_qty"), STATE.get("qty", 0.0)),
            "smart_money": smart_money, "momentum": momentum,
            "exit_risk_snapshot": dict(STATE.get("exit_risk_snapshot") or {}),
            "entry_order_block": dict(STATE.get("entry_order_block") or {}),
            "entry_fvg": dict(STATE.get("entry_fvg") or {}),
            "entry_liquidity": dict(STATE.get("entry_liquidity") or {}),
            "entry_golden_zone": dict(STATE.get("entry_golden_zone") or {}),
            "distribution_risk": safe_float(smart_money.get("distribution_risk"), 0.0),
            "exhaustion_risk": safe_float(momentum.get("exhaustion_risk"), 0.0),
            "climax_risk": safe_float(momentum.get("climax_risk"), 0.0),
            "momentum_decay": bool(momentum.get("momentum_decay", False)),
            "flow_bias": str(momentum.get("flow_bias") or "UNKNOWN"),
            "counter_pressure": safe_float(STATE.get("counter_pressure"), 0.0),
            "reclaim_risk": safe_float(STATE.get("reclaim_risk"), 0.0),
            "hold_quality": str(STATE.get("hold_quality") or "UNKNOWN"),
            "trend_strength": safe_float(STATE.get("trend_strength"), 0.0),
            "structure_aligned": bool(STATE.get("structure_aligned", False)),
            "structure_broken": bool(STATE.get("structure_broken", False)),
            "structural_invalidation": bool(STATE.get("structural_invalidation", False)),
            "opposing_zone_distance_atr": safe_float(STATE.get("opposing_zone_distance_atr"), None),
            "management_support_resistance": dict(STATE.get("management_support_resistance") or {"status": "UNKNOWN", "supports": [], "resistances": []}),
            "opposing_zone_level": safe_float(STATE.get("opposing_zone_level"), 0.0),
            "opposing_zone_rejection": bool(STATE.get("opposing_zone_rejection", False)),
            "liquidity": dict(STATE.get("management_liquidity") or STATE.get("entry_liquidity") or {}),
            "zone_event": str((STATE.get("market_intelligence") or {}).get("zone_event") or "UNKNOWN"),
            "adx": adx_now, "adx_slope": safe_float(STATE.get("adx_slope")),
            "di_plus": plus_di, "di_minus": minus_di,
            "regime": STATE.get("market_regime", "UNKNOWN"),
            "thesis": STATE.get("trade_thesis", {}),
            "continuation_eval": continuation_eval,
            "continuation_probability": safe_float(STATE.get("continuation_probability"), 0.5),
            "momentum_strength": safe_float(momentum.get("momentum_health"), 50.0),
            "volume_ratio": volume_ratio,
            "thesis_failure_score": STATE.get("thesis_failure_score", 0),
            "thesis_failure_reason": ",".join(STATE.get("thesis_failure_reasons", [])),
            "thesis_failure_reasons": list(STATE.get("thesis_failure_reasons", [])),
            "structural_invalidation": bool(STATE.get("structural_invalidation", False)),
            "max_price": STATE.get("max_price", entry), "min_price": STATE.get("min_price", entry),
            "trail_active": STATE.get("trail_activated", False), "trail_stop": STATE.get("trail_stop", 0),
            "tp1_hit": STATE.get("tp1_hit", False), "tp2_hit": STATE.get("tp2_hit", False),
            "tp1_price": STATE.get("tp1_price", STATE.get("synthetic_tp1", 0)),
            "synthetic_sl": STATE.get("synthetic_sl", 0),
            "synthetic_tp1": STATE.get("synthetic_tp1", 0), "tp2_price": STATE.get("tp2_price", 0),
            "range_target_price": STATE.get("range_target_price", 0),
            "range_target_move_pct": STATE.get("range_target_move_pct", 0),
            "opposing_liquidity_status": STATE.get("opposing_liquidity_status", "UNKNOWN"),
            "opposing_liquidity_level": safe_float(STATE.get("opposing_liquidity_level"), 0.0),
            "scalp_target_status": STATE.get("scalp_target_status", "UNKNOWN"),
            "scalp_target_price": safe_float(STATE.get("scalp_target_price"), 0.0),
            "scalp_target_tp1_price": safe_float(STATE.get("scalp_target_tp1_price"), 0.0),
            "scalp_target_selected_pct": safe_float(STATE.get("scalp_target_selected_pct"), 0.0),
            "scalp_target_selection_reason": str(STATE.get("scalp_target_selection_reason") or ""),
            "opposing_liquidity": {
                "status": STATE.get("opposing_liquidity_status", "UNKNOWN"),
                "level": safe_float(STATE.get("opposing_liquidity_level"), 0.0),
                "distance_pct": safe_float(STATE.get("opposing_liquidity_distance_pct"), 0.0),
                "source": "CLOSED_CANDLE_SWING_POOLS",
                "timestamp": safe_float(STATE.get("opposing_liquidity_timestamp"), 0.0),
            },

            "profit_mode": STATE.get("profit_mode", STANDARD_MODE),
            "entry_context": dict(STATE.get("entry_context_snapshot") or {}),
            "profit_mode_reason": STATE.get("profit_mode_reason", ""),
            "fresh_ema9_vwap_cross": bool(STATE.get("fresh_ema9_vwap_cross", False)),
            "fresh_ema50_vwap_cross": bool(STATE.get("fresh_ema50_vwap_cross", False)),
            "vwap_cross_direction": str(STATE.get("vwap_cross_direction") or ""),
            "vwap_cross_trigger": str(STATE.get("vwap_cross_trigger") or "NONE"),
            "vwap_cross_snapshot": dict(STATE.get("vwap_cross_snapshot") or {}),
            "vwap_cross_age_seconds": STATE.get("vwap_cross_age_seconds"),
            "vwap_cross_max_age_seconds": STATE.get("vwap_cross_max_age_seconds"),
            "entry_mode": str(STATE.get("trading_mode") or "UNCLASSIFIED"),
            "entry_behavior": str(STATE.get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
            "entry_mode_confidence": safe_float(STATE.get("entry_behavior_confidence"), 0.0),
            "tp2_decision": tp2_action, "tp2_decision_reason": tp2_reason,
            "entry_type": STATE.get("entry_type", ""), "trade_state": trade_state,
            "trade_classification": str(STATE.get("trade_classification") or "UNCLASSIFIED"),
            "trade_classification_reasons": list(STATE.get("trade_classification_reasons") or []),
            "entry_time": safe_float(STATE.get("entry_time"), time.time()),
            "native_sl_verified": bool(STATE.get("native_sl_verified")),
            "protection_status": "PAPER_SIMULATED" if PAPER_MODE else ("EXCHANGE_VERIFIED" if STATE.get("native_sl_verified") else "UNVERIFIED"),
            "execution_result": "UNRESOLVED" if STATE.get("execution_unresolved") else "NOT_SUBMITTED",
            "exchange_verification_result": "RECOVERY_HOLD" if STATE.get("recovery_hold") else "POSITION_SYNCED",
            "risk_state": "BLOCKED_DATA_QUALITY" if not _decision_allowed else ("PROTECTED" if STATE.get("tp1_hit") else "MANAGING"),
            "evidence_used": ["ADX", "DI", "CONTINUATION", "REGIME", "EMA9_VWAP", "EMA50", "LIQUIDITY",
                              "SMART_MONEY", "MOMENTUM", "STRUCTURE"],
            "evidence_missing": list(_missing) + list(_stale),
            "be_done": STATE.get("be_done", False), "breakeven_done": STATE.get("be_done", False),
            "partial_done": STATE.get("partial_closed", False),
            "execution_unresolved": STATE.get("execution_unresolved", False),
            "close_reconciliation_required": STATE.get("close_reconciliation_required", False),
            "recovery_hold": STATE.get("recovery_hold", False),
            "smart_trail_mult": self.brain.get_trail_multiplier(),
            "peak_roe": safe_float(STATE.get("peak_roe"), 0.0),
            "peak_drawdown_roe": safe_float(STATE.get("peak_drawdown_roe"), 0.0),
            "distribution_risk": safe_float(smart_money.get("distribution_risk", 0.0)),
            "exit_warning": int(safe_float(STATE.get("exit_warning", 0))),
            "ema_state": str(ema_context_live.get("state", "INSUFFICIENT_DATA")),
            "emergency_exit": bool(STATE.get("emergency_exit", False)),
            "emergency_reason": STATE.get("emergency_reason", ""),
        }

        # Closed-candle zone lifecycle; context only, no order side effects.
        try:
            _zone_low = safe_float(STATE.get("entry_zone_low"), 0.0)
            _zone_high = safe_float(STATE.get("entry_zone_high"), 0.0)
            _last_zone_ts = safe_float(STATE.get("zone_event_last_candle_ts"), 0.0)
            if last_closed_ts is not None and _zone_low > 0 and _zone_high > _zone_low and len(df_live) >= 3 and last_closed_ts > _last_zone_ts:
                # Re-derive the completed-candle frame at the point of use. This
                # prevents a live/in-progress tail row from entering the zone
                # classifier even if the upstream fetch format changes.
                _zone_frame = _closed_candle_frame(raw_frame, INTERVAL)
                if _zone_frame is None or getattr(_zone_frame, "empty", True):
                    _zone_frame = df_live.copy()
                _zone_frame = _zone_frame.tail(12).copy()
                if "timestamp" not in _zone_frame.columns:
                    _zone_frame["timestamp"] = _zone_frame.index
                _zone_rows = []
                for _candle in _zone_frame[["timestamp", "open", "high", "low", "close"]].to_dict("records"):
                    _stamp = safe_float(_candle.get("timestamp"), 0.0)
                    if _stamp > 1e12:
                        _stamp /= 1000.0
                    _candle["timestamp"] = _stamp
                    _zone_rows.append(_candle)
                _zone_result = classify_zone_event(
                    _zone_rows, side=side, zone_low=_zone_low, zone_high=_zone_high, atr=atr
                )
                if _zone_result.get("event") != "INSUFFICIENT_DATA":
                    _event_ts = safe_float(_zone_result.get("timestamp"), last_closed_ts)
                    if _event_ts > _last_zone_ts:
                        _prior_event = str(STATE.get("zone_event") or "").upper()
                        _prior_streak = int(safe_float(STATE.get("failed_breakout_streak"), 0))
                        _management_event, _failure_streak = advance_failed_breakout_confirmation(
                            previous_event=_prior_event,
                            previous_timestamp=_last_zone_ts,
                            previous_streak=_prior_streak,
                            current_event=_zone_result["event"],
                            current_timestamp=_event_ts,
                            timeframe_seconds=_timeframe_seconds(INTERVAL),
                        )
                        _failure_active = (
                            _management_event == "FAILED_BREAKOUT" and _failure_streak >= 2
                        )
                        _previous_mi = STATE.get("market_intelligence") or {}
                        _mi_confidence = (
                            max(safe_float(_previous_mi.get("zone_event_confidence"), 0.0),
                                safe_float(_zone_result.get("confidence"), 0.0))
                            if _failure_active else safe_float(_zone_result.get("confidence"), 0.0)
                        )
                        STATE["zone_event"] = _management_event
                        STATE["zone_event_last_candle_ts"] = _event_ts
                        STATE["failed_breakout_streak"] = _failure_streak
                        STATE["failed_breakout_last_candle_ts"] = _event_ts if _failure_active else 0.0
                        STATE["market_intelligence"] = {
                            **_previous_mi,
                            "zone_event": _management_event,
                            "raw_zone_event": _zone_result["event"],
                            "zone_event_confirmed": (bool(_zone_result.get("confirmed")) if _management_event != "FAILED_BREAKOUT" else _failure_active),
                            "zone_event_confidence": _mi_confidence,
                            "zone_reference_level": safe_float(_zone_result.get("level"), 0.0),
                            "zone_event_reasons": list(_zone_result.get("reason_codes") or []),
                            "zone_event_timestamp": _event_ts,
                            "zone_event_fresh": (
                                0 <= time.time() - _event_ts
                                <= max(2 * _timeframe_seconds(INTERVAL), 300)
                            ),
                            "zone_side": str(side or "").upper(),
                            "failed_breakout_streak": _failure_streak,
                            "failed_breakout_last_candle_ts": _event_ts if _failure_active else 0.0,
                            "failed_breakout_thesis_credit_applied": bool(STATE.get("failed_breakout_thesis_credit_applied", False)),
                        }
                        record_trade_management_event(
                            "MARKET_STRUCTURE_EVENT",
                            "CONFIRMED" if (bool(_zone_result.get("confirmed")) and _management_event != "FAILED_BREAKOUT") or _failure_active else "OBSERVED",
                            dedupe_key=f"zone:{symbol}:{_management_event}:{_event_ts}",
                            dedupe_seconds=0, symbol=symbol, side=side,
                            reason=f"{_management_event}; raw={_zone_result['event']}; streak={_failure_streak}",
                            price=mark_price, reference_level=_zone_result.get("level"),
                            confidence=_mi_confidence,
                        )
        except Exception as _zone_error:
            log_execution(
                f"[MARKET_INTELLIGENCE] {symbol}: {type(_zone_error).__name__}: {_zone_error}",
                "WARN", debounce_key=f"zone_observation_{symbol}", debounce_sec=60
            )

        # Refresh the intelligence snapshot AFTER the closed-candle observer.
        # The management context is constructed earlier in this cycle, so without
        # this hand-off the board would only ever see the previous candle's evidence.
        try:
            _mi_context = dict(STATE.get("market_intelligence") or {})
            _mi_event_ts = safe_float(STATE.get("zone_event_last_candle_ts"), 0.0)
            _mi_context.update({
                "zone_event_fresh": (
                    _mi_event_ts > 0
                    and 0 <= time.time() - _mi_event_ts
                    <= max(2 * _timeframe_seconds(INTERVAL), 300)
                ),
                "zone_side": str(side or "").upper(),
                "failed_breakout_streak": int(safe_float(STATE.get("failed_breakout_streak"), 0)),
                "failed_breakout_thesis_credit_applied": bool(
                    STATE.get("failed_breakout_thesis_credit_applied", False)
                ),
            })
            context["market_intelligence"] = _mi_context
        except Exception as _mi_context_error:
            context["market_intelligence"] = {}
            log_execution(
                f"[MARKET_INTELLIGENCE_CONTEXT] {symbol}: "
                f"{type(_mi_context_error).__name__}: {_mi_context_error}",
                "WARN", debounce_key=f"mi_context_{symbol}", debounce_sec=60
            )

        # ========== GET DECISION FROM BOARD ==========
        decision = self.board.evaluate(context)
        self._last_decision = decision
        # Structured decision is already recorded by TradeManagementBoard; do not emit a second competing log.

        # ========== EXECUTE DECISION ==========
        self._execute_decision(decision, symbol)

    def _execute_decision(self, decision: TradeDecision, symbol: str):
        action, params = decision.action, decision.params or {}
        if action in (TradeDecisionType.NO_ACTION, TradeDecisionType.HOLD):
            return
        side = str(STATE.get("side") or "").upper()
        current_price = safe_float(STATE.get("mark_price"))
        common = {
            "symbol": symbol, "side": side, "price": current_price,
            "roe_pct": safe_float(STATE.get("roe_pct")),
            "profit_mode": str(STATE.get("profit_mode") or STANDARD_MODE),
            "reason": str(decision.reason), "trade_id": str(STATE.get("trade_id") or ""),
        }
        if action in (TradeDecisionType.MOVE_SL, TradeDecisionType.BREAKEVEN, TradeDecisionType.LOCK_PROFITS):
            new_sl = safe_float(params.get("new_sl"), safe_float(STATE.get("entry")))
            current_sl = safe_float(STATE.get("synthetic_sl"), safe_float(STATE.get("sl")))
            improves = (new_sl > current_sl) if side == "BUY" else (new_sl < current_sl or current_sl <= 0)
            if new_sl > 0 and improves:
                # Local ratchet is immediate, but a venue cancel/replace is only
                # submitted for a material improvement and never duplicated.
                min_step = max(abs(safe_float(STATE.get("entry"))) * 0.0005, 1e-12)
                material = (new_sl - current_sl) >= min_step if side == "BUY" else (current_sl - new_sl) >= min_step
                STATE["synthetic_sl"] = new_sl
                if action == TradeDecisionType.BREAKEVEN:
                    STATE["be_done"] = True
                if material:
                    move_ok, move_status, move_record = _move_native_stop_verified(
                        symbol, new_sl, action=action.value, reason=str(decision.reason))
                else:
                    move_ok, move_status, move_record = False, "NOT_MATERIAL", {}
                moved = move_status == "MOVED"
                event = "BREAKEVEN_APPLIED" if action == TradeDecisionType.BREAKEVEN else "TRAIL_UPDATED"
                record_trade_management_event(
                    event, "EXCHANGE_VERIFIED" if moved else "LOCAL_STATE_UPDATED",
                    log_level="INFO", **common, new_sl=new_sl,
                    native_sl_unchanged=not moved,
                    native_sl_move_status=move_status,
                    native_sl_order_id=STATE.get("native_sl_order_id"),
                    protection_layer="EXCHANGE_VERIFIED" if moved else "INTERNAL_SYNTHETIC_ONLY")
                _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
            return
        if action == TradeDecisionType.PARTIAL_CLOSE:
            target_remaining = params.get("target_remaining_qty")
            ratio = safe_float(params.get("ratio"), 0.5)
            before_remaining = safe_float(STATE.get("remaining_qty"), safe_float(STATE.get("qty")))
            initial = safe_float(STATE.get("initial_qty"), safe_float(STATE.get("qty")))
            close_reason = str(params.get("close_reason", "TP1"))
            if safe_close_partial(symbol, ratio, close_reason=close_reason,
                                  target_remaining_qty=target_remaining):
                remaining = safe_float(STATE.get("remaining_qty"))
                filled = safe_float(STATE.get("last_close_filled_qty"), max(0.0, before_remaining - remaining))
                if initial > 0 and remaining <= initial * 0.505:
                    STATE["partial_closed"] = True
                    STATE["tp1_hit"] = True
                    be_buffer = max(safe_float(STATE.get("entry")) * 0.0010, safe_float(STATE.get("atr")) * 0.10)
                    be_stop = safe_float(STATE.get("entry")) + be_buffer if side == "BUY" else safe_float(STATE.get("entry")) - be_buffer
                    current_sl = safe_float(STATE.get("synthetic_sl"))
                    be_improves = (side == "BUY" and be_stop > current_sl) or (side == "SELL" and (current_sl <= 0 or be_stop < current_sl))
                    if be_improves:
                        STATE["synthetic_sl"] = be_stop
                    STATE["be_done"] = True
                    STATE["trail_activated"] = True
                    STATE["be_local"] = True
                    if be_improves:
                        move_ok, move_status, move_record = _move_native_stop_verified(
                            symbol, be_stop, action="BREAKEVEN", reason="tp1_verified_protect_runner")
                        if move_status == "MOVED":
                            STATE["be_local"] = False
                    else:
                        move_ok, move_status, move_record = False, "NOT_MATERIAL", {}
                    _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
                    fill_price = safe_float(STATE.get("last_close_fill_price")) or None
                    order_id = STATE.get("last_close_order_id")
                    source = str(STATE.get("last_close_order_pnl_source") or STATE.get("last_close_pnl_source") or "UNAVAILABLE")
                    status = "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED"
                    record_trade_management_event(
                        "TP1_VERIFIED", status, log_level="SUCCESS", **common,
                        qty_closed=filled, remaining_qty=remaining, original_qty=initial,
                        fill_price=fill_price, order_id=order_id, realized_pnl_usdt=STATE.get("last_close_order_realized_pnl"),
                        pnl_source=source, runner_protected=True, target_remaining_qty=target_remaining,
                        native_sl_move_status=move_status,
                    )
                    tp1_realized = STATE.get("last_close_order_realized_pnl")
                    tp1_margin = safe_float(STATE.get("initial_margin")) * (filled / initial if initial > 0 else 0.5)
                    tp1_realized_roe = (safe_float(tp1_realized) / tp1_margin * 100.0) if tp1_realized is not None and tp1_margin > 0 else None
                    tg_tp_hit(symbol, 1, tp1_realized_roe, qty=filled, fill_price=fill_price, order_id=order_id,
                              realized_pnl_usdt=tp1_realized,
                              pnl_source=STATE.get("last_close_order_pnl_source"), side=side,
                              entry_price=STATE.get("entry"))
                else:
                    record_trade_management_event(
                        "PARTIAL_CLOSE_VERIFIED", "PARTIAL_DELTA_CONFIRMED", log_level="WARN",
                        **common, qty_closed=filled, remaining_qty=remaining, target_remaining_qty=target_remaining,
                        order_id=STATE.get("last_close_order_id"), note="TP1 flag remains false until the 50% target is reconciled",
                    )
                _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
            else:
                status = "UNRESOLVED" if STATE.get("execution_unresolved") or STATE.get("close_reconciliation_required") else "NOT_VERIFIED"
                record_trade_management_event("EXECUTION_UNCERTAIN" if status == "UNRESOLVED" else "CLOSE_FAILED",
                                              status, log_level="ERROR", **common,
                                              close_reason=close_reason, requested_ratio=ratio,
                                              remaining_qty=safe_float(STATE.get("remaining_qty")),
                                              order_id=STATE.get("pending_close_order_id"))
            return
        if action == TradeDecisionType.TRAIL:
            new_stop = safe_float(params.get("new_stop"))
            if new_stop > 0:
                current = safe_float(STATE.get("trail_stop"))
                improves = new_stop > current if side == "BUY" else (new_stop < current or current <= 0)
                if improves:
                    min_step = max(abs(safe_float(STATE.get("entry"))) * 0.0005, 1e-12)
                    material = (new_stop - current) >= min_step if side == "BUY" else (current - new_stop) >= min_step
                    STATE["trail_stop"] = new_stop
                    STATE["trail_activated"] = True
                    STATE["trail_tightened"] = True
                    if material:
                        move_ok, move_status, move_record = _move_native_stop_verified(
                            symbol, new_stop, action="TRAIL", reason=str(decision.reason))
                        if move_status == "MOVED":
                            STATE["synthetic_sl"] = new_stop
                    else:
                        move_ok, move_status, move_record = False, "NOT_MATERIAL", {}
                    moved = move_status == "MOVED"
                    record_trade_management_event(
                        "TRAIL_UPDATED", "EXCHANGE_VERIFIED" if moved else "LOCAL_STATE_UPDATED",
                        log_level="INFO", **common, trail_stop=new_stop,
                        multiplier=params.get("multiplier"), native_sl_unchanged=not moved,
                        native_sl_move_status=move_status,
                        native_sl_order_id=STATE.get("native_sl_order_id"),
                        protection_layer="EXCHANGE_VERIFIED" if moved else "INTERNAL_SYNTHETIC_ONLY")
                    _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
            return
        if action in (TradeDecisionType.FULL_CLOSE, TradeDecisionType.THESIS_FAILURE_EXIT, TradeDecisionType.EMERGENCY_EXIT):
            close_reason = str(params.get("close_reason") or decision.reason or action.value)
            before_qty = safe_float(STATE.get("remaining_qty"), safe_float(STATE.get("qty")))
            close_ok = safe_close_position(symbol, close_reason=close_reason)
            if close_ok and STATE.get("close_verified"):
                filled = safe_float(STATE.get("last_close_filled_qty"), before_qty)
                fill_price = safe_float(STATE.get("last_close_fill_price")) or None
                order_id = STATE.get("last_close_order_id")
                if close_reason == "TP2":
                    STATE["tp2_hit"] = True
                    _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
                    tp2_realized = STATE.get("last_close_order_realized_pnl")
                    tp2_margin = safe_float(STATE.get("initial_margin")) * (filled / safe_float(STATE.get("initial_qty"), filled) if safe_float(STATE.get("initial_qty"), filled) > 0 else 1.0)
                    tp2_realized_roe = (safe_float(tp2_realized) / tp2_margin * 100.0) if tp2_realized is not None and tp2_margin > 0 else None
                    tg_tp_hit(symbol, 2, tp2_realized_roe, qty=filled, fill_price=fill_price, order_id=order_id,
                              realized_pnl_usdt=tp2_realized,
                              pnl_source=STATE.get("last_close_order_pnl_source"), side=side,
                              entry_price=STATE.get("entry"))
                    record_trade_management_event("TP2_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                                   log_level="SUCCESS", **common, qty_closed=filled,
                                                   remaining_qty=0.0, fill_price=fill_price, order_id=order_id,
                                                   realized_pnl_usdt=tp2_realized,
                                                   pnl_source=STATE.get("last_close_order_pnl_source", STATE.get("last_close_pnl_source")))
                elif close_reason in {"RANGE_SCALP_TARGET", "SCALP_TARGET"}:
                    record_trade_management_event("RANGE_SCALP_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                                   log_level="SUCCESS", **common, qty_closed=filled,
                                                   remaining_qty=0.0, range_target=STATE.get("range_target_price"),
                                                   range_target_move_pct=STATE.get("range_target_move_pct"),
                                                   fill_price=fill_price, order_id=order_id)
                elif close_reason == "TP2_ADAPTIVE_STRICT_EXIT":
                    record_trade_management_event("RUNNER_ADAPTIVE_EXIT", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                                   log_level="SUCCESS", **common, qty_closed=filled,
                                                   remaining_qty=0.0, fill_price=fill_price, order_id=order_id)
                else:
                    record_trade_management_event("STRICT_EXIT_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                                   log_level="SUCCESS", **common, close_reason=close_reason,
                                                   qty_closed=filled, remaining_qty=0.0, fill_price=fill_price, order_id=order_id)
                finalize_trade_with_reality(symbol, close_reason=close_reason)
                self.lifecycle_state = TradeLifecycleState.CLOSED
                DASHBOARD_STATE["live_trade_mode"] = False
            elif not STATE.get("open") and MEMORY.get("position_status") == "FLAT":
                record_trade_management_event("EXTERNAL_CLOSE", "EXCHANGE_FLAT_CONFIRMED", log_level="WARN",
                                               **common, close_reason=STATE.get("last_close_reason", "EXTERNAL_CLOSE_UNKNOWN"),
                                               diagnostic=STATE.get("last_close_diagnostic"))
                self.lifecycle_state = TradeLifecycleState.CLOSED
                DASHBOARD_STATE["live_trade_mode"] = False
            else:
                record_trade_management_event("EXECUTION_UNCERTAIN", "UNRESOLVED", log_level="ERROR",
                                               **common, close_reason=close_reason,
                                               order_id=STATE.get("pending_close_order_id"),
                                               remaining_qty=safe_float(STATE.get("remaining_qty")))
            return

class ExchangeReconciliationSupervisor:
    """Independent periodic exchange-state monitor; UNKNOWN never means FLAT."""
    def __init__(self, exchange_sync, interval_sec=15.0):
        self.exchange_sync = exchange_sync
        self.interval_sec = max(5.0, float(interval_sec))
        self._stop = threading.Event()
        self._lifecycle_lock = threading.RLock()
        self._thread = None
        self._started = False
        self.last_result = "NOT_STARTED"
        self.last_success_ts = 0.0
        self.cycle_count = 0

    def metrics(self):
        return {
            "last_result": self.last_result,
            "last_success_ts": self.last_success_ts,
            "last_success_age_sec": (round(time.time() - self.last_success_ts, 3)
                                     if self.last_success_ts else None),
            "cycle_count": self.cycle_count,
            "interval_sec": self.interval_sec,
        }

    def start(self):
        """Start (or safely restart after a dead thread) exactly one supervisor."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="ggp-exchange-reconciliation", daemon=True)
            self._started = True
            self.last_result = "STARTED"
            self._thread.start()
            return True

    def stop(self, timeout=5.0):
        with self._lifecycle_lock:
            self._stop.set()
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.1, float(timeout)))
        with self._lifecycle_lock:
            self._started = bool(thread is not None and thread.is_alive())

    def _run(self):
        while not self._stop.wait(self.interval_sec):
            self.cycle_count += 1
            watchdog = globals().get("NAISO_WATCHDOG")
            if watchdog is not None:
                watchdog.beat("exchange_reconciliation", cycle=self.cycle_count)
            try:
                # A close is already doing its own strictly verified exchange
                # reconciliation. A periodic supervisor must not concurrently
                # reinterpret a transient in-flight close as an external FLAT.
                if globals().get("_CLOSE_ORDER_LOCK") is not None and _CLOSE_ORDER_LOCK.locked():
                    self.last_result = "SKIPPED_CLOSE_IN_PROGRESS"
                    continue
                if not STATE.get("open"):
                    self.last_result = "FLAT"
                    continue
                symbol = STATE.get("current_symbol")
                if not symbol:
                    STATE["recovery_hold"] = True
                    STATE["recovery_reason"] = "OPEN_STATE_WITHOUT_SYMBOL"
                    self.last_result = "HOLD_NO_SYMBOL"
                    log_execution("[RECON_SUPERVISOR] local OPEN state has no symbol; recovery hold", "CRITICAL",
                                  debounce_key="recon_missing_symbol", debounce_sec=60)
                    continue
                result = self.exchange_sync.reconcile(symbol, STATE, force=True)
                self.last_result = str(result)
                if result in {"OPEN", "FLAT"}:
                    self.last_success_ts = time.time()
                    STATE["last_successful_reconciliation"] = self.last_success_ts
                if result == "UNKNOWN":
                    log_execution(f"[RECON_SUPERVISOR] {symbol} state UNKNOWN; exposure retained and entries blocked", "ERROR",
                                  debounce_key=f"recon_supervisor_unknown_{symbol}", debounce_sec=60)
            except Exception as exc:
                self.last_result = f"ERROR:{type(exc).__name__}"
                log_execution(f"[RECON_SUPERVISOR] cycle failed: {type(exc).__name__}: {exc}", "ERROR",
                              debounce_key="recon_supervisor_cycle_error", debounce_sec=60)

# ========== EXISTING GLOBALS (reused) ==========
_event_bus = EventBus()
_exchange_sync = ExchangeSyncService(_event_bus)
_recovery_guard = RecoveryGuard(_event_bus, _exchange_sync)
_live_manager = LiveTradeManager(_event_bus, _exchange_sync, _recovery_guard)
# Workers are constructed here but started only by the application lifecycle.
# Importing this module must never create a background thread.
_reconciliation_supervisor = ExchangeReconciliationSupervisor(_exchange_sync, interval_sec=15.0)

# Owned long-lived periodic workers. A named handle is the only authority for
# starting/restarting a thread, so a timed-out worker that is still alive can
# never be replaced by a second instance.
_PERIODIC_WORKER_STOPS = {
    "institutional_updater": threading.Event(),
    "keep_alive": threading.Event(),
}
_PERIODIC_WORKER_THREADS = {}
_PERIODIC_WORKER_LOCK = threading.RLock()
_RUNTIME_WORKERS_STARTED = False
_RUNTIME_WORKERS_LOCK = threading.RLock()
_RUNTIME_SHUTTING_DOWN = False


def _start_named_worker(name, target):
    if _RUNTIME_SHUTTING_DOWN:
        return False
    with _PERIODIC_WORKER_LOCK:
        thread = _PERIODIC_WORKER_THREADS.get(name)
        if thread is not None and thread.is_alive():
            return False
        stop_event = _PERIODIC_WORKER_STOPS.setdefault(name, threading.Event())
        stop_event.clear()
        thread = threading.Thread(target=target, daemon=True, name=f"ggp_{name}")
        _PERIODIC_WORKER_THREADS[name] = thread
        thread.start()
        return True


def _stop_named_worker(name, timeout=5.0):
    with _PERIODIC_WORKER_LOCK:
        stop_event = _PERIODIC_WORKER_STOPS.get(name)
        if stop_event is not None:
            stop_event.set()
        thread = _PERIODIC_WORKER_THREADS.get(name)
    if thread is not None and thread.is_alive():
        thread.join(timeout=max(0.1, float(timeout)))
    return not bool(thread is not None and thread.is_alive())


def named_worker_status():
    """Return lifecycle state used by shutdown diagnostics and health checks."""
    with _PERIODIC_WORKER_LOCK:
        return {
            name: {"thread_alive": bool(thread is not None and thread.is_alive())}
            for name, thread in _PERIODIC_WORKER_THREADS.items()
        }


def sync_position_state(symbol=None):
    if PAPER_MODE:
        if STATE.get("open"):
            price = get_ticker_safe(STATE["current_symbol"])
            if price:
                raw_pnl = (price - STATE["entry"])/STATE["entry"]*100 if STATE["side"]=="BUY" else (STATE["entry"]-price)/STATE["entry"]*100
                roe_pct = raw_pnl * LEVERAGE
                STATE["roe_pct"] = roe_pct
                STATE["mark_price"] = price
                STATE["unrealized_pnl_usdt"] = (price - STATE["entry"]) * STATE["qty"] if STATE["side"]=="BUY" else (STATE["entry"] - price) * STATE["qty"]
                return price, 0.0, 0.0, roe_pct
        return None, None, None, None

    if not symbol and STATE.get("open"):
        symbol = STATE["current_symbol"]
    if not symbol:
        return None, None, None, None

    snap = _exchange_sync.fetch_live_snapshot(symbol)
    if snap is None:
        if STATE.get("open"):
            log_execution(f"[POS_SYNC] Position closed externally on {symbol}, cleaning state", "WARN")
            with _TRADE_LOCK:
                STATE["open"] = False
                TRADE_STATE["in_position"] = False
                _live_manager.lifecycle_state = TradeLifecycleState.CLOSED
                DASHBOARD_STATE["live_trade_mode"] = False
        return None, None, None, None

    with _TRADE_LOCK:
        if not STATE.get("open"):
            STATE["open"] = True
            STATE["side"] = snap.side
            STATE["entry"] = snap.entry_price
            STATE["qty"] = snap.qty
            STATE["remaining_qty"] = snap.qty
            STATE["current_symbol"] = symbol
            STATE["entry_time"] = time.time()
            TRADE_STATE.update({
                "in_position": True,
                "symbol": symbol,
                "side": snap.side,
                "entry": snap.entry_price,
                "qty": snap.qty,
                "last_update_ts": time.time()
            })
            _live_manager.start_trade(symbol, snap.side, snap.entry_price, snap.qty, 0.0, 0.0, 0.0)
        else:
            STATE["entry"] = snap.entry_price
            STATE["qty"] = snap.qty
            STATE["remaining_qty"] = snap.qty
            STATE["side"] = snap.side
            TRADE_STATE.update({
                "entry": snap.entry_price,
                "qty": snap.qty,
                "side": snap.side
            })

        STATE["margin"] = snap.margin
        STATE["unrealized_pnl_usdt"] = snap.unrealized_pnl
        STATE["roe_pct"] = snap.roe_pct
        STATE["leverage"] = snap.leverage
        STATE["mark_price"] = snap.mark_price
        STATE["liquidation_price"] = snap.liquidation_price

    return snap.mark_price, snap.unrealized_pnl, snap.margin, snap.roe_pct

def get_realized_pnl_for_symbol(symbol, lookback_seconds=30):
    if PAPER_MODE:
        return 0.0, 0.0
    try:
        sym = normalize_symbol(symbol)
        since = int((time.time() - lookback_seconds) * 1000)
        trades = safe_api_call(ex.fetch_my_trades, sym, limit=100, params={'since': since})
        if not trades:
            return 0.0, 0.0
        pnl_usdt = 0.0
        for trade in trades:
            side = trade['side'].lower()
            qty = trade['amount']
            price = trade['price']
            cost = qty * price
            if side == 'buy':
                pnl_usdt -= cost
            else:
                pnl_usdt += cost
        balance = get_balance_safe()
        pnl_pct = (pnl_usdt / balance * 100) if balance > 0 else 0.0
        return pnl_usdt, pnl_pct
    except Exception as e:
        log_execution(f"[REALIZED_PNL] Error: {e}", "WARN")
        return 0.0, 0.0

# ========== INDICATORS (unchanged) ==========
def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()

def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_atr(df, period=14):
    if df is None or len(df) < period+1:
        return pd.Series([0.0]*len(df))
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = rma(tr, period)
    atr = atr.bfill().ffill().fillna(tr.mean())
    atr = atr.clip(lower=1e-8)
    return atr

def compute_adx(df, period=14):
    if df is None or len(df) < period*2:
        return pd.Series([0.0]*len(df))
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = rma(tr, period) + 1e-9
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    plus_di = 100 * rma(plus_dm, period) / (atr + 1e-9)
    minus_di = 100 * rma(minus_dm, period) / (atr + 1e-9)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    adx = rma(dx, period)
    adx = adx.bfill().ffill().fillna(0).clip(0, 100)
    return adx

def compute_rsi(df, period=14):
    if df is None or len(df) < period+1:
        return pd.Series([50.0]*len(df))
    close = df['close']
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period) + 1e-9
    avg_loss = rma(loss, period) + 1e-9
    rs = avg_gain / (avg_loss + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.bfill().ffill().fillna(50).clip(0, 100)
    return rsi

def compute_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def macd_first_flip(hist):
    if len(hist) < 2:
        return False
    return hist.iloc[-2] < 0 and hist.iloc[-1] > 0

def volume_pressure_real(df, window=20, threshold=1.2):
    if len(df) < window + 1:
        return False
    vol = df['volume']
    mean = vol.rolling(window).mean().iloc[-1]
    std = vol.rolling(window).std().iloc[-1]
    if std == 0:
        return False
    z = (vol.iloc[-1] - mean) / std
    return z > threshold

def flow_engine(df):
    if len(df) < 2:
        return "neutral"
    last = df.iloc[-1]
    body = last['close'] - last['open']
    vol = last['volume']
    avg_vol = df['volume'].rolling(20).mean().iloc[-1] if len(df) >= 20 else vol
    if vol > avg_vol * 1.5:
        if body > 0:
            return "aggressive_buy"
        else:
            return "aggressive_sell"
    if vol > avg_vol and abs(body) < (last['high'] - last['low']) * 0.3:
        return "absorption"
    return "neutral"

def orderbook_imbalance(ob, depth=10):
    if not ob or 'bids' not in ob or 'asks' not in ob:
        return 0.0
    bids_sum = sum([b[1] for b in ob['bids'][:depth]]) if ob['bids'] else 0
    asks_sum = sum([a[1] for a in ob['asks'][:depth]]) if ob['asks'] else 0
    total = bids_sum + asks_sum
    if total == 0:
        return 0.0
    return (bids_sum - asks_sum) / total

def detect_walls(ob, depth=10, threshold=3.0):
    if not ob or 'bids' not in ob or 'asks' not in ob:
        return False, False
    bid_sizes = [b[1] for b in ob['bids'][:depth]]
    ask_sizes = [a[1] for a in ob['asks'][:depth]]
    if bid_sizes:
        avg_bid = sum(bid_sizes) / len(bid_sizes)
        bid_wall = any(s > avg_bid * threshold for s in bid_sizes)
    else:
        bid_wall = False
    if ask_sizes:
        avg_ask = sum(ask_sizes) / len(ask_sizes)
        ask_wall = any(s > avg_ask * threshold for s in ask_sizes)
    else:
        ask_wall = False
    return bid_wall, ask_wall

def is_late_move(df, atr, multiplier=1.5):
    if len(df) < 1 or atr <= 0:
        return False
    last = df.iloc[-1]
    candle_range = last['high'] - last['low']
    return candle_range > multiplier * atr

def early_score(df, ob, atr, side):
    score = 0
    reasons = []
    macd, signal, hist = compute_macd(df)
    if macd_first_flip(hist):
        score += 2
        reasons.append("macd_flip")
    if volume_pressure_real(df):
        score += 2
        reasons.append("volume_pressure")
    flow = flow_engine(df)
    if side == "BUY" and flow == "aggressive_buy":
        score += 2
        reasons.append("flow_buy")
    elif side == "SELL" and flow == "aggressive_sell":
        score += 2
        reasons.append("flow_sell")
    elif flow == "absorption":
        reasons.append("absorption")
    obi = orderbook_imbalance(ob, depth=10)
    if side == "BUY" and obi > 0.2:
        score += 2
        reasons.append(f"obi_bullish_{obi:.2f}")
    elif side == "SELL" and obi < -0.2:
        score += 2
        reasons.append(f"obi_bearish_{obi:.2f}")
    bid_wall, ask_wall = detect_walls(ob, depth=10, threshold=3.0)
    if side == "BUY" and bid_wall:
        score += 1
        reasons.append("bid_wall")
    elif side == "SELL" and ask_wall:
        score += 1
        reasons.append("ask_wall")
    if is_late_move(df, atr, multiplier=1.5):
        score -= 3
        reasons.append("late_move_penalty")
    return score, reasons

# ========== RF ENGINE (unchanged) ==========
class RFEngine:
    def __init__(self, period=20, multiplier=3.5):
        self.period = period
        self.multiplier = multiplier

    def ema(self, s, length):
        return s.ewm(span=length, adjust=False).mean()

    def rng_size(self, x):
        n = self.period
        qty = self.multiplier
        wper = (n * 2) - 1
        avrng = self.ema((x - x.shift(1)).abs(), n)
        return self.ema(avrng, wper) * qty

    def rng_filt(self, x, rng):
        filt = np.zeros(len(x))
        hi = np.zeros(len(x))
        lo = np.zeros(len(x))
        for i in range(len(x)):
            if i == 0:
                filt[i] = x.iloc[i]
            else:
                prev = filt[i - 1]
                r = rng.iloc[i]
                if x.iloc[i] - r > prev:
                    filt[i] = x.iloc[i] - r
                elif x.iloc[i] + r < prev:
                    filt[i] = x.iloc[i] + r
                else:
                    filt[i] = prev
            hi[i] = filt[i] + rng.iloc[i]
            lo[i] = filt[i] - rng.iloc[i]
        return pd.Series(hi, index=x.index), pd.Series(lo, index=x.index), pd.Series(filt, index=x.index)

    def compute(self, df, src="close"):
        x = df[src]
        rng = self.rng_size(x)
        h, l, filt = self.rng_filt(x, rng)
        fdir = np.zeros(len(filt))
        for i in range(1, len(filt)):
            if filt.iloc[i] > filt.iloc[i - 1]:
                fdir[i] = 1
            elif filt.iloc[i] < filt.iloc[i - 1]:
                fdir[i] = -1
            else:
                fdir[i] = fdir[i - 1]
        longCond = (x > filt) & (pd.Series(fdir) == 1)
        shortCond = (x < filt) & (pd.Series(fdir) == -1)
        CondIni = np.zeros(len(x))
        for i in range(1, len(x)):
            if longCond.iloc[i]:
                CondIni[i] = 1
            elif shortCond.iloc[i]:
                CondIni[i] = -1
            else:
                CondIni[i] = CondIni[i - 1]
        longSignal = longCond & (pd.Series(CondIni).shift(1) == -1)
        shortSignal = shortCond & (pd.Series(CondIni).shift(1) == 1)
        signal = None
        if longSignal.iloc[-1]:
            signal = "BUY"
        elif shortSignal.iloc[-1]:
            signal = "SELL"
        triggered = bool(longSignal.iloc[-1] or shortSignal.iloc[-1])
        distance = (x.iloc[-1] - filt.iloc[-1]) / x.iloc[-1] if x.iloc[-1] != 0 else 0
        return {
            "signal": signal,
            "triggered": triggered,
            "filt": filt.iloc[-1],
            "h_band": h.iloc[-1],
            "l_band": l.iloc[-1],
            "distance": distance
        }

# ========== ADVANCED CANDLE INTELLIGENCE ==========
def candle_metrics(candle):
    body = abs(candle['close'] - candle['open'])
    range_ = candle['high'] - candle['low']
    upper_wick = candle['high'] - max(candle['open'], candle['close'])
    lower_wick = min(candle['open'], candle['close']) - candle['low']
    return body, range_, upper_wick, lower_wick

def is_pinbar(candle, atr, side, body_atr_min=0.5, wick_body_ratio=2.5, wick_range_ratio=0.6):
    body, range_, upper_wick, lower_wick = candle_metrics(candle)
    if range_ == 0 or atr <= 0:
        return False
    if side == "BUY":
        return (lower_wick >= wick_body_ratio * body and
                lower_wick / range_ >= wick_range_ratio and
                body / atr >= body_atr_min)
    else:
        return (upper_wick >= wick_body_ratio * body and
                upper_wick / range_ >= wick_range_ratio and
                body / atr >= body_atr_min)

def classify_volume(df, period=20, expansion_threshold=1.8, normal_threshold=1.3, exhaustion_threshold=0.7):
    if len(df) < period + 1:
        return "neutral"
    vol = df['volume']
    avg_vol = vol.rolling(period).mean().iloc[-1]
    if avg_vol == 0:
        return "neutral"
    ratio = vol.iloc[-1] / avg_vol
    if ratio > expansion_threshold:
        return "expansion"
    elif ratio > normal_threshold:
        return "normal"
    elif ratio < exhaustion_threshold:
        return "exhaustion"
    else:
        return "neutral"

def detect_displacement(df, side, atr, volume_state, body_atr_threshold=0.8, volume_expansion_required=False):
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    body, range_, _, _ = candle_metrics(last)
    if body / atr < body_atr_threshold:
        return False
    if side == "BUY" and last['close'] <= last['open']:
        return False
    if side == "SELL" and last['close'] >= last['open']:
        return False
    if volume_expansion_required and volume_state != "expansion":
        return False
    return True

def detect_location(df, price, supports, resistances, threshold=0.003):
    near_support = False
    near_resistance = False
    if supports:
        min_dist_sup = min(abs(price - s) / price for s in supports)
        if min_dist_sup < threshold:
            near_support = True
    if resistances:
        min_dist_res = min(abs(price - r) / price for r in resistances)
        if min_dist_res < threshold:
            near_resistance = True
    if near_support and not near_resistance:
        return "LOW"
    elif near_resistance and not near_support:
        return "HIGH"
    else:
        return "MID"

def get_liquidity_sweep_for_side(df, side, lookback=5):
    ctx = detect_liquidity_context(df, lookback=lookback)
    if side == "BUY" and ctx == "sell_side_taken":
        return True
    if side == "SELL" and ctx == "buy_side_taken":
        return True
    return False

def get_rejection_pinbar(df, side, atr):
    if len(df) < 1:
        return False
    candle = df.iloc[-1]
    return is_pinbar(candle, atr, side)

def advanced_detect_scenario(df, side, atr, volume_state):
    if len(df) < 3:
        return "NONE"
    sweep = get_liquidity_sweep_for_side(df, side)
    rejection = get_rejection_pinbar(df, side, atr)
    displacement = detect_displacement(df, side, atr, volume_state, body_atr_threshold=0.8, volume_expansion_required=False)
    if sweep and rejection:
        return "TRAP_REVERSAL"
    elif displacement and not rejection:
        return "TREND_CONTINUATION"
    else:
        return "NONE"

def advanced_decision_engine(scenario, adx, volume_state, location):
    if scenario == "NONE":
        return "SKIP", None
    if volume_state == "exhaustion":
        log_execution(f"[ADV_DECISION] Volume exhaustion -> SKIP", "INFO", debounce_key=f"adv_vol_exhaustion", debounce_sec=120)
        return "SKIP", None
    adx = float(adx) if adx is not None else 20.0
    if adx < 18:
        log_execution(f"[ADV_DECISION] ADX too low ({adx:.1f}) -> SKIP", "INFO", debounce_key=f"adv_adx_low", debounce_sec=120)
        return "SKIP", None
    if scenario == "TRAP_REVERSAL":
        if adx < 35:
            return "ENTER", "STRONG"
        else:
            log_execution(f"[ADV_DECISION] TRAP_REVERSAL but ADX >=35 ({adx:.1f}) -> SKIP", "INFO", debounce_key=f"adv_trap_adx_high", debounce_sec=120)
            return "SKIP", None
    elif scenario == "TREND_CONTINUATION":
        if 20 < adx < 45:
            return "ENTER", "MEDIUM"
        else:
            log_execution(f"[ADV_DECISION] TREND_CONTINUATION but ADX out of range (20-45) -> {adx:.1f} SKIP", "INFO", debounce_key=f"adv_trend_adx_range", debounce_sec=120)
            return "SKIP", None
    return "SKIP", None

# ========== LEGACY SMC FUNCTIONS ==========
def detect_bos(df, lookback=5):
    if len(df) < lookback+2:
        return False, False
    recent_high = df['high'].iloc[-lookback-1:-1].max()
    recent_low = df['low'].iloc[-lookback-1:-1].min()
    current_close = df['close'].iloc[-1]
    bos_up = current_close > recent_high
    bos_down = current_close < recent_low
    return bos_up, bos_down

def detect_scenario(df):
    if len(df) < 30:
        return "NONE"
    row = df.iloc[-1]
    liquidity_ctx = detect_liquidity_context(df, lookback=10)
    sweep_up = (liquidity_ctx == "buy_side_taken")
    sweep_down = (liquidity_ctx == "sell_side_taken")
    bos_up, bos_down = detect_bos(df)
    vol_spike_flag = volume_spike(df)
    vol_sma = df['volume'].iloc[-21:-1].mean() if len(df) >= 21 else df['volume'].mean()
    volume_ok = row['volume'] > 1.5 * vol_sma if vol_sma > 0 else False
    range_ = row['high'] - row['low']
    if range_ == 0:
        rejection_buy = False
        rejection_sell = False
    else:
        body = abs(row['close'] - row['open'])
        lower_wick = min(row['open'], row['close']) - row['low']
        upper_wick = row['high'] - max(row['open'], row['close'])
        rejection_buy = (lower_wick > 2 * body + 1e-9) or (row['close'] > row['open'] and body/range_ > 0.5)
        rejection_sell = (upper_wick > 2 * body + 1e-9) or (row['close'] < row['open'] and body/range_ > 0.5)
    if sweep_down and rejection_buy:
        return "REVERSAL_BUY"
    if sweep_up and rejection_sell:
        return "REVERSAL_SELL"
    if bos_up and volume_ok:
        return "TREND_BUY"
    if bos_down and volume_ok:
        return "TREND_SELL"
    if sweep_down and not rejection_buy:
        return "TRAP_SELL"
    if sweep_up and not rejection_sell:
        return "TRAP_BUY"
    return "NONE"

def decision_engine(scenario, rf_signal, adx):
    if scenario == "NONE":
        return "SKIP"
    if scenario == "REVERSAL_BUY" and rf_signal == "BUY":
        if adx < 35:
            return "STRONG"
        else:
            return "SKIP"
    if scenario == "REVERSAL_SELL" and rf_signal == "SELL":
        if adx < 35:
            return "STRONG"
        else:
            return "SKIP"
    if scenario == "TREND_BUY" and rf_signal == "BUY":
        if 20 < adx < 45:
            return "MEDIUM"
        else:
            return "SKIP"
    if scenario == "TREND_SELL" and rf_signal == "SELL":
        if 20 < adx < 45:
            return "MEDIUM"
        else:
            return "SKIP"
    if "TRAP" in scenario:
        return "STRONG"
    return "SKIP"

def apply_profit_engine(symbol, current_price, df, idx, position_state):
    # This is now replaced by TradeManagementBoard, kept for compatibility if called elsewhere
    return "HOLD"

def detect_liquidity_context(df, lookback=10):
    sweeps = []
    for i in range(-lookback, 0):
        if i == -1:
            continue
        prev_low = df['low'].iloc[i-1]
        curr_low = df['low'].iloc[i]
        lower_wick = min(df['open'].iloc[i], df['close'].iloc[i]) - curr_low
        if curr_low < prev_low and lower_wick > 0.0001:
            sweeps.append("sell_side_taken")
        prev_high = df['high'].iloc[i-1]
        curr_high = df['high'].iloc[i]
        upper_wick = curr_high - max(df['open'].iloc[i], df['close'].iloc[i])
        if curr_high > prev_high and upper_wick > 0.0001:
            sweeps.append("buy_side_taken")
    if len(sweeps) == 0:
        return None
    return sweeps[-1]

def detect_zone_context(price, supports, resistances, threshold=0.003):
    near_support = min([abs(price - s)/price for s in supports]) < threshold if supports else False
    near_resistance = min([abs(price - r)/price for r in resistances]) < threshold if resistances else False
    return {"near_support": near_support, "near_resistance": near_resistance}

def detect_structure_shift(df):
    if len(df) < 10:
        return None
    last_high = df['high'].iloc[-3]
    prev_high = df['high'].iloc[-6]
    last_low = df['low'].iloc[-3]
    prev_low = df['low'].iloc[-6]
    if last_high > prev_high and last_low > prev_low:
        return "bullish_shift"
    elif last_high < prev_high and last_low < prev_low:
        return "bearish_shift"
    return None

def get_clustered_zones(df, lookback=120, cluster_pct=0.002):
    """Compatibility wrapper over the canonical confirmed S/R zone engine."""
    if df is None or not isinstance(df, pd.DataFrame) or len(df) < 20:
        return [], []
    try:
        validator = globals().get("roro_validate_closed_frame")
        if not callable(validator):
            return [], []
        now_ms, _time_source = _entry_exchange_time_ms()
        closed = validator(
            df.copy(), now_ms=int(now_ms),
            timeframe_s=int(_roro_timeframe_seconds(str(INTERVAL))),
        )
        if not isinstance(closed, pd.DataFrame) or len(closed) < 20:
            return [], []
        atr_series = compute_atr(closed)
        atr_value = safe_float(atr_series.iloc[-1], 0.0) if atr_series is not None and len(atr_series) else 0.0
        price = safe_float(closed["close"].iloc[-1], 0.0)
        if atr_value <= 0 or price <= 0:
            return [], []
        zones = build_support_resistance_zones(
            closed.tail(max(20, int(lookback))).to_dict("records"),
            price=price, atr=atr_value, lookback=lookback,
            pivot_span=2, cluster_atr=0.22, min_touches=2, max_zones=8,
        )
        if zones.get("status") != "VALID":
            return [], []
        supports = [
            float(z["level"]) for z in zones.get("supports", [])
            if z.get("status") == "CONFIRMED" and float(z.get("strength", 0) or 0) >= 5.0
        ]
        resistances = [
            float(z["level"]) for z in zones.get("resistances", [])
            if z.get("status") == "CONFIRMED" and float(z.get("strength", 0) or 0) >= 5.0
        ]
        return supports, resistances
    except Exception as exc:
        log_execution(f"[STRUCTURE_ZONES] S/R calculation unavailable: {type(exc).__name__}: {exc}", "WARN")
        return [], []

def detect_liquidity_cluster(df, lb=20, tol=0.001):
    highs = df['high'].iloc[-lb:]
    lows  = df['low'].iloc[-lb:]
    if highs.max() == highs.min() or lows.max() == lows.min():
        return False, False
    eqh = (highs.max() - highs.min()) / highs.mean() < tol
    eql = (lows.max() - lows.min()) / lows.mean() < tol
    return eqh, eql

def detect_sweep_v2(df, side):
    if len(df) < 22:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    eqh, eql = detect_liquidity_cluster(df, lb=20, tol=0.001)
    range_ = last['high'] - last['low']
    if range_ == 0:
        return False, False
    if side == "BUY":
        lower_wick = min(last['open'], last['close']) - last['low']
        wick_ratio = lower_wick / range_
        low_cluster_min = df['low'].iloc[-21:-1].min()
        sweep_broke = eql and (last['low'] < low_cluster_min)
        reclaim = last['close'] > last['low']
        valid = (wick_ratio > 0.6 and reclaim)
        return sweep_broke, valid
    else:
        upper_wick = last['high'] - max(last['open'], last['close'])
        wick_ratio = upper_wick / range_
        high_cluster_max = df['high'].iloc[-21:-1].max()
        sweep_broke = eqh and (last['high'] > high_cluster_max)
        reclaim = last['close'] < last['high']
        valid = (wick_ratio > 0.6 and reclaim)
        return sweep_broke, valid

def candle_rejection(df, side):
    if len(df) < 1:
        return False
    last = df.iloc[-1]
    range_ = last['high'] - last['low']
    if range_ == 0:
        return False
    body = abs(last['close'] - last['open'])
    if side == "BUY":
        lower_wick = min(last['open'], last['close']) - last['low']
        return (lower_wick > 1.5 * body) or (last['close'] > last['open'] and body/range_ > 0.5)
    else:
        upper_wick = last['high'] - max(last['open'], last['close'])
        return (upper_wick > 1.5 * body) or (last['close'] < last['open'] and body/range_ > 0.5)

def volume_spike(df):
    if len(df) < 21:
        return False
    avg_vol = df['volume'].iloc[-21:-1].mean()
    last_vol = df['volume'].iloc[-1]
    return last_vol >= 1.5 * avg_vol

def is_late_entry(df, side):
    if len(df) < 6:
        return False
    last5_move = abs(df['close'].iloc[-1] - df['close'].iloc[-6]) / df['close'].iloc[-6]
    if last5_move > 0.008:
        if side == "BUY":
            recent_high = df['high'].iloc[-5:].max()
            pullback = (recent_high - df['close'].iloc[-1]) / (recent_high - df['close'].iloc[-6]) if (recent_high - df['close'].iloc[-6]) != 0 else 0
            if pullback < 0.3:
                return True
        else:
            recent_low = df['low'].iloc[-5:].min()
            pullback = (df['close'].iloc[-1] - recent_low) / (df['close'].iloc[-6] - recent_low) if (df['close'].iloc[-6] - recent_low) != 0 else 0
            if pullback < 0.3:
                return True
    return False

def compute_location(df, price, side):
    low50 = df['low'].iloc[-50:].min()
    high50 = df['high'].iloc[-50:].max()
    if high50 == low50:
        return "mid"
    relative = (price - low50) / (high50 - low50)
    if side == "BUY":
        if relative <= 0.3:
            return "discount"
        elif relative >= 0.7:
            return "premium"
        else:
            return "mid"
    else:
        if relative >= 0.7:
            return "premium"
        elif relative <= 0.3:
            return "discount"
        else:
            return "mid"

def swing_points(df, lb=5):
    highs = df['high'].values
    lows = df['low'].values
    swing_highs = []
    swing_lows = []
    for i in range(lb, len(df)-lb):
        if highs[i] == max(highs[i-lb:i+lb+1]):
            swing_highs.append((i, highs[i]))
        if lows[i] == min(lows[i-lb:i+lb+1]):
            swing_lows.append((i, lows[i]))
    return swing_highs, swing_lows

def equal_levels(points, tolerance=0.0015):
    if len(points) < 2:
        return False
    avg = sum(points)/len(points)
    return all(abs(p - avg) / avg < tolerance for p in points)

def build_liquidity_pools(df):
    sh, sl = swing_points(df, lb=5)
    recent_highs = [p[1] for p in sh[-3:]] if len(sh) >= 3 else [sh[-1][1]] if sh else []
    if len(recent_highs) >= 2 and equal_levels(recent_highs):
        pools_high = recent_highs
    else:
        pools_high = [sh[-1][1]] if sh else []
    recent_lows = [p[1] for p in sl[-3:]] if len(sl) >= 3 else [sl[-1][1]] if sl else []
    if len(recent_lows) >= 2 and equal_levels(recent_lows):
        pools_low = recent_lows
    else:
        pools_low = [sl[-1][1]] if sl else []
    return {"high_pools": pools_high, "low_pools": pools_low}

def detect_sweep(df, pools):
    if len(df) < 2:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    swept_high = False
    swept_low = False
    for h in pools["high_pools"]:
        if last['high'] > h and prev['high'] <= h and last['close'] < last['high']:
            swept_high = True
            break
    for l in pools["low_pools"]:
        if last['low'] < l and prev['low'] >= l and last['close'] > last['low']:
            swept_low = True
            break
    return swept_high, swept_low

def classify_sweep(df, side):
    if len(df) < 2:
        return "fake", -2
    last = df.iloc[-1]
    range_ = last['high'] - last['low']
    if range_ == 0:
        return "weak", 1
    if side == "BUY":
        lower_wick = min(last['open'], last['close']) - last['low']
        wick_ratio = lower_wick / range_
        reclaimed = last['close'] > last['low']
        if wick_ratio > 0.6 and reclaimed:
            return "strong", 3
        elif wick_ratio > 0.3:
            return "weak", 1.5
        else:
            return "fake", -2
    else:
        upper_wick = last['high'] - max(last['open'], last['close'])
        wick_ratio = upper_wick / range_
        reclaimed = last['close'] < last['high']
        if wick_ratio > 0.6 and reclaimed:
            return "strong", 3
        elif wick_ratio > 0.3:
            return "weak", 1.5
        else:
            return "fake", -2

def volume_engine(df):
    avg_vol = df['volume'].iloc[-20:].mean() if len(df) >= 20 else df['volume'].mean()
    last_vol = df['volume'].iloc[-1]
    if last_vol >= 1.5 * avg_vol:
        return "spike", 2
    elif last_vol < 0.7 * avg_vol:
        return "exhaustion", -1
    else:
        last = df.iloc[-1]
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        if range_ > 0 and body / range_ < 0.4 and last_vol > avg_vol:
            return "absorption", 1
        else:
            return "normal", 0

def structure_engine(df):
    sh, sl = swing_points(df, lb=5)
    last_close = df['close'].iloc[-1]
    bos = False
    choch = False
    if len(sh) >= 2:
        if last_close > sh[-2][1]:
            bos = True
    if len(sl) >= 2:
        if last_close < sl[-2][1]:
            bos = True
    if len(sh) >= 2 and len(sl) >= 2:
        if sh[-1][1] > sh[-2][1] and sl[-1][1] > sl[-2][1]:
            choch = True
        elif sh[-1][1] < sh[-2][1] and sl[-1][1] < sl[-2][1]:
            choch = True
    return bos, choch

def pre_rf_context_boost(df, side):
    if len(df) < 3:
        return 0, []
    last2 = df.iloc[-3:-1]
    boost = 0
    reasons = []
    if side == "BUY":
        if last2['close'].iloc[-2] < last2['close'].iloc[-1] and last2['close'].iloc[-1] < df['close'].iloc[-1]:
            boost += 1
            reasons.append("consecutive_bullish")
        if (last2['close'].iloc[-1] - last2['low'].iloc[-1]) / (last2['high'].iloc[-1] - last2['low'].iloc[-1] + 1e-9) > 0.7:
            boost += 1
            reasons.append("strong_bullish_candle")
    else:
        if last2['close'].iloc[-2] > last2['close'].iloc[-1] and last2['close'].iloc[-1] > df['close'].iloc[-1]:
            boost += 1
            reasons.append("consecutive_bearish")
        if (last2['high'].iloc[-1] - last2['close'].iloc[-1]) / (last2['high'].iloc[-1] - last2['low'].iloc[-1] + 1e-9) > 0.7:
            boost += 1
            reasons.append("strong_bearish_candle")
    return min(boost, 2), reasons

def market_intent(df):
    if len(df) < 20:
        return None, 0
    recent_range = df['high'].iloc[-10:].max() - df['low'].iloc[-10:].min()
    avg_range = (df['high'].rolling(20).max() - df['low'].rolling(20).min()).iloc[-1]
    absorption = (recent_range / avg_range) < 0.5 if avg_range > 0 else False
    vol_state, _ = volume_engine(df)
    if absorption and vol_state == "absorption":
        return "accumulation", 1
    last = df.iloc[-1]
    prev = df.iloc[-2]
    if vol_state == "spike" and last['close'] < prev['high'] and last['high'] > prev['high']:
        return "distribution", 1
    if len(df) >= 3:
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
        if c2['high'] > c1['high'] and c3['close'] < c2['high'] and c3['close'] < c3['open']:
            return "trap", 2
        if c2['low'] < c1['low'] and c3['close'] > c2['low'] and c3['close'] > c3['open']:
            return "trap", 2
    return None, 0

def council_decision(context):
    score = 0
    reasons = []
    if context["location"] == "discount" and context["side"] == "BUY":
        score += 3
        reasons.append("discount_location")
    elif context["location"] == "premium" and context["side"] == "SELL":
        score += 3
        reasons.append("premium_location")
    else:
        score -= 4
        reasons.append("bad_location")
    if context["sweep"] == "strong":
        score += 3
        reasons.append("strong_sweep")
    elif context["sweep"] == "weak":
        score += 1
        reasons.append("weak_sweep")
    elif context["sweep"] == "fake":
        score -= 2
        reasons.append("fake_sweep")
    if context["in_zone"]:
        score += 2.5
        reasons.append("zone_hit")
    if context["volume"] == "spike":
        score += 2
        reasons.append("volume_spike")
    elif context["volume"] == "absorption":
        score += 1
        reasons.append("absorption")
    elif context["volume"] == "exhaustion":
        score -= 1
        reasons.append("exhaustion")
    if context["bos"] or context["choch"]:
        score += 2
        reasons.append("structure_shift")
    intent = context.get("intent")
    if intent == "trap":
        score += 2
        reasons.append("trap_intent")
    elif intent == "accumulation":
        score += 1
        reasons.append("accumulation")
    elif intent == "distribution":
        score += 1
        reasons.append("distribution")
    score += context.get("pre_rf_boost", 0)
    if context.get("pre_rf_reasons"):
        reasons.extend(context["pre_rf_reasons"])
    if context.get("distance_penalty", 0) == -100:
        return -100, ["far_from_zone_skip"]
    score += context.get("distance_penalty", 0)
    if context.get("distance_reasons"):
        reasons.extend(context["distance_reasons"])
    adx = context["adx"]
    if adx < 18:
        score -= 1
        reasons.append("weak_trend")
    elif 20 <= adx <= 30:
        score += 2
        reasons.append("ideal_trend_phase")
    elif 30 < adx <= 35:
        score += 0.5
        reasons.append("mid_trend")
    elif adx > 40:
        score -= 2
        reasons.append("late_trend_no_entry")
    final_score = max(0, min(12, score))
    return final_score, reasons

def _market_price_tick_size(symbol, reference_price):
    """Return one exchange price increment for strict TP ordering."""
    try:
        market = ex.market(normalize_symbol(symbol))
        precision = (market.get("precision") or {}).get("price")
        if precision is not None:
            precision = float(precision)
            if math.isfinite(precision) and precision > 0:
                mode = getattr(ex, "precisionMode", None)
                if mode == getattr(ccxt, "TICK_SIZE", object()):
                    return precision
                if mode == getattr(ccxt, "DECIMAL_PLACES", object()):
                    return 10.0 ** (-int(precision))
                if mode == getattr(ccxt, "SIGNIFICANT_DIGITS", object()):
                    magnitude = math.floor(math.log10(abs(float(reference_price))))
                    return 10.0 ** (magnitude - int(precision) + 1)
    except Exception:
        pass
    # Conservative numerical fallback for PAPER/mocked markets without metadata.
    return max(abs(float(reference_price)) * 1e-8, 1e-12)


def _order_take_profit_levels(entry_price, side, tp1_price, tp2_price, tick_size):
    """Keep TP1 nearest and TP2 strictly farther from entry after rounding."""
    entry = float(entry_price)
    tp1 = float(tp1_price)
    tp2 = float(tp2_price)
    tick = max(float(tick_size), max(abs(entry) * 1e-12, 1e-12))
    direction = str(side or "").strip().upper()
    if direction == "BUY":
        tp1 = max(tp1, entry + tick)
        tp2_distance = max(abs(entry - tp2), abs(tp1 - entry) + tick)
        tp2 = entry + tp2_distance
        if tp2 <= tp1:
            tp2 = tp1 + tick
    elif direction == "SELL":
        tp1 = min(tp1, entry - tick)
        tp2_distance = max(abs(entry - tp2), abs(entry - tp1) + tick)
        tp2 = entry - tp2_distance
        if tp2 >= tp1:
            tp2 = tp1 - tick
    else:
        raise ValueError("side must be BUY or SELL when ordering TP levels")
    return tp1, tp2


def compute_sl_tp(entry_price, side, classification, atr, df, symbol=None):
    """Build directional SL/TP levels; TP2 must remain beyond TP1 after min-distance adjustment."""
    side = str(side or "").strip().upper()
    entry_price = float(entry_price)
    atr = float(atr)
    if side not in {"BUY", "SELL"} or not math.isfinite(entry_price) or entry_price <= 0:
        raise ValueError("invalid entry price or side for SL/TP calculation")
    if not math.isfinite(atr) or atr <= 0:
        raise ValueError("ATR must be finite and positive for SL/TP calculation")
    pricing_symbol = symbol or getattr(df, "symbol", None) or DEFAULT_SYMBOL
    if classification == "REVERSAL":
        pools = build_liquidity_pools(df)
        if side == "BUY":
            sl = min(pools["low_pools"]) - 0.5 * atr if pools["low_pools"] else entry_price - atr * 1.2
        else:
            sl = max(pools["high_pools"]) + 0.5 * atr if pools["high_pools"] else entry_price + atr * 1.2
        min_sl_dist = 1.2 * atr
        if abs(entry_price - sl) < min_sl_dist:
            sl = entry_price - min_sl_dist if side == "BUY" else entry_price + min_sl_dist
        # Normal reversal geometry is the canonical 0.50% / 1.00% price-move
        # two-stage plan. The live manager may choose a more conservative
        # adaptive 0.25% target, but entry geometry must not become a trend
        # 2.00% / 3.00% target by accident.
        tp1 = entry_price * (1.0 + (0.50 / 100.0 if side == "BUY" else -0.50 / 100.0))
        tp2 = entry_price * (1.0 + (1.00 / 100.0 if side == "BUY" else -1.00 / 100.0))
    elif classification == "EARLY_TREND":
        ema50 = ema(df['close'], 50).iloc[-1]
        sl = ema50 - atr * 1.2 if side == "BUY" else ema50 + atr * 1.2
        tp1, tp2 = trend_target_prices(side, entry_price)
    else:
        sl = entry_price - atr * 1.6 if side == "BUY" else entry_price + atr * 1.6
        tp1, tp2 = trend_target_prices(side, entry_price)

    # Preserve the existing SL volatility safety adjustment. Profit targets
    # are fixed price-move percentages and must not be silently widened by ATR.
    sl, tp1 = PrecisionSafety.adjust_sl_tp(
        pricing_symbol, entry_price, sl, tp1, side, atr
    )
    # The legacy adjustment only moved TP1; when ATR was wide it could push
    # TP1 beyond the fixed-percentage TP2. Move TP2 outward, never TP1 inward.
    tick_size = _market_price_tick_size(pricing_symbol, entry_price)
    tp2 = PrecisionSafety.normalize_price(pricing_symbol, tp2)
    tp1, tp2 = _order_take_profit_levels(entry_price, side, tp1, tp2, tick_size)
    # Re-normalize, then enforce strict ordering once more to account for rounding
    # collapsing adjacent levels to the same exchange tick.
    tp1 = PrecisionSafety.normalize_price(pricing_symbol, tp1)
    tp2 = PrecisionSafety.normalize_price(pricing_symbol, tp2)
    tp1, tp2 = _order_take_profit_levels(entry_price, side, tp1, tp2, tick_size)
    if side == "BUY" and not (entry_price < tp1 < tp2):
        raise ValueError(
            f"TP_PRECISION_ORDER_INVALID BUY entry={entry_price} tp1={tp1} tp2={tp2} tick={tick_size}"
        )
    if side == "SELL" and not (tp2 < tp1 < entry_price):
        raise ValueError(
            f"TP_PRECISION_ORDER_INVALID SELL entry={entry_price} tp1={tp1} tp2={tp2} tick={tick_size}"
        )
    return float(sl), float(tp1), float(tp2)

# ========== EXECUTION LAYER ==========
STATE = {
    "open": False, "side": None, "entry": 0.0, "qty": 0.0, "remaining_qty": 0.0,
    "sl": 0.0, "tp1_done": False, "trail_activated": False, "trail_stop": 0.0,
    "peak": 0.0, "cooldown_until": None, "daily_trades": 0, "last_trade_day": None,
    "consecutive_losses": 0, "daily_peak_balance": None, "daily_loss_limit_hit": False,
    "current_symbol": None, "balance": 0.0, "atr": 0.0, "entry_time": None,
    "entry_reasons": [], "trade_score": 0, "partial_closed": False,
    "tp1_price": 0.0, "tp2_price": 0.0, "trade_type": None, "entry_type": None,
    "be_done": False, "classification": None, "location": None, "zone_info": None,
    "runner_active": False, "scale_ins": 0, "decision_log": [],
    "tp1_hit": False, "tp2_hit": False,
    "zone": {},
    "initial_margin": 0.0, "real_unrealized_pnl": 0.0, "roe_pct": 0.0, "leverage": LEVERAGE,
    "smart_tightened": False, "smart_partial_done": False, "smart_exit_triggered": False,
    "mark_price": 0.0, "unrealized_pnl_usdt": 0.0,
    "margin": 0.0, "liquidation_price": 0.0,
    "narrative_classification": None, "narrative_confidence": 0.0,
    "confidence_level": None,
    "continuation_probability": 0.5,
    "hold_quality": "UNKNOWN",
    "counter_pressure": 0.0,
    "reclaim_risk": 0.0,
    "trend_strength": 0.0,
    "continuation_reasons": [],
    "trade_thesis": None,
    "current_confidence": 50.0,
    "market_regime": "UNKNOWN",
    "continuation_pressure": 50,
    "thesis_failure_score": 0,
    "prev_di_spread": 0.0,
    "adx_live": 0.0,
    "di_plus_live": 0.0,
    "di_minus_live": 0.0,
    "trade_personality": "NEUTRAL",
    "institutional_flow": "NEUTRAL",
    "profit_lock_activated": False,
    "trail_tightened": False,
    "smart_money": {},
    "momentum_flow": {},
    "trade_state": "RANGE_CHOP",
    "delay_tp1": False,
    "smart_trail_mult": 1.5,
    "synthetic_sl": 0.0,
    "synthetic_tp1": 0.0,
    "max_price": 0.0,
    "min_price": 0.0,
    "peak_roe": 0.0,
    "peak_drawdown_roe": 0.0,
    "ema_profit_state": "INSUFFICIENT_DATA",
    "profit_mode": None, "profit_mode_reason": "", "profit_mode_evidence": [],
    "profit_mode_candidate": None, "profit_mode_candidate_count": 0, "profit_mode_last_candle_ts": 0.0,
    "management_mode": "STANDARD", "management_mode_report": {}, "management_mode_history": [],
    "management_mode_last_change_ts": 0.0, "entry_risk_plan": {}, "institutional_report": {},
    "range_target_price": 0.0, "range_target_move_pct": 0.0, "range_target_reason": "",

    "opposing_liquidity_status": "UNKNOWN",
    "opposing_liquidity_level": 0.0,
    "opposing_liquidity_distance_pct": 0.0,
    "opposing_liquidity_timestamp": 0.0,
    "scalp_target_selected_pct": 0.0,
    "scalp_target_price": 0.0,
    "scalp_target_tp1_price": 0.0,
    "scalp_target_status": "UNKNOWN",
    "scalp_target_selection_reason": "",

            "native_sl_policy": "FIXED_FROM_ENTRY", "be_local": False,
    "native_sl_last_verified_price": 0.0, "native_sl_last_requested_price": 0.0,
    "native_sl_last_update_ts": 0.0, "native_sl_last_result": "NONE", "native_sl_last_reason": "",
    "native_sl_original_qty": 0.0, "native_sl_local_target": 0.0,
    "tp1_filled_qty": 0.0, "tp2_filled_qty": 0.0, "tp1_execution_price": None,
    "execution_unresolved": False, "close_reconciliation_required": False,
    "close_verified": False, "last_close_verified": False, "last_close_order_id": None,
    "last_close_reason": None, "last_close_fill_price": None, "last_close_filled_qty": 0.0,
    "last_close_realized_pnl": 0.0, "last_close_pnl_source": None, "last_close_pnl_complete": True,
    "bot_close_order_ids": [],
    "pending_close_order_id": None, "pending_close_reason": None,
    "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
    "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
    "structural_invalidation": False, "thesis_failure_reasons": [],
    "emergency_exit": False, "emergency_reason": None,
    "emergency_source": None, "emergency_ts": 0.0,
    "zone_event_last_candle_ts": 0.0,
    "failed_breakout_streak": 0,
    "failed_breakout_last_candle_ts": 0.0,
    "failed_breakout_thesis_credit_applied": False,
    "entry_zone_low": 0.0,
    "entry_zone_high": 0.0,
    "support_resistance": {"status": "UNKNOWN", "supports": [], "resistances": []},
    "entry_behavior_confidence": 0.0,
    "entry_behavior_reasons": [],
    "trading_mode": "UNCLASSIFIED",
    "entry_behavior": "WAIT_FOR_STRUCTURE",
    "market_intelligence": {},
}
paper = {"balance": 10000.0, "position": None}
_ACTIVE_TRADE = False
_TRADE_LOCK = threading.RLock()
_CLOSE_ORDER_LOCK = threading.Lock()

# ========== DASHBOARD STATE ==========
DASHBOARD_STATE = {
    "account": {"balance": 0.0, "free_balance": 0.0, "available_margin": 0.0, "mode": "PAPER"},
    "stats": {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0},
    "position": None,
    "logs": [],
    "errors": [],
    "system_errors": [],
    "live_trade_mode": False,
    "lifecycle_state": "IDLE",
    "live_supervisor": {},
    "institutional_flow": {}
}

_RUNTIME_LOGGER = None
_RUNTIME_LOGGER_LOCK = threading.Lock()

def _get_runtime_logger():
    """Bounded on-disk runtime log; dashboard buffers remain independently bounded."""
    global _RUNTIME_LOGGER
    if _RUNTIME_LOGGER is not None:
        return _RUNTIME_LOGGER
    with _RUNTIME_LOGGER_LOCK:
        if _RUNTIME_LOGGER is not None:
            return _RUNTIME_LOGGER
        try:
            log_dir = os.path.join(ENV_DIR, "logs")
            os.makedirs(log_dir, exist_ok=True)
            logger = logging.getLogger("ggp.runtime")
            logger.setLevel(logging.INFO)
            logger.propagate = False
            if not logger.handlers:
                handler = RotatingFileHandler(
                    os.path.join(log_dir, "ggp_runtime.log"),
                    maxBytes=5 * 1024 * 1024, backupCount=10, encoding="utf-8"
                )
                handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
                logger.addHandler(handler)
            _RUNTIME_LOGGER = logger
        except Exception:
            _RUNTIME_LOGGER = False
    return _RUNTIME_LOGGER

def _system_error_record(message, severity, timestamp):
    """Build a bounded, dashboard-safe incident record from the canonical log event."""
    raw = str(message or "")[:8000]
    upper = raw.upper()
    if "[WATCHDOG]" in upper and "STALLED FOR" in upper:
        error_type = "WATCHDOG_STALL"
        component = "main_loop" if "MAIN_LOOP" in upper else "WATCHDOG_COMPONENT"
        cause = "No real progress heartbeat was recorded before the watchdog grace window expired."
    elif "[WATCHDOG]" in upper and "RESTART FAILED" in upper:
        error_type = "WATCHDOG_RESTART_FAILED"
        component = "main_loop" if "MAIN_LOOP" in upper else "WATCHDOG_COMPONENT"
        cause = "In-process recovery did not restore component liveness; inspect restart_guard and the captured stack."
    elif "[WATCHDOG]" in upper and (
        "PROCESS RESTART BLOCKED" in upper
        or "PROCESS RESTART DEFERRED" in upper
        or "NO EXTERNAL PROCESS SUPERVISOR" in upper
    ):
        error_type = "WATCHDOG_RECOVERY_BLOCKED"
        component = "main_loop"
        cause = raw[:1600]
    elif "CRITICAL EXCEPTION" in upper:
        error_type = "MAIN_LOOP_EXCEPTION"
        component = "main_loop"
        cause = "The foreground main loop raised an uncaught exception; traceback is included in the incident message."
    else:
        error_type = "SYSTEM_ERROR"
        component = "runtime"
        cause = raw.split("|", 1)[1].strip()[:1600] if "|" in raw else raw[:1600]
    if "restart_guard=" in raw:
        tail = raw.split("restart_guard=", 1)[1]
        cause = ("restart_guard=" + tail.split()[0])[:1600]
    stack = ""
    marker = "[MAIN_THREAD_STACK]"
    if marker in raw:
        stack = raw.split(marker, 1)[1].strip()[-6000:]
    return {
        "timestamp": str(timestamp),
        "severity": str(severity).upper(),
        "error_type": error_type,
        "component": component,
        "message": raw[:2400],
        "cause": str(cause)[:1600],
        "stack_trace": stack,
    }


_SYSTEM_ERROR_STATE_KEY = "dashboard_system_errors_v1"
_SYSTEM_ERROR_LOCK = threading.RLock()


def _persist_system_errors():
    """Persist the bounded dashboard incident ring in the existing trade journal."""
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    setter = getattr(journal, "set_runtime_state", None)
    if not callable(setter):
        return False
    with _SYSTEM_ERROR_LOCK:
        records = list(DASHBOARD_STATE.get("system_errors", []))[-50:]
        payload = {"version": 1, "records": records}
    try:
        setter(_SYSTEM_ERROR_STATE_KEY, payload)
        return True
    except Exception:
        # Incident persistence must never interrupt trade management or recovery.
        return False


def _restore_system_errors():
    """Restore recent incidents after a process restart so Dashboard retains history."""
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    getter = getattr(journal, "get_runtime_state", None)
    if not callable(getter):
        return 0
    try:
        payload = getter(_SYSTEM_ERROR_STATE_KEY, default={})
    except TypeError:
        try:
            payload = getter(_SYSTEM_ERROR_STATE_KEY)
        except Exception:
            return 0
    except Exception:
        return 0
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        return 0
    valid = [row for row in records[-50:] if isinstance(row, dict)]
    with _SYSTEM_ERROR_LOCK:
        DASHBOARD_STATE["system_errors"] = valid
        DASHBOARD_STATE["errors"] = [
            f"[{row.get('timestamp', '-')}] {row.get('message', '')}"
            for row in valid
        ][-50:]
    return len(valid)


def log_execution(msg, level="INFO", debounce_key=None, debounce_sec=60):
    level_name = str(level or "INFO").upper()
    if debounce_key:
        now = time.time()
        last = MEMORY.get("log_debounce", {}).get(debounce_key, 0)
        if now - last < debounce_sec:
            return
        MEMORY.setdefault("log_debounce", {})[debounce_key] = now
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level_name == "INFO":
        colored = color_text(msg, CYAN)
    elif level_name == "SUCCESS":
        colored = color_text(msg, GREEN)
    elif level_name in {"ERROR", "CRITICAL"}:
        colored = color_text(msg, RED)
    elif level_name == "WARN":
        colored = color_text(msg, YELLOW)
    else:
        colored = msg
    entry = f"[{ts}] {msg}"
    DASHBOARD_STATE["logs"].append(entry)
    if len(DASHBOARD_STATE["logs"]) > 200:
        DASHBOARD_STATE["logs"].pop(0)
    try:
        print(colored)
    except UnicodeError:
        # A console encoding failure must never abort a close/recovery path.
        print(str(msg).encode("ascii", "backslashreplace").decode("ascii"))
    runtime_logger = _get_runtime_logger()
    if runtime_logger:
        try:
            runtime_logger.log(getattr(logging, level_name, logging.INFO), str(msg))
        except Exception:
            pass
    if level_name in {"ERROR", "CRITICAL"}:
        DASHBOARD_STATE["errors"].append(entry)
        if len(DASHBOARD_STATE["errors"]) > 50:
            DASHBOARD_STATE["errors"].pop(0)
        DASHBOARD_STATE.setdefault("system_errors", []).append(
            _system_error_record(msg, level_name, ts)
        )
        if len(DASHBOARD_STATE["system_errors"]) > 50:
            DASHBOARD_STATE["system_errors"].pop(0)
        _persist_system_errors()
        # Watchdog alerts have one canonical path to both Dashboard and Telegram.
        error_type = "NAISO_HEALTH" if "[WATCHDOG]" in str(msg).upper() else level_name
        tg_error(msg, error_type)

def update_stats(pnl_pct):
    DASHBOARD_STATE["stats"]["trades"] += 1
    if pnl_pct >= 0:
        DASHBOARD_STATE["stats"]["wins"] += 1
    else:
        DASHBOARD_STATE["stats"]["losses"] += 1
    total = DASHBOARD_STATE["stats"]["trades"]
    DASHBOARD_STATE["stats"]["win_rate"] = (DASHBOARD_STATE["stats"]["wins"] / total * 100) if total else 0

def _cancel_and_verify_entry_order_terminal(symbol, order_id=None, client_order_id=None, *, cancel_if_open=True):
    """Cancel an unfilled market-order remainder once, then require terminal read-back."""
    lookup_id = str(order_id or client_order_id or "").strip()
    client_id = str(client_order_id or "").strip()
    if not lookup_id:
        return False, "ORDER_ID_UNAVAILABLE"
    fetcher = getattr(ex, "fetch_order", None)
    if not callable(fetcher):
        return False, "FETCH_ORDER_UNAVAILABLE"
    params = {"clientOrderId": client_id} if client_id else {}

    def read_order():
        if params:
            return safe_api_call(fetcher, lookup_id, symbol, params, _api_max_attempts=1)
        return safe_api_call(fetcher, lookup_id, symbol, _api_max_attempts=1)

    def is_terminal(order):
        if not isinstance(order, dict):
            return False, "UNKNOWN", None
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        status = str(order.get("status") or info.get("status") or info.get("orderStatus") or "UNKNOWN").upper()
        filled = safe_float(order.get("filled", info.get("executedQty", 0.0)), 0.0)
        remaining_raw = order.get("remaining")
        if remaining_raw in (None, ""):
            amount_value = safe_float(order.get("amount", info.get("origQty", 0.0)), 0.0)
            remaining = max(0.0, amount_value - filled) if amount_value > 0 else None
        else:
            remaining = max(0.0, safe_float(remaining_raw, 0.0))
        terminal = status in {"CLOSED", "FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
        if terminal or (remaining is not None and remaining <= 1e-12):
            return True, status, {"filled": filled, "remaining": remaining}
        return False, status, {"filled": filled, "remaining": remaining}

    try:
        observed = read_order()
    except Exception as exc:
        return False, f"FETCH_BEFORE_CANCEL_FAILED:{type(exc).__name__}"
    terminal, status, details = is_terminal(observed)
    if terminal:
        return True, status
    remaining = details.get("remaining") if isinstance(details, dict) else None
    if remaining is None or remaining <= 1e-12:
        return False, f"ORDER_REMAINING_UNKNOWN:{status}"
    if not cancel_if_open:
        return False, f"ORDER_STILL_OPEN:{status}"
    canceler = getattr(ex, "cancel_order", None)
    if not callable(canceler):
        return False, "CANCEL_ORDER_UNAVAILABLE"
    try:
        if params:
            safe_api_call(canceler, lookup_id, symbol, params, _api_max_attempts=1)
        else:
            safe_api_call(canceler, lookup_id, symbol, _api_max_attempts=1)
    except Exception as exc:
        # Cancellation is one-shot. Read-back, not a blind retry, decides whether
        # the remainder is still live.
        log_execution(
            f"[ENTRY_RECONCILE] cancel outcome uncertain for {lookup_id}: "
            f"{type(exc).__name__}: {exc}", "WARN",
        )
    try:
        observed = read_order()
    except Exception as exc:
        return False, f"FETCH_AFTER_CANCEL_FAILED:{type(exc).__name__}"
    terminal, status, details = is_terminal(observed)
    if terminal:
        return True, status
    return False, f"REMAINDER_STILL_UNRESOLVED:{status}:{details}"


def open_position(side, amount, symbol, stop_loss=None, trade_id=None, management_state=None):
    global _ACTIVE_TRADE, RECOVERY_BLOCK_ENTRIES
    sym = normalize_symbol(symbol)
    side = str(side or "").upper()
    if side not in ("BUY", "SELL"):
        log_execution(f"[ENTRY] Invalid order side: {side!r}", "ERROR")
        return None
    # The execution boundary repeats the account/exposure gate. Strategy callers
    # are not trusted to be the only route into this non-idempotent operation.
    blocked_position_state = str(MEMORY.get("position_status") or "").upper()
    if (
        STATE.get("execution_unresolved") or STATE.get("recovery_hold")
        or STATE.get("close_reconciliation_required")
        or RECOVERY_BLOCK_ENTRIES
        or blocked_position_state == "UNKNOWN"
    ):
        log_execution(
            f"[ENTRY_BLOCKED] {sym}: execution boundary exposure/recovery gate "
            f"state_open={bool(STATE.get('open'))} "
            f"trade_state_open={bool(TRADE_STATE.get('in_position'))} "
            f"position_status={blocked_position_state or 'UNSET'} "
            f"execution_unresolved={bool(STATE.get('execution_unresolved'))} "
            f"recovery_hold={bool(STATE.get('recovery_hold'))}",
            "WARN", debounce_key=f"entry_boundary_hold:{sym}", debounce_sec=10,
        )
        return None
    portfolio_reservation_created = False
    with _TRADE_LOCK:
        trade_id, reservation, portfolio_reservation_created = _portfolio_ensure_open_reservation(
            trade_id, sym, management_state
        )
        if reservation is None:
            # Capacity is now authority: _ACTIVE_TRADE is only an aggregate view.
            log_execution(
                f"[ENTRY_LOCKED] {sym}: no six-slot reservation available "
                f"| state_open={bool(STATE.get('open'))} "
                f"| trade_state_open={bool(TRADE_STATE.get('in_position'))} "
                f"| recovery_hold={bool(STATE.get('recovery_hold'))} "
                f"| execution_unresolved={bool(STATE.get('execution_unresolved'))} "
                f"| close_reconciliation_required={bool(STATE.get('close_reconciliation_required'))} "
                f"| recovery_block_entries={bool(globals().get('RECOVERY_BLOCK_ENTRIES', False))}",
                "WARN",
                debounce_key=f"entry_locked:{sym}",
                debounce_sec=10,
            )
            return None
        _ACTIVE_TRADE = True
    order = None
    try:
        market = ex.market(sym)
        contract_size = safe_float(market.get("contractSize"), 0.0)
        if contract_size <= 0:
            raise ValueError(f"Invalid contractSize for {sym}: {contract_size}")
        amount = float(ex.amount_to_precision(sym, amount))
        if amount <= 0:
            raise ValueError("Order amount rounded to zero")

        bal = safe_api_call(ex.fetch_balance)
        if bal is None:
            return None
        usdt = safe_float((bal.get("free") or {}).get("USDT"), 0.0)
        ticker = safe_api_call(ex.fetch_ticker, sym)
        if not isinstance(ticker, dict):
            return None
        price = safe_float(ticker.get("last"), 0.0)
        if price <= 0:
            raise ValueError("Invalid live ticker price")

        required_margin = (amount * contract_size * price) / LEVERAGE
        if usdt < required_margin * 1.01:
            log_execution(f"Insufficient margin: need {required_margin:.2f}, have {usdt:.2f}", "ERROR")
            global INSUFFICIENT_MARGIN_COOLDOWN_UNTIL
            INSUFFICIENT_MARGIN_COOLDOWN_UNTIL = time.time() + INSUFFICIENT_MARGIN_COOLDOWN_SEC
            return None

        max_spread = dynamic_spread_tolerance(symbol)
        spread = get_spread_bps(symbol)
        if spread > max_spread:
            log_execution(f"Spread {spread:.2f}% > {max_spread}%", "WARN")
            return None

        if not PAPER_MODE:
            if stop_loss is None or not math.isfinite(float(stop_loss)) or float(stop_loss) <= 0:
                log_execution("[LIVE_SAFETY] Refusing LIVE entry without a valid structural stop", "ERROR")
                return None
            stop_value = float(stop_loss)
            if (side == "BUY" and stop_value >= price) or (side == "SELL" and stop_value <= price):
                log_execution(f"[LIVE_SAFETY] Structural SL is on the wrong side: side={side} entry={price} SL={stop_value}", "ERROR")
                return None
            service = TradeExecutionService(
                ex, journal=globals().get("INSTITUTIONAL_TRADE_JOURNAL"),
                logger=lambda message: log_execution(f"[EXECUTION] {message}", "INFO"),
                api_caller=safe_api_call,
            )
            result = service.submit_market_entry(
                symbol=sym, side=side, quantity=amount, leverage=LEVERAGE,
                structural_stop=stop_value, trade_id=str(trade_id or STATE.get("trade_id") or uuid.uuid4().hex),
                management_state=management_state,
            )
            if result.state not in {ExecutionState.FILLED}:
                filled_qty = safe_float(getattr(result, "filled_qty", 0.0), 0.0)
                raw_position = getattr(result, "raw_position", None)
                # PARTIAL_FILL / ENTRY_PRICE_UNKNOWN can represent real exposure,
                # not a clean rejection. Keep the single-entry lock and force an
                # account-wide reconciliation before any new order is possible.
                unresolved = bool(
                    result.state is ExecutionState.EXECUTION_UNCERTAIN
                    or result.state in {
                        ExecutionState.PARTIAL_FILL,
                        ExecutionState.ENTRY_PRICE_UNKNOWN,
                        ExecutionState.PROTECTION_HOLD,
                    }
                    or filled_qty > 0
                    or isinstance(raw_position, dict)
                )
                entry_order_terminal = result.state is ExecutionState.REJECTED
                entry_order_status = "REJECTED" if entry_order_terminal else "UNKNOWN"
                if result.state in {
                    ExecutionState.EXECUTION_UNCERTAIN,
                    ExecutionState.PARTIAL_FILL,
                    ExecutionState.ENTRY_PRICE_UNKNOWN,
                }:
                    entry_order_terminal, entry_order_status = _cancel_and_verify_entry_order_terminal(
                        sym, getattr(result, "exchange_order_id", None),
                        getattr(result, "client_order_id", None),
                    )
                partial_stop_id = None
                if filled_qty > 0 and isinstance(raw_position, dict):
                    # A known partial fill is already exchange exposure. Protect
                    # exactly the confirmed filled quantity before handing the
                    # unresolved remainder to account-wide recovery.
                    try:
                        partial_mode = PositionMode(str(getattr(result, "position_mode", "")).upper())
                        partial_position_side = str(getattr(result, "position_side", "") or "").upper()
                        if partial_position_side in {"BOTH", "LONG", "SHORT"}:
                            partial_stop_id = service.place_and_verify_native_stop(
                                symbol=sym, side=side, quantity=filled_qty,
                                trigger_price=float(stop_value), mode=partial_mode,
                                position_side=partial_position_side,
                            )
                    except Exception as protection_exc:
                        log_execution(
                            f"[LIVE_SAFETY] Partial fill native-stop placement failed: "
                            f"{type(protection_exc).__name__}: {protection_exc}",
                            "CRITICAL",
                        )
                STATE.update({
                    "execution_unresolved": unresolved,
                    "recovery_hold": unresolved,
                    "entry_execution_state": result.state.value,
                    "entry_client_order_id": result.client_order_id,
                    "entry_exchange_order_id": result.exchange_order_id,
                    "entry_requested_qty": safe_float(getattr(result, "requested_qty", amount), amount),
                    "entry_filled_qty": filled_qty,
                    "entry_remaining_qty": safe_float(getattr(result, "remaining_qty", 0.0), 0.0),
                    "entry_order_terminal": bool(entry_order_terminal),
                    "entry_order_status": str(entry_order_status or "UNKNOWN"),
                    "current_symbol": sym if unresolved else STATE.get("current_symbol"),
                    "side": side if unresolved else STATE.get("side"),
                    "trade_id": str(trade_id or getattr(result, "trade_id", "") or STATE.get("trade_id") or ""),
                    "qty": filled_qty if filled_qty > 0 else STATE.get("qty", 0.0),
                    "initial_qty": filled_qty if filled_qty > 0 else STATE.get("initial_qty", 0.0),
                    "remaining_qty": filled_qty if filled_qty > 0 else STATE.get("remaining_qty", 0.0),
                    "exchange_qty": filled_qty if filled_qty > 0 else STATE.get("exchange_qty", 0.0),
                    "synthetic_sl": float(stop_value) if filled_qty > 0 else STATE.get("synthetic_sl"),
                    "sl": float(stop_value) if filled_qty > 0 else STATE.get("sl"),
                    "native_sl_verified": bool(partial_stop_id),
                    "native_sl_order_id": partial_stop_id,
                    "native_sl_status": "VERIFIED" if partial_stop_id else (
                        "UNVERIFIED_PARTIAL_FILL" if filled_qty > 0 else STATE.get("native_sl_status", "UNKNOWN")
                    ),
                    "native_sl_original_price": float(stop_value) if filled_qty > 0 else STATE.get("native_sl_original_price", 0.0),
                    "native_sl_original_qty": filled_qty if filled_qty > 0 else STATE.get("native_sl_original_qty", 0.0),
                    "native_sl_last_verified_price": float(stop_value) if partial_stop_id else STATE.get("native_sl_last_verified_price", 0.0),
                })
                if unresolved:
                    RECOVERY_BLOCK_ENTRIES = True
                    MEMORY["position_status"] = "UNKNOWN"
                    MEMORY["recovery_status"] = (
                        "PARTIAL_FILL_PROTECTION_VERIFIED_RECOVERY_REQUIRED"
                        if partial_stop_id else "ENTRY_EXECUTION_RECONCILIATION_REQUIRED"
                    )
                log_execution(
                    f"[LIVE_SAFETY] Entry not fully verified: {result.state.value} "
                    f"filled={filled_qty:.10f} remaining={safe_float(getattr(result, 'remaining_qty', 0.0), 0.0):.10f} "
                    f"({result.reason}); entries held pending exchange reconciliation",
                    "ERROR",
                )
                return None
            native_sl_id = service.place_and_verify_native_stop(
                symbol=sym, side=side, quantity=result.filled_qty,
                trigger_price=stop_value, mode=PositionMode(result.position_mode),
                position_side=result.position_side,
            )
            order = dict(result.raw_order or {})
            order.update({"id": result.exchange_order_id, "filled": result.filled_qty,
                          "remaining": result.remaining_qty, "average": result.average_fill_price,
                          "clientOrderId": result.client_order_id,
                          "positionMode": result.position_mode, "positionSide": result.position_side,
                          "native_sl_order_id": native_sl_id, "native_sl_verified": bool(native_sl_id),
                          "protection_unverified": not bool(native_sl_id)})
            if not native_sl_id:
                # Exposure is real and must be represented in the caller's durable
                # management state. Keep the entry blocked; do not discard the fill.
                STATE.update({"execution_unresolved": True, "recovery_hold": True,
                              "entry_execution_state": ExecutionState.PROTECTION_HOLD.value,
                              "position_mode": result.position_mode,
                              "position_side": result.position_side,
                              "trade_id": str(trade_id or STATE.get("trade_id") or result.trade_id)})
                log_execution("[LIVE_SAFETY] Native STOP_MARKET could not be verified; filled exposure retained in RECOVERY_HOLD", "ERROR")
        else:
            order = safe_api_call(ex.create_order, sym, "market", side.lower(), amount, params={})
        if order:
            log_execution(
                f"Order submitted: {side} {amount} {sym} @ reference={price:.8f} "
                f"| contractSize={contract_size:g} | margin≈{required_margin:.4f} | leverage={LEVERAGE}x",
                "SUCCESS"
            )
            return order
    except Exception as e:
        log_execution(f"Open position error: {type(e).__name__}: {e}", "ERROR")
    finally:
        # Keep the entry lock held after a returned order; execute_entry adopts the
        # position state immediately after this function returns. Do not create a
        # concurrency window where a second entry can race the state update.
        if order is None and not STATE.get("execution_unresolved"):
            with _TRADE_LOCK:
                _ACTIVE_TRADE = False
                if portfolio_reservation_created:
                    _portfolio_release_reservation(
                        trade_id, sym, "PRE_ORDER_FAILURE_NO_EXPOSURE"
                    )
    return None


def close_position(amount, symbol):
    """Legacy adapter; delegates to the single verified close path."""
    ok = safe_close_position(symbol, qty=amount, close_reason="LEGACY_CLOSE_ADAPTER")
    if not ok:
        return None
    return {"id": STATE.get("last_close_order_id"),
            "status": "closed" if STATE.get("close_verified") else "partially_closed",
            "filled": STATE.get("last_close_filled_qty", 0.0),
            "average": STATE.get("last_close_fill_price")}


def close_position_full(close_reason="LEGACY_FULL_CLOSE"):
    """Full-close compatibility adapter; never finalizes before verified exposure is flat."""
    if not STATE.get("open") or not STATE.get("current_symbol"):
        return False
    symbol = STATE["current_symbol"]
    ok = safe_close_position(symbol, close_reason=str(close_reason))
    if ok and STATE.get("close_verified"):
        finalize_trade_with_reality(symbol, close_reason=str(close_reason))
        log_execution("[CLOSE_VERIFY] Compatibility full-close adapter completed after exchange verification", "SUCCESS")
        return True
    if not STATE.get("open") and MEMORY.get("position_status") == "FLAT":
        log_execution(f"[CLOSE_VERIFY] {symbol} was already FLAT on BingX; external close reconciled", "SUCCESS")
        return True
    log_execution(f"[CLOSE_VERIFY] Full close not finalized; exchange FLAT was not verified ({close_reason})", "ERROR")
    return False

def close_partial(ratio):
    # This is kept for backward compatibility; use safe_close_partial now
    return safe_close_partial(STATE["current_symbol"], ratio)


# ========== INSTITUTIONAL ENTRY CLOSED-CANDLE GATE ==========
ENTRY_CLOSED_CANDLE_MIN_BARS = 80
ENTRY_CLOSED_CANDLE_MAX_AGE_INTERVALS = 2.0
ENTRY_CANDLE_RETRY_DELAY_SEC = 0.35
_ENTRY_TIME_LOCK = threading.Lock()
_ENTRY_TIME_SNAPSHOT = {"monotonic": 0.0, "epoch_ms": 0, "source": "LOCAL_UTC_EPOCH"}


def _entry_exchange_time_ms():
    """Use cached BingX server time where available; otherwise use UTC epoch time."""
    now_mono = time.monotonic()
    with _ENTRY_TIME_LOCK:
        snap = dict(_ENTRY_TIME_SNAPSHOT)
        if snap["epoch_ms"] > 0 and now_mono - snap["monotonic"] <= 15.0:
            return int(snap["epoch_ms"] + (now_mono - snap["monotonic"]) * 1000.0), snap["source"]

        epoch_ms = None
        source = "LOCAL_UTC_EPOCH"
        exchange_obj = globals().get("ex")
        has = getattr(exchange_obj, "has", {}) if exchange_obj is not None else {}
        fetch_time = getattr(exchange_obj, "fetch_time", None) if exchange_obj is not None else None
        if callable(fetch_time) and isinstance(has, dict) and has.get("fetchTime"):
            try:
                value = safe_api_call(fetch_time)
                if value is not None:
                    candidate = int(float(value))
                    epoch_ms = candidate if candidate > 10**12 else candidate * 1000
                    if epoch_ms > 0:
                        source = "BINGX_SERVER_TIME"
            except Exception as exc:
                logger = globals().get("log_execution")
                if logger:
                    logger(
                        f"[ENTRY_TIME] exchange time unavailable; UTC epoch fallback ({type(exc).__name__})",
                        "WARN", debounce_key="entry_exchange_time_fallback", debounce_sec=60,
                    )
        if epoch_ms is None:
            epoch_ms = int(time.time() * 1000)

        _ENTRY_TIME_SNAPSHOT.update({
            "monotonic": now_mono, "epoch_ms": epoch_ms, "source": source,
        })
        return int(epoch_ms), source


def _entry_symbol_key(symbol):
    canonical = globals().get("roro_canonical_symbol")
    try:
        return str(canonical(symbol)) if callable(canonical) else str(symbol).strip().upper()
    except Exception:
        return str(symbol).strip().upper()


def _validate_entry_closed_frame(symbol, frame, context=None, *, now_ms=None, time_source=""):
    """Validate symbol/timeframe, closed-bar status, age and minimum history."""
    context = context if isinstance(context, dict) else {}
    expected_symbol = context.get("exchange_symbol") or context.get("symbol")
    if expected_symbol and _entry_symbol_key(expected_symbol) != _entry_symbol_key(symbol):
        return None, {
            "reason": "SYMBOL_TIMEFRAME_MISMATCH",
            "detail": f"context_symbol={expected_symbol} requested_symbol={symbol}",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }

    expected_tf = str(INTERVAL)
    context_tf = str(context.get("candle_timeframe") or expected_tf)
    if context_tf != expected_tf:
        return None, {
            "reason": "SYMBOL_TIMEFRAME_MISMATCH",
            "detail": f"context_timeframe={context_tf} expected_timeframe={expected_tf}",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }

    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        fetch_diag = OHLCV_FETCH_DIAGNOSTICS.get(str(normalize_symbol(symbol)), {})
        last_rate = _LAST_API_RATE_LIMIT
        rate_recent = (
            last_rate.get("method") == "fetch_ohlcv"
            and time.time() - float(last_rate.get("ts", 0.0)) <= 15.0
        )
        reason = fetch_diag.get("reason")
        if reason not in {"OHLCV_RATE_LIMITED", "INSUFFICIENT_CLOSED_BARS"}:
            reason = "OHLCV_RATE_LIMITED" if rate_recent else "OHLCV_EMPTY"
        return None, {
            "reason": reason,
            "detail": fetch_diag.get("detail") or "no usable OHLCV frame was available",
            "received_candles": int(fetch_diag.get("received_candles", 0) or 0),
            "source": context.get("closed_candle_source", "EXCHANGE_FETCH"),
        }

    required = {"timestamp", "open", "high", "low", "close", "volume"}
    if not required.issubset(set(frame.columns)):
        return None, {
            "reason": "OHLCV_EMPTY",
            "detail": f"missing_columns={sorted(required - set(frame.columns))}",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }

    timestamps = pd.to_numeric(frame["timestamp"], errors="coerce").dropna()
    if timestamps.empty or float(timestamps.max()) < 10**12:
        return None, {
            "reason": "SYMBOL_TIMEFRAME_MISMATCH",
            "detail": "timestamps are missing or not Unix milliseconds",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }
    timeframe_s = int(_roro_timeframe_seconds(expected_tf))
    expected_interval_ms = timeframe_s * 1000
    diffs = timestamps.sort_values().diff().dropna()
    positive_diffs = diffs[diffs > 0]
    if len(positive_diffs) >= 5:
        median_interval_ms = float(positive_diffs.median())
        ratio = median_interval_ms / float(expected_interval_ms)
        if not 0.80 <= ratio <= 1.20:
            return None, {
                "reason": "SYMBOL_TIMEFRAME_MISMATCH",
                "detail": (
                    f"median_bar_interval_ms={median_interval_ms:.0f} "
                    f"expected_interval_ms={expected_interval_ms}"
                ),
                "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
            }

    if now_ms is None:
        now_ms, time_source = _entry_exchange_time_ms()
    validator = globals().get("roro_validate_closed_frame")
    if not callable(validator):
        return None, {
            "reason": "OHLCV_EMPTY",
            "detail": "closed-candle validator is unavailable",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }
    try:
        closed = validator(
            frame.copy(), now_ms=int(now_ms), timeframe_s=timeframe_s,
        )
    except Exception as exc:
        return None, {
            "reason": "OHLCV_EMPTY",
            "detail": f"validator_error={type(exc).__name__}:{exc}",
            "source": context.get("closed_candle_source", "ENTRY_CONTEXT"),
        }

    source = context.get("closed_candle_source", "ENTRY_CONTEXT")
    received = int(len(frame))
    closed_count = int(len(closed)) if isinstance(closed, pd.DataFrame) else 0
    if closed_count == 0:
        return None, {
            "reason": "ONLY_FORMING_CANDLE",
            "detail": "OHLCV rows exist but none has a confirmed close time",
            "received_candles": received,
            "closed_candles": 0,
            "time_source": time_source,
            "source": source,
        }
    if closed_count < ENTRY_CLOSED_CANDLE_MIN_BARS:
        return None, {
            "reason": "INSUFFICIENT_CLOSED_BARS",
            "detail": (
                f"closed_candles={closed_count} "
                f"required={ENTRY_CLOSED_CANDLE_MIN_BARS}"
            ),
            "received_candles": received,
            "closed_candles": closed_count,
            "time_source": time_source,
            "source": source,
        }

    last_open_ms = int(float(closed["timestamp"].iloc[-1]))
    close_time_ms = last_open_ms + expected_interval_ms
    age_ms = int(now_ms) - close_time_ms
    if age_ms < -2000:
        return None, {
            "reason": "ONLY_FORMING_CANDLE",
            "detail": f"latest_bar_not_closed_yet_by_exchange_time age_ms={age_ms}",
            "received_candles": received,
            "closed_candles": closed_count,
            "time_source": time_source,
            "source": source,
        }
    max_age_ms = int(expected_interval_ms * ENTRY_CLOSED_CANDLE_MAX_AGE_INTERVALS)
    if age_ms > max_age_ms:
        return None, {
            "reason": "CLOSED_CANDLE_STALE",
            "detail": (
                f"last_closed_ts={last_open_ms} age_sec={age_ms / 1000.0:.1f} "
                f"max_age_sec={max_age_ms / 1000.0:.1f}"
            ),
            "received_candles": received,
            "closed_candles": closed_count,
            "time_source": time_source,
            "source": source,
        }
    if not validate_dataframe(closed, ENTRY_CLOSED_CANDLE_MIN_BARS):
        return None, {
            "reason": "OHLCV_EMPTY",
            "detail": "closed OHLCV frame failed structural/finite data validation",
            "received_candles": received,
            "closed_candles": closed_count,
            "time_source": time_source,
            "source": source,
        }

    return closed.reset_index(drop=True), {
        "reason": "VALID",
        "received_candles": received,
        "closed_candles": closed_count,
        "last_closed_candle_ts": last_open_ms,
        "closed_candle_age_sec": round(max(0.0, age_ms / 1000.0), 3),
        "time_source": time_source,
        "source": source,
        "timeframe": expected_tf,
    }


ENTRY_CANDLE_RETRYABLE_REASONS = frozenset({"CLOSED_CANDLE_STALE"})


def resolve_entry_closed_candles(symbol, context=None):
    """Reuse the authorized frame; refresh once only for a genuinely stale snapshot."""
    context = context if isinstance(context, dict) else {}
    shared_frame = context.get("closed_candle_frame")
    if isinstance(shared_frame, pd.DataFrame):
        frame = shared_frame.copy()
        source = str(context.get("closed_candle_source") or "SHARED_AUTHORIZATION_FRAME")
    else:
        frame = get_ohlcv_safe(symbol, 150)
        source = "SCANNER_CACHE_OR_EXCHANGE"

    local_context = dict(context)
    local_context.setdefault("exchange_symbol", symbol)
    local_context.setdefault("candle_timeframe", str(INTERVAL))
    local_context["closed_candle_source"] = source

    final_frame = None
    final_diag = None
    for attempt in range(2):
        if attempt == 1:
            # A single governed refresh through the shared single-flight cache.
            # It never bypasses the API governor and only runs for a reason a
            # newer closed candle can actually repair.
            time.sleep(ENTRY_CANDLE_RETRY_DELAY_SEC)
            refreshed, refresh_source = get_ohlcv_safe_meta(symbol, 150, force_refresh=True)
            frame = refreshed
            source = f"FRESH_EXCHANGE_RETRY:{refresh_source}"
            local_context["closed_candle_source"] = source
        now_ms, time_source = _entry_exchange_time_ms()
        checked, diagnostic = _validate_entry_closed_frame(
            symbol, frame, local_context, now_ms=now_ms, time_source=time_source,
        )
        if checked is not None:
            final_frame, final_diag = checked, diagnostic
            break
        final_diag = diagnostic
        reason = str(diagnostic.get("reason", "OHLCV_EMPTY"))
        if reason not in ENTRY_CANDLE_RETRYABLE_REASONS:
            break

    MEMORY.setdefault("last_closed_candle_validation", {}).update({
        "symbol": str(symbol),
        **(final_diag or {"reason": "OHLCV_EMPTY"}),
        "checked_at": time.time(),
    })
    if final_frame is not None:
        log_execution(
            f"[ENTRY_CANDLE_VALID] {symbol} timeframe={final_diag.get('timeframe')} "
            f"closed={final_diag.get('closed_candles')} "
            f"last_closed_ts={final_diag.get('last_closed_candle_ts')} "
            f"age_sec={final_diag.get('closed_candle_age_sec')} "
            f"time_source={final_diag.get('time_source')} source={final_diag.get('source')}",
            "INFO",
        )
    return final_frame, (final_diag or {"reason": "OHLCV_EMPTY"})


def record_entry_data_rejection(symbol, side, diagnostic, *, stage="INSTITUTIONAL_ENTRY_GATE"):
    """Feed structured candle-data rejections into the Dashboard WHY NO ENTRY panel."""
    diagnostic = diagnostic if isinstance(diagnostic, dict) else {}
    reason_code = str(diagnostic.get("reason") or "OHLCV_EMPTY")
    detail = str(diagnostic.get("detail") or "closed-candle validation failed")
    now = time.time()
    metrics = MEMORY.setdefault("entry_rejection_metrics", {})
    metrics[reason_code] = int(metrics.get(reason_code, 0) or 0) + 1
    MEMORY["last_closed_candle_validation"] = {
        "symbol": str(symbol), "side": str(side), "reason": reason_code,
        "detail": detail, "stage": stage, "checked_at": now,
        **{k: v for k, v in diagnostic.items() if k not in {"reason", "detail"}},
    }
    feed = MEMORY.setdefault("no_entry_feed", [])
    feed.append({
        "time": now, "symbol": str(symbol), "side": str(side),
        "reason": f"{reason_code}: {detail}", "score": None,
        "category": reason_code, "stage": stage,
    })
    MEMORY["no_entry_feed"] = feed[-100:]
    log_execution(
        f"[ENTRY_DATA_REJECT] {symbol} {side} stage={stage} reason={reason_code} detail={detail}",
        "ERROR",
        debounce_key=f"entry_data_reject:{symbol}:{reason_code}",
        debounce_sec=5,
    )


def _entry_location_permission(report, *, strict_freshness):
    """Read-only location decision used by every path that reaches execute_entry.

    Strict mode is the LIVE production path: an ALLOWED gate is mandatory, so a
    missing, stale or non-validated report can never be presented as a fresh
    allowance. Non-strict paper/legacy paths retain the neutral-missing contract
    and only block on an observed insufficient-room decision.
    """
    gate = validate_institutional_location(report)
    allowed = bool(gate["allowed"]) if strict_freshness else gate["state"] != "BLOCKED"
    reason = gate["blocking_reason"] or "|".join(gate["reasons"])
    if not allowed and not reason:
        reason = "location_evidence_insufficient_or_stale"
    return allowed, gate, reason


def _validate_selected_entry_zone(zone, side, *, price, atr, latest_timestamp, timeframe_ms):
    """Final fail-closed proof that a live institutional entry retested its own side's zone."""
    expected_direction = "DEMAND" if str(side or "").upper() == "BUY" else "SUPPLY" if str(side or "").upper() == "SELL" else ""
    if not expected_direction or not isinstance(zone, dict):
        return False, "missing_selected_entry_zone"
    if str(zone.get("side") or "").upper() != str(side).upper():
        return False, "entry_zone_side_mismatch"
    if str(zone.get("zone_direction") or "").upper() != expected_direction:
        return False, "entry_zone_direction_mismatch"
    kind = str(zone.get("kind") or "").upper()
    if kind not in {"ORDER_BLOCK", "FVG", "SUPPORT", "RESISTANCE"}:
        return False, "entry_zone_kind_invalid"
    try:
        low = float(zone.get("low"))
        high = float(zone.get("high"))
        formed = int(zone.get("available_timestamp"))
        latest = int(latest_timestamp)
        duration = int(timeframe_ms)
        px = float(price)
        volatility = float(atr)
    except (TypeError, ValueError, OverflowError):
        return False, "entry_zone_geometry_or_timestamp_missing"
    if not all(math.isfinite(v) for v in (low, high, px, volatility)) or not (0 < low < high and px > 0 and volatility > 0):
        return False, "entry_zone_geometry_invalid"
    if formed <= 0 or latest <= 0 or duration <= 0 or formed > latest:
        return False, "entry_zone_timestamp_invalid"
    age_bars = (latest - formed) / duration
    if not math.isfinite(age_bars) or age_bars > 48:
        return False, "entry_zone_stale"
    if max(low - px, 0.0, px - high) / volatility > 1.5:
        return False, "entry_zone_outside_no_chase_distance"
    if kind == "ORDER_BLOCK":
        expected_definition = "BULLISH_LOW_TO_OPEN" if side == "BUY" else "BEARISH_OPEN_TO_HIGH"
        if str(zone.get("zone_definition") or "").upper() != expected_definition:
            return False, "entry_zone_order_block_definition_mismatch"
        try:
            if float(zone.get("strength", 0) or 0) < 5.0:
                return False, "entry_zone_order_block_quality_below_threshold"
        except (TypeError, ValueError, OverflowError):
            return False, "entry_zone_order_block_quality_invalid"
    elif kind == "FVG":
        expected = "BULLISH" if side == "BUY" else "BEARISH"
        if str(zone.get("direction") or "").upper() != expected:
            return False, "entry_zone_fvg_direction_mismatch"
        if str(zone.get("status") or "").upper() not in {"UNFILLED", "PARTIALLY_FILLED"}:
            return False, "entry_zone_fvg_not_actionable"
        try:
            size = float(zone.get("size_atr"))
            if not math.isfinite(size) or size < 0.08:
                return False, "entry_zone_fvg_too_small"
        except (TypeError, ValueError, OverflowError):
            return False, "entry_zone_fvg_size_invalid"
    else:
        expected = "SUPPORT" if side == "BUY" else "RESISTANCE"
        if kind != expected or str(zone.get("status") or "").upper() != "CONFIRMED":
            return False, "entry_zone_support_resistance_wrong_side_or_unconfirmed"
        try:
            if float(zone.get("strength", 0) or 0) < 5.0:
                return False, "entry_zone_support_resistance_quality_below_threshold"
        except (TypeError, ValueError, OverflowError):
            return False, "entry_zone_support_resistance_quality_invalid"
    return True, "selected_entry_zone_validated"


def execute_entry(side, symbol, price, sl, tp1, tp2, score, reason, atr_val, trade_type, entry_type, classification):
    _portfolio_ensure_test_scope()
    if daily_loss_cooldown_active():
        log_execution(f"[ENTRY_BLOCKED] drawdown cooldown active; {symbol} {side} rejected", "WARN",
                      debounce_key=f"loss_cooldown_entry:{symbol}", debounce_sec=15)
        return False
    # Canonicalize once at the strategy/execution boundary so every downstream
    # risk, position, SL/TP and management branch sees the same direction token.
    side = str(side or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        log_execution(f"[ENTRY_DIRECTION_REJECT] invalid side={side!r}; expected BUY or SELL", "ERROR")
        return False
    trade_id = uuid.uuid4().hex
    institutional_entry_ctx = globals().get("_INSTITUTIONAL_ENTRY_CONTEXT") or {}
    # Final execution-boundary gate: no institutional, sniper, RF or legacy
    # setup can bypass the mandatory EMA9-to-session-VWAP closed-candle cross.
    gate_frame = institutional_entry_ctx.get("closed_candle_frame")
    gate_frame_source = institutional_entry_ctx.get("closed_candle_source") or "ENTRY_CONTEXT"
    gate_tf = str(institutional_entry_ctx.get("candle_timeframe") or INTERVAL)
    try:
        # Legacy callers may not carry the RORO frame in their context. Re-fetch
        # through the existing shared scanner-frame cache; missing data never bypasses the gate.
        if gate_frame is None:
            gate_frame = _integration_fetch_frame(symbol)
            gate_frame_source = "SHARED_SCANNER_FRAME_FALLBACK"
        gate_result = _validate_dual_ema_vwap_entry_gate(
            gate_frame, side, ema, timeframe_seconds=_roro_timeframe_seconds(gate_tf),
            now_ms=int(time.time() * 1000), lookback_candles=2,
        )
        gate_result["frame_source"] = gate_frame_source
    except Exception as gate_exc:
        gate_result = {"ready": False, "reason": f"gate_error:{type(gate_exc).__name__}"}
    raw_signal_id = str(gate_result.get("signal_candle_id") or "").strip()
    if raw_signal_id:
        try:
            event_symbol = normalize_symbol(symbol)
        except Exception:
            event_symbol = str(symbol or "").strip().upper()
        gate_result["durable_event_id"] = f"{event_symbol}:{gate_tf}:{raw_signal_id}"
    else:
        gate_result["durable_event_id"] = ""
    cross_event_id = str(institutional_entry_ctx.get("cross_event_id") or "").strip()
    if not cross_event_id and not PAPER_MODE and MODE_LIVE:
        # Expected fail-closed decision, not a runtime failure. Keep it out of
        # SYSTEM ERRORS and Telegram ERROR alerts; entry authority is unchanged.
        log_execution(
            f"[CROSS_EVENT] BLOCKED LIVE {symbol} {side}: entry requires a durable confirmed cross event",
            "WARN",
        )
        return False
    if cross_event_id:
        if not _cross_event_is_usable(cross_event_id, side):
            log_execution(
                f"[CROSS_EVENT] BLOCKED {symbol} {side}: cross event {cross_event_id} "
                "is not active or has no confirmed closed rejection breakout", "WARN",
            )
            return False
        # Delayed breakout entries intentionally outlive the 2-candle fresh-cross
        # gate. Bind execution back to the ORIGINAL cross identity and let the
        # durable rejection lifecycle authorize this specific closed breakout.
        _hydrate_cross_lifecycle_context(
            institutional_entry_ctx, cross_event_id, gate_result, side, symbol, gate_tf,
        )
        durable_zone = gate_result.get("selected_entry_zone")
        supplied_zone = institutional_entry_ctx.get("entry_zone")
        if not isinstance(durable_zone, dict) or not isinstance(supplied_zone, dict):
            log_execution(
                f"[ENTRY_ZONE_GATE] BLOCKED {symbol} {side}: durable cross event has no matching selected structural zone",
                "WARN", debounce_key=f"cross_zone_missing:{cross_event_id}", debounce_sec=10,
            )
            return False
        zone_identity = ("kind", "side", "zone_direction", "available_timestamp")
        try:
            same_zone = all(str(supplied_zone.get(key)) == str(durable_zone.get(key)) for key in zone_identity)
            same_zone = same_zone and math.isclose(float(supplied_zone.get("low")), float(durable_zone.get("low")), rel_tol=1e-10, abs_tol=1e-12)
            same_zone = same_zone and math.isclose(float(supplied_zone.get("high")), float(durable_zone.get("high")), rel_tol=1e-10, abs_tol=1e-12)
        except (TypeError, ValueError, OverflowError):
            same_zone = False
        if not same_zone:
            log_execution(
                f"[ENTRY_ZONE_GATE] BLOCKED {symbol} {side}: Watchlist zone differs from durable cross-event zone",
                "WARN", debounce_key=f"cross_zone_mismatch:{cross_event_id}", debounce_sec=10,
            )
            return False
        if gate_result.get("durable_event_id") and cross_event_id != gate_result["durable_event_id"]:
            log_execution(
                f"[CROSS_EVENT] BLOCKED {symbol} {side}: watchlist event {cross_event_id} "
                f"does not match execution gate event {gate_result['durable_event_id']}", "ERROR",
            )
            return False
        zone_ok, zone_reason = _validate_selected_entry_zone(
            supplied_zone, side, price=price, atr=atr_val,
            latest_timestamp=institutional_entry_ctx.get("closed_candle_timestamp"),
            timeframe_ms=institutional_entry_ctx.get("timeframe_ms")
                or int(_roro_timeframe_seconds(gate_tf) * 1000),
        )
        if not zone_ok:
            log_execution(
                f"[ENTRY_ZONE_GATE] BLOCKED {symbol} {side}: durable Watchlist zone failed final validation: {zone_reason}",
                "WARN", debounce_key=f"cross_zone_final:{cross_event_id}:{zone_reason}", debounce_sec=10,
            )
            return False
        institutional_entry_ctx["entry_zone_gate"] = {
            "allowed": True, "reason": zone_reason,
            "kind": supplied_zone.get("kind"),
            "zone_direction": supplied_zone.get("zone_direction"),
            "low": supplied_zone.get("low"), "high": supplied_zone.get("high"),
            "available_timestamp": supplied_zone.get("available_timestamp"),
            "authority": "UNIFIED_WATCHLIST_ENTRY_ZONE",
        }
    institutional_entry_ctx["dual_ema_vwap_gate"] = dict(gate_result)
    # Preserve the exact gate snapshot for the serialized final execution wrapper.
    globals()["_LAST_EXECUTION_GATE_RESULT"] = dict(gate_result)
    if not gate_result.get("ready"):
        log_execution(
            f"[VWAP_ENTRY_GATE] BLOCKED {symbol} {side} reason={gate_result.get('reason')} "
            f"EMA9={gate_result.get('ema9')} EMA50={gate_result.get('ema50')} "
            f"VWAP={gate_result.get('vwap')} EMA9_cross={gate_result.get('ema9_cross')} "
            f"EMA50_cross={gate_result.get('ema50_cross')} closed_ts={gate_result.get('latest_closed_timestamp')}",
            "WARN", debounce_key=f"dual_ema_vwap_gate:{symbol}:{side}:{gate_result.get('reason')}",
            debounce_sec=5,
        )
        return False
    # Final location enforcement lives in the single executor so no Watchlist,
    # Sniper or legacy caller can bypass a strong opposing supply/demand zone.
    # A synthesized fallback never claims fresh validated closed data it cannot
    # prove; LIVE additionally requires an explicit ALLOWED gate.
    # Institutional Watchlist entries must carry the exact retested zone chosen
    # by the structural engine. Sniper/legacy and PAPER contracts are unchanged.
    if not PAPER_MODE and MODE_LIVE and institutional_entry_ctx.get("setup_type") in {
        "REVERSAL_ENGINE", "FAILED_AUCTION_REVERSAL", "TREND_CONTINUATION_ENGINE",
        "TREND_CONTINUATION_RETEST",
    }:
        selected_zone = institutional_entry_ctx.get("entry_zone")
        zone_ok, zone_reason = _validate_selected_entry_zone(
            selected_zone, side, price=price, atr=atr_val,
            latest_timestamp=institutional_entry_ctx.get("closed_candle_timestamp"),
            timeframe_ms=institutional_entry_ctx.get("timeframe_ms"),
        )
        if not zone_ok:
            log_execution(
                f"[ENTRY_ZONE_GATE] BLOCKED {symbol} {side}: {zone_reason}",
                "WARN", debounce_key=f"entry_zone_gate:{symbol}:{side}:{zone_reason}", debounce_sec=10,
            )
            return False
        institutional_entry_ctx["entry_zone_gate"] = {
            "allowed": True, "reason": zone_reason,
            "kind": selected_zone.get("kind"),
            "zone_direction": selected_zone.get("zone_direction"),
            "low": selected_zone.get("low"), "high": selected_zone.get("high"),
            "available_timestamp": selected_zone.get("available_timestamp"),
        }
    report = institutional_entry_ctx.get("institutional_report")
    if not isinstance(report, dict) or not report:
        report = build_institutional_report({
            "side": side,
            "price": price,
            "atr": atr_val,
            "support_resistance": institutional_entry_ctx.get("support_resistance"),
            "market_regime": institutional_entry_ctx.get("market_regime"),
            "liquidity": institutional_entry_ctx.get("liquidity"),
            "ema9": gate_result.get("ema9"),
            "vwap": gate_result.get("vwap"),
            "cross_state": str(gate_result.get("ema9_cross") or "").upper(),
            "cross_event_id": cross_event_id,
            "data_freshness": institutional_entry_ctx.get("data_freshness", "UNKNOWN"),
            "data_quality": institutional_entry_ctx.get("data_quality", "UNKNOWN"),
            "closed_candle_source": institutional_entry_ctx.get("closed_candle_source", "UNKNOWN"),
            "last_updated": institutional_entry_ctx.get("last_updated", 0.0),
            "timeframe_ms": _roro_timeframe_seconds(gate_tf) * 1000.0,
        }, side=side, now_ts=time.time())
    allowed, location_gate, blocking_reason = _entry_location_permission(
        report, strict_freshness=not PAPER_MODE and MODE_LIVE,
    )
    institutional_entry_ctx["institutional_report"] = report
    institutional_entry_ctx["institutional_location_gate"] = location_gate
    if not allowed:
        log_execution(
            f"[INSTITUTIONAL_LOCATION] BLOCKED {symbol} {side}: {blocking_reason}",
            "WARN", debounce_key=f"institutional_location_executor:{symbol}:{side}", debounce_sec=10,
        )
        return False
    # Every executed entry must be traceable to a durable confirmed-cross event,
    # even when an older caller did not carry the Watchlist event id. The row is
    # created only from the strict gate result above; scanner rows can never
    # manufacture it.
    gate_event_id = str(gate_result.get("durable_event_id") or "").strip()
    gate_journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if not cross_event_id and gate_event_id and gate_journal is not None:
        try:
            gate_journal.record_cross_event(
                event_id=gate_event_id,
                symbol=roro_canonical_symbol(symbol),
                exchange_symbol=str(symbol),
                side=side,
                direction=str(gate_result.get("ema9_cross") or "").upper(),
                source=CONFIRMED_CROSS_EVENT_SOURCE,
                timeframe=gate_tf,
                signal_candle_timestamp=int(gate_result.get("signal_candle_timestamp") or 0),
                closed_candle_timestamp=int(gate_result.get("latest_closed_timestamp") or 0),
                expires_ts=time.time() + CONFIRMED_CROSS_EVENT_TTL_SEC,
                metadata={**gate_result, "detection_method": "execute_entry_final_gate"},
            )
            cross_event_id = gate_event_id
            institutional_entry_ctx["cross_event_id"] = cross_event_id
        except Exception as exc:
            log_execution(
                f"[CROSS_EVENT] BLOCKED {symbol} {side}: cannot persist gate event "
                f"{gate_event_id}: {type(exc).__name__}: {exc}", "ERROR",
            )
            return False
        if not _cross_event_is_usable(cross_event_id, side):
            log_execution(
                f"[CROSS_EVENT] BLOCKED {symbol} {side}: gate event {cross_event_id} "
                "is not an active confirmed cross", "ERROR",
            )
            return False
    # Consume the current signal identity only after this call has computed it,
    # and before sizing/order submission. Fail closed if durable state is unreadable.
    entry_event_id = str(gate_result.get("durable_event_id") or "").strip()
    entry_event_journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if not entry_event_id and not PAPER_MODE and MODE_LIVE:
        log_execution(f"[ENTRY_EVENT_GUARD] BLOCKED live entry: missing signal candle identity for {symbol}", "ERROR")
        return False
    if entry_event_id and entry_event_journal is None and not PAPER_MODE and MODE_LIVE:
        log_execution(f"[ENTRY_EVENT_GUARD] BLOCKED live entry: durable journal unavailable for {entry_event_id}", "ERROR")
        return False
    if entry_event_id and entry_event_journal is not None:
        try:
            if entry_event_journal.is_event_consumed(entry_event_id):
                log_execution(
                    f"[ENTRY_EVENT_GUARD] BLOCKED duplicate closed-candle signal {entry_event_id}",
                    "WARN", debounce_key=f"entry_event_consumed:{entry_event_id}", debounce_sec=30,
                )
                return False
        except Exception as event_exc:
            log_execution(
                f"[ENTRY_EVENT_GUARD] BLOCKED: cannot verify durable event state "
                f"for {entry_event_id}: {type(event_exc).__name__}: {event_exc}", "ERROR",
            )
            return False
    # A verified RF-opposite close can only be re-entered on a STRICTLY later
    # closed-candle signal. This runs after the consumed-event guard so a reused
    # signal can never satisfy the wait, and before sizing/order submission.
    rf_allowed, rf_reason = _rf_reentry_block_permission(
        symbol, side, gate_result, cross_event_id=cross_event_id,
    )
    if not rf_allowed:
        log_execution(
            f"[RF_REENTRY] BLOCKED {symbol} {side}: {rf_reason}",
            "WARN", debounce_key=f"rf_reentry_wait:{symbol}:{side}:{rf_reason}", debounce_sec=15,
        )
        return False
    gate_result["rf_reentry_signal_timestamp"] = _rf_reentry_signal_timestamp(
        gate_result, cross_event_id)
    log_execution(
        f"[VWAP_ENTRY_GATE] PASSED {symbol} {side} "
        f"EMA9_cross_ts={gate_result.get('ema9_cross_timestamp')} "
        f"EMA50_cross_ts={gate_result.get('ema50_cross_timestamp')} "
        f"EMA9={gate_result.get('ema9')} EMA50={gate_result.get('ema50')} VWAP={gate_result.get('vwap')}",
        "INFO", debounce_key=f"dual_ema_vwap_gate_pass:{symbol}:{side}", debounce_sec=5,
    )
    # Freeze ADX/DI from the exact closed gate frame, not from a later scanner
    # cycle. These values are part of the immutable entry context used by the
    # single profit classifier during management.
    try:
        ctx_di_plus, ctx_di_minus, ctx_adx, _ = get_di_components(gate_frame)
        institutional_entry_ctx["di_plus"] = ctx_di_plus
        institutional_entry_ctx["di_minus"] = ctx_di_minus
        institutional_entry_ctx["adx"] = ctx_adx
    except Exception:
        pass
    institutional_entry_ctx["persistent_entry_context"] = _build_persistent_entry_context(
        institutional_entry_ctx, gate_result, side, symbol, trade_id, atr_val,
    )
    entry_spread_pct = safe_float(institutional_entry_ctx.get("spread_pct"), 0.0) or 0.0
    entry_liquidity_quality = str(institutional_entry_ctx.get("liquidity_quality") or "UNKNOWN")
    entry_orderbook_depth = safe_float(institutional_entry_ctx.get("orderbook_depth_usdt"), 0.0) or 0.0
    try:
        reference_price = float(price)
        structural_stop = float(sl)
        if not math.isfinite(reference_price) or reference_price <= 0:
            raise ValueError("entry price must be finite and positive")
        if not math.isfinite(structural_stop) or structural_stop <= 0:
            raise ValueError("structural stop must be finite and positive")
        try:
            validate_trade_geometry(
                side=side, entry_price=reference_price, stop_price=structural_stop,
                tp1_price=tp1, tp2_price=tp2,
            )
        except ValueError as geometry_exc:
            raise ValueError(
                f"{geometry_exc}; side={side} entry={reference_price:.12g} "
                f"SL={structural_stop:.12g} TP1={float(tp1):.12g} TP2={float(tp2):.12g}"
            ) from geometry_exc
        # Real available balance from BingX Futures ("free" USDT), with source and
        # fetch timestamp so freshness is provable. A stale/unreadable balance must
        # block the entry rather than silently produce a tiny position.
        # Every strategy uses the same authoritative sizing engine on available
        # (free) USDT margin. Sniper V2 additionally verifies wallet total is
        # readable before using free margin; no second allocation is computed.
        if institutional_entry_ctx.get("sniper_v2") is True:
            if institutional_entry_ctx.get("sizing_balance_basis") != "AVAILABLE_FREE":
                raise ValueError("SNIPER_V2_WALLET_TOTAL_BASIS_REQUIRED")
            wallet_snapshot = None if PAPER_MODE else safe_api_call(ex.fetch_balance)
            wallet_balance, free_balance, balance_source = _sniper_v2_wallet_balances(
                wallet_snapshot=wallet_snapshot,
                paper_wallet_balance=paper.get("balance", 0.0),
                paper_mode=PAPER_MODE,
            )
            balance_ts = time.time()
            target_margin = free_balance * 0.20
            if wallet_balance <= 0 or target_margin <= 0:
                raise ValueError("SNIPER_WALLET_TOTAL_INVALID")
            if free_balance <= 0:
                raise ValueError(
                    f"SNIPER_FREE_MARGIN_UNAVAILABLE: wallet_total={wallet_balance:.4f} "
                    f"free={free_balance:.4f}"
                )
            balance = free_balance
            log_execution(
                f"[SNIPER_SIZING_BASIS] wallet_total={wallet_balance:.4f} "
                f"target_margin_20pct={target_margin:.4f} free_margin={free_balance:.4f}",
                "INFO",
            )
        else:
            free_bal, balance_source, balance_ts = get_available_balance_snapshot()
            balance = max(0.0, free_bal * BALANCE_SAFETY_FACTOR)
            if balance <= 0:
                raise ValueError("available USDT balance is zero or unreadable")
        sym = normalize_symbol(symbol)
        try:
            market = ex.market(sym)
            contract_size = safe_float(market.get("contractSize"), 0.0)
            if contract_size <= 0:
                raise ValueError(f"invalid contractSize for {sym}")
        except Exception:
            if not PAPER_MODE:
                raise
            market = {}
            contract_size = 1.0  # PAPER fallback only; LIVE must use exchange market metadata.

        # A caller-supplied label such as SNIPER is not proof of setup quality.
        # Only the unified institutional Watchlist may authorize the strong 30% tier.
        if institutional_entry_ctx.get("sniper_v2") is True:
            if classification != "SNIPER_V2" or institutional_entry_ctx.get("sizing_balance_basis") != "AVAILABLE_FREE":
                raise ValueError("SNIPER_V2_SIZING_POLICY_CONTEXT_INVALID")
            strong_tier_authorized = False
        else:
            strong_tier_authorized = bool(
                institutional_entry_ctx.get("capital_allocation_tier") == "STRONG_30_PERCENT"
                and institutional_entry_ctx.get("sizing_eligible_strong") is True
            )
        sizing_classification = effective_sizing_classification(
            classification, strong_tier_authorized=strong_tier_authorized
        )
        sizing = calculate_position_size(
            classification=sizing_classification,
            available_balance=balance,
            entry_price=reference_price,
            stop_price=structural_stop,
            side=side,
            leverage=LEVERAGE,
            contract_size=contract_size,
            max_trade_risk_pct=MAX_TRADE_RISK_PCT,
            stop_execution_buffer_pct=STOP_EXECUTION_BUFFER_PCT,
        )
        qty = sizing.quantity
        if market:
            qty = float(ex.amount_to_precision(sym, qty))
        else:
            qty = math.floor(qty * 100000000) / 100000000
        if not math.isfinite(qty) or qty <= 0:
            raise ValueError("position quantity rounded to zero")

        actual_notional = qty * contract_size * reference_price
        margin = actual_notional / LEVERAGE
        if institutional_entry_ctx.get("sniper_v2") is True:
            required_free_margin = margin * 1.01
            if free_balance + 1e-9 < required_free_margin:
                raise ValueError(
                    f"SNIPER_FREE_MARGIN_INSUFFICIENT_AFTER_RISK_GATE: "
                    f"wallet_total={wallet_balance:.4f} target_margin={target_margin:.4f} "
                    f"actual_margin={margin:.4f} required_with_buffer={required_free_margin:.4f} "
                    f"free={free_balance:.4f}"
                )
        effective_stop_distance = abs(reference_price - structural_stop) + reference_price * (STOP_EXECUTION_BUFFER_PCT / 100.0)
        estimated_stop_loss = qty * contract_size * effective_stop_distance
        max_stop_loss = sizing.max_risk_usdt
        if estimated_stop_loss > max_stop_loss * 1.000001:
            raise ValueError(
                f"rounded quantity exceeds stop-risk cap: estimated={estimated_stop_loss:.6f} "
                f"limit={max_stop_loss:.6f} USDT"
            )
        target_margin_cap = balance * sizing.allocation_pct
        if margin > target_margin_cap * 1.000001:
            raise ValueError("rounded quantity exceeds configured margin allocation cap")
        min_qty = ((market.get("limits") or {}).get("amount") or {}).get("min") if market else None
        min_cost = ((market.get("limits") or {}).get("cost") or {}).get("min") if market else None
        if min_qty is not None and qty < float(min_qty):
            raise ValueError(f"quantity {qty} is below exchange minimum {min_qty}")
        if min_cost is not None and actual_notional < float(min_cost):
            raise ValueError(f"notional {actual_notional:.6f} is below exchange minimum cost {min_cost}")

        margin_percent = sizing.allocation_pct
        notional = actual_notional
        trade_type_label = "STRONG" if margin_percent >= 0.30 else "STANDARD"
        # Structured, auditable proof that the allocation reached the order:
        # target (from the real available balance) vs the actual exchange quantity.
        min_notional = float(min_cost) if min_cost is not None else 0.0
        expected_margin = actual_notional / LEVERAGE
        log_execution(
            "[POSITION_SIZING_AUDIT]\n"
            f"  Symbol: {sym}\n"
            f"  Direction: {side}\n"
            f"  Sizing Balance Basis: AVAILABLE_FREE\n"
            f"  Sizing Balance: {balance:.4f} USDT\n"
            f"  Balance Source: {balance_source}\n"
            f"  Balance Timestamp: {balance_ts:.3f}\n"
            f"  Allocation Percentage: {margin_percent*100:.0f}%\n"
            f"  Target Margin: {sizing.target_margin:.4f} USDT\n"
            f"  Leverage: {LEVERAGE}x\n"
            f"  Target Notional: {sizing.target_notional:.4f} USDT\n"
            f"  Entry Price: {reference_price:.10f}\n"
            f"  Raw Quantity: {sizing.quantity:.10f}\n"
            f"  Exchange Precision: {ex.amount_to_precision(sym, sizing.quantity) if market else 'n/a'}\n"
            f"  Final Exchange Quantity: {qty:.10f}\n"
            f"  Expected Actual Notional: {actual_notional:.4f} USDT\n"
            f"  Expected Margin: {expected_margin:.4f} USDT\n"
            f"  Sizing Difference: margin_target_minus_actual={sizing.target_margin - expected_margin:+.4f} USDT "
            f"(achieved={sizing.margin_achieved_ratio*100:.1f}%)\n"
            f"  Binding Constraint: {sizing.binding_constraint}\n"
            f"  Minimum Notional: {min_notional:.4f}\n"
            f"  Contract Size: {contract_size:g}\n"
            f"  Risk Check: estimated_stop_loss={estimated_stop_loss:.4f} USDT "
            f"({estimated_stop_loss/sizing.target_margin*100:.3f}% of target margin) "
            f"cap={MAX_TRADE_RISK_PCT:.2f}% of target margin => "
            f"{'PASS' if estimated_stop_loss <= max_stop_loss * 1.000001 else 'REJECT'}\n"
            f"  Native SL Check: pending exchange verification\n"
            f"  Final Status: PASS",
            "INFO"
        )
        if sizing.binding_constraint == "RISK_GATE":
            log_execution(
                f"[SIZING_RISK_GATE] {sym} {side}: target margin {sizing.target_margin:.4f} USDT "
                f"({margin_percent*100:.0f}%) not achievable at this structural stop; "
                f"risk gate capped notional to {sizing.final_notional:.4f} USDT "
                f"(margin {expected_margin:.4f} USDT, {sizing.margin_achieved_ratio*100:.1f}% of target). "
                f"Size reduced by design, not silently.",
                "WARN"
            )
        # Freeze one immutable decision before any order is submitted. No later
        # stage is permitted to resize the quantity; precision only rounds it.
        fee_rate = ESTIMATED_TAKER_FEE_PCT / 100.0
        slippage_rate = ESTIMATED_SLIPPAGE_PCT / 100.0
        reduction_reason = (
            "STRUCTURAL_STOP_RISK_GATE" if sizing.binding_constraint == "RISK_GATE"
            else "NONE_ALLOCATION_CEILING"
        )
        if qty < sizing.quantity - 1e-12:
            reduction_reason += "+EXCHANGE_PRECISION_ROUND_DOWN"
        sizing_decision = {
            **sizing.as_dict(),
            "schema_version": SIZING_DECISION_SCHEMA_VERSION,
            "trade_id": trade_id,
            "symbol": str(symbol),
            "canonical_symbol": sym,
            "side": side,
            "balance_basis": "AVAILABLE_FREE",
            "balance_source": str(balance_source),
            "balance_timestamp": float(balance_ts),
            "available_balance": balance,
            "requested_classification": str(classification or "").upper(),
            "strong_tier_authorized": bool(strong_tier_authorized),
            "leverage": LEVERAGE,
            "target_allocation_pct": margin_percent,
            "entry_price": reference_price,
            "structural_stop": structural_stop,
            "stop_distance": sizing.stop_distance,
            "risk_budget_usdt": sizing.max_risk_usdt,
            "max_qty_before_precision": sizing.quantity,
            "final_exchange_qty": qty,
            "estimated_margin": expected_margin,
            "estimated_sl_loss": estimated_stop_loss,
            "estimated_fees": {
                "taker_fee_pct_each_side": ESTIMATED_TAKER_FEE_PCT,
                "entry_fee_usdt": notional * fee_rate,
                "exit_fee_usdt": notional * fee_rate,
                "round_trip_fee_usdt": 2.0 * notional * fee_rate,
                "source": "CONSERVATIVE_PLANNING_ASSUMPTION_NOT_EXCHANGE_QUOTE",
            },
            "estimated_slippage": {
                "slippage_pct_each_side": ESTIMATED_SLIPPAGE_PCT,
                "round_trip_usdt": 2.0 * notional * slippage_rate,
                "source": "CONSERVATIVE_PLANNING_ASSUMPTION_NOT_ACTUAL_FILL",
            },
            "reduction_reason": reduction_reason,
            "structural_reduction_count": 1 if sizing.binding_constraint == "RISK_GATE" else 0,
            "downstream_reduction_count": 0,
            "cross_event_id": str(cross_event_id or gate_event_id or ""),
            "entry_event_id": str(entry_event_id or ""),
            "profit_target_convention": profit_target_convention(),
        }
        institutional_entry_ctx["sizing_decision"] = dict(sizing_decision)
        # Candidate globals are withheld until the durable slot is reserved. Keep
        # this audit data local until the execution boundary is authorized.
        entry_risk_plan = {}
        initial_mode_report = {}
        management_mode_history = []
        sizing_decision_persistence_failed = False
        # One pre-trade risk record from the frozen sizing decision. This does
        # not size, approve or execute anything; it makes leverage, margin,
        # net return, liquidation distance and R/R explicit before the order.
        try:
            risk_plan = build_entry_risk_plan(
                side=side, entry_price=reference_price, stop_price=structural_stop,
                quantity=qty, target_price=tp1, account_equity=balance,
                contract_size=contract_size, leverage=LEVERAGE,
                sizing_decision=sizing_decision,
            )
            entry_risk_plan = dict(risk_plan)
            institutional_entry_ctx["entry_risk_plan"] = risk_plan
            persistent_ctx = institutional_entry_ctx.get("persistent_entry_context")
            if isinstance(persistent_ctx, dict):
                persistent_ctx["entry_risk_plan"] = risk_plan
            initial_mode_report = build_management_mode_report(
                {"side": side, "entry": reference_price, "tp1_price": tp1, "tp2_price": tp2,
                 "range_target_price": STATE.get("range_target_price")},
                profit_mode=STANDARD_MODE,
                reason="pending_first_closed_candle_management_classification",
                evidence=(),
            )
            management_mode_history = [{
                "at": time.time(), "candle_ts": 0.0,
                "management_mode": initial_mode_report["management_mode"],
                "canonical_profit_mode": initial_mode_report["canonical_profit_mode"],
                "reason": initial_mode_report["classification_reason"],
                "selected_target_gross_roe_pct": initial_mode_report["selected_target_gross_roe_pct"],
            }]
        except Exception as risk_plan_exc:
            log_execution(
                f"[RISK_PLAN] BLOCKED {symbol} {side}: pre-trade risk record unavailable "
                f"{type(risk_plan_exc).__name__}: {risk_plan_exc}", "ERROR",
            )
            return False
        if gate_journal is not None:
            try:
                persisted_decision = gate_journal.persist_sizing_decision(trade_id, sizing_decision)
            except Exception as decision_exc:
                persisted_decision = False
                log_execution(
                    f"[SIZING_DECISION] CRITICAL persist failed for {trade_id}: "
                    f"{type(decision_exc).__name__}: {decision_exc}", "ERROR",
                )
            if not persisted_decision:
                if not PAPER_MODE and MODE_LIVE:
                    log_execution(
                        f"[SIZING_DECISION] BLOCKED LIVE entry {trade_id}: durable decision unavailable", "ERROR",
                    )
                    return False
                sizing_decision_persistence_failed = True
    except Exception as sizing_exc:
        log_execution(f"[SIZING_REJECT] {symbol}: {type(sizing_exc).__name__}: {sizing_exc}", "ERROR")
        return False
    # Freeze the pre-trade audit record once. Both successful fill paths must
    # carry these fields into the open-trade state instead of resetting them.
    frozen_entry_trace = {
        "sizing_decision": dict(sizing_decision),
        "entry_risk_plan": dict(entry_risk_plan),
        "management_mode": str(initial_mode_report.get("management_mode") or "STANDARD"),
        "management_mode_report": dict(initial_mode_report),
        "management_mode_history": list(management_mode_history),
        "institutional_report": dict(institutional_entry_ctx.get("institutional_report") or {}),
        "sizing_decision_persistence_failed": bool(sizing_decision_persistence_failed),
    }
    institutional_ctx = globals().get("_INSTITUTIONAL_ENTRY_CONTEXT")
    if not PAPER_MODE and MODE_LIVE and institutional_ctx:
        df, candle_diagnostic = resolve_entry_closed_candles(symbol, institutional_ctx)
        if df is None:
            record_entry_data_rejection(symbol, side, candle_diagnostic)
            log_execution(
                f"[ENTRY] {candle_diagnostic.get('reason', 'OHLCV_EMPTY')} for authorized "
                f"institutional setup; LIVE blocked",
                "ERROR",
            )
            return False
    else:
        # Preserve the existing PAPER and legacy/manual data path.
        df = get_ohlcv_safe(symbol, 100)

    adx_series = compute_adx(df) if df is not None else None
    adx_val = adx_series.iloc[-1] if adx_series is not None and len(adx_series) > 0 else 20.0

    if not PAPER_MODE and MODE_LIVE:
        institutional_ctx = globals().get("_INSTITUTIONAL_ENTRY_CONTEXT")
        if institutional_ctx:
            setup = str(institutional_ctx.get("setup_type", "")).upper()
            evidence = [str(x).lower() for x in institutional_ctx.get("evidence", [])]
            if setup in ("REVERSAL_ENGINE", "LIQUIDITY_SWEEP_REVERSAL"):
                required = ("prior_liquidity_pool", "sweep", "reclaim", "closed_mss", "directional_trigger")
                valid = all(any(token in ev for ev in evidence) for token in required) and any(ev.startswith("post_mss_retest:") for ev in evidence)
            elif setup == "FAILED_AUCTION_REVERSAL":
                required = ("failed_auction", "outside_acceptance_failed", "reclaim", "closed_mss", "directional_trigger")
                valid = all(any(token in ev for ev in evidence) for token in required) and any(ev.startswith("post_mss_retest:") for ev in evidence)
            elif setup in ("TREND_CONTINUATION_ENGINE", "TREND_CONTINUATION_RETEST"):
                has_bos = any("bos" in ev for ev in evidence)
                has_displacement = any("displacement" in ev for ev in evidence)
                has_retest = any(token in ev for ev in evidence for token in ("retest:", "pullback:", "micro_pullback:"))
                has_trigger = any("post_retest_trigger" in ev for ev in evidence)
                valid = has_bos and has_displacement and has_retest and has_trigger
            else:
                valid = False
            if not valid:
                log_execution(f"[ENTRY] Institutional authorization incomplete for {setup}; LIVE blocked", "ERROR")
                return False
            # ADX, EMA and VWAP remain contextual evidence. They are not universal entry gates.
            log_execution(f"[ENTRY] Institutional setup authorized: {setup}; ADX={adx_val:.1f} treated as context, not a hard cross gate", "INFO")
        else:
            # Legacy/manual entry path keeps its existing stricter safeguards.
            if not (25 <= adx_val <= 38):
                log_execution(f"[ENTRY] Legacy path ADX {adx_val:.1f} not in [25,38] – skipping", "WARN")
                return False
            if df is not None:
                liquidity_ctx = detect_liquidity_context(df, lookback=10)
                if side == "BUY" and liquidity_ctx != "sell_side_taken":
                    log_execution(f"[ENTRY] Legacy BUY requires sell-side liquidity sweep, got {liquidity_ctx}", "WARN")
                    return False
                if side == "SELL" and liquidity_ctx != "buy_side_taken":
                    log_execution(f"[ENTRY] Legacy SELL requires buy-side liquidity sweep, got {liquidity_ctx}", "WARN")
                    return False
            else:
                log_execution("[ENTRY] No DF for legacy liquidity check, skipping", "WARN")
                return False

    plus_di, minus_di, _, _ = get_di_components(df) if df is not None else (None, None, None, None)
    di_dominance = False
    if plus_di is not None and minus_di is not None:
        di_dominance = (side == "BUY" and plus_di > minus_di) or (side == "SELL" and minus_di > plus_di)
    weak_pullback = False
    if df is not None:
        last = df.iloc[-1]
        if side == "BUY":
            if last['close'] < last['open'] and abs(last['close'] - last['open']) < atr_val * 0.3:
                weak_pullback = True
        else:
            if last['close'] > last['open'] and abs(last['close'] - last['open']) < atr_val * 0.3:
                weak_pullback = True
    structure_aligned = False
    struct_shift = detect_structure_shift(df) if df is not None else None
    if side == "BUY" and struct_shift == "bullish_shift":
        structure_aligned = True
    elif side == "SELL" and struct_shift == "bearish_shift":
        structure_aligned = True
    counter_displacement = 0.0
    if df is not None:
        last = df.iloc[-1]
        if side == "SELL" and last['close'] > last['open']:
            body = abs(last['close'] - last['open'])
            if body > atr_val * 0.6:
                counter_displacement = body / atr_val
        elif side == "BUY" and last['close'] < last['open']:
            body = abs(last['close'] - last['open'])
            if body > atr_val * 0.6:
                counter_displacement = body / atr_val
    # Durable six-slot boundary: classify centrally and reserve BEFORE any
    # candidate global is written or any PAPER/LIVE order is submitted.
    if _portfolio_uncertain():
        log_execution(
            f"[PORTFOLIO_BLOCKED] {symbol}: unresolved/recovery exposure; no new order",
            "ERROR", debounce_key=f"portfolio_uncertain:{symbol}", debounce_sec=10,
        )
        return False
    try:
        portfolio_hint = _portfolio_trusted_hint(institutional_entry_ctx)
        reservation = PORTFOLIO_ALLOCATOR.reserve(
            str(sym or normalize_symbol(symbol)), trade_id,
            class_hint=portfolio_hint,
            metadata={"side": side, "reason": str(reason or "")[:200]},
        )
    except Exception as reserve_exc:
        log_execution(
            f"[PORTFOLIO_RESERVE] {trade_id}: {type(reserve_exc).__name__}: {reserve_exc}",
            "ERROR",
        )
        return False
    if not reservation.ok:
        log_execution(
            f"[PORTFOLIO_BLOCKED] {symbol}: no slot available ({reservation.reason})",
            "WARN", debounce_key=f"portfolio_full:{reservation.reason}", debounce_sec=10,
        )
        return False
    # Publish the frozen candidate audit only after the durable slot is held.
    STATE["sizing_decision"] = dict(sizing_decision)
    STATE["entry_risk_plan"] = dict(entry_risk_plan)
    STATE["management_mode"] = str(initial_mode_report.get("management_mode") or "STANDARD")
    STATE["management_mode_report"] = dict(initial_mode_report)
    STATE["management_mode_history"] = list(management_mode_history)
    STATE["sizing_decision_persistence_failed"] = bool(sizing_decision_persistence_failed)
    market_state = {
        "adx": adx_val,
        "regime": MEMORY.get("regime", "UNKNOWN"),
        "di_dominance": di_dominance,
        "weak_pullback": weak_pullback,
        "structure_aligned": structure_aligned,
        "counter_displacement": counter_displacement,
        "trend_health": trend_engine.get_trend_health(df, side) if df is not None else 5
    }
    narrative = {"classification": classification}
    entry_context = {"price": price, "atr": atr_val}
    thesis = _thesis_engine.build_thesis(symbol, side, trade_type, market_state, narrative, entry_context)
    STATE["trade_thesis"] = thesis.__dict__

    regime_class = MarketRegimeClassifier.classify(df) if df is not None else "UNKNOWN"
    di_spread = abs(plus_di - minus_di) if plus_di is not None else 0
    location_quality = "mid"
    initial_conf = ConfidenceEngine.calculate_initial_confidence(score, narrative.get("narrative_score", 0), regime_class, adx_val, di_spread, location_quality)

    if df is not None:
        smart_money = SmartMoneyEngine.analyze_smart_money(df)
        momentum = MomentumFlowEngine.analyze_momentum_flow(df)
        dominance_weight = 0.7 if smart_money["smart_money_dominant"] else 0.3
        initial_conf += (dominance_weight - 0.5) * 12
        if momentum["trend_expansion"]:
            initial_conf += 8
        if momentum["momentum_decay"]:
            initial_conf -= 12
        dist_risk = smart_money["distribution_risk"] / 100.0
        initial_conf -= dist_risk * 15
        if smart_money["retail_euphoria"]:
            initial_conf -= 10
        continuation_strength = momentum.get("continuation_strength", 50)
        initial_conf = ConfidenceEngine.apply_institutional_modifiers(initial_conf, smart_money, momentum, continuation_strength)
        initial_conf = max(0, min(95, initial_conf))

    STATE["current_confidence"] = initial_conf
    STATE["market_regime"] = regime_class

    if PAPER_MODE:
        paper["position"] = {"side": side, "entry": price, "qty": qty, "remaining_qty": qty}
        STATE.update({
            "open": True, "side": side, "entry": price, "qty": qty, "initial_qty": qty,
            "exchange_qty": qty, "remaining_qty": qty,
            "sl": sl, "current_symbol": symbol, "tp1_done": False, "trail_activated": False,
            "peak": 0.0, "atr": atr_val, "entry_time": time.time(), "entry_reasons": [reason],
            "entry_context_snapshot": dict(institutional_entry_ctx.get("persistent_entry_context") or {}),
             "trade_score": score, "partial_closed": False, "tp1_price": tp1, "tp2_price": tp2,
            "margin_allocation_pct": margin_percent, "margin_allocated": margin,
            "position_notional": notional, "position_leverage": LEVERAGE,
             "estimated_stop_loss_usdt": estimated_stop_loss,
            "max_trade_risk_pct": MAX_TRADE_RISK_PCT,
            "risk_budget_basis": "TARGET_MARGIN",
            "risk_budget_usdt": sizing.max_risk_usdt,
            "trade_type": trade_type, "entry_type": entry_type, "be_done": False,
            "zone_event": "INSIDE_ZONE" if safe_float(institutional_entry_ctx.get("entry_zone_low"), 0.0) > 0 else "INSUFFICIENT_DATA",
            "zone_event_last_candle_ts": 0.0,
            "failed_breakout_streak": 0,
            "failed_breakout_last_candle_ts": 0.0,
            "failed_breakout_thesis_credit_applied": False,
            "entry_zone_low": safe_float(institutional_entry_ctx.get("entry_zone_low"), 0.0),
            "entry_zone_high": safe_float(institutional_entry_ctx.get("entry_zone_high"), 0.0),
            "support_resistance": dict(institutional_entry_ctx.get("support_resistance") or {"status": "UNKNOWN", "supports": [], "resistances": []}),
            "entry_behavior_confidence": safe_float(institutional_entry_ctx.get("entry_behavior_confidence"), 0.0),
            "entry_behavior_reasons": list(institutional_entry_ctx.get("entry_behavior_reasons") or []),
            "trading_mode": str(institutional_entry_ctx.get("trading_mode") or (institutional_entry_ctx.get("market_intelligence") or {}).get("mode") or "UNCLASSIFIED"),
            "entry_behavior": str(institutional_entry_ctx.get("entry_behavior") or (institutional_entry_ctx.get("market_intelligence") or {}).get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
            "market_intelligence": {
                **(institutional_entry_ctx.get("market_intelligence") or {}),
                "zone_side": side,
                "failed_breakout_streak": 0,
                "failed_breakout_thesis_credit_applied": False,
            },
            "entry_order_block": dict(institutional_entry_ctx.get("order_block") or {}),
            "entry_fvg": dict(institutional_entry_ctx.get("fvg") or {}),
            "entry_liquidity": dict(institutional_entry_ctx.get("liquidity") or {}),
            "entry_golden_zone": dict(institutional_entry_ctx.get("golden_zone") or {}),
            "tp1_hit": False, "tp2_hit": False, "trail_stop": 0.0,
            "smart_tightened": False, "smart_partial_done": False, "smart_exit_triggered": False,
            "roe_pct": 0.0, "mark_price": price, "initial_margin": margin,
            "narrative_classification": STATE.get("narrative_classification", ""),
            "narrative_confidence": STATE.get("narrative_confidence", 0.0),
            "confidence_level": STATE.get("confidence_level", ""),
            "trade_thesis": thesis.__dict__,
            "current_confidence": initial_conf,
            "market_regime": regime_class,
            "adx_live": adx_val,
            "di_plus_live": plus_di if plus_di else 0,
            "di_minus_live": minus_di if minus_di else 0,
            "trade_personality": "NEUTRAL",
            "institutional_flow": "NEUTRAL",
            "synthetic_sl": sl,
            "native_sl_original_price": sl,
            "native_sl_original_qty": qty,
            "native_sl_last_verified_price": sl,
            "entry_spread_pct": entry_spread_pct,
            "liquidity_quality": entry_liquidity_quality,
            "orderbook_depth_usdt": entry_orderbook_depth,
            "synthetic_tp1": tp1,
            "trade_id": trade_id,
            "profit_mode": None, "profit_mode_reason": "", "profit_mode_evidence": [],
            "profit_mode_candidate": None, "profit_mode_candidate_count": 0, "profit_mode_last_candle_ts": 0.0,
            **frozen_entry_trace,
            "management_mode_last_change_ts": 0.0,
            "range_target_price": 0.0, "range_target_move_pct": 0.0, "range_target_reason": "",
            "opposing_liquidity_status": "UNKNOWN", "opposing_liquidity_level": 0.0,
            "opposing_liquidity_distance_pct": 0.0, "opposing_liquidity_timestamp": 0.0,
            "scalp_target_selected_pct": 0.0, "scalp_target_price": 0.0, "scalp_target_tp1_price": 0.0, "scalp_target_status": "UNKNOWN", "scalp_target_selection_reason": "",

            "native_sl_policy": "FIXED_FROM_ENTRY", "be_local": False,
    "native_sl_last_verified_price": 0.0, "native_sl_last_requested_price": 0.0,
    "native_sl_last_update_ts": 0.0, "native_sl_last_result": "NONE", "native_sl_last_reason": "",
    "native_sl_original_qty": 0.0, "native_sl_local_target": 0.0,
            "tp1_filled_qty": 0.0, "tp2_filled_qty": 0.0, "tp1_execution_price": None,
            "execution_unresolved": False, "close_reconciliation_required": False,
            "close_verified": False, "last_close_verified": False, "last_close_order_id": None,
            "last_close_reason": None, "last_close_fill_price": None, "last_close_filled_qty": 0.0,
            "last_close_realized_pnl": 0.0, "last_close_pnl_source": None, "last_close_pnl_complete": True,
            "bot_close_order_ids": [], "pending_close_order_id": None, "pending_close_reason": None,
            "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
            "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
            "structural_invalidation": False, "thesis_failure_reasons": [],
            "native_sl_status": "PAPER_SIMULATED" if PAPER_MODE else "ATTACHED_ON_ENTRY_UNVERIFIED",
            "max_price": price,
            "min_price": price,
            "peak_roe": 0.0,
            "peak_drawdown_roe": 0.0,
            "ema_profit_state": "INSUFFICIENT_DATA"
        })
        TRADE_STATE.update({
            "in_position": True, "symbol": symbol, "side": side, "entry": price, "qty": qty,
            "tp1_hit": False, "trail_on": False, "last_update_ts": time.time()
        })
        _live_manager.start_trade(symbol, side, price, qty, sl, tp1, tp2)
        if not _portfolio_adopt_candidate(trade_id, sym, portfolio_hint):
            _portfolio_release_reservation(trade_id, sym, "ADOPT_FAILED_NO_EXPOSURE")
            return False
        update_position_dashboard(symbol, side, price, qty)
        log_execution(f"PAPER {entry_type} {side} {qty:.6f} @ {price} | {trade_type_label} | {reason}", "SUCCESS")
        tg_entry(side, symbol, price, sl, tp1, score, reason, entry_type, tp2=tp2, qty=qty, leverage=LEVERAGE, trade_id=trade_id)
        _recompute_portfolio_globals()
        return True

    sym = normalize_symbol(symbol)
    if not PAPER_MODE and not market:
        log_execution(f"[SIZING_REJECT] LIVE market metadata unavailable for {sym}", "ERROR")
        _portfolio_release_reservation(trade_id, sym, "PRE_ORDER_METADATA_MISSING")
        return False
    pending_management_state = {
        "trade_id": trade_id, "current_symbol": symbol, "side": side,
        "synthetic_sl": sl, "sl": sl, "native_sl_original_price": sl,
        "tp1_price": tp1, "tp2_price": tp2, "synthetic_tp1": tp1,
        "initial_qty": qty, "qty": qty, "remaining_qty": qty,
        "trade_type": trade_type, "entry_type": entry_type,
        "margin_allocation_pct": margin_percent, "position_leverage": LEVERAGE,
        "max_trade_risk_pct": MAX_TRADE_RISK_PCT,
        "entry_time": time.time(), "tp1_hit": False, "tp2_hit": False,
        "trail_activated": False, "be_done": False,
    }
    if daily_loss_cooldown_active():
        log_execution(f"[ENTRY_BLOCKED] cooldown activated during validation; order not submitted for {symbol}", "WARN")
        _portfolio_release_reservation(trade_id, sym, "PRE_ORDER_COOLDOWN")
        return False
    order = open_position(side, qty, symbol, stop_loss=sl, trade_id=trade_id,
                          management_state=pending_management_state)
    if order:
        actual_filled_qty = safe_float(order.get("filled"), 0.0)
        actual_entry_price = safe_float(order.get("average"), 0.0)
        if actual_filled_qty <= 0 or actual_entry_price <= 0:
            STATE.update({"open": True, "current_symbol": symbol, "side": side,
                          "trade_id": trade_id, "qty": qty, "recovery_hold": True,
                          "execution_unresolved": True})
            globals()["RECOVERY_BLOCK_ENTRIES"] = True
            log_execution("[RECOVERY_HOLD] Exchange acknowledged exposure but actual fill quantity/price is unavailable", "ERROR")
            _portfolio_adopt_candidate(trade_id, sym, portfolio_hint)
            _recompute_portfolio_globals()
            return False
        qty = actual_filled_qty
        price = actual_entry_price
        margin = (qty * contract_size * price) / LEVERAGE
        notional = qty * contract_size * price
        STATE.update({
            "open": True, "side": side, "entry": price, "qty": qty, "initial_qty": qty,
            "exchange_qty": qty, "remaining_qty": qty,
            "sl": sl, "current_symbol": symbol, "tp1_done": False, "trail_activated": False,
            "peak": 0.0, "atr": atr_val, "entry_time": time.time(), "entry_reasons": [reason],
            "entry_context_snapshot": dict(institutional_entry_ctx.get("persistent_entry_context") or {}),
             "trade_score": score, "partial_closed": False, "tp1_price": tp1, "tp2_price": tp2,
            "margin_allocation_pct": margin_percent, "margin_allocated": margin,
            "position_notional": notional, "position_leverage": LEVERAGE,
             "estimated_stop_loss_usdt": estimated_stop_loss,
            "max_trade_risk_pct": MAX_TRADE_RISK_PCT,
            "risk_budget_basis": "TARGET_MARGIN",
            "risk_budget_usdt": sizing.max_risk_usdt,
            "trade_type": trade_type, "entry_type": entry_type, "be_done": False,
            "zone_event": "INSIDE_ZONE" if safe_float(institutional_entry_ctx.get("entry_zone_low"), 0.0) > 0 else "INSUFFICIENT_DATA",
            "zone_event_last_candle_ts": 0.0,
            "failed_breakout_streak": 0,
            "failed_breakout_last_candle_ts": 0.0,
            "failed_breakout_thesis_credit_applied": False,
            "entry_zone_low": safe_float(institutional_entry_ctx.get("entry_zone_low"), 0.0),
            "entry_zone_high": safe_float(institutional_entry_ctx.get("entry_zone_high"), 0.0),
            "support_resistance": dict(institutional_entry_ctx.get("support_resistance") or {"status": "UNKNOWN", "supports": [], "resistances": []}),
            "entry_behavior_confidence": safe_float(institutional_entry_ctx.get("entry_behavior_confidence"), 0.0),
            "entry_behavior_reasons": list(institutional_entry_ctx.get("entry_behavior_reasons") or []),
            "trading_mode": str(institutional_entry_ctx.get("trading_mode") or (institutional_entry_ctx.get("market_intelligence") or {}).get("mode") or "UNCLASSIFIED"),
            "entry_behavior": str(institutional_entry_ctx.get("entry_behavior") or (institutional_entry_ctx.get("market_intelligence") or {}).get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
            "market_intelligence": {
                **(institutional_entry_ctx.get("market_intelligence") or {}),
                "zone_side": side,
                "failed_breakout_streak": 0,
                "failed_breakout_thesis_credit_applied": False,
            },
            "entry_order_block": dict(institutional_entry_ctx.get("order_block") or {}),
            "entry_fvg": dict(institutional_entry_ctx.get("fvg") or {}),
            "entry_liquidity": dict(institutional_entry_ctx.get("liquidity") or {}),
            "entry_golden_zone": dict(institutional_entry_ctx.get("golden_zone") or {}),
            "tp1_hit": False, "tp2_hit": False, "trail_stop": 0.0,
            "smart_tightened": False, "smart_partial_done": False, "smart_exit_triggered": False,
            "roe_pct": 0.0, "mark_price": price, "initial_margin": margin,
            "narrative_classification": STATE.get("narrative_classification", ""),
            "narrative_confidence": STATE.get("narrative_confidence", 0.0),
            "confidence_level": STATE.get("confidence_level", ""),
            "trade_thesis": thesis.__dict__,
            "current_confidence": initial_conf,
            "market_regime": regime_class,
            "adx_live": adx_val,
            "di_plus_live": plus_di if plus_di else 0,
            "di_minus_live": minus_di if minus_di else 0,
            "trade_personality": "NEUTRAL",
            "institutional_flow": "NEUTRAL",
            "synthetic_sl": sl,
            "native_sl_original_price": sl,
            "native_sl_original_qty": qty,
            "native_sl_last_verified_price": sl,
            "entry_spread_pct": entry_spread_pct,
            "liquidity_quality": entry_liquidity_quality,
            "orderbook_depth_usdt": entry_orderbook_depth,
            "synthetic_tp1": tp1,
            "trade_id": trade_id,
            "position_mode": order.get("positionMode", "PAPER"),
            "position_side": order.get("positionSide"),
            "entry_client_order_id": order.get("clientOrderId"),
            "entry_exchange_order_id": order.get("id"),
            "entry_execution_state": "FILLED",
            "native_sl_order_id": order.get("native_sl_order_id"),
            "native_sl_verified": bool(order.get("native_sl_verified", PAPER_MODE)),
            "recovery_hold": bool(order.get("protection_unverified", False)),
            "native_sl_status": "VERIFIED" if order.get("native_sl_verified") else ("PAPER_SIMULATED" if PAPER_MODE else "UNVERIFIED"),
            "execution_unresolved": bool(order.get("protection_unverified", False)),
            "profit_mode": None, "profit_mode_reason": "", "profit_mode_evidence": [],
            "profit_mode_candidate": None, "profit_mode_candidate_count": 0, "profit_mode_last_candle_ts": 0.0,
            **frozen_entry_trace,
            "management_mode_last_change_ts": 0.0,
            "range_target_price": 0.0, "range_target_move_pct": 0.0, "range_target_reason": "",
            "opposing_liquidity_status": "UNKNOWN", "opposing_liquidity_level": 0.0,
            "opposing_liquidity_distance_pct": 0.0, "opposing_liquidity_timestamp": 0.0,
            "scalp_target_selected_pct": 0.0, "scalp_target_price": 0.0, "scalp_target_tp1_price": 0.0,
            "scalp_target_status": "UNKNOWN", "scalp_target_selection_reason": "",
            "native_sl_policy": "FIXED_FROM_ENTRY", "be_local": False,
    "native_sl_last_verified_price": 0.0, "native_sl_last_requested_price": 0.0,
    "native_sl_last_update_ts": 0.0, "native_sl_last_result": "NONE", "native_sl_last_reason": "",
    "native_sl_original_qty": 0.0, "native_sl_local_target": 0.0,
            "tp1_filled_qty": 0.0, "tp2_filled_qty": 0.0, "tp1_execution_price": None,
            "close_reconciliation_required": False,
            "close_verified": False, "last_close_verified": False, "last_close_order_id": None,
            "last_close_reason": None, "last_close_fill_price": None, "last_close_filled_qty": 0.0,
            "last_close_realized_pnl": 0.0, "last_close_pnl_source": None, "last_close_pnl_complete": True,
            "bot_close_order_ids": [], "pending_close_order_id": None, "pending_close_reason": None,
            "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
            "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
            "structural_invalidation": False, "thesis_failure_reasons": [],
            "native_sl_status": "PAPER_SIMULATED" if PAPER_MODE else "ATTACHED_ON_ENTRY_UNVERIFIED",
            "max_price": price,
            "min_price": price,
            "peak_roe": 0.0,
            "peak_drawdown_roe": 0.0,
            "ema_profit_state": "INSUFFICIENT_DATA"
        })
        TRADE_STATE.update({
            "in_position": True, "symbol": symbol, "side": side, "entry": price, "qty": qty,
            "tp1_hit": False, "trail_on": False, "last_update_ts": time.time()
        })
        _live_manager.start_trade(symbol, side, price, qty, sl, tp1, tp2)
        # TradeExecutionService already placed and read back the stop. Do not
        # perform a second API verification that can turn a verified stop into a
        # transient false-negative. Re-query only when that service did not prove it.
        if STATE.get("native_sl_verified") and STATE.get("native_sl_order_id"):
            native_status, native_order_id = "VERIFIED", STATE.get("native_sl_order_id")
        else:
            native_status, native_order_id = verify_native_stop_loss(
                symbol, side, sl, qty,
                position_side=(STATE.get("position_side") or order.get("positionSide")))
        STATE["native_sl_status"] = native_status
        STATE["native_sl_original_price"] = float(sl)
        if native_order_id:
            STATE["native_sl_order_id"] = native_order_id
        if native_status != "VERIFIED":
            STATE["recovery_hold"] = True
            STATE["execution_unresolved"] = True
            globals()["RECOVERY_BLOCK_ENTRIES"] = True
            MEMORY["recovery_status"] = f"NATIVE_SL_{native_status}"
            log_execution(f"[LIVE_SAFETY] Native SL not verified ({native_status}) for {symbol}; new entries blocked. Validate in BingX immediately.", "ERROR")
            tg_error(f"Native stop loss not verified for {symbol}; check BingX position protection", "NATIVE_SL")
        else:
            STATE["recovery_hold"] = False
            STATE["execution_unresolved"] = False
            STATE["native_sl_verified"] = True
            globals()["RECOVERY_BLOCK_ENTRIES"] = False
            MEMORY["recovery_status"] = "NATIVE_SL_VERIFIED"
            log_execution(f"[LIVE_SAFETY] Native STOP_MARKET verified for {symbol} qty={qty}", "SUCCESS")
        # Exposure is real here; an adoption failure must never release the slot.
        _portfolio_adopt_candidate(trade_id, sym, portfolio_hint)
        _recompute_portfolio_globals()
        _persist_trade_journal_start(setup_type=trade_type)
        _persist_trade_journal_snapshot(force=True)
        update_position_dashboard(symbol, side, price, qty)
        log_execution(f"LIVE {entry_type} {side} {qty:.6f} @ {price} | {trade_type_label} | {reason}", "SUCCESS")
        tg_entry(side, symbol, price, sl, tp1, score, reason, entry_type, tp2=tp2, qty=qty, leverage=LEVERAGE, trade_id=trade_id)
        time.sleep(1)
        sync_position_state(symbol)
        _recompute_portfolio_globals()
        return True
    else:
        if (
            STATE.get("execution_unresolved") or STATE.get("recovery_hold")
            or STATE.get("close_reconciliation_required") or _portfolio_uncertain()
        ):
            _portfolio_adopt_candidate(trade_id, sym, portfolio_hint)
            _recompute_portfolio_globals()
        else:
            _portfolio_release_reservation(trade_id, sym, "ORDER_REJECTED_NO_EXPOSURE")
        return False

def dynamic_spread_tolerance(symbol):
    df = get_ohlcv_safe(symbol, 50)
    if df is None:
        return MAX_SPREAD_PERCENT_DEFAULT
    atr = compute_atr(df).iloc[-1]
    price = df['close'].iloc[-1]
    atr_pct = (atr/price)*100 if price>0 else 0.5
    if atr_pct > 2.0:
        return MAX_SPREAD_PERCENT_VOLATILE
    return MAX_SPREAD_PERCENT_DEFAULT

# ========== RF SCANNER ==========
_SCANNER_FRAME_CACHE = {}
_SCANNER_FRAME_CACHE_LOCK = threading.RLock()
_SCANNER_FRAME_CACHE_TTL = _env_float_range("SCANNER_FRAME_CACHE_TTL", 90.0, 15.0, 180.0)

def _get_scanner_frame(symbol, limit=150):
    """Use the same canonical single-flight snapshot as live management."""
    frame, _source = get_ohlcv_safe_meta(symbol, limit)
    return frame


# ===== CONFIRMED CLOSED-CANDLE EMA9/SESSION-VWAP CROSS EVENTS =====
# Scanner/RF/V2/professional/radar rows remain discovery hints. Only the strict
# execution gate below can create these durable, unique Watchlist opportunities.
CONFIRMED_CROSS_EVENT_SOURCE = "CLOSED_CANDLE_EMA9_VWAP_CROSS"
CONFIRMED_CROSS_EVENT_TTL_SEC = 4.0 * 3600.0
CROSS_LIFECYCLE_SCHEMA = "CROSS_REJECTION_V1"
CROSS_WATCHLIST_LIFECYCLE = "CROSS_DETECTED"
ACTIVE_WATCHLIST_LIFECYCLE = "ACTIVE_WATCHLIST"
try:
    CROSS_REJECTION_MAX_CHASE_ATR = max(0.25, min(3.0, float(os.getenv("CROSS_REJECTION_MAX_CHASE_ATR", "1.5"))))
except (TypeError, ValueError, OverflowError):
    CROSS_REJECTION_MAX_CHASE_ATR = 1.5
SIZING_DECISION_SCHEMA_VERSION = 1
# Conservative planning assumptions for audit records only. They never resize a
# position and are never presented as measured exchange fills or net PnL.
ESTIMATED_TAKER_FEE_PCT = 0.05
ESTIMATED_SLIPPAGE_PCT = 0.02


def _confirmed_cross_event_id(symbol, timeframe, gate_result):
    raw_signal_id = str((gate_result or {}).get("signal_candle_id") or "").strip()
    if not raw_signal_id:
        return ""
    try:
        event_symbol = normalize_symbol(symbol)
    except Exception:
        event_symbol = str(symbol or "").strip().upper()
    return f"{event_symbol}:{timeframe}:{raw_signal_id}"


def _advance_cross_event_lifecycles(rows, progress_callback=None, max_items=None):
    """Advance a rotating bounded batch of active cross events on closed candles."""
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    evaluator = globals().get("_evaluate_post_cross_rejection_breakout")
    if journal is None or not callable(evaluator):
        return 0
    eligible = [
        row for row in list(rows or [])
        if isinstance(row, dict)
        and isinstance(row.get("metadata"), dict)
        and str(row["metadata"].get("lifecycle_schema") or "") == CROSS_LIFECYCLE_SCHEMA
    ]
    if not eligible:
        return 0
    if max_items is not None:
        try:
            batch_limit = max(1, min(len(eligible), int(max_items)))
        except (TypeError, ValueError, OverflowError):
            batch_limit = len(eligible)
        cursor = int(MEMORY.get("_cross_event_lifecycle_cursor", 0) or 0) % len(eligible)
        selected = [eligible[(cursor + offset) % len(eligible)] for offset in range(batch_limit)]
        MEMORY["_cross_event_lifecycle_cursor"] = (cursor + batch_limit) % len(eligible)
    else:
        selected = eligible
    advanced = 0
    for row in selected:
        symbol = row.get("exchange_symbol") or row.get("symbol")
        try:
            if callable(progress_callback):
                progress_callback("CROSS_EVENT_LIFECYCLE_FETCH", symbol)
            frame = _get_scanner_frame(symbol)
            lifecycle = evaluator(
                frame, str(row.get("side") or ""), row,
                ema_fn=ema, timeframe_seconds=_roro_timeframe_seconds(row.get("timeframe") or INTERVAL),
                now_ms=int(time.time() * 1000), max_chase_atr=CROSS_REJECTION_MAX_CHASE_ATR,
            )
            breakout_confirmed = (
                str(lifecycle.get("phase") or "").upper() == "BREAKOUT_CONFIRMED"
                and lifecycle.get("entry_ready") is True
            )
            # Every breakout remains a candidate until the unified Watchlist
            # has selected and validated a structural zone for this exact event.
            lifecycle["breakout_candidate"] = bool(breakout_confirmed)
            lifecycle["entry_ready"] = False
            lifecycle["structural_zone_gate"] = {
                "allowed": False,
                "reason": "watchlist_structural_zone_not_yet_authorized",
                "required": True,
                "authority": "UNIFIED_WATCHLIST_ENTRY_ZONE",
            }
            if breakout_confirmed:
                event_id = str(row.get("event_id") or "")
                side = str(row.get("side") or "").upper()
                watchlist = MEMORY.get("watchlist") if isinstance(MEMORY.get("watchlist"), dict) else {}
                structural_row = next((
                    item for item in watchlist.values()
                    if isinstance(item, dict)
                    and str(item.get("cross_event_id") or "") == event_id
                    and str(item.get("side") or "").upper() == side
                ), None)
                if structural_row is None:
                    lifecycle["structural_zone_gate"]["reason"] = "missing_matching_watchlist_event"
                else:
                    selected_zone = structural_row.get("entry_zone")
                    try:
                        latest_price = float(frame["close"].iloc[-1])
                        latest_ts = int(lifecycle.get("latest_closed_timestamp") or 0)
                        atr_value = float(lifecycle.get("atr") or 0.0)
                        tf_ms = int(_roro_timeframe_seconds(row.get("timeframe") or INTERVAL) * 1000)
                        zone_ok, zone_reason = _validate_selected_entry_zone(
                            selected_zone, side, price=latest_price, atr=atr_value,
                            latest_timestamp=latest_ts, timeframe_ms=tf_ms,
                        )
                    except Exception as zone_exc:
                        zone_ok, zone_reason = False, f"structural_zone_validation_error:{type(zone_exc).__name__}"
                    if zone_ok:
                        lifecycle["selected_entry_zone"] = dict(selected_zone)
                        lifecycle["entry_behavior"] = str(
                            structural_row.get("entry_behavior")
                            or (structural_row.get("market_intelligence") or {}).get("entry_behavior")
                            or "WAIT_FOR_STRUCTURE"
                        ).upper()
                        lifecycle["trading_mode"] = str(
                            structural_row.get("trading_mode")
                            or (structural_row.get("market_intelligence") or {}).get("mode")
                            or "UNCLASSIFIED"
                        ).upper()
                        lifecycle["setup_type"] = str(
                            structural_row.get("decision_setup_type")
                            or structural_row.get("setup_type")
                            or "UNCLASSIFIED"
                        ).upper()
                        lifecycle["structural_zone_gate"] = {
                            "allowed": True, "reason": zone_reason,
                            "required": True, "authority": "UNIFIED_WATCHLIST_ENTRY_ZONE",
                        }
                        lifecycle["entry_ready"] = True
                    else:
                        lifecycle["structural_zone_gate"]["reason"] = zone_reason
                        lifecycle["reason"] = f"structural_zone_not_authorized:{zone_reason}"
            if journal.update_cross_event_lifecycle(row.get("event_id"), lifecycle):
                advanced += 1
        except Exception as exc:
            log_execution(
                f"[CROSS_EVENT] lifecycle advance failed for {row.get('event_id')}: "
                f"{type(exc).__name__}: {exc}", "ERROR",
                debounce_key=f"cross_lifecycle:{row.get('event_id')}", debounce_sec=10,
            )
        finally:
            if callable(progress_callback):
                progress_callback("CROSS_EVENT_LIFECYCLE_COMPLETE", symbol)
    return advanced


def _project_confirmed_cross_row(row):
    """Add explicit cross state to an already durable journal row (read-only)."""
    return _project_cross_state(row, now_ts=time.time())


def _sync_confirmed_cross_events(progress_callback=None, lifecycle_max_items=None, memory_target=None):
    """Reload active cross events; optionally advance a bounded rotating lifecycle batch."""
    target_memory = memory_target if isinstance(memory_target, dict) else MEMORY
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    rows = []
    if journal is not None:
        try:
            journal.expire_cross_events()
            rows = journal.list_cross_events(statuses=("ACTIVE",))
            if _advance_cross_event_lifecycles(
                rows, progress_callback=progress_callback, max_items=lifecycle_max_items
            ):
                rows = journal.list_cross_events(statuses=("ACTIVE",))
        except Exception as exc:
            log_execution(f"[CROSS_EVENT] sync failed: {type(exc).__name__}: {exc}", "ERROR")
    lifecycle_projection = {}
    for row in rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if str(metadata.get("lifecycle_schema") or "") == CROSS_LIFECYCLE_SCHEMA:
            lifecycle_projection[str(row.get("event_id") or "")] = {
                "cross_event_phase": str(metadata.get("phase") or "WAITING_FOR_PULLBACK"),
                "cross_event_lifecycle_schema": CROSS_LIFECYCLE_SCHEMA,
                "cross_event_lifecycle_status": str(metadata.get("lifecycle_status") or CROSS_WATCHLIST_LIFECYCLE),
                "cross_event_entry_ready": bool(metadata.get("entry_ready") is True),
                "cross_event_rejection_high": metadata.get("rejection_high"),
                "cross_event_rejection_low": metadata.get("rejection_low"),
                "cross_event_rejection_timestamp": metadata.get("rejection_candle_timestamp"),
                "cross_event_breakout_timestamp": metadata.get("breakout_candle_timestamp"),
                "cross_event_breakout_close": metadata.get("breakout_close"),
                "cross_event_chase_atr": metadata.get("chase_atr"),
            }
    target_memory["confirmed_cross_events"] = []
    for row in rows:
        if not (row.get("event_id") and row.get("exchange_symbol")):
            continue
        cross_context = _project_confirmed_cross_row(row)
        target_memory["confirmed_cross_events"].append({
            "symbol": str(row.get("symbol") or ""),
            "exchange_symbol": str(row.get("exchange_symbol") or row.get("symbol") or ""),
            "side": str(row.get("side") or "").upper(),
            "cross_event_id": str(row.get("event_id") or ""),
            "cross_event_source": str(row.get("source") or CONFIRMED_CROSS_EVENT_SOURCE),
            "cross_event_direction": str(row.get("direction") or "").upper(),
            "cross_event_timestamp": int(row.get("signal_candle_timestamp") or 0),
            "cross_event_status": str(row.get("status") or "ACTIVE").upper(),
            "cross_event_timeframe": str(row.get("timeframe") or INTERVAL),
            "cross_event_detected_ts": float(row.get("detected_ts") or 0.0),
            "cross_event_expires_ts": float(row.get("expires_ts") or 0.0),
            **lifecycle_projection.get(str(row.get("event_id") or ""), {}),
            "cross_state": cross_context["cross_state"],
            "cross_context": cross_context,
            "discovery_source": "confirmed_cross_event",
            "role": "CONFIRMED_CROSS_EVENT",
            "score": 100.0,
            "side_hint": str(row.get("side") or "").upper(),
            "sources": ["confirmed_cross_event"],
        })
    target_memory["confirmed_cross_events_last_sync"] = time.time()
    return rows


def _mark_cross_events_watchlist_delivered(event_ids, delivered_at=None):
    """Record durable Watchlist handoff without creating another event or store."""
    delivered_at = float(delivered_at or time.time())
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    changed = []
    for event_id in list(event_ids or []):
        event_id = str(event_id or "")
        if not event_id:
            continue
        persisted = False
        if journal is not None and hasattr(journal, "mark_cross_event_lifecycle_status"):
            try:
                persisted = bool(journal.mark_cross_event_lifecycle_status(
                    event_id, ACTIVE_WATCHLIST_LIFECYCLE, watchlist_delivered_at=delivered_at,
                ))
            except Exception as exc:
                log_execution(
                    f"[CROSS_EVENT] lifecycle delivery mark failed for {event_id}: "
                    f"{type(exc).__name__}: {exc}", "ERROR",
                    debounce_key=f"cross_delivery:{event_id}", debounce_sec=10,
                )
        for row in MEMORY.get("confirmed_cross_events", []):
            if str(row.get("cross_event_id") or "") != event_id:
                continue
            row["cross_event_lifecycle_status"] = ACTIVE_WATCHLIST_LIFECYCLE
            row["cross_event_watchlist_delivered_at"] = delivered_at
            metadata = row.get("metadata")
            if isinstance(metadata, dict):
                metadata["lifecycle_status"] = ACTIVE_WATCHLIST_LIFECYCLE
            cross_context = row.get("cross_context")
            if isinstance(cross_context, dict):
                context_metadata = cross_context.get("metadata")
                if isinstance(context_metadata, dict):
                    context_metadata["lifecycle_status"] = ACTIVE_WATCHLIST_LIFECYCLE
        for row in MEMORY.get("watchlist", {}).values():
            if isinstance(row, dict) and str(row.get("cross_event_id") or "") == event_id:
                row["cross_event_lifecycle_status"] = ACTIVE_WATCHLIST_LIFECYCLE
        changed.append(event_id)
    return changed


def _invalidate_superseded_cross_events(symbol, direction, journal, keep_event_id=None):
    key = roro_canonical_symbol(symbol)
    keep = str(keep_event_id or "").strip()
    for event in journal.list_cross_events(statuses=("ACTIVE",)):
        event_id = str(event.get("event_id") or "")
        if str(event.get("symbol") or "") != key or event_id == keep:
            continue
        reason = "opposite_confirmed_cross" if str(event.get("direction") or "").upper() != str(direction or "").upper() else "superseded_confirmed_cross"
        try:
            journal.invalidate_cross_event(event_id, reason)
        except Exception as exc:
            log_execution(
                f"[CROSS_EVENT] invalidate failed for {event.get('event_id')}: "
                f"{type(exc).__name__}: {exc}", "ERROR",
            )


def _detect_confirmed_cross_events(symbols):
    """Create durable cross events only from the strict closed-candle gate."""
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None:
        MEMORY["confirmed_cross_events"] = []
        return 0
    created = 0
    now_ms = int(time.time() * 1000)
    timeframe_seconds = _roro_timeframe_seconds(INTERVAL)
    for symbol in list(symbols or [])[:SCANNER_UNIVERSE_MAX]:
        try:
            frame = _get_scanner_frame(symbol)
        except Exception:
            continue
        if frame is None:
            continue
        for side in ("BUY", "SELL"):
            try:
                gate = _validate_ema9_vwap_entry_gate(
                    frame, side, ema, timeframe_seconds=timeframe_seconds,
                    now_ms=now_ms, lookback_candles=2,
                )
            except Exception:
                continue
            authorization_state = str(gate.get("authorization_state") or "")
            if not gate.get("ready") or authorization_state != str(_AUTH_EMA9_VWAP_TRIGGER_VALID):
                continue
            # The authoritative trigger is the strict EMA9/Session-VWAP cross.
            # EMA15 is deliberately not part of scanner authorization.
            direction = str(gate.get("ema9_vwap_cross") or gate.get("ema9_cross") or "").upper()
            expected_direction = "BULLISH" if side == "BUY" else "BEARISH"
            if direction != expected_direction:
                continue
            event_id = _confirmed_cross_event_id(symbol, INTERVAL, gate)
            if not event_id:
                continue
            metadata = dict(gate)
            metadata.update({
                "source": CONFIRMED_CROSS_EVENT_SOURCE,
                "detection_method": "validate_ema9_vwap_entry_gate",
                "lookback_candles": 2,
                "lifecycle_schema": CROSS_LIFECYCLE_SCHEMA,
                "lifecycle_status": CROSS_WATCHLIST_LIFECYCLE,
                "cross_state": "CONFIRMED_BUY_CROSS" if side == "BUY" else "CONFIRMED_SELL_CROSS",
                "phase": "WAITING_FOR_PULLBACK",
                "entry_ready": False,
                "cross_price": float(frame["close"].iloc[-1]),
                "max_chase_atr": CROSS_REJECTION_MAX_CHASE_ATR,
            })
            try:
                inserted = journal.record_cross_event(
                    event_id=event_id,
                    symbol=roro_canonical_symbol(symbol),
                    exchange_symbol=str(symbol),
                    side=side,
                    direction=direction,
                    source=CONFIRMED_CROSS_EVENT_SOURCE,
                    timeframe=str(INTERVAL),
                    signal_candle_timestamp=int(gate.get("signal_candle_timestamp") or 0),
                    closed_candle_timestamp=int(gate.get("latest_closed_timestamp") or 0),
                    expires_ts=time.time() + CONFIRMED_CROSS_EVENT_TTL_SEC,
                    metadata=metadata,
                )
            except Exception as exc:
                log_execution(
                    f"[CROSS_EVENT] persist failed for {event_id}: {type(exc).__name__}: {exc}",
                    "ERROR",
                )
                continue
            if inserted:
                created += 1
                _invalidate_superseded_cross_events(symbol, direction, journal, keep_event_id=event_id)
                log_execution(
                    f"[CROSS_EVENT] CONFIRMED {event_id} side={side} "
                    f"candle={gate.get('signal_candle_timestamp')}", "INFO",
                )
    _sync_confirmed_cross_events()
    return created


def _cross_event_is_usable(event_id, side=None):
    event_id = str(event_id or "").strip()
    if not event_id:
        return False
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None:
        return False
    try:
        row = journal.get_cross_event(event_id)
    except Exception:
        return False
    if not row or str(row.get("status") or "").upper() != "ACTIVE":
        return False
    if side and str(row.get("side") or "").upper() != str(side).upper():
        return False
    if str(row.get("event_id") or "") != event_id:
        return False
    if float(row.get("expires_ts") or 0.0) <= time.time():
        return False
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    if str(metadata.get("lifecycle_schema") or "") == CROSS_LIFECYCLE_SCHEMA:
        zone = metadata.get("selected_entry_zone")
        zone_gate = metadata.get("structural_zone_gate")
        return (
            str(metadata.get("phase") or "").upper() == "BREAKOUT_CONFIRMED"
            and metadata.get("entry_ready") is True
            and metadata.get("breakout_candidate") is True
            and isinstance(zone, dict)
            and isinstance(zone_gate, dict)
            and zone_gate.get("allowed") is True
            and str(zone_gate.get("authority") or "") == "UNIFIED_WATCHLIST_ENTRY_ZONE"
        )
    return True


def _get_confirmed_cross_event(event_id):
    event_id = str(event_id or "").strip()
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if not event_id or journal is None:
        return None
    try:
        return journal.get_cross_event(event_id)
    except Exception:
        return None


def _hydrate_cross_lifecycle_context(ctx, event_id, gate_result, side, symbol, timeframe):
    """Bind an execution to the original cross event and its rejection breakout.

    The fresh-cross gate may legitimately be stale by the time a delayed
    pullback/rejection/breakout occurs. In that case the durable lifecycle is
    authoritative, but only when it explicitly says BREAKOUT_CONFIRMED.
    """
    row = _get_confirmed_cross_event(event_id)
    if not row:
        return False
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    if str(metadata.get("lifecycle_schema") or "") != CROSS_LIFECYCLE_SCHEMA:
        return False
    context = ctx if isinstance(ctx, dict) else {}
    context.update({
        "cross_event_phase": str(metadata.get("phase") or "WAITING_FOR_PULLBACK"),
        "cross_event_lifecycle_schema": CROSS_LIFECYCLE_SCHEMA,
        "cross_event_entry_ready": bool(metadata.get("entry_ready") is True),
        "cross_event_rejection_high": metadata.get("rejection_high"),
        "cross_event_rejection_low": metadata.get("rejection_low"),
        "cross_event_rejection_timestamp": metadata.get("rejection_candle_timestamp"),
        "cross_event_breakout_timestamp": metadata.get("breakout_candle_timestamp"),
        "cross_event_breakout_close": metadata.get("breakout_close"),
        "cross_event_chase_atr": metadata.get("chase_atr"),
        "cross_event_max_chase_atr": metadata.get("max_chase_atr"),
        "cross_event_cross_price": metadata.get("cross_price"),
        "cross_event_cross_atr": metadata.get("cross_atr"),
    })
    gate = gate_result if isinstance(gate_result, dict) else {}
    gate["durable_event_id"] = str(row.get("event_id") or event_id)
    gate["signal_candle_timestamp"] = int(row.get("signal_candle_timestamp") or 0)
    original_signal_id = str(metadata.get("signal_candle_id") or "")
    if not original_signal_id:
        original_direction = "BUY" if str(side).upper() == "BUY" else "SELL"
        original_signal_id = f"{original_direction}:{int(row.get('signal_candle_timestamp') or 0)}"
    gate["signal_candle_id"] = original_signal_id
    recomputed_event_id = _confirmed_cross_event_id(symbol, timeframe, gate)
    if recomputed_event_id:
        gate["durable_event_id"] = recomputed_event_id
    gate["lifecycle_phase"] = context["cross_event_phase"]
    gate["lifecycle_entry_ready"] = context["cross_event_entry_ready"]
    selected_zone = metadata.get("selected_entry_zone")
    zone_gate = metadata.get("structural_zone_gate")
    if isinstance(selected_zone, dict):
        gate["selected_entry_zone"] = dict(selected_zone)
    if isinstance(zone_gate, dict):
        gate["structural_zone_gate"] = dict(zone_gate)
    zone_authorized = (
        context["cross_event_entry_ready"] is True
        and str(context["cross_event_phase"]).upper() == "BREAKOUT_CONFIRMED"
        and metadata.get("breakout_candidate") is True
        and isinstance(selected_zone, dict)
        and isinstance(zone_gate, dict)
        and zone_gate.get("allowed") is True
        and str(zone_gate.get("authority") or "") == "UNIFIED_WATCHLIST_ENTRY_ZONE"
    )
    if zone_authorized:
        gate["ready"] = True
        gate["authorization_state"] = "WATCHLIST_STRUCTURAL_ZONE_AUTHORIZED"
        gate["reason"] = "breakout_and_matching_watchlist_zone_confirmed"
    else:
        gate["ready"] = False
        gate["authorization_state"] = "BREAKOUT_CANDIDATE_AWAITING_STRUCTURAL_ZONE"
        gate["reason"] = str((zone_gate or {}).get("reason") or "watchlist_structural_zone_not_authorized")
    return True


def _build_persistent_entry_context(ctx, gate_result, side, symbol, trade_id, atr_val=None):
    """Freeze the actual entry evidence; never read live scanner state later."""
    source = ctx if isinstance(ctx, dict) else {}
    gate = gate_result if isinstance(gate_result, dict) else {}
    ob = source.get("order_block") if isinstance(source.get("order_block"), dict) else {}
    golden = source.get("golden_zone") if isinstance(source.get("golden_zone"), dict) else {}
    liquidity = source.get("liquidity") if isinstance(source.get("liquidity"), dict) else {}
    structure = source.get("market_structure") if isinstance(source.get("market_structure"), dict) else {}
    market_intel = source.get("market_intelligence") if isinstance(source.get("market_intelligence"), dict) else {}
    adx_di = source.get("adx_di") if isinstance(source.get("adx_di"), dict) else {}
    support_resistance = source.get("support_resistance") if isinstance(source.get("support_resistance"), dict) else {}

    def _number(value, default=None):
        try:
            number = float(value)
            return number if math.isfinite(number) else default
        except (TypeError, ValueError):
            return default

    def _first(*values):
        for value in values:
            if value is not None and value != "":
                return value
        return None

    ob_fresh = str(ob.get("status") or source.get("order_block_status") or "").upper() in {
        "FRESH", "UNTESTED", "VALID"
    }
    try:
        canonical = roro_canonical_symbol(symbol)
    except Exception:
        canonical = str(symbol or "").strip().upper()
    return {
        "schema_version": 1,
        "trade_id": str(trade_id or ""),
        "symbol": canonical,
        "exchange_symbol": str(symbol or ""),
        "side": str(side or "").upper(),
        "cross_event_id": str(_first(source.get("cross_event_id"), gate.get("durable_event_id")) or ""),
        "cross_event_source": str(_first(source.get("cross_event_source"), CONFIRMED_CROSS_EVENT_SOURCE)),
        "cross_event_direction": str(_first(source.get("cross_event_direction"), gate.get("ema9_15_cross"), gate.get("ema9_cross")) or "").upper(),
        "cross_event_timestamp": _first(source.get("cross_event_timestamp"), gate.get("signal_candle_timestamp")) or 0,
        "cross_event_timeframe": str(_first(source.get("cross_event_timeframe"), gate.get("timeframe")) or INTERVAL),
        "cross_event_status": str(_first(source.get("cross_event_status"), "ACTIVE")),
        "cross_signal_candle_id": str(gate.get("signal_candle_id") or ""),
        "durable_event_id": str(gate.get("durable_event_id") or ""),
        "rejection_candle": {
            "phase": str(_first(source.get("cross_event_phase"), gate.get("lifecycle_phase"), "WAITING_FOR_PULLBACK")),
            "lifecycle_schema": str(_first(source.get("cross_event_lifecycle_schema"), CROSS_LIFECYCLE_SCHEMA)),
            "entry_ready": bool(_first(source.get("cross_event_entry_ready"), gate.get("lifecycle_entry_ready"), False)),
            "rejection_timestamp": _first(source.get("cross_event_rejection_timestamp"), gate.get("rejection_candle_timestamp")),
            "rejection_high": _number(_first(source.get("cross_event_rejection_high"), gate.get("rejection_high"))),
            "rejection_low": _number(_first(source.get("cross_event_rejection_low"), gate.get("rejection_low"))),
            "breakout_timestamp": _first(source.get("cross_event_breakout_timestamp"), gate.get("breakout_candle_timestamp")),
            "breakout_close": _number(_first(source.get("cross_event_breakout_close"), gate.get("breakout_close"))),
            "chase_atr": _number(_first(source.get("cross_event_chase_atr"), gate.get("chase_atr"))),
            "max_chase_atr": _number(source.get("cross_event_max_chase_atr"), CROSS_REJECTION_MAX_CHASE_ATR),
        },
        "actual_entry_price": None,
        "actual_filled_qty": None,
        "profit_target_convention": profit_target_convention(),
        "entry_zone": {
            "low": _number(source.get("entry_zone_low"), 0.0),
            "high": _number(source.get("entry_zone_high"), 0.0),
            "quality": _first(source.get("zone_quality"), golden.get("score")),
            "state": str(golden.get("state") or "UNKNOWN"),
        },
        "order_block": {
            "type": str(ob.get("type") or source.get("zone_type") or "UNKNOWN"),
            "freshness": str(ob.get("status") or source.get("order_block_status") or "UNKNOWN"),
            "valid": bool(ob.get("valid", ob_fresh)),
            "low": _number(ob.get("low")),
            "high": _number(ob.get("high")),
            "strength": _number(ob.get("strength")),
            "distance_atr": _number(ob.get("distance_atr")),
        },
        "supply_demand_zone": {
            "zone_type": str(source.get("zone_type") or ("DEMAND" if str(side).upper() == "BUY" else "SUPPLY")),
            "status": str(golden.get("state") or "UNKNOWN"),
            "support_resistance": support_resistance,
        },
        "liquidity": dict(liquidity),
        "market_structure": dict(structure),
        "relative_volume": _number(_first(source.get("volume_ratio"), market_intel.get("volume_ratio"))),
        "adx_di": {
            "adx": _number(_first(source.get("adx"), adx_di.get("adx"))),
            "di_plus": _number(_first(source.get("di_plus"), adx_di.get("di_plus"))),
            "di_minus": _number(_first(source.get("di_minus"), adx_di.get("di_minus"))),
        },
        "atr": _number(_first(source.get("atr"), atr_val)),
        "ema9": _number(_first(gate.get("ema9"), source.get("ema9"))),
        "ema50": _number(_first(gate.get("ema50"), source.get("ema50"))),
        "vwap": _number(_first(gate.get("vwap"), source.get("vwap"))),
        "entry_regime": str(_first(source.get("market_regime"), market_intel.get("regime"), "UNKNOWN")),
        "trading_mode": str(_first(source.get("trading_mode"), market_intel.get("mode"), "UNCLASSIFIED")),
        "entry_behavior": str(_first(source.get("entry_behavior"), market_intel.get("entry_behavior"), "WAIT_FOR_STRUCTURE")),
        "entry_type": _first(
            source.get("entry_type"),
            (source.get("entry_behavior_context") or {}).get("entry_type")
            if isinstance(source.get("entry_behavior_context"), dict) else None,
        ),
        "entry_behavior_context": (dict(source["entry_behavior_context"])
                                   if isinstance(source.get("entry_behavior_context"), dict) else {}),
        "institutional_report": dict(source.get("institutional_report") or {}),
        "institutional_location_gate": dict(source.get("institutional_location_gate") or {}),
        "entry_risk_plan": dict(source.get("entry_risk_plan") or {}),
        "lifecycle_status": _first(source.get("lifecycle_status"), source.get("cross_event_lifecycle_status"), "CROSS_DETECTED"),
        "entry_behavior_confidence": _number(source.get("entry_behavior_confidence"), 0.0),
        "evidence": list(source.get("evidence") or []),
        "unified_decision_context": (source.get("unified_decision_context") or source.get("decision_context")
                                     if isinstance(source.get("unified_decision_context"), dict)
                                     or isinstance(source.get("decision_context"), dict) else None),
        "structural_stop": _number(source.get("structural_stop")),
        "closed_candle_timestamp": source.get("closed_candle_timestamp") or gate.get("latest_closed_timestamp") or 0,
        "frame_source": str(source.get("closed_candle_source") or gate.get("frame_source") or "UNKNOWN"),
        "snapshot_ts": time.time(),
    }


def get_usdt_perp_symbols():
    """Load and validate the USDT swap universe through the central governor."""
    try:
        safe_api_call(ex.load_markets)
        markets = getattr(ex, "markets", None) or {}
        symbols = []
        for market_symbol, meta in markets.items():
            meta = meta if isinstance(meta, dict) else {}
            if not (meta.get('swap') and meta.get('active', True)):
                continue
            normalized = normalize_symbol(market_symbol)
            if normalized and ("USDT" in str(market_symbol).upper()):
                symbols.append(normalized)
        MEMORY["market_universe"] = {
            "state": "READY" if symbols else "BLOCKED",
            "reason": "" if symbols else "NO_ACTIVE_USDT_SWAPS",
            "count": len(symbols),
        }
        return symbols[:SCANNER_UNIVERSE_MAX]
    except Exception as exc:
        reason = f"MARKET_UNIVERSE_UNKNOWN:{type(exc).__name__}"
        MEMORY["market_universe"] = {"state": "BLOCKED", "reason": reason, "count": 0}
        log_execution(f"[MARKET_UNIVERSE] {reason}: {str(exc)[:180]}", "ERROR",
                      debounce_key="market_universe_failed", debounce_sec=30)
        return []

def rf_proximity_score(rf, adx_val, vol_ok, rsi_val, atr_pct):
    dist = abs(rf["distance"]) if rf["distance"] else 1.0
    proximity = max(0.0, 1.0 - (dist / 0.015))
    if adx_val < 18:
        trend = 0.2
    elif 18 <= adx_val <= 30:
        trend = 1.0
    elif 30 < adx_val <= 40:
        trend = 0.6
    else:
        trend = 0.2
    if 30 <= rsi_val <= 70:
        rsi_score = 0.5
    elif 20 <= rsi_val < 30 or 70 < rsi_val <= 80:
        rsi_score = 0.3
    else:
        rsi_score = 0.0
    vol_score = 1.0 if vol_ok else 0.0
    vol_boost = 0.3 if 0.5 <= atr_pct <= 2.0 else 0.0
    trigger_boost = 1.2 if rf["triggered"] else 0.0
    score = (proximity * 0.35) + (trend * 0.25) + (vol_score * 0.15) + (rsi_score * 0.1) + (vol_boost * 0.05) + trigger_boost
    return float(score)

def scan_market_rf(top_n=40, symbols=None):
    symbols = list(symbols) if symbols is not None else get_usdt_perp_symbols()[:SCANNER_UNIVERSE_MAX]
    symbols = symbols[:SCANNER_UNIVERSE_MAX]
    if not symbols:
        return []
    rf_engine = RFEngine(period=20, multiplier=3.5)
    results = []
    for sym in symbols[:150]:
        # Yield immediately if an entry opens while this background scan is active.
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            break
        try:
            # Shared 150-bar cache key lets Scanner V2 and RF scanner reuse one OHLCV snapshot.
            df = _get_scanner_frame(sym, 150)
            if df is None or not validate_dataframe(df, 100):
                continue
            try:
                atr_series = compute_atr(df, 14)
                adx_series = compute_adx(df, 14)
                rsi_series = compute_rsi(df, 14)
                atr_val = float(atr_series.iloc[-1])
                adx_val = float(adx_series.iloc[-1])
                rsi_val = float(rsi_series.iloc[-1])
                if rsi_val == 0 or rsi_val is None or math.isnan(rsi_val):
                    continue
                if atr_val == 0 or atr_val is None or math.isnan(atr_val):
                    continue
                if adx_val is None or math.isnan(adx_val):
                    adx_val = 20.0
                atr_pct = (atr_val / df['close'].iloc[-1]) * 100 if df['close'].iloc[-1] > 0 else 0
            except Exception:
                continue
            rf = rf_engine.compute(df)
            if rf["signal"] is None and abs(rf.get("distance", 1.0)) > 0.015:
                continue
            avg_vol = df['volume'].iloc[-20:].mean()
            vol_ok = df['volume'].iloc[-1] >= avg_vol * 0.7
            atr_pct = (atr_val / df['close'].iloc[-1]) * 100 if df['close'].iloc[-1] > 0 else 0
            score = rf_proximity_score(rf, adx_val, vol_ok, rsi_val, atr_pct)
            if score < 0.3:
                continue
            if rf["triggered"]:
                status = "TRIGGERED"
            elif score >= 0.6:
                status = "READY"
            else:
                status = "PROXIMITY"
            results.append({
                "symbol": sym,
                "score": round(score, 3),
                "rf_signal": rf["signal"],
                "rf_triggered": rf["triggered"],
                "rf_distance": round(rf.get("distance", 0), 4),
                "adx": round(adx_val, 1),
                "rsi": round(rsi_val, 1),
                "atrp": round(atr_pct, 2),
                "status": status
            })
        except Exception:
            continue
    results = sorted(results, key=lambda x: x["score"], reverse=True)
    return results[:top_n]

# ========== SMART SCANNER v2 ==========
def smart_scanner_v2(symbols=None):
    symbols = list(symbols) if symbols is not None else get_usdt_perp_symbols()[:SCANNER_UNIVERSE_MAX]
    symbols = symbols[:SCANNER_UNIVERSE_MAX]
    buy_candidates = []
    sell_candidates = []
    for sym in symbols:
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            break
        try:
            df = _get_scanner_frame(sym, 150)
            if df is None or len(df) < 100:
                continue
            price = df['close'].iloc[-1]
            rf_engine = RFEngine(period=20, multiplier=3.5)
            rf = rf_engine.compute(df)
            if rf["distance"] is None:
                continue
            rf_prox = abs(rf["distance"])
            vol_ma = df['volume'].iloc[-21:-1].mean()
            if df['volume'].iloc[-1] < 0.5 * vol_ma:
                continue
            atr_val = compute_atr(df).iloc[-1]
            atr_pct = (atr_val / price) * 100 if price > 0 else 0
            if atr_pct < 0.2:
                continue
            liquidity_ctx = detect_liquidity_context(df, lookback=10)
            supports, resistances = get_clustered_zones(df, lookback=120, cluster_pct=0.002)
            zone_ctx = detect_zone_context(price, supports, resistances, threshold=0.003)
            structure_ctx = detect_structure_shift(df)
            rejection_buy = candle_rejection(df, "BUY")
            rejection_sell = candle_rejection(df, "SELL")
            vol_spike_flag = volume_spike(df)
            location = compute_location(df, price, "BUY")

            smart_money = SmartMoneyEngine.analyze_smart_money(df)
            momentum = MomentumFlowEngine.analyze_momentum_flow(df)

            score_mod_buy = 0
            score_mod_sell = 0

            if smart_money["smart_money_dominant"]:
                if smart_money["institutional_bias"] == "BUY":
                    score_mod_buy += 2.5
                elif smart_money["institutional_bias"] == "SELL":
                    score_mod_sell += 2.5
            if smart_money["distribution_risk"] > 70:
                score_mod_sell += 1.5
                score_mod_buy -= 2.0
            if smart_money["accumulation_strength"] > 60:
                score_mod_buy += 1.5
                score_mod_sell -= 2.0
            if smart_money["retail_euphoria"]:
                score_mod_buy -= 1.5
                score_mod_sell -= 1.5

            if momentum["trend_expansion"]:
                if momentum["flow_bias"] == "BUY":
                    score_mod_buy += 2.0
                elif momentum["flow_bias"] == "SELL":
                    score_mod_sell += 2.0
            if momentum["momentum_decay"]:
                score_mod_buy -= 1.5
                score_mod_sell -= 1.5
            if momentum["exhaustion_risk"] > 70:
                score_mod_buy -= 2.0
                score_mod_sell -= 2.0
            if momentum["climax_risk"] > 70:
                score_mod_buy -= 1.5
                score_mod_sell -= 1.5
            if momentum["greed_state"]:
                score_mod_buy -= 1.0
                score_mod_sell -= 1.0

            base_score_buy = 0
            if liquidity_ctx == "sell_side_taken":
                base_score_buy += 2
            if zone_ctx["near_support"]:
                base_score_buy += 2
            if structure_ctx == "bullish_shift":
                base_score_buy += 1.5
            if rf_prox < 0.0015:
                base_score_buy += 2
            elif rf_prox < 0.003:
                base_score_buy += 1
            if rejection_buy:
                base_score_buy += 1.5
            if vol_spike_flag:
                base_score_buy += 1

            base_score_sell = 0
            if liquidity_ctx == "buy_side_taken":
                base_score_sell += 2
            if zone_ctx["near_resistance"]:
                base_score_sell += 2
            if structure_ctx == "bearish_shift":
                base_score_sell += 1.5
            if rf_prox < 0.0015:
                base_score_sell += 2
            elif rf_prox < 0.003:
                base_score_sell += 1
            if rejection_sell:
                base_score_sell += 1.5
            if vol_spike_flag:
                base_score_sell += 1

            final_score_buy = base_score_buy + score_mod_buy
            final_score_sell = base_score_sell + score_mod_sell

            if final_score_buy >= 5:
                buy_candidates.append({
                    "symbol": sym,
                    "score": round(final_score_buy, 2),
                    "rf_prox": round(rf_prox*100, 3),
                    "liquidity": liquidity_ctx,
                    "zone": zone_ctx,
                    "structure": structure_ctx,
                    "rejection": rejection_buy,
                    "volume_spike": vol_spike_flag,
                    "location": location,
                    "smart_money": {
                        "bias": smart_money["institutional_bias"],
                        "bias_detailed": smart_money.get("institutional_bias_detailed", "NEUTRAL"),
                        "dominant": smart_money["smart_money_dominant"],
                        "distribution_risk": round(smart_money["distribution_risk"], 1),
                        "accumulation": round(smart_money["accumulation_strength"], 1)
                    },
                    "momentum": {
                        "expansion": momentum["trend_expansion"],
                        "decay": momentum["momentum_decay"],
                        "exhaustion_risk": round(momentum["exhaustion_risk"], 1),
                        "greed": momentum["greed_state"]
                    }
                })
            if final_score_sell >= 5:
                sell_candidates.append({
                    "symbol": sym,
                    "score": round(final_score_sell, 2),
                    "rf_prox": round(rf_prox*100, 3),
                    "liquidity": liquidity_ctx,
                    "zone": zone_ctx,
                    "structure": structure_ctx,
                    "rejection": rejection_sell,
                    "volume_spike": vol_spike_flag,
                    "location": compute_location(df, price, "SELL"),
                    "smart_money": {
                        "bias": smart_money["institutional_bias"],
                        "bias_detailed": smart_money.get("institutional_bias_detailed", "NEUTRAL"),
                        "dominant": smart_money["smart_money_dominant"],
                        "distribution_risk": round(smart_money["distribution_risk"], 1),
                        "accumulation": round(smart_money["accumulation_strength"], 1)
                    },
                    "momentum": {
                        "expansion": momentum["trend_expansion"],
                        "decay": momentum["momentum_decay"],
                        "exhaustion_risk": round(momentum["exhaustion_risk"], 1),
                        "greed": momentum["greed_state"]
                    }
                })
        except Exception as e:
            continue
    buy_sorted = sorted(buy_candidates, key=lambda x: x["score"], reverse=True)[:10]
    sell_sorted = sorted(sell_candidates, key=lambda x: x["score"], reverse=True)[:10]
    return buy_sorted, sell_sorted

# ========== INSTITUTIONAL LIQUIDITY NARRATIVE ENGINE ==========
def equal_levels_points(highs, lows, tolerance=0.002):
    eq_highs = []
    eq_lows = []
    def cluster(points, tol):
        if not points:
            return []
        points = sorted(points)
        clusters = []
        current = [points[0]]
        for p in points[1:]:
            if abs(p - current[-1]) / current[-1] < tol:
                current.append(p)
            else:
                clusters.append(current)
                current = [p]
        clusters.append(current)
        return clusters
    high_clusters = cluster(highs, tolerance)
    low_clusters = cluster(lows, tolerance)
    for cl in high_clusters:
        if len(cl) >= 2:
            eq_highs.extend([sum(cl)/len(cl)])
    for cl in low_clusters:
        if len(cl) >= 2:
            eq_lows.extend([sum(cl)/len(cl)])
    return eq_highs, eq_lows

def find_swing_points(df, window=3):
    highs = df['high'].values
    lows = df['low'].values
    swing_highs = []
    swing_lows = []
    for i in range(window, len(df)-window):
        if highs[i] == max(highs[i-window:i+window+1]):
            swing_highs.append(highs[i])
        if lows[i] == min(lows[i-window:i+window+1]):
            swing_lows.append(lows[i])
    return swing_highs, swing_lows

def detect_equal_highs_lows(df, lookback=50):
    sub = df.iloc[-lookback:]
    sh, sl = find_swing_points(sub, window=2)
    return equal_levels_points(sh, sl)

def detect_order_block(df, side, lookback=4):
    if len(df) < lookback+2:
        return None
    atr = compute_atr(df).iloc[-1]
    move = abs(df['close'].iloc[-1] - df['close'].iloc[-2])
    if move < atr * 1.2:
        return None
    for i in range(2, lookback+2):
        if i >= len(df):
            break
        candle = df.iloc[-i]
        if side == "BUY" and candle['close'] < candle['open']:
            return {"low": candle['low'], "high": candle['high'], "idx": -i}
        elif side == "SELL" and candle['close'] > candle['open']:
            return {"low": candle['low'], "high": candle['high'], "idx": -i}
    return None

def detect_fvg(df, threshold=0.001):
    if len(df) < 2:
        return None
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    if curr['low'] > prev['high'] * (1+threshold):
        return ("bullish", prev['high'], curr['low'])
    elif curr['high'] < prev['low'] * (1-threshold):
        return ("bearish", curr['high'], prev['low'])
    return None

def evaluate_liquidity_narrative(df, ob, atr, side):
    narrative = {"sweep": False, "choch_bos": False, "retest": False, "rejection": False,
                 "displacement": False, "rf_alignment": False, "volume_confirmation": False}
    price = df['close'].iloc[-1]
    pools = build_liquidity_pools(df)
    swept_h, swept_l = detect_sweep(df, pools)
    if side == "BUY" and swept_l:
        narrative["sweep"] = True
    elif side == "SELL" and swept_h:
        narrative["sweep"] = True
    bos_up, bos_down = detect_bos(df)
    struct_shift = detect_structure_shift(df)
    choch = struct_shift is not None
    if side == "BUY" and (bos_up or (choch and struct_shift == "bullish_shift")):
        narrative["choch_bos"] = True
    elif side == "SELL" and (bos_down or (choch and struct_shift == "bearish_shift")):
        narrative["choch_bos"] = True
    zones = get_smart_zones(df.symbol if hasattr(df, 'symbol') else "unknown", df, ob)
    required_zone = None
    if zones:
        if side == "BUY" and zones["buy_zones"]:
            required_zone = zones["buy_zones"][0]
        elif side == "SELL" and zones["sell_zones"]:
            required_zone = zones["sell_zones"][0]
    if required_zone:
        dist = abs(price - required_zone["price"]) / price
        if dist < 0.003:
            narrative["retest"] = True
    if candle_rejection(df, side):
        narrative["rejection"] = True
    vol_state = classify_volume(df)
    if detect_displacement(df, side, atr, vol_state):
        narrative["displacement"] = True
    if vol_state in ("expansion", "spike"):
        narrative["volume_confirmation"] = True
    rf = RFEngine(20, 3.5).compute(df)
    if rf["signal"] == side and abs(rf["distance"]) < 0.003:
        narrative["rf_alignment"] = True
    score = 0
    if narrative["sweep"]: score += 2
    if narrative["choch_bos"]: score += 2
    if narrative["retest"]: score += 2
    if narrative["rejection"]: score += 1.5
    if narrative["displacement"]: score += 1.5
    if narrative["volume_confirmation"]: score += 1
    if narrative["rf_alignment"]: score += 2
    return narrative, score

def smart_opportunity_selection():
    candidates = []
    for c in MEMORY.get("scanner_v2_buy", [])[:5]:
        candidates.append({"symbol": c["symbol"], "side": "BUY", "score": c["score"], "source": "v2"})
    for c in MEMORY.get("scanner_v2_sell", [])[:5]:
        candidates.append({"symbol": c["symbol"], "side": "SELL", "score": c["score"], "source": "v2"})
    for c in MEMORY.get("rf_watchlist", [])[:10]:
        if c.get("rf_signal") in ("BUY", "SELL"):
            candidates.append({"symbol": c["symbol"], "side": c["rf_signal"], "score": c["score"], "source": "rf"})
    seen = {}
    for cand in candidates:
        sym = cand["symbol"]
        if sym not in seen or cand["score"] > seen[sym]["score"]:
            seen[sym] = cand
    candidates = list(seen.values())
    best_setup = None
    best_score = -1
    for cand in candidates[:15]:
        try:
            sym = cand["symbol"]
            side = cand["side"]
            df = get_ohlcv_safe(sym, 100)
            if df is None or not validate_dataframe(df, 80):
                continue
            df.symbol = sym
            ob = get_orderbook_cached(sym, limit=10)
            atr = compute_atr(df).iloc[-1] if len(df) > 14 else df['close'].iloc[-1] * 0.01
            narrative, nscore = evaluate_liquidity_narrative(df, ob, atr, side)
            smart_money = SmartMoneyEngine.analyze_smart_money(df)
            momentum = MomentumFlowEngine.analyze_momentum_flow(df)
            total_confidence_adjust = 0
            if smart_money["smart_money_dominant"] and smart_money["institutional_bias"] == side:
                total_confidence_adjust += 15
            if momentum["trend_expansion"] and momentum["flow_bias"] == side:
                total_confidence_adjust += 10
            if smart_money["distribution_risk"] > 70:
                total_confidence_adjust -= 15
            if momentum["momentum_decay"]:
                total_confidence_adjust -= 12
            if momentum["exhaustion_risk"] > 70:
                total_confidence_adjust -= 10
            adjusted_nscore = nscore + (total_confidence_adjust / 10)
            record_watchlist_entry(sym, side, narrative, adjusted_nscore, smart_money, momentum)
            if adjusted_nscore < 7:
                continue
            zones = get_smart_zones(sym, df, ob)
            zone_strength = 0
            if side == "BUY" and zones["buy_zones"]:
                zone_strength = zones["buy_zones"][0]["strength"]
            elif side == "SELL" and zones["sell_zones"]:
                zone_strength = zones["sell_zones"][0]["strength"]
            total = adjusted_nscore + zone_strength * 0.5
            if total > best_score:
                best_score = total
                best_setup = (sym, side, total, narrative, zones, df, ob, atr)
        except Exception:
            continue
    if best_setup and best_score >= 9:
        sym, side, score, narrative, zones, df, ob, atr = best_setup
        price = df['close'].iloc[-1]
        leg_class = "REVERSAL"
        sl, tp1, tp2 = compute_sl_tp(price, side, leg_class, atr, df)
        reason_str = f"INST_SWEEP+CHOCH+RETEST | nscore={score:.1f}"
        ok = execute_entry(side, sym, price, sl, tp1, tp2, score, reason_str, atr,
                           trade_type="INSTITUTIONAL", entry_type="NARRATIVE", classification="SNIPER")
        if ok:
            return True
    return False

def record_watchlist_entry(symbol, side, narrative, score, smart_money=None, momentum=None):
    now = time.time()
    state = "DETECTED"
    if narrative.get("retest"):
        state = "RETEST"
    if narrative.get("rejection"):
        state = "REJECTION"
    if narrative.get("displacement"):
        state = "DISPLACEMENT"
    if narrative.get("sweep") and narrative.get("choch_bos") and narrative.get("retest") and narrative.get("rejection"):
        state = "CONFIRMED"
    reasons_list = []
    if narrative["sweep"]: reasons_list.append("Sweep")
    if narrative["choch_bos"]: reasons_list.append("CHoCH/BOS")
    if narrative["retest"]: reasons_list.append("ZONE_RETEST")
    if narrative["rejection"]: reasons_list.append("OB")
    if narrative["displacement"]: reasons_list.append("Displacement")
    if narrative["volume_confirmation"]: reasons_list.append("Volume")
    if narrative["rf_alignment"]: reasons_list.append("RF")
    trade_type = "REVERSAL" if (narrative["sweep"] or narrative["retest"]) else "TREND"
    strength = "WEAK"
    if score >= 7:
        strength = "STRONG"
    elif score >= 4:
        strength = "MEDIUM"
    entry = {
        "symbol": symbol,
        "side": side,
        "score": round(score, 2),
        "state": state,
        "reasons": reasons_list,
        "trade_type": trade_type,
        "strength": strength,
        "last_update": now
    }
    if smart_money:
        entry["smart_money_bias"] = smart_money.get("institutional_bias", "NEUTRAL")
        entry["smart_money_bias_detailed"] = smart_money.get("institutional_bias_detailed", "NEUTRAL")
        entry["distribution_risk"] = round(smart_money.get("distribution_risk", 0), 1)
        entry["accumulation"] = round(smart_money.get("accumulation_strength", 0), 1)
    if momentum:
        entry["momentum_expansion"] = momentum.get("trend_expansion", False)
        entry["momentum_decay"] = momentum.get("momentum_decay", False)
        entry["exhaustion_risk"] = round(momentum.get("exhaustion_risk", 0), 1)
        entry["continuation_strength"] = round(momentum.get("continuation_strength", 0), 1)
    if "watchlist" not in MEMORY:
        MEMORY["watchlist"] = {}
    MEMORY["watchlist"][symbol] = entry

def cleanup_watchlist(ttl=300):
    now = time.time()
    if "watchlist" not in MEMORY:
        return
    expired = [sym for sym, v in MEMORY["watchlist"].items() if now - v["last_update"] > ttl]
    for sym in expired:
        del MEMORY["watchlist"][sym]

# ========== VWAP ENGINE ==========
def compute_vwap(df):
    tp = (df['high'] + df['low'] + df['close']) / 3
    cum_vol = df['volume'].cumsum()
    cum_tp_vol = (tp * df['volume']).cumsum()
    vwap = cum_tp_vol / cum_vol
    return vwap

def vwap_features(df):
    vwap = compute_vwap(df)
    price = df['close'].iloc[-1]
    distance = (price - vwap.iloc[-1]) / vwap.iloc[-1] if vwap.iloc[-1] != 0 else 0.0
    slope = vwap.iloc[-1] - vwap.iloc[-5] if len(vwap) >= 5 else 0.0
    return {"vwap": vwap.iloc[-1], "distance": distance, "slope": slope}

def detect_exhaustion_zone(df):
    atr = compute_atr(df).iloc[-1]
    rsi = compute_rsi(df).iloc[-1]
    vw = vwap_features(df)
    last = df.iloc[-1]
    impulse = (last['high'] - last['low']) >= 1.4 * atr if atr > 0 else False
    stretched = abs(vw["distance"]) >= 0.012
    rsi_extreme = rsi >= 70 or rsi <= 30
    if not (impulse and stretched and rsi_extreme):
        return False, None, None
    if rsi >= 70 and vw["distance"] > 0.012:
        return True, last['high'], "TOP"
    elif rsi <= 30 and vw["distance"] < -0.012:
        return True, last['low'], "BOTTOM"
    return False, None, None

def detect_reset(df, zone_price, zone_type):
    last_close = df['close'].iloc[-1]
    if zone_type == "TOP":
        drop = (zone_price - last_close) / zone_price
        return drop >= 0.003
    else:
        rise = (last_close - zone_price) / zone_price
        return rise >= 0.003

def confirm_reversal(df, ob, zone_type):
    last = df.iloc[-1]
    wick = (last['high'] - max(last['open'], last['close'])) if zone_type == "TOP" else (min(last['open'], last['close']) - last['low'])
    body = abs(last['close'] - last['open'])
    wick_reject = wick > body * 1.5 if body > 0 else False
    macd_hist = compute_macd(df)[2]
    macd_flip = macd_first_flip(macd_hist)
    flow = flow_engine(df)
    flow_agree = (zone_type == "TOP" and flow == "aggressive_sell") or (zone_type == "BOTTOM" and flow == "aggressive_buy")
    obi = orderbook_imbalance(ob)
    obi_agree = (zone_type == "TOP" and obi < -0.2) or (zone_type == "BOTTOM" and obi > 0.2)
    score = sum([wick_reject, macd_flip, flow_agree, obi_agree])
    return score >= 2

def detect_stop_hunt(df):
    pools = build_liquidity_pools(df)
    swept_high, swept_low = detect_sweep(df, pools)
    last = df.iloc[-1]
    inside = last['close'] < last['high'] and last['close'] > last['low']
    reclaim = (swept_high and last['close'] < last['high']) or (swept_low and last['close'] > last['low'])
    volume_ok = volume_pressure_real(df)
    if swept_high and reclaim and volume_ok:
        return True, "SELL"
    elif swept_low and reclaim and volume_ok:
        return True, "BUY"
    return False, None

def choose_mode(df):
    adx = compute_adx(df).iloc[-1] if len(df) >= 20 else 20
    return "TREND" if adx >= 20 else "RANGE"

def smart_decision(df, ob, symbol):
    mode = choose_mode(df)
    is_hunt, hunt_side = detect_stop_hunt(df)
    if is_hunt and hunt_side:
        return "STOP_HUNT", hunt_side, {"mode": mode}
    is_zone, zone_price, zone_type = detect_exhaustion_zone(df)
    if is_zone and zone_price is not None:
        STATE["zone"][symbol] = (zone_price, zone_type)
    if symbol in STATE["zone"]:
        zone_price, zone_type = STATE["zone"][symbol]
        if detect_reset(df, zone_price, zone_type) and confirm_reversal(df, ob, zone_type):
            side = "SELL" if zone_type == "TOP" else "BUY"
            return "EXHAUSTION_ENTRY", side, {"mode": mode, "zone": zone_price}
    return None, None, None

# ========== OPPOSING ZONE SMART EXIT ENGINE ==========
def find_nearest_opposing_zone(df, side):
    supports, resistances = get_clustered_zones(df, lookback=80, cluster_pct=0.002)
    price = df['close'].iloc[-1]
    if side == "BUY":
        valid = [r for r in resistances if r > price]
        if valid:
            nearest = min(valid, key=lambda x: x - price)
            return nearest, "RESISTANCE"
    else:
        valid = [s for s in supports if s < price]
        if valid:
            nearest = max(valid, key=lambda x: x)
            return nearest, "SUPPORT"
    return None, None

def compute_opposing_zone_strength(df, ob, atr, side, zone_price, zone_type):
    score = 0
    price = df['close'].iloc[-1]
    dist_pct = abs(price - zone_price) / price
    if dist_pct <= 0.002:
        score += 2
    elif dist_pct <= 0.005:
        score += 1
    last = df.iloc[-1]
    body = abs(last['close'] - last['open'])
    range_ = last['high'] - last['low']
    if range_ > 0:
        if side == "BUY":
            upper_wick = last['high'] - max(last['open'], last['close'])
            if upper_wick > body * 1.5 and price >= zone_price - 0.002*price:
                score += 2
        else:
            lower_wick = min(last['open'], last['close']) - last['low']
            if lower_wick > body * 1.5 and price <= zone_price + 0.002*price:
                score += 2
    vol_state = classify_volume(df)
    if vol_state == "exhaustion":
        score += 1
    elif vol_state == "neutral" and df['volume'].iloc[-1] < df['volume'].rolling(20).mean().iloc[-1] * 0.8:
        score += 1
    if side == "BUY":
        if last['high'] > zone_price and last['close'] < zone_price:
            score += 2
    else:
        if last['low'] < zone_price and last['close'] > zone_price:
            score += 2
    obi = orderbook_imbalance(ob)
    if side == "BUY" and obi < -0.15:
        score += 2
    elif side == "SELL" and obi > 0.15:
        score += 2
    adx_series = compute_adx(df)
    if len(adx_series) >= 2:
        if adx_series.iloc[-1] < adx_series.iloc[-2]:
            score += 1
    return score

def opposing_zone_smart_exit(df, ob, atr, side, entry_price, current_price, state):
    zone_price, zone_type = find_nearest_opposing_zone(df, side)
    if zone_price is None:
        return "HOLD", None
    strength = compute_opposing_zone_strength(df, ob, atr, side, zone_price, zone_type)
    if strength >= 6:
        if not state.get("smart_exit_triggered"):
            return "EXIT", None
    elif strength >= 4:
        if not state.get("smart_partial_done") and not state.get("smart_exit_triggered"):
            return "PARTIAL", None
    elif strength >= 2:
        if not state.get("smart_tightened"):
            return "TIGHTEN", 0.8
    return "HOLD", None

# ========== NARRATIVE + CONTEXT ENGINE v1 ==========
def get_di_components(df, period=14):
    if df is None or len(df) < period*2:
        return None, None, None, 0.0
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = rma(tr, period)
    atr = atr.clip(lower=1e-9)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    plus_di = 100 * rma(plus_dm, period) / (atr + 1e-9)
    minus_di = 100 * rma(minus_dm, period) / (atr + 1e-9)
    adx_series = compute_adx(df, period)
    adx_current = adx_series.iloc[-1] if len(adx_series) > 0 else 20.0
    adx_prev = adx_series.iloc[-2] if len(adx_series) > 1 else adx_current
    adx_slope = adx_current - adx_prev
    return plus_di.iloc[-1], minus_di.iloc[-1], adx_current, adx_slope

def get_vwap_narrative(df):
    vwap = compute_vwap(df)
    price = df['close'].iloc[-1]
    vwap_last = vwap.iloc[-1]
    vwap_prev = vwap.iloc[-2] if len(vwap) > 1 else vwap_last
    distance = (price - vwap_last) / vwap_last if vwap_last != 0 else 0.0
    above = price > vwap_last
    below = price < vwap_last
    prev_above = df['close'].iloc[-2] > vwap_prev if len(df) > 1 else above
    reclaim = (not prev_above) and above
    reject = prev_above and (not above)
    return {
        "vwap": vwap_last,
        "distance": distance,
        "above": above,
        "below": below,
        "reclaim": reclaim,
        "reject": reject,
        "slope": vwap_last - vwap_prev
    }

def compute_enhanced_zone_strength(df, level, zone_type, atr, ob, sweep_detected=False):
    price = df['close'].iloc[-1]
    touches = 0
    rejection_count = 0
    volume_at_touches = []
    for i in range(max(0, len(df)-60), len(df)):
        candle_high = df['high'].iloc[i]
        candle_low = df['low'].iloc[i]
        if zone_type == "support":
            if abs(candle_low - level) < atr * 0.5:
                touches += 1
                if i < len(df)-1:
                    next_close = df['close'].iloc[i+1]
                    if next_close > df['close'].iloc[i]:
                        rejection_count += 1
                        volume_at_touches.append(df['volume'].iloc[i])
        else:
            if abs(candle_high - level) < atr * 0.5:
                touches += 1
                if i < len(df)-1:
                    next_close = df['close'].iloc[i+1]
                    if next_close < df['close'].iloc[i]:
                        rejection_count += 1
                        volume_at_touches.append(df['volume'].iloc[i])
    vol_score = 0.0
    if volume_at_touches:
        avg_vol_touch = sum(volume_at_touches) / len(volume_at_touches)
        avg_vol_overall = df['volume'].iloc[-60:].mean()
        if avg_vol_overall > 0:
            vol_score = min(3.0, avg_vol_touch / avg_vol_overall)
    strength = touches * 1.5 + rejection_count * 2.0 + vol_score
    if sweep_detected:
        strength += 2.0
    last = df.iloc[-1]
    body, range_, upper_wick, lower_wick = candle_metrics(last)
    if zone_type == "support" and lower_wick > body * 1.5 and abs(last['low'] - level) < atr:
        strength += 2.0
    elif zone_type == "resistance" and upper_wick > body * 1.5 and abs(last['high'] - level) < atr:
        strength += 2.0
    return min(10.0, strength)

def classify_market_narrative(df, ob, atr, side, rf_signal):
    reasons = []
    score = 0.0
    plus_di, minus_di, adx, adx_slope = get_di_components(df)
    if plus_di is not None:
        if side == "BUY" and plus_di > minus_di:
            score += 2.0
            reasons.append("DI+ dominance")
        elif side == "SELL" and minus_di > plus_di:
            score += 2.0
            reasons.append("DI- dominance")
        elif abs(plus_di - minus_di) < 5:
            reasons.append("DI tangled")
    if adx_slope > 1.5:
        score += 1.5
        reasons.append(f"ADX rising ({adx_slope:.1f})")
    elif adx_slope < -1.5:
        score -= 1.0
        reasons.append("ADX falling")
    vwap_n = get_vwap_narrative(df)
    if side == "BUY":
        if vwap_n["above"]:
            score += 1.5
            reasons.append("VWAP above")
        elif vwap_n["reclaim"]:
            score += 2.0
            reasons.append("VWAP reclaim")
    else:
        if vwap_n["below"]:
            score += 1.5
            reasons.append("VWAP below")
        elif vwap_n["reject"]:
            score += 2.0
            reasons.append("VWAP reject")
    pools = build_liquidity_pools(df)
    swept_h, swept_l = detect_sweep(df, pools)
    sweep_detected = (side == "BUY" and swept_l) or (side == "SELL" and swept_h)
    if sweep_detected:
        score += 2.5
        reasons.append("Liquidity sweep")
    supports, resistances = get_clustered_zones(df, lookback=80, cluster_pct=0.002)
    zone_strength = 0.0
    if side == "BUY" and supports:
        nearest_sup = max([s for s in supports if s <= df['close'].iloc[-1]], default=None)
        if nearest_sup:
            zone_strength = compute_enhanced_zone_strength(df, nearest_sup, "support", atr, ob, sweep_detected)
            score += zone_strength * 0.5
            reasons.append(f"Zone strength {zone_strength:.1f}")
    elif side == "SELL" and resistances:
        nearest_res = min([r for r in resistances if r >= df['close'].iloc[-1]], default=None)
        if nearest_res:
            zone_strength = compute_enhanced_zone_strength(df, nearest_res, "resistance", atr, ob, sweep_detected)
            score += zone_strength * 0.5
            reasons.append(f"Zone strength {zone_strength:.1f}")
    bos_up, bos_down = detect_bos(df)
    struct_shift = detect_structure_shift(df)
    if (side == "BUY" and (bos_up or struct_shift == "bullish_shift")):
        score += 2.0
        reasons.append("Bullish structure")
    elif (side == "SELL" and (bos_down or struct_shift == "bearish_shift")):
        score += 2.0
        reasons.append("Bearish structure")
    vol_state = classify_volume(df)
    if vol_state in ("expansion", "spike"):
        score += 1.5
        reasons.append("Volume expansion")
    elif vol_state == "exhaustion":
        score -= 1.0
        reasons.append("Volume exhaustion")
    if candle_rejection(df, side):
        score += 1.5
        reasons.append("Rejection candle")
    if detect_displacement(df, side, atr, vol_state, body_atr_threshold=0.8, volume_expansion_required=False):
        score += 1.5
        reasons.append("Displacement")
    if rf_signal == side:
        score += 1.5
        reasons.append("RF aligned")
    if adx is not None and adx < 18 and plus_di is not None and abs(plus_di - minus_di) < 6:
        score = 0
        reasons = ["CHOP market (ADX<18 + DI tangled)"]
    if score >= 9.0:
        classification = "REVERSAL_SNIPER" if (sweep_detected or zone_strength > 5) else "TREND_CONTINUATION"
        confidence = "HIGH"
    elif score >= 7.0:
        classification = "TREND_CONTINUATION" if (bos_up or bos_down or struct_shift) else "ACCUMULATION_LONG" if side == "BUY" else "DISTRIBUTION_SHORT"
        confidence = "MEDIUM"
    elif score >= 5.0:
        classification = "FAKE_BREAKOUT" if not sweep_detected else "LOW_CONFIDENCE"
        confidence = "LOW"
    else:
        classification = "CHOP_NO_TRADE"
        confidence = "NO_TRADE"
    return {
        "classification": classification,
        "confidence": confidence,
        "narrative_score": round(score, 2),
        "reasons": reasons,
        "sweep": sweep_detected,
        "zone_strength": zone_strength,
        "di_dominance": ("BUY" if plus_di > minus_di else "SELL") if plus_di is not None else "NEUTRAL",
        "adx_slope": adx_slope,
        "vwap_reclaim": vwap_n["reclaim"],
        "vwap_reject": vwap_n["reject"]
    }

def detect_market_regime(df):
    if len(df) < 50:
        return "RANGE"
    try:
        adx = compute_adx(df).iloc[-1]
        plus_di, minus_di, _, _ = get_di_components(df)
        vwap_n = get_vwap_narrative(df)
        atr = compute_atr(df).iloc[-1]
        atr_avg = compute_atr(df).rolling(20).mean().iloc[-1] if len(compute_atr(df))>=20 else atr
        atr_ratio = atr / atr_avg if atr_avg else 1.0
        ema20 = ema(df['close'], 20).iloc[-1]
        ema50 = ema(df['close'], 50).iloc[-1] if len(df)>=50 else ema20
        price = df['close'].iloc[-1]
        di_delta = abs(plus_di - minus_di)
        if adx < 18 and di_delta < 6:
            return "CHOP"
        if adx > 20 and di_delta > 5:
            struct = detect_structure_shift(df)
            bullish_aligned = plus_di > minus_di and ema20 > ema50 and price > ema20
            bearish_aligned = minus_di > plus_di and ema20 < ema50 and price < ema20
            if bullish_aligned or bearish_aligned:
                return "TREND"
            if struct == "bullish_shift" and plus_di > minus_di:
                return "TREND"
            if struct == "bearish_shift" and minus_di > plus_di:
                return "TREND"
        if adx > 20 and atr_ratio > 1.4:
            return "EXPANSION"
        if atr_ratio < 0.7 and adx < 25:
            return "COMPRESSION"
        return "RANGE"
    except:
        return "RANGE"

def get_trend_direction(df):
    try:
        plus_di, minus_di, _, _ = get_di_components(df)
        ema20 = ema(df['close'], 20).iloc[-1]
        ema50 = ema(df['close'], 50).iloc[-1] if len(df)>=50 else ema20
        price = df['close'].iloc[-1]
        struct = detect_structure_shift(df)
        if (plus_di > minus_di and ema20 > ema50 and price > ema20) or struct == "bullish_shift":
            return "BULLISH"
        elif (minus_di > plus_di and ema20 < ema50 and price < ema20) or struct == "bearish_shift":
            return "BEARISH"
        return "NEUTRAL"
    except:
        return "NEUTRAL"

def adjust_narrative_confidence(narrative, regime, side, trend_direction):
    orig_conf = narrative["confidence"]
    score = narrative["narrative_score"]
    side_aligned = False
    if (trend_direction == "BULLISH" and side == "BUY") or (trend_direction == "BEARISH" and side == "SELL"):
        side_aligned = True
    final_conf = orig_conf
    final_class = narrative["classification"]
    if regime == "CHOP":
        return "NO_TRADE", "CHOP_NO_TRADE"
    if orig_conf == "NO_TRADE" or score < 5.0:
        return "NO_TRADE", "CHOP_NO_TRADE"
    if regime == "TREND":
        if side_aligned:
            if orig_conf == "HIGH":
                final_conf = "HIGH"
                final_class = "SNIPER"
            elif orig_conf == "MEDIUM":
                final_conf = "MEDIUM"
                final_class = "TREND"
            elif orig_conf == "LOW":
                if score >= 5.0:
                    final_conf = "MEDIUM"
                    final_class = "TREND"
                else:
                    final_conf = "NO_TRADE"
                    final_class = "NO_TRADE"
        else:
            if orig_conf == "HIGH":
                final_conf = "HIGH"
                final_class = "SNIPER"
            else:
                final_conf = "NO_TRADE"
                final_class = "NO_TRADE"
    elif regime in ("EXPANSION", "COMPRESSION"):
        if orig_conf == "HIGH":
            final_conf = "HIGH"
            final_class = "SNIPER"
        else:
            final_conf = "NO_TRADE"
            final_class = "NO_TRADE"
    else:
        if orig_conf == "HIGH":
            final_conf = "HIGH"
            final_class = "SNIPER"
        elif orig_conf == "MEDIUM" and side_aligned:
            final_conf = "NO_TRADE"
            final_class = "NO_TRADE"
        else:
            final_conf = "NO_TRADE"
            final_class = "NO_TRADE"
    return final_conf, final_class

def evaluate_with_narrative(symbol, side, price, atr_val, df, ob, rf_signal, existing_score=0):
    regime = detect_market_regime(df)
    trend_dir = get_trend_direction(df)
    narrative = classify_market_narrative(df, ob, atr_val, side, rf_signal)
    final_conf, final_class = adjust_narrative_confidence(narrative, regime, side, trend_dir)
    narrative["confidence"] = final_conf
    narrative["classification"] = final_class
    narrative["regime"] = regime
    MEMORY[f"last_narrative_{symbol}"] = {**narrative, "timestamp": time.time(), "side": side}
    should_enter = final_conf in ("HIGH", "MEDIUM")
    if not should_enter:
        reason = f"{final_class} ({final_conf}) Regime={regime} Score={narrative['narrative_score']:.1f}"
        MEMORY.setdefault("no_entry_feed", []).append({
            "time": time.time(),
            "symbol": symbol,
            "side": side,
            "reason": reason,
            "score": narrative["narrative_score"]
        })
        if len(MEMORY["no_entry_feed"]) > 20:
            MEMORY["no_entry_feed"] = MEMORY["no_entry_feed"][-20:]
        return False, None, narrative
    STATE["narrative_classification"] = final_class
    STATE["narrative_confidence"] = narrative["narrative_score"]
    STATE["confidence_level"] = final_conf
    return True, final_class, narrative

def narrative_debug():
    debug_data = []
    for key, val in MEMORY.items():
        if key.startswith("last_narrative_"):
            debug_data.append({
                "symbol": key.replace("last_narrative_", ""),
                "side": val.get("side"),
                "classification": val.get("classification"),
                "confidence": val.get("confidence"),
                "score": val.get("narrative_score"),
                "reasons": val.get("reasons"),
                "timestamp": val.get("timestamp")
            })
    radar_candidates = MEMORY.get("radar_top5", [])
    for cand in radar_candidates:
        sym = cand["symbol"]
        if not any(d["symbol"] == sym for d in debug_data):
            df = get_ohlcv_safe(sym, 100)
            if df is not None:
                ob = get_orderbook_cached(sym, 10)
                atr = compute_atr(df).iloc[-1] if len(df) > 14 else df['close'].iloc[-1] * 0.01
                side = "BUY"
                narrative = classify_market_narrative(df, ob, atr, side, None)
                debug_data.append({
                    "symbol": sym,
                    "side": "analysis",
                    "classification": narrative["classification"],
                    "confidence": narrative["confidence"],
                    "score": narrative["narrative_score"],
                    "reasons": narrative["reasons"][:5],
                    "timestamp": time.time()
                })
    return jsonify({"narrative_debug": debug_data})

# ========== SMART INSTITUTIONAL ENTRY ENGINE ==========
def check_institutional_entry(symbol, side, df, ob, atr, price):
    reasons = []
    pools = build_liquidity_pools(df)
    swept_high, swept_low = detect_sweep(df, pools)
    sweep_ok = (side == "BUY" and swept_low) or (side == "SELL" and swept_high)
    if not sweep_ok:
        return False, None, "No liquidity sweep"
    reasons.append("Sweep")
    zones = get_smart_zones(symbol, df, ob)
    zone_ok = False
    zone_price = None
    if side == "BUY":
        if zones["buy_zones"] and zones["buy_zones"][0]["strength"] >= 5:
            zone_price = zones["buy_zones"][0]["price"]
            if abs(price - zone_price) / price < 0.003:
                zone_ok = True
                reasons.append(f"Buy zone {zone_price:.4f} (strength {zones['buy_zones'][0]['strength']})")
    else:
        if zones["sell_zones"] and zones["sell_zones"][0]["strength"] >= 5:
            zone_price = zones["sell_zones"][0]["price"]
            if abs(price - zone_price) / price < 0.003:
                zone_ok = True
                reasons.append(f"Sell zone {zone_price:.4f} (strength {zones['sell_zones'][0]['strength']})")
    if not zone_ok:
        fvg = detect_fvg(df)
        if side == "BUY" and fvg and fvg[0] == "bullish":
            if price >= fvg[1] and price <= fvg[2]:
                zone_ok = True
                reasons.append("Bullish FVG")
        elif side == "SELL" and fvg and fvg[0] == "bearish":
            if price >= fvg[1] and price <= fvg[2]:
                zone_ok = True
                reasons.append("Bearish FVG")
    if not zone_ok:
        ob_level = detect_order_block(df, side)
        if side == "BUY" and ob_level:
            if abs(price - ob_level["low"]) / price < 0.003:
                zone_ok = True
                reasons.append("Bullish OB")
        elif side == "SELL" and ob_level:
            if abs(price - ob_level["high"]) / price < 0.003:
                zone_ok = True
                reasons.append("Bearish OB")
    if not zone_ok:
        return False, None, "No strong zone tap"
    struct_shift = detect_structure_shift(df)
    bos_up, bos_down = detect_bos(df)
    choch_ok = False
    if side == "BUY" and (struct_shift == "bullish_shift" or bos_up):
        choch_ok = True
        reasons.append("Bullish MSS/CHoCH")
    elif side == "SELL" and (struct_shift == "bearish_shift" or bos_down):
        choch_ok = True
        reasons.append("Bearish MSS/CHoCH")
    if not choch_ok:
        return False, None, "No MSS/CHoCH confirmation"
    rejection_ok = candle_rejection(df, side)
    vol_state = classify_volume(df)
    displacement_ok = detect_displacement(df, side, atr, vol_state, body_atr_threshold=0.8, volume_expansion_required=False)
    if not (rejection_ok or displacement_ok):
        return False, None, "No rejection/displacement candle"
    if rejection_ok:
        reasons.append("Rejection candle")
    if displacement_ok:
        reasons.append("Displacement")
    vol_state = classify_volume(df)
    volume_ok = vol_state in ("expansion", "spike")
    if not volume_ok:
        return False, None, "No volume expansion"
    reasons.append(f"Volume {vol_state}")
    adx_series = compute_adx(df)
    if len(adx_series) < 3:
        return False, None, "Insufficient ADX data"
    adx_now = adx_series.iloc[-1]
    adx_prev = adx_series.iloc[-2]
    adx_slope = adx_now - adx_prev
    if not (18 <= adx_now <= 35 and adx_slope > 0):
        return False, None, f"ADX {adx_now:.1f} (slope {adx_slope:.1f}) not in rising 18-35"
    reasons.append(f"ADX rising {adx_now:.1f} (+{adx_slope:.1f})")
    rf = RFEngine(20, 3.5).compute(df)
    if rf["signal"] != side:
        return False, None, f"RF signal {rf['signal']} does not match {side}"
    if abs(rf["distance"]) > 0.003:
        return False, None, f"RF distance {rf['distance']:.4f} too far"
    reasons.append("RF aligned")
    if zone_price:
        move_from_zone = abs(price - zone_price) / zone_price * 100
        if move_from_zone > 0.5:
            return False, None, f"Price moved {move_from_zone:.2f}% from zone, too late"
    last_candle = df.iloc[-1]
    candle_range_pct = (last_candle['high'] - last_candle['low']) / last_candle['close'] * 100
    if candle_range_pct > 1.5 * (atr / price * 100):
        return False, None, "Large displacement candle already occurred, too late"
    reason_str = " | ".join(reasons)
    return True, "INSTITUTIONAL_SNIPER", reason_str

# ========== DECISION FUNCTIONS ==========
def decision_score_v1(df, ob, atr_val, side):
    es, reasons = early_score(df, ob, atr_val, side)
    ctx = detect_liquidity_context(df)
    scenario = "TREND"
    direction = side
    if ctx == "sell_side_taken" and side == "BUY":
        scenario = "REVERSAL"
    elif ctx == "buy_side_taken" and side == "SELL":
        scenario = "REVERSAL"
    total_score = min(10, max(0, es + 2 if scenario == "REVERSAL" else es))
    return total_score, scenario, direction, reasons

def apply_overrides_v1(df, atr_val, score):
    if is_late_move(df, atr_val):
        score = max(0, score - 3)
    return score

def decide_and_execute_v1(symbol, side, total_score, reasons, price, sl, tp1, tp2):
    if total_score < 5:
        return False
    df = get_ohlcv_safe(symbol, 100)
    if df is None:
        return False
    ob = get_orderbook_cached(symbol, 10)
    atr_val = compute_atr(df).iloc[-1] if len(df) > 14 else price * 0.01
    should_enter, classification, narrative = evaluate_with_narrative(symbol, side, price, atr_val, df, ob, side)
    if not should_enter:
        return False
    reason_str = f"DECISION_V1 score={total_score} reasons={reasons} | NARR={narrative['classification']}"
    return execute_entry(side, symbol, price, sl, tp1, tp2, total_score, reason_str, atr_val,
                         trade_type="DECISION_V1", entry_type="V1", classification=classification)

def decision_score(df, ob, atr_val, side):
    vol_state = classify_volume(df)
    scenario = advanced_detect_scenario(df, side, atr_val, vol_state)
    es, reasons = early_score(df, ob, atr_val, side)
    total = es
    if scenario == "TRAP_REVERSAL":
        total += 3
    elif scenario == "TREND_CONTINUATION":
        total += 2
    total = min(10, max(0, total))
    direction = side
    return total, scenario, direction, reasons

def near_key_zone(df, price):
    supports, resistances = get_clustered_zones(df, lookback=80, cluster_pct=0.002)
    for s in supports:
        if abs(price - s) / price < 0.003:
            return True
    for r in resistances:
        if abs(price - r) / price < 0.003:
            return True
    return False

# ========== MONITOR WATCHLIST ==========
def monitor_watchlist():
    watchlist = MEMORY.get("rf_watchlist", [])
    for c in watchlist:
        sym = c["symbol"]
        df = get_ohlcv_safe(sym, 150)
        if df is None or not validate_dataframe(df, 100):
            continue
        ob = get_orderbook_cached(sym, limit=10)
        if ob is not None:
            price = df['close'].iloc[-1]
            atr_val = compute_atr(df).iloc[-1] if len(df) > 14 else price * 0.01
            for side_try in ("BUY", "SELL"):
                should_enter, classification, reason_str = check_institutional_entry(sym, side_try, df, ob, atr_val, price)
                if should_enter:
                    should_enter_narr, final_class, narrative = evaluate_with_narrative(sym, side_try, price, atr_val, df, ob, side_try)
                    if not should_enter_narr:
                        continue
                    sl, tp1, tp2 = compute_sl_tp(price, side_try, "REVERSAL", atr_val, df)
                    ok = execute_entry(side_try, sym, price, sl, tp1, tp2, 85, reason_str, atr_val,
                                       trade_type="INSTITUTIONAL_V3", entry_type="SMART_EARLY", classification=classification)
                    if ok:
                        return True
            decision, dec_side, dec_info = smart_decision(df, ob, sym)
            if decision == "STOP_HUNT":
                price = df['close'].iloc[-1]
                atr_val = compute_atr(df).iloc[-1] if len(df) > 14 else price * 0.01
                should_enter, classification, narrative = evaluate_with_narrative(sym, dec_side, price, atr_val, df, ob, dec_side)
                if not should_enter:
                    continue
                sl, tp1, tp2 = compute_sl_tp(price, dec_side, "REVERSAL", atr_val, df)
                reason_str = f"SMART_STOP_HUNT mode={dec_info.get('mode')} | NARR={narrative['classification']}"
                ok = execute_entry(dec_side, sym, price, sl, tp1, tp2, 8, reason_str, atr_val,
                                   trade_type="SMART", entry_type="STOP_HUNT", classification=classification)
                if ok:
                    return True
            elif decision == "EXHAUSTION_ENTRY":
                price = df['close'].iloc[-1]
                atr_val = compute_atr(df).iloc[-1] if len(df) > 14 else price * 0.01
                should_enter, classification, narrative = evaluate_with_narrative(sym, dec_side, price, atr_val, df, ob, dec_side)
                if not should_enter:
                    continue
                sl, tp1, tp2 = compute_sl_tp(price, dec_side, "REVERSAL", atr_val, df)
                reason_str = f"SMART_EXHAUSTION zone={dec_info.get('zone')} mode={dec_info.get('mode')} | NARR={narrative['classification']}"
                ok = execute_entry(dec_side, sym, price, sl, tp1, tp2, 8, reason_str, atr_val,
                                   trade_type="SMART", entry_type="EXHAUSTION", classification=classification)
                if ok:
                    return True
        rf_engine = RFEngine(period=20, multiplier=3.5)
        rf = rf_engine.compute(df)
        if not rf["triggered"]:
            continue
        side = rf["signal"]
        if side is None:
            continue
        price = df['close'].iloc[-1]
        atr_val = compute_atr(df).iloc[-1] if len(df) > 14 else price * 0.01
        adx_series = compute_adx(df)
        adx_val = adx_series.iloc[-1] if adx_series is not None else 20.0
        volume_state = classify_volume(df)
        should_enter, classification, narrative = evaluate_with_narrative(sym, side, price, atr_val, df, ob, side)
        if not should_enter:
            continue
        if is_late_entry(df, side):
            continue
        ob_v1 = get_orderbook_cached(sym, limit=10)
        if ob_v1 is not None:
            total_v1, scn_v1, dir_v1, reasons_v1 = decision_score_v1(df, ob_v1, atr_val, side)
            total_v1 = apply_overrides_v1(df, atr_val, total_v1)
            if dir_v1 and total_v1 >= 5:
                sl_v1, tp1_v1, tp2_v1 = compute_sl_tp(price, dir_v1,
                                                       "REVERSAL" if scn_v1 in ("TRAP","REVERSAL") else "EARLY_TREND",
                                                       atr_val, df)
                ok = decide_and_execute_v1(sym, dir_v1, total_v1, reasons_v1, price, sl_v1, tp1_v1, tp2_v1)
                if ok:
                    return True
        ob = get_orderbook_cached(sym, limit=10)
        if ob is not None:
            total_score, scenario_name, scenario_dir, all_reasons = decision_score(df, ob, atr_val, side)
            if total_score >= 7:
                sl, tp1, tp2 = compute_sl_tp(price, scenario_dir, "REVERSAL" if scenario_name=="REVERSAL" else "EARLY_TREND", atr_val, df)
                reason_str = f"UNIFIED_SNIPER ({scenario_name}) score={total_score} | NARR={narrative['classification']} | {'+'.join(all_reasons[:3])}"
                ok = execute_entry(scenario_dir, sym, price, sl, tp1, tp2, total_score, reason_str, atr_val,
                                   trade_type="SCENARIO_ENGINE", entry_type="UNIFIED_SNIPER", classification=classification)
                if ok:
                    return True
            elif total_score >= 5:
                sl, tp1, tp2 = compute_sl_tp(price, scenario_dir, "EARLY_TREND", atr_val, df)
                reason_str = f"UNIFIED_EARLY ({scenario_name}) score={total_score} | NARR={narrative['classification']} | {'+'.join(all_reasons[:3])}"
                ok = execute_entry(scenario_dir, sym, price, sl, tp1, tp2, total_score, reason_str, atr_val,
                                   trade_type="SCENARIO_ENGINE", entry_type="UNIFIED_EARLY", classification=classification)
                if ok:
                    return True
        ob = get_orderbook_cached(sym, limit=10)
        if ob is None:
            continue
        else:
            early_score_val, early_reasons = early_score(df, ob, atr_val, side)
            if early_score_val >= 6:
                sl, tp1, tp2 = compute_sl_tp(price, side, "EARLY_TREND", atr_val, df)
                reason_str = f"EARLY_SNIPER ({','.join(early_reasons)}) score={early_score_val} | NARR={narrative['classification']}"
                ok = execute_entry(side, sym, price, sl, tp1, tp2, early_score_val, reason_str, atr_val,
                                   trade_type="EARLY_ENGINE", entry_type="EARLY_SNIPER", classification=classification)
                if ok:
                    return True
            elif early_score_val >= 4:
                sl, tp1, tp2 = compute_sl_tp(price, side, "EARLY_TREND", atr_val, df)
                reason_str = f"EARLY_ENTRY ({','.join(early_reasons)}) score={early_score_val} | NARR={narrative['classification']}"
                ok = execute_entry(side, sym, price, sl, tp1, tp2, early_score_val, reason_str, atr_val,
                                   trade_type="EARLY_ENGINE", entry_type="EARLY_ENTRY", classification=classification)
                if ok:
                    return True
        supports, resistances = get_clustered_zones(df, lookback=120, cluster_pct=0.002)
        location = detect_location(df, price, supports, resistances, threshold=0.003)
        if side == "BUY" and location != "LOW":
            continue
        if side == "SELL" and location != "HIGH":
            continue
        scenario = advanced_detect_scenario(df, side, atr_val, volume_state)
        if scenario == "NONE":
            continue
        decision, adv_class = advanced_decision_engine(scenario, adx_val, volume_state, location)
        if decision != "ENTER":
            continue
        if scenario == "TRAP_REVERSAL":
            leg_class = "REVERSAL"
        elif scenario == "TREND_CONTINUATION":
            leg_class = "EARLY_TREND"
        else:
            leg_class = "TREND_CONTINUATION"
        sl, tp1, tp2 = compute_sl_tp(price, side, leg_class, atr_val, df)
        reason_str = f"ADV SMC {adv_class} | {scenario} | RF {side} | Loc {location} | NARR={narrative['classification']}"
        trade_type = "SMC_ADV"
        TRADE_STATE["zone"] = "support" if side=="BUY" else "resistance"
        TRADE_STATE["location"] = location
        TRADE_STATE["reason"] = [scenario, adv_class, location, narrative['classification']]
        ok = execute_entry(side, sym, price, sl, tp1, tp2, 0, reason_str, atr_val, trade_type, adv_class, classification)
        if ok:
            return True
    return False

# ========== TRADE MANAGEMENT ==========
UPDATE_INTERVAL_SEC = 5

def get_last_price(symbol):
    return get_ticker_safe(symbol)

def update_trailing_simple(current_price):
    if not TRADE_STATE["trail_on"]:
        return False
    entry = TRADE_STATE["entry"]
    side = TRADE_STATE["side"]
    if "trail_stop" not in TRADE_STATE:
        if side == "BUY":
            TRADE_STATE["trail_stop"] = entry * 1.005
        else:
            TRADE_STATE["trail_stop"] = entry * 0.995
    if side == "BUY":
        new_stop = current_price * 0.995
        if new_stop > TRADE_STATE["trail_stop"]:
            TRADE_STATE["trail_stop"] = new_stop
        if current_price <= TRADE_STATE["trail_stop"]:
            return True
    else:
        new_stop = current_price * 1.005
        if new_stop < TRADE_STATE["trail_stop"]:
            TRADE_STATE["trail_stop"] = new_stop
        if current_price >= TRADE_STATE["trail_stop"]:
            return True
    return False

def stop_hit(current_price):
    if not STATE["open"]:
        return False
    side = STATE["side"]
    sl = STATE.get("synthetic_sl", 0.0)
    if side == "BUY" and current_price <= sl:
        return True
    if side == "SELL" and current_price >= sl:
        return True
    return False

def _finalize_trade_with_reality_in_focus(symbol, close_reason=None, trade_id=None):
    """Finalize the focused trade once, only after verified flat evidence."""
    if not STATE.get("open"):
        return None, None
    if not STATE.get("close_verified"):
        log_execution(f"[FINALIZE] Refusing to finalize {symbol}: exchange FLAT/close fill is not verified", "ERROR")
        return None, None

    entry = safe_float(STATE.get("entry"))
    initial_qty = safe_float(STATE.get("initial_qty"), STATE.get("qty", 0.0))
    close_price = safe_float(STATE.get("last_close_fill_price"))
    realized_pnl = safe_float(STATE.get("last_close_realized_pnl"), 0.0)
    pnl_source = str(STATE.get("last_close_pnl_source") or "UNAVAILABLE")
    if close_price <= 0:
        close_price = None
    pnl_known = bool(STATE.get("last_close_pnl_complete", False)) and (pnl_source in {"BINGX_ORDER_REALIZED_PNL", "PAPER_MARK_ESTIMATE"})
    pnl_usdt = realized_pnl if pnl_known else None
    if pnl_known and entry > 0 and initial_qty > 0:
        initial_margin_est = safe_float(STATE.get("initial_margin"), 0.0)
        if initial_margin_est <= 0:
            initial_margin_est = (entry * initial_qty) / max(float(LEVERAGE), 1.0)
        pnl_pct = (realized_pnl / initial_margin_est * 100.0) if initial_margin_est > 0 else 0.0
    else:
        pnl_pct = None

    focused_trade_id = str(trade_id or STATE.get("trade_id") or "")
    trade_id = STATE.get("trade_id")
    reason = str(close_reason or STATE.get("last_close_reason") or "BOT_CLOSE_VERIFIED")
    journal_finalized = True
    already_finalized = False
    if trade_id and INSTITUTIONAL_TRADE_JOURNAL:
        try:
            journal_finalized = INSTITUTIONAL_TRADE_JOURNAL.finalize_once(
                str(trade_id), reason, close_price, pnl_usdt,
                confidence="EXCHANGE_FILL_VERIFIED" if pnl_known else "EXCHANGE_FLAT_PNL_UNAVAILABLE")
        except Exception as exc:
            log_execution(f"[JOURNAL] finalization failed for {trade_id}: {exc}", "ERROR")
            journal_finalized = False
    if trade_id and not journal_finalized:
        existing = INSTITUTIONAL_TRADE_JOURNAL.get(str(trade_id)) if INSTITUTIONAL_TRADE_JOURNAL else None
        if existing and existing.get("finalized"):
            already_finalized = True
            log_execution(f"[FINALIZE] Trade {trade_id} already finalized; local state will still be cleared without duplicate performance update", "WARN")
        else:
            log_execution(f"[FINALIZE] Journal finalization for {trade_id} was not confirmed; clearing only exchange-confirmed flat exposure", "ERROR")

    # An RF-opposite close must leave a durable re-entry block even if the
    # journal finalization itself failed. External close reasons never reach
    # this branch because they carry no trusted RF close context.
    _persist_rf_reentry_block_from_close(symbol, reason, STATE)

    if not already_finalized:
        if pnl_known and pnl_pct is not None:
            PERF["total_pnl_usdt"] += pnl_usdt
            PERF["total_pnl_pct"] += pnl_pct / 100.0
            PERF["trades"] += 1
            if pnl_usdt >= 0:
                PERF["wins"] += 1
                result = "WIN"
            else:
                PERF["losses"] += 1
                result = "LOSS"
            PERF["last_trade"] = {"result": result, "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
                                  "source": pnl_source, "close_reason": reason}
            update_stats(pnl_pct)
            log_execution(f"Trade closed: {result} {pnl_pct:.2f}% ROE-est | USDT: {pnl_usdt:+.4f} | source={pnl_source} | reason={reason}",
                          "SUCCESS" if pnl_usdt >= 0 else "WARN")
        else:
            PERF["trades"] += 1
            PERF["last_trade"] = {"result": "PNL_UNAVAILABLE", "pnl_pct": None,
                                  "source": pnl_source, "close_reason": reason}
            log_execution(f"Trade {symbol} closed and verified; realized PnL unavailable from exchange response (reason={reason})", "WARN")
        try:
            tg_close(symbol, pnl_pct if pnl_known else None,
                     (time.time()-safe_float(STATE.get("entry_time"), time.time()))/60.0,
                     STATE.get("side"), close_reason=reason,
                     pnl_usdt=pnl_usdt if pnl_known else None,
                     entry_price=entry, exit_price=close_price, pnl_source=pnl_source,
                      total_qty=initial_qty, trade_id=trade_id)
        except Exception:
            pass

    # Snapshot unresolved safety gates BEFORE local close-state cleanup.
    # A verified exchange close does not automatically resolve recovery work.
    unresolved_execution = bool(STATE.get("execution_unresolved"))
    close_reconciliation_required = bool(STATE.get("close_reconciliation_required"))
    recovery_hold_active = bool(STATE.get("recovery_hold"))
    recovery_block_entries = bool(globals().get("RECOVERY_BLOCK_ENTRIES", False))

    preserved = {"last_close_diagnostic": STATE.get("last_close_diagnostic"),
                 "last_close_reason": reason, "last_close_fill_price": close_price,
                 "last_close_pnl_source": pnl_source,
                 "last_close_diagnostic": STATE.get("last_close_diagnostic")}
    STATE.update({"open": False, "side": None, "entry": 0.0, "qty": 0.0,
                  "initial_qty": 0.0, "remaining_qty": 0.0, "exchange_qty": 0.0,
                  "current_symbol": None, "trade_id": None, "tp1_hit": False, "tp2_hit": False,
                  "tp1_done": False, "partial_closed": False, "trail_activated": False,
                  "trail_stop": 0.0, "be_done": False, "close_verified": False,
                  "execution_unresolved": unresolved_execution,
                  "close_reconciliation_required": close_reconciliation_required,
                  "recovery_hold": recovery_hold_active, "profit_mode": None, "range_target_price": 0.0,
                  "range_target_move_pct": 0.0, "tp1_filled_qty": 0.0, "tp2_filled_qty": 0.0,
                  "last_close_verified": False, "last_close_order_id": None,
                  "last_close_filled_qty": 0.0, "last_close_realized_pnl": 0.0,
                  "last_close_pnl_complete": True, "bot_close_order_ids": [], "rf_opposite_close": None})
    STATE.update(preserved)
    # This finalizer is gated by close_verified=True: publish the verified flat
    # exposure to the runtime memory consumed by dashboard/synchronization.
    MEMORY["position_status"] = "FLAT"
    MEMORY["current_position"] = None
    TRADE_STATE.update({"in_position": False, "symbol": None, "side": None, "entry": 0.0,
                        "qty": 0.0, "tp1_hit": False, "tp2_hit": False,
                        "trail_on": False, "zone": None, "location": None, "reason": []})

    # This function is entered only after close_verified=True. Release the
    # single-trade lock only if every local/recovery gate agrees the account is
    # flat and no execution uncertainty remains.
    release_reasons = []
    if STATE.get("open"):
        release_reasons.append("STATE_OPEN")
    if TRADE_STATE.get("in_position"):
        release_reasons.append("TRADE_STATE_OPEN")
    if unresolved_execution:
        release_reasons.append("EXECUTION_UNRESOLVED")
    if close_reconciliation_required:
        release_reasons.append("CLOSE_RECONCILIATION_REQUIRED")
    if recovery_hold_active:
        release_reasons.append("RECOVERY_HOLD")
    if recovery_block_entries:
        release_reasons.append("RECOVERY_BLOCK_ENTRIES")
    if not release_reasons:
        if _portfolio_release_reservation(
            focused_trade_id, symbol, f"VERIFIED_FLAT:{reason}"
        ):
            log_execution(
                f"[ENTRY_LOCK_RELEASED] {symbol}: verified full close finalized; slot released",
                "SUCCESS",
            )
        else:
            log_execution(
                f"[PORTFOLIO_RELEASE] focused trade {focused_trade_id} did not own an active reservation",
                "WARN",
            )
    else:
        log_execution(
            f"[ENTRY_LOCK_RETAINED] {symbol}: unresolved safety gates: {','.join(release_reasons)}",
            "ERROR",
        )
    _recompute_portfolio_globals()

    _live_manager.lifecycle_state = TradeLifecycleState.CLOSED
    _recompute_portfolio_globals()
    return pnl_usdt, pnl_pct


def finalize_trade_with_reality(symbol, close_reason=None, trade_id=None):
    """Finalize one trade in isolation, then rebuild portfolio-level views.

    ``trade_id`` is explicit because callers may finalize a non-focused trade
    during portfolio reconciliation. Legacy callers without an id continue to
    finalize the focused trade.
    """
    _portfolio_ensure_test_scope()
    runtime = globals().get("PORTFOLIO_RUNTIME")
    focused_id = str(trade_id or (runtime or None) and getattr(runtime, "_focused_trade_id", None) or "")
    if not focused_id:
        focused_id = _portfolio_trade_id_for_symbol(symbol)
    already_focused = bool(
        runtime is not None
        and getattr(runtime, "_focus_stack", None)
        and getattr(runtime, "_focus_stack", [None])[-1] == focused_id
    )
    if (
        runtime is not None and not already_focused and focused_id
        and focused_id in getattr(runtime, "_positions", {})
    ):
        try:
            with runtime.focus(focused_id):
                result = _finalize_trade_with_reality_in_focus(
                    symbol, close_reason=close_reason, trade_id=focused_id
                )
        except Exception as exc:
            log_execution(
                f"[FINALIZE] {focused_id}: {type(exc).__name__}: {exc}", "ERROR"
            )
            result = None, None
    else:
        result = _finalize_trade_with_reality_in_focus(
            symbol, close_reason=close_reason, trade_id=focused_id
        )
    _recompute_portfolio_globals()
    return result


def apply_50_50_profit_engine(df, idx, price, atr, side, entry, state, roe_pct, trade_state=None):
    # This function is now deprecated; kept for compatibility.
    return "HOLD", state.get("sl", 0.0), state.get("trail_stop", 0.0)

def manage_open_trade_light():
    _live_manager.manage_live_trade()

def get_dashboard_metrics():
    winrate = (PERF["wins"] / PERF["trades"] * 100) if PERF["trades"] else 0
    total_pnl = PERF["total_pnl_pct"] * 100
    last = PERF["last_trade"]
    last_txt = "N/A"
    if last:
        last_pct = last.get("pnl_pct")
        if last_pct is None:
            last_txt = f'{last.get("result", "CLOSED")} (PnL unavailable)'
        else:
            sign = "+" if last_pct >= 0 else ""
            last_txt = f'{last.get("result", "CLOSED")} ({sign}{last_pct:.2f}%)'
    return {
        "winrate": f"{winrate:.1f}%",
        "total_pnl": f"{total_pnl:+.2f}%",
        "total_pnl_usdt": PERF["total_pnl_usdt"],
        "last_trade": last_txt,
        "trades": PERF["trades"],
        "wins": PERF["wins"],
        "losses": PERF["losses"]
    }

def manage_take_profit(price, atr):
    return "HOLD"

def scaling_logic(symbol, df, ind):
    if not STATE["open"] or STATE.get("scale_ins", 0) >= MAX_SCALE_INS:
        return False
    pnl_pct = (df['close'].iloc[-1] - STATE["entry"])/STATE["entry"]*100 if STATE["side"]=="BUY" else (STATE["entry"]-df['close'].iloc[-1])/STATE["entry"]*100
    if pnl_pct < SCALE_IN_PROFIT_PCT:
        return False
    if abs(df['close'].iloc[-1] - STATE["entry"])/STATE["entry"] < 0.005:
        additional_qty = STATE["qty"] * SCALE_IN_SIZE_PCT
        sym_norm = normalize_symbol(symbol)
        try:
            market = ex.market(sym_norm)
            precision = market['precision']['amount']
            qty = math.floor(additional_qty / precision) * precision
        except:
            qty = additional_qty
        if qty > 0:
            order = open_position(STATE["side"], qty, symbol, stop_loss=STATE.get("synthetic_sl", STATE.get("sl")))
            if order:
                STATE["qty"] += qty
                STATE["remaining_qty"] += qty
                STATE["scale_ins"] = STATE.get("scale_ins", 0) + 1
                log_execution(f"Scaled in {qty:.6f} at {df['close'].iloc[-1]:.4f}", "SUCCESS")
                return True
    return False

def council_exit(df, price):
    """Deprecated observer-only compatibility hook; LiveTradeManager owns exits."""
    if STATE.get("open"):
        log_execution("[EXIT_AUTHORITY] council_exit is observer-only; LiveTradeManager is the sole exit authority", "DEBUG",
                      debounce_key="council_exit_observer_only", debounce_sec=300)
    return False

def update_pnl_and_learning(pnl_pct):
    duration = (time.time() - STATE.get("entry_time", time.time())) / 60
    log_execution(f"CLOSE {STATE['current_symbol']} ({STATE['side']}) PnL: {pnl_pct:.2f}% in {duration:.1f} min",
                  "SUCCESS" if pnl_pct >= 0 else "ERROR")
    update_stats(pnl_pct)
    if pnl_pct < 0:
        STATE["consecutive_losses"] += 1
        cooldown = COOLDOWN_MINUTES_DRAWDOWN if STATE["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES else COOLDOWN_MINUTES_LOSS
        STATE["cooldown_until"] = datetime.now(timezone.utc) + timedelta(minutes=cooldown)
    else:
        STATE["consecutive_losses"] = 0

def cooldown_active():
    return STATE["cooldown_until"] and datetime.now(timezone.utc) < STATE["cooldown_until"]

def emergency_kill_switch_active():
    """Compatibility alias; drawdown now pauses entries instead of liquidating."""
    return daily_loss_cooldown_active()

def trailing_stop_new(price, atr):
    return False

# ========== RADAR FUNCTIONS ==========
def fast_market_filter(df):
    price = df['close'].iloc[-1]
    vol_usdt = df['volume'].iloc[-1] * price
    atr = compute_atr(df).iloc[-1]
    if vol_usdt < 1_000_000:
        return False
    if (atr / price) < 0.003:
        return False
    return True

def accumulation_v2(df):
    ema20 = df['close'].ewm(span=20).mean()
    ema50 = df['close'].ewm(span=50).mean()
    compression = abs(ema20.iloc[-1] - ema50.iloc[-1]) < df['close'].iloc[-1] * 0.002
    tight_range = (df['high'].rolling(10).max() - df['low'].rolling(10).min()) < df['close'].iloc[-1] * 0.01
    volume_dry = df['volume'].iloc[-1] < df['volume'].rolling(20).mean().iloc[-1]
    return compression and tight_range and volume_dry

def detect_sweep_simple(df):
    ctx = detect_liquidity_context(df)
    return ctx is not None

def radar_score(df):
    score = 0
    if accumulation_v2(df):
        score += 3
    if volume_pressure_real(df):
        score += 2
    if detect_sweep_simple(df):
        score += 2
    if near_key_zone(df, df['close'].iloc[-1]):
        score += 2
    return score

def rebuild_radar_watchlist():
    symbols = get_usdt_perp_symbols()
    candidates = []
    for sym in symbols[:150]:
        try:
            df = get_ohlcv_safe(sym, 100)
            if df is None or not validate_dataframe(df, 80) or not fast_market_filter(df):
                continue
            score = radar_score(df)
            if score > 0:
                candidates.append({"symbol": sym, "score": score})
        except Exception:
            continue
    candidates.sort(key=lambda x: x["score"], reverse=True)
    MEMORY["radar_watchlist"] = candidates[:30]
    MEMORY["radar_top5"] = candidates[:5]
    log_execution(f"Radar rebuilt: {len(candidates)} candidates, top5: {[c['symbol'] for c in MEMORY['radar_top5']]}", "INFO")

def refresh_radar_watchlist():
    wl = MEMORY.get("radar_watchlist", [])
    updated = []
    for entry in wl:
        sym = entry["symbol"]
        try:
            df = get_ohlcv_safe(sym, 100)
            if df is None or not validate_dataframe(df, 80):
                continue
            score = radar_score(df)
            if score > 0:
                updated.append({"symbol": sym, "score": score})
        except Exception:
            continue
    updated.sort(key=lambda x: x["score"], reverse=True)
    MEMORY["radar_watchlist"] = updated[:30]
    MEMORY["radar_top5"] = updated[:5]
    log_execution(f"Radar refreshed: {len(updated)} symbols remain in watchlist", "INFO")

def radar_entry_scan():
    if not MEMORY.get("radar_top5"):
        return
    now = time.time()
    for entry in MEMORY["radar_top5"]:
        sym = entry["symbol"]
        last = LAST_ENTRY_PER_SYMBOL.get(sym, 0)
        if now - last < RADAR_COOLDOWN_SEC:
            continue
        df = get_ohlcv_safe(sym, 120)
        if df is None or not validate_dataframe(df, 80):
            continue
        price = df['close'].iloc[-1]
        atr_val = compute_atr(df).iloc[-1]
        ob = get_orderbook_cached(sym, limit=10)
        if ob is None:
            continue
        for side_try in ("BUY", "SELL"):
            should_enter, classification, reason_str = check_institutional_entry(sym, side_try, df, ob, atr_val, price)
            if should_enter:
                should_enter_narr, final_class, narrative = evaluate_with_narrative(sym, side_try, price, atr_val, df, ob, side_try)
                if not should_enter_narr:
                    continue
                sl, tp1, tp2 = compute_sl_tp(price, side_try, "REVERSAL", atr_val, df)
                ok = execute_entry(side_try, sym, price, sl, tp1, tp2, 85, reason_str, atr_val,
                                   trade_type="RADAR_INST", entry_type="SMART_EARLY", classification=classification)
                if ok:
                    LAST_ENTRY_PER_SYMBOL[sym] = now
                    return True
        decision, dec_side, dec_info = smart_decision(df, ob, sym)
        if decision == "STOP_HUNT":
            should_enter, classification, narrative = evaluate_with_narrative(sym, dec_side, price, atr_val, df, ob, dec_side)
            if not should_enter:
                continue
            sl, tp1, tp2 = compute_sl_tp(price, dec_side, "REVERSAL", atr_val, df)
            reason_str = f"RADAR_STOP_HUNT mode={dec_info.get('mode')} | NARR={narrative['classification']}"
            ok = execute_entry(dec_side, sym, price, sl, tp1, tp2, 8, reason_str, atr_val,
                               trade_type="RADAR_SMART", entry_type="RADAR_STOP_HUNT", classification=classification)
            if ok:
                LAST_ENTRY_PER_SYMBOL[sym] = now
                return True
        elif decision == "EXHAUSTION_ENTRY":
            should_enter, classification, narrative = evaluate_with_narrative(sym, dec_side, price, atr_val, df, ob, dec_side)
            if not should_enter:
                continue
            sl, tp1, tp2 = compute_sl_tp(price, dec_side, "REVERSAL", atr_val, df)
            reason_str = f"RADAR_EXHAUSTION zone={dec_info.get('zone')} mode={dec_info.get('mode')} | NARR={narrative['classification']}"
            ok = execute_entry(dec_side, sym, price, sl, tp1, tp2, 8, reason_str, atr_val,
                               trade_type="RADAR_SMART", entry_type="RADAR_EXHAUSTION", classification=classification)
            if ok:
                LAST_ENTRY_PER_SYMBOL[sym] = now
                return True
        total_v1, scn_v1, dir_v1, reasons_v1 = decision_score_v1(df, ob, atr_val, "BUY")
        total_v1 = apply_overrides_v1(df, atr_val, total_v1)
        if dir_v1 and total_v1 >= 5:
            should_enter, classification, narrative = evaluate_with_narrative(sym, dir_v1, price, atr_val, df, ob, dir_v1)
            if not should_enter:
                continue
            sl_v1, tp1_v1, tp2_v1 = compute_sl_tp(price, dir_v1,
                                                   "REVERSAL" if scn_v1 in ("TRAP","REVERSAL") else "EARLY_TREND",
                                                   atr_val, df)
            ok = decide_and_execute_v1(sym, dir_v1, total_v1, reasons_v1, price, sl_v1, tp1_v1, tp2_v1)
            if ok:
                LAST_ENTRY_PER_SYMBOL[sym] = now
                return True
        total_score, scenario_name, scenario_dir, all_reasons = decision_score(df, ob, atr_val, "BUY")
        if total_score >= 7:
            should_enter, classification, narrative = evaluate_with_narrative(sym, scenario_dir, price, atr_val, df, ob, scenario_dir)
            if not should_enter:
                continue
            sl, tp1, tp2 = compute_sl_tp(price, scenario_dir, "EARLY_TREND", atr_val, df)
            reason_str = f"RADAR_UNIFIED_SNIPER ({scenario_name}) score={total_score} | NARR={narrative['classification']}"
            ok = execute_entry(scenario_dir, sym, price, sl, tp1, tp2, total_score, reason_str, atr_val,
                               trade_type="RADAR_SCENARIO", entry_type="RADAR_SNIPER", classification=classification)
            if ok:
                LAST_ENTRY_PER_SYMBOL[sym] = now
                return True
        elif total_score >= 5:
            should_enter, classification, narrative = evaluate_with_narrative(sym, scenario_dir, price, atr_val, df, ob, scenario_dir)
            if not should_enter:
                continue
            sl, tp1, tp2 = compute_sl_tp(price, scenario_dir, "EARLY_TREND", atr_val, df)
            reason_str = f"RADAR_UNIFIED_EARLY ({scenario_name}) score={total_score} | NARR={narrative['classification']}"
            ok = execute_entry(scenario_dir, sym, price, sl, tp1, tp2, total_score, reason_str, atr_val,
                               trade_type="RADAR_SCENARIO", entry_type="RADAR_EARLY", classification=classification)
            if ok:
                LAST_ENTRY_PER_SYMBOL[sym] = now
                return True
        for side in ("BUY", "SELL"):
            es, reasons = early_score(df, ob, atr_val, side)
            if es >= 6:
                should_enter, classification, narrative = evaluate_with_narrative(sym, side, price, atr_val, df, ob, side)
                if not should_enter:
                    continue
                sl, tp1, tp2 = compute_sl_tp(price, side, "EARLY_TREND", atr_val, df)
                reason_str = f"RADAR_EARLY ({','.join(reasons)}) score={es} | NARR={narrative['classification']}"
                ok = execute_entry(side, sym, price, sl, tp1, tp2, es, reason_str, atr_val,
                                   trade_type="RADAR_EARLY", entry_type="RADAR_SNIPER", classification=classification)
                if ok:
                    LAST_ENTRY_PER_SYMBOL[sym] = now
                    return True
    return False

def compute_zone_strength(df, level, zone_type, atr, ob):
    price = df['close'].iloc[-1]
    dist_pct = abs(price - level) / price
    touch_indices = []
    for i in range(max(0, len(df)-30), len(df)):
        candle_high = df['high'].iloc[i]
        candle_low = df['low'].iloc[i]
        if (zone_type == "support" and abs(candle_low - level) < atr) or \
           (zone_type == "resistance" and abs(candle_high - level) < atr):
            touch_indices.append(i)
    vol_strength = 0
    if touch_indices:
        volumes = df['volume'].iloc[touch_indices]
        avg_vol = volumes.mean()
        overall_avg = df['volume'].iloc[-30:].mean() if len(df) >= 30 else df['volume'].mean()
        vol_strength = min(3.0, avg_vol / overall_avg) if overall_avg > 0 else 0
    reaction_count = 0
    for idx in touch_indices:
        if idx < len(df)-1:
            next_close = df['close'].iloc[idx+1]
            if (zone_type == "support" and next_close > df['close'].iloc[idx]) or \
               (zone_type == "resistance" and next_close < df['close'].iloc[idx]):
                reaction_count += 1
    reaction_score = min(3.0, reaction_count)
    liquidity_score = 0
    if ob:
        obi = orderbook_imbalance(ob)
        if zone_type == "support" and obi > 0.1:
            liquidity_score = 2
        elif zone_type == "resistance" and obi < -0.1:
            liquidity_score = 2
        elif abs(obi) > 0.05:
            liquidity_score = 1
    inst_score = 0
    bos_up, bos_down = detect_bos(df, lookback=5)
    struct_shift = detect_structure_shift(df)
    if zone_type == "support" and (bos_up or struct_shift == "bullish_shift"):
        inst_score = 2
    elif zone_type == "resistance" and (bos_down or struct_shift == "bearish_shift"):
        inst_score = 2
    rejection_score = 0
    if len(df) >= 1:
        last = df.iloc[-1]
        body, range_, upper_wick, lower_wick = candle_metrics(last)
        if zone_type == "support" and lower_wick > body * 1.5 and abs(last['low'] - level) < atr:
            rejection_score = 2
        elif zone_type == "resistance" and upper_wick > body * 1.5 and abs(last['high'] - level) < atr:
            rejection_score = 2
    total = vol_strength + reaction_score + liquidity_score + inst_score + rejection_score
    strength = min(10.0, total * 10 / 10)
    return round(strength, 1), {
        "vol_strength": round(vol_strength, 1),
        "reaction_count": reaction_count,
        "liquidity_score": liquidity_score,
        "institutional_score": inst_score,
        "rejection_score": rejection_score
    }

def build_smart_zone_map(symbol, df, ob=None):
    atr = compute_atr(df).iloc[-1]
    supports, resistances = get_clustered_zones(df, lookback=120, cluster_pct=0.002)
    buy_zones = []
    for sup in supports:
        strength, details = compute_zone_strength(df, sup, "support", atr, ob)
        buy_zones.append({"price": sup, "strength": strength, "details": details, "type": "support"})
    sell_zones = []
    for res in resistances:
        strength, details = compute_zone_strength(df, res, "resistance", atr, ob)
        sell_zones.append({"price": res, "strength": strength, "details": details, "type": "resistance"})
    buy_zones.sort(key=lambda x: x["strength"], reverse=True)
    sell_zones.sort(key=lambda x: x["strength"], reverse=True)
    return {"buy_zones": buy_zones, "sell_zones": sell_zones}

def get_smart_zones(symbol, df, ob):
    key = f"smart_zones_{symbol}"
    cached = MEMORY.get(key)
    if cached and time.time() - cached.get("ts", 0) < 90:
        return cached["data"]
    zones = build_smart_zone_map(symbol, df, ob)
    MEMORY[key] = {"data": zones, "ts": time.time()}
    return zones

# ========== FLASK DASHBOARD ==========
app = Flask(__name__)

def update_position_dashboard(symbol, side, entry, qty, pnl=0.0):
    DASHBOARD_STATE["position"] = {
        "symbol": symbol,
        "side": side,
        "entry": round(entry, 4),
        "qty": qty,
        "pnl": round(pnl, 2),
        "sl": round(STATE.get("synthetic_sl", 0), 4),
        "tp1": round(STATE.get("synthetic_tp1", 0), 4),
        "tp2": round(STATE.get("tp2_price", 0), 4),
        "tp1_done": STATE.get("tp1_hit", False),
        "trailing_active": STATE.get("trail_activated", False),
        "regime": MEMORY.get("regime", "UNKNOWN"),
        "trade_type": STATE.get("trade_type", "N/A"),
        "entry_type": STATE.get("entry_type", "N/A"),
        "classification": STATE.get("classification", "N/A"),
        "location": STATE.get("location", "N/A"),
        "zone": STATE.get("zone_info", "N/A"),
        "score": STATE.get("trade_score", 0),
        "narrative_classification": STATE.get("narrative_classification", ""),
        "narrative_confidence": STATE.get("narrative_confidence", 0.0),
        "confidence_level": STATE.get("confidence_level", ""),
        "current_confidence": STATE.get("current_confidence", 50.0),
        "market_regime": STATE.get("market_regime", "UNKNOWN"),
        "continuation_pressure": STATE.get("continuation_pressure", 50),
        "trade_state": STATE.get("trade_state", "RANGE_CHOP"),
        "trail_multiplier": STATE.get("smart_trail_mult", 1.5),
        "delay_tp1": STATE.get("delay_tp1", False)
    }

def clear_position_dashboard():
    DASHBOARD_STATE["position"] = None

def render_live_supervisor_panel():
    return """
    <div id="rf-live-panel" style="display:none;" class="rf-live-supervisor">
      <div class="rf-live-header">
        <span class="rf-live-title">🧠 RF v28.1 Professional Trade Mgmt</span>
        <span id="rf-live-status-badge" class="rf-live-pill rf-live-pill-idle">⚡ ADAPTIVE LIVE SYNC</span>
      </div>
      <div class="rf-live-grid">
        <div class="rf-live-card"><div class="rf-live-metric-icon">💰</div><div class="rf-live-metric-label">Entry</div><div class="rf-live-metric-value" id="rf-sup-entry">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">📈</div><div class="rf-live-metric-label">Mark Price</div><div class="rf-live-metric-value" id="rf-sup-mark">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">⚡</div><div class="rf-live-metric-label">ROE%</div><div class="rf-live-metric-value" id="rf-sup-roe">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">💵</div><div class="rf-live-metric-label">Unrealized PnL</div><div class="rf-live-metric-value" id="rf-sup-upnl">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">📊</div><div class="rf-live-metric-label">ADX</div><div class="rf-live-metric-value" id="rf-sup-adx">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🟢</div><div class="rf-live-metric-label">DI+</div><div class="rf-live-metric-value" id="rf-sup-dip">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🔴</div><div class="rf-live-metric-label">DI-</div><div class="rf-live-metric-value" id="rf-sup-dim">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🔥</div><div class="rf-live-metric-label">Continuation</div><div class="rf-live-metric-value" id="rf-sup-cont">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🧠</div><div class="rf-live-metric-label">Thesis Failure</div><div class="rf-live-metric-value" id="rf-sup-fail">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">✅</div><div class="rf-live-metric-label">Confidence</div><div class="rf-live-metric-value" id="rf-sup-conf">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🎯</div><div class="rf-live-metric-label">TP1</div><div class="rf-live-metric-value" id="rf-sup-tp1">❌</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🎯</div><div class="rf-live-metric-label">TP2</div><div class="rf-live-metric-value" id="rf-sup-tp2">❌</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">⚡</div><div class="rf-live-metric-label">Trailing</div><div class="rf-live-metric-value" id="rf-sup-trail">❌</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🧠</div><div class="rf-live-metric-label">Personality</div><div class="rf-live-metric-value" id="rf-sup-personality">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">🏦</div><div class="rf-live-metric-label">Institutional Flow</div><div class="rf-live-metric-value" id="rf-sup-flow">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">⚙️</div><div class="rf-live-metric-label">Trade State</div><div class="rf-live-metric-value" id="rf-sup-state">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">📏</div><div class="rf-live-metric-label">Trail Mult</div><div class="rf-live-metric-value" id="rf-sup-trail-mult">-</div></div>
        <div class="rf-live-card"><div class="rf-live-metric-icon">⏰</div><div class="rf-live-metric-label">Delay TP1</div><div class="rf-live-metric-value" id="rf-sup-delay-tp1">❌</div></div>
      </div>
      <div class="rf-live-status-row">
        <span id="rf-pill-thesis" class="rf-live-pill rf-live-pill-active">🧠 THESIS ACTIVE</span>
        <span id="rf-pill-trail" class="rf-live-pill">⚡ TRAILING OFF</span>
        <span id="rf-pill-flow" class="rf-live-pill">🏦 NEUTRAL</span>
        <span id="rf-pill-reclaim" class="rf-live-pill">🟢 RECLAIM LOW</span>
      </div>
    </div>
    <style>
    .rf-live-supervisor {
      background: linear-gradient(145deg, #0f1724 0%, #0a0f17 100%);
      border-radius: 20px;
      padding: 20px;
      margin-bottom: 20px;
      border: 1px solid #2c3e50;
    }
    .rf-live-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 18px;
      padding-bottom: 12px;
      border-bottom: 1px solid #2c3e50;
    }
    .rf-live-title {
      font-size: 18px;
      font-weight: bold;
      color: #00ffa6;
    }
    .rf-live-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .rf-live-card {
      background: #111827;
      border-radius: 14px;
      padding: 10px;
      text-align: center;
      transition: 0.2s;
    }
    .rf-live-metric-icon {
      font-size: 22px;
      margin-bottom: 4px;
    }
    .rf-live-metric-label {
      font-size: 11px;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .rf-live-metric-value {
      font-size: 15px;
      font-weight: bold;
      color: #e6edf3;
      margin-top: 4px;
    }
    .rf-live-status-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .rf-live-pill {
      background: #111827;
      padding: 6px 14px;
      border-radius: 30px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid #2c3e50;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .rf-live-pill-active {
      background: rgba(0, 255, 166, 0.1);
      border-color: #00ffa6;
      color: #00ffa6;
    }
    .rf-live-pill-failed {
      background: rgba(255, 77, 77, 0.1);
      border-color: #ff4d4d;
      color: #ff4d4d;
    }
    .rf-live-pill-trail {
      background: rgba(0, 255, 166, 0.1);
      border-color: #00ffa6;
    }
    .rf-live-pill-flow-buy {
      background: rgba(0, 255, 166, 0.1);
      border-color: #00ffa6;
      color: #00ffa6;
    }
    .rf-live-pill-flow-sell {
      background: rgba(255, 77, 77, 0.1);
      border-color: #ff4d4d;
      color: #ff4d4d;
    }
    .rf-live-pill-risk-low {
      color: #00ffa6;
    }
    .rf-live-pill-risk-mid {
      color: #ffc800;
    }
    .rf-live-pill-risk-high {
      color: #ff4d4d;
    }
    </style>
    """

@app.route("/")
def dashboard():
    rf_items = MEMORY.get("rf_dashboard", [])[:20]
    rf_html = "".join([f"<div>{item['icon']} {item['symbol']} | {item['status']} | score={item['score']:.2f} | ADX={item['adx']:.1f} | RSI={item['rsi']:.1f}</div>" for item in rf_items])

    scanner_buy = MEMORY.get("scanner_v2_buy", [])
    scanner_sell = MEMORY.get("scanner_v2_sell", [])
    buy_html = ""
    for b in scanner_buy:
        icon = "🔥" if b["score"] >= 7 else "⚡"
        sm = b.get("smart_money", {})
        mom = b.get("momentum", {})
        sm_str = f"{sm.get('bias_detailed', sm.get('bias', '?'))} "
        if sm.get("dominant"): sm_str += "🧠"
        mom_str = ""
        if mom.get("expansion"): mom_str += "🚀"
        if mom.get("decay"): mom_str += "📉"
        buy_html += f"<div>{icon} {b['symbol']} | Score: {b['score']}<br>📍 {b['location']} | RF: {b['rf_prox']}% | Vol: {'Spike' if b['volume_spike'] else 'Norm'} | Rej: {'✔' if b['rejection'] else '✖'}<br>🏦 {sm_str} | 📈 {mom_str}</div><hr>"
    sell_html = ""
    for s in scanner_sell:
        icon = "🔥" if s["score"] >= 7 else "⚡"
        sm = s.get("smart_money", {})
        mom = s.get("momentum", {})
        sm_str = f"{sm.get('bias_detailed', sm.get('bias', '?'))} "
        if sm.get("dominant"): sm_str += "🧠"
        mom_str = ""
        if mom.get("expansion"): mom_str += "🚀"
        if mom.get("decay"): mom_str += "📉"
        sell_html += f"<div>{icon} {s['symbol']} | Score: {s['score']}<br>📍 {s['location']} | RF: {s['rf_prox']}% | Vol: {'Spike' if s['volume_spike'] else 'Norm'} | Rej: {'✔' if s['rejection'] else '✖'}<br>🏦 {sm_str} | 📈 {mom_str}</div><hr>"
    scanner_v2_section = f"""
    <div class="section smart-layer"><div class="title">📡 SMART SCANNER v2 (Ranked)</div>
    <div style="display:flex; gap:20px;">
        <div style="flex:1; background:#0f1724; padding:12px; border-radius:8px;"><b>🟢 TOP 10 BUY</b><br>{buy_html or 'No candidates'}</div>
        <div style="flex:1; background:#0f1724; padding:12px; border-radius:8px;"><b>🔴 TOP 10 SELL</b><br>{sell_html or 'No candidates'}</div>
    </div>
    </div>
    """

    decision_panel_html = """
    <div id="decision-panel" style="padding:12px; border:1px solid #2c3e50; margin-bottom:16px; border-radius:8px; background:#0a0c10;">
      <h3>🧠 UNIFIED TRADE MANAGEMENT LOG · TP1 / TP2 / STRICT EXIT</h3>
      <div id="decision-summary" style="font-size:12px;color:#9ca3af;margin:4px 0 8px;">Waiting for management events...</div>
      <div id="decision-list" style="max-height:400px; overflow-y:auto; font-size:13px;"></div>
    </div>
    """

    watchlist_panel_html = """
    <div class="section smart-layer">
      <div class="title">👁 WATCHLIST / ACTIVE CANDIDATES</div>
      <div id="watchlist-panel" style="max-height:400px; overflow-y:auto; font-size:13px; background:#0f1724; padding:10px; border-radius:8px;">
        Loading...
      </div>
    </div>
    """

    no_entry_feed_section_html = """
    <div class="section smart-layer"><div class="title">🚫 WHY NO ENTRY (Last 5)</div>
    <div id="no-entry-feed" class="card" style="font-size:12px;"></div>
    </div>
    """

    free_balance_card = '<div class="card">FREE BALANCE<div id="free_bal">-</div><div id="avail_margin">-</div></div>'

    continuation_panel_html = """
    <div class="section smart-layer">
      <div class="title">📈 CONTINUATION ENGINE</div>
      <div id="continuation-panel" class="card" style="font-size:12px;"></div>
    </div>
    """

    thesis_panel_html = """
    <div class="section smart-layer">
      <div class="title">🧠 TRADE THESIS</div>
      <div id="thesis-panel" class="card" style="font-size:12px;"></div>
    </div>
    """

    confidence_regime_panel = """
    <div class="section smart-layer">
      <div class="title">📊 CONFIDENCE & REGIME</div>
      <div class="grid">
        <div class="card">Current Confidence<div id="current_conf">-</div></div>
        <div class="card">Market Regime<div id="market_regime">-</div></div>
        <div class="card">Continuation Pressure<div id="cont_pressure">-</div></div>
        <div class="card">Thesis Failure Score<div id="thesis_failure">-</div></div>
      </div>
    </div>
    """

    flow_section_html = """
    <div class="section smart-layer">
      <div class="title">🧠 Institutional Flow Intelligence</div>
      <div class="rf-flow-grid">
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Banker Pressure</div><div id="flow-banker" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Retail Pressure</div><div id="flow-retail" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Hot Money</div><div id="flow-hot" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Institutional Bias</div><div id="flow-bias" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Flow Alignment</div><div id="flow-align" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Distribution Risk</div><div id="flow-dist" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Momentum Health</div><div id="flow-mom-health" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Continuation Strength</div><div id="flow-cont-str" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Exhaustion Risk</div><div id="flow-exh-risk" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Climax Risk</div><div id="flow-climax" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Greed State</div><div id="flow-greed" class="rf-flow-value">-</div></div>
        <div class="rf-flow-card"><div class="rf-flow-metric-label">Smart Money Dominant</div><div id="flow-dom" class="rf-flow-value">-</div></div>
      </div>
    </div>
    <style>
    .rf-flow-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-top: 8px;
    }
    .rf-flow-card {
      background: #111827;
      border-radius: 12px;
      padding: 8px;
      text-align: center;
    }
    .rf-flow-metric-label {
      font-size: 11px;
      color: #9ca3af;
      text-transform: uppercase;
    }
    .rf-flow-value {
      font-size: 16px;
      font-weight: bold;
      margin-top: 4px;
      color: #e6edf3;
    }
    </style>
    """

    supervisor_panel_html = render_live_supervisor_panel()

    html = f"""
<!DOCTYPE html>
<html><head><title>RF v28.1 Professional Trade Mgmt</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
body{{background:#0b0f14;color:#e6edf3;font-family:Consolas;margin:0}}
.header{{padding:14px 16px;background:#111827;color:#00ff9f;font-size:22px;}}
.section{{padding:12px 14px;border-bottom:1px solid #1f2937}}
.title{{color:#9ca3af;margin-bottom:6px}}
.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}}
.card{{background:#111827;border-radius:10px;padding:10px}}
.green{{color:#00ffa6}}
.red{{color:#ff4d4d}}
.log,.err{{max-height:220px;overflow:auto;white-space:pre-wrap;font-size:12px}}
.btn{{background:#2d3748;border:none;color:white;padding:8px 16px;margin:4px;border-radius:6px;cursor:pointer}}
.btn-buy{{background:#0f7b3a}}
.btn-sell{{background:#9b2c2c}}
.btn-close{{background:#4a5568}}
.smart-layer{{background:#0f1724;margin-top:12px;border-radius:8px}}
.position-details{{font-size:14px}}
</style>
</head>
<body>
<div class="header">🔥 RF v28.1 Professional Trade Mgmt (20x)</div>
{decision_panel_html}
{scanner_v2_section}
<div class="section smart-layer"><div class="title">🌐 SCANNER 1 — PROFESSIONAL GLOBAL DISCOVERY (RF + SMART SCANNER V2)</div>
<div id="discovery-scanner-panel" class="card" style="font-size:12px;">Waiting for initial market scan...</div>
</div>
<div class="section smart-layer"><div class="title">🧭 EMA9 + EMA50 / SESSION VWAP (CLOSED CANDLES · ENTRY GATE)</div>
<div id="proximity-scanner-panel" class="card" style="font-size:12px;max-height:320px;overflow-y:auto;">Waiting for first rotating batch...</div>
</div>
{supervisor_panel_html}
{flow_section_html}
{continuation_panel_html}
{thesis_panel_html}
{confidence_regime_panel}
<div class="section"><div class="title">💰 ACCOUNT & PERFORMANCE</div><div class="grid">
<div class="card">Balance<div id="bal">-</div></div>
{free_balance_card}
<div class="card">Mode<div id="mode">-</div></div>
<div class="card">Trades<div id="trades">0</div></div>
<div class="card">Wins<div id="wins" class="green">0</div></div>
<div class="card">Losses<div id="losses" class="red">0</div></div>
<div class="card">WinRate<div id="winrate">0%</div></div>
</div></div>
<div class="section"><div class="title">📊 TOTAL P&L & LAST TRADE</div><div class="grid">
<div class="card">Total PnL%<div id="total_pnl" class="green">0%</div></div>
<div class="card">Total PnL USDT<div id="total_pnl_usdt">0.00</div></div>
<div class="card">Last Trade<div id="last_trade">N/A</div></div>
</div></div>
<div class="section"><div class="title">📍 LIVE POSITION</div>
<div id="pos" class="card"></div>
</div>
<div class="section smart-layer"><div class="title">📡 TOP RF OPPORTUNITIES</div>
<div id="top5" class="card"></div>
</div>
<div class="section smart-layer"><div class="title">📡 RF SIGNALS (Trigger Candidates)</div>
<div id="rfSignals" class="card">{rf_html}</div>
</div>
{watchlist_panel_html}
<div class="section"><div class="title">📜 EXECUTION LOG</div><div id="logs" class="card log"></div></div>
<div class="section"><div class="title">🚨 SYSTEM ERRORS</div><div id="errors" class="card err"></div></div>
{no_entry_feed_section_html}
<div class="section"><div class="title">🎮 MANUAL CONTROLS</div>
<button class="btn btn-buy" onclick="manualTrade('BUY')">BUY</button>
<button class="btn btn-sell" onclick="manualTrade('SELL')">SELL</button>
<button class="btn btn-close" onclick="manualClose()">CLOSE</button>
</div>
<div class="section smart-layer"><div class="title">📡 MONITORING</div><div class="grid">
<div class="card">Regime<div id="regimeLabel">-</div></div>
<div class="card">Scanned<div id="scanned">0</div></div>
<div class="card">Last Scan<div id="lastScan">-</div></div>
</div></div>
<div class="section smart-layer"><div class="title">🩺 SYSTEM HEALTH</div><div class="grid">
<div class="card">API Status<div id="apiStatus">-</div></div>
<div class="card">Errors<div id="errCount">0</div></div>
<div class="card">Bot Status<div id="botStatus">-</div></div>
</div></div>
<script>
let lastFetch = 0;
let cachedData = null;
function uniEsc(value) {{
    return String(value ?? "-").replace(/[&<>"']/g, ch => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[ch]));
}}
async function fetchData() {{
    const now = Date.now();
    if (cachedData && (now - lastFetch) < 6000) {{
        updateUI(cachedData);
        return;
    }}
    lastFetch = now;
    try {{
        const r = await fetch('/data');
        const d = await r.json();
        cachedData = d;
        updateUI(d);
    }} catch(e) {{ console.error(e); }}
}}
function updateUI(d) {{
    document.getElementById("bal").innerText = d.balance.toFixed(2);
    document.getElementById("free_bal").innerText = "$" + d.free_balance.toFixed(2);
    document.getElementById("avail_margin").innerText = "Margin: " + d.avail_margin.toFixed(2);
    document.getElementById("mode").innerText = d.mode;
    document.getElementById("trades").innerText = d.stats.trades;
    document.getElementById("wins").innerText = d.stats.wins;
    document.getElementById("losses").innerText = d.stats.losses;
    document.getElementById("winrate").innerText = d.stats.win_rate.toFixed(1)+"%";
    document.getElementById("total_pnl").innerHTML = d.total_pnl || "0%";
    document.getElementById("total_pnl_usdt").innerHTML = d.total_pnl_usdt ? d.total_pnl_usdt.toFixed(2) : "0.00";
    document.getElementById("last_trade").innerText = d.last_trade || "N/A";
    if(d.position) {{
        let pnlClass = d.position.pnl >= 0 ? "green" : "red";
        document.getElementById("pos").innerHTML = `
            <div><b>${{d.position.symbol}}</b> | ${{d.position.side}} | ${{d.position.entry_type}} (${{d.position.classification}})</div>
            <div>Entry: ${{d.position.entry}} | PnL: <span class="${{pnlClass}}">${{d.position.pnl}}%</span></div>
            <div>SL: ${{d.position.sl}} | TP1: ${{d.position.tp1}} | TP2: ${{d.position.tp2}}</div>
            <div>TP1 done: ${{d.position.tp1_done}} | Trailing: ${{d.position.trailing_active}}</div>
            <div>Location: ${{d.position.location}} | Zone: ${{d.position.zone}}</div>
            <div>Narrative: ${{d.position.narrative_classification}} (Conf: ${{d.position.narrative_confidence}}) | Conf Level: ${{d.position.confidence_level}}</div>
            <div>Current Confidence: ${{d.position.current_confidence}} | Regime: ${{d.position.market_regime}} | Cont. Pressure: ${{d.position.continuation_pressure}}</div>
            <div>Trade State: ${{d.position.trade_state}} | Trail Mult: ${{d.position.trail_multiplier}} | Delay TP1: ${{d.position.delay_tp1}}</div>
            <div>Management: ${{d.position.management_mode}} | Selected Target ROE: ${{d.position.selected_target_gross_roe_pct ?? "-"}}% | Net Risk: ${{d.position.entry_risk_plan?.estimated_net_loss ?? "-"}} USDT | R/R(net): ${{d.position.entry_risk_plan?.risk_reward_net ?? "-"}} | Liq. Dist.: ${{d.position.entry_risk_plan?.idealized_liquidation_distance_pct ?? "-"}}% | Mode Changes: ${{d.position.management_mode_history?.length || 0}}</div>
        `;
    }} else {{
        document.getElementById("pos").innerHTML = "No active trade";
    }}
    if(d.live_trade_mode && d.supervisor) {{
        const sup = d.supervisor;
        document.getElementById("rf-sup-entry").innerText = sup.entry_price?.toFixed(4) || "-";
        document.getElementById("rf-sup-mark").innerText = sup.mark_price?.toFixed(4) || "-";
        document.getElementById("rf-sup-roe").innerHTML = sup.roe_pct?.toFixed(2) + "%";
        document.getElementById("rf-sup-upnl").innerText = sup.unrealized_pnl?.toFixed(2) || "-";
        document.getElementById("rf-sup-adx").innerText = sup.adx?.toFixed(1) || "-";
        document.getElementById("rf-sup-dip").innerText = sup.di_plus?.toFixed(1) || "-";
        document.getElementById("rf-sup-dim").innerText = sup.di_minus?.toFixed(1) || "-";
        document.getElementById("rf-sup-cont").innerText = sup.continuation_pressure || "-";
        document.getElementById("rf-sup-fail").innerText = sup.thesis_failure_score || "-";
        document.getElementById("rf-sup-conf").innerText = sup.current_confidence?.toFixed(1) || "-";
        document.getElementById("rf-sup-tp1").innerHTML = sup.tp1_hit ? "✅" : "❌";
        document.getElementById("rf-sup-tp2").innerHTML = sup.tp2_hit ? "✅" : "❌";
        document.getElementById("rf-sup-trail").innerHTML = sup.trailing_active ? "✅" : "❌";
        document.getElementById("rf-sup-personality").innerText = sup.trade_personality || "NEUTRAL";
        document.getElementById("rf-sup-flow").innerText = sup.institutional_flow || "NEUTRAL";
        document.getElementById("rf-sup-state").innerText = sup.trade_state || "RANGE_CHOP";
        document.getElementById("rf-sup-trail-mult").innerText = sup.trail_multiplier || "1.5";
        document.getElementById("rf-sup-delay-tp1").innerHTML = sup.delay_tp1 ? "✅" : "❌";
        const reclaim = sup.reclaim_risk || 0;
        let reclaimClass = "rf-live-pill-risk-low";
        if (reclaim > 0.6) reclaimClass = "rf-live-pill-risk-high";
        else if (reclaim > 0.3) reclaimClass = "rf-live-pill-risk-mid";
        document.getElementById("rf-pill-reclaim").innerHTML = `🟢 RECLAIM ${{(reclaim*100).toFixed(0)}}%`;
        document.getElementById("rf-pill-reclaim").className = `rf-live-pill ${{reclaimClass}}`;
        const trailActive = sup.trailing_active;
        document.getElementById("rf-pill-trail").innerHTML = trailActive ? "⚡ TRAILING ON" : "⚡ TRAILING OFF";
        document.getElementById("rf-pill-trail").className = trailActive ? "rf-live-pill rf-live-pill-trail" : "rf-live-pill";
        document.getElementById("rf-live-panel").style.display = "block";
    }} else {{
        document.getElementById("rf-live-panel").style.display = "none";
    }}
    if(d.continuation_probability) {{
        let color = d.continuation_probability >= 0.65 ? "green" : (d.continuation_probability >= 0.5 ? "yellow" : "red");
        document.getElementById("continuation-panel").innerHTML = `
            <div>Continuation: <span style="color:${{color}};">${{(d.continuation_probability*100).toFixed(1)}}%</span></div>
            <div>Hold Quality: ${{d.hold_quality}}</div>
            <div>Trend Strength: ${{d.trend_strength}}</div>
            <div>Counter Pressure: ${{d.counter_pressure}}</div>
            <div>Reclaim Risk: ${{d.reclaim_risk}}</div>
            <div>Reasons: ${{(d.continuation_reasons || []).join(", ")}}</div>
        `;
    }}
    if(d.trade_thesis) {{
        let t = d.trade_thesis;
        document.getElementById("thesis-panel").innerHTML = `
            <div>Status: ${{t.current_status || "ACTIVE"}}</div>
            <div>Confidence: ${{t.confidence}}</div>
            <div>Continuation Prob: ${{t.continuation_probability}}</div>
            <div>Exhaustion Prob: ${{t.exhaustion_probability}}</div>
            <div>Entry Reasons: ${{(t.entry_reason || []).join(", ")}}</div>
            <div>Risks: ${{(t.risk_factors || []).join(", ")}}</div>
        `;
    }}
    document.getElementById("current_conf").innerHTML = (d.current_confidence || 50).toFixed(1);
    document.getElementById("market_regime").innerHTML = d.market_regime || "UNKNOWN";
    document.getElementById("cont_pressure").innerHTML = d.continuation_pressure || 50;
    document.getElementById("thesis_failure").innerHTML = d.thesis_failure_score || 0;
    document.getElementById("logs").innerHTML = (d.logs || []).slice(-15).join("<br>");
    const systemErrors = (d.system_errors || []).slice(-8).reverse();
    const systemErrorHtml = systemErrors.map(function(e) {{
        const stack = e.stack_trace
            ? '<details><summary>MainThread stack trace</summary><pre>' + uniEsc(e.stack_trace) + '</pre></details>'
            : '';
        return '<div class="system-error"><div><b>' + uniEsc(e.error_type || "SYSTEM_ERROR") +
            '</b> · ' + uniEsc(e.severity || "ERROR") + ' · ' + uniEsc(e.component || "runtime") +
            ' · ' + uniEsc(e.timestamp || "-") + '</div><div>' + uniEsc(e.message || "-") +
            '</div><div><b>Cause:</b> ' + uniEsc(e.cause || "Not classified; inspect diagnostic details.") +
            '</div>' + stack + '</div><hr>';
    }}).join("");
    document.getElementById("errors").innerHTML = systemErrorHtml ||
        (d.errors || []).slice(-5).map(uniEsc).join("<br>");
    let top5Html = "";
    (d.top5 || []).forEach(o => {{
        top5Html += `<div><b>${{o.symbol}}</b> | Score: ${{o.score.toFixed(2)}} | ADX: ${{o.adx || 0}} | RSI: ${{o.rsi || 0}}</div><hr>`;
    }});
    document.getElementById("top5").innerHTML = top5Html || "No opportunities";
    document.getElementById("scanned").innerText = d.scanned_count;
    document.getElementById("lastScan").innerText = d.last_scan ? new Date(d.last_scan*1000).toLocaleTimeString() : "-";
    document.getElementById("regimeLabel").innerText = d.regime;
    document.getElementById("apiStatus").innerText = d.health.api;
    document.getElementById("errCount").innerText = d.health.errors;
    document.getElementById("botStatus").innerText = d.health.status;
    if(d.rf_dashboard) {{
        let rfHtml = "";
        d.rf_dashboard.forEach(item => {{
            let signalIcon = item.signal === "BUY" ? "🟢" : (item.signal === "SELL" ? "🔴" : "⚪");
            rfHtml += `<div>${{item.icon}} ${{signalIcon}} ${{item.symbol}} | ${{item.status}} | score=${{item.score.toFixed(2)}} | ADX=${{item.adx||0}} | RSI=${{item.rsi||0}}</div>`;
        }});
        document.getElementById("rfSignals").innerHTML = rfHtml || "No RF signals";
    }}
    if(d.discovery_scanner) {{
        let ds = d.discovery_scanner || {{}};
        let buys = (d.scanner_v2_buy || []).slice(0, 5);
        let sells = (d.scanner_v2_sell || []).slice(0, 5);
        let rows = [...buys.map(x => ({{...x, _side:"BUY"}})), ...sells.map(x => ({{...x, _side:"SELL"}}))];
        let rHtml = rows.map(x => `${{x._side === "BUY" ? "🟢" : "🔴"}} ${{x.symbol}} | score ${{x.score}} | ${{x.structure || "structure n/a"}} | ${{x.liquidity || "liquidity n/a"}}`).join("<br>");
        let ps = d.professional_scanner || {{}};
        document.getElementById("discovery-scanner-panel").innerHTML = `State: <b>${{ds.state || "UNKNOWN"}}</b> | Symbols: ${{ds.symbols_scanned ?? 0}}/${{ds.universe_limit ?? "-"}} | PRO: ${{ds.professional_candidates ?? 0}} | RF: ${{ds.rf_candidates ?? 0}} | Radar: ${{ds.radar_candidates ?? 0}} | Duration: ${{ds.duration_seconds ?? "-"}}s<br><small>PRO scan: ${{ps.state || "IDLE"}} · selected ${{ps.selected ?? 0}} · valid ${{ps.valid_data ?? 0}} · discovery only</small><br>${{rHtml || "No ranked BUY/SELL candidates yet"}}`;
    }}
    if(d.ema9_vwap_proximity_scanner) {{
        let pHtml = "";
        d.ema9_vwap_proximity_scanner.forEach(item => {{
            let icon = item.cross ? "⚡" : (item.state === "NEAR_CROSS" || item.state === "CONVERGING" ? "🟡" : item.direction === "BUY" ? "📈" : item.direction === "SELL" ? "📉" : "⚪");
            pHtml += `<div style="padding:6px;border-bottom:1px solid #26364a;">${{icon}} <b>${{item.symbol}}</b> | ${{item.direction || "UNKNOWN"}} | ${{item.cross_state_display || item.state || "UNKNOWN"}} | Score: ${{item.score ?? "-"}} | Gap ATR: ${{item.gap_atr ?? "-"}} | EMA9: ${{item.ema9 ?? "-"}} | EMA50: ${{item.ema50 ?? "-"}} | VWAP: ${{item.vwap ?? "-"}} | Cross: ${{item.cross_trigger || "NONE"}} <small>· Closed candle · Ranking only</small></div>`;
        }});
        let pm = d.ema9_vwap_proximity_scanner_meta || {{}};
        document.getElementById("proximity-scanner-panel").innerHTML = (pHtml || "No proximity candidates") + `<small>Batch: ${{pm.observed_this_batch ?? 0}} | Pool: ${{pm.symbols ?? 0}} | Updated: ${{pm.updated_at ? new Date(pm.updated_at*1000).toLocaleTimeString() : "-"}} | Role: ${{pm.role || "RANKING_ONLY"}}</small>`;
    }} else {{
        document.getElementById("proximity-scanner-panel").innerHTML = "Waiting for scanner data";
    }}
    if(d.watchlist) {{
        const delivery = d.cross_watchlist_delivery || {{}};
        const entries = Object.entries(d.watchlist);
        const wHtml = entries.map(([sym, w]) => {{
            const q = w.diagnostics || {{}};
            const sideIcon = w.side === "BUY" ? "🟢" : (w.side === "SELL" ? "🔴" : "⚪");
            const state = w.state || "UNKNOWN";
            const stateColor = state === "ENTRY_READY" ? "#2ecc71" :
                (String(state).startsWith("WAITING") || state === "NO_CHASE" ? "#f1c40f" :
                (state === "INVALIDATED" || state === "REJECTED" ? "#e74c3c" : "#95a5a6"));
            const price = Number.isFinite(Number(w.price)) ? Number(w.price).toPrecision(8) : "-";
            const stopText = Number.isFinite(Number(w.structural_stop)) && Number(w.structural_stop) > 0 ? Number(w.structural_stop).toPrecision(8) : "UNKNOWN";
            const updated = w.updated_at ? new Date(w.updated_at * 1000).toLocaleTimeString() : "-";
            return `<div style="margin-bottom:7px;border:1px solid #26364a;border-radius:7px;padding:7px;background:#101a29;">
              <div><b>${{sideIcon}} ${{uniEsc(w.symbol || sym)}}</b> · ${{uniEsc(w.regime)}} · <span style="color:${{stateColor}}">${{uniEsc(state)}}</span> · Score ${{uniEsc(w.score)}}</div>
              <small>Price ${{uniEsc(price)}} · Structure ${{uniEsc(w.structure)}} · Liquidity ${{uniEsc(w.liquidity)}}/${{uniEsc(w.liquidity_stage)}} · Zone ${{uniEsc(w.zone_quality ?? "-")}}/100 · Valid ${{uniEsc(w.zone_valid ?? "UNKNOWN")}}</small>
              <br><small>Next: ${{uniEsc(w.next_event)}} · Block: ${{uniEsc(w.blocking || "none")}} · Stop: ${{uniEsc(w.stop_status)}} · Data: ${{uniEsc(w.freshness)}} · Cross: ${{uniEsc(w.cross)}} · Updated: ${{uniEsc(updated)}}</small>
              <details style="margin-top:5px;"><summary style="cursor:pointer;color:#8fa8c7;">Advanced diagnostics (collapsed)</summary>
                <small>Decision projection (read-only): ${{uniEsc(w.symbol || sym)}} / ${{uniEsc(w.side || "UNKNOWN")}} | Cross: ${{uniEsc(q.cross_state)}} | durable=${{uniEsc(q.cross_durable)}} | role=${{uniEsc(q.cross_role)}}</small>
                <br><small>Zone: quality=${{uniEsc(q.zone_quality ?? w.zone_quality ?? "-")}}/100 | valid=${{uniEsc(q.zone_valid ?? "UNKNOWN")}} | Liquidity: ${{uniEsc(q.liquidity_state)}} / ${{uniEsc(q.liquidity_stage)}} | Confirmation: ${{uniEsc(q.confirmation)}} | Pullback/Retest: ${{uniEsc(q.pullback)}}</small>
                <br><small>Structural Stop: ${{uniEsc(q.stop_valid ? "VALID" : "STOP_INVALID_NOT_READY")}} | Validation: ${{uniEsc(q.stop_validation)}} | Final decision: ${{uniEsc(q.decision_state)}} | Gate: ${{uniEsc(q.gate)}} | Blocking: ${{uniEsc(q.blocking)}} | Next: ${{uniEsc(q.next_event)}} | authority=${{uniEsc(q.authority || "UNCHANGED_EXECUTE_ENTRY")}}</small>
                <br><small>Entry type: ${{uniEsc(q.entry_type)}} | reason: ${{uniEsc(q.entry_reason)}} | confirmation: ${{uniEsc(q.entry_confirmation ?? "?")}} | invalidation: ${{uniEsc(q.entry_invalidation)}} | source: ${{uniEsc(q.entry_source)}}</small>
                <br><small>Institutional report: cross=${{uniEsc(q.report_cross)}} | regime=${{uniEsc(q.report_regime)}} | zone=${{uniEsc(q.report_zone)}} | structure=${{uniEsc(q.report_structure)}} | liquidity=${{uniEsc(q.report_liquidity)}} | volume=${{uniEsc(q.report_volume)}} | trend=${{uniEsc(q.report_trend)}} | room=${{uniEsc(q.room_pct ?? "?")}}% (${{uniEsc(q.room_status)}}) | invalidation=${{uniEsc(q.invalidation_status)}} | freshness=${{uniEsc(q.report_freshness)}} | confidence=${{uniEsc(q.confidence ?? "?")}}</small>
                <br><small>Entry classification: ${{uniEsc(q.classification)}} | reason: ${{uniEsc(q.classification_reasons)}} | cross-zone: ${{uniEsc(q.cross_zone)}} (${{uniEsc(q.cross_zone_reasons)}}) | authority: ${{uniEsc(q.authority)}}</small>
                <br><small>Lifecycle: ${{uniEsc(q.lifecycle)}} | waiting: ${{uniEsc(q.waiting)}} | analysis: ${{uniEsc(q.analysis_status)}} | durable=${{uniEsc(q.cross_durable)}} | role=${{uniEsc(q.cross_role)}}</small>
                <br><small>Cross delivery: same_cycle=${{uniEsc(delivery.same_cycle ?? false)}} | detected=${{uniEsc(delivery.detected_at ?? 0)}} | delivered=${{uniEsc(delivery.delivered_at ?? 0)}} | refresh_s=${{uniEsc(delivery.refresh_seconds ?? 0)}} | new=${{uniEsc(delivery.new_events ?? 0)}} | status=${{uniEsc(delivery.reason || "NO_NEW_CROSS_EVENT")}}</small>
              </details>
            </div>`;
        }}).join("");
        document.getElementById("watchlist-panel").innerHTML = wHtml || "No active candidates";
    }} else {{
        document.getElementById("watchlist-panel").innerHTML = "No watchlist data";
    }}
    let noEntryHtml = "";
    if(d.no_entry_feed) {{
        d.no_entry_feed.forEach(item => {{
            let timeStr = new Date(item.time * 1000).toLocaleTimeString();
            noEntryHtml += `<div>${{timeStr}} | ${{item.symbol}} ${{item.side}}: ${{item.reason}} (score ${{item.score}})</div>`;
        }});
    }}
    document.getElementById("no-entry-feed").innerHTML = noEntryHtml || "No recent skips";
    if(d.institutional_flow) {{
        let flow = d.institutional_flow;
        document.getElementById("flow-banker").innerHTML = flow.banker_pressure.toFixed(1);
        document.getElementById("flow-retail").innerHTML = flow.retailer_pressure.toFixed(1);
        document.getElementById("flow-hot").innerHTML = flow.hot_money.toFixed(1);
        document.getElementById("flow-bias").innerHTML = flow.institutional_bias_detailed || flow.institutional_bias;
        document.getElementById("flow-align").innerHTML = flow.flow_alignment.toFixed(1);
        document.getElementById("flow-dist").innerHTML = flow.distribution_risk.toFixed(1);
        document.getElementById("flow-mom-health").innerHTML = flow.momentum_health.toFixed(1);
        document.getElementById("flow-cont-str").innerHTML = flow.continuation_strength.toFixed(1);
        document.getElementById("flow-exh-risk").innerHTML = flow.exhaustion_risk.toFixed(1);
        document.getElementById("flow-climax").innerHTML = flow.climax_risk.toFixed(1);
        document.getElementById("flow-greed").innerHTML = flow.greed_state ? "🚨 Yes" : "✅ No";
        document.getElementById("flow-dom").innerHTML = flow.smart_money_dominant ? "✅ Yes" : "❌ No";
    }}
}}
async function manualTrade(side){{ const r=await fetch('/trade',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{side:side}})}}); const res=await r.json(); alert(res.message); }}
async function manualClose(){{ const r=await fetch('/close',{{method:'POST'}}); const res=await r.json(); alert(res.message); }}
setInterval(fetchData, 6000);
async function loadDecision() {{
  const container = document.getElementById("decision-list");
  const summary = document.getElementById("decision-summary");
  try {{
    const res = await fetch('/decision', {{cache: 'no-store'}});
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const json = await res.json();
    const list = Array.isArray(json.data) ? json.data : [];
    const active = json.active_trade || {{open:false}};
    if (summary) {{
      summary.textContent = active.open
        ? `🧠 ${{json.authority || 'Unified Manager'}} | ${{json.mode || '-'}} | ${{active.symbol || '-'}} ${{active.side || ''}} | ${{active.profit_mode || '-'}} | Mgmt: ${{active.management_mode || '-'}} | Target ROE: ${{active.selected_target_gross_roe_pct ?? '-'}}% | TP1: ${{active.tp1_hit ? '✅' : '⏳'}} | TP2: ${{active.tp2_hit ? '✅' : '⏳'}} | SL: ${{active.native_sl_status || 'UNKNOWN'}}`
        : `🧠 ${{json.authority || 'Unified Manager'}} | ${{json.mode || '-'}} | No active trade`;
    }}
    container.replaceChildren();
    if (!list.length) {{
      const empty = document.createElement('div');
      empty.textContent = 'No trade-management events yet. Decisions will appear here after the manager evaluates a position.';
      empty.style.color = '#9ca3af';
      container.appendChild(empty);
      return;
    }}
    for (const item of list) {{
      const card = document.createElement('div');
      card.style.borderBottom = '1px solid #222';
      card.style.padding = '8px 4px';
      const title = document.createElement('div');
      title.style.fontWeight = '700';
      title.style.color = (String(item.status || '').includes('VERIFIED') || item.status === 'DECIDED') ? '#2ecc71' : '#f1c40f';
      title.textContent = `${{item.icon || '🧠'}} ${{item.label || item.event_type || 'Management event'}} | ${{item.status || 'INFO'}}`;
      const details = document.createElement('div');
      details.style.fontSize = '12px';
      details.style.color = '#cbd5e1';
      const parts = [];
      if (item.symbol) parts.push(`${{item.symbol}} ${{item.side || ''}}`);
      if (item.profit_mode) parts.push(`Mode: ${{item.profit_mode}}`);
      if (item.management_mode) parts.push(`Mgmt: ${{item.management_mode}}`);
      if (item.selected_target_roe_pct !== undefined && item.selected_target_roe_pct !== null) parts.push(`Target ROE: ${{item.selected_target_roe_pct}}%`);
      if (item.target_convention_basis) parts.push(`Basis: ${{item.target_convention_basis}}`);
      if (item.entry_mode) parts.push(`Entry: ${{item.entry_mode}}`);
      if (item.price !== undefined && item.price !== null) parts.push(`Price: ${{item.price}}`);
      if (item.roe_pct !== undefined && item.roe_pct !== null) parts.push(`ROE: ${{item.roe_pct}}%`);
      if (item.qty_closed !== undefined && item.qty_closed !== null) parts.push(`Closed: ${{item.qty_closed}}`);
      if (item.remaining_qty !== undefined && item.remaining_qty !== null) parts.push(`Remaining: ${{item.remaining_qty}}`);
      if (item.fill_price) parts.push(`Fill: ${{item.fill_price}}`);
      if (item.order_id) parts.push(`Order: ${{item.order_id}}`);
      if (item.confidence) parts.push(`Confidence: ${{item.confidence}}`);
      details.textContent = parts.join(' | ');
      const reason = document.createElement('div');
      reason.style.fontSize = '12px';
      reason.style.color = '#94a3b8';
      reason.textContent = item.actual_exit_reason || item.reason || item.close_reason || '';
      const date = document.createElement('div');
      date.style.fontSize = '10px';
      date.style.color = '#64748b';
      date.textContent = item.timestamp ? new Date(item.timestamp * 1000).toLocaleString() : '';
      card.append(title, details, reason, date);
      container.appendChild(card);
    }}
  }} catch(e) {{
    if (summary) summary.textContent = '⚠️ Management log API unavailable: ' + String(e.message || e);
    console.error(e);
  }}
}}
setInterval(loadDecision, 3000);
loadDecision();
fetchData();
</script>
</body></html>
"""
    return html

@app.route("/decision")
def decision_feed():
    """Return the single-source trade-management event feed used by the Dashboard."""
    try:
        active = bool(STATE.get("open"))
        payload = {
            "data": TRADE_MANAGEMENT_LOG.snapshot(60),
            "authority": "TradeManagementBoard / LiveTradeManager",
            "mode": "LIVE" if MODE_LIVE else "PAPER",
            "active_trade": {
                "open": active,
                "symbol": STATE.get("current_symbol"),
                "side": STATE.get("side"),
                "profit_mode": STATE.get("profit_mode", STANDARD_MODE),
                "management_mode": STATE.get("management_mode") or "STANDARD",
                "management_mode_report": dict(STATE.get("management_mode_report") or {}),
                "management_mode_history": list(STATE.get("management_mode_history") or [])[-20:],
                "selected_target_gross_roe_pct": (STATE.get("management_mode_report") or {}).get("selected_target_gross_roe_pct"),
                "entry_risk_plan": dict(STATE.get("entry_risk_plan") or {}),
                "actual_exit_reason": STATE.get("last_close_reason") or STATE.get("pending_close_reason") or "",
                "tp1_hit": bool(STATE.get("tp1_hit")),
                "tp2_hit": bool(STATE.get("tp2_hit")),
                "remaining_qty": safe_float(STATE.get("remaining_qty")),
                "native_sl_status": STATE.get("native_sl_status", "UNKNOWN"),
                "recovery_hold": bool(STATE.get("recovery_hold")),
            } if active else {"open": False},
        }
        response = jsonify(safe_json(payload))
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response, 200
    except Exception as exc:
        log_execution(f"/decision error: {type(exc).__name__}: {exc}", "ERROR")
        return jsonify({"data": [], "error": "decision_feed_unavailable"}), 200

def _unified_decision_dashboard_sections():
    """Read-only projection of existing feeds; no scan, exchange call or order."""
    return _project_dashboard_sections(
        watchlist=MEMORY.get("watchlist", {}),
        confirmed_crosses=MEMORY.get("confirmed_cross_events", []),
        ranking=MEMORY.get("ema9_vwap_proximity_scanner", []),
        cross_delivery=MEMORY.get("_cross_watchlist_delivery", {}),
    )


@app.route("/data")
def data():
    try:
        bal = get_balance_safe()
        free_bal = get_free_balance_safe()
        avail_margin = free_bal
        mode = "LIVE" if MODE_LIVE else "PAPER"
        DASHBOARD_STATE["account"]["balance"] = bal
        DASHBOARD_STATE["account"]["free_balance"] = free_bal
        DASHBOARD_STATE["account"]["available_margin"] = avail_margin
        DASHBOARD_STATE["account"]["mode"] = mode
        perf = get_dashboard_metrics()
        pos = None
        if STATE["open"] and STATE.get("current_symbol"):
            roe = STATE.get("roe_pct", 0.0)
            pos = {
                "symbol": STATE["current_symbol"],
                "side": STATE["side"],
                "entry": round(STATE["entry"],4),
                "qty": STATE["qty"],
                "pnl": round(roe, 2),
                "sl": round(STATE.get("synthetic_sl",0),4),
                "tp1": round(STATE.get("synthetic_tp1",0),4),
                "tp2": round(STATE.get("tp2_price",0),4),
                "tp1_done": STATE.get("tp1_hit", False),
                "trailing_active": STATE.get("trail_activated", False),
                "regime": MEMORY.get("regime", "UNKNOWN"),
                "trade_type": STATE.get("trade_type", "N/A"),
                "entry_type": STATE.get("entry_type", "N/A"),
                "classification": STATE.get("classification", "N/A"),
                "location": STATE.get("location", "N/A"),
                "zone": STATE.get("zone_info", "N/A"),
                "score": STATE.get("trade_score", 0),
                "narrative_classification": STATE.get("narrative_classification", ""),
                "narrative_confidence": STATE.get("narrative_confidence", 0.0),
                "confidence_level": STATE.get("confidence_level", ""),
                "current_confidence": STATE.get("current_confidence", 50.0),
                "market_regime": STATE.get("market_regime", "UNKNOWN"),
                "continuation_pressure": STATE.get("continuation_pressure", 50),
                "trade_state": STATE.get("trade_state", "RANGE_CHOP"),
                "trail_multiplier": STATE.get("smart_trail_mult", 1.5),
                "delay_tp1": STATE.get("delay_tp1", False),
                "management_mode": STATE.get("management_mode") or "STANDARD",
                "management_mode_report": dict(STATE.get("management_mode_report") or {}),
                "management_mode_history": list(STATE.get("management_mode_history") or [])[-20:],
                "selected_target_gross_roe_pct": (STATE.get("management_mode_report") or {}).get("selected_target_gross_roe_pct"),
                "entry_risk_plan": dict(STATE.get("entry_risk_plan") or {}),
                "actual_exit_reason": STATE.get("last_close_reason") or STATE.get("pending_close_reason") or ""
            }
        else:
            pos = DASHBOARD_STATE["position"]
        health = MEMORY["health"].copy()
        health["errors"] = len(DASHBOARD_STATE["errors"])
        top5 = MEMORY.get("top_candidates", [])[:5] if "top_candidates" in MEMORY else []
        cleanup_watchlist()
        watchlist_data = MEMORY.get("watchlist", {})
        watchlist_dashboard = build_watchlist_dashboard_payload(watchlist_data, limit=30)
        no_entry_feed = MEMORY.get("no_entry_feed", [])[-5:]
        live_data = {}
        supervisor_data = None
        if DASHBOARD_STATE.get("live_trade_mode", False) and STATE.get("open"):
            supervisor_data = {
                "side": STATE["side"],
                "entry_price": STATE["entry"],
                "mark_price": STATE.get("mark_price", 0),
                "unrealized_pnl": STATE.get("unrealized_pnl_usdt", 0),
                "roe_pct": STATE.get("roe_pct", 0),
                "liquidation_price": STATE.get("liquidation_price", 0),
                "position_size": STATE["qty"],
                "leverage": LEVERAGE,
                "tp1_hit": STATE.get("tp1_hit", False),
                "tp2_hit": STATE.get("tp2_hit", False),
                "trailing_active": STATE.get("trail_activated", False),
                "adx": STATE.get("adx_live", 0),
                "di_plus": STATE.get("di_plus_live", 0),
                "di_minus": STATE.get("di_minus_live", 0),
                "continuation_pressure": STATE.get("continuation_pressure", 50),
                "trend_strength": STATE.get("trend_strength", 0),
                "thesis_failure_score": STATE.get("thesis_failure_score", 0),
                "current_confidence": STATE.get("current_confidence", 50),
                "trade_personality": STATE.get("trade_personality", "NEUTRAL"),
                "institutional_flow": STATE.get("institutional_flow", "NEUTRAL"),
                "reclaim_risk": STATE.get("reclaim_risk", 0),
                "trade_state": STATE.get("trade_state", "RANGE_CHOP"),
                "trail_multiplier": STATE.get("smart_trail_mult", 1.5),
                "delay_tp1": STATE.get("delay_tp1", False),
                "management_mode": STATE.get("management_mode") or "STANDARD",
                "selected_target_gross_roe_pct": (STATE.get("management_mode_report") or {}).get("selected_target_gross_roe_pct"),
                "estimated_net_loss": (STATE.get("entry_risk_plan") or {}).get("estimated_net_loss"),
                "risk_reward_net": (STATE.get("entry_risk_plan") or {}).get("risk_reward_net"),
                "idealized_liquidation_distance_pct": (STATE.get("entry_risk_plan") or {}).get("idealized_liquidation_distance_pct")
            }
            live_data["live_trade_mode"] = True
            live_data["supervisor"] = supervisor_data
            live_data["lifecycle_state"] = _live_manager.lifecycle_state.value
        else:
            live_data["live_trade_mode"] = False

        institutional_flow_data = DASHBOARD_STATE.get("institutional_flow", {})
        if not institutional_flow_data and STATE.get("smart_money"):
            mf = STATE.get("momentum_flow", {})
            institutional_flow_data = {
                "banker_pressure": STATE["smart_money"].get("banker_pressure", 0),
                "retailer_pressure": STATE["smart_money"].get("retailer_pressure", 0),
                "hot_money": STATE["smart_money"].get("hot_money_pressure", 0),
                "institutional_bias": STATE["smart_money"].get("institutional_bias", "NEUTRAL"),
                "institutional_bias_detailed": STATE["smart_money"].get("institutional_bias_detailed", "NEUTRAL"),
                "flow_alignment": STATE["smart_money"].get("flow_alignment", 0),
                "distribution_risk": STATE["smart_money"].get("distribution_risk", 0),
                "momentum_health": mf.get("momentum_health", 0),
                "continuation_strength": mf.get("continuation_strength", 0),
                "exhaustion_risk": mf.get("exhaustion_risk", 0),
                "climax_risk": mf.get("climax_risk", 0),
                "greed_state": mf.get("greed_state", False),
                "smart_money_dominant": STATE["smart_money"].get("smart_money_dominant", False)
            }

        health_snapshot = naiso_health_snapshot()
        payload = {
            "balance": bal,
            "free_balance": free_bal,
            "avail_margin": avail_margin,
            "mode": mode,
            "stats": DASHBOARD_STATE["stats"],
            "position": pos,
            "logs": DASHBOARD_STATE["logs"][-30:],
            "errors": DASHBOARD_STATE["errors"][-10:],
            "system_errors": DASHBOARD_STATE.get("system_errors", [])[-20:],
            "top5": top5,
            "candidates": MEMORY.get("top_candidates", []),
            "scanned_count": len(MEMORY.get("top_candidates", [])),
            "last_scan": MEMORY["last_scan"],
            "regime": MEMORY["regime"],
            "health": health,
            "rf_dashboard": MEMORY.get("rf_dashboard", [])[:20],
            "total_pnl": perf["total_pnl"],
            "total_pnl_usdt": perf["total_pnl_usdt"],
            "last_trade": perf["last_trade"],
            "scanner_v2_buy": MEMORY.get("scanner_v2_buy", []),
            "scanner_v2_sell": MEMORY.get("scanner_v2_sell", []),
            "professional_scan_candidates": MEMORY.get("professional_scan_candidates", [])[:40],
            "professional_scanner": MEMORY.get("professional_scanner", {}),
            "discovery_scanner": MEMORY.get("discovery_scanner", {}),
            "scanner_tasks": MEMORY.get("scanner_tasks", {}),
            "ema9_vwap_proximity_scanner": MEMORY.get("ema9_vwap_proximity_scanner", [])[:30],
            "ema9_vwap_proximity_scanner_meta": MEMORY.get("ema9_vwap_proximity_scanner_meta", {}),
            "watchlist": watchlist_dashboard,
            "no_entry_feed": no_entry_feed,
            "entry_rejection_metrics": MEMORY.get("entry_rejection_metrics", {}),
            "last_closed_candle_validation": MEMORY.get("last_closed_candle_validation", {}),
            "continuation_probability": STATE.get("continuation_probability", 0.5),
            "hold_quality": STATE.get("hold_quality", "UNKNOWN"),
            "counter_pressure": STATE.get("counter_pressure", 0.0),
            "reclaim_risk": STATE.get("reclaim_risk", 0.0),
            "trend_strength": STATE.get("trend_strength", 0.0),
            "continuation_reasons": STATE.get("continuation_reasons", []),
            "trade_thesis": STATE.get("trade_thesis", {}),
            "current_confidence": STATE.get("current_confidence", 50.0),
            "market_regime": STATE.get("market_regime", "UNKNOWN"),
            "continuation_pressure": STATE.get("continuation_pressure", 50),
            "thesis_failure_score": STATE.get("thesis_failure_score", 0),
            "institutional_flow": institutional_flow_data,
            "last_live_refresh": DASHBOARD_STATE.get("last_live_refresh", time.time()),
            "naiso_health": health_snapshot,
            "readiness": _health_readiness(health_snapshot),
            "unified_decision": _unified_decision_dashboard_sections(),
            "cross_watchlist_delivery": dict(MEMORY.get("_cross_watchlist_delivery", {})),
            **live_data
        }
        safe_payload = safe_json(payload)
        return jsonify(safe_payload), 200
    except Exception as e:
        log_execution(f"/data error: {e}", "ERROR")
        return jsonify({"error": str(e)}), 200

@app.route("/trade", methods=["POST"])
def manual_trade():
    if daily_loss_cooldown_active():
        return jsonify({"error": "New entries paused by daily drawdown cooldown"}), 423
    if RECOVERY_BLOCK_ENTRIES or STATE.get("execution_unresolved") or STATE.get("close_reconciliation_required"):
        return jsonify({"error": "Entry blocked until existing exchange/recovery state is reconciled"}), 409
    data = request.json or {}
    side = data.get("side")
    if not side or side not in ["BUY","SELL"]:
        return jsonify({"error": "Invalid side"}),400
    if STATE["open"]:
        return jsonify({"error": "Position open"}),400
    sync_position_state(DEFAULT_SYMBOL)
    if STATE["open"]:
        return jsonify({"error": "Already in position (synced with exchange)"}),400
    if INSUFFICIENT_MARGIN_COOLDOWN_UNTIL and time.time() < INSUFFICIENT_MARGIN_COOLDOWN_UNTIL:
        return jsonify({"error": "Insufficient margin cooldown active"}),400
    price = get_ticker_safe(DEFAULT_SYMBOL)
    if not price or price <= 0:
        return jsonify({"error": "No price"}),400
    df = get_ohlcv_safe(DEFAULT_SYMBOL, 100)
    if df is None:
        return jsonify({"error": "No data"}),500
    atr = compute_atr(df).iloc[-1]
    sl = price - atr*1.6 if side=="BUY" else price + atr*1.6
    tp1 = price*1.006 if side=="BUY" else price*0.994
    tp2 = price*1.02 if side=="BUY" else price*0.98
    classification = "SNIPER"
    entry_lock = globals().get("_ENTRY_DISCOVERY_TASK_LOCK")
    if entry_lock is not None and not entry_lock.acquire(blocking=False):
        return jsonify({"error": "Another entry-analysis cycle is active; retry after it completes"}), 409
    previous_entry_context = globals().get("_INSTITUTIONAL_ENTRY_CONTEXT")
    try:
        # Manual override has its own validated market snapshot; never inherit a
        # concurrent scanner's symbol/zone/cross context.
        globals()["_INSTITUTIONAL_ENTRY_CONTEXT"] = None
        ok = execute_entry(side, DEFAULT_SYMBOL, price, sl, tp1, tp2, 80,
                           "Manual override", atr, "HYBRID", "MANUAL", classification)
    finally:
        globals()["_INSTITUTIONAL_ENTRY_CONTEXT"] = previous_entry_context
        if entry_lock is not None:
            entry_lock.release()
    return jsonify({"message": "Done" if ok else "Failed"}),200 if ok else 500

@app.route("/close", methods=["POST"])
def manual_close():
    if not STATE["open"]:
        return jsonify({"error": "No position"}), 400
    symbol = STATE.get("current_symbol")
    if not symbol:
        return jsonify({"error": "Open state has no symbol; reconciliation required"}), 409
    close_ok = safe_close_position(symbol, close_reason="MANUAL_DASHBOARD_CLOSE")
    if not close_ok and not (not STATE.get("open") and MEMORY.get("position_status") == "FLAT"):
        return jsonify({"error": "Close not confirmed; position state is retained for reconciliation",
                        "execution_unresolved": bool(STATE.get("execution_unresolved")),
                        "exchange_state": STATE.get("exchange_state", "UNKNOWN")}), 409
    if not STATE.get("open") and MEMORY.get("position_status") == "FLAT":
        return jsonify({"message": "Position was already FLAT on BingX and has been reconciled",
                        "close_reason": STATE.get("last_close_reason", "EXTERNAL_CLOSE_UNKNOWN")}), 200
    if not STATE.get("close_verified"):
        return jsonify({"error": "Order was not verified as fully closed; remaining position is still managed",
                        "remaining_qty": safe_float(STATE.get("remaining_qty"))}), 409
    record_trade_management_event("MANUAL_CLOSE_VERIFIED", "PAPER_SIMULATED" if PAPER_MODE else "EXCHANGE_VERIFIED",
                                  log_level="WARN", symbol=symbol, side=STATE.get("side"),
                                  reason="MANUAL_DASHBOARD_CLOSE", qty_closed=STATE.get("last_close_filled_qty"),
                                  fill_price=STATE.get("last_close_fill_price"), order_id=STATE.get("last_close_order_id"))
    finalize_trade_with_reality(symbol, close_reason="MANUAL_DASHBOARD_CLOSE")
    return jsonify({"message": "Closed and exchange-verified",
                    "close_reason": STATE.get("last_close_reason", "MANUAL_DASHBOARD_CLOSE")}), 200

def naiso_health_snapshot():
    """Unified, side-effect-free view of long-run safety and resource health."""
    watchdog = globals().get("NAISO_WATCHDOG")
    maintenance = globals().get("NAISO_MAINTENANCE")
    resilience = globals().get("API_RESILIENCE")
    supervisor = globals().get("_reconciliation_supervisor")
    loss = _LOSS_COOLDOWN_STATE if "_LOSS_COOLDOWN_STATE" in globals() else {}
    return {
        "version": "NAISO-LIVE-v1.1.0-longrun",
        "resources": MEMORY.get("resource_metrics", {}),
        "maintenance": maintenance.metrics() if maintenance is not None else None,
        "threads": (watchdog.snapshot() if watchdog is not None else None),
        "watchlist_worker": (
            _watchlist_worker_health_snapshot()
            if callable(globals().get("_watchlist_worker_health_snapshot")) else {}
        ),
        "api": resilience.metrics() if resilience is not None else None,
        "scanner": {
            "tasks": MEMORY.get("scanner_tasks", {}),
            "discovery": MEMORY.get("discovery_scanner", {}),
            "proximity": MEMORY.get("ema9_vwap_proximity_scanner_meta", {}),
            "watchlist": MEMORY.get("watchlist", {}),
        },
        "positions": {
            "open": bool(STATE.get("open") or TRADE_STATE.get("in_position")),
            "symbol": STATE.get("current_symbol"),
            "remaining_qty": STATE.get("remaining_qty"),
            "native_sl_status": STATE.get("native_sl_status"),
            "native_sl_verified": bool(STATE.get("native_sl_verified")),
        },
        "recovery": {
            "hold": bool(STATE.get("recovery_hold")),
            "reason": STATE.get("recovery_reason"),
            "block_entries": bool(globals().get("RECOVERY_BLOCK_ENTRIES", False)),
            "execution_unresolved": bool(STATE.get("execution_unresolved")),
            "close_reconciliation_required": bool(STATE.get("close_reconciliation_required")),
            "status": MEMORY.get("recovery_status"),
        },
        "reconciliation": supervisor.metrics() if supervisor is not None else None,
        "last_successful_reconciliation": STATE.get("last_successful_reconciliation"),
        "last_successful_market_refresh": DASHBOARD_STATE.get("last_live_refresh"),
        "restart_count": len(_NAISO_LOOP_RESTARTS) if "_NAISO_LOOP_RESTARTS" in globals() else 0,
        "restarts": list(_NAISO_LOOP_RESTARTS[-10:]) if "_NAISO_LOOP_RESTARTS" in globals() else [],
        "daily_loss": {
            "active": bool(loss.get("until", 0) > time.time()),
            "until": loss.get("until"),
            "peak_balance": loss.get("peak_balance") or STATE.get("daily_peak_balance"),
            "trigger_count": loss.get("trigger_count", 0),
            "limit_pct": MAX_DAILY_LOSS_PCT,
        },
        "journal": MEMORY.get("journal_maintenance", {}),
    }


# Explicit readiness vocabulary. These are additive API states; legacy keys and
# HTTP ports remain intact. A live HTTP response is PROCESS_LIVE by definition,
# but it is never proof that the critical main_loop is progressing.
HEALTH_PROCESS_LIVE = "PROCESS_LIVE"
HEALTH_MAIN_LOOP_RUNNING = "MAIN_LOOP_RUNNING"
HEALTH_MAIN_LOOP_STALLED = "MAIN_LOOP_STALLED"
HEALTH_MAIN_LOOP_UNKNOWN = "MAIN_LOOP_UNKNOWN"
HEALTH_RECOVERY_NONE = "NONE"
HEALTH_RECOVERY_IN_PROGRESS = "RECOVERY_IN_PROGRESS"
HEALTH_RECOVERY_FAILED = "RECOVERY_FAILED"
HEALTH_TRADING_READY = "TRADING_READY"
HEALTH_TRADING_BLOCKED = "TRADING_BLOCKED"
_MAIN_LOOP_FAILED_STATUSES = {
    "STALLED", "STALLED_SAFE_HOLD", "FAILED", "RESTART_FAILED",
    "RECOVERY_DEFERRED", "RESTART_BUDGET_EXHAUSTED", "IDLE_SAFE_HOLD",
}
_RECOVERY_FAILED_STATUSES = {
    "RESTART_FAILED", "RESTART_BUDGET_EXHAUSTED",
    "SUPERVISOR_RESTART_LIMIT_EXHAUSTED", "STARTUP_RECOVERY_ERROR",
    "MARKETS_UNAVAILABLE", "PERSISTED_RECOVERY_HOLD",
}


def _health_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _health_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _health_main_loop_state(snapshot):
    """Report genuine main-loop progress without creating a beat."""
    watchdog = globals().get("NAISO_WATCHDOG")
    threads = snapshot.get("threads") if isinstance(snapshot, dict) else None
    components = threads.get("components", {}) if isinstance(threads, dict) else {}
    if not isinstance(components, dict):
        components = {}
    row = components.get("main_loop")
    progress = None
    progress_fn = getattr(watchdog, "progress_snapshot", None)
    if callable(progress_fn):
        try:
            progress = progress_fn("main_loop")
        except Exception:
            progress = None
    if not isinstance(row, dict):
        row = progress if isinstance(progress, dict) else {}
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    owner = _main_loop_owner() if callable(globals().get("_main_loop_owner")) else {}
    with _MAIN_LOOP_OWNER_LOCK:
        last = dict(_MAIN_LOOP_LAST_OWNER_PROGRESS)
    seq = _health_int(globals().get("_MAIN_LOOP_PROGRESS_SEQ", 0))
    monotonic_fn = getattr(watchdog, "monotonic", None)
    try:
        mono_now = float(monotonic_fn()) if callable(monotonic_fn) else time.monotonic()
    except Exception:
        mono_now = time.monotonic()
    age_sec = float(row.get("age_sec") or 0.0)
    last_mono = float(last.get("ts") or 0.0)
    if seq > 0 and last_mono > 0:
        progress_age = max(0.0, mono_now - last_mono)
        age_sec = max(age_sec, progress_age)
        last_progress_ts = round(time.time() - progress_age, 3)
    elif age_sec > 0:
        last_progress_ts = round(time.time() - age_sec, 3)
    else:
        last_progress_ts = None
    try:
        liveness = bool(_main_loop_liveness_confirmed())
    except Exception:
        liveness = False
    status = str(row.get("status") or "UNKNOWN")
    grace_sec = float(row.get("grace_sec") or _MAIN_LOOP_LIVENESS_WINDOW_SEC)
    next_due_sec = row.get("next_due_sec")
    bounded_startup = bool(
        row and seq <= 0 and str(row.get("lifecycle") or "") == "BLOCKED"
        and status not in _MAIN_LOOP_FAILED_STATUSES
        and next_due_sec is not None and float(next_due_sec) > 0
    )
    if not row:
        state = HEALTH_MAIN_LOOP_UNKNOWN
        reason = "MAIN_LOOP_NOT_REGISTERED"
    elif bounded_startup:
        state = HEALTH_MAIN_LOOP_UNKNOWN
        reason = "MAIN_LOOP_BOUNDED_STARTUP"
    elif status in _MAIN_LOOP_FAILED_STATUSES or not liveness or age_sec > grace_sec:
        state = HEALTH_MAIN_LOOP_STALLED
        reason = str(row.get("reason") or diagnostics.get("verification_last_reason")
                     or "MAIN_LOOP_LIVENESS_NOT_CONFIRMED")
    else:
        state = HEALTH_MAIN_LOOP_RUNNING
        reason = ""
    return {
        "state": state,
        "bounded_startup": bounded_startup,
        "status": status,
        "lifecycle": row.get("lifecycle", "UNKNOWN"),
        "critical": bool(row.get("critical", True)),
        "reason": str(reason)[:200],
        "last_progress_ts": last_progress_ts,
        "last_progress_monotonic": last_mono or row.get("last_progress_monotonic"),
        "last_progress_age_sec": round(age_sec, 3),
        "grace_sec": round(grace_sec, 3),
        "next_due_sec": next_due_sec,
        "progress_seq": seq,
        "liveness_confirmed": liveness,
        "owner_thread_name": str(owner.get("thread_name") or ""),
        "owner_thread_registered": bool(owner.get("registered")),
        "owner_thread_alive": bool(_main_loop_owner_alive()) if callable(globals().get("_main_loop_owner_alive")) else False,
        "diagnostics": diagnostics,
    }


def _health_recovery_state(snapshot):
    """Report persisted/runtime recovery state from real recovery sources."""
    watchdog = globals().get("NAISO_WATCHDOG")
    threads = snapshot.get("threads") if isinstance(snapshot, dict) else None
    components = threads.get("components", {}) if isinstance(threads, dict) else {}
    row = components.get("main_loop") if isinstance(components, dict) else {}
    if not isinstance(row, dict):
        row = {}
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    recovery = snapshot.get("recovery") if isinstance(snapshot, dict) else {}
    if not isinstance(recovery, dict):
        recovery = {}
    restart = dict(globals().get("_MAIN_LOOP_RESTART_STATE") or {})
    status = str(MEMORY.get("recovery_status") or "")
    reason = str(STATE.get("recovery_reason") or recovery.get("reason")
                 or row.get("reason") or diagnostics.get("verification_last_reason") or "")
    failed = (
        bool(restart.get("supervisor_exhausted"))
        or status in _RECOVERY_FAILED_STATUSES
        or str(row.get("status") or "") in {"RESTART_FAILED", "RESTART_BUDGET_EXHAUSTED"}
        or _health_bool(diagnostics.get("verification_exhausted"))
    )
    in_progress = (
        bool(restart.get("in_progress"))
        or status in {"VERIFYING", "RECOVERY_IN_PROGRESS", "RECOVERY_HOLD"}
        or str(row.get("status") or "") == "STALLED_SAFE_HOLD"
        or bool(STATE.get("recovery_hold"))
    )
    if failed:
        state = HEALTH_RECOVERY_FAILED
    elif in_progress:
        state = HEALTH_RECOVERY_IN_PROGRESS
    else:
        state = HEALTH_RECOVERY_NONE
    return {
        "state": state,
        "reason": reason[:200],
        "hold": bool(recovery.get("hold", STATE.get("recovery_hold"))),
        "block_entries": bool(recovery.get("block_entries", globals().get("RECOVERY_BLOCK_ENTRIES", False))),
        "execution_unresolved": bool(recovery.get("execution_unresolved", STATE.get("execution_unresolved"))),
        "close_reconciliation_required": bool(recovery.get(
            "close_reconciliation_required", STATE.get("close_reconciliation_required"))),
        "status": status or None,
        "restart_in_progress": bool(restart.get("in_progress")),
        "supervisor_exhausted": bool(restart.get("supervisor_exhausted")),
        "verification_attempts": _health_int(diagnostics.get("verification_attempts")),
        "verification_exhausted": _health_bool(diagnostics.get("verification_exhausted")),
        "watchdog_running": bool(getattr(watchdog, "_thread", None) is not None),
    }


def _health_readiness(snapshot):
    """Return fail-closed readiness plus the explicit health-state vocabulary."""
    if not isinstance(snapshot, dict):
        snapshot = {}
    main_loop = _health_main_loop_state(snapshot)
    recovery = _health_recovery_state(snapshot)
    blockers = []
    process_failures = []
    if main_loop["state"] != HEALTH_MAIN_LOOP_RUNNING:
        blockers.append(main_loop["state"])
        if not main_loop.get("bounded_startup"):
            process_failures.append(main_loop["state"])
    if recovery["state"] == HEALTH_RECOVERY_FAILED:
        blockers.append(HEALTH_RECOVERY_FAILED)
        process_failures.append(HEALTH_RECOVERY_FAILED)
    elif recovery["state"] == HEALTH_RECOVERY_IN_PROGRESS:
        blockers.append(HEALTH_RECOVERY_IN_PROGRESS)
    if recovery["hold"]:
        blockers.append("RECOVERY_HOLD")
    if recovery["block_entries"]:
        blockers.append("ENTRY_BLOCK")
    if recovery["execution_unresolved"]:
        blockers.append("EXECUTION_UNRESOLVED")
    if recovery["close_reconciliation_required"]:
        blockers.append("CLOSE_RECONCILIATION_REQUIRED")
    watchlist_worker = snapshot.get("watchlist_worker") if isinstance(snapshot.get("watchlist_worker"), dict) else {}
    if watchlist_worker.get("in_progress"):
        blockers.append("UNIFIED_WATCHLIST_REFRESH_IN_PROGRESS")
    if watchlist_worker.get("stalled"):
        blockers.append("UNIFIED_WATCHLIST_WORKER_STALLED")
        process_failures.append("UNIFIED_WATCHLIST_WORKER_STALLED")
    positions = snapshot.get("positions") if isinstance(snapshot.get("positions"), dict) else {}
    if bool(positions.get("open")):
        # Open-position management is a trading restriction, not a process failure.
        blockers.append("OPEN_POSITION_MANAGEMENT")
    daily_loss = snapshot.get("daily_loss") if isinstance(snapshot.get("daily_loss"), dict) else {}
    if bool(daily_loss.get("active")):
        blockers.append("DAILY_LOSS_COOLDOWN")
    api = snapshot.get("api") if isinstance(snapshot.get("api"), dict) else {}
    counters = api.get("counters") if isinstance(api.get("counters"), dict) else {}
    if any(str((endpoint or {}).get("state", "")).upper() == "OPEN"
           for endpoint in counters.values() if isinstance(endpoint, dict)):
        blockers.append("API_CIRCUIT_OPEN")
    threads = snapshot.get("threads") if isinstance(snapshot.get("threads"), dict) else {}
    components = threads.get("components") if isinstance(threads.get("components"), dict) else {}
    for name, component in sorted(components.items()):
        if name == "main_loop" or not isinstance(component, dict) or not component.get("critical"):
            continue
        if str(component.get("status") or "") in _MAIN_LOOP_FAILED_STATUSES:
            blocker = f"CRITICAL_COMPONENT_STALLED:{name}"
            blockers.append(blocker)
            process_failures.append(blocker)
    readiness = HEALTH_TRADING_READY if not blockers else HEALTH_TRADING_BLOCKED
    return {
        "ok": readiness == HEALTH_TRADING_READY,
        "status": readiness,
        "readiness": readiness,
        "process": {"state": HEALTH_PROCESS_LIVE, "pid": os.getpid()},
        "main_loop": main_loop,
        "watchlist_worker": watchlist_worker,
        "recovery": recovery,
        "blockers": blockers,
        "process_failures": process_failures,
        "http_status": 503 if process_failures else 200,
    }


@app.route("/naiso/health")
def naiso_health():
    snapshot = naiso_health_snapshot()
    payload = safe_json(snapshot) if isinstance(snapshot, dict) else snapshot
    report = _health_readiness(snapshot)
    if isinstance(payload, dict):
        payload.update({key: value for key, value in report.items() if key != "http_status"})
    return jsonify(safe_json(payload)), int(report["http_status"])


@app.route("/liveness")
def liveness():
    """DigitalOcean liveness probe; restart only after the exchange exit gate approved it."""
    snapshot = naiso_health_snapshot()
    report = _health_readiness(snapshot)
    recovery = snapshot.get("recovery") if isinstance(snapshot.get("recovery"), dict) else {}
    verified_exit_states = {"EXIT_RECONCILED_FLAT", "EXIT_VERIFIED_PROTECTED_OPEN"}
    restart_authorized = str(recovery.get("status") or "").upper() in verified_exit_states
    should_restart = bool(report.get("process_failures")) and restart_authorized
    payload = {
        "ok": not should_restart,
        "process": report.get("process"),
        "main_loop": report.get("main_loop"),
        "watchlist_worker": report.get("watchlist_worker", {}),
        "restart_authorized_by_exchange_reconciliation": restart_authorized,
        "process_failures": report.get("process_failures", []),
    }
    return jsonify(safe_json(payload)), 503 if should_restart else 200


@app.route("/health")
def health():
    snapshot = naiso_health_snapshot()
    report = _health_readiness(snapshot)
    return jsonify(safe_json({
        "ok": report["ok"],
        "mode": "LIVE" if MODE_LIVE else "PAPER",
        "dashboard_ports": globals().get("DASHBOARD_ACTIVE_PORTS", []),
        "discovery_scanner": MEMORY.get("discovery_scanner", {}).get("state", "UNKNOWN"),
        "proximity_scanner": MEMORY.get("ema9_vwap_proximity_scanner_meta", {}).get("state", "UNKNOWN"),
        "trade_management_authority": "TradeManagementBoard / LiveTradeManager",
        "trade_management_events": len(TRADE_MANAGEMENT_LOG),
        "telegram": TELEGRAM_NOTIFIER.status(),
        "naiso": {
            "resources": snapshot.get("resources"),
            "threads": (snapshot["threads"]["components"] if snapshot.get("threads") else None),
            "api": snapshot["api"]["counters"] if snapshot.get("api") else None,
            "recovery": snapshot.get("recovery"),
            "native_sl": snapshot.get("positions"),
            "daily_loss": snapshot.get("daily_loss"),
            "restart_count": snapshot.get("restart_count", 0),
        },
        "status": report["status"],
        "readiness": report["readiness"],
        "process": report["process"],
        "main_loop": report["main_loop"],
        "watchlist_worker": report.get("watchlist_worker", {}),
        "recovery_state": report["recovery"],
        "blockers": report["blockers"],
        "process_failures": report["process_failures"],
    })), int(report["http_status"])

app.add_url_rule('/narrative-debug', 'narrative_debug', narrative_debug)

def keep_alive():
    stop_event = _PERIODIC_WORKER_STOPS.setdefault("keep_alive", threading.Event())
    while not stop_event.is_set():
        watchdog = globals().get("NAISO_WATCHDOG")
        if watchdog is not None:
            watchdog.beat("keep_alive")
        if stop_event.wait(max(0.1, float(KEEP_ALIVE_INTERVAL))):
            break
        ports = DASHBOARD_ACTIVE_PORTS or [int(os.getenv("PORT", "8000"))]
        for port in ports:
            try:
                requests.get(f"http://127.0.0.1:{port}/health", timeout=5)
            except Exception:
                continue

_last_cleanup = 0
def hourly_cleanup():
    """Bound actual root-level cache entries without wiping fresh live data."""
    global _last_cleanup
    now = time.time()
    if now - _last_cleanup < 300:
        return
    removed = 0
    ttl_by_prefix = {"ohlcv_": 180, "ticker_": 5, "ob_": 30,
                     "orderbook_": 30, "balance": 15, "free_balance": 15}
    with CACHE_LOCK:
        for key, item in list(CACHE.items()):
            if not (isinstance(item, dict) and "ts" in item and "value" in item):
                continue
            prefix = next((p for p in ttl_by_prefix if str(key).startswith(p)), None)
            ttl = ttl_by_prefix.get(prefix, 600)
            if now - float(item.get("ts", 0)) > ttl:
                CACHE.pop(key, None)
                removed += 1
        if len(CACHE) > CACHE_MAX_ITEMS:
            dated = sorted(((float(v.get("ts", 0)), k) for k, v in CACHE.items()
                            if isinstance(v, dict) and "ts" in v and "value" in v), key=lambda x: x[0])
            for _, key in dated[:max(0, len(CACHE) - CACHE_MAX_ITEMS)]:
                CACHE.pop(key, None)
                removed += 1
    manager = globals().get("INSTITUTIONAL_RESOURCE_MANAGER")
    if manager:
        try:
            metrics = manager.metrics()
        except Exception as exc:
            metrics = {"error": f"{type(exc).__name__}: {exc}"}
        MEMORY["resource_metrics"] = {**metrics, "cleanup_removed": removed}
    # Maintenance decides whether explicit GC, WAL checkpoint and journal
    # integrity work are safe. During an open position or unresolved recovery
    # only bounded cache pruning above is allowed.
    maintenance = globals().get("NAISO_MAINTENANCE")
    if maintenance is not None:
        try:
            result = maintenance.run(force=True)
            MEMORY["maintenance"] = maintenance.metrics()
            if result.get("journal_checkpoint"):
                MEMORY["journal_maintenance"] = {**result["journal_checkpoint"], "checked_at": now}
            elif result.get("status") in {"THROTTLED", "OK"} and not result.get("flags"):
                MEMORY.setdefault("journal_maintenance", {"deferred": True, "checked_at": now})
            if result.get("flags") and any(v for k, v in result["flags"].items() if k != "daily_loss_cooldown"):
                MEMORY["journal_maintenance"] = {
                    "deferred": True,
                    "reason": "trading_safety_gate_active",
                    "checked_at": now,
                    "flags": result["flags"],
                }
        except Exception as exc:
            log_execution(f"[MAINTENANCE] cleanup cycle failed: {type(exc).__name__}: {exc}", "WARN")
    else:
        journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
        unsafe = bool(STATE.get("open") or STATE.get("recovery_hold")
                      or STATE.get("execution_unresolved")
                      or STATE.get("close_reconciliation_required"))
        if not unsafe and journal is not None and callable(getattr(journal, "checkpoint_wal", None)):
            try:
                checkpoint = journal.checkpoint_wal("PASSIVE")
                MEMORY["journal_maintenance"] = {**checkpoint, "checked_at": now}
            except Exception as exc:
                log_execution(f"[JOURNAL] passive WAL checkpoint failed: {type(exc).__name__}: {exc}", "WARN")
        else:
            MEMORY["journal_maintenance"] = {"deferred": True, "reason": "trading_safety_gate_active",
                                             "checked_at": now}

    _last_cleanup = now
    log_execution(f"[RESOURCE] bounded cleanup removed={removed} cache={len(CACHE)}", "INFO")

_last_snapshot_time = 0
def print_snapshot():
    global _last_snapshot_time
    now = time.time()
    if now - _last_snapshot_time < SNAPSHOT_INTERVAL:
        return
    _last_snapshot_time = now
    bal = get_balance_safe()
    free_bal = get_free_balance_safe()
    mode = "LIVE" if MODE_LIVE else "PAPER"
    perf = get_dashboard_metrics()
    print("\n" + "="*70)
    print(color_text(f"🔥 RF v28.1 Professional Trade Mgmt ({mode}) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", BOLD))
    print(f"💰 Balance (Total): {color_text(f'{bal:.2f} USDT', GREEN)}   Free: {color_text(f'{free_bal:.2f} USDT', GREEN)}")
    print(f"📊 Total PnL: {color_text(perf['total_pnl'], GREEN if perf['total_pnl'].startswith('+') else RED)} | Last Trade: {perf['last_trade']}")
    regime = MEMORY.get("regime", "RANGE")
    regime_color = CYAN if regime == "TREND" else YELLOW
    print(f"🧠 Regime: {color_text(regime, regime_color)} | Scanned: {len(MEMORY.get('top_candidates', []))}")
    print_rf_dashboard()
    buy = MEMORY.get("scanner_v2_buy", [])
    sell = MEMORY.get("scanner_v2_sell", [])
    if buy or sell:
        print(color_text("=== Smart Scanner v2 (Ranked) ===", MAGENTA))
        if buy:
            print(f"  BUY top: {buy[0]['symbol']} score={buy[0]['score']}")
        if sell:
            print(f"  SELL top: {sell[0]['symbol']} score={sell[0]['score']}")
    if STATE["open"]:
        roe = STATE.get("roe_pct", 0.0)
        roe_colored = color_pnl(roe)
        print(f"📊 POSITION: {STATE['current_symbol']} {STATE['side']} ({STATE.get('entry_type','?')} / {STATE.get('classification','?')})")
        print(f"   Entry: {STATE['entry']:.4f} | ROE: {roe_colored} ({LEVERAGE}x leveraged)")
        print(f"   SL: {STATE.get('synthetic_sl',0):.4f} | TP1: {STATE.get('synthetic_tp1',0):.4f} | TP2: {STATE.get('tp2_price',0):.4f}")
        print(f"   Narrative: {STATE.get('narrative_classification','N/A')} (Conf: {STATE.get('narrative_confidence',0):.1f}) | Conf Level: {STATE.get('confidence_level','')}")
        cp = STATE.get("continuation_probability", 0.5)
        hq = STATE.get("hold_quality", "UNKNOWN")
        print(f"   Continuation: {cp*100:.1f}% | Hold Quality: {hq}")
        print(f"   Current Confidence: {STATE.get('current_confidence',50):.1f} | Market Regime: {STATE.get('market_regime','UNKNOWN')} | Cont. Pressure: {STATE.get('continuation_pressure',50)}")
        print(f"   Trade State: {STATE.get('trade_state','RANGE_CHOP')} | Trail Mult: {STATE.get('smart_trail_mult',1.5)} | Delay TP1: {STATE.get('delay_tp1',False)}")
        if STATE.get("tp1_hit"): print(color_text(f"   ✅ TP1 achieved - partial close, SL to breakeven", GREEN))
        if DASHBOARD_STATE.get("live_trade_mode", False):
            print(color_text(f"   [LIVE MGMT] State: {_live_manager.lifecycle_state.value}", MAGENTA))
    else:
        print("📊 POSITION: None")
    print("="*70 + "\n")

def print_rf_dashboard():
    print("\n" + color_text("=== RF TRIGGER CANDIDATES (Top 20) ===", MAGENTA))
    for item in MEMORY.get("rf_dashboard", [])[:20]:
        signal_icon = "🟢" if item["signal"] == "BUY" else "🔴" if item["signal"] == "SELL" else "⚪"
        print(f"{item['icon']} {signal_icon} {item['symbol']} | {item['status']} | score={item['score']:.2f} | ADX={item['adx']:.1f} | RSI={item['rsi']:.1f}")
    print("")

def build_rf_dashboard(candidates=None):
    dashboard = []
    candidates = list(candidates) if candidates is not None else scan_market_rf(top_n=30)
    for c in candidates:
        dashboard.append({
            "symbol": c["symbol"],
            "status": c.get("status", "PROXIMITY"),
            "icon": "🔔" if c["rf_triggered"] else "📡",
            "score": c["score"],
            "adx": c["adx"],
            "rsi": c["rsi"],
            "atrp": c["atrp"],
            "signal": c["rf_signal"] or "N/A"
        })
    MEMORY["rf_dashboard"] = dashboard
    return dashboard

def run_scanner_v2(symbols=None):
    try:
        buy, sell = smart_scanner_v2(symbols=symbols)
        MEMORY["scanner_v2_buy"] = buy
        MEMORY["scanner_v2_sell"] = sell
        MEMORY["scanner_v2_last_scan"] = time.time()
        log_execution(f"[SCANNER] TOP BUY updated: {len(buy)} candidates", "INFO")
        log_execution(f"[SCANNER] TOP SELL updated: {len(sell)} candidates", "INFO")
    except Exception as e:
        log_execution(f"Smart Scanner v2 error: {e}", "ERROR")

# ========== SNIPER V2 ==========
SNIPER_ZONES = {}

# ========== SNIPER V2 — CLOSED-CANDLE, RORO-AUTHORIZED ==========
# Stateful zone watcher. It is a signal provider only: actual orders still pass
# through execute_entry(), which remains the only entry gateway.
SNIPER_ZONES = {}
SNIPER_V2_LAST_PIVOT_TS = {}
SNIPER_V2_ENABLED = _env_bool("SNIPER_V2_ENABLED", True)
SNIPER_V2_SCAN_INTERVAL = _env_int_range("SNIPER_V2_SCAN_INTERVAL", 15, 5, 120)
SNIPER_V2_MAX_SYMBOLS = _env_int_range("SNIPER_V2_MAX_SYMBOLS", 8, 1, 20)
SNIPER_V2_PIVOT_LOOKBACK = _env_int_range("SNIPER_V2_PIVOT_LOOKBACK", 3, 2, 8)
SNIPER_V2_ZONE_ATR_WIDTH = _env_float_range("SNIPER_V2_ZONE_ATR_WIDTH", 0.50, 0.15, 1.50)
SNIPER_V2_SL_ATR_BUFFER = _env_float_range("SNIPER_V2_SL_ATR_BUFFER", 0.40, 0.10, 1.00)
SNIPER_V2_TP1_MOVE_PCT = 0.40
SNIPER_V2_TP2_MOVE_PCT = 1.00
SNIPER_V2_ZONE_MAX_AGE_SEC = _env_int_range("SNIPER_V2_ZONE_MAX_AGE_SEC", 14400, 900, 86400)
SNIPER_V2_CONFIRMATION_WINDOW_BARS = _env_int_range("SNIPER_V2_CONFIRMATION_WINDOW_BARS", 3, 1, 10)
SNIPER_V2_MAX_ENTRY_DISTANCE_ATR = _env_float_range("SNIPER_V2_MAX_ENTRY_DISTANCE_ATR", 0.75, 0.20, 2.00)
_SNIPER_V2_LAST_SCAN = 0.0
_SNIPER_V2_LOCK = threading.Lock()
SNIPER_V2_METRICS = {
    "state": "DISABLED" if not SNIPER_V2_ENABLED else "IDLE",
    "symbols_scanned": 0, "zones_waiting": 0, "zones_ready": 0,
    "entries": 0, "last_scan": 0.0, "last_error": None,
}


def _sniper_v2_wallet_balances(*, wallet_snapshot=None, paper_wallet_balance=0.0, paper_mode=False):
    """Return (total wallet USDT, free USDT, source); total sizes, free only gates execution."""
    if paper_mode:
        total = safe_float(paper_wallet_balance, 0.0)
        free = total
        source = "PAPER_WALLET_TOTAL"
    else:
        if not isinstance(wallet_snapshot, dict):
            raise ValueError("SNIPER_WALLET_BALANCE_UNAVAILABLE")
        total = safe_float((wallet_snapshot.get("total") or {}).get("USDT"), 0.0)
        free = safe_float((wallet_snapshot.get("free") or {}).get("USDT"), 0.0)
        source = "BINGX_FUTURES_FREE_USDT"
    if not math.isfinite(total) or total <= 0:
        raise ValueError("SNIPER_WALLET_TOTAL_INVALID")
    if not math.isfinite(free) or free < 0:
        raise ValueError("SNIPER_FREE_MARGIN_INVALID")
    return total, free, source


def is_pivot_high(df, lookback=None):
    """A pivot high is valid only after configured right-side candles are closed."""
    n = int(lookback or SNIPER_V2_PIVOT_LOOKBACK)
    if df is None or len(df) < (2 * n + 1):
        return False
    idx = len(df) - n - 1
    pivot = float(df["high"].iloc[idx])
    left = df["high"].iloc[idx - n:idx]
    right = df["high"].iloc[idx + 1:idx + n + 1]
    return bool(pivot > float(left.max()) and pivot >= float(right.max()))


def is_pivot_low(df, lookback=None):
    """A pivot low is valid only after configured right-side candles are closed."""
    n = int(lookback or SNIPER_V2_PIVOT_LOOKBACK)
    if df is None or len(df) < (2 * n + 1):
        return False
    idx = len(df) - n - 1
    pivot = float(df["low"].iloc[idx])
    left = df["low"].iloc[idx - n:idx]
    right = df["low"].iloc[idx + 1:idx + n + 1]
    return bool(pivot < float(left.min()) and pivot <= float(right.min()))


def detect_strong_pivot(df, side, atr):
    """Confirm displacement/rejection/extension using closed candles."""
    if df is None or len(df) < 55 or not np.isfinite(float(atr)) or float(atr) <= 0:
        return False, []
    last = df.iloc[-1]
    body = abs(float(last["close"]) - float(last["open"]))
    reasons = []
    if str(side).upper() == "TOP":
        move = float(last["high"]) - float(df["low"].iloc[-6])
        wick = float(last["high"]) - max(float(last["open"]), float(last["close"]))
        if move > atr * 1.5:
            reasons.append("strong_move_up")
    else:
        move = float(df["high"].iloc[-6]) - float(last["low"])
        wick = min(float(last["open"]), float(last["close"])) - float(last["low"])
        if move > atr * 1.5:
            reasons.append("strong_move_down")
    if body > 0 and wick > body:
        reasons.append("rejection_wick")
    ema50 = ema(df["close"], 50).iloc[-1]
    if np.isfinite(ema50) and ema50 > 0 and abs(float(last["close"]) - float(ema50)) > atr:
        reasons.append("overextended")
    return len(set(reasons)) >= 2, reasons


def check_sniper_confirmation(df, zone_type, zone=None):
    """Closed-candle rejection and reclaim; no forming-candle confirmation."""
    if df is None or len(df) < 2 or not isinstance(zone, dict):
        return False, 0
    last, prev = df.iloc[-1], df.iloc[-2]
    body = abs(float(last["close"]) - float(last["open"]))
    candle_range = float(last["high"]) - float(last["low"])
    if candle_range <= 0:
        return False, 0
    checks = 0
    if str(zone_type).upper() == "TOP":
        wick = float(last["high"]) - max(float(last["open"]), float(last["close"]))
        if wick >= max(body * 0.5, candle_range * 0.20):
            checks += 1
        if float(last["high"]) > float(zone["high"]) and float(last["close"]) < float(zone["high"]):
            checks += 1
        if float(last["close"]) < float(last["open"]) and float(prev["close"]) >= float(prev["open"]):
            checks += 1
    else:
        wick = min(float(last["open"]), float(last["close"])) - float(last["low"])
        if wick >= max(body * 0.5, candle_range * 0.20):
            checks += 1
        if float(last["low"]) < float(zone["low"]) and float(last["close"]) > float(zone["low"]):
            checks += 1
        if float(last["close"]) > float(last["open"]) and float(prev["close"]) <= float(prev["open"]):
            checks += 1
    return checks >= 2, checks


_SNIPER_V2_SYMBOL_CURSOR = 0


def _sniper_v2_symbols():
    """Build a rotating bounded batch from the live unified Watchlist.

    Legacy Radar and scanner feeds are fallbacks only. The current unified
    Watchlist is the primary universe, so Sniper observes the same symbols
    already enriched by the production discovery pipeline.
    """
    global _SNIPER_V2_SYMBOL_CURSOR
    now = time.time()
    candidates = {}

    def add_candidate(raw_symbol, score=0.0):
        if not raw_symbol:
            return
        exchange_symbol = roro_exchange_symbol(raw_symbol)
        key = roro_canonical_symbol(exchange_symbol)
        if not key:
            return
        try:
            rank = float(score or 0.0)
        except (TypeError, ValueError, OverflowError):
            rank = 0.0
        current = candidates.get(key)
        if current is None or rank > current[0]:
            candidates[key] = (rank, exchange_symbol)

    # Primary: active unified institutional Watchlist entries.
    watchlist = MEMORY.get("watchlist", {})
    if isinstance(watchlist, dict):
        terminal_states = {
            "REJECTED", "HARD_REJECT", "INVALIDATED", "EXPIRED",
            "NO_DATA", "STALE", "ENTRY_WINDOW_MISSED",
        }
        for canonical, item in watchlist.items():
            if not isinstance(item, dict):
                continue
            state = str(item.get("decision_state") or item.get("state") or "").upper()
            if state in terminal_states:
                continue
            updated = safe_float(
                item.get("last_update", item.get("last_updated", item.get("updated_at", 0.0))),
                0.0,
            )
            # Ignore old residual Watchlist rows; fresh discovery refreshes these.
            if updated > 0 and now - updated > 1800:
                continue
            raw = item.get("exchange_symbol") or item.get("symbol") or canonical
            add_candidate(raw, item.get("score", item.get("discovery_score", 0.0)))

    # Fallbacks keep the feature useful during Watchlist warm-up without
    # introducing a new broad-market scan or extra exchange API fan-out.
    fallback_feeds = (
        MEMORY.get("radar_top5") or [],
        MEMORY.get("radar_watchlist") or [],
        MEMORY.get("scanner_v2_buy") or [],
        MEMORY.get("scanner_v2_sell") or [],
        MEMORY.get("rf_watchlist") or [],
    )
    if not candidates:
        for feed in fallback_feeds:
            for row in feed:
                if not isinstance(row, dict):
                    continue
                raw = row.get("exchange_symbol") or row.get("symbol")
                add_candidate(raw, row.get("score", row.get("discovery_score", 0.0)))

    ranked = [value[1] for _, value in sorted(
        candidates.items(), key=lambda pair: pair[1][0], reverse=True
    )]
    if not ranked:
        return []
    batch_size = min(SNIPER_V2_MAX_SYMBOLS, len(ranked))
    start = _SNIPER_V2_SYMBOL_CURSOR % len(ranked)
    selected = [ranked[(start + offset) % len(ranked)] for offset in range(batch_size)]
    _SNIPER_V2_SYMBOL_CURSOR = (start + batch_size) % len(ranked)
    return selected


def _sniper_v2_update_zone(symbol, closed):
    """Create/update a persistent zone from closed candles; never places an order.

    Lifecycle is deliberately explicit:
      PIVOT_CONFIRMED -> WAIT_FOR_DEPARTURE -> WAIT_FOR_RETEST -> READY.
    A candle that created/confirmed the pivot cannot also count as its retest.
    """
    key = roro_canonical_symbol(symbol)
    if not key or closed is None or len(closed) < 80:
        return None
    try:
        candle_ts = int(closed.timestamp.iloc[-1])
        price = float(closed.close.iloc[-1])
        previous_close = float(closed.close.iloc[-2])
    except (AttributeError, IndexError, TypeError, ValueError, OverflowError):
        return None
    if candle_ts <= 0 or not np.isfinite(price) or price <= 0:
        return None
    atr_series = compute_atr(closed, 14)
    atr_value = float(atr_series.iloc[-1]) if len(atr_series) else float("nan")
    if not np.isfinite(atr_value) or atr_value <= 0:
        return None

    zone = SNIPER_ZONES.get(key)
    if isinstance(zone, dict):
        # One transition pass per confirmed closed candle, not every 15-second scan.
        last_processed_ts = int(zone.get("last_candle_ts", 0) or 0)
        if candle_ts <= last_processed_ts:
            return zone

        zone["last_candle_ts"] = candle_ts
        age = time.time() - float(zone.get("created_at", time.time()))
        if age > SNIPER_V2_ZONE_MAX_AGE_SEC:
            zone["state"] = "EXPIRED"

        zone_type = str(zone.get("type", "")).upper()
        zone_low = float(zone.get("low", 0.0))
        zone_high = float(zone.get("high", 0.0))
        if zone_type not in {"TOP", "BOTTOM"} or zone_low <= 0 or zone_high <= zone_low:
            zone["state"] = "INVALIDATED"

        if zone.get("state") == "WAIT":
            if zone_type == "TOP":
                if price > zone_high + atr_value * 0.20:
                    zone["state"] = "INVALIDATED"
                elif not zone.get("departed") and price < zone_low - atr_value * 0.50:
                    zone["departed"] = True
                    zone["departure_candle_ts"] = candle_ts
                    log_execution(f"[SNIPER_V2] {key} TOP departure confirmed; waiting for retest", "INFO")
                elif (
                    zone.get("departed")
                    and int(zone.get("departure_candle_ts", 0)) < candle_ts
                    and previous_close < zone_low
                    and float(closed.high.iloc[-1]) >= zone_low
                    and zone_low <= price <= zone_high
                ):
                    zone["state"] = "READY"
                    zone["retest_candle_ts"] = candle_ts
                    log_execution(f"[SNIPER_V2] {key} TOP retest confirmed; state=READY", "INFO")
            else:
                if price < zone_low - atr_value * 0.20:
                    zone["state"] = "INVALIDATED"
                elif not zone.get("departed") and price > zone_high + atr_value * 0.50:
                    zone["departed"] = True
                    zone["departure_candle_ts"] = candle_ts
                    log_execution(f"[SNIPER_V2] {key} BOTTOM departure confirmed; waiting for retest", "INFO")
                elif (
                    zone.get("departed")
                    and int(zone.get("departure_candle_ts", 0)) < candle_ts
                    and previous_close > zone_high
                    and float(closed.low.iloc[-1]) <= zone_high
                    and zone_low <= price <= zone_high
                ):
                    zone["state"] = "READY"
                    zone["retest_candle_ts"] = candle_ts
                    log_execution(f"[SNIPER_V2] {key} BOTTOM retest confirmed; state=READY", "INFO")

        elif zone.get("state") == "READY":
            # Do not keep a stale retest authorization indefinitely.
            retest_ts = int(zone.get("retest_candle_ts", 0) or 0)
            max_age_ms = int(_roro_timeframe_seconds(INTERVAL) * 1000 * SNIPER_V2_CONFIRMATION_WINDOW_BARS)
            if retest_ts <= 0 or candle_ts - retest_ts > max_age_ms:
                zone["state"] = "EXPIRED"
            elif zone_type == "TOP" and price > zone_high + atr_value * 0.20:
                zone["state"] = "INVALIDATED"
            elif zone_type == "BOTTOM" and price < zone_low - atr_value * 0.20:
                zone["state"] = "INVALIDATED"

        if zone.get("state") in {"EXPIRED", "INVALIDATED", "USED", "NO_CHASE"}:
            SNIPER_ZONES.pop(key, None)
            zone = None
        else:
            return zone

    if zone is None:
        n = SNIPER_V2_PIVOT_LOOKBACK
        if is_pivot_high(closed, n):
            pivot_idx = len(closed) - n - 1
            pivot_ts = int(closed.timestamp.iloc[pivot_idx])
            if int(SNIPER_V2_LAST_PIVOT_TS.get(key, 0)) == pivot_ts:
                return None
            pivot_frame = closed.iloc[:pivot_idx + 1]
            pivot_atr_series = compute_atr(pivot_frame, 14)
            pivot_atr = float(pivot_atr_series.iloc[-1]) if len(pivot_atr_series) else float("nan")
            strong, reasons = detect_strong_pivot(pivot_frame, "TOP", pivot_atr)
            if strong and np.isfinite(pivot_atr) and pivot_atr > 0:
                pivot_high = float(closed.high.iloc[pivot_idx])
                zone = {
                    "symbol": symbol, "canonical_symbol": key, "type": "TOP",
                    "high": pivot_high, "low": pivot_high - pivot_atr * SNIPER_V2_ZONE_ATR_WIDTH,
                    "pivot_timestamp": pivot_ts,
                    "created_at": time.time(), "last_candle_ts": candle_ts,
                    "state": "WAIT", "departed": False, "reasons": reasons,
                }
                # Right-side closed candles may already have moved price away.
                # Record the departure, but never mark READY on this same candle.
                if price < float(zone["low"]) - atr_value * 0.50:
                    zone["departed"] = True
                    zone["departure_candle_ts"] = candle_ts
        elif is_pivot_low(closed, n):
            pivot_idx = len(closed) - n - 1
            pivot_ts = int(closed.timestamp.iloc[pivot_idx])
            if int(SNIPER_V2_LAST_PIVOT_TS.get(key, 0)) == pivot_ts:
                return None
            pivot_frame = closed.iloc[:pivot_idx + 1]
            pivot_atr_series = compute_atr(pivot_frame, 14)
            pivot_atr = float(pivot_atr_series.iloc[-1]) if len(pivot_atr_series) else float("nan")
            strong, reasons = detect_strong_pivot(pivot_frame, "BOTTOM", pivot_atr)
            if strong and np.isfinite(pivot_atr) and pivot_atr > 0:
                pivot_low = float(closed.low.iloc[pivot_idx])
                zone = {
                    "symbol": symbol, "canonical_symbol": key, "type": "BOTTOM",
                    "high": pivot_low + pivot_atr * SNIPER_V2_ZONE_ATR_WIDTH, "low": pivot_low,
                    "pivot_timestamp": pivot_ts,
                    "created_at": time.time(), "last_candle_ts": candle_ts,
                    "state": "WAIT", "departed": False, "reasons": reasons,
                }
                if price > float(zone["high"]) + atr_value * 0.50:
                    zone["departed"] = True
                    zone["departure_candle_ts"] = candle_ts

        if zone:
            SNIPER_V2_LAST_PIVOT_TS[key] = int(zone["pivot_timestamp"])
            SNIPER_ZONES[key] = zone
            log_execution(
                f"[SNIPER_V2] {key} {zone['type']} zone created "
                f"pivot_ts={zone['pivot_timestamp']} bounds={zone['low']:.12g}-{zone['high']:.12g} "
                f"departed={bool(zone.get('departed'))}",
                "INFO",
            )
    return zone


def _sniper_v2_confirmation_due(zone, candle_ts):
    """Require a later closed candle after the retest candle for confirmation."""
    if not isinstance(zone, dict) or str(zone.get("state", "")).upper() != "READY":
        return False
    retest_ts = int(zone.get("retest_candle_ts", 0) or 0)
    current_ts = int(candle_ts or 0)
    if retest_ts <= 0 or current_ts <= retest_ts:
        return False
    max_age_ms = int(_roro_timeframe_seconds(INTERVAL) * 1000 * SNIPER_V2_CONFIRMATION_WINDOW_BARS)
    return (current_ts - retest_ts) <= max_age_ms


def _sniper_v2_structural_stop(side, zone, atr_value, roro_stop):
    """Keep the RORO stop at least as far beyond the pivot zone as configured."""
    direction = str(side or "").strip().upper()
    if direction not in {"BUY", "SELL"} or not isinstance(zone, dict):
        raise ValueError("invalid Sniper V2 stop inputs")
    atr = float(atr_value)
    authority_stop = float(roro_stop)
    zone_low = float(zone.get("low"))
    zone_high = float(zone.get("high"))
    if not all(np.isfinite(v) and v > 0 for v in (atr, authority_stop, zone_low, zone_high)):
        raise ValueError("Sniper V2 stop inputs must be finite and positive")
    if zone_high <= zone_low:
        raise ValueError("Sniper V2 zone geometry is invalid")
    buffer = atr * SNIPER_V2_SL_ATR_BUFFER
    if direction == "SELL":
        return max(authority_stop, zone_high + buffer)
    return min(authority_stop, zone_low - buffer)


def sniper_engine_v2():
    """One bounded scan cycle; execution stays on the unified GGP/RORO path."""
    if daily_loss_cooldown_active():
        SNIPER_V2_METRICS["state"] = "PAUSED_DAILY_LOSS_COOLDOWN"
        return False

    if not SNIPER_V2_ENABLED:
        SNIPER_V2_METRICS["state"] = "DISABLED"
        return False
    if (STATE.get("open") or TRADE_STATE.get("in_position")
            or globals().get("RECOVERY_BLOCK_ENTRIES", False)
            or MEMORY.get("position_status") == "UNKNOWN"):
        SNIPER_V2_METRICS["state"] = "PAUSED_FOR_POSITION_OR_RECOVERY"
        return False
    if not _SNIPER_V2_LOCK.acquire(blocking=False):
        return False
    try:
        symbols = _sniper_v2_symbols()
        SNIPER_V2_METRICS.update({
            "state": "RUNNING", "last_scan": time.time(), "symbols_scanned": 0,
            "zones_waiting": 0, "zones_ready": 0, "last_error": None,
        })
        _scanner_progress("sniper_v2", "universe_ready", symbols=len(symbols))
        for symbol_index, symbol in enumerate(symbols, start=1):
            if STATE.get("open") or TRADE_STATE.get("in_position"):
                _scanner_progress("sniper_v2", "yielded_to_open_position", symbol=symbol)
                break
            _scanner_progress("sniper_v2", "symbol_start", symbol=symbol, index=symbol_index)
            raw_frame = _get_scanner_frame(symbol, 150)
            _scanner_progress("sniper_v2", "ohlcv_complete", symbol=symbol,
                              available=bool(raw_frame is not None))
            if raw_frame is None or not validate_dataframe(raw_frame, 80):
                continue
            closed = roro_validate_closed_frame(
                raw_frame, now_ms=int(time.time() * 1000),
                timeframe_s=_roro_timeframe_seconds(INTERVAL),
            )
            if closed is None or len(closed) < 80:
                continue
            key = roro_canonical_symbol(symbol)
            zone = _sniper_v2_update_zone(symbol, closed)
            SNIPER_V2_METRICS["symbols_scanned"] += 1
            if not isinstance(zone, dict):
                continue
            zone_state = str(zone.get("state", ""))
            if zone_state == "WAIT":
                SNIPER_V2_METRICS["zones_waiting"] += 1
                continue
            if zone_state != "READY":
                continue
            SNIPER_V2_METRICS["zones_ready"] += 1
            candle_ts = int(closed.timestamp.iloc[-1])
            if not _sniper_v2_confirmation_due(zone, candle_ts):
                continue
            if int(zone.get("confirmation_candle_ts", 0)) == candle_ts:
                continue
            # Mark before evaluation so a closed candle is evaluated once only.
            zone["confirmation_candle_ts"] = candle_ts
            confirmed, count = check_sniper_confirmation(closed, zone["type"], zone)
            zone["confirmation_count"] = count
            if not confirmed:
                continue
            side = "SELL" if zone["type"] == "TOP" else "BUY"
            live_price = get_ticker_safe(symbol)
            if not live_price or not np.isfinite(float(live_price)) or float(live_price) <= 0:
                log_execution(f"[SNIPER_V2] {key} rejected: live_price_unavailable", "WARN")
                continue
            price = float(live_price)
            atr_value = float(compute_atr(closed, 14).iloc[-1])
            if not np.isfinite(atr_value) or atr_value <= 0:
                continue
            distance_atr = abs(price - float(closed.close.iloc[-1])) / atr_value
            if distance_atr > SNIPER_V2_MAX_ENTRY_DISTANCE_ATR:
                zone["state"] = "NO_CHASE"
                zone["no_chase_reason"] = "live_price_too_far_from_closed_confirmation"
                log_execution(f"[SNIPER_V2] {key} NO_CHASE distance_atr={distance_atr:.3f}", "WARN")
                continue
            authority = roro_analyze_symbol_both(
                symbol, closed, now_ms=int(time.time() * 1000), side_hints=[side]
            )
            _scanner_progress("sniper_v2", "roro_authority_complete", symbol=symbol,
                              decision=str(authority.get("decision_state") or "UNKNOWN"))
            if authority.get("decision_state") != "ENTRY_READY" or str(authority.get("side")).upper() != side:
                log_execution(
                    f"[SNIPER_V2] {key} blocked by RORO authority: "
                    f"{authority.get('decision_reason') or authority.get('rejection_reasons')}",
                    "INFO",
                )
                continue
            event_id = str(authority.get("event_id") or "")
            evidence = list(authority.get("evidence") or [])
            setup = str(authority.get("decision_setup_type") or authority.get("setup_type") or "").upper()
            if not event_id or not setup or not evidence:
                log_execution(f"[SNIPER_V2] {key} blocked: incomplete_RORO_authorization", "WARN")
                continue
            if INSTITUTIONAL_TRADE_JOURNAL and INSTITUTIONAL_TRADE_JOURNAL.is_event_consumed(event_id):
                log_execution(f"[SNIPER_V2] {key} blocked: duplicate_RORO_event={event_id}", "WARN")
                continue
            if "REVERSAL" not in setup and setup != "FAILED_AUCTION_REVERSAL":
                log_execution(f"[SNIPER_V2] {key} blocked: RORO_setup_not_reversal ({setup})", "INFO")
                continue
            structural_stop = authority.get("structural_stop")
            if structural_stop is None or not np.isfinite(float(structural_stop)):
                log_execution(f"[SNIPER_V2] {key} blocked: structural_stop_missing", "WARN")
                continue
            structural_stop = _sniper_v2_structural_stop(side, zone, atr_value, float(structural_stop))
            if (side == "SELL" and structural_stop <= price) or (side == "BUY" and structural_stop >= price):
                log_execution(f"[SNIPER_V2] {key} blocked: invalid_RORO_stop_geometry", "WARN")
                continue
            tp1_move, tp2_move = SNIPER_V2_TP1_MOVE_PCT / 100.0, SNIPER_V2_TP2_MOVE_PCT / 100.0
            tp1 = price * (1.0 - tp1_move if side == "SELL" else 1.0 + tp1_move)
            tp2 = price * (1.0 - tp2_move if side == "SELL" else 1.0 + tp2_move)
            _INSTITUTIONAL_ENTRY_CONTEXT = {
                "setup_type": setup, "event_id": event_id, "evidence": evidence,
                "structural_stop": structural_stop, "pullback_type": "SNIPER_ZONE_RETEST",
                "trading_mode": "SNIPER",
                "entry_behavior": "RETEST",
                "entry_zone_low": safe_float(zone.get("low"), 0.0),
                "entry_zone_high": safe_float(zone.get("high"), 0.0),
                "entry_behavior_confidence": 0.88,
                "entry_behavior_reasons": ["sniper_pivot_retest", "liquidity_mss_confirmed"],
                "market_intelligence": {
                    "mode": "SNIPER", "entry_behavior": "RETEST",
                    "reference_low": safe_float(zone.get("low"), 0.0),
                    "reference_high": safe_float(zone.get("high"), 0.0),
                    "zone_side": side,
                },
                "exchange_symbol": symbol, "sniper_v2": True,
                "capital_allocation_tier": "STANDARD_20_PERCENT",
                "sizing_eligible_strong": False, "sizing_balance_basis": "WALLET_TOTAL",
                "candle_timeframe": str(INTERVAL),
                "closed_candle_frame": closed.copy(),
                "closed_candle_source": "SNIPER_V2_CONFIRMED_FRAME",
                "closed_candle_timestamp": int(closed["timestamp"].iloc[-1]),
                "data_freshness": "FRESH",
                "data_quality": "VALID_CLOSED_CANDLES",
                "last_updated": float(int(closed["timestamp"].iloc[-1]) / 1000.0),
                "timeframe_ms": _roro_timeframe_seconds(INTERVAL) * 1000.0,
            }
            try:
                _scanner_progress("sniper_v2", "entry_authorization", symbol=symbol, side=side)
                ok = execute_entry(
                    side, symbol, price, structural_stop, float(tp1), float(tp2), 9.0,
                    f"SNIPER_V2 {zone['type']} PIVOT_RETEST confirmation={count} "
                    f"zone={zone['low']:.12g}-{zone['high']:.12g} RORO={setup}",
                    atr_value, trade_type="SNIPER_V2", entry_type="SNIPER_V2",
                    classification="SNIPER_V2",
                )
                _scanner_progress("sniper_v2", "entry_execution_complete",
                                  symbol=symbol, executed=bool(ok))
            finally:
                _INSTITUTIONAL_ENTRY_CONTEXT = None
            if ok:
                zone["state"] = "USED"
                SNIPER_ZONES.pop(key, None)
                SNIPER_V2_METRICS["entries"] += 1
                if INSTITUTIONAL_TRADE_JOURNAL:
                    _persist_trade_journal_start(setup, authority)
                    trade_id = str(STATE.get("trade_id") or "")
                    if trade_id:
                        INSTITUTIONAL_TRADE_JOURNAL.consume_event(event_id, trade_id)
                RORO_ENTRY_AUTHORITY.consume(event_id)
                log_execution(
                    f"[SNIPER_V2] EXECUTED {side} {symbol} event={event_id} "
                    f"margin_target=20% wallet leverage={LEVERAGE}x TP1=0.4% TP2=1.0%",
                    "SUCCESS",
                )
                return True
        SNIPER_V2_METRICS["state"] = "IDLE"
        return False
    except Exception as exc:
        SNIPER_V2_METRICS.update({
            "state": "ERROR", "last_error": f"{type(exc).__name__}: {str(exc)[:180]}",
        })
        log_execution(f"[SNIPER_V2] scan failed: {type(exc).__name__}: {exc}", "ERROR")
        return False
    finally:
        _SNIPER_V2_LOCK.release()


def _run_sniper_scan_if_due():
    """Dispatch Sniper discovery off MainThread; execution still uses unified entry authority."""
    global _SNIPER_V2_LAST_SCAN
    if not SNIPER_V2_ENABLED:
        return False
    now = time.time()
    if now - _SNIPER_V2_LAST_SCAN < SNIPER_V2_SCAN_INTERVAL:
        return False
    dispatched = dispatch_scanner_task("sniper_v2", sniper_engine_v2)
    if dispatched:
        _SNIPER_V2_LAST_SCAN = now
    return dispatched


# ========== SCANNER CONTEXT MODE – update institutional flow without a trade ==========
def update_institutional_flow_scanner():
    try:
        df = get_ohlcv_safe(DEFAULT_SYMBOL, 100)
        if df is None or not validate_dataframe(df, 80):
            log_execution("[SCANNER] No valid data for institutional flow update, using defaults", "WARN")
            DASHBOARD_STATE["institutional_flow"] = {
                "banker_pressure": 50.0, "retailer_pressure": 50.0, "hot_money": 50.0,
                "institutional_bias": "NEUTRAL", "institutional_bias_detailed": "NEUTRAL",
                "flow_alignment": 25.0, "distribution_risk": 0.0,
                "momentum_health": 50.0, "continuation_strength": 0.0, "exhaustion_risk": 0.0,
                "climax_risk": 0.0, "greed_state": False, "smart_money_dominant": False
            }
            return
        smart = SmartMoneyEngine.analyze_smart_money(df)
        mom = MomentumFlowEngine.analyze_momentum_flow(df)
        DASHBOARD_STATE["institutional_flow"] = {
            "banker_pressure": smart["banker_pressure"],
            "retailer_pressure": smart["retailer_pressure"],
            "hot_money": smart["hot_money_pressure"],
            "institutional_bias": smart["institutional_bias"],
            "institutional_bias_detailed": smart.get("institutional_bias_detailed", "NEUTRAL"),
            "flow_alignment": smart["flow_alignment"],
            "distribution_risk": smart["distribution_risk"],
            "momentum_health": mom["momentum_health"],
            "continuation_strength": mom["continuation_strength"],
            "exhaustion_risk": mom["exhaustion_risk"],
            "climax_risk": mom["climax_risk"],
            "greed_state": mom["greed_state"],
            "smart_money_dominant": smart["smart_money_dominant"]
        }
        DASHBOARD_STATE["last_live_refresh"] = time.time()
        log_execution(f"[SCANNER] Institutional flow updated: bias={smart['institutional_bias_detailed']}, dominance={smart['smart_money_dominant']}", "INFO")
    except Exception as e:
        log_execution(f"[SCANNER] Error updating institutional flow: {e}", "ERROR")

# ========== LIVE INSTITUTIONAL UPDATER THREAD (PAUSED DURING TRADE) ==========
def live_institutional_updater():
    stop_event = _PERIODIC_WORKER_STOPS.setdefault("institutional_updater", threading.Event())
    while not stop_event.is_set():
        try:
            watchdog = globals().get("NAISO_WATCHDOG")
            if watchdog is not None:
                watchdog.beat("institutional_updater", paused=bool(STATE.get("open")))
            # Do not run during open trade to avoid duplicate API calls and thread contention
            if STATE.get("open"):
                if stop_event.wait(5):
                    break
                continue
            symbol = DEFAULT_SYMBOL
            df = get_ohlcv_safe(symbol, 100)
            if df is not None and validate_dataframe(df, 80):
                smart = SmartMoneyEngine.analyze_smart_money(df)
                mom = MomentumFlowEngine.analyze_momentum_flow(df)
                with _TRADE_LOCK:
                    DASHBOARD_STATE["institutional_flow"] = {
                        "banker_pressure": smart["banker_pressure"],
                        "retailer_pressure": smart["retailer_pressure"],
                        "hot_money": smart["hot_money_pressure"],
                        "institutional_bias": smart["institutional_bias"],
                        "institutional_bias_detailed": smart.get("institutional_bias_detailed", "NEUTRAL"),
                        "flow_alignment": smart["flow_alignment"],
                        "distribution_risk": smart["distribution_risk"],
                        "momentum_health": mom["momentum_health"],
                        "continuation_strength": mom["continuation_strength"],
                        "exhaustion_risk": mom["exhaustion_risk"],
                        "climax_risk": mom["climax_risk"],
                        "greed_state": mom["greed_state"],
                        "smart_money_dominant": smart["smart_money_dominant"]
                    }
                DASHBOARD_STATE["last_live_refresh"] = time.time()
            else:
                update_institutional_flow_scanner()
        except Exception as e:
            log_execution(f"[LIVE_UPDATER] Error: {e}", "ERROR")
        if stop_event.wait(5):
            break

# ========== MAIN LOOP ==========
MEMORY = {
    "candidates": [],
    "top_candidates": [],
    "regime": "NEUTRAL",
    "last_scan": 0,
    "scanned_count": 0,
    "health": {"api": "OK", "errors": 0, "status": "RUNNING"},
    "rf_watchlist": [],
    "rf_dashboard": [],
    "scanner_v2_buy": [],
    "scanner_v2_sell": [],
    "scanner_v2_last_scan": 0,
    "professional_scan_candidates": [],
    "professional_scanner": {"state": "IDLE", "last_started": 0, "last_completed": 0},
    "discovery_scanner": {"state": "IDLE", "last_started": 0, "last_completed": 0, "symbols_scanned": 0},
    "ema9_vwap_proximity_scanner": [],
    "ema9_vwap_proximity_scanner_meta": {"state": "IDLE", "role": "RANKING_ONLY"},
    "radar_watchlist": [],
    "radar_top5": [],
    "log_debounce": {},
    "watchlist": {},
    "unified_watchlist_cursor": 0,
    "unified_watchlist_last_update": 0,
    "position_status": "UNKNOWN",
    "recovery_status": "NOT_STARTED",
    "no_entry_feed": []
}

# ========== SIX-SLOT PORTFOLIO INTEGRATION ==========
# All globals above (STATE / TRADE_STATE / DASHBOARD_STATE / MEMORY) and the
# singleton manager now exist, so the durable allocator and the isolated runtime
# registry can be bound once per process. Both share _TRADE_LOCK with the rest
# of the execution boundary.
_PORTFOLIO_TRUSTED_HINT_KEYS = (
    "strategy", "source", "strategy_name", "source_name", "entry_source",
    "signal_source", "signal_origin", "trade_type", "setup_type", "setup",
    "engine", "origin",
)
_PORTFOLIO_RAW_LABEL_KEYS = frozenset({
    "classification", "slot_class", "class_label", "label", "slot", "portfolio_class",
    "class_hint", "portfolio_slot", "slot_label",
})
_PORTFOLIO_UNCERTAIN_FLAGS = (
    "execution_unresolved", "close_reconciliation_required", "recovery_hold",
)
_PORTFOLIO_PENDING_ENTRY = None


def _portfolio_allocator_path():
    configured = str(os.environ.get("PORTFOLIO_ALLOCATOR_DB") or "").strip()
    if configured:
        return configured if os.path.isabs(configured) else os.path.join(ENV_DIR, configured)
    if "pytest" in sys.modules:
        # Tests must never inherit real durable slot state from a previous run.
        return os.path.join(tempfile.gettempdir(), f"ggpbot-portfolio-{os.getpid()}.sqlite3")
    return os.path.join(ENV_DIR, "data", "portfolio_allocator.sqlite3")


PORTFOLIO_ALLOCATOR = portfolio_allocator.PortfolioAllocator(_portfolio_allocator_path())
PORTFOLIO_RUNTIME = PortfolioRuntime(
    STATE, TRADE_STATE, DASHBOARD_STATE, _live_manager, lock=_TRADE_LOCK
)


def _portfolio_trusted_hint(context=None):
    """Derive one class hint from trusted strategy/source fields only.

    Raw caller labels (``classification``, ``slot_class``, ``label``, ...) are
    never copied into the hint. Unknown source tokens are ignored and fall back
    to the central TECHNICAL default; conflicting tokens fail closed through
    ``classify_slot``.
    """
    source = context if isinstance(context, dict) else {}
    recognized = {}
    for key in _PORTFOLIO_TRUSTED_HINT_KEYS:
        if key in _PORTFOLIO_RAW_LABEL_KEYS or key not in source:
            continue
        value = source[key]
        if value is None or isinstance(value, (dict, list, tuple, set)):
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            portfolio_allocator.classify_slot(text)
        except portfolio_allocator.UnknownClassHintError as exc:
            if "ambiguous" in str(exc):
                raise
            continue
        recognized[key] = text
    if not recognized:
        recognized["strategy"] = "TECHNICAL"
    # One central call: conflicting trusted fields cannot silently pick a winner.
    portfolio_allocator.classify_slot(recognized)
    return recognized


def _portfolio_flat_evidence(symbol, source):
    """Strict, structured verified-flat evidence accepted by the allocator."""
    return {
        "verified_flat": True,
        "qty": 0,
        "status": "FLAT",
        "source": str(source),
        "snapshot": {str(symbol): 0.0},
    }


def _portfolio_uncertain():
    state = globals().get("STATE") or {}
    return any(bool(state.get(key)) for key in _PORTFOLIO_UNCERTAIN_FLAGS)


def _portfolio_active_ids():
    runtime = globals().get("PORTFOLIO_RUNTIME")
    try:
        return {str(item["trade_id"]) for item in runtime.status().get("active", [])}
    except Exception:
        return set()


def _portfolio_release_reservation(trade_id, symbol, source):
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    if allocator is None or not trade_id:
        return False
    try:
        result = allocator.release(
            str(trade_id), require_flat=True,
            verified_flat=_portfolio_flat_evidence(symbol, source),
        )
    except Exception as exc:
        log_execution(f"[PORTFOLIO_RELEASE] {trade_id}: {type(exc).__name__}: {exc}", "ERROR")
        return False
    # already_released is the idempotent exactly-once path, not a new release.
    if not result.ok and result.reason not in {"already_released"}:
        log_execution(
            f"[PORTFOLIO_RELEASE] {trade_id}: slot retained ({result.reason})", "ERROR"
        )
    return bool(result.ok) or result.reason == "already_released"


def _recompute_portfolio_globals(*, clear_recovery=False):
    """Publish allocator + registry state as aggregate flags and views."""
    global _ACTIVE_TRADE, RECOVERY_BLOCK_ENTRIES
    runtime = globals().get("PORTFOLIO_RUNTIME")
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    active_ids = _portfolio_active_ids()
    try:
        allocator_total = int(allocator.counts().get("TOTAL", 0)) if allocator else 0
    except Exception:
        allocator_total = 0
    has_positions = bool(active_ids or allocator_total)
    uncertain = _portfolio_uncertain()
    entries = getattr(runtime, "_positions", {}) if runtime is not None else {}
    try:
        for entry in list(entries.values()):
            if any(bool(entry.state.get(key)) for key in _PORTFOLIO_UNCERTAIN_FLAGS):
                uncertain = True
    except Exception:
        uncertain = True
    if clear_recovery:
        RECOVERY_BLOCK_ENTRIES = bool(uncertain)
    else:
        RECOVERY_BLOCK_ENTRIES = bool(globals().get("RECOVERY_BLOCK_ENTRIES", False) or uncertain)
    state = globals().get("STATE") or {}
    trade_state = globals().get("TRADE_STATE") or {}
    _ACTIVE_TRADE = bool(
        has_positions or uncertain or state.get("open") or trade_state.get("in_position")
    )
    memory = globals().get("MEMORY")
    if isinstance(memory, dict):
        memory["position_status"] = "OPEN" if has_positions else (
            "UNKNOWN" if uncertain else "FLAT"
        )
    dashboard = globals().get("DASHBOARD_STATE")
    if isinstance(dashboard, dict):
        dashboard["live_trade_mode"] = bool(has_positions)
        try:
            dashboard["portfolio_counts"] = {
                key: int(allocator.counts().get(key, 0))
                for key in ("TECHNICAL", "NEWS_TRADING", "TOTAL")
            } if allocator else {"TECHNICAL": 0, "NEWS_TRADING": 0, "TOTAL": 0}
        except Exception:
            dashboard["portfolio_counts"] = {"TECHNICAL": 0, "NEWS_TRADING": 0, "TOTAL": 0}
        dashboard["active_trade_ids"] = sorted(active_ids)
    paper_view = globals().get("paper")
    if isinstance(paper_view, dict):
        positions = {}
        for trade_id, entry in list(entries.items()):
            positions[trade_id] = {
                "symbol": entry.symbol,
                "side": entry.state.get("side"),
                "entry": entry.state.get("entry", 0.0),
                "qty": entry.state.get("qty", 0.0),
                "remaining_qty": entry.state.get("remaining_qty", 0.0),
            }
        paper_view["positions"] = positions
        paper_view["position"] = None
        if len(positions) == 1:
            only = next(iter(positions.values()))
            paper_view["position"] = dict(only)


def _portfolio_reconcile_paper_stale():
    """PAPER-only cleanup of reservations/runtime entries with no live trade.

    PAPER exposure is simulated locally, so a flat local state with no registry
    entry is explicit verified-flat evidence. LIVE never uses this path.
    """
    if not PAPER_MODE:
        return
    state = globals().get("STATE") or {}
    trade_state = globals().get("TRADE_STATE") or {}
    if state.get("open") or trade_state.get("in_position"):
        return
    runtime = globals().get("PORTFOLIO_RUNTIME")
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    if runtime is None or allocator is None:
        return
    try:
        active_ids = _portfolio_active_ids()
        for reservation in allocator.active_reservations():
            if reservation.trade_id not in active_ids:
                _portfolio_release_reservation(
                    reservation.trade_id, reservation.symbol, "PAPER_FLAT_NO_REGISTRY"
                )
        allocator_ids = {item.trade_id for item in allocator.active_reservations()}
        for trade_id, entry in list(getattr(runtime, "_positions", {}).items()):
            if trade_id in allocator_ids:
                continue
            try:
                with runtime.focus(trade_id):
                    runtime.release(
                        trade_id,
                        verified_flat=_portfolio_flat_evidence(entry.symbol, "PAPER_FLAT_NO_RESERVATION"),
                    )
            except Exception:
                continue
    except Exception:
        return


_PORTFOLIO_BASE_STATE = copy.deepcopy(STATE)
_PORTFOLIO_BASE_TRADE_STATE = copy.deepcopy(TRADE_STATE)
_PORTFOLIO_BASE_PAPER = copy.deepcopy(paper)
_PORTFOLIO_CROSS_TRADE_STATE_KEYS = (
    "cooldown_until", "daily_trades", "last_trade_day", "consecutive_losses",
    "daily_peak_balance", "daily_loss_limit_hit", "balance", "decision_log",
    "last_close_diagnostic", "rf_opposite_close", "entry_context_persistence_failed",
    "cross_event_persistence_failed", "entry_event_persistence_failed",
)
_PORTFOLIO_DEFAULT_ALLOCATOR = PORTFOLIO_ALLOCATOR
_PORTFOLIO_TEST_SCOPE = {"key": None, "allocator": None}


def _portfolio_reset_globals(*, detach_current=True):
    """Restore a clean candidate view without touching cross-trade counters."""
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if detach_current and runtime is not None:
        focused = getattr(runtime, "_focused_trade_id", None)
        entry = getattr(runtime, "_positions", {}).get(focused) if focused else None
        if entry is not None:
            try:
                runtime._save_current_into(entry)
            except Exception:
                pass
    preserved = {
        key: copy.deepcopy(STATE.get(key))
        for key in _PORTFOLIO_CROSS_TRADE_STATE_KEYS if key in STATE
    }
    STATE.clear()
    STATE.update(copy.deepcopy(_PORTFOLIO_BASE_STATE))
    STATE.update(preserved)
    TRADE_STATE.clear()
    TRADE_STATE.update(copy.deepcopy(_PORTFOLIO_BASE_TRADE_STATE))
    paper_balance = paper.get("balance")
    paper.clear()
    paper.update(copy.deepcopy(_PORTFOLIO_BASE_PAPER))
    if isinstance(paper_balance, (int, float)) and not isinstance(paper_balance, bool):
        paper["balance"] = paper_balance
    dashboard = globals().get("DASHBOARD_STATE")
    if isinstance(dashboard, dict):
        dashboard["position"] = None
        dashboard["live_trade_mode"] = False
        dashboard["lifecycle_state"] = "IDLE"


def _portfolio_ensure_test_scope():
    """Give each pytest test a private durable allocator and empty registry.

    Tests explicitly injecting their own allocator are left untouched; this only
    isolates tests that drive the real module-level integration boundary.
    """
    global PORTFOLIO_ALLOCATOR
    if "pytest" not in sys.modules:
        return
    key = str(os.environ.get("PYTEST_CURRENT_TEST") or "")
    if not key or _PORTFOLIO_TEST_SCOPE["key"] == key:
        return
    previous = _PORTFOLIO_TEST_SCOPE["allocator"] or _PORTFOLIO_DEFAULT_ALLOCATOR
    if globals().get("PORTFOLIO_ALLOCATOR") is not previous:
        return
    _PORTFOLIO_TEST_SCOPE["key"] = key
    path = os.path.join(
        tempfile.gettempdir(),
        f"ggpbot-portfolio-{os.getpid()}-{abs(hash(key))}.sqlite3",
    )
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except OSError:
            pass
    PORTFOLIO_ALLOCATOR = portfolio_allocator.PortfolioAllocator(path)
    _PORTFOLIO_TEST_SCOPE["allocator"] = PORTFOLIO_ALLOCATOR
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if runtime is not None and not getattr(runtime, "_focus_stack", None):
        # A fresh test gets a fresh registry, but never a rewritten candidate
        # view: callers legitimately seed STATE before invoking the boundary.
        try:
            runtime._positions.clear()
            runtime._focus_stack.clear()
            runtime._focused_trade_id = None
        except Exception:
            pass


def _portfolio_has_open_positions():
    """True when any durable or local view reports exposure."""
    try:
        if int(PORTFOLIO_ALLOCATOR.counts().get("TOTAL", 0)) > 0:
            return True
    except Exception:
        return True
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if runtime is not None and getattr(runtime, "_positions", None):
        return True
    state = globals().get("STATE") or {}
    trade_state = globals().get("TRADE_STATE") or {}
    return bool(state.get("open") or trade_state.get("in_position"))


def _portfolio_entries_blocked():
    """True when any tracked exposure is uncertain or capacity is exhausted."""
    if _portfolio_uncertain():
        return True
    try:
        counts = PORTFOLIO_ALLOCATOR.counts()
        if int(counts.get("TOTAL", 0)) >= portfolio_allocator.TOTAL_CAPACITY:
            return True
    except Exception:
        return True
    runtime = globals().get("PORTFOLIO_RUNTIME")
    for entry in list(getattr(runtime, "_positions", {}).values()):
        if any(bool(entry.state.get(key)) for key in _PORTFOLIO_UNCERTAIN_FLAGS):
            return True
    return False


def _portfolio_trade_id_for_symbol(symbol):
    runtime = globals().get("PORTFOLIO_RUNTIME")
    try:
        target = normalize_symbol(symbol)
    except Exception:
        target = str(symbol or "").strip().upper()
    try:
        for item in runtime.status().get("active", []):
            try:
                candidate = normalize_symbol(item.get("symbol"))
            except Exception:
                candidate = str(item.get("symbol") or "").strip().upper()
            if candidate == target:
                return str(item["trade_id"])
    except Exception:
        pass
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    try:
        for reservation in allocator.active_reservations():
            try:
                candidate = normalize_symbol(reservation.symbol)
            except Exception:
                candidate = str(reservation.symbol or "").strip().upper()
            if candidate == target:
                return str(reservation.trade_id)
    except Exception:
        pass
    return ""


def _portfolio_ensure_open_reservation(trade_id, symbol, context=None):
    """Return ``(trade_id, reservation, created)`` for an order boundary.

    An execute_entry reservation is reused. Direct callers are reserved here so
    no LIVE order can be submitted without a durable slot claim.
    """
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    if allocator is None:
        return None, None, False
    candidate_id = str(trade_id or uuid.uuid4().hex)
    try:
        existing = allocator.reservation(candidate_id)
    except Exception:
        return None, None, False
    if existing is not None and existing.status == "RESERVED":
        return candidate_id, existing, False
    try:
        counts = allocator.counts()
    except Exception:
        return None, None, False
    if int(counts.get("TOTAL", 0)) >= portfolio_allocator.TOTAL_CAPACITY:
        return None, None, False
    ctx = dict(context or {})
    ctx.update(globals().get("_INSTITUTIONAL_ENTRY_CONTEXT") or {})
    try:
        hint = _portfolio_trusted_hint(ctx)
        result = allocator.reserve(normalize_symbol(symbol), candidate_id, class_hint=hint)
    except Exception:
        return None, None, False
    if not result.ok:
        return None, None, False
    return candidate_id, result.reservation, True


def _portfolio_expected_symbols():
    """Canonical symbols owned by the runtime registry."""
    runtime = globals().get("PORTFOLIO_RUNTIME")
    expected = set()
    for item in list(getattr(runtime, "status", lambda: {})().get("active", [])):
        try:
            expected.add(normalize_symbol(item.get("symbol")))
        except Exception:
            expected.add(str(item.get("symbol") or "").strip().upper())
    return expected


def _portfolio_exchange_unexpected(positions):
    """True when exchange exposure is not exactly the expected registered set."""
    expected = _portfolio_expected_symbols()
    actual = set()
    for position in positions or []:
        try:
            actual.add(normalize_symbol(_position_symbol(position)))
        except Exception:
            actual.add(str(_position_symbol(position) or "").strip().upper())
    return actual != expected


def _portfolio_adopt_candidate(trade_id, symbol, hint):
    """Adopt the freshly written candidate globals into the isolated registry."""
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if runtime is None:
        return False
    try:
        result = runtime.adopt(str(trade_id), normalize_symbol(symbol), class_hint=hint)
    except Exception as exc:
        log_execution(f"[PORTFOLIO_ADOPT] {trade_id}: {type(exc).__name__}: {exc}", "ERROR")
        return False
    if not result.ok:
        log_execution(f"[PORTFOLIO_ADOPT] {trade_id}: {result.reason}", "ERROR")
        return False
    _recompute_portfolio_globals()
    return True


SNIPER_MODE = True
CANDIDATE_SCAN_INTERVAL = 15

def sync_position_with_exchange(symbol):
    """Return a position only when the exchange confirms it; preserve UNKNOWN."""
    global LAST_POSITION_FETCH_STATUS
    try:
        sym, reason, _diagnostics = resolve_position_symbol(symbol)
        if not sym:
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return None
        fetch = getattr(ex, 'fetch_positions', None)
        if not callable(fetch):
            fetch = getattr(ex, 'fetch_open_positions', None)
        if not callable(fetch):
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return None
        positions = safe_api_call(fetch, [sym])
        if positions is None or not isinstance(positions, (list, tuple)):
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return None
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            info = pos.get('info') if isinstance(pos.get('info'), dict) else {}
            pos_sym = str(pos.get('symbol') or info.get('symbol') or '')
            if sym in pos_sym and float(pos.get('contracts', 0) or 0) > 0:
                LAST_POSITION_FETCH_STATUS = "OPEN"
                return pos
        LAST_POSITION_FETCH_STATUS = "FLAT"
        return None
    except Exception as e:
        LAST_POSITION_FETCH_STATUS = "UNKNOWN"
        log_execution(f"[SYNC] error: {e}", "ERROR")
        return None

def get_realized_pnl(symbol, limit=100):
    try:
        sym, reason, _diagnostics = resolve_position_symbol(symbol)
        if not sym:
            return 0.0, 0.0
        # Main-loop accounting is best-effort: a transient read failure is
        # retried on the next cycle, not three times while holding MainThread.
        trades = safe_api_call(ex.fetch_my_trades, sym, limit=limit, _api_max_attempts=1)
        if not trades:
            return 0.0, 0.0
        buy_value = 0.0
        sell_value = 0.0
        for trade in trades:
            side = trade['side'].lower()
            qty = trade['amount']
            price = trade['price']
            cost = qty * price
            if side == 'buy':
                buy_value += cost
            elif side == 'sell':
                sell_value += cost
        pnl = sell_value - buy_value
        balance = get_balance_safe(api_max_attempts=1)
        pnl_pct = (pnl / balance * 100) if balance > 0 else 0.0
        return pnl, pnl_pct
    except Exception as e:
        log_execution(f"[SYNC] error in get_realized_pnl: {e}", "ERROR")
        return 0.0, 0.0

def validate_position_state(local_pos, symbol):
    real_pos = sync_position_with_exchange(symbol)
    if LAST_POSITION_FETCH_STATUS == "UNKNOWN":
        return None
    if LAST_POSITION_FETCH_STATUS == "FLAT" or real_pos is None:
        return None
    return local_pos

def sync_all_states():
    """Sync local exposure without ever turning an API failure into FLAT."""
    global RECOVERY_BLOCK_ENTRIES
    if PAPER_MODE:
        MEMORY["position_status"] = "OPEN" if STATE.get("open") else "CLOSED"
        MEMORY["total_pnl"] = PERF.get("total_pnl_usdt", 0.0)
        MEMORY["total_pnl_pct"] = PERF.get("total_pnl_pct", 0.0) * 100
        return
    symbol = STATE.get("current_symbol") if STATE.get("open") else (TRADE_STATE.get("symbol") if TRADE_STATE.get("in_position") else None)
    target = symbol or DEFAULT_SYMBOL
    real_pos = sync_position_with_exchange(target)
    if LAST_POSITION_FETCH_STATUS == "UNKNOWN":
        # An API failure is UNKNOWN, never a confirmed close. Keep entries and
        # discovery blocked while open exposure remains under Recovery Hold.
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "EXCHANGE_SYNC_UNKNOWN"
        MEMORY["current_position"] = None
        RECOVERY_BLOCK_ENTRIES = True
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            STATE["recovery_hold"] = True
            STATE["recovery_reason"] = "exchange_sync_unknown"
        return
    if not symbol:
        if real_pos is None:
            MEMORY["position_status"] = "CLOSED"
            MEMORY["current_position"] = None
        else:
            MEMORY["position_status"] = "OPEN"
            MEMORY["current_position"] = real_pos
            RECOVERY_BLOCK_ENTRIES = True
        real_pnl, real_pnl_pct = get_realized_pnl(DEFAULT_SYMBOL)
        MEMORY["total_pnl"] = real_pnl
        MEMORY["total_pnl_pct"] = real_pnl_pct
        return
    if real_pos is None:
        log_execution(f"[SYNC] Position on {symbol} closed externally – cleaning local state.", "WARN")
        with _TRADE_LOCK:
            STATE["open"] = False
            TRADE_STATE["in_position"] = False
        MEMORY["position_status"] = "CLOSED"
        MEMORY["current_position"] = None
        MEMORY["entry"] = None
        MEMORY["sl"] = None
        MEMORY["tp"] = None
    else:
        MEMORY["position_status"] = "OPEN"
        MEMORY["current_position"] = real_pos
    real_pnl, real_pnl_pct = get_realized_pnl(symbol)
    MEMORY["total_pnl"] = real_pnl
    MEMORY["total_pnl_pct"] = real_pnl_pct
    if "total_pnl_usdt" in PERF:
        PERF["total_pnl_usdt"] = real_pnl
        PERF["total_pnl_pct"] = real_pnl_pct / 100

def main_loop_sniper():
    global INSUFFICIENT_MARGIN_COOLDOWN_UNTIL, RECOVERY_BLOCK_ENTRIES
    last_scan = 0
    last_candidate_scan = 0
    last_flow_update = 0
    try:
        safe_api_call(ex.load_markets)
        log_execution("Markets loaded", "INFO")
    except Exception as e:
        # No market metadata means no validated symbol universe; do not allow
        # discovery or entries to proceed on a stale/empty market list.
        globals()["RECOVERY_BLOCK_ENTRIES"] = True
        log_execution(f"Failed to load markets: {e}", "ERROR")
    tg_start(get_balance_safe(), "LIVE" if MODE_LIVE else "PAPER")
    if dispatch_scanner_task("global_discovery", _run_global_discovery_scanner_with_heartbeat):
        last_scan = time.time()
        log_execution("[SCANNER_1] Initial bounded global discovery dispatched", "INFO")
    else:
        log_execution("[SCANNER_1] Initial discovery dispatch refused; next due unchanged", "WARN")
    try:
        # Idempotent: the application lifecycle normally owns this worker; this
        # call only covers a direct main-loop invocation in development/tests.
        _start_named_worker("institutional_updater", live_institutional_updater)
    except Exception as exc:
        log_execution(f"[THREAD_START] live_institutional_updater failed: {type(exc).__name__}: {exc}",
                      "ERROR")
    while True:
        try:
            now = time.time()
            _main_loop_progress("iteration_start",
                                recovery_hold=bool(STATE.get("recovery_hold")))
            sync_all_states()
            _main_loop_progress("exchange_sync_complete")
            cooldown_active_now = daily_loss_cooldown_active()
            if cooldown_active_now and not (STATE.get("open") or TRADE_STATE.get("in_position")):
                # Keep the normal bounded TTL/resource cleanup alive while entry scanning is paused.
                # Do not force full GC on every one-second cooldown poll.
                hourly_cleanup()
                time.sleep(1)
                continue

            if now - last_flow_update > 60:
                if not (STATE.get("open") or TRADE_STATE.get("in_position")):
                    dispatch_scanner_task(
                        "institutional_flow_context", update_institutional_flow_scanner
                    )
                last_flow_update = now
                _main_loop_progress("institutional_flow_update_dispatched")

            portfolio_has_open = _portfolio_has_open_positions()
            if not portfolio_has_open:
                sync_position_state()
                portfolio_has_open = _portfolio_has_open_positions()
            if portfolio_has_open:
                if not _main_loop_manage_open_position():
                    time.sleep(BASE_SLEEP)
                    continue
            if _portfolio_entries_blocked():
                _main_loop_progress("discovery_deferred", reason="PORTFOLIO_CAPACITY_OR_HOLD")
                time.sleep(1)
                continue
            if INSUFFICIENT_MARGIN_COOLDOWN_UNTIL and time.time() < INSUFFICIENT_MARGIN_COOLDOWN_UNTIL:
                time.sleep(1)
                continue
            if now - last_scan >= GLOBAL_SCAN_INTERVAL:
                if dispatch_scanner_task("global_discovery", _run_global_discovery_scanner_with_heartbeat):
                    last_scan = now
                else:
                    _main_loop_progress("discovery_dispatch_deferred", reason="CYCLE_ACTIVE")
            # Scanner 2 is the rotating institutional/proximity Watchlist pass.
            # smart_opportunity_selection() refreshes it at the configured batch interval,
            # sharing the exact OHLCV frame with OB/FVG/liquidity/structure analysis.
            if now - last_candidate_scan >= CANDIDATE_SCAN_INTERVAL:
                # Candidate revalidation performs OHLCV reads, institutional
                # analysis and final entry checks. Keep that work off the
                # foreground supervisor so open-position management remains
                # schedulable even when market data or analysis is slow.
                dispatched = dispatch_scanner_task(
                    "institutional_entry_revalidation", smart_opportunity_selection
                )
                last_candidate_scan = now
                _main_loop_progress(
                    "entry_revalidation_dispatched" if dispatched
                    else "entry_revalidation_deferred"
                )
            if _run_sniper_scan_if_due():
                _main_loop_progress("sniper_scan_dispatched")
            time.sleep(1)
            if STATE["open"] and STATE.get("current_symbol"):
                # LiveTradeManager already owns the authoritative market snapshot,
                # ATR/thesis updates and exit decisions. Do not issue a second
                # ticker/OHLCV cycle from MainThread after management; use the
                # reconciled state for the dashboard projection only.
                sym = STATE["current_symbol"]
                current_pnl = STATE.get("roe_pct", 0.0)
                update_position_dashboard(
                    sym, STATE.get("side"), STATE.get("entry", 0.0),
                    STATE.get("qty", 0.0), current_pnl,
                )
                _main_loop_progress("open_trade_state_projection_complete", symbol=str(sym))
            # Drawdown cooldown never force-closes exposure; LiveTradeManager owns exits.
            # Cooldown does not own cleanup; the existing bounded lifecycle cleanup does.
            daily_loss_cooldown_active()
            _main_loop_progress("snapshot")
            print_snapshot()
            _main_loop_progress("cleanup")
            hourly_cleanup()
            _main_loop_progress("iteration_complete")
            time.sleep(BASE_SLEEP)
        except Exception as e:
            log_execution(f"Main loop error: {traceback.format_exc()}", "ERROR")
            time.sleep(BASE_SLEEP)

main_loop = main_loop_sniper


_MAIN_LOOP_PROGRESS_SEQ = 0
# Liveness belongs to the single foreground supervisor thread. Other threads may
# still report observations, but only the registered owner may advance the
# progress sequence that the watchdog treats as proof of a live main_loop.
_MAIN_LOOP_OWNER_LOCK = threading.Lock()
_MAIN_LOOP_OWNER = {"thread_id": None, "thread_name": "MainThread", "registered": False}
_MAIN_LOOP_LAST_OWNER_PROGRESS = {"seq": 0, "ts": 0.0, "thread_id": None, "thread_name": ""}
# Existing main_loop grace window (60s); this is not a new/increased threshold.
_MAIN_LOOP_LIVENESS_WINDOW_SEC = 60.0


def _main_loop_register_owner(thread=None, *, force=False):
    """Bind main_loop liveness ownership to the foreground thread (no new thread)."""
    thread = thread or threading.current_thread()
    with _MAIN_LOOP_OWNER_LOCK:
        if force or not _MAIN_LOOP_OWNER["registered"]:
            _MAIN_LOOP_OWNER.update({
                "thread_id": thread.ident, "thread_name": str(thread.name), "registered": True,
            })
        return dict(_MAIN_LOOP_OWNER)


def _main_loop_owner():
    with _MAIN_LOOP_OWNER_LOCK:
        return dict(_MAIN_LOOP_OWNER)


def _main_loop_owner_alive():
    """True only when the registered owner thread exists and is alive."""
    owner = _main_loop_owner()
    for thread in threading.enumerate():
        if not thread.is_alive():
            continue
        if owner["thread_id"] is not None:
            if thread.ident == owner["thread_id"] and thread.name == owner["thread_name"]:
                return True
        elif thread.name == owner["thread_name"]:
            return True
    return False


def _main_loop_is_owner():
    """Only the registered foreground thread may claim a progress beat."""
    current = threading.current_thread()
    owner = _main_loop_owner()
    if owner["registered"] and owner["thread_id"] is not None:
        return current.ident == owner["thread_id"] and current.name == owner["thread_name"]
    # Before supervisor registration, only the process main thread may claim.
    return current.name == (owner["thread_name"] or "MainThread")


def _main_loop_liveness_confirmed():
    """Owner progress must be recent *and* the watchdog must not be stalled."""
    watchdog = globals().get("NAISO_WATCHDOG")
    component = getattr(watchdog, "_components", {}).get("main_loop")
    if component is None or getattr(component, "status", "") in {
            "STALLED", "STALLED_SAFE_HOLD", "FAILED", "RESTART_FAILED",
            "RECOVERY_DEFERRED", "RESTART_BUDGET_EXHAUSTED", "IDLE_SAFE_HOLD"}:
        return False
    owner = _main_loop_owner()
    with _MAIN_LOOP_OWNER_LOCK:
        last = dict(_MAIN_LOOP_LAST_OWNER_PROGRESS)
    seq = int(globals().get("_MAIN_LOOP_PROGRESS_SEQ", 0))
    if seq <= 0 or last["seq"] < seq or last["thread_id"] != owner["thread_id"]:
        return False
    if last["thread_name"] != owner["thread_name"] or not _main_loop_owner_alive():
        return False
    return (time.monotonic() - float(last["ts"] or 0.0)) <= _MAIN_LOOP_LIVENESS_WINDOW_SEC


def _main_loop_progress(phase, **details):
    """Report a real loop phase, not an artificial keepalive.

    Non-owner callers (tests, diagnostics, helper threads) downgrade to a
    status-only observation and never advance the liveness sequence.
    """
    global _MAIN_LOOP_PROGRESS_SEQ
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return
    current = threading.current_thread()
    if not _main_loop_is_owner():
        status_details = dict(details)
        status_details.pop("progress", None)
        status_details.setdefault("caller_thread_name", current.name)
        status_details.setdefault("owner_thread_name", _main_loop_owner()["thread_name"])
        _main_loop_status(phase, "NON_OWNER_STATUS_ONLY", **status_details)
        return
    with _MAIN_LOOP_OWNER_LOCK:
        _MAIN_LOOP_PROGRESS_SEQ += 1
        seq = _MAIN_LOOP_PROGRESS_SEQ
        _MAIN_LOOP_LAST_OWNER_PROGRESS.update({
            "seq": seq, "ts": time.monotonic(), "thread_id": current.ident,
            "thread_name": str(current.name),
        })
    payload = {
        "state": "RUNNING", "phase": phase, "progress": True,
        "progress_seq": seq, "thread_name": current.name,
        "owner_thread_id": current.ident,
    }
    payload.update(details)
    payload["progress"] = True
    watchdog.beat("main_loop", **payload)


def _exchange_call_phase_deadline_sec():
    """Bound a synchronous exchange phase by the configured HTTP timeout.

    This is not a watchdog timeout change: the watchdog grace window is
    unchanged. It only labels a known blocking call with its real deadline so
    a slow-but-bounded exchange read is not misreported as an engine stall.
    """
    try:
        timeout_ms = float(getattr(ex, "timeout", 12000) or 12000)
    except Exception:
        timeout_ms = 12000.0
    return max(5.0, min(90.0, (timeout_ms / 1000.0) + 10.0))


def _main_loop_status(phase, reason, **details):
    """Record a terminal phase outcome without advancing liveness.

    Used for failed/blocked outcomes so a failure cannot masquerade as a
    genuinely completed operation.
    """
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return
    payload = {
        "state": "RUNNING", "phase": phase, "reason": reason,
        "thread_name": threading.current_thread().name,
    }
    payload.update(details)
    payload["progress"] = False
    watchdog.beat("main_loop", **payload)


def _main_loop_blocked_phase(phase, reason, *, timeout_sec=None, **details):
    """Announce entry into a bounded blocking phase without claiming progress.

    This is a status-only observation (``progress=False``). Only the completion
    beat advances liveness, so a dead thread or a permanently blocked call can
    never be kept alive by the deadline itself.
    """
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return
    deadline = max(5.0, float(timeout_sec or _exchange_call_phase_deadline_sec()))
    payload = {
        "state": "BLOCKED", "phase": phase, "reason": reason,
        "thread_name": threading.current_thread().name,
        "blocked_until": watchdog.monotonic() + deadline,
    }
    payload.update(details)
    payload["progress"] = False
    watchdog.beat("main_loop", **payload)


def _main_loop_manage_open_position():
    """Open-trade management step; deliberately independent of discovery."""
    if STATE.get("recovery_hold"):
        reason = str(STATE.get("recovery_reason") or MEMORY.get("recovery_status")
                     or "management_state_incomplete")
        log_execution(
            f"[RECOVERY_HOLD] {reason} | exposure tracked; retrying journal/exchange reconciliation; "
            "no new entries or fabricated TP/SL actions",
            "ERROR", debounce_key=f"recovery_hold_active:{reason}", debounce_sec=60)
        hourly_cleanup()
        _main_loop_progress("recovery_hold_sleep")
        return False
    _main_loop_progress("open_trade_management")

    def _manage_one_focused_trade():
        # The runtime has already swapped STATE/TRADE_STATE/DASHBOARD_STATE to
        # this trade. The manager itself remains single-position by design.
        _live_manager.manage_live_trade()
        _recompute_portfolio_globals()

    runtime = globals().get("PORTFOLIO_RUNTIME")
    summary = None
    if runtime is not None and hasattr(runtime, "manage_all"):
        try:
            summary = runtime.manage_all(_manage_one_focused_trade)
        except Exception as exc:
            log_execution(
                f"[PORTFOLIO_MANAGE] cycle failed: {type(exc).__name__}: {exc}", "ERROR"
            )
    processed = list(getattr(summary, "processed", ()) or ())
    if not processed and (STATE.get("open") or TRADE_STATE.get("in_position")):
        # Legacy/manual exposure without a registry entry still gets one safe
        # management cycle; the authoritative multi-position path above is
        # always attempted first.
        _manage_one_focused_trade()
    failed = list(getattr(summary, "failed", ()) or ())
    if failed:
        log_execution(
            f"[PORTFOLIO_MANAGE] isolated failures: {failed}", "ERROR"
        )
    _recompute_portfolio_globals()
    _main_loop_progress("open_trade_management_complete")
    return True


_ENGINE_RECOVERY_GUARD = {"ts": 0.0, "symbol": None}
_ENGINE_RECOVERY_LOCK = threading.RLock()
# Explicit single-flight guard for verifier-driven recovery. No main_loop thread
# is ever created here; this only prevents overlapping recovery callbacks.
_MAIN_LOOP_RESTART_LOCK = threading.RLock()
_MAIN_LOOP_RESTART_STATE = {"in_progress": False, "attempts": 0, "last_reason": "",
                            "supervisor_exhausted": False}


def _named_thread_alive(name):
    return any(thread.name == name and thread.is_alive() for thread in threading.enumerate())


def _engine_recovery_is_recent(symbol):
    """Empty symbols are a distinct identity; a changed symbol is never recent."""
    now = time.time()
    with _ENGINE_RECOVERY_LOCK:
        guard_symbol = _ENGINE_RECOVERY_GUARD.get("symbol")
        guard_ts = float(_ENGINE_RECOVERY_GUARD.get("ts") or 0.0)
    return (bool(guard_ts) and guard_symbol == symbol and (now - guard_ts) < 60.0)


def _main_loop_restart_guard(symbol=""):
    """Return (allowed, reason, details) for one verifier-driven restart.

    An alive owner with confirmed recent liveness must not be restarted; a dead
    or genuinely blocked owner is eligible for verifier-driven recovery only.
    """
    with _MAIN_LOOP_RESTART_LOCK:
        in_progress = bool(_MAIN_LOOP_RESTART_STATE["in_progress"])
        supervisor_exhausted = bool(_MAIN_LOOP_RESTART_STATE["supervisor_exhausted"])
    owner = _main_loop_owner()
    thread_alive = _main_loop_owner_alive()
    liveness = _main_loop_liveness_confirmed()
    details = {
        "owner_thread_name": owner["thread_name"],
        "owner_thread_id": owner["thread_id"],
        "owner_thread_alive": thread_alive,
        "liveness_confirmed": liveness,
        "symbol": str(symbol or "")[:80],
    }
    if in_progress:
        return False, "RESTART_IN_PROGRESS", details
    if thread_alive and liveness:
        return False, "MAIN_LOOP_ALIVE_AND_PROGRESSING", details
    if supervisor_exhausted:
        return False, "SUPERVISOR_RESTART_LIMIT_EXHAUSTED", details
    return True, "RESTART_ALLOWED", details


def _exchange_position_matches_control_state(row, symbol, side=None, qty=None):
    """Match one BingX position against the locally controlled trade identity."""
    if not isinstance(row, dict):
        return False
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    raw_symbol = row.get("symbol") or info.get("symbol")
    if not raw_symbol or normalize_symbol(raw_symbol) != normalize_symbol(symbol):
        return False
    raw_position_side = str(row.get("positionSide") or row.get("posSide")
                            or info.get("positionSide") or info.get("posSide") or "").upper()
    raw_side = str(row.get("side") or info.get("side") or "").upper()
    direction = raw_position_side if raw_position_side in {"LONG", "SHORT"} else (
        raw_side if raw_side in {"LONG", "SHORT"} else "")
    expected = "LONG" if str(side or "").upper() == "BUY" else (
        "SHORT" if str(side or "").upper() == "SELL" else "")
    if direction and expected and direction != expected:
        return False
    contracts = safe_float(row.get("contracts"),
                           safe_float(info.get("positionAmt"), 0.0))
    if abs(contracts) <= 0:
        return False
    expected_qty = safe_float(qty, 0.0)
    if expected_qty > 0 and abs(abs(contracts) - expected_qty) > max(0.01, expected_qty * 0.005):
        return False
    return True


def _verify_engine_recovery(symbol=None, *, caller="direct", component="engine",
                            verification_source=""):
    """Verify BingX position, conditional orders and native SL before resume.

    Returns ``(verified, reason, context)``. Any failed or partial exchange read
    returns UNKNOWN-like failure, sets the recovery hold for an open exposure and
    never authorizes management or scanner work. Empty runtime symbols are never
    converted into an exchange lookup; a flat state uses a symbol-free position
    query and an open exposure with no symbol is explicitly rejected.
    """
    explicit_symbol = symbol is not None
    if explicit_symbol:
        raw_symbol = symbol
        raw_symbol_source = "EXPLICIT_ARGUMENT"
    else:
        raw_symbol = STATE.get("current_symbol")
        raw_symbol_source = "STATE.current_symbol"
        if not raw_symbol:
            raw_symbol = TRADE_STATE.get("symbol")
            raw_symbol_source = "TRADE_STATE.symbol"
        if not raw_symbol:
            raw_symbol_source = "NONE"

    local_open = bool(STATE.get("open") or TRADE_STATE.get("in_position"))
    has_raw_symbol = bool(str(raw_symbol or "").strip())
    if explicit_symbol or has_raw_symbol:
        sym, symbol_reason, symbol_diagnostics = resolve_position_symbol(raw_symbol)
        symbol_status = "VALID" if sym else "INVALID"
    else:
        sym, symbol_reason = "", "MISSING_SYMBOL"
        symbol_status = "MISSING"
        symbol_diagnostics = {
            "original_symbol": "", "normalized_symbol": "", "source": "NONE",
            "market_type": "UNKNOWN", "market_id": "",
        }
    expected_side = str(STATE.get("side") or TRADE_STATE.get("side") or "").upper()
    expected_qty = safe_float(STATE.get("remaining_qty") or STATE.get("exchange_qty")
                              or TRADE_STATE.get("qty"), 0.0)
    context = {
        "symbol": sym or "",
        "raw_symbol": str(raw_symbol or "")[:80],
        "symbol_source": raw_symbol_source,
        "symbol_status": symbol_status,
        "caller": str(caller)[:80],
        "component": str(component)[:80],
        "verification_source": str(verification_source or raw_symbol_source)[:120],
        "local_open": local_open, "open_position_verified": False,
        "flat_verified": False, "orders_verified": False, "stop_verified": False,
        "paper_mode": bool(PAPER_MODE), "symbol_diagnostics": symbol_diagnostics,
    }
    if PAPER_MODE:
        if not sym and local_open and symbol_status != "VALID":
            reason = ("engine_recovery_invalid_base_asset" if symbol_status == "INVALID"
                      else "engine_recovery_symbol_missing_open_exposure")
            # Paper-mode exposure is still real control state: fail closed rather
            # than pretending the symbol resolved or that management can proceed.
            _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                           details={"symbol_diagnostics": symbol_diagnostics,
                                    "symbol_status": symbol_status, "paper_mode": True})
            return False, reason, context
        context.update({"open_position_verified": local_open, "flat_verified": not local_open,
                        "paper_only": True})
        return True, "PAPER_MODE_RECOVERY", context
    if not sym:
        if symbol_status == "INVALID" or explicit_symbol:
            reason = f"engine_recovery_{symbol_reason.lower()}"
            if local_open:
                _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                               details={"symbol_diagnostics": symbol_diagnostics,
                                        "symbol_status": symbol_status})
            return False, reason, context
        if local_open:
            reason = "engine_recovery_symbol_missing_open_exposure"
            _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                           details={"symbol_diagnostics": symbol_diagnostics,
                                    "symbol_status": symbol_status})
            return False, reason, context
        # Flat with no controlled symbol: ask the exchange for ALL account-wide
        # USDT-margined swap positions. No symbol is inferred or synthesized.
        # A FLAT result is accepted only from a complete, authoritative snapshot;
        # every partial/malformed/failed/ambiguous response stays UNKNOWN.
        from bingx_protection import validate_account_position_snapshot
        try:
            raw_positions = safe_api_call(ex.fetch_positions)
        except Exception as exc:
            reason = f"engine_recovery_flat_query_unknown:{type(exc).__name__}"
            context["snapshot_reason"] = reason
            return False, reason, context
        status, open_rows, snapshot_reason, snapshot_diag = validate_account_position_snapshot(
            ex, raw_positions)
        context["snapshot_reason"] = snapshot_reason
        context["snapshot_diagnostics"] = snapshot_diag
        if status == "UNKNOWN":
            return False, f"engine_recovery_flat_query_unknown:{snapshot_reason}", context
        if status == "OPEN":
            reason = "engine_recovery_unexpected_exchange_position"
            detected_symbol = _position_symbol(open_rows[0]) if open_rows else ""
            _recovery_hold(reason, symbol=detected_symbol,
                           details={"symbol_status": "UNEXPECTED_EXCHANGE_SYMBOL",
                                    "snapshot_diagnostics": snapshot_diag})
            return False, reason, context
        context["flat_verified"] = True
        return True, "FLAT_VERIFIED_NO_SYMBOL", context
    if local_open and (expected_side not in {"BUY", "SELL"} or expected_qty <= 0):
        reason = "engine_recovery_local_state_incomplete"
        if local_open:
            _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"))
        return False, reason, context
    try:
        positions = safe_api_call(ex.fetch_positions, [sym])
    except Exception as exc:
        reason = f"engine_recovery_position_query_unknown:{type(exc).__name__}"
        if local_open:
            _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"))
        return False, reason, context
    if not isinstance(positions, list):
        reason = "engine_recovery_position_query_invalid"
        if local_open:
            _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"))
        return False, reason, context
    matches = [row for row in positions if _exchange_position_matches_control_state(
        row, sym, side=expected_side, qty=expected_qty)]
    if not local_open:
        if matches:
            reason = "engine_recovery_unexpected_exchange_position"
            _recovery_hold(reason, symbol=sym)
            return False, reason, context
        context["flat_verified"] = True
        return True, "FLAT_VERIFIED", context
    if len(matches) != 1:
        reason = "engine_recovery_position_unverified"
        _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                       details={"matched_positions": len(matches)})
        return False, reason, context
    try:
        from bingx_protection import fetch_native_stop_evidence
        records, authoritative_ok, query_details = fetch_native_stop_evidence(
            ex, sym, api_call=safe_api_call,
            logger=lambda message: log_execution(f"[ENGINE_RECOVERY] {message}", "WARN"))
    except Exception as exc:
        reason = f"engine_recovery_orders_query_unknown:{type(exc).__name__}"
        _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"))
        return False, reason, context
    if not authoritative_ok:
        reason = "engine_recovery_orders_unknown"
        _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                       details={"query": query_details, "records": len(records)})
        return False, reason, context
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    saved_state = {}
    if journal is not None and STATE.get("trade_id"):
        try:
            row = journal.get(str(STATE["trade_id"])) or {}
            saved_state = row.get("state") if isinstance(row.get("state"), dict) else {}
        except Exception:
            saved_state = {}
    stop_prices = _journal_native_stop_prices(
        saved_state, primary=safe_float(STATE.get("native_sl_last_verified_price"),
                                        safe_float(STATE.get("native_sl_original_price"),
                                                   safe_float(STATE.get("sl")))))
    if not stop_prices:
        reason = "engine_recovery_journal_sl_unproven"
        _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"))
        return False, reason, context
    position_side = str(STATE.get("position_side") or saved_state.get("position_side") or "").upper()
    status, order_id, matched_price = verify_native_stop_loss_detailed(
        sym, expected_side, stop_prices[0], expected_qty, position_side=position_side,
        trigger_prices=stop_prices[1:])
    if status != "VERIFIED":
        reason = f"engine_recovery_native_sl_{str(status).lower()}"
        _recovery_hold(reason, symbol=sym, trade_id=STATE.get("trade_id"),
                       details={"expected_sl_levels": stop_prices})
        return False, reason, context
    context.update({"open_position_verified": True, "orders_verified": True,
                    "stop_verified": True, "native_sl_order_id": order_id,
                    "native_sl_price": matched_price})
    return True, "OPEN_POSITION_VERIFIED", context


def _scanner_recovery_hard_blocked():
    """Entry/recovery gates that no exchange read may override."""
    if STATE.get("recovery_hold") or STATE.get("execution_unresolved"):
        return "RECOVERY_HOLD"
    if globals().get("RECOVERY_BLOCK_ENTRIES"):
        return "RECOVERY_BLOCK_ENTRIES"
    return ""


def _scanner_recovery_blocked(context=None):
    """Return a reason when exchange state is unresolved or an entry is blocked.

    A stale initial UNKNOWN is not proof of a failed sync when the caller has a
    fresh exchange-verified FLAT context. A real UNKNOWN read or a failed sync
    still blocks; neither is ever treated as flat.
    """
    hard_block = _scanner_recovery_hard_blocked()
    if hard_block:
        return hard_block
    fresh_flat = bool(isinstance(context, dict) and (
        context.get("flat_verified") or
        (context.get("paper_mode") and context.get("paper_only"))))
    if fresh_flat:
        return ""
    if LAST_POSITION_FETCH_STATUS == "UNKNOWN":
        return "POSITION_SYNC_UNKNOWN"
    if str(MEMORY.get("position_status", "")).startswith("UNKNOWN"):
        return "EXCHANGE_STATE_UNKNOWN"
    return ""


def _verify_scanner_recovery():
    """Scanner recovery is flat-only, single-flight and exchange-verified."""
    if _scanner_cycle_active("global_discovery"):
        return False, "SCANNER_CYCLE_ACTIVE", {"scanner_running": True}
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        return False, "OPEN_POSITION_SCANNER_PAUSED", {"open_position_verified": True}
    hard_block = _scanner_recovery_hard_blocked()
    if hard_block:
        return False, hard_block, {}
    verified, reason, context = _verify_engine_recovery()
    if not verified or not context.get("flat_verified"):
        return False, f"SCANNER_VERIFY_{reason}", context
    blocked = _scanner_recovery_blocked(context)
    if blocked:
        return False, blocked, context
    return True, "SCANNER_FLAT_VERIFIED", context


def _resume_scanner_after_flat_verification(context=None):
    """Scanner restart is a separate flat-only path; it never manages a trade."""
    context = dict(context or {})
    if _scanner_cycle_active("global_discovery"):
        return False
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        return False
    if _scanner_recovery_blocked(context):
        return False
    if not (context.get("flat_verified") or (context.get("paper_mode") and context.get("paper_only"))):
        return False
    return bool(dispatch_scanner_task("global_discovery", _run_global_discovery_scanner_with_heartbeat))


def _wait_for_main_loop_progress(baseline, timeout_sec=2.0):
    """Prove the registered owner advanced before claiming a loop restart."""
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        # Direct/diagnostic calls are not watchdog-driven recovery; there is no
        # liveness claim to validate in that context.
        return True
    component = getattr(watchdog, "_components", {}).get("main_loop")
    if component is None or not getattr(component, "resume_requires_liveness", False):
        # A watchdog without a liveness-gated main_loop registration is not
        # claiming to have restarted the foreground loop.
        return True
    owner = _main_loop_owner()
    deadline = time.monotonic() + max(0.0, float(timeout_sec))
    while True:
        seq = int(globals().get("_MAIN_LOOP_PROGRESS_SEQ", 0))
        with _MAIN_LOOP_OWNER_LOCK:
            last = dict(_MAIN_LOOP_LAST_OWNER_PROGRESS)
        owner_progress = (
            seq > int(baseline) and last["seq"] >= seq and
            last["thread_id"] is not None and last["thread_id"] == owner["thread_id"] and
            last["thread_name"] == owner["thread_name"] and _main_loop_owner_alive())
        if owner_progress:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)


def _resume_main_loop_after_verification(context=None):
    """Resume verified open-trade management without opening a new trade.

    A restart is claimed only when no overlapping recovery is running and the
    previous main_loop owner is dead or genuinely blocked. Recovery itself never
    starts a replacement foreground thread.
    """
    # Mutate the caller's verification context when possible: the watchdog reads
    # liveness proof from the same dict after this callback returns.
    context = context if isinstance(context, dict) else {}
    open_verified = bool(context.get("open_position_verified") or
                         (context.get("paper_mode") and context.get("paper_only")
                          and context.get("open_position")))
    symbol = str(context.get("symbol") or STATE.get("current_symbol") or "")
    allowed, guard_reason, guard_details = _main_loop_restart_guard(symbol)
    context.update({
        "restart_guard": guard_reason, "restart_guard_reason": guard_reason,
        "restart_guard_details": guard_details,
        "main_loop_thread_name": str(guard_details["owner_thread_name"]),
        "main_loop_thread_alive": bool(guard_details["owner_thread_alive"]),
    })
    if not allowed:
        context["engine_resumed"] = False
        with _MAIN_LOOP_RESTART_LOCK:
            _MAIN_LOOP_RESTART_STATE["last_reason"] = guard_reason
        return False
    with _MAIN_LOOP_RESTART_LOCK:
        if _MAIN_LOOP_RESTART_STATE["in_progress"]:
            context["engine_resumed"] = False
            context["restart_guard_reason"] = "RESTART_IN_PROGRESS"
            context["restart_guard"] = "RESTART_IN_PROGRESS"
            return False
        _MAIN_LOOP_RESTART_STATE["in_progress"] = True
        _MAIN_LOOP_RESTART_STATE["attempts"] += 1
    try:
        if open_verified:
            progress_baseline = int(globals().get("_MAIN_LOOP_PROGRESS_SEQ", 0))
            recent = _engine_recovery_is_recent(symbol)
            if recent:
                resumed = not bool(STATE.get("recovery_hold"))
            else:
                if not startup_trade_recovery():
                    context["engine_resumed"] = False
                    return False
                with _ENGINE_RECOVERY_LOCK:
                    _ENGINE_RECOVERY_GUARD.update({"ts": time.time(), "symbol": symbol})
                resumed = not bool(STATE.get("recovery_hold"))
            # Exchange verification alone does not prove the foreground loop is alive.
            context["engine_resumed"] = bool(
                resumed and _wait_for_main_loop_progress(progress_baseline))
            return bool(context["engine_resumed"])
        return _resume_scanner_after_flat_verification(context)
    finally:
        with _MAIN_LOOP_RESTART_LOCK:
            _MAIN_LOOP_RESTART_STATE["in_progress"] = False
            _MAIN_LOOP_RESTART_STATE["last_reason"] = str(
                context.get("restart_guard_reason") or guard_reason)


def _restart_main_loop_process_after_stall(context=None):
    """Request an OS-process restart only after the final exposure safety gate passes.

    Python cannot terminate or replace a blocked MainThread. The watchdog runs on
    its own worker, verifies the exchange state, persists the recovery contract,
    then exits the process so the hosting supervisor can create a clean process.
    If no external supervisor is detected, or exposure/protection is UNKNOWN,
    remain alive in fail-closed recovery hold and keep diagnostics available.
    """
    context = context if isinstance(context, dict) else {}
    owner = _main_loop_owner()
    owner_alive = bool(_main_loop_owner_alive())
    context["process_restart_required"] = True
    context["process_restart_owner"] = str(owner.get("thread_name") or "UNKNOWN")
    render_detected = bool(
        os.getenv("RENDER", "").strip().lower() in {"1", "true", "yes", "on"}
        or os.getenv("RENDER_SERVICE_ID", "").strip()
        or os.getenv("RENDER_INSTANCE_ID", "").strip()
    )
    # DigitalOcean bindable APP_ID is not automatically injected unless the
    # app component maps it to a runtime env var. Support both the direct name
    # and an explicit DIGITALOCEAN_APP_ID mapping from ${APP_ID}.
    digitalocean_app_platform_detected = bool(
        os.getenv("APP_ID", "").strip() or os.getenv("DIGITALOCEAN_APP_ID", "").strip()
    )
    railway_detected = bool(os.getenv("RAILWAY_ENVIRONMENT", "").strip())
    explicit_restart_opt_in = bool(
        os.getenv("NAISO_PROCESS_RESTART_ON_MAIN_LOOP_STALL", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    supervisor_kind = (
        "DIGITALOCEAN_APP_PLATFORM" if digitalocean_app_platform_detected else
        "RENDER" if render_detected else
        "RAILWAY" if railway_detected else
        "EXPLICIT_OPT_IN" if explicit_restart_opt_in else
        "NONE"
    )
    supervisor_available = supervisor_kind != "NONE"
    context["process_supervisor_detected"] = supervisor_available
    context["process_supervisor_kind"] = supervisor_kind
    if not supervisor_available:
        context["engine_resumed"] = False
        context["process_restart_blocked_reason"] = "EXTERNAL_PROCESS_SUPERVISOR_NOT_DETECTED"
        log_execution(
            "[WATCHDOG] main_loop is stalled on a live foreground thread; in-process thread restart is impossible. "
            "No external process supervisor detected; preserving fail-closed state and diagnostics.",
            "CRITICAL", debounce_key="main_loop_process_restart_no_supervisor", debounce_sec=120,
        )
        return True
    if not owner_alive or owner.get("thread_name") != "MainThread":
        context["engine_resumed"] = False
        context["process_restart_blocked_reason"] = "FOREGROUND_OWNER_IDENTITY_NOT_CONFIRMED"
        log_execution(
            f"[WATCHDOG] process restart deferred: foreground owner identity is not verified ({owner}).",
            "CRITICAL", debounce_key="main_loop_process_restart_owner_unknown", debounce_sec=120,
        )
        return True
    restart_reason = str(context.get("reason") or "MAIN_LOOP_WATCHDOG_STALL")
    if not _recovery_exit_gate(restart_reason):
        context["engine_resumed"] = False
        context["process_restart_blocked_reason"] = "RECOVERY_EXIT_GATE_REFUSED_UNVERIFIED_EXPOSURE"
        log_execution(
            f"[WATCHDOG] process restart blocked for {restart_reason}: exchange exposure/native protection "
            "could not be verified. Recovery Hold remains active; no process exit was attempted.",
            "CRITICAL", debounce_key="main_loop_process_restart_safety_hold", debounce_sec=60,
        )
        return True
    log_execution(
        f"[WATCHDOG] {restart_reason} confirmed; exchange state and recovery contract verified. "
        "Exiting process for external supervisor restart.",
        "CRITICAL", debounce_key="main_loop_verified_process_restart", debounce_sec=30,
    )
    context["engine_resumed"] = True
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(EXIT_RECOVERY_EXHAUSTED)
    return True


def _resume_exchange_reconciliation_after_verification(_context=None):
    """Restart only the reconciliation monitor, never Scanner or trade startup."""
    supervisor = globals().get("_reconciliation_supervisor")
    return bool(supervisor is not None and supervisor.start())


def _non_trading_engine_verifier():
    """Non-order engines may resume while a position is open; they never trade."""
    return True, "NON_TRADING_ENGINE", {"non_trading_engine": True}


def _resume_institutional_updater_after_verification(_context=None):
    # Owned single-flight restart: a timed-out worker that is still alive is
    # never replaced. Only a verified dead worker may be restarted.
    return _start_named_worker("institutional_updater", live_institutional_updater)


def _resume_keep_alive_after_verification(_context=None):
    return _start_named_worker("keep_alive", keep_alive)


def _verify_main_loop_recovery():
    """Watchdog entry point: keep caller/component provenance in recovery context."""
    return _verify_engine_recovery(
        caller="watchdog_stalled", component="main_loop",
        verification_source="STATE.current_symbol|TRADE_STATE.symbol")


_NAISO_SERVICES_STARTED = False
_NAISO_SERVICES_LOCK = threading.RLock()


def start_runtime_workers(*, restart=False):
    """Idempotently start every owned background worker from the app lifecycle.

    Imports stay side-effect free. A shutdown in progress is never reversed by
    a late startup call; a fresh explicit lifecycle cycle must pass
    ``restart=True`` after all owned workers have been joined.
    """
    global _RUNTIME_WORKERS_STARTED, _RUNTIME_SHUTTING_DOWN
    with _RUNTIME_WORKERS_LOCK:
        if _RUNTIME_WORKERS_STARTED:
            return False
        if _RUNTIME_SHUTTING_DOWN and not restart:
            return False
        if restart:
            _RUNTIME_SHUTTING_DOWN = False
        _RUNTIME_WORKERS_STARTED = True
    try:
        _event_bus.start()
        _reconciliation_supervisor.start()
        _start_named_worker("institutional_updater", live_institutional_updater)
        _start_named_worker("keep_alive", keep_alive)
        TELEGRAM_NOTIFIER.reopen()
    except Exception:
        with _RUNTIME_WORKERS_LOCK:
            _RUNTIME_WORKERS_STARTED = False
        raise
    return True


def start_naiso_services():
    """Register engines with the watchdog and verified recovery callbacks."""
    global _NAISO_SERVICES_STARTED
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return False
    with _NAISO_SERVICES_LOCK:
        if _NAISO_SERVICES_STARTED:
            return True
        _NAISO_SERVICES_STARTED = True
    # The supervisor runs on this foreground thread; bind liveness ownership
    # before any recovery callback can inspect it.
    _main_loop_register_owner()
    watchdog.register("main_loop", interval_sec=max(5.0, safe_float(BASE_SLEEP, 5.0)),
                      grace_sec=60.0, critical=True, restartable=True,
                      recovery_verifier=_verify_main_loop_recovery,
                      restart_fn=_restart_main_loop_process_after_stall,
                      lifecycle="BLOCKED", resume_requires_liveness=True)
    # Startup exchange recovery runs before the first loop iteration. Classify it
    # as a bounded startup phase instead of letting the watchdog call it a stall.
    watchdog.beat("main_loop", state="BLOCKED", phase="startup_recovery",
                  reason="STARTUP_RECOVERY", progress=False,
                  thread_name=threading.current_thread().name,
                  blocked_until=watchdog.monotonic() + 300.0)
    watchdog.register("exchange_reconciliation", interval_sec=15.0, grace_sec=60.0,
                      critical=True, restartable=True,
                      recovery_verifier=_non_trading_engine_verifier,
                      restart_fn=_resume_exchange_reconciliation_after_verification)
    watchdog.register("institutional_updater", interval_sec=5.0, grace_sec=45.0,
                      critical=False, restartable=True,
                      recovery_verifier=_non_trading_engine_verifier,
                      restart_fn=_resume_institutional_updater_after_verification)
    watchdog.register("keep_alive", interval_sec=max(60.0, safe_float(KEEP_ALIVE_INTERVAL, 300.0)),
                      grace_sec=max(120.0, safe_float(KEEP_ALIVE_INTERVAL, 300.0) * 2.0),
                      critical=False, restartable=True,
                      recovery_verifier=_non_trading_engine_verifier,
                      restart_fn=_resume_keep_alive_after_verification)
    try:
        watchdog.start()
    except Exception as exc:
        log_execution(f"[THREAD_START] NAISO watchdog failed: {type(exc).__name__}: {exc}", "ERROR")
        return False
    log_execution("[NAISO_WATCHDOG] health monitoring started for core engines", "INFO")
    return True


_NAISO_LOOP_RESTARTS = []
_NAISO_SUPERVISOR_MAX_RESTARTS = 3
_NAISO_SUPERVISOR_LOCK = threading.RLock()
_NAISO_SUPERVISOR_STATE = {"window_started": 0.0, "restarts": 0, "exhausted": False,
                            "last_reason": "", "last_attempt": 0}


def _naiso_supervisor_state():
    with _NAISO_SUPERVISOR_LOCK:
        return dict(_NAISO_SUPERVISOR_STATE)


def _naiso_supervisor_reset():
    """A genuine long healthy run re-arms the bounded supervisor budget."""
    with _NAISO_SUPERVISOR_LOCK:
        _NAISO_SUPERVISOR_STATE.update({
            "window_started": 0.0, "restarts": 0, "exhausted": False,
            "last_reason": "", "last_attempt": 0,
        })
        _MAIN_LOOP_RESTART_STATE["supervisor_exhausted"] = False


def safe_main_loop():
    """Foreground supervisor: bounded backoff, reason/result logged, fail-closed."""
    _main_loop_register_owner(force=True)
    consecutive_failures = 0
    while True:
        requested = _shutdown_reason()
        if requested:
            log_execution(f"[SHUTDOWN] {requested} observed at supervisor boundary; deferring exit to safety gate", "WARN")
            return requested
        reason = "CLEAN_EXIT"
        started = time.monotonic()
        try:
            main_loop()
            reason = "CLEAN_EXIT"
        except Exception as e:
            reason = f"{type(e).__name__}: {str(e)[:180]}"
            log_execution(f"CRITICAL EXCEPTION: {traceback.format_exc()}", "ERROR")
        elapsed = time.monotonic() - started
        consecutive_failures += 1
        watchdog = globals().get("NAISO_WATCHDOG")
        if reason == "CLEAN_EXIT" and elapsed >= _MAIN_LOOP_LIVENESS_WINDOW_SEC:
            _naiso_supervisor_reset()
            consecutive_failures = 0
        with _NAISO_SUPERVISOR_LOCK:
            state = _NAISO_SUPERVISOR_STATE
            state["window_started"] = state["window_started"] or time.monotonic()
            state["restarts"] += 1
            attempt = state["restarts"]
            state["last_reason"] = reason
            state["last_attempt"] = attempt
            exhausted = attempt >= _NAISO_SUPERVISOR_MAX_RESTARTS
            state["exhausted"] = exhausted
            _MAIN_LOOP_RESTART_STATE["supervisor_exhausted"] = exhausted
        if watchdog is not None:
            try:
                # Status-only: a backoff beat must never mask a dead or hung loop.
                watchdog.beat("main_loop", phase="supervisor_backoff", progress=False,
                              state="RUNNING", last_restart_reason=reason[:100],
                              supervisor_attempt=attempt, supervisor_exhausted=exhausted,
                              thread_name=threading.current_thread().name)
            except Exception:
                pass
        # Keep the bounded backoff strictly below the watchdog grace window.
        # A 60s cap plus jitter previously produced 66s silent gaps that were
        # indistinguishable from a real main_loop stall.
        delay = min(45.0, 5.0 * (2 ** min(consecutive_failures - 1, 4)))
        delay *= 1.0 + random.uniform(-0.10, 0.10)
        _NAISO_LOOP_RESTARTS.append({
            "ts": time.time(), "reason": reason, "backoff_sec": round(delay, 3),
            "consecutive_failures": consecutive_failures, "supervisor_attempt": attempt,
        })
        if len(_NAISO_LOOP_RESTARTS) > 50:
            _NAISO_LOOP_RESTARTS.pop(0)
        log_execution(f"[SUPERVISOR] restart #{consecutive_failures} after {delay:.1f}s: {reason}", "WARN")
        if exhausted:
            # Fail closed: no further restart, no clean-looking heartbeats.
            if watchdog is not None:
                try:
                    watchdog.beat("main_loop", state="FAILED",
                                  phase="supervisor_restart_limit_exhausted",
                                  reason="SUPERVISOR_RESTART_LIMIT_EXHAUSTED", progress=False,
                                  supervisor_attempt=attempt, last_restart_reason=reason[:100],
                                  thread_name=threading.current_thread().name)
                except Exception:
                    pass
            globals()["RECOVERY_BLOCK_ENTRIES"] = True
            STATE["recovery_hold"] = True
            STATE["recovery_reason"] = "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
            MEMORY["recovery_status"] = "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
            MEMORY["supervisor_restarts"] = len(_NAISO_LOOP_RESTARTS)
            _persist_recovery_state("SUPERVISOR_RESTART_LIMIT_EXHAUSTED", "UNKNOWN",
                                    safe_to_exit=False,
                                    details={"phase": "supervisor_exhausted",
                                             "attempt": attempt,
                                             "last_reason": reason[:100]})
            log_execution(
                f"[SUPERVISOR] restart budget exhausted after {attempt} attempts; "
                "fail-closed hold, no further main_loop restart", "CRITICAL")
            return False
        time.sleep(delay)
        # Recovery always precedes scanner work in the restarted loop.
        MEMORY["supervisor_restarts"] = len(_NAISO_LOOP_RESTARTS)

# ========== RORO -> GGP INSTITUTIONAL INTEGRATION ==========
# The bridge is deliberately appended so legacy GGP callers remain compatible.
from ggp_institutional_bridge import (
    update_unified_watchlist, analyze_symbol as roro_analyze_symbol,
    analyze_symbol_both as roro_analyze_symbol_both,
    validate_closed_frame as roro_validate_closed_frame,
    canonical_symbol as roro_canonical_symbol, exchange_symbol as roro_exchange_symbol,
    merge_candidates as roro_merge_candidates, AUTHORITY as RORO_ENTRY_AUTHORITY,
)
from ggp_discovery_scanner import professional_lightweight_scan as _professional_lightweight_scan
from roro_confluence import (ema9_vwap_proximity as _roro_ema9_vwap_proximity,
                             validate_ema9_vwap_entry_gate as _validate_ema9_vwap_entry_gate,
                             evaluate_post_cross_rejection_breakout as _evaluate_post_cross_rejection_breakout,
                             closed_candles as _roro_closed_candles,
                             AUTH_EMA9_VWAP_TRIGGER_VALID as _AUTH_EMA9_VWAP_TRIGGER_VALID,
                             timeframe_seconds as _roro_timeframe_seconds)
# Preserve the legacy monkeypatch/import seam while the canonical implementation
# and scanner authorization are EMA9/Session-VWAP only.
_validate_dual_ema_vwap_entry_gate = _validate_ema9_vwap_entry_gate
try:
    from roro_resource_manager import ResourceManager
    from roro_trade_journal import TradeJournal
    INSTITUTIONAL_RESOURCE_MANAGER = ResourceManager(max_cache_items=5000, max_history=2000)
    INSTITUTIONAL_TRADE_JOURNAL = TradeJournal(os.getenv("TRADE_JOURNAL_PATH", "data/ggp_trade_journal.sqlite3"))
    _restore_system_errors()
except Exception as _integration_init_error:
    INSTITUTIONAL_RESOURCE_MANAGER = None
    INSTITUTIONAL_TRADE_JOURNAL = None
    log_execution(f"[INTEGRATION] optional persistence unavailable: {_integration_init_error}", "WARN")


# --- RF opposite-signal re-entry wait -------------------------------------- #
# State lives in the trade journal so a restart cannot forget why a position
# was closed. RF is evaluated only on fully closed candles; external closes are
# never relabelled as RF closes.
RF_OPPOSITE_CLOSE_REASON = "RF_OPPOSITE_SIGNAL"
RF_REENTRY_CLOSED_SOURCE = "CLOSED_CANDLES"


def _rf_reentry_journal():
    return globals().get("INSTITUTIONAL_TRADE_JOURNAL")


def _rf_reentry_symbol(symbol):
    """Use the exchange-normalized symbol identity shared with journal trades."""
    try:
        normalized = normalize_symbol(symbol)
    except Exception:
        normalized = ""
    return str(normalized or str(symbol or "").strip().upper())


def _rf_reentry_signal_timestamp(gate_result, cross_event_id=""):
    """Return the ORIGINAL cross/signal candle identity, including delayed entries."""
    gate = gate_result if isinstance(gate_result, dict) else {}
    event_id = str(cross_event_id or "").strip()
    if event_id:
        row = _get_confirmed_cross_event(event_id)
        try:
            original_ts = int((row or {}).get("signal_candle_timestamp") or 0)
        except (TypeError, ValueError, OverflowError):
            original_ts = 0
        if original_ts > 0:
            return original_ts
    for key in ("signal_candle_timestamp", "ema9_cross_timestamp"):
        try:
            value = int(gate.get(key) or 0)
        except (TypeError, ValueError, OverflowError):
            value = 0
        if value > 0:
            return value
    return 0


def _rf_reentry_block_permission(symbol, side, gate_result, cross_event_id=""):
    """Check the durable wait. Any unreadable state fails closed (allowed=False)."""
    direction = str(side or "").strip().upper()
    if direction not in {"BUY", "SELL"}:
        return False, "invalid_entry_side"
    gate = gate_result if isinstance(gate_result, dict) else {}
    if not gate.get("ready"):
        return False, "not_a_valid_closed_candle_signal"
    journal = _rf_reentry_journal()
    if journal is None or not hasattr(journal, "get_active_rf_reentry_block"):
        return False, "rf_reentry_state_unavailable"
    try:
        block = journal.get_active_rf_reentry_block(_rf_reentry_symbol(symbol), direction)
    except Exception as exc:
        return False, f"rf_reentry_state_unreadable:{type(exc).__name__}"
    if not block:
        return True, "no_active_rf_reentry_block"
    try:
        block_ts = int(block.get("rf_signal_timestamp") or 0)
    except (TypeError, ValueError, OverflowError):
        return False, "rf_reentry_state_malformed"
    if block_ts <= 0:
        return False, "rf_reentry_state_malformed"
    signal_ts = _rf_reentry_signal_timestamp(gate, cross_event_id)
    if signal_ts <= block_ts:
        return False, f"rf_wait_signal_not_later:{signal_ts}<={block_ts}"
    return True, "rf_wait_satisfied_by_later_closed_signal"


def _rf_signal_from_closed_candles(symbol, df=None, *, timeframe=None, now_ms=None):
    """Compute an RF signal from closed candles only; never from an open candle."""
    frame = df if df is not None else _integration_fetch_frame(symbol)
    try:
        timeframe_seconds = int(_roro_timeframe_seconds(timeframe or INTERVAL))
    except Exception:
        return None
    closed = _roro_closed_candles(frame, timeframe_seconds, now_ms=now_ms)
    if closed is None or len(closed) == 0:
        return None
    try:
        rf = RFEngine(period=20, multiplier=3.5).compute(closed)
    except Exception:
        return None
    signal = str(rf.get("signal") or "").upper()
    if signal not in {"BUY", "SELL"}:
        return None
    try:
        candle_ts = int(closed["timestamp"].iloc[-1])
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
    if candle_ts <= 0:
        return None
    return {
        "signal": signal,
        "signal_candle_timestamp": candle_ts,
        "closed_candle_timestamp": candle_ts,
        "source": RF_REENTRY_CLOSED_SOURCE,
    }


def _persist_rf_reentry_block_from_close(symbol, close_reason, state=None):
    """Persist the block for a verified RF-opposite close; fail closed on error."""
    if str(close_reason or "").upper() != RF_OPPOSITE_CLOSE_REASON:
        return False
    snap = state if isinstance(state, dict) else STATE
    ctx = snap.get("rf_opposite_close")
    journal = _rf_reentry_journal()

    def _fail(detail):
        globals()["RECOVERY_BLOCK_ENTRIES"] = True
        snap["rf_reentry_block_persistence_failed"] = True
        log_execution(
            f"[RF_REENTRY] CRITICAL: {symbol} block not durable ({detail}); new entries blocked",
            "ERROR",
        )
        return False

    if not isinstance(ctx, dict):
        return _fail("missing_rf_close_context")
    position_side = str(ctx.get("position_side") or "").upper()
    rf_signal = str(ctx.get("rf_signal") or "").upper()
    source = str(ctx.get("source") or "").upper()
    try:
        signal_ts = int(ctx.get("rf_signal_candle_timestamp") or 0)
        close_ts = float(ctx.get("close_ts") or 0.0)
    except (TypeError, ValueError, OverflowError):
        return _fail("invalid_rf_close_timestamps")
    if position_side not in {"BUY", "SELL"} or rf_signal not in {"BUY", "SELL"}:
        return _fail("invalid_rf_directions")
    if rf_signal == position_side:
        return _fail("rf_signal_not_opposite")
    if source != RF_REENTRY_CLOSED_SOURCE or signal_ts <= 0 or close_ts <= 0:
        return _fail("rf_signal_not_from_closed_candle")
    canonical = _rf_reentry_symbol(symbol)
    if not canonical or canonical != _rf_reentry_symbol(ctx.get("symbol") or symbol):
        return _fail("rf_close_symbol_mismatch")
    if journal is None or not hasattr(journal, "record_rf_reentry_block"):
        return _fail("durable_journal_unavailable")
    try:
        inserted = journal.record_rf_reentry_block(
            symbol=canonical, direction=position_side, rf_signal_timestamp=signal_ts,
            close_ts=close_ts,
            metadata={
                "rf_signal": rf_signal, "close_reason": RF_OPPOSITE_CLOSE_REASON,
                "closed_candle_timestamp": int(ctx.get("closed_candle_timestamp") or signal_ts),
                "trade_id": str(snap.get("trade_id") or ctx.get("trade_id") or ""),
                "source": RF_REENTRY_CLOSED_SOURCE,
            },
        )
    except Exception as exc:
        return _fail(f"persist_error:{type(exc).__name__}")
    if not inserted:
        log_execution(
            f"[RF_REENTRY] {symbol} {position_side} block already durable for signal {signal_ts}",
            "WARN", debounce_key=f"rf_reentry_duplicate:{canonical}:{position_side}:{signal_ts}",
            debounce_sec=30,
        )
    return True


def close_position_on_opposite_rf(symbol, rf_result=None, *, df=None, now_ms=None):
    """Close an open position only on a proven opposite RF signal from closed candles."""
    if not STATE.get("open"):
        return False
    position_side = str(STATE.get("side") or "").upper()
    sym = str(symbol or STATE.get("current_symbol") or "")
    if position_side not in {"BUY", "SELL"} or not sym:
        return False
    rf = rf_result if isinstance(rf_result, dict) else _rf_signal_from_closed_candles(
        sym, df=df, now_ms=now_ms)
    if not isinstance(rf, dict) or str(rf.get("source") or "").upper() != RF_REENTRY_CLOSED_SOURCE:
        return False
    rf_signal = str(rf.get("signal") or "").upper()
    if rf_signal not in {"BUY", "SELL"} or rf_signal == position_side:
        return False
    try:
        signal_ts = int(rf.get("signal_candle_timestamp") or rf.get("closed_candle_timestamp") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    if signal_ts <= 0:
        return False
    STATE["rf_opposite_close"] = {
        "symbol": sym, "position_side": position_side, "rf_signal": rf_signal,
        "rf_signal_candle_timestamp": signal_ts,
        "closed_candle_timestamp": int(rf.get("closed_candle_timestamp") or signal_ts),
        "close_ts": time.time(), "source": RF_REENTRY_CLOSED_SOURCE,
        "trade_id": str(STATE.get("trade_id") or ""),
    }
    if not safe_close_position(sym, close_reason=RF_OPPOSITE_CLOSE_REASON):
        STATE.pop("rf_opposite_close", None)
        return False
    if not STATE.get("close_verified"):
        STATE.pop("rf_opposite_close", None)
        return False
    finalize_trade_with_reality(sym, close_reason=RF_OPPOSITE_CLOSE_REASON)
    return True


def _release_rf_reentry_block_after_fill(journal, symbol, side, gate_result):
    """Release the RF wait only after the verified fill consumed its later signal."""
    if not journal:
        return 0
    try:
        signal_ts = int((gate_result or {}).get("rf_reentry_signal_timestamp") or 0)
    except (TypeError, ValueError, OverflowError):
        signal_ts = 0
    if signal_ts <= 0:
        return 0
    if not hasattr(journal, "release_rf_reentry_blocks"):
        raise RuntimeError("release_rf_reentry_blocks unavailable")
    return int(journal.release_rf_reentry_blocks(
        symbol=_rf_reentry_symbol(symbol), direction=str(side or "").upper(),
        signal_timestamp=signal_ts,
    ) or 0)


# --- Controlled recovery-exit contract ------------------------------------ #
# The process never exits to "ask" an unverified exposure to disappear. It may
# exit only after a final read-only reconciliation proves FLAT or an open
# position whose exchange-side native stop is still VERIFIED. The non-zero code
# is the restart signal for an external supervisor (Railway ON_FAILURE) while a
# failing /health state is the restart signal for Render health checks.
_RECOVERY_STATE_KEY = "recovery_state_v1"
EXIT_RECOVERY_EXHAUSTED = 3
EXIT_SIGINT = 130
EXIT_SIGTERM = 143
RECOVERY_EXIT_RECHECK_SEC = 30.0
_SHUTDOWN_STATE = {"requested": None, "reason": "", "reconciling": False,
                   "trading_stopped": False}
_SHUTDOWN_LOCK = threading.RLock()


def _load_recovery_state():
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None or not hasattr(journal, "get_runtime_state"):
        return None
    try:
        value = journal.get_runtime_state(_RECOVERY_STATE_KEY)
        return value if isinstance(value, dict) else None
    except Exception as exc:
        log_execution(f"[RECOVERY_STATE] read failed: {type(exc).__name__}: {exc}", "ERROR")
        return None


def _persisted_recovery_hold_active():
    record = _load_recovery_state()
    return bool(record and not _health_bool(record.get("safe_to_exit", False)))


def _mark_persisted_recovery_resolved(exposure_status, status_label):
    """Clear an inherited hold only after this attempt's reconciliation succeeded."""
    return _persist_recovery_state(status_label, exposure_status, safe_to_exit=True,
                                   details={"phase": "startup_reconciled"})


def _persist_recovery_state(reason, exposure_status, *, safe_to_exit=False, details=None):
    """Persist the pre-exit safety contract before any process termination."""
    payload = {
        "version": 1,
        "reason": str(reason or "UNKNOWN"),
        "exposure_status": str(exposure_status or "UNKNOWN"),
        "safe_to_exit": bool(safe_to_exit),
        "ts": time.time(),
        "pid": os.getpid(),
        "symbol": str(STATE.get("current_symbol") or ""),
        "side": str(STATE.get("side") or ""),
        "trade_id": str(STATE.get("trade_id") or ""),
        "remaining_qty": safe_float(STATE.get("remaining_qty"), 0.0),
        "native_sl_status": str(STATE.get("native_sl_status") or "UNKNOWN"),
        "native_sl_order_id": str(STATE.get("native_sl_order_id") or ""),
        "native_sl_price": safe_float(STATE.get("native_sl_original_price")
                                      or STATE.get("sl") or STATE.get("synthetic_sl"), 0.0),
        "native_tp1_price": safe_float(STATE.get("tp1_price"), 0.0),
        "native_tp2_price": safe_float(STATE.get("tp2_price"), 0.0),
        "recovery_hold": bool(STATE.get("recovery_hold")),
        "entry_blocked": bool(globals().get("RECOVERY_BLOCK_ENTRIES", False)),
        "details": dict(details or {}),
    }
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None or not hasattr(journal, "set_runtime_state"):
        log_execution("[RECOVERY_STATE] persistence unavailable; exit gate remains fail-closed", "ERROR")
        return False
    try:
        journal.set_runtime_state(_RECOVERY_STATE_KEY, payload)
        return True
    except Exception as exc:
        log_execution(f"[RECOVERY_STATE] write failed; exit gate remains fail-closed: {type(exc).__name__}: {exc}", "ERROR")
        return False


def _paper_exit_state_verified(state):
    """Read-only protection check for one PAPER position snapshot."""
    trigger = safe_float(state.get("synthetic_sl") or state.get("sl"), 0.0)
    return bool(state.get("open") and str(state.get("side") or "").upper() in {"BUY", "SELL"}
                and safe_float(state.get("remaining_qty"), 0.0) > 0 and trigger > 0
                and str(state.get("native_sl_status") or "PAPER_SIMULATED")
                in {"PAPER_SIMULATED", "VERIFIED"}
                and not state.get("execution_unresolved")
                and not state.get("close_reconciliation_required"))


def _live_exit_focused_position_verified(positions):
    """Verify the focused LIVE position against its own exchange exposure."""
    if not STATE.get("open"):
        return False
    symbol = str(STATE.get("current_symbol") or "")
    side = str(STATE.get("side") or "").upper()
    qty = safe_float(STATE.get("remaining_qty") or STATE.get("exchange_qty"), 0.0)
    trigger = safe_float(STATE.get("native_sl_original_price")
                         or STATE.get("sl") or STATE.get("synthetic_sl"), 0.0)
    if not symbol or side not in {"BUY", "SELL"} or qty <= 0 or trigger <= 0:
        return False
    matches = [
        position for position in (positions or [])
        if normalize_symbol(_position_symbol(position)) == normalize_symbol(symbol)
    ]
    if len(matches) != 1:
        return False
    status, order_id, matched_price = verify_native_stop_loss_detailed(
        symbol, side, trigger, qty, position_side=STATE.get("position_side"))
    verified = status == "VERIFIED"
    if verified:
        STATE.update({"native_sl_status": "VERIFIED", "native_sl_verified": True,
                      "native_sl_order_id": str(order_id or ""),
                      "native_sl_original_price": safe_float(matched_price, trigger)})
    return verified


def _verified_open_position_for_exit(positions):
    """Verify every registered position, not just the currently focused one."""
    runtime = globals().get("PORTFOLIO_RUNTIME")
    active = []
    if runtime is not None:
        try:
            active = list(runtime.status().get("active", []))
        except Exception:
            active = []
    if PAPER_MODE:
        if not active:
            return _paper_exit_state_verified(STATE)
        if len(active) > portfolio_allocator.TOTAL_CAPACITY:
            return False
        for item in active:
            entry = getattr(runtime, "_positions", {}).get(str(item.get("trade_id") or ""))
            if entry is None or not _paper_exit_state_verified(entry.state):
                return False
        return True
    if active:
        if (not isinstance(positions, (list, tuple))
                or len(positions) != len(active)
                or len(positions) > portfolio_allocator.TOTAL_CAPACITY):
            return False
        if _portfolio_exchange_unexpected(positions):
            return False
        for item in active:
            try:
                with runtime.focus(str(item["trade_id"])):
                    if not _live_exit_focused_position_verified(positions):
                        return False
            except Exception:
                return False
        return True
    # Legacy/manual exposure without a registry entry keeps the strict
    # single-position rule; it must never be broadened to ambiguous sets.
    if not isinstance(positions, (list, tuple)) or len(positions) != 1:
        return False
    return _live_exit_focused_position_verified(positions)


def _recovery_exit_gate(reason, *, requested_signal=None):
    """Return True only when a controlled restart is safe; otherwise hold."""
    global RECOVERY_BLOCK_ENTRIES
    with _SHUTDOWN_LOCK:
        if _SHUTDOWN_STATE["reconciling"]:
            return False
        _SHUTDOWN_STATE.update({"reconciling": True, "trading_stopped": True,
                                "requested": requested_signal or _SHUTDOWN_STATE["requested"]})
    try:
        RECOVERY_BLOCK_ENTRIES = True
        STATE["recovery_hold"] = True
        STATE["recovery_reason"] = str(reason or "RECOVERY_EXIT_RECONCILIATION")
        MEMORY["recovery_status"] = "RECOVERY_EXIT_RECONCILIATION"
        _MAIN_LOOP_RESTART_STATE["supervisor_exhausted"] = True
        persisted = _persist_recovery_state(
            STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
            details={"phase": "reconciling",
                     "requested_signal": requested_signal or _SHUTDOWN_STATE["requested"]})
        if not PAPER_MODE and not persisted:
            log_execution("[EXIT_GATE] Recovery state could not be persisted; refusing process exit", "CRITICAL")
            return False
        if _MAIN_LOOP_RESTART_STATE.get("in_progress"):
            # No second verifier may be spawned while an older verifier is alive.
            _persist_recovery_state(STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
                                    details={"phase": "verifier_single_flight_hold"})
            log_execution("[EXIT_GATE] Verifier already in progress; refusing a second verification", "WARN")
            return False
        status, positions = fetch_position_tri_state(None, strict_account_snapshot=True)
        positions = positions or []
        symbol = str(STATE.get("current_symbol") or "")
        if status == "FLAT":
            runtime = globals().get("PORTFOLIO_RUNTIME")
            active_items = []
            if runtime is not None:
                try:
                    active_items = list(runtime.status().get("active", []))
                except Exception:
                    active_items = []
            if active_items:
                # Every registry position must be finalized in its own focus
                # before the process may declare a portfolio-wide FLAT state.
                for item in active_items:
                    try:
                        with runtime.focus(str(item["trade_id"])):
                            item_symbol = str(STATE.get("current_symbol") or item.get("symbol") or "")
                            if not item_symbol or not _clear_confirmed_flat(item_symbol, reason):
                                raise RuntimeError(f"flat_finalization_failed:{item['trade_id']}")
                    except Exception as exc:
                        _persist_recovery_state(
                            STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
                            details={"phase": "portfolio_flat_finalization_failed", "error": str(exc)},
                        )
                        return False
                _recompute_portfolio_globals()
                if _portfolio_active_ids() or _portfolio_has_open_positions():
                    _persist_recovery_state(STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
                                            details={"phase": "portfolio_flat_incomplete"})
                    return False
            else:
                if STATE.get("open") and not symbol:
                    _persist_recovery_state(STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
                                            details={"phase": "flat_without_symbol"})
                    return False
                if STATE.get("open") and symbol and not _clear_confirmed_flat(symbol, reason):
                    _persist_recovery_state(STATE["recovery_reason"], "UNKNOWN", safe_to_exit=False,
                                            details={"phase": "flat_finalization_failed"})
                    return False
            STATE.update({"recovery_hold": False, "execution_unresolved": False,
                          "close_reconciliation_required": False})
            MEMORY["recovery_status"] = "EXIT_RECONCILED_FLAT"
            persisted = _persist_recovery_state(
                STATE["recovery_reason"], "FLAT", safe_to_exit=True,
                details={"phase": "verified_flat", "exchange_flat_confirmed": True})
            return True if PAPER_MODE or persisted else False
        if status == "OPEN" and _verified_open_position_for_exit(positions):
            MEMORY["recovery_status"] = "EXIT_VERIFIED_PROTECTED_OPEN"
            persisted = _persist_recovery_state(
                STATE["recovery_reason"], "OPEN", safe_to_exit=True,
                details={"phase": "verified_open",
                         "native_sl_verified": True,
                         "position_count": len(positions),
                         "exchange_native_stop_verified": not PAPER_MODE})
            return True if PAPER_MODE or persisted else False
        exposure = str(status or "UNKNOWN")
        STATE["recovery_hold"] = True
        RECOVERY_BLOCK_ENTRIES = True
        MEMORY["recovery_status"] = "EXIT_REFUSED_UNVERIFIED_EXPOSURE"
        _persist_recovery_state(STATE["recovery_reason"], exposure, safe_to_exit=False,
                                details={"phase": "exit_refused",
                                         "position_count": len(positions),
                                         "paper_mode": bool(PAPER_MODE)})
        log_execution(
            f"[EXIT_GATE] Refusing process exit: exposure={exposure}; "
            "position state and exchange-side protection are not verified. Holding lock and read-only recovery.",
            "CRITICAL", debounce_key=f"exit_gate_refused:{exposure}", debounce_sec=30)
        return False
    finally:
        with _SHUTDOWN_LOCK:
            _SHUTDOWN_STATE["reconciling"] = False


def _shutdown_reason():
    with _SHUTDOWN_LOCK:
        return str(_SHUTDOWN_STATE.get("requested") or "")


def _request_shutdown(signum, _frame):
    """Signals request shutdown; only _recovery_exit_gate may allow the exit."""
    global RECOVERY_BLOCK_ENTRIES
    name = "SIGINT" if int(signum) == int(signal.SIGINT) else "SIGTERM"
    with _SHUTDOWN_LOCK:
        _SHUTDOWN_STATE["requested"] = name
        _SHUTDOWN_STATE["reason"] = name
    # Stop new trading work immediately; the final gate still decides whether
    # the process may exit or must remain in read-only recovery.
    RECOVERY_BLOCK_ENTRIES = True
    STATE["recovery_hold"] = True
    STATE["recovery_reason"] = f"SHUTDOWN_{name}"
    log_execution(f"[SHUTDOWN] {name} requested; entries blocked and exit deferred until exposure is verified", "WARN")


def _install_shutdown_handlers():
    if threading.current_thread() is not threading.main_thread():
        return
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signum, _request_shutdown)
        except (ValueError, OSError):
            pass


def _close_dashboard_server(server):
    """Close a Waitress server using its supported API exactly once."""
    close = getattr(server, "close", None)
    if callable(close):
        close()
        return
    # Only a genuinely legacy server without close() gets the Werkzeug path.
    for method_name in ("shutdown", "server_close"):
        method = getattr(server, method_name, None)
        if callable(method):
            method()


def _stop_dashboard_server(server, thread):
    try:
        _close_dashboard_server(server)
    except Exception as exc:
        log_execution(f"[SHUTDOWN] dashboard close failed: {type(exc).__name__}: {exc}", "WARN")
    try:
        thread.join(timeout=2.0)
        is_alive = getattr(thread, "is_alive", None)
        if callable(is_alive) and is_alive():
            log_execution("[SHUTDOWN] dashboard thread did not exit within 2.0s", "WARN")
    except Exception as exc:
        log_execution(f"[SHUTDOWN] dashboard join failed: {type(exc).__name__}: {exc}", "WARN")


def _stop_runtime_services():
    """Stop and join every owned worker, then close Waitress and the watchdog."""
    global _RUNTIME_SHUTTING_DOWN, _RUNTIME_WORKERS_STARTED, _NAISO_SERVICES_STARTED
    with _RUNTIME_WORKERS_LOCK:
        _RUNTIME_SHUTTING_DOWN = True

    for name in ("institutional_updater", "keep_alive"):
        _stop_named_worker(name)

    supervisor = globals().get("_reconciliation_supervisor")
    if supervisor is not None and hasattr(supervisor, "stop"):
        try:
            supervisor.stop()
        except Exception as exc:
            log_execution(f"[SHUTDOWN] reconciliation stop failed: {type(exc).__name__}: {exc}", "WARN")

    notifier = globals().get("TELEGRAM_NOTIFIER")
    if notifier is not None and hasattr(notifier, "close"):
        try:
            notifier.close()
        except Exception as exc:
            log_execution(f"[SHUTDOWN] Telegram stop failed: {type(exc).__name__}: {exc}", "WARN")

    event_bus = globals().get("_event_bus")
    if event_bus is not None and hasattr(event_bus, "stop"):
        try:
            event_bus.stop()
        except Exception as exc:
            log_execution(f"[SHUTDOWN] event bus stop failed: {type(exc).__name__}: {exc}", "WARN")

    for server, thread in list(globals().get("DASHBOARD_SERVERS") or []):
        _stop_dashboard_server(server, thread)
    with _RUNTIME_WORKERS_LOCK:
        globals()["DASHBOARD_SERVERS"] = []
        globals()["DASHBOARD_ACTIVE_PORTS"] = []

    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is not None and hasattr(watchdog, "stop"):
        try:
            watchdog.stop()
        except Exception as exc:
            log_execution(f"[SHUTDOWN] watchdog stop failed: {type(exc).__name__}: {exc}", "WARN")

    with _RUNTIME_WORKERS_LOCK:
        _RUNTIME_WORKERS_STARTED = False
        _NAISO_SERVICES_STARTED = False


def _release_runtime_lock(handle):
    from ggp_instance_lock import release_instance_lock
    return release_instance_lock(handle)


def run_trade_supervisor(*, lock_handle=None, lock_path=None):
    """Foreground runtime entry point implementing the controlled restart contract."""
    _install_shutdown_handlers()
    try:
        try:
            result = safe_main_loop()
            requested = _shutdown_reason()
            if requested:
                reason = requested
            elif result is False:
                reason = "SUPERVISOR_RESTART_LIMIT_EXHAUSTED"
            else:
                reason = "CLEAN_EXIT"
        except KeyboardInterrupt:
            reason = "SIGINT"
        while not _recovery_exit_gate(reason, requested_signal=_shutdown_reason()):
            log_execution("[EXIT_GATE] Holding read-only recovery; no new trading work and no process exit",
                          "ERROR", debounce_key="exit_gate_hold", debounce_sec=60)
            time.sleep(RECOVERY_EXIT_RECHECK_SEC)
        _stop_runtime_services()
        code = (EXIT_SIGTERM if reason == "SIGTERM" else
                EXIT_SIGINT if reason == "SIGINT" else EXIT_RECOVERY_EXHAUSTED)
        log_execution(f"[EXIT_GATE] Controlled restart exit code={code} reason={reason}; "
                      f"lock={lock_path or 'managed-by-caller'}", "CRITICAL")
        raise SystemExit(code)
    finally:
        _release_runtime_lock(lock_handle)


# NAISO-LIVE v1.1.0 long-run support: API circuit breaker, watchdog and
# trade-state-aware maintenance. Construction has no I/O side effects; services
# are started explicitly by the runtime supervisor, never at import time.
try:
    from naiso_api_resilience import ApiResilience, CircuitOpenError, classify_api_error
    from naiso_health import HealthWatchdog, RestartPolicy
    from naiso_maintenance import MaintenanceManager

    def _naiso_alert(message, level="ERROR"):
        # log_execution is the single incident fan-out: Dashboard + runtime log + Telegram.
        # Do not send a second Telegram-only copy from the watchdog callback.
        log_execution(str(message), str(level))

    def _naiso_safety_gate():
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            return False, "OPEN_POSITION"
        if STATE.get("recovery_hold") or globals().get("RECOVERY_BLOCK_ENTRIES", False):
            return False, "RECOVERY_HOLD"
        if STATE.get("execution_unresolved") or STATE.get("close_reconciliation_required"):
            return False, "EXECUTION_UNRESOLVED"
        if daily_loss_cooldown_active():
            return False, "DAILY_LOSS_COOLDOWN"
        return True, "READY"

    API_RESILIENCE = ApiResilience(
        failure_threshold=_env_int_range("API_CIRCUIT_FAILURE_THRESHOLD", 10, 3, 50),
        cooldown_sec=_env_float_range("API_CIRCUIT_COOLDOWN_SEC", 20.0, 5.0, 300.0),
        max_attempts=3,
    )
    API_RESILIENCE.set_enabled(not PAPER_MODE and os.getenv("NAISO_API_CIRCUIT", "true").lower() != "false")
    NAISO_WATCHDOG = HealthWatchdog(
        restart_policy=RestartPolicy(
            max_attempts=_env_int_range("WATCHDOG_RESTART_MAX_ATTEMPTS", 3, 1, 10),
            window_sec=_env_float_range("WATCHDOG_RESTART_WINDOW_SEC", 300.0, 30.0, 3600.0),
        ),
        alert_fn=_naiso_alert,
        safety_gate=_naiso_safety_gate,
    )
    NAISO_MAINTENANCE = MaintenanceManager(
        state_provider=lambda: {
            **STATE,
            "in_position": bool(TRADE_STATE.get("in_position")),
            "loss_cooldown_active": bool(_LOSS_COOLDOWN_STATE.get("until", 0) > time.time()),
        },
        journal_provider=lambda: globals().get("INSTITUTIONAL_TRADE_JOURNAL"),
        resource_provider=lambda: globals().get("INSTITUTIONAL_RESOURCE_MANAGER"),
        alert_fn=_naiso_alert,
        min_interval_sec=_env_float_range("MAINTENANCE_MIN_INTERVAL_SEC", 300.0, 30.0, 3600.0),
        runtime_ttl_days=_env_float_range("JOURNAL_RUNTIME_TTL_DAYS", 30.0, 1.0, 365.0),
        enable_gc=os.getenv("MAINTENANCE_ENABLE_GC", "true").lower() != "false",
    )
except Exception as _naiso_support_error:
    API_RESILIENCE = None
    NAISO_WATCHDOG = None
    NAISO_MAINTENANCE = None
    CircuitOpenError = RuntimeError
    classify_api_error = None
    log_execution(f"[NAISO_SUPPORT] optional services unavailable: {_naiso_support_error}", "WARN")

# Daily drawdown pause: separate from trade lifecycle and persisted independently.
LOSS_COOLDOWN_SECONDS = _env_int_range("LOSS_COOLDOWN_SECONDS", 600, 60, 3600)
_LOSS_COOLDOWN_LOCK = threading.RLock()
_LOSS_COOLDOWN_STATE_KEY = "daily_drawdown_cooldown_v1"
_LOSS_COOLDOWN_STATE = {"until": 0.0, "triggered_at": 0.0, "trigger_balance": None,
                        "utc_day": None, "peak_balance": None, "trigger_count": 0}
_LOSS_COOLDOWN_STORE_READY = False


def _persist_loss_cooldown_state():
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None:
        raise RuntimeError("trade journal unavailable; loss cooldown cannot be persisted")
    journal.set_runtime_state(_LOSS_COOLDOWN_STATE_KEY, dict(_LOSS_COOLDOWN_STATE))


def _load_loss_cooldown_state():
    global _LOSS_COOLDOWN_STORE_READY
    with _LOSS_COOLDOWN_LOCK:
        if _LOSS_COOLDOWN_STORE_READY:
            return True
        journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
        if journal is None:
            STATE["loss_cooldown_persistence_failed"] = True
            return False
        try:
            saved = journal.get_runtime_state(_LOSS_COOLDOWN_STATE_KEY, {})
            if isinstance(saved, dict):
                for key in _LOSS_COOLDOWN_STATE:
                    if key in saved:
                        _LOSS_COOLDOWN_STATE[key] = saved[key]
            _LOSS_COOLDOWN_STORE_READY = True
            STATE["daily_peak_balance"] = _LOSS_COOLDOWN_STATE.get("peak_balance")
            STATE["last_trade_day"] = _LOSS_COOLDOWN_STATE.get("utc_day")
            STATE["loss_cooldown_until"] = float(_LOSS_COOLDOWN_STATE.get("until") or 0.0)
            return True
        except Exception as exc:
            STATE["loss_cooldown_persistence_failed"] = True
            log_execution(f"[LOSS_COOLDOWN] persisted state load failed; entries fail-closed: {exc}", "ERROR")
            return False


def _loss_cooldown_readiness():
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        return False, "POSITION_STILL_OPEN"
    if STATE.get("execution_unresolved") or STATE.get("close_reconciliation_required"):
        return False, "EXECUTION_UNRESOLVED"
    if STATE.get("recovery_hold") or globals().get("RECOVERY_BLOCK_ENTRIES", False):
        return False, "RECOVERY_HOLD"
    if MEMORY.get("position_status") not in {"FLAT", "CLOSED", "PAPER_RECONCILED_FLAT", "RECONCILED_FLAT"}:
        return False, f"POSITION_STATUS_{MEMORY.get('position_status', 'UNKNOWN')}"
    return True, "READY"


def daily_loss_cooldown_active():
    """Pause new entries after a 5% balance drawdown; never liquidate an open trade."""
    now = time.time()
    if not _load_loss_cooldown_state():
        return True
    with _LOSS_COOLDOWN_LOCK:
        until = float(_LOSS_COOLDOWN_STATE.get("until") or 0.0)
        if now < until:
            return True
        if until > 0:
            ready, reason = _loss_cooldown_readiness()
            if not ready:
                log_execution(f"[LOSS_COOLDOWN] elapsed; entries remain blocked: {reason}", "WARN",
                              debounce_key=f"loss_cooldown_readiness:{reason}", debounce_sec=30)
                return True
            bal = get_balance_safe()
            if not bal or not math.isfinite(float(bal)) or float(bal) <= 0:
                log_execution("[LOSS_COOLDOWN] cannot rebase: exchange balance unavailable; entries remain blocked", "ERROR")
                return True
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            _LOSS_COOLDOWN_STATE.update({"until": 0.0, "peak_balance": float(bal),
                                         "utc_day": today, "trigger_balance": None})
            STATE.update({"loss_cooldown_until": 0.0, "daily_peak_balance": float(bal),
                          "last_trade_day": today, "daily_loss_limit_hit": False})
            try:
                _persist_loss_cooldown_state()
            except Exception as exc:
                STATE["loss_cooldown_persistence_failed"] = True
                log_execution(f"[LOSS_COOLDOWN] rebase persistence failed; entries fail-closed: {exc}", "ERROR")
                return True
            log_execution(f"[LOSS_COOLDOWN] pause complete; new drawdown reference={float(bal):.4f} USDT", "SUCCESS")
        bal = get_balance_safe()
        if not bal or not math.isfinite(float(bal)) or float(bal) <= 0:
            log_execution("[LOSS_COOLDOWN] balance unavailable; no new entries authorized", "WARN",
                          debounce_key="loss_cooldown_balance_unavailable", debounce_sec=30)
            return True
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state_changed = False
        if _LOSS_COOLDOWN_STATE.get("utc_day") != today or not _LOSS_COOLDOWN_STATE.get("peak_balance"):
            _LOSS_COOLDOWN_STATE.update({"utc_day": today, "peak_balance": float(bal)})
            state_changed = True
        elif float(bal) > float(_LOSS_COOLDOWN_STATE["peak_balance"]):
            _LOSS_COOLDOWN_STATE["peak_balance"] = float(bal)
            state_changed = True
        peak = float(_LOSS_COOLDOWN_STATE.get("peak_balance") or bal)
        loss_pct = max(0.0, (peak - float(bal)) / peak * 100.0) if peak > 0 else 0.0
        STATE["daily_peak_balance"] = peak
        STATE["last_trade_day"] = today
        if state_changed:
            try:
                _persist_loss_cooldown_state()
            except Exception as exc:
                STATE["loss_cooldown_persistence_failed"] = True
                log_execution(f"[LOSS_COOLDOWN] state persistence failed; entries fail-closed: {exc}", "ERROR")
                return True
        if loss_pct >= MAX_DAILY_LOSS_PCT:
            until = now + LOSS_COOLDOWN_SECONDS
            _LOSS_COOLDOWN_STATE.update({"until": until, "triggered_at": now,
                "trigger_balance": float(bal), "utc_day": today,
                "trigger_count": int(_LOSS_COOLDOWN_STATE.get("trigger_count") or 0) + 1})
            STATE["loss_cooldown_until"] = until
            try:
                _persist_loss_cooldown_state()
            except Exception as exc:
                STATE["loss_cooldown_persistence_failed"] = True
                log_execution(f"[LOSS_COOLDOWN] trigger persistence failed; entries fail-closed: {exc}", "ERROR")
                return True
            log_execution(f"[LOSS_COOLDOWN] drawdown={loss_pct:.2f}% reached {MAX_DAILY_LOSS_PCT:.2f}%; "
                f"new entries paused for {LOSS_COOLDOWN_SECONDS // 60} minutes; open trade management remains active", "ERROR")
            return True
        return False


def _integration_fetch_frame(symbol):
    return _get_scanner_frame(roro_exchange_symbol(symbol), 150)

# Per-symbol stateful trend/context engines. Keeping these instances separate avoids
# one asset's trend state leaking into another asset's Watchlist decision.
_WATCHLIST_TREND_ENGINES = {}
_WATCHLIST_TRADE_STATE_MACHINES = {}
def _enrich_with_existing_gpp_engines(frame, side, analysis, *, memory_target=None):
    """Closed-candle GGP evidence enrichment; stateful machines advance once per symbol candle."""
    target_memory = memory_target if isinstance(memory_target, dict) else MEMORY
    result = {}
    try:
        now_ms = int(time.time() * 1000)
        timeframe_seconds = _roro_timeframe_seconds(INTERVAL)
        symbol_for_snapshot = str(analysis.get("symbol") or analysis.get("exchange_symbol") or "")
        raw_last_ts = int(frame.timestamp.iloc[-1]) if frame is not None and len(frame) and "timestamp" in frame else 0
        # The in-progress candle may change, but the closed-candle snapshot remains
        # stable until the next interval. Cache key also includes the raw frame shape.
        snapshot_key = (roro_canonical_symbol(symbol_for_snapshot), str(INTERVAL),
                        raw_last_ts, len(frame) if frame is not None else 0)
        def _build_closed_snapshot():
            return roro_validate_closed_frame(frame, now_ms=now_ms, timeframe_s=timeframe_seconds)
        closed = WATCHLIST_SNAPSHOT_CACHE.get_or_compute(
            snapshot_key, _build_closed_snapshot, ttl_seconds=max(5.0, timeframe_seconds * 2.0)
        )
        if closed is None or len(closed) < 55:
            return {"gpp_engine_diagnostics": {"status": "PARTIAL", "reason": "insufficient_closed_candles"}}
        candle_ts = int(closed.timestamp.iloc[-1])
        evidence_key = (snapshot_key, candle_ts)
        def _build_direction_neutral_evidence():
            return {
                "smart_money": SmartMoneyEngine.analyze_smart_money(closed),
                "momentum": MomentumFlowEngine.analyze_momentum_flow(closed),
                "adx_state": ADXDIIntelligence.get_adx_state(closed),
                "di_state": ADXDIIntelligence.get_di_state(closed),
                "atr_value": float(compute_atr(closed).iloc[-1]),
            }
        evidence = WATCHLIST_SNAPSHOT_CACHE.get_or_compute(
            ("evidence", evidence_key), _build_direction_neutral_evidence,
            ttl_seconds=max(5.0, timeframe_seconds * 2.0)
        )
        smart = evidence["smart_money"]
        momentum = evidence["momentum"]
        adx_state = evidence["adx_state"]
        di_state = evidence["di_state"]
        result.update({
            "smart_money": smart,
            "momentum": momentum,
            "adx_di": {"adx": float(adx_state.get("value", 0) or 0),
                       "adx_state": str(adx_state.get("state", "UNKNOWN")),
                       "adx_slope": float(adx_state.get("slope", 0) or 0),
                       "di_dominant": str(di_state.get("dominant", "UNKNOWN")),
                       "di_spread": float(di_state.get("spread", 0) or 0)},
        })
        # Scanner 2 is informational/ranking-only; it never authorizes an entry.
        try:
            proximity = _roro_ema9_vwap_proximity(
                closed, ema, ema_len=9, timeframe_seconds=timeframe_seconds, now_ms=now_ms
            )
            if proximity:
                symbol_for_proximity = str(analysis.get("symbol") or analysis.get("exchange_symbol") or "")
                symbol_key = roro_canonical_symbol(symbol_for_proximity)
                candle_ts = int(closed.timestamp.iloc[-1])
                last_seen = target_memory.setdefault("_proximity_last_seen_by_symbol", {})
                if last_seen.get(symbol_key) != candle_ts:
                    target_memory["_proximity_batch_updates"] = int(target_memory.get("_proximity_batch_updates", 0)) + 1
                    last_seen[symbol_key] = candle_ts
                    _observed_at = time.time()
                    _proximity_frame_seconds = float(timeframe_seconds)
                    _proximity_cross_event = bool(proximity.get("cross_ema9") or proximity.get("cross_ema50"))
                    _proximity_cross_age = (
                        max(0.0, _observed_at - (float(proximity.get("candle_timestamp") or 0) / 1000.0
                                                 + _proximity_frame_seconds))
                        if _proximity_cross_event else None
                    )
                    _proximity_cross_max_age = 2.0 * _proximity_frame_seconds
                    proximity_item = {
                        "symbol": symbol_key, "exchange_symbol": roro_exchange_symbol(symbol_for_proximity),
                        **proximity,
                        "updated_at": _observed_at,
                        "observation_age_seconds": 0.0,
                        "timeframe_seconds": _proximity_frame_seconds,
                        "cross_age_seconds": _proximity_cross_age,
                        "cross_max_age_seconds": _proximity_cross_max_age,
                        "cross_fresh": bool(_proximity_cross_event and _proximity_cross_age <= _proximity_cross_max_age),
                        "data_quality": "CLOSED_CANDLES",
                        "role": "RANKING_ONLY_NOT_ENTRY_GATE",
                    }
                    _ranking_projection = _project_watchlist_row(
                        proximity_item, now_ts=_observed_at, ranking_only=True
                    )
                    _ranking_cross = _ranking_projection["cross_context"]
                    proximity_item["cross_state"] = _ranking_cross["cross_state"]
                    proximity_item["cross_context"] = _ranking_cross
                    # Scanner rows are ranking observations. Publish the already
                    # projected non-durable label so the dashboard cannot show a
                    # bare CROSS_CONFIRMED next to a durable cross event.
                    proximity_item["cross_state_display"] = _ranking_projection["cross_state_display"]
                    rows = target_memory.setdefault("ema9_vwap_proximity_scanner", [])
                    rows = [x for x in rows if x.get("symbol") != symbol_key]
                    rows.append(proximity_item)
                    rows.sort(key=lambda x: (float(x.get("score", 0) or 0), float(x.get("updated_at", 0) or 0)), reverse=True)
                    target_memory["ema9_vwap_proximity_scanner"] = rows[:30]
                result["ema9_vwap_proximity"] = {**proximity, "role": "RANKING_ONLY_NOT_ENTRY_GATE"}
                icons = dict(analysis.get("icons") or {})
                icons["ema_vwap_proximity"] = ("⚡" if proximity.get("cross_any") else
                                                "🟡" if proximity.get("state") in ("NEAR_CROSS", "CONVERGING") else
                                                "📈" if proximity.get("direction") == "BUY" else
                                                "📉" if proximity.get("direction") == "SELL" else "⚪")
                analysis["icons"] = icons
        except Exception as proximity_exc:
            result["ema9_vwap_proximity"] = {"state": "UNKNOWN", "reason": f"{type(proximity_exc).__name__}",
                                                "role": "RANKING_ONLY_NOT_ENTRY_GATE"}

        symbol_key = str(analysis.get("symbol") or roro_canonical_symbol(analysis.get("exchange_symbol")))
        engine_last = target_memory.setdefault("_watchlist_engine_last_candle", {})
        advance_state = engine_last.get(symbol_key) != candle_ts
        if symbol_key not in _WATCHLIST_TREND_ENGINES:
            if len(_WATCHLIST_TREND_ENGINES) >= 200:
                _WATCHLIST_TREND_ENGINES.pop(next(iter(_WATCHLIST_TREND_ENGINES)))
            _WATCHLIST_TREND_ENGINES[symbol_key] = InstitutionalTrendEngine()
        trend = _WATCHLIST_TREND_ENGINES[symbol_key]
        if advance_state:
            trend.update_trend_state(closed, analysis.get("order_block"))
        atr_value = float(evidence.get("atr_value", float("nan")))
        price_value = float(closed.close.iloc[-1])
        trend_ok, trend_reason = trend.should_enter(side, closed, analysis.get("order_block"), atr_value, price_value)
        result["institutional_trend"] = {
            "state": trend.trend_state.value,
            "health": int(trend.get_trend_health(closed, side)),
            "persistence": int(trend.trend_persistence),
            "supporting_check": bool(trend_ok),
            "supporting_reason": str(trend_reason),
        }
        if symbol_key not in _WATCHLIST_TRADE_STATE_MACHINES:
            if len(_WATCHLIST_TRADE_STATE_MACHINES) >= 200:
                _WATCHLIST_TRADE_STATE_MACHINES.pop(next(iter(_WATCHLIST_TRADE_STATE_MACHINES)))
            _WATCHLIST_TRADE_STATE_MACHINES[symbol_key] = TradeStateMachine()
        state_machine = _WATCHLIST_TRADE_STATE_MACHINES[symbol_key]
        if advance_state:
            state_machine.update(
                smart, momentum, float(adx_state.get("value", 0) or 0),
                str(analysis.get("market_regime", "UNKNOWN")), side=side,
                source="WATCHLIST_DISCOVERY", symbol=symbol_key, candle_ts=candle_ts
            )
            engine_last[symbol_key] = candle_ts
        result["trade_state"] = state_machine.current_state
        result["engine_candle_timestamp"] = candle_ts
        result["engine_state_advanced"] = bool(advance_state)
        icons = dict(analysis.get("icons") or {})
        icons["smart_money"] = "🧠" if smart.get("smart_money_dominant") else "⚪"
        icons["momentum"] = "⚡" if momentum.get("trend_expansion") else "🟡" if momentum.get("momentum_decay") else "⚪"
        icons["trend"] = "📈" if trend.trend_state.value in ("BULLISH", "PROBATION_BULLISH") else "📉" if trend.trend_state.value in ("BEARISH", "PROBATION_BEARISH") else "⚪"
        analysis["icons"] = icons
        # One read-only projection over fields already produced above. It never
        # alters decision_state, entry_readiness, authority, SL/TP or sizing.
        try:
            combined_row = {**analysis, **result}
            projected = _project_watchlist_row(combined_row, now_ts=now_ms / 1000.0)
            # Additive evidence report over the same closed-candle row. It adds no
            # scan, no cache entry and no authority; it only makes the existing
            # institutional evidence auditable in the Watchlist/dashboard.
            result["institutional_report"] = build_institutional_report(
                combined_row, side=str(analysis.get("side") or "").upper(), now_ts=now_ms / 1000.0,
            )
            result["unified_decision_context"] = projected
            result["cross_state"] = projected["cross_context"]["cross_state"]
            result["projected_decision_state"] = projected["decision_context"]["decision_state"]
            result["analysis_status"] = projected["decision_context"]["analysis_status"]
            result["unified_entry_readiness"] = projected["entry_readiness"]
            result["timeframe_view"] = projected["timeframe_view"]
        except Exception as projection_exc:
            result["unified_projection_diagnostics"] = {
                "status": "PARTIAL", "reason": f"{type(projection_exc).__name__}",
            }
    except Exception as exc:
        result["gpp_engine_diagnostics"] = {"status": "PARTIAL", "reason": f"{type(exc).__name__}:{str(exc)[:120]}"}
    return result

_last_unified_watchlist_refresh = 0.0
INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC = _env_float_range(
    "INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC", 90.0, 30.0, 300.0
)
_UNIFIED_WATCHLIST_REFRESH_STATE = {
    "lock": threading.Lock(), "in_progress": False, "generation": 0, "last_attempt": 0.0,
    "last_attempt_monotonic": 0.0, "started_monotonic": 0.0,
    "last_progress_monotonic": 0.0, "last_completed_monotonic": 0.0,
    "phase": "IDLE", "current_symbol": "", "timed_out": False,
    "restart_requested": False, "last_error": "", "worker_name": "",
}


def _watchlist_worker_progress(phase, symbol="", generation=None):
    """Record real worker phase progress; never let a stale worker publish progress."""
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        if (not state.get("in_progress") or state.get("timed_out")
                or (generation is not None and int(state.get("generation", 0)) != int(generation))):
            return
        state["last_progress_monotonic"] = time.monotonic()
        state["phase"] = str(phase or "UNKNOWN")[:80]
        state["current_symbol"] = str(symbol or "")[:80]


def _watchlist_worker_health_snapshot():
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        row = {key: value for key, value in state.items() if key != "lock"}
    now = time.monotonic()
    last_progress = float(row.get("last_progress_monotonic") or row.get("started_monotonic") or 0.0)
    age = max(0.0, now - last_progress) if row.get("in_progress") else 0.0
    stalled = bool(row.get("timed_out")) or bool(
        row.get("in_progress") and age >= INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC
    )
    row.update({
        "progress_age_sec": round(age, 3),
        "stall_timeout_sec": float(INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC),
        "stalled": stalled,
    })
    return row


def _watchlist_worker_generation_active(generation):
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        return bool(state.get("in_progress") and not state.get("timed_out")
                    and int(state.get("generation", 0)) == int(generation))


def _publish_watchlist_worker_delta(staged_memory, baseline_watchlist, baseline_refs, generation):
    """Publish only this worker's changed rows; never overwrite concurrent scanner writes."""
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    with state["lock"]:
        if (not state.get("in_progress") or state.get("timed_out")
                or int(state.get("generation", 0)) != int(generation)):
            return False
        live = MEMORY.setdefault("watchlist", {})
        staged = staged_memory.get("watchlist", {})
        for key, row in staged.items():
            if not isinstance(row, dict):
                continue
            baseline_row = baseline_watchlist.get(key)
            if baseline_row is not None:
                staged_ts = float(row.get("last_update", 0.0) or 0.0)
                baseline_ts = float(baseline_row.get("last_update", 0.0) or 0.0)
                if staged_ts <= baseline_ts:
                    continue
            current = live.get(key)
            baseline_ref = baseline_refs.get(key)
            if key in baseline_watchlist:
                if current is not baseline_ref:
                    continue
                current_ts = float(current.get("last_update", 0.0) or 0.0) if isinstance(current, dict) else 0.0
                if current_ts > baseline_ts:
                    continue
            elif current is not None:
                continue
            # The staged row is already detached from the live Watchlist; transfer
            # ownership rather than deep-copying arbitrary nested pandas objects.
            live[key] = row
        for key in ("unified_watchlist_cursor", "unified_watchlist_last_update",
                    "unified_watchlist_last_batch_count", "unified_watchlist_source_count",
                    "_proximity_batch_updates", "_proximity_last_seen_by_symbol",
                    "ema9_vwap_proximity_scanner", "confirmed_cross_events",
                    "confirmed_cross_events_last_sync", "_watchlist_engine_last_candle"):
            if key in staged_memory:
                MEMORY[key] = staged_memory[key]
        return True


def _finalize_cross_watchlist_delivery_after_publish():
    """Mark durable cross events delivered only after their rows are published."""
    delivery = MEMORY.get("_cross_watchlist_delivery")
    if not isinstance(delivery, dict):
        return False
    pending_ids = {
        str(value).strip() for value in (delivery.get("event_ids") or [])
        if str(value).strip()
    }
    if not pending_ids:
        return False
    published_ids = {
        str(row.get("cross_event_id")).strip()
        for row in (MEMORY.get("watchlist") or {}).values()
        if isinstance(row, dict) and row.get("cross_event_id")
    }
    delivered_ids = sorted(pending_ids.intersection(published_ids))
    if not delivered_ids:
        return False
    delivered_at = time.time()
    marked = _mark_cross_events_watchlist_delivered(delivered_ids, delivered_at)
    delivery = dict(delivery)
    all_delivered = len(delivered_ids) == len(pending_ids)
    delivery.update({
        "delivered_at": delivered_at,
        "delivered_ids": delivered_ids,
        "lifecycle_marked": list(marked or []),
        "delivered": all_delivered,
        "same_cycle": False,
        "lifecycle_status": ACTIVE_WATCHLIST_LIFECYCLE if all_delivered
                            else delivery.get("lifecycle_status", CROSS_WATCHLIST_LIFECYCLE),
        "refresh_seconds": round(delivered_at - float(delivery.get("detected_at", delivered_at)), 3),
        "reason": "ACTIVE_WATCHLIST_DELIVERED" if all_delivered
                  else "PARTIAL_WATCHLIST_DELIVERY",
    })
    MEMORY["_cross_watchlist_delivery"] = delivery
    return bool(delivery["delivered"])


def _run_unified_institutional_watchlist_refresh(generation=None):
    """Analyze a private snapshot; publish only if this worker generation is still current."""
    global _last_unified_watchlist_refresh
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    if generation is None:
        with state["lock"]:
            generation = int(state.get("generation", 0))
    if not _watchlist_worker_generation_active(generation):
        # A timed-out worker must never publish staged rows or clear the
        # process-level recovery fence, even if it completes late.
        with state["lock"]:
            if state.get("timed_out"):
                MEMORY["_unified_watchlist_refresh_ok"] = False
                MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
                state["in_progress"] = False
                state["worker_name"] = ""
                state["last_completed_monotonic"] = time.monotonic()
        return
    success = False
    baseline_watchlist = copy.deepcopy(MEMORY.get("watchlist", {}))
    baseline_refs = dict(MEMORY.get("watchlist", {}))
    staged_memory = dict(MEMORY)
    staged_memory["watchlist"] = copy.deepcopy(baseline_watchlist)
    staged_memory["_proximity_last_seen_by_symbol"] = copy.deepcopy(MEMORY.get("_proximity_last_seen_by_symbol", {}))
    staged_memory["ema9_vwap_proximity_scanner"] = copy.deepcopy(MEMORY.get("ema9_vwap_proximity_scanner", []))
    staged_memory["_watchlist_engine_last_candle"] = copy.deepcopy(MEMORY.get("_watchlist_engine_last_candle", {}))
    staged_memory["confirmed_cross_events"] = copy.deepcopy(MEMORY.get("confirmed_cross_events", []))
    staged_memory["_proximity_batch_updates"] = 0
    progress = lambda phase, symbol="": _watchlist_worker_progress(phase, symbol, generation)
    try:
        progress("CROSS_EVENT_SYNC")
        _sync_confirmed_cross_events(
            progress_callback=progress,
            lifecycle_max_items=max(1, INSTITUTIONAL_WATCHLIST_BATCH_SIZE * 2),
            memory_target=staged_memory,
        )

        def tracked_fetch_frame(symbol):
            progress("OHLCV_FETCH", symbol)
            try:
                return _integration_fetch_frame(symbol)
            finally:
                progress("OHLCV_FETCH_COMPLETE", symbol)

        def tracked_enrich(frame, side, analysis):
            progress("INSTITUTIONAL_ANALYSIS", analysis.get("symbol", ""))
            try:
                return _enrich_with_existing_gpp_engines(frame, side, analysis, memory_target=staged_memory)
            finally:
                progress("INSTITUTIONAL_ANALYSIS_COMPLETE", analysis.get("symbol", ""))

        progress("WATCHLIST_BATCH_ANALYSIS")
        result = update_unified_watchlist(
            staged_memory, tracked_fetch_frame,
            max_items=INSTITUTIONAL_WATCHLIST_MAX_SYMBOLS,
            batch_size=INSTITUTIONAL_WATCHLIST_BATCH_SIZE,
            enrich=tracked_enrich
        )
        if not _publish_watchlist_worker_delta(staged_memory, baseline_watchlist, baseline_refs, generation):
            # A timed-out generation is intentionally fenced from publishing.
            # Record its terminal state explicitly; do not leave the refresh
            # looking permanently in progress after the stale worker exits.
            with state["lock"]:
                if state.get("timed_out"):
                    MEMORY["_unified_watchlist_refresh_ok"] = False
                    MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
            return
        _finalize_cross_watchlist_delivery_after_publish()
        proximity_meta = MEMORY.setdefault("ema9_vwap_proximity_scanner_meta", {})
        proximity_meta.update({
            "observed_this_batch": int(staged_memory.get("_proximity_batch_updates", 0)),
            "symbols": len(staged_memory.get("ema9_vwap_proximity_scanner", [])),
            "updated_at": time.time(), "role": "RANKING_ONLY",
            "batch_size": INSTITUTIONAL_WATCHLIST_BATCH_SIZE,
            "refresh_seconds": INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS,
            "closed_candles_only": True,
        })
        _last_unified_watchlist_refresh = time.time()
        with state["lock"]:
            state["last_error"] = ""
        success = True
        if INSTITUTIONAL_RESOURCE_MANAGER:
            INSTITUTIONAL_RESOURCE_MANAGER.record({"event":"watchlist_refresh", "count":len(result), "ts":time.time()})
        log_execution(f"[INTEGRATION] unified watchlist analyzed={len(result)}", "INFO")
    except Exception as exc:
        with state["lock"]:
            if int(state.get("generation", 0)) == int(generation) and not state.get("timed_out"):
                MEMORY["_unified_watchlist_refresh_ok"] = False
                MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
        log_execution(
            f"[INTEGRATION] watchlist refresh failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            "ERROR",
        )
    finally:
        with state["lock"]:
            if int(state.get("generation", 0)) == int(generation):
                state["in_progress"] = False
                state["worker_name"] = ""
                state["last_completed_monotonic"] = time.monotonic()
                if success and not state.get("timed_out"):
                    state["phase"] = "IDLE"
                    state["current_symbol"] = ""
                    MEMORY["_unified_watchlist_refresh_ok"] = True
                    MEMORY["_unified_watchlist_refresh_status"] = "SUCCESS"
                else:
                    MEMORY["_unified_watchlist_refresh_ok"] = False
                    MEMORY["_unified_watchlist_refresh_status"] = "FAILED"


def refresh_unified_institutional_watchlist(force=False):
    """Schedule one background refresh; stale workers trigger verified process recovery.

    The foreground trading loop never waits for Scanner OHLCV or institutional
    analysis. A worker that stops making progress is permanently fenced for this
    process and can only be recovered through the external process supervisor.
    """
    now = time.time()
    now_mono = time.monotonic()
    state = _UNIFIED_WATCHLIST_REFRESH_STATE
    restart_context = None
    with state["lock"]:
        if state.get("timed_out"):
            MEMORY["_unified_watchlist_refresh_ok"] = False
            MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
            return MEMORY.setdefault("watchlist", {})
        if state["in_progress"]:
            last_progress = float(state.get("last_progress_monotonic") or state.get("started_monotonic") or now_mono)
            stale_for = max(0.0, now_mono - last_progress)
            if stale_for >= INSTITUTIONAL_WATCHLIST_WORKER_STALL_SEC and not state.get("restart_requested"):
                state["timed_out"] = True
                state["generation"] = int(state.get("generation", 0)) + 1
                state["restart_requested"] = True
                state["last_error"] = f"WORKER_STALLED:{state.get('phase')}:{stale_for:.1f}s"
                MEMORY["_unified_watchlist_refresh_ok"] = False
                MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
                globals()["RECOVERY_BLOCK_ENTRIES"] = True
                restart_context = {
                    "reason": "UNIFIED_WATCHLIST_WORKER_STALLED",
                    "worker_name": state.get("worker_name"),
                    "worker_phase": state.get("phase"),
                    "worker_symbol": state.get("current_symbol"),
                    "worker_stale_for_sec": round(stale_for, 3),
                }
            else:
                # IN_PROGRESS is not a refresh failure. The entry worker will
                # defer candidate validation until this generation completes.
                MEMORY["_unified_watchlist_refresh_status"] = "IN_PROGRESS"
                return MEMORY.setdefault("watchlist", {})
        else:
            last_attempt = float(state.get("last_attempt_monotonic") or 0.0)
            last_success = float(state.get("last_completed_monotonic") or 0.0)
            last_completed = max(last_attempt, last_success)
            if not force and now_mono - last_completed < INSTITUTIONAL_WATCHLIST_REFRESH_SECONDS:
                return MEMORY.setdefault("watchlist", {})
            generation = int(state.get("generation", 0)) + 1
            state.update({
                "generation": generation,
                "in_progress": True, "timed_out": False, "restart_requested": False,
                "last_attempt": now,
                "last_attempt_monotonic": now_mono,
                "started_monotonic": now_mono, "last_progress_monotonic": now_mono,
                "phase": "SCHEDULED", "current_symbol": "",
                "worker_name": "unified-institutional-watchlist",
            })
            MEMORY["_unified_watchlist_refresh_ok"] = False
            MEMORY["_unified_watchlist_refresh_status"] = "IN_PROGRESS"

    if restart_context is not None:
        log_execution(
            f"[WATCHDOG] Unified Watchlist worker stopped progressing: {restart_context}; "
            "entries remain blocked and verified process recovery is requested.",
            "CRITICAL", debounce_key="unified_watchlist_worker_stalled", debounce_sec=60,
        )
        _restart_main_loop_process_after_stall(restart_context)
        return MEMORY.setdefault("watchlist", {})

    try:
        worker = threading.Thread(
            target=_run_unified_institutional_watchlist_refresh,
            args=(generation,),
            name="unified-institutional-watchlist", daemon=True,
        )
        worker.start()
    except Exception as exc:
        with state["lock"]:
            state["in_progress"] = False
            state["worker_name"] = ""
            state["phase"] = "START_FAILED"
            state["last_error"] = f"worker_start_failed:{type(exc).__name__}"
            state["last_completed_monotonic"] = time.monotonic()
        MEMORY["_unified_watchlist_refresh_ok"] = False
        MEMORY["_unified_watchlist_refresh_status"] = "FAILED"
        log_execution(f"[INTEGRATION] watchlist worker start failed: {type(exc).__name__}: {exc}", "ERROR")
    return MEMORY.setdefault("watchlist", {})

def record_watchlist_entry(symbol, side, narrative, score, smart_money=None, momentum=None, source="ggp"):
    key = roro_canonical_symbol(symbol)
    entry = MEMORY.setdefault("watchlist", {}).setdefault(key, {})
    entry.update({"symbol":key, "side":side, "score":float(score or 0), "source":source,
                  "reasons": [k for k,v in (narrative or {}).items() if v is True],
                  "last_update":time.time(), "data_quality":"PENDING_INSTITUTIONAL_ANALYSIS"})
    return entry

def cleanup_watchlist(ttl=900):
    now=time.time(); wl=MEMORY.setdefault("watchlist", {})
    # Keep active WAITING/ENTRY_READY setups until their own evidence expires.
    # Snapshot before iteration: scanner threads may add/remove watchlist keys.
    for key, value in wl.copy().items():
        age=now-float(value.get("last_update", value.get("last_updated", now)) or now)
        active_cross_event = bool(
            value.get("cross_event_id")
            and str(value.get("cross_event_status", "ACTIVE")).upper() == "ACTIVE"
        )
        if (age>ttl and value.get("decision_state") not in
                ("ENTRY_READY","WAITING_FOR_RETEST","WAITING_FOR_CONFIRMATION")
                and not active_cross_event):
            wl.pop(key,None)
    if INSTITUTIONAL_RESOURCE_MANAGER:
        INSTITUTIONAL_RESOURCE_MANAGER.cleanup(preserve_keys=())

def check_institutional_entry(symbol, side, df, ob, atr, price):
    try:
        result=roro_analyze_symbol(roro_canonical_symbol(symbol), side, df)
        result.update(_enrich_with_existing_gpp_engines(df, side, result))
        MEMORY.setdefault("watchlist", {})[roro_canonical_symbol(symbol)] = {**MEMORY.get("watchlist",{}).get(roro_canonical_symbol(symbol),{}), **result}
        ready=result.get("decision_state")=="ENTRY_READY"
        return ready, result.get("setup_type"), result.get("decision_reason") or " | ".join(result.get("rejection_reasons",[]))
    except Exception as exc:
        return False, None, f"institutional_analysis_error: {exc}"

_INSTITUTIONAL_ENTRY_CONTEXT = None
RECOVERY_BLOCK_ENTRIES = False


def smart_opportunity_selection():
    """Unified Watchlist -> institutional engines -> one existing GGP execution path.

    This function is dispatched as a single-flight scanner task; it must never
    run on MainThread because candidate revalidation performs market-data reads.
    """
    global _INSTITUTIONAL_ENTRY_CONTEXT
    _scanner_progress("institutional_entry_revalidation", "candidate_cycle_start")
    # Hard exposure/recovery gates run before any balance or market-data read so
    # an unresolved exchange state can never trigger an unnecessary API call.
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        return False
    if globals().get("RECOVERY_BLOCK_ENTRIES", False) or MEMORY.get("position_status") == "UNKNOWN":
        log_execution("[ENTRY_GUARD] Exchange recovery is unresolved; new entries blocked", "WARN")
        return False
    if daily_loss_cooldown_active():
        return False
    refresh_unified_institutional_watchlist()
    # An asynchronous refresh is neither success nor failure. Defer this entry
    # cycle cleanly; the next cycle may consume the newly published snapshot.
    watchlist_refresh_status = str(
        MEMORY.get("_unified_watchlist_refresh_status") or ""
    ).upper()
    if watchlist_refresh_status == "IN_PROGRESS":
        log_execution(
            "[ENTRY_GUARD] unified Watchlist refresh in progress; candidate validation deferred",
            "DEBUG", debounce_key="watchlist_refresh_in_progress", debounce_sec=10,
        )
        return False
    # Never authorize a new entry from a stale Watchlist after a completed failure.
    # Existing position management runs in its independent lifecycle and is unaffected.
    if (watchlist_refresh_status == "FAILED"
            or MEMORY.get("_unified_watchlist_refresh_ok") is False):
        log_execution(
            "[ENTRY_GUARD] unified Watchlist refresh failed; stale candidates cannot authorize entries",
            "WARN", debounce_key="watchlist_refresh_failed", debounce_sec=10,
        )
        return False

    watchlist = MEMORY.get("watchlist", {})
    ready = [(sym, item) for sym, item in watchlist.items()
             if isinstance(item, dict) and item.get("decision_state") == "ENTRY_READY"]
    def _location_first_rank(pair):
        _, candidate = pair
        side_value = str(candidate.get("side") or "").upper()
        price_value = safe_float(candidate.get("price"), 0.0)
        market_intel = candidate.get("market_intelligence") if isinstance(candidate.get("market_intelligence"), dict) else {}
        atr_value = safe_float(candidate.get("atr"), 0.0)
        if atr_value <= 0 and price_value > 0:
            # The bridge exposes ATR as a percentage in its internal intelligence
            # snapshot; convert it only for ranking, never for entry authorization.
            atr_pct = safe_float(market_intel.get("volatility_atr"), 0.0)
            atr_value = price_value * atr_pct / 100.0 if atr_pct > 0 else 0.0
        distances = []
        ob = candidate.get("order_block")
        if isinstance(ob, dict) and ob.get("valid", True):
            try:
                distance = float(ob.get("distance_atr"))
                if math.isfinite(distance) and distance >= 0 and float(ob.get("strength", 0) or 0) >= 5.0:
                    distances.append(distance)
            except (TypeError, ValueError):
                pass
        fvg = candidate.get("fvg")
        if isinstance(fvg, dict):
            try:
                expected = "BULLISH" if side_value == "BUY" else "BEARISH"
                low, high = float(fvg.get("lower")), float(fvg.get("upper"))
                if (str(fvg.get("direction") or "").upper() == expected
                        and str(fvg.get("status") or "").upper() in ("UNFILLED", "PARTIALLY_FILLED")
                        and float(fvg.get("size_atr", 0) or 0) >= 0.08
                        and atr_value > 0 and price_value > 0 and 0 < low < high):
                    distances.append(max(low - price_value, 0.0, price_value - high) / atr_value)
            except (TypeError, ValueError):
                pass
        sr = candidate.get("support_resistance")
        if isinstance(sr, dict):
            key = "supports" if side_value == "BUY" else "resistances"
            for zone in sr.get(key, []):
                if not isinstance(zone, dict) or str(zone.get("status") or "").upper() != "CONFIRMED":
                    continue
                try:
                    distance = float(zone.get("distance_atr"))
                    if math.isfinite(distance) and distance >= 0 and float(zone.get("strength", 0) or 0) >= 5.0:
                        distances.append(distance)
                except (TypeError, ValueError):
                    continue
        golden = candidate.get("golden_zone") if isinstance(candidate.get("golden_zone"), dict) else {}
        location_distance = min(distances) if distances else 999.0
        location_quality = float(golden.get("score", 0) or 0)
        score = float(candidate.get("score", candidate.get("discovery_score", 0)) or 0)
        # Location proximity and quality lead; score is only the final tie-breaker.
        return (location_distance <= 1.5, -location_distance, location_quality, score)

    ready.sort(key=_location_first_rank, reverse=True)
    # Bound entry revalidation work per loop; the rotating Watchlist analyzes the rest.
    for canonical, item in ready[:8]:
        try:
            if STATE.get("open") or TRADE_STATE.get("in_position"):
                _scanner_progress("institutional_entry_revalidation", "yielded_to_open_position",
                                  symbol=str(canonical))
                return True
            if globals().get("RECOVERY_BLOCK_ENTRIES", False) or MEMORY.get("position_status") == "UNKNOWN":
                _scanner_progress("institutional_entry_revalidation", "blocked_by_recovery",
                                  symbol=str(canonical))
                return False
            _scanner_progress("institutional_entry_revalidation", "candidate_start",
                              symbol=str(canonical))
            side = str(item.get("side", "")).upper()
            if side not in ("BUY", "SELL"):
                continue
            exchange_sym = item.get("exchange_symbol") or roro_exchange_symbol(canonical)
            df = _integration_fetch_frame(exchange_sym)
            _scanner_progress("institutional_entry_revalidation", "candidate_ohlcv_complete",
                              symbol=str(exchange_sym), available=bool(df is not None))
            if df is None:
                item["decision_state"] = "WAITING_FOR_CONFIRMATION"
                item["rejection_reason"] = "market_data_unavailable"
                continue
            closed_df = roro_validate_closed_frame(
                df, now_ms=int(time.time() * 1000), timeframe_s=_roro_timeframe_seconds(INTERVAL)
            )
            if len(closed_df) < 80 or not validate_dataframe(closed_df, 80):
                item["decision_state"] = "WAITING_FOR_CONFIRMATION"
                item["rejection_reason"] = "insufficient_fresh_closed_ohlcv"
                continue
            # Re-evaluate BUY and SELL independently immediately before authorization.
            # RF/V2 direction is only a tie-break hint and can never suppress the other side.
            fresh = roro_analyze_symbol_both(
                exchange_sym, closed_df,
                now_ms=int(time.time() * 1000),
                side_hints=item.get("side_hints") or [side],
            )
            fresh.update(_enrich_with_existing_gpp_engines(closed_df, fresh.get("side"), fresh))
            _scanner_progress("institutional_entry_revalidation", "candidate_analysis_complete",
                              symbol=str(exchange_sym),
                              decision=str(fresh.get("decision_state") or "UNKNOWN"))
            item.update(fresh)
            item["source"] = item.get("source") or "unified_watchlist"
            if item.get("decision_state") != "ENTRY_READY":
                continue
            side = str(item.get("side", "")).upper()
            if side not in ("BUY", "SELL"):
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "no_unambiguous_authorized_side"
                continue
            institutional_report = item.get("institutional_report")
            if not isinstance(institutional_report, dict) or not institutional_report:
                report_row = dict(item)
                report_row.setdefault("data_freshness", "FRESH")
                report_row.setdefault("data_quality", "VALID_CLOSED_CANDLES")
                report_row.setdefault("closed_candle_source", "UNIFIED_WATCHLIST_AUTHORIZED_FRAME")
                report_row.setdefault("last_updated", float(int(closed_df["timestamp"].iloc[-1]) / 1000.0))
                report_row.setdefault("timeframe_ms", _roro_timeframe_seconds(INTERVAL) * 1000.0)
                institutional_report = build_institutional_report(report_row, side=side, now_ts=time.time())
            allowed, location_gate, blocking_reason = _entry_location_permission(
                institutional_report, strict_freshness=not PAPER_MODE and MODE_LIVE,
            )
            item["institutional_report"] = institutional_report
            item["institutional_location_gate"] = location_gate
            if not allowed:
                reason = blocking_reason or "strong_opposing_zone_requires_wait"
                item["decision_state"] = "NO_CHASE"
                item["rejection_reason"] = reason
                MEMORY.setdefault("no_entry_feed", []).append({
                    "time": time.time(), "symbol": exchange_sym, "side": side,
                    "reason": reason, "score": float(item.get("score", 0) or 0),
                })
                MEMORY["no_entry_feed"] = MEMORY["no_entry_feed"][-20:]
                log_execution(
                    f"[INSTITUTIONAL_LOCATION] {exchange_sym} {side} held: {reason}",
                    "WARN", debounce_key=f"institutional_location:{exchange_sym}:{side}:{reason}", debounce_sec=10,
                )
                continue
            event_id = str(item.get("event_id") or "")
            if not event_id:
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "missing_event_id"
                continue
            if INSTITUTIONAL_TRADE_JOURNAL and INSTITUTIONAL_TRADE_JOURNAL.is_event_consumed(event_id):
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "duplicate_event_persisted"
                continue
            cross_event_id = str(item.get("cross_event_id") or "")
            if cross_event_id and not _cross_event_is_usable(cross_event_id, side):
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "confirmed_cross_event_not_breakout_confirmed"
                continue
            atr_value = compute_atr(closed_df).iloc[-1]
            if not np.isfinite(atr_value) or atr_value <= 0:
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "invalid_atr"
                continue
            live_price = get_ticker_safe(exchange_sym)
            price = float(live_price if live_price and live_price > 0 else closed_df["close"].iloc[-1])

            # Final no-chase check uses the latest execution reference price.
            # The exact same authority used by RORO analysis validates the zone again.
            latest_closed_ts = int(closed_df["timestamp"].iloc[-1])
            timeframe_ms = int(_roro_timeframe_seconds(INTERVAL) * 1000)
            location_candidates = []
            ob_for_location = item.get("order_block")
            if isinstance(ob_for_location, dict):
                ob_zone = dict(ob_for_location)
                ob_zone.update({
                    "kind": "ORDER_BLOCK",
                    "available_timestamp": ob_zone.get("impulse_timestamp", ob_zone.get("timestamp")),
                })
                location_candidates.append(ob_zone)
            fvg_for_location = item.get("fvg")
            if isinstance(fvg_for_location, dict):
                fvg_zone = dict(fvg_for_location)
                fvg_zone.update({"kind": "FVG", "low": fvg_zone.get("lower"),
                                 "high": fvg_zone.get("upper"),
                                 "available_timestamp": fvg_zone.get("timestamp")})
                location_candidates.append(fvg_zone)
            sr_for_location = item.get("support_resistance")
            if isinstance(sr_for_location, dict):
                sr_key = "supports" if side == "BUY" else "resistances"
                for sr_zone in sr_for_location.get(sr_key, []):
                    if isinstance(sr_zone, dict):
                        sr_candidate = dict(sr_zone)
                        sr_candidate.update({
                            "kind": "SUPPORT" if side == "BUY" else "RESISTANCE",
                            "available_timestamp": sr_candidate.get("confirmed_at_timestamp"),
                        })
                        location_candidates.append(sr_candidate)
            location_checks = [
                validate_location_zone(
                    zone, side, price, float(atr_value), latest_closed_ts, timeframe_ms,
                    max_age_bars=48, max_distance_atr=1.5,
                )
                for zone in location_candidates
            ]
            actionable_locations = [check for check in location_checks if check.get("valid")]
            if not actionable_locations:
                item["decision_state"] = "NO_CHASE"
                item["rejection_reason"] = "ENTRY_WINDOW_MISSED_EXECUTION_PRICE_OUTSIDE_INSTITUTIONAL_ZONE"
                log_execution(
                    f"[LOCATION_FIRST] {exchange_sym} {side} blocked: no actionable zone within 1.5 ATR at execution price",
                    "WARN",
                )
                continue

            setup = str(item.get("decision_setup_type") or item.get("setup_type", "")).upper()
            if not ("REVERSAL" in setup or "CONTINUATION" in setup):
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "unsupported_setup_type"
                continue
            classification = "REVERSAL" if "REVERSAL" in setup else "TREND"
            sl = item.get("structural_stop")
            if sl is None or not np.isfinite(float(sl)) or float(sl) <= 0:
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "structural_stop_missing"
                continue
            _, tp1, tp2 = compute_sl_tp(
                price, side, classification, float(atr_value), closed_df, symbol=exchange_sym
            )
            if not (tp1 and tp2 and np.isfinite(float(tp1)) and np.isfinite(float(tp2))):
                item["decision_state"] = "REJECTED"
                item["rejection_reason"] = "profit_targets_invalid"
                continue
            evidence = list(item.get("evidence") or [])
            market_intelligence = item.get("market_intelligence") if isinstance(item.get("market_intelligence"), dict) else {}

            _INSTITUTIONAL_ENTRY_CONTEXT = {
                "setup_type": setup,
                "entry_zone": dict(item.get("entry_zone")) if isinstance(item.get("entry_zone"), dict) else None,
                "entry_zone_status": str(item.get("entry_zone_status") or "NOT_SELECTED"),
                "entry_zone_direction": str(item.get("entry_zone_direction") or "UNKNOWN"),
                "event_id": item.get("event_id"),
                "evidence": evidence,
                "structural_stop": float(sl),
                "trading_mode": str(item.get("trading_mode") or market_intelligence.get("mode") or "UNCLASSIFIED"),
                "entry_behavior": str(item.get("entry_behavior") or market_intelligence.get("entry_behavior") or "WAIT_FOR_STRUCTURE"),
                "entry_zone_low": safe_float(item.get("entry_zone_low", market_intelligence.get("reference_low")), 0.0),
                "entry_zone_high": safe_float(item.get("entry_zone_high", market_intelligence.get("reference_high")), 0.0),
                "support_resistance": dict(item.get("support_resistance") or {"status": "UNKNOWN", "supports": [], "resistances": []}),
                "order_block": dict(item.get("order_block") or {}),
                "fvg": dict(item.get("fvg") or {}),
                "liquidity": dict(item.get("liquidity") or {}),
                "golden_zone": dict(item.get("golden_zone") or {}),
                "entry_behavior_confidence": safe_float(item.get("entry_behavior_confidence", market_intelligence.get("confidence")), 0.0),
                "entry_behavior_reasons": list(item.get("entry_behavior_reasons") or market_intelligence.get("reason_codes") or []),
                "market_intelligence": dict(market_intelligence),
                "zone_quality": item.get("zone_quality"),
                "zone_type": item.get("zone_type"),
                "market_structure": dict(item.get("market_structure") or {}),
                "institutional_report": dict(institutional_report),
                "institutional_location_gate": dict(location_gate),
                "volume_ratio": item.get("volume_ratio"),
                "adx": item.get("adx"),
                "adx_di": dict(item.get("adx_di") or {}),
                "ema9": item.get("ema9"),
                "ema50": item.get("ema50"),
                "vwap": item.get("vwap"),
                "market_regime": item.get("market_regime"),
                "unified_decision_context": (item.get("unified_decision_context") or item.get("decision_context")
                                             if isinstance(item.get("unified_decision_context"), dict)
                                             or isinstance(item.get("decision_context"), dict) else None),
                "cross_event_id": str(item.get("cross_event_id") or ""),
                "cross_event_source": item.get("cross_event_source"),
                "cross_event_direction": item.get("cross_event_direction"),
                "cross_event_timestamp": item.get("cross_event_timestamp"),
                "cross_event_phase": item.get("cross_event_phase"),
                "cross_event_lifecycle_schema": item.get("cross_event_lifecycle_schema"),
                "cross_event_entry_ready": item.get("cross_event_entry_ready"),
                "cross_event_rejection_high": item.get("cross_event_rejection_high"),
                "cross_event_rejection_low": item.get("cross_event_rejection_low"),
                "cross_event_rejection_timestamp": item.get("cross_event_rejection_timestamp"),
                "cross_event_breakout_timestamp": item.get("cross_event_breakout_timestamp"),
                "cross_event_breakout_close": item.get("cross_event_breakout_close"),
                "cross_event_chase_atr": item.get("cross_event_chase_atr"),
                "pullback_type": next((x.split(":", 1)[0] for x in evidence
                                        if any(t in x.lower() for t in ("pullback:", "retest:", "micro_pullback:"))), None),
                "exchange_symbol": exchange_sym,
                "candle_timeframe": str(INTERVAL),
                "closed_candle_frame": closed_df.copy(),
                "closed_candle_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
                "closed_candle_timestamp": int(closed_df["timestamp"].iloc[-1]),
                "data_freshness": "FRESH",
                "data_quality": "VALID_CLOSED_CANDLES",
                "last_updated": float(int(closed_df["timestamp"].iloc[-1]) / 1000.0),
                "timeframe_ms": timeframe_ms,
            }
            reason = "UNIFIED_INSTITUTIONAL_ENTRY | " + " | ".join(evidence)
            try:
                is_strong = is_strong_candidate(item)
                sizing_class = "SNIPER" if is_strong else "TREND"
                item["capital_allocation_tier"] = "STRONG_30_PERCENT" if is_strong else "STANDARD_20_PERCENT"
                _INSTITUTIONAL_ENTRY_CONTEXT["capital_allocation_tier"] = item["capital_allocation_tier"]
                _INSTITUTIONAL_ENTRY_CONTEXT["sizing_eligible_strong"] = bool(is_strong)
                _scanner_progress("institutional_entry_revalidation", "entry_authorization",
                                  symbol=str(exchange_sym), side=side)
                ok = execute_entry(side, exchange_sym, price, float(sl), float(tp1), float(tp2),
                                   float(item.get("score", 0) or 0), reason, float(atr_value),
                                   trade_type=classification, entry_type="RORO_INSTITUTIONAL",
                                   classification=sizing_class)
                _scanner_progress("institutional_entry_revalidation", "entry_execution_complete",
                                  symbol=str(exchange_sym), executed=bool(ok))
            finally:
                _INSTITUTIONAL_ENTRY_CONTEXT = None
            if ok:
                item["entry_status"] = "EXECUTED_VIA_GGP_EXECUTE_ENTRY"
                item["entry_timestamp"] = time.time()
                _persist_trade_journal_start(setup, item)
                trade_id = str(STATE.get("trade_id") or "")
                if INSTITUTIONAL_TRADE_JOURNAL and trade_id:
                    INSTITUTIONAL_TRADE_JOURNAL.consume_event(event_id, trade_id)
                RORO_ENTRY_AUTHORITY.consume(event_id)
                log_execution(f"[INSTITUTIONAL_ENTRY] {setup} {side} {exchange_sym} event={event_id}", "SUCCESS")
                return True
        except Exception as exc:
            _INSTITUTIONAL_ENTRY_CONTEXT = None
            log_execution(f"[INSTITUTIONAL_ENTRY] candidate {canonical} failed: {type(exc).__name__}: {exc}", "ERROR")
    return False

# Explicit alias for auditability; old radar paths remain available but feed the unified store.
unified_institutional_watchlist = refresh_unified_institutional_watchlist

# Scanner workers are bounded by name: at most one execution of each scanner can run at a time.
# They are scheduled workers (not continuous engines): IDLE is a real state with
# an explicit next_due deadline, and RUNNING requires real progress beats.
_SCANNER_TASKS_LOCK = threading.RLock()
_SCANNER_TASK_LOCKS = {}
_SCANNER_TASK_STATUS = {}
_SCANNER_TASK_THREADS = {}
_SCANNER_TASK_CYCLES = {}


def _scanner_progress(name, phase, **extra):
    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return
    details = {
        "phase": phase,
        "cycle_id": _SCANNER_TASK_CYCLES.get(name, 0),
        "thread_name": f"ggp_{name}",
    }
    details.update(extra)
    watchdog.beat(f"scanner:{name}", state="RUNNING", **details)


def _scanner_cycle_active(name):
    thread = _SCANNER_TASK_THREADS.get(name)
    return bool(thread is not None and thread.is_alive())


def _finish_scanner_task(name, state, *, reason="", error=None):
    final_state = state if state in {"IDLE", "BLOCKED", "FAILED"} else "FAILED"
    now = time.time()
    previous = _SCANNER_TASK_STATUS.get(name) or {}
    status = {
        "state": final_state,
        "cycle_id": previous.get("cycle_id", 0),
        "last_attempt": previous.get("last_attempt", now),
        "last_success": previous.get("last_success"),
        "last_completed": now,
        "next_due": None,
        "reason": str(reason)[:160],
        "last_error": str(error)[:240] if error else None,
    }
    if final_state == "IDLE":
        status["last_success"] = now
        status["next_due"] = now + float(GLOBAL_SCAN_INTERVAL)
    else:
        status["next_due"] = now + min(60.0, float(GLOBAL_SCAN_INTERVAL))
    _SCANNER_TASK_STATUS[name] = status
    MEMORY.setdefault("scanner_tasks", {})[name] = dict(status)

    watchdog = globals().get("NAISO_WATCHDOG")
    if watchdog is None:
        return
    details = {"reason": status["reason"],
               "phase": "cycle_complete" if final_state == "IDLE" else final_state}
    if final_state == "IDLE":
        details["idle_until"] = watchdog.monotonic() + float(GLOBAL_SCAN_INTERVAL)
    elif final_state == "BLOCKED":
        details["blocked_until"] = watchdog.monotonic() + min(60.0, float(GLOBAL_SCAN_INTERVAL))
    watchdog.beat(f"scanner:{name}", state=final_state, **details)


def dispatch_scanner_task(name, function, *args, **kwargs):
    with _SCANNER_TASKS_LOCK:
        lock = _SCANNER_TASK_LOCKS.setdefault(name, threading.Lock())
        if not lock.acquire(blocking=False):
            return False
        previous = _SCANNER_TASK_STATUS.get(name) or {}
        cycle_id = int(previous.get("cycle_id", 0)) + 1
        _SCANNER_TASK_CYCLES[name] = cycle_id
        now = time.time()
        _SCANNER_TASK_STATUS[name] = {
            "state": "RUNNING", "cycle_id": cycle_id, "last_attempt": now,
            "last_success": previous.get("last_success"), "next_due": None,
            "started_at": now, "last_error": None, "reason": "",
        }
        MEMORY.setdefault("scanner_tasks", {})[name] = dict(_SCANNER_TASK_STATUS[name])

    def runner():
        finished = False
        try:
            _scanner_progress(name, "started", cycle_id=cycle_id)
            if STATE.get("open") or TRADE_STATE.get("in_position"):
                _finish_scanner_task(name, "BLOCKED", reason="OPEN_POSITION")
                finished = True
                return
            function(*args, **kwargs)
            _finish_scanner_task(name, "IDLE", reason="CYCLE_COMPLETE")
            finished = True
        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)[:180]}"
            log_execution(f"[SCANNER_TASK] {name} failed: {error}", "ERROR")
            _finish_scanner_task(name, "FAILED", reason="EXCEPTION", error=error)
            finished = True
        finally:
            try:
                if not finished:
                    _finish_scanner_task(name, "FAILED", reason="UNEXPECTED_EXIT",
                                         error="worker exited without a result")
            finally:
                lock.release()

    watchdog = globals().get("NAISO_WATCHDOG")
    watchdog_name = f"scanner:{name}"
    started = False
    try:
        if watchdog is not None:
            watchdog.register(
                watchdog_name, interval_sec=float(GLOBAL_SCAN_INTERVAL),
                grace_sec=max(180.0, float(GLOBAL_SCAN_INTERVAL) * 0.25),
                critical=False, restartable=True,
                recovery_verifier=_verify_scanner_recovery,
                restart_fn=_resume_scanner_after_flat_verification)
            _scanner_progress(name, "dispatch", cycle_id=cycle_id)
        thread = threading.Thread(target=runner, daemon=True, name=f"ggp_{name}")
        _SCANNER_TASK_THREADS[name] = thread
        thread.start()
        started = True
        return True
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:180]}"
        _finish_scanner_task(name, "FAILED", reason="START_FAILED", error=error)
        if not started:
            try:
                lock.release()
            except RuntimeError:
                pass
        log_execution(f"[SCANNER_TASK] {name} could not start: {error}", "ERROR")
        return False


def _run_global_discovery_scanner_with_heartbeat():
    """Single-flight scheduled discovery cycle with real in-cycle progress beats.

    A long scan can legitimately run past the watchdog grace window. The
    heartbeat below reports phase progress from the worker itself; it does not
    inflate a timeout or pretend an idle worker is running.
    """
    stop_heartbeat = threading.Event()

    def _progress_heartbeat():
        while not stop_heartbeat.wait(30.0):
            _scanner_progress("global_discovery", "in_cycle", heartbeat=True)

    _scanner_progress("global_discovery", "cycle_enter")
    heartbeat = threading.Thread(target=_progress_heartbeat, daemon=True,
                                 name="ggp_global_discovery_heartbeat")
    try:
        heartbeat.start()
    except Exception as exc:
        stop_heartbeat.set()
        raise RuntimeError(f"DISCOVERY_HEARTBEAT_START_FAILED:{type(exc).__name__}: {exc}") from exc
    try:
        run_global_discovery_scanner()
    finally:
        stop_heartbeat.set()
        heartbeat.join(timeout=1.0)


def run_global_discovery_scanner():
    """Scanner 1: bounded global discovery; RF, V2 and professional scan share one OHLCV cache.

    All scanner outputs are discovery hints only. The institutional bridge evaluates
    BUY and SELL independently and the unified authority alone can authorize entry.
    """
    started = time.time()
    meta = MEMORY.setdefault("discovery_scanner", {})
    meta.update({"state": "RUNNING", "last_started": started, "symbols_scanned": 0,
                 "universe_limit": SCANNER_UNIVERSE_MAX})
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        meta.update({"state": "BLOCKED", "reason": "OPEN_POSITION", "last_completed": time.time()})
        _scanner_progress("global_discovery", "blocked_open_position", state="BLOCKED",
                          reason="OPEN_POSITION")
        return
    blocked = _scanner_recovery_blocked() if "_scanner_recovery_blocked" in globals() else ""
    if blocked:
        meta.update({"state": "BLOCKED", "reason": blocked, "last_completed": time.time()})
        _scanner_progress("global_discovery", "blocked_recovery", state="BLOCKED", reason=blocked)
        return
    _scanner_progress("global_discovery", "loading_market_universe")
    symbols = get_usdt_perp_symbols()[:SCANNER_UNIVERSE_MAX]
    with _SCANNER_FRAME_CACHE_LOCK:
        _SCANNER_FRAME_CACHE.clear()
    if not symbols:
        meta.update({"state": "BLOCKED", "reason": "NO_SYMBOLS", "last_completed": time.time()})
        _scanner_progress("global_discovery", "blocked_no_symbols", state="BLOCKED",
                          reason="NO_SYMBOLS")
        return

    # V2 first fetches market data into the bounded per-symbol cache.
    _scanner_progress("global_discovery", "scanner_v2", symbols=len(symbols))
    run_scanner_v2(symbols=symbols)
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        meta.update({"state": "BLOCKED", "reason": "YIELDED_TO_POSITION",
                     "last_completed": time.time()})
        _scanner_progress("global_discovery", "yielded_to_position", state="BLOCKED",
                          reason="YIELDED_TO_POSITION")
        return

    timeframe_seconds = _roro_timeframe_seconds(INTERVAL)
    _scanner_progress("global_discovery", "professional_scan", symbols=len(symbols))
    pro_started = time.time()
    pro_candidates, pro_stats = _professional_lightweight_scan(
        symbols=symbols,
        fetch_frame=lambda symbol: _get_scanner_frame(symbol, 150),
        timeframe_seconds=timeframe_seconds,
        top_n=min(40, SCANNER_UNIVERSE_MAX),
        min_score=4.5,
        max_symbols=SCANNER_UNIVERSE_MAX,
        should_stop=lambda: bool(STATE.get("open") or TRADE_STATE.get("in_position")),
    )
    MEMORY["professional_scan_candidates"] = [
        {**row, "score": round(float(row.get("score", 0) or 0) * 10.0, 2),
         "side": row.get("side_hint", "NEUTRAL"), "source": "professional_lightweight_scan",
         "side_hint_is_soft": True}
        for row in pro_candidates
    ]
    MEMORY["professional_scanner"] = {
        "state": "IDLE", "last_started": pro_started, "last_completed": time.time(),
        "symbols_scanned": int(pro_stats.get("total", 0)),
        "valid_data": int(pro_stats.get("valid_data", 0)),
        "selected": len(pro_candidates), "stats": pro_stats,
        "duration_seconds": round(time.time() - pro_started, 2),
        "role": "DISCOVERY_ONLY_BOTH_SIDES_REANALYZED",
    }
    if STATE.get("open") or TRADE_STATE.get("in_position"):
        meta.update({"state": "BLOCKED", "reason": "YIELDED_TO_POSITION",
                     "last_completed": time.time(), "professional_candidates": len(pro_candidates)})
        _scanner_progress("global_discovery", "yielded_to_position", state="BLOCKED",
                          reason="YIELDED_TO_POSITION")
        return

    _scanner_progress("global_discovery", "rf_scan", symbols=len(symbols))
    rf_candidates = scan_market_rf(top_n=40, symbols=symbols)
    MEMORY["rf_watchlist"] = rf_candidates[:30]
    MEMORY["last_scan"] = time.time()
    MEMORY["scanned_count"] = len(symbols)
    MEMORY["rf_dashboard"] = [{
        "symbol": item["symbol"], "status": item.get("status", "PROXIMITY"),
        "icon": "🔔" if item.get("rf_triggered") else "📡", "score": item.get("score", 0),
        "adx": item.get("adx", 0), "rsi": item.get("rsi", 0), "atrp": item.get("atrp", 0),
        "signal": item.get("rf_signal") or "N/A",
        "role": "DISCOVERY_HINT_ONLY",
    } for item in rf_candidates[:30]]

    # Radar is intentionally applied only to the highest-ranked distinct names.
    radar_candidates = []
    top_symbols = []
    for item in (MEMORY.get("scanner_v2_buy", [])[:10] + MEMORY.get("scanner_v2_sell", [])[:10]
                 + pro_candidates[:20] + rf_candidates[:20]):
        symbol = item.get("symbol")
        if symbol and symbol not in top_symbols:
            top_symbols.append(symbol)
    for symbol in top_symbols[:30]:
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            break
        frame = _get_scanner_frame(symbol, 150)
        if frame is None or not validate_dataframe(frame, 80):
            continue
        try:
            score = radar_score(frame)
            if score > 0:
                radar_candidates.append({"symbol": symbol, "score": float(score), "source": "discovery_radar"})
        except Exception:
            continue
    radar_candidates.sort(key=lambda item: item["score"], reverse=True)
    MEMORY["radar_watchlist"] = radar_candidates[:30]
    MEMORY["radar_top5"] = radar_candidates[:5]

    # Present a deduplicated blended discovery panel; do not replace the independent RF panel.
    blended = []
    seen = set()
    for source_name, source_rows in (
        ("professional", MEMORY.get("professional_scan_candidates", [])),
        ("scanner_v2_buy", MEMORY.get("scanner_v2_buy", [])),
        ("scanner_v2_sell", MEMORY.get("scanner_v2_sell", [])),
        ("radar", radar_candidates),
        ("rf", rf_candidates),
    ):
        for row in source_rows or []:
            symbol = row.get("symbol")
            key = roro_canonical_symbol(symbol)
            if not key:
                continue
            score = float(row.get("score", 0) or 0)
            if key not in seen:
                blended.append({**row, "symbol": symbol, "canonical_symbol": key,
                                "discovery_source": source_name, "discovery_sources": [source_name]})
                seen.add(key)
            else:
                target = next((x for x in blended if x.get("canonical_symbol") == key), None)
                if target is not None:
                    if source_name not in target["discovery_sources"]:
                        target["discovery_sources"].append(source_name)
                    if score > float(target.get("score", 0) or 0):
                        target["score"] = score
    blended.sort(key=lambda item: float(item.get("score", 0) or 0), reverse=True)
    MEMORY["top_candidates"] = blended[:60]

    # Confirmed crosses are derived only from the strict closed-candle gate over
    # the USDT swap universe, never from any of the supplementary scanner rows.
    cross_detected_at = time.time()
    new_cross_events = _detect_confirmed_cross_events(symbols)
    cross_delivery = {
        "new_events": int(new_cross_events or 0),
        "detected_at": cross_detected_at,
        "delivered_at": cross_detected_at,
        "same_cycle": False,
        "delivered": False,
        "lifecycle_marked": [],
        "lifecycle_status": CROSS_WATCHLIST_LIFECYCLE,
        "reason": "NO_NEW_CROSS_EVENT",
        "watchlist_refresh_observed": False,
    }
    cross_delivery["event_ids"] = [
        str(row.get("cross_event_id") or "")
        for row in MEMORY.get("confirmed_cross_events", [])
        if row.get("cross_event_id")
    ]
    if cross_delivery["new_events"] > 0:
        if STATE.get("open") or TRADE_STATE.get("in_position"):
            cross_delivery["reason"] = "POSITION_OPEN_DISCOVERY_PAUSED"
        else:
            # The Watchlist refresh is asynchronous. Publish PENDING before
            # scheduling so the worker can mark delivery only after the event
            # row is actually committed to the authoritative Watchlist.
            cross_delivery["reason"] = "WATCHLIST_REFRESH_SCHEDULED"
            cross_delivery["same_cycle"] = False
            cross_delivery["delivered"] = False
            cross_delivery["delivered_at"] = None
            cross_delivery["watchlist_refresh_observed"] = True
            MEMORY["_cross_watchlist_delivery"] = dict(cross_delivery)
            refresh_unified_institutional_watchlist(force=True)
            completed_delivery = MEMORY.get("_cross_watchlist_delivery")
            if isinstance(completed_delivery, dict):
                cross_delivery = dict(completed_delivery)
                if bool(cross_delivery.get("delivered")):
                    # The worker finished inside this scanner call. A later
                    # asynchronous completion must never be relabelled as
                    # same-cycle just because a scanner reads the acknowledgement.
                    cross_delivery["same_cycle"] = True
            cross_delivery["refresh_seconds"] = round(time.time() - cross_detected_at, 3)
    cross_delivery["age_seconds"] = round(time.time() - cross_detected_at, 3)
    # The async Watchlist worker may finish between our read above and this
    # assignment. Preserve its newer delivery acknowledgement for the same IDs.
    existing_delivery = MEMORY.get("_cross_watchlist_delivery")
    if isinstance(existing_delivery, dict):
        existing_ids = set(existing_delivery.get("event_ids") or [])
        current_ids = set(cross_delivery.get("event_ids") or [])
        if (
            bool(cross_delivery.get("watchlist_refresh_observed"))
            and existing_ids == current_ids
            and (
                bool(existing_delivery.get("delivered"))
                or len(existing_delivery.get("delivered_ids") or []) >
                   len(cross_delivery.get("delivered_ids") or [])
            )
        ):
            # Keep the worker's newer delivery acknowledgement, but never let
            # it rewrite this scanner cycle's detection metadata. A duplicate
            # cycle must still report zero new events and its own reason.
            preserved = dict(existing_delivery)
            for key in ("new_events", "detected_at", "age_seconds", "event_ids"):
                preserved[key] = cross_delivery.get(key)
            if not int(cross_delivery.get("new_events") or 0):
                preserved["reason"] = cross_delivery.get("reason", "NO_NEW_CROSS_EVENT")
                preserved["same_cycle"] = False
            elif bool(cross_delivery.get("same_cycle")):
                preserved["same_cycle"] = True
            cross_delivery = preserved
    MEMORY["_cross_watchlist_delivery"] = cross_delivery

    meta.update({
        "state": "IDLE", "last_completed": time.time(), "symbols_scanned": len(symbols),
        "professional_candidates": len(pro_candidates), "professional_valid_data": pro_stats.get("valid_data", 0),
        "rf_candidates": len(rf_candidates),
        "buy_candidates": len(MEMORY.get("scanner_v2_buy", [])),
        "sell_candidates": len(MEMORY.get("scanner_v2_sell", [])),
        "radar_candidates": len(radar_candidates), "blended_candidates": len(blended),
        "confirmed_cross_events": len(MEMORY.get("confirmed_cross_events", [])),
        "confirmed_cross_events_new": int(new_cross_events),
        "cross_watchlist_delivery": dict(cross_delivery),
        "duration_seconds": round(time.time() - started, 2),
        "universe_limit": SCANNER_UNIVERSE_MAX,
        "directional_cross_required": False,
        "confirmed_cross_event_source": CONFIRMED_CROSS_EVENT_SOURCE,
    })
    log_execution(
        f"[DISCOVERY_SCANNER] complete symbols={len(symbols)} pro={len(pro_candidates)} "
        f"buy={meta['buy_candidates']} sell={meta['sell_candidates']} rf={len(rf_candidates)} "
        f"radar={len(radar_candidates)} blended={len(blended)} "
        f"cross_events={meta['confirmed_cross_events']} new_crosses={new_cross_events} "
        f"duration={meta['duration_seconds']}s",
        "INFO", debounce_key="discovery_scanner_complete", debounce_sec=60
    )



# ===== 24/7 TRADE JOURNAL + TRI-STATE EXCHANGE RECOVERY =====
LAST_POSITION_FETCH_STATUS = "UNKNOWN"
RECOVERY_BLOCK_ENTRIES = False
_journal_last_snapshot_ts = 0.0


def _position_contracts(pos):
    info = pos.get("info") if isinstance(pos, dict) else {}
    info = info if isinstance(info, dict) else {}
    raw = pos.get("contracts")
    if raw in (None, ""):
        raw = info.get("positionAmt", info.get("positionSize", info.get("availableAmt", 0)))
    try:
        return abs(float(raw or 0))
    except (TypeError, ValueError):
        return 0.0


def _position_symbol(pos):
    info = pos.get("info") if isinstance(pos, dict) else {}
    info = info if isinstance(info, dict) else {}
    return str(pos.get("symbol") or info.get("symbol") or "")


def fetch_position_tri_state(symbol=None, *, strict_account_snapshot=False):
    """Return OPEN/FLAT/UNKNOWN; API errors must never be interpreted as FLAT.

    ``strict_account_snapshot`` requires a complete authoritative account-wide
    BingX swap snapshot before an unsymbolized query may conclude FLAT. It is
    used by the main-loop sync and recovery paths; symbol-scoped callers keep
    the legacy shape for compatibility.
    """
    global LAST_POSITION_FETCH_STATUS
    if PAPER_MODE:
        status = "OPEN" if STATE.get("open") else "FLAT"
        LAST_POSITION_FETCH_STATUS = status
        return status, None
    try:
        if not hasattr(ex, "fetch_positions"):
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return "UNKNOWN", None
        if symbol:
            resolved, reason, diagnostics = resolve_position_symbol(symbol)
            if not resolved:
                LAST_POSITION_FETCH_STATUS = "UNKNOWN"
                log_execution(
                    f"[POS_SYNC] tri-state blocked by {reason}: "
                    f"original={diagnostics.get('original_symbol')!r} "
                    f"final={diagnostics.get('normalized_symbol')!r}",
                    "ERROR", debounce_key=f"tri_state_symbol_{reason}", debounce_sec=30)
                return "UNKNOWN", None
            result = safe_api_call(ex.fetch_positions, [resolved], _api_max_attempts=1)
        else:
            # Position state is safety-critical, but repeated synchronous retries
            # can pin MainThread beyond the watchdog grace window. Treat a failed
            # snapshot as UNKNOWN and let the next governed cycle retry once.
            result = safe_api_call(ex.fetch_positions, _api_max_attempts=1)
        if result is None or not isinstance(result, (list, tuple)):
            LAST_POSITION_FETCH_STATUS = "UNKNOWN"
            return "UNKNOWN", None
        if strict_account_snapshot and not symbol:
            from bingx_protection import validate_account_position_snapshot
            status, strict_rows, reason, snapshot_diag = validate_account_position_snapshot(ex, result)
            if status == "UNKNOWN":
                LAST_POSITION_FETCH_STATUS = "UNKNOWN"
                log_execution(
                    f"[POS_SYNC] account-wide snapshot not authoritative: {reason} "
                    f"rows={snapshot_diag.get('rows')} supported_markets={snapshot_diag.get('supported_markets')}",
                    "ERROR", debounce_key=f"tri_state_snapshot_{reason}", debounce_sec=30)
                return "UNKNOWN", None
            open_positions = strict_rows
        else:
            open_positions = [p for p in result if isinstance(p, dict) and _position_contracts(p) > 0]
        if symbol:
            target = roro_canonical_symbol(symbol)
            matches = [p for p in open_positions if roro_canonical_symbol(_position_symbol(p)) == target]
            if not matches:
                LAST_POSITION_FETCH_STATUS = "FLAT"
                return "FLAT", None
            if len(matches) > 1:
                # Multiple position sides for the same market require explicit hedge-mode handling.
                LAST_POSITION_FETCH_STATUS = "UNKNOWN"
                return "UNKNOWN", None
            LAST_POSITION_FETCH_STATUS = "OPEN"
            return "OPEN", matches[0]
        LAST_POSITION_FETCH_STATUS = "OPEN" if open_positions else "FLAT"
        return LAST_POSITION_FETCH_STATUS, open_positions
    except Exception as exc:
        LAST_POSITION_FETCH_STATUS = "UNKNOWN"
        log_execution(f"[POS_SYNC] tri-state fetch failed: {type(exc).__name__}: {exc}", "ERROR")
        return "UNKNOWN", None


def _snapshot_from_position(symbol, pos):
    snap = PositionSnapshot()
    info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
    raw_side = str(pos.get("side") or info.get("positionSide") or info.get("side") or "").lower()
    snap.side = "BUY" if raw_side in ("long", "buy") else "SELL" if raw_side in ("short", "sell") else ""
    if not snap.side:
        raise ValueError("exchange position side is unknown")
    snap.symbol = symbol
    snap.qty = _position_contracts(pos)
    snap.entry_price = safe_float(pos.get("entryPrice") or pos.get("average") or info.get("avgPrice"))
    snap.mark_price = safe_float(pos.get("markPrice") or info.get("markPrice"))
    snap.unrealized_pnl = safe_float(pos.get("unrealizedPnl") or info.get("unrealizedProfit"))
    snap.margin = safe_float(pos.get("initialMargin") or info.get("initialMargin"))
    snap.leverage = safe_float(pos.get("leverage") or info.get("leverage"), LEVERAGE)
    snap.liquidation_price = safe_float(pos.get("liquidationPrice") or info.get("liquidationPrice"))
    if snap.qty <= 0 or snap.entry_price <= 0:
        raise ValueError("exchange position lacks a valid quantity or entry price")
    if snap.margin > 0:
        snap.roe_pct = snap.unrealized_pnl / snap.margin * 100.0
    elif snap.mark_price > 0:
        move = ((snap.mark_price - snap.entry_price) / snap.entry_price * 100.0
                if snap.side == "BUY" else (snap.entry_price - snap.mark_price) / snap.entry_price * 100.0)
        snap.roe_pct = move * snap.leverage
    else:
        snap.roe_pct = 0.0
    snap.updated_at = time.time()
    snap.source = "bingx_tristate_rest"
    return snap


def verify_native_stop_loss_detailed(symbol, side, trigger_price, required_qty, position_side=None,
                                      *, trigger_prices=None):
    """Verify a native stop against all journal-proven trigger prices.

    Recovery may legitimately see a stop moved after entry. Every candidate is
    still matched strictly (native type, side, positionSide, active status,
    exact trigger and quantity coverage); no generic tolerance is introduced.
    Returns ``(status, order_id, matched_price)``.
    """
    if PAPER_MODE:
        return "PAPER_SIMULATED", None, None
    expected_position_side = str(position_side or STATE.get("position_side") or "").upper()
    if expected_position_side not in {"BOTH", "LONG", "SHORT"}:
        log_execution(f"[NATIVE_SL] Position Side unavailable for {symbol}; refusing to guess BOTH", "ERROR")
        return "UNKNOWN", None, None
    try:
        from bingx_protection import (fetch_native_stop_evidence, is_matching_native_stop_order,
                                      native_stop_candidate_summary)
        # Conditional-order absence is UNKNOWN unless both BingX conditional
        # endpoints succeed. A plain fetch_open_orders=[] is not evidence.
        records, query_succeeded, query_details = fetch_native_stop_evidence(
            ex, normalize_symbol(symbol), api_call=safe_api_call,
            logger=lambda message: log_execution(f"[NATIVE_SL] {message}", "WARN"))
        close_side = "sell" if str(side).upper() == "BUY" else "buy"
        candidate_prices = []
        for value in [trigger_price, *(trigger_prices or [])]:
            number = safe_float(value, 0.0)
            if number > 0 and number not in candidate_prices:
                candidate_prices.append(number)
        if not candidate_prices:
            return "UNKNOWN", None, None
        price_to_precision = getattr(ex, "price_to_precision", None)
        for expected_price in candidate_prices:
            # BingX stores trigger prices at the market's permitted precision.
            # Compute only the quantization delta for this exact requested stop;
            # do not loosen matching by a generic tick or percentage.
            rounding_delta = 0.0
            if callable(price_to_precision):
                try:
                    exchange_price = safe_float(price_to_precision(normalize_symbol(symbol), expected_price))
                    if exchange_price > 0:
                        rounding_delta = abs(expected_price - exchange_price) + 1e-12
                except Exception as exc:
                    log_execution(f"[NATIVE_SL] price precision read unavailable; strict price match retained: {type(exc).__name__}", "WARN")
            for order in records:
                if is_matching_native_stop_order(
                    order, symbol=normalize_symbol(symbol), position_side=expected_position_side,
                    close_side=close_side, trigger_price=expected_price,
                    required_qty=float(required_qty), price_tolerance_abs=rounding_delta,
                ):
                    info = order.get("info") if isinstance(order.get("info"), dict) else {}
                    order_id = (order.get("id") or order.get("orderId") or order.get("triggerOrderId")
                                or info.get("orderId") or info.get("triggerOrderId"))
                    return "VERIFIED", str(order_id) if order_id is not None else None, expected_price
        if records:
            # Redacted forensic evidence: enough to identify the failed match,
            # without logging credentials or the complete private API payload.
            for candidate in records[:12]:
                log_execution(f"[NATIVE_SL_DIAG] candidate={native_stop_candidate_summary(candidate)}", "WARN")
            log_execution(f"[NATIVE_SL_DIAG] expected symbol={normalize_symbol(symbol)} side={close_side.upper()} "
                          f"positionSide={expected_position_side} triggers={candidate_prices} qty={float(required_qty)} "
                          f"candidates={len(records)}", "WARN")
        else:
            log_execution(f"[NATIVE_SL_DIAG] no conditional orders returned; query_succeeded={query_succeeded} "
                          f"symbol={normalize_symbol(symbol)}", "WARN")
        return (("MISSING", None, None) if query_succeeded else ("UNKNOWN", None, None))
    except Exception as exc:
        log_execution(f"[NATIVE_SL] verification error: {exc}", "ERROR")
        return "UNKNOWN", None, None


def verify_native_stop_loss(symbol, side, trigger_price, required_qty, position_side=None,
                            *, trigger_prices=None):
    """Compatibility wrapper returning ``(status, order_id)``."""
    status, order_id, _ = verify_native_stop_loss_detailed(
        symbol, side, trigger_price, required_qty, position_side, trigger_prices=trigger_prices)
    return status, order_id


def _fetch_external_close_diagnostic(symbol, side, entry_time=None, stop_order_id=None,
                                     stop_price=None, bot_close_order_ids=None):
    """Read BingX close evidence; return a reason only to the confidence supported by data."""
    from ggp_close_diagnostics import classify_external_close
    if PAPER_MODE:
        return {"reason": "PAPER_CLOSE_OR_RESTART", "confidence": "PAPER", "close_price": None,
                "realized_pnl": None, "filled_qty": 0.0, "order_id": None, "evidence": ["paper_mode"]}
    sym = normalize_symbol(symbol)
    since = int((safe_float(entry_time, time.time()-86400))*1000) if entry_time else int((time.time()-86400)*1000)
    orders = []
    trades = []
    for method_name in ("fetch_closed_orders", "fetch_orders"):
        method = getattr(ex, method_name, None)
        if not callable(method):
            continue
        try:
            fetched = safe_api_call(method, sym, since, 200)
            if isinstance(fetched, list):
                orders = fetched
                if orders:
                    break
        except Exception as exc:
            log_execution(f"[CLOSE_DIAG] {method_name} unavailable for {symbol}: {exc}", "WARN",
                          debounce_key=f"close_diag_{method_name}_{symbol}", debounce_sec=120)
    try:
        fetched_trades = safe_api_call(ex.fetch_my_trades, sym, since, 500)
        if isinstance(fetched_trades, list):
            trades = fetched_trades
    except Exception as exc:
        log_execution(f"[CLOSE_DIAG] fetch_my_trades unavailable for {symbol}: {exc}", "WARN",
                      debounce_key=f"close_diag_trades_{symbol}", debounce_sec=120)
    return classify_external_close(orders, trades, position_side=side,
                                   expected_stop_order_id=stop_order_id,
                                   expected_stop_price=stop_price,
                                   bot_close_order_ids=bot_close_order_ids or [])


def _clear_confirmed_flat_unlocked(symbol, reason="EXCHANGE_FLAT_CONFIRMED"):
    """Clear exposure only after BingX FLAT; classify close cause from exchange evidence."""
    global RECOVERY_BLOCK_ENTRIES, _ACTIVE_TRADE
    if not STATE.get("open"):
        MEMORY["position_status"] = "FLAT"
        MEMORY["current_position"] = None
        return True
    trade_id = STATE.get("trade_id")
    side = STATE.get("side")
    entry_time = STATE.get("entry_time")
    stop_id = STATE.get("native_sl_order_id")
    stop_price = STATE.get("native_sl_original_price") or STATE.get("sl") or STATE.get("synthetic_sl")
    try:
        diagnostic = _fetch_external_close_diagnostic(
            symbol, side, entry_time, stop_id, stop_price, STATE.get("bot_close_order_ids", []))
    except Exception as exc:
        diagnostic = {"reason": "EXTERNAL_CLOSE_UNKNOWN", "confidence": "UNKNOWN",
                      "close_price": None, "realized_pnl": None, "filled_qty": 0.0,
                      "order_id": None, "evidence": [f"diagnostic_error:{type(exc).__name__}"]}
    diagnostic["detected_at"] = time.time()
    diagnostic["symbol"] = symbol
    diagnostic["entry_side"] = side
    diagnostic["exchange_flat_confirmed"] = True
    MEMORY["last_close_diagnostic"] = diagnostic
    STATE["last_close_diagnostic"] = diagnostic

    classified_reason = str(diagnostic.get("reason") or "EXTERNAL_CLOSE_UNKNOWN")
    record_trade_management_event(
        "STOP_LOSS_CLASSIFIED" if classified_reason in {"NATIVE_STOP_LOSS", "STOP_LOSS_SUSPECTED"} else "EXTERNAL_CLOSE",
        "EXCHANGE_FLAT_CONFIRMED", log_level="WARN", symbol=symbol, side=side,
        reason=classified_reason, confidence=diagnostic.get("confidence", "UNKNOWN"),
        close_price=diagnostic.get("close_price"), filled_qty=diagnostic.get("filled_qty"),
        order_id=diagnostic.get("order_id"), evidence=diagnostic.get("evidence", []),
        realized_pnl_usdt=diagnostic.get("realized_pnl"), pnl_source="BINGX_TRADE_HISTORY" if diagnostic.get("realized_pnl") is not None else "UNAVAILABLE",
    )
    # Preserve UNKNOWN as UNKNOWN. The supplied `reason` describes the detection path,
    # not proof of why the exchange position closed.
    final_reason = classified_reason
    close_price = safe_float(diagnostic.get("close_price"), 0.0) or None
    realized = diagnostic.get("realized_pnl")
    realized_known = realized is not None
    # Include any previously verified partial TP1 fills in the final trade total.
    # If either the earlier partial or this final exchange close lacks realized PnL,
    # keep the aggregate explicitly incomplete instead of presenting a false total.
    prior_realized = safe_float(STATE.get("last_close_realized_pnl"), 0.0)
    prior_complete = bool(STATE.get("last_close_pnl_complete", True))
    aggregate_realized = prior_realized + safe_float(realized, 0.0)
    aggregate_complete = prior_complete and realized_known
    STATE.update({
        "close_verified": True,  # BingX has authoritatively confirmed FLAT.
        "last_close_verified": True,
        "last_close_reason": final_reason,
        "last_close_fill_price": close_price,
        "last_close_filled_qty": safe_float(diagnostic.get("filled_qty"), 0.0),
        "last_close_realized_pnl": aggregate_realized,
        "last_close_pnl_source": "BINGX_ORDER_REALIZED_PNL" if aggregate_complete else "REALIZED_PNL_UNAVAILABLE",
        "last_close_pnl_complete": aggregate_complete,
        "last_exchange_close_order_id": diagnostic.get("order_id"),
    })

    if classified_reason == "STOP_LOSS_SUSPECTED":
        log_execution(f"[STOP_DIAG] {symbol}: SL suspected from exit-price proximity, but no filled native stop order was verified", "WARN")
    elif classified_reason == "NATIVE_STOP_LOSS":
        if realized_known:
            initial_margin = safe_float(STATE.get("initial_margin"), 0.0)
            if initial_margin <= 0:
                initial_margin = (safe_float(STATE.get("entry")) * safe_float(STATE.get("initial_qty"), STATE.get("qty", 0.0))) / max(float(LEVERAGE), 1.0)
            total_realized = aggregate_realized if aggregate_complete else None
            roe_pct = (total_realized / initial_margin * 100.0) if total_realized is not None and initial_margin > 0 else None
            tg_sl_hit(symbol, roe_pct, pnl_usdt=total_realized, fill_price=close_price,
                      order_id=diagnostic.get("order_id"),
                      pnl_source="BINGX_TRADE_HISTORY" if aggregate_complete else "REALIZED_PNL_INCOMPLETE")
        else:
            send_once(f"🛑 <b>CONFIRMED NATIVE STOP LOSS</b> on {symbol}\nExecution confirmed; realized PnL unavailable", f"sl_confirmed_{symbol}_{trade_id}", 30)
        log_execution(f"[STOP_DIAG] {symbol}: confirmed native STOP fill order={diagnostic.get('order_id')} price={close_price}; evidence={diagnostic.get('evidence')}", "WARN")
    else:
        log_execution(f"[CLOSE_DIAG] {symbol}: reason={classified_reason} confidence={diagnostic.get('confidence')} evidence={diagnostic.get('evidence')} detection_path={reason}", "WARN")

    # An exchange/external close is never an RF close. Drop any stale bot-side
    # RF context before finalizing so it cannot be relabelled as an RF trigger.
    if str(final_reason).upper() != RF_OPPOSITE_CLOSE_REASON:
        STATE.pop("rf_opposite_close", None)

    # Run the same idempotent finalizer used by bot-driven closes. It records PnL
    # only when BingX supplies realized PnL; otherwise it records PnL_UNAVAILABLE.
    if trade_id and INSTITUTIONAL_TRADE_JOURNAL:
        try:
            finalize_trade_with_reality(symbol, close_reason=final_reason)
        except Exception as exc:
            log_execution(f"[JOURNAL] external-flat finalization failed: {exc}", "ERROR")
    else:
        finalize_trade_with_reality(symbol, close_reason=final_reason)

    with _TRADE_LOCK:
        # Finalizer normally clears these fields; enforce the exchange-confirmed flat invariant.
        STATE.update({"open": False, "remaining_qty": 0.0, "exchange_qty": 0.0,
                      "close_verified": False, "execution_unresolved": False,
                      "close_reconciliation_required": False, "recovery_hold": False})
        TRADE_STATE["in_position"] = False
        TRADE_STATE["qty"] = 0.0
        _live_manager.lifecycle_state = TradeLifecycleState.CLOSED
        _ACTIVE_TRADE = False
        DASHBOARD_STATE["live_trade_mode"] = False
    MEMORY["position_status"] = "FLAT"
    MEMORY["current_position"] = None
    MEMORY["recovery_status"] = "RECONCILED_FLAT"
    RECOVERY_BLOCK_ENTRIES = False
    clear_position_dashboard()
    STATE["last_close_verified"] = True
    STATE["last_close_exchange_flat_confirmed"] = True
    log_execution(f"[POS_SYNC] {symbol} confirmed FLAT by BingX; close_reason={final_reason}; confidence={diagnostic.get('confidence')}; no duplicate close sent", "WARN")
    return True

_EXTERNAL_FLAT_FINALIZE_LOCK = threading.Lock()

def _clear_confirmed_flat(symbol, reason="EXCHANGE_FLAT_CONFIRMED"):
    """Serialize external-flat finalization; duplicate supervisors cannot finalize twice."""
    if not _EXTERNAL_FLAT_FINALIZE_LOCK.acquire(blocking=False):
        log_execution(f"[POS_SYNC] duplicate FLAT finalization suppressed for {symbol}", "WARN",
                      debounce_key=f"flat_finalize_busy_{symbol}", debounce_sec=30)
        return False
    try:
        return _clear_confirmed_flat_unlocked(symbol, reason=reason)
    except Exception as exc:
        log_execution(f"[POS_SYNC] FLAT finalization failed for {symbol}: {type(exc).__name__}: {exc}", "CRITICAL")
        return False
    finally:
        _EXTERNAL_FLAT_FINALIZE_LOCK.release()


def _resolve_pending_close_from_snapshot(symbol, snap):
    """Resolve an ambiguous bot close after a fresh OPEN snapshot and terminal order read."""
    order_id = STATE.get("pending_close_order_id")
    if not order_id:
        return not bool(STATE.get("execution_unresolved"))
    try:
        order = safe_api_call(ex.fetch_order, str(order_id), normalize_symbol(symbol)) if hasattr(ex, "fetch_order") else None
    except Exception as exc:
        log_execution(f"[CLOSE_RECOVERY] cannot read pending order {order_id}: {exc}", "WARN")
        return False
    if not isinstance(order, dict):
        return False
    status = str(order.get("status") or "UNKNOWN").upper()
    terminal = status in {"CLOSED", "FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
    if not terminal:
        return False
    before = safe_float(STATE.get("pending_close_before_qty"))
    after = safe_float(snap.qty)
    filled_by_delta = max(0.0, before-after) if before > 0 else 0.0
    if before > 0 and filled_by_delta <= max(before*0.002, 1e-10):
        # A terminal rejected/cancelled order with no position delta is safe to clear.
        if status in {"REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}:
            STATE.update({"pending_close_order_id": None, "pending_close_reason": None,
                          "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
                          "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
                          "execution_unresolved": False, "close_reconciliation_required": False})
            globals()["RECOVERY_BLOCK_ENTRIES"] = False
            _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
            log_execution(f"[CLOSE_RECOVERY] pending close {order_id} ended {status} without a position reduction; normal management may resume", "WARN")
            return True
        return False
    if status in {"REJECTED", "CANCELED", "CANCELLED", "EXPIRED"} and filled_by_delta <= 0:
        return False
    reason = str(STATE.get("pending_close_reason") or "BOT_EXECUTED_CLOSE")
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    avg = safe_float(order.get("average") or order.get("price") or info.get("avgPrice"))
    realized = None
    for key in ("realizedPnl", "realizedProfit", "realized_pnl", "profit"):
        if info.get(key) not in (None, ""):
            try:
                realized = float(info[key]); break
            except (TypeError, ValueError):
                pass
    complete = realized is not None
    if realized is None:
        realized = 0.0
    STATE.update({"last_close_order_id": str(order_id), "last_close_reason": reason,
                  "last_close_fill_price": avg or STATE.get("last_close_fill_price"),
                  "last_close_filled_qty": filled_by_delta,
                  "last_close_order_realized_pnl": realized if complete else None,
                  "last_close_order_pnl_source": "BINGX_ORDER_REALIZED_PNL" if complete else "REALIZED_PNL_UNAVAILABLE",
                  "last_close_realized_pnl": safe_float(STATE.get("last_close_realized_pnl"), 0.0)+realized,
                  "last_close_pnl_source": "BINGX_ORDER_REALIZED_PNL" if complete else "REALIZED_PNL_UNAVAILABLE",
                  "last_close_pnl_complete": bool(STATE.get("last_close_pnl_complete", True) and complete),
                  "pending_close_order_id": None, "pending_close_reason": None,
                  "pending_close_before_qty": 0.0, "pending_close_requested_qty": 0.0,
                  "pending_close_target_remaining_qty": None, "pending_close_started_at": 0.0,
                  "execution_unresolved": False, "close_reconciliation_required": False,
                  "remaining_qty": after, "exchange_qty": after})
    if reason == "TP1":
        STATE["tp1_filled_qty"] = safe_float(STATE.get("tp1_filled_qty"), 0.0)+filled_by_delta
        if STATE.get("initial_qty", 0) > 0 and after <= safe_float(STATE.get("initial_qty"))*0.505:
            STATE["tp1_hit"] = True
            STATE["partial_closed"] = True
    elif reason == "TP2":
        STATE["tp2_filled_qty"] = safe_float(STATE.get("tp2_filled_qty"), 0.0)+filled_by_delta
    globals()["RECOVERY_BLOCK_ENTRIES"] = False
    log_execution(f"[CLOSE_RECOVERY] reconciled order={order_id} status={status} position_delta={filled_by_delta:.10f} remaining={after:.10f} reason={reason}", "SUCCESS")
    _persist_trade_journal_snapshot(force=True) if "_persist_trade_journal_snapshot" in globals() else None
    return True


def _apply_open_snapshot(symbol, snap):
    global _ACTIVE_TRADE
    with _TRADE_LOCK:
        _ACTIVE_TRADE = True
        was_open = bool(STATE.get("open"))
        if not was_open:
            STATE.update({
                "open": True, "side": snap.side, "entry": snap.entry_price,
                # Current exchange exposure is known; original filled quantity is not.
                # Keep it unknown until a matching journal row proves the original fill.
                "qty": 0.0, "initial_qty": 0.0, "actual_filled_qty": 0.0,
                "exchange_qty": snap.qty, "remaining_qty": snap.qty,
                "current_symbol": symbol, "entry_time": 0.0,
                "recovery_hold": True, "recovery_reason": "OPEN_EXPOSURE_WITHOUT_RESTORED_JOURNAL",
            })
            TRADE_STATE.update({"in_position": True, "symbol": symbol, "side": snap.side,
                                "entry": snap.entry_price, "qty": snap.qty, "last_update_ts": time.time()})
        else:
            # Exchange quantity is authoritative for remaining exposure, while the
            # original filled quantity remains immutable for TP1/TP2 accounting.
            original_qty = safe_float(STATE.get("initial_qty"), 0.0)
            if original_qty > 0:
                STATE["qty"] = original_qty
            else:
                STATE["qty"] = 0.0
                STATE["initial_qty"] = 0.0
            STATE["exchange_qty"] = snap.qty
            STATE["remaining_qty"] = snap.qty
            STATE["entry"] = snap.entry_price
            STATE["side"] = snap.side
            TRADE_STATE.update({"in_position": True, "symbol": symbol, "side": snap.side,
                                "entry": snap.entry_price, "qty": snap.qty})
        STATE["margin"] = snap.margin
        STATE["unrealized_pnl_usdt"] = snap.unrealized_pnl
        STATE["roe_pct"] = snap.roe_pct
        STATE["leverage"] = snap.leverage
        STATE["mark_price"] = snap.mark_price
        STATE["liquidation_price"] = snap.liquidation_price
        STATE["exchange_state"] = "OPEN"
    MEMORY["position_status"] = "OPEN"
    MEMORY["current_position"] = {"symbol": symbol, "side": snap.side, "qty": snap.qty,
                                  "entry": snap.entry_price, "mark": snap.mark_price}
    return snap


def _journal_snapshot_state():
    # A compact serializable copy of management state; preserve TP/trailing/SL fields.
    return {k: v for k, v in STATE.items() if k not in {"dataframe", "df", "raw_response"}}


def _persist_trade_journal_start(setup_type=None, item=None):
    if not INSTITUTIONAL_TRADE_JOURNAL or not STATE.get("open"):
        return False
    trade_id = str(STATE.get("trade_id") or uuid.uuid4().hex)
    STATE["trade_id"] = trade_id
    symbol = STATE.get("current_symbol") or STATE.get("symbol")
    side = STATE.get("side")
    qty = safe_float(STATE.get("qty"))
    entry = safe_float(STATE.get("entry"))
    sl = safe_float(STATE.get("synthetic_sl", STATE.get("sl")))
    if not (symbol and side and qty > 0 and entry > 0):
        return False
    setup = setup_type or (item or {}).get("setup_type") or STATE.get("trade_type", "UNKNOWN")
    try:
        INSTITUTIONAL_TRADE_JOURNAL.start_trade(trade_id, symbol, side, str(setup), qty, entry, sl or None)
        INSTITUTIONAL_TRADE_JOURNAL.save_snapshot(trade_id, _journal_snapshot_state(),
                                                  event_key=f"entry-state:{trade_id}", event_type="ENTRY_STATE")
        STATE["journal_status"] = "PERSISTED"
        return True
    except Exception as exc:
        STATE["journal_status"] = "ERROR"
        log_execution(f"[JOURNAL] failed to persist new trade: {exc}", "ERROR")
        return False


def _persist_trade_journal_snapshot(force=False):
    global _journal_last_snapshot_ts
    if not INSTITUTIONAL_TRADE_JOURNAL or not STATE.get("open") or not STATE.get("trade_id"):
        return False
    now = time.time()
    if not force and now - _journal_last_snapshot_ts < 5:
        return False
    try:
        key = f"snapshot:{int(now * 1000)}"
        INSTITUTIONAL_TRADE_JOURNAL.save_snapshot(str(STATE["trade_id"]), _journal_snapshot_state(),
                                                  event_key=key, event_type="MANAGEMENT_SNAPSHOT")
        _journal_last_snapshot_ts = now
        return True
    except Exception as exc:
        log_execution(f"[JOURNAL] snapshot write failed: {exc}", "ERROR")
        return False


def _recovery_hold(reason, *, symbol=None, trade_id=None, details=None):
    """Keep a real exchange exposure visible while prohibiting fabricated management."""
    reason = str(reason or "management_state_incomplete")
    details = dict(details or {})
    symbol_status = str(details.get("symbol_status") or ("MISSING" if not symbol else "PRESENT"))
    STATE.update({"open": True, "recovery_hold": True, "recovery_reason": reason,
                  "recovery_symbol_status": symbol_status, "native_sl_verified": False})
    if STATE.get("native_sl_status") not in {"MISSING", "UNKNOWN", "MISMATCH"}:
        STATE["native_sl_status"] = "RECOVERY_PENDING"
    if symbol:
        STATE["current_symbol"] = symbol
    if trade_id:
        STATE["trade_id"] = str(trade_id)
    globals()["RECOVERY_BLOCK_ENTRIES"] = True
    MEMORY["position_status"] = "OPEN_RECOVERY_PENDING"
    MEMORY["recovery_status"] = "RECOVERY_HOLD"
    _live_manager.lifecycle_state = TradeLifecycleState.ERROR_DEGRADED
    DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.ERROR_DEGRADED.value
    DASHBOARD_STATE["live_trade_mode"] = False
    log_execution(f"[RECOVERY_HOLD] {reason}; exposure remains tracked; no new entries or TP/SL fabrication", "ERROR",
                  debounce_key=f"recovery_hold:{symbol or STATE.get('current_symbol')}:{reason}", debounce_sec=30)
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal and trade_id:
        try:
            journal.event(str(trade_id), f"recovery-hold:{reason}:{int(time.time())}",
                          "RECOVERY_HOLD", {"reason": reason, "details": details or {},
                                           "symbol": symbol or STATE.get("current_symbol")})
        except Exception as exc:
            log_execution(f"[RECOVERY_JOURNAL] Could not append recovery event: {exc}", "WARN")
    return False


def _reconcile_journal_entry_fill(row, snap):
    """Prove a lost original fill from the exchange entry order before restoring.

    A crash between the durable ENTRY_PENDING checkpoint and the post-fill
    checkpoint leaves `actual_filled_qty` unset. We query the exchange for the
    recorded entry order and, only when the exchange confirms a terminal fill,
    promote the persisted row in place. This is exchange evidence, not a guess.
    """
    from ggp_recovery import reconcile_entry_fill
    saved = row.get("state") if isinstance(row.get("state"), dict) else {}
    has_fill = safe_float(row.get("actual_filled_qty"), safe_float(saved.get("actual_filled_qty"))) > 0
    if has_fill:
        return False
    client_order_id = str(row.get("client_order_id") or saved.get("entry_client_order_id") or "")
    exchange_order_id = str(row.get("exchange_order_id") or saved.get("entry_exchange_order_id") or "")
    if not client_order_id and not exchange_order_id:
        return False
    order = None
    fetch_order = getattr(ex, "fetch_order", None)
    if callable(fetch_order):
        for candidate in (exchange_order_id, client_order_id):
            if not candidate:
                continue
            try:
                fetched = safe_api_call(fetch_order, candidate, normalize_symbol(snap.symbol))
            except Exception as exc:
                log_execution(f"[RECOVERY] entry-order read-back failed for {candidate}: {type(exc).__name__}: {exc}", "WARN")
                continue
            if isinstance(fetched, dict):
                order = fetched
                break
    if order is None:
        return False
    updates, error = reconcile_entry_fill(row, order, exchange_open_qty=snap.qty)
    if error or not updates:
        log_execution(f"[RECOVERY] entry-fill reconciliation withheld: {error or 'no_evidence'}", "WARN")
        return False
    merged = dict(saved)
    merged.update(updates)
    merged["remaining_qty"] = float(snap.qty)
    merged["exchange_qty"] = float(snap.qty)
    row["state"] = merged
    for key, value in updates.items():
        if key in row:
            row[key] = value
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal and row.get("trade_id"):
        try:
            journal.update(str(row["trade_id"]),
                           actual_filled_qty=float(updates["actual_filled_qty"]),
                           entry_qty=float(updates["actual_filled_qty"]),
                           entry_price=float(updates["entry"]),
                           recovery_status="ENTRY_FILL_RECONCILED_FROM_EXCHANGE")
            journal.save_snapshot(str(row["trade_id"]), merged,
                                  event_key=f"entry-fill-reconciled:{int(time.time())}",
                                  event_type="ENTRY_FILL_RECONCILED")
        except Exception as exc:
            log_execution(f"[RECOVERY] journal entry-fill update failed: {exc}", "WARN")
    log_execution(f"[RECOVERY] entry fill proven from exchange order {exchange_order_id or client_order_id}: "
                  f"filled={updates['actual_filled_qty']} entry={updates['entry']}", "SUCCESS")
    return True


def _collect_tp1_close_order_ids(saved_state, journal_row=None):
    """Gather the bot's own close-order ids for this trade from persisted evidence.

    Recovery must not depend on the live STATE alone: after a restart the ids live
    in the journal snapshot (`state_json`). Both the snapshot and the flat row are
    inspected, and only ids that were explicitly recorded by the bot are returned.
    No id is invented.
    """
    ids = set()
    sources = []
    for container in (saved_state, journal_row):
        if not isinstance(container, dict):
            continue
        sources.append(container.get("bot_close_order_ids"))
        sources.append([container.get("pending_close_order_id")])
        sources.append([container.get("last_close_order_id")])
        sources.append([container.get("last_exchange_close_order_id")])
    for source in sources:
        if isinstance(source, (list, tuple, set)):
            for value in source:
                if value not in (None, ""):
                    ids.add(str(value))
        elif source not in (None, ""):
            ids.add(str(source))
    return ids


def _tp1_fill_verified_on_exchange(symbol, side, expected_fill_qty=None, tolerance_ratio=0.02,
                                   position_side=None, position_mode=None,
                                   close_order_ids=None, trade_entry_ts=None):
    """Return True only when a filled bot TP1 reduce order is proven on BingX.

    A reduced live quantity is not, by itself, proof that TP1 executed: the
    reduction could be a manual or external close. This helper accepts an order
    only when ALL of the following hold:

      1. it carries a non-empty exchange order id;
      2. its symbol matches after normalization;
      3. its position identity matches the account mode:
         - HEDGE: `positionSide` must be present and equal LONG/SHORT;
         - ONE_WAY: `positionSide` must be absent or BOTH (never a hedge leg);
      4. its side is the opposite close side;
      5. its status proves an actual fill and `filled > 0`;
      6. its filled quantity matches the TP1 policy (50% of the original fill);
      7. it is linked to THIS trade, either by a recorded bot close-order id, or by
         a TP/partial tag combined with an order timestamp at/after entry.

    `close_order_ids` should be supplied explicitly by the caller (from the journal
    row) so recovery never depends on the live STATE. When omitted, the ids are
    read from STATE only for non-recovery callers.
    """
    if PAPER_MODE:
        return False
    if close_order_ids is None:
        close_order_ids = _collect_tp1_close_order_ids(STATE, None)
    recorded_ids = {str(x) for x in (close_order_ids or []) if x not in (None, "")}

    mode = str(position_mode or "").upper().replace("-", "_").replace(" ", "_")
    expected_position_side = str(position_side or "").upper()
    if mode not in {"HEDGE", "ONE_WAY"}:
        log_execution("[RECOVERY] TP1 proof refused: account position mode is not established", "WARN")
        return False

    close_side = "SELL" if str(side).upper() == "BUY" else "BUY"
    want_symbol = str(symbol).upper().replace("/", "-").split(":")[0]
    since = int((safe_float(STATE.get("entry_time"), time.time() - 86400)) * 1000)
    orders = []
    for method_name in ("fetch_closed_orders", "fetch_orders"):
        method = getattr(ex, method_name, None)
        if not callable(method):
            continue
        try:
            fetched = safe_api_call(method, normalize_symbol(symbol), since, 200)
        except Exception as exc:
            log_execution(f"[RECOVERY] {method_name} unavailable for TP1 proof: {exc}", "WARN")
            continue
        if isinstance(fetched, list):
            orders.extend(fetched)

    expected = safe_float(expected_fill_qty, 0.0)
    entry_ts_ms = safe_float(trade_entry_ts, 0.0)
    if entry_ts_ms and entry_ts_ms < 1e12:
        entry_ts_ms *= 1000.0
    for order in orders:
        if not isinstance(order, dict):
            continue
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        order_id = str(order.get("id") or order.get("orderId") or order.get("orderID")
                       or info.get("orderId") or info.get("orderID") or "")
        status = str(order.get("status") or order.get("state") or info.get("status")
                     or info.get("orderStatus") or "").upper()
        filled = safe_float(order.get("filled") or info.get("executedQty") or order.get("executedQty"), 0.0)
        order_side = str(order.get("side") or info.get("side") or "").upper()
        order_position_side = str(order.get("positionSide") or order.get("posSide")
                                  or info.get("positionSide") or info.get("posSide") or "").upper()
        raw_symbol = str(order.get("symbol") or info.get("symbol") or "").upper().replace("/", "-").split(":")[0]
        type_values = {str(v).upper() for v in (order.get("type"), info.get("type"), order.get("orderType"),
                                                info.get("origType")) if v not in (None, "")}

        # 1) exchange order id present
        if not order_id:
            continue
        # 2) symbol identity (mandatory: an order with no symbol cannot be trusted)
        if not raw_symbol or raw_symbol != want_symbol:
            continue
        # 3) position identity per account mode
        if mode == "HEDGE":
            # positionSide is mandatory and must match the expected leg.
            if order_position_side not in {"LONG", "SHORT"}:
                continue
            if not expected_position_side or order_position_side != expected_position_side:
                continue
        else:  # ONE_WAY
            if order_position_side in {"LONG", "SHORT"}:
                continue
        # 4) close side
        if order_side != close_side:
            continue
        # 5) filled
        if status not in {"CLOSED", "FILLED", "PARTIALLY_FILLED", "PARTIALLYFILLED"} or filled <= 0:
            continue
        # 6) TP1 quantity policy
        if expected > 0 and abs(filled - expected) > max(expected * tolerance_ratio, 1e-10):
            continue
        # 7) link to this trade
        order_ts = safe_float(order.get("timestamp") or info.get("time") or info.get("updateTime")
                              or order.get("time") or info.get("createTime"), 0.0)
        linked_by_id = bool(order_id and order_id in recorded_ids)
        type_blob = " ".join(type_values)
        tagged = any(tag in type_blob for tag in ("PARTIAL", "TP1", "TAKE_PROFIT"))
        if not linked_by_id:
            # Without a recorded id, require a TP/partial tag AND a timestamp that
            # proves the order belongs to the current trade (not a previous one on
            # the same symbol). A missing timestamp cannot prove linkage.
            if not tagged:
                continue
            if entry_ts_ms and order_ts:
                if order_ts < entry_ts_ms - 300000:  # 5-minute clock-skew tolerance
                    continue
            elif entry_ts_ms and not order_ts:
                continue
        log_execution(f"[RECOVERY] TP1 fill proven order={order_id} qty={filled} "
                      f"positionSide={order_position_side or 'n/a'} mode={mode} "
                      f"linked={'id' if linked_by_id else 'tag+time'}", "SUCCESS")
        return True
    return False


def _journal_native_stop_prices(saved_state=None, *, primary=0.0):
    """Journal-proven stop levels, newest first; never invents a price."""
    saved = saved_state if isinstance(saved_state, dict) else {}
    prices = []
    for value in (primary, saved.get("native_sl_last_verified_price"),
                  saved.get("native_sl_local_target"), saved.get("native_sl_original_price"),
                  saved.get("synthetic_sl"), saved.get("sl")):
        number = safe_float(value, 0.0)
        if number > 0 and number not in prices:
            prices.append(number)
    return prices


def _repair_native_sl_from_journal(symbol, side, quantity, *, prices, mode, position_side,
                                   trade_id=None):
    """Optional idempotent protection repair; HOLD stays set until read-back."""
    if os.getenv("NAISO_NATIVE_SL_REPAIR", "false").lower() not in {"1", "true", "yes"}:
        return False, "REPAIR_DISABLED", None
    if not prices or quantity <= 0 or str(position_side).upper() not in {"BOTH", "LONG", "SHORT"}:
        return False, "INSUFFICIENT_JOURNAL_EVIDENCE", None
    try:
        from bingx_execution import PositionMode, TradeExecutionService
        service = TradeExecutionService(
            ex, journal=globals().get("INSTITUTIONAL_TRADE_JOURNAL"),
            logger=lambda message: log_execution(f"[NATIVE_SL_REPAIR] {message}", "INFO"),
            api_caller=safe_api_call)
        result = service.ensure_native_stop(
            symbol=normalize_symbol(symbol), side=str(side).upper(), quantity=float(quantity),
            trigger_price=float(prices[0]), mode=PositionMode(str(mode).upper()),
            position_side=str(position_side).upper(), trigger_prices=prices[1:])
    except Exception as exc:
        return False, f"REPAIR_EXCEPTION:{type(exc).__name__}", None
    if not result.verified:
        log_execution(f"[NATIVE_SL_REPAIR] HOLD retained: {result.outcome}/{result.reason}", "WARN",
                      debounce_key=f"native_repair_{symbol}", debounce_sec=60)
        return False, result.reason, getattr(result, "order_id", None)
    STATE.update({
        "native_sl_status": "VERIFIED_REPAIRED",
        "native_sl_verified": True,
        "native_sl_order_id": getattr(result, "order_id", None),
        "native_sl_original_price": getattr(result, "verified_price", None) or prices[0],
        "native_sl_last_verified_price": getattr(result, "verified_price", None) or prices[0],
        "native_sl_policy": "EXCHANGE_VERIFIED_RECOVERY_REPAIR",
    })
    record_trade_management_event(
        "NATIVE_SL_REPAIR", "EXCHANGE_VERIFIED", log_level="SUCCESS",
        symbol=symbol, side=side, trade_id=trade_id,
        outcome=result.outcome, reason=result.reason,
        order_id=getattr(result, "order_id", None),
        verified_price=getattr(result, "verified_price", None))
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal and trade_id:
        try:
            journal.update(str(trade_id), native_sl_verified=1,
                           native_sl_status="VERIFIED_REPAIRED",
                           native_sl_order_id=getattr(result, "order_id", None))
            _persist_trade_journal_snapshot(force=True)
        except Exception as exc:
            log_execution(f"[NATIVE_SL_REPAIR] journal update failed: {exc}", "WARN")
    log_execution(f"[NATIVE_SL_REPAIR] verified protection restored for {symbol} via {result.outcome}", "SUCCESS")
    return True, result.reason, getattr(result, "order_id", None)


def _restore_trade_from_journal(position, journal_row):
    """Restore GGP's existing LiveTradeManager from persisted facts + BingX truth."""
    try:
        symbol = str(_position_symbol(position) or journal_row.get("symbol") or "")
        snap = _snapshot_from_position(symbol, position)
        row = dict(journal_row or {})
        saved = row.get("state") if isinstance(row.get("state"), dict) else {}
        row["state"] = saved

        if roro_canonical_symbol(row.get("symbol")) != roro_canonical_symbol(symbol):
            return _recovery_hold("journal_symbol_mismatch", symbol=symbol, trade_id=row.get("trade_id"))
        if str(row.get("side") or saved.get("side") or "").upper() != snap.side:
            return _recovery_hold("journal_side_mismatch", symbol=symbol, trade_id=row.get("trade_id"),
                                  details={"journal_side": row.get("side"), "exchange_side": snap.side})

        # If the durable row never recorded the executed size (crash before the
        # post-fill checkpoint), prove it from the exchange entry order before
        # deciding that management state is incomplete.
        try:
            _reconcile_journal_entry_fill(row, snap)
        except Exception as exc:
            log_execution(f"[RECOVERY] entry-fill reconciliation error: {type(exc).__name__}: {exc}", "WARN")
        saved = row.get("state") if isinstance(row.get("state"), dict) else {}

        mode_result = BingXPositionModeAdapter(ex).read(normalize_symbol(symbol))
        exchange_mode = getattr(mode_result.mode, "value", str(mode_result.mode))
        position_mode, position_side, identity_error = resolve_position_identity(
            row, position, exchange_mode=exchange_mode, trade_side=snap.side)
        if identity_error:
            candidate, missing = build_restored_management_state(
                row, snap, position_mode="UNKNOWN", position_side="UNKNOWN")
            STATE.update(candidate)
            # Preserve exchange-side values for visibility even though the mode is not trusted.
            STATE.update({"entry": snap.entry_price, "remaining_qty": snap.qty,
                          "exchange_qty": snap.qty, "position_mode": "UNKNOWN",
                          "position_side": "UNKNOWN"})
            return _recovery_hold(identity_error, symbol=symbol, trade_id=row.get("trade_id"),
                                  details={"mode_reason": getattr(mode_result, "reason", "unknown"),
                                           "missing": list(missing)})

        # A reduced live quantity is not proof of a TP1 fill. Only promote TP1
        # when the exchange shows a filled bot TP1 reduce order (or the journal
        # already recorded tp1_hit before the crash). Otherwise recovery stays in
        # Hold so a manual/external close is never relabelled as TP1.
        #
        # The close-order ids are read from the PERSISTED journal row (not from the
        # live STATE), because STATE may not yet hold them at this point in the
        # recovery lifecycle. `position_mode` is passed explicitly for the same
        # reason.
        saved_state = row.get("state") if isinstance(row.get("state"), dict) else {}
        executed_for_tp1 = safe_float(row.get("actual_filled_qty"),
                                      safe_float(saved_state.get("actual_filled_qty")))
        tp1_verified = False
        recorded_tp1 = bool(saved_state.get("tp1_hit")) or bool(row.get("tp1_hit"))
        if not recorded_tp1 and executed_for_tp1 > 0 and snap.qty < executed_for_tp1 * 0.995:
            tp1_verified = _tp1_fill_verified_on_exchange(
                symbol, snap.side, expected_fill_qty=executed_for_tp1 * 0.5,
                position_side=position_side, position_mode=position_mode,
                close_order_ids=_collect_tp1_close_order_ids(saved_state, row),
                trade_entry_ts=safe_float(saved_state.get("entry_time"), row.get("entry_ts")))

        candidate, missing = build_restored_management_state(
            row, snap, position_mode=position_mode, position_side=position_side,
            tp1_fill_verified=tp1_verified)
        STATE.update(candidate)
        STATE.update({"margin": snap.margin, "mark_price": snap.mark_price,
                      "unrealized_pnl_usdt": snap.unrealized_pnl, "roe_pct": snap.roe_pct,
                      "leverage": snap.leverage, "liquidation_price": snap.liquidation_price,
                      "exchange_state": "OPEN", "current_symbol": symbol,
                      "position_mode": position_mode, "position_side": position_side})
        TRADE_STATE.update({"in_position": True, "symbol": symbol, "side": snap.side,
                            "entry": snap.entry_price, "qty": snap.qty,
                            "tp1_hit": bool(STATE.get("tp1_hit")),
                            "tp2_hit": bool(STATE.get("tp2_hit")),
                            "trail_on": bool(STATE.get("trail_activated")),
                            "last_update_ts": time.time()})

        if missing:
            return _recovery_hold("management_fields_missing:" + ",".join(missing),
                                  symbol=symbol, trade_id=row.get("trade_id"),
                                  details={"missing_fields": list(missing),
                                           "position_mode": position_mode,
                                           "position_side": position_side})

        original_native_sl = safe_float(STATE.get("native_sl_original_price"))
        saved_for_stop = row.get("state") if isinstance(row.get("state"), dict) else {}
        stop_prices = _journal_native_stop_prices(saved_for_stop, primary=original_native_sl)
        if not stop_prices:
            return _recovery_hold("persisted_native_sl_price_missing", symbol=symbol,
                                  trade_id=row.get("trade_id"))
        protection_status, protection_id, matched_price = verify_native_stop_loss_detailed(
            symbol, snap.side, stop_prices[0], snap.qty, position_side=position_side,
            trigger_prices=stop_prices[1:])
        STATE["native_sl_status"] = protection_status
        STATE["native_sl_verified"] = protection_status == "VERIFIED"
        if protection_id:
            STATE["native_sl_order_id"] = protection_id
        if matched_price:
            STATE["native_sl_last_verified_price"] = matched_price
        if protection_status == "UNKNOWN":
            # Query failure is never a MISSING stop and never a reason to place one.
            return _recovery_hold("native_sl_unknown", symbol=symbol, trade_id=row.get("trade_id"),
                                  details={"expected_sl_levels": stop_prices,
                                           "exchange_qty": snap.qty,
                                           "position_mode": position_mode,
                                           "position_side": position_side})
        if protection_status != "VERIFIED":
            repaired, repair_reason, repair_order_id = _repair_native_sl_from_journal(
                symbol, snap.side, snap.qty, prices=stop_prices, mode=position_mode,
                position_side=position_side, trade_id=row.get("trade_id"))
            if not repaired:
                return _recovery_hold("native_sl_" + str(protection_status).lower(),
                                      symbol=symbol, trade_id=row.get("trade_id"),
                                      details={"expected_sl_levels": stop_prices,
                                               "exchange_qty": snap.qty,
                                               "position_mode": position_mode,
                                               "position_side": position_side,
                                               "repair_reason": repair_reason,
                                               "repair_order_id": repair_order_id})
        else:
            STATE["native_sl_status"] = "VERIFIED"

        # Persist the recovered, exchange-confirmed state before unlocking management.
        STATE["recovery_hold"] = False
        STATE["recovery_reason"] = None
        STATE["native_sl_verified"] = True
        STATE["native_sl_status"] = "VERIFIED"
        STATE["recovery_status"] = "RESTORED_VERIFIED"
        journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
        if journal:
            journal.update(str(STATE["trade_id"]), status="OPEN", native_sl_order_id=STATE.get("native_sl_order_id"),
                           native_sl_verified=1, native_sl_status="VERIFIED",
                           position_mode=position_mode, position_side=position_side,
                           recovery_status="RESTORED_VERIFIED")
            journal.save_snapshot(str(STATE["trade_id"]), _journal_snapshot_state(),
                                  event_key=f"recovery-restored:{int(time.time())}",
                                  event_type="RECOVERY_RESTORED_VERIFIED")
        _live_manager.start_trade(symbol, snap.side, snap.entry_price, snap.qty,
                                  safe_float(STATE.get("synthetic_sl")),
                                  safe_float(STATE.get("tp1_price")), safe_float(STATE.get("tp2_price")))
        _live_manager.lifecycle_state = TradeLifecycleState.LIVE
        DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.LIVE.value
        DASHBOARD_STATE["live_trade_mode"] = True
        MEMORY["position_status"] = "OPEN"
        MEMORY["recovery_status"] = "RESTORED_VERIFIED"
        MEMORY["current_position"] = {"symbol": symbol, "side": snap.side,
                                       "qty": snap.qty, "entry": snap.entry_price}
        update_position_dashboard(symbol, snap.side, snap.entry_price, snap.qty)
        log_execution(f"[RECOVERY_RESTORED] trade={STATE.get('trade_id')} {symbol} {snap.side} "
                      f"mode={position_mode}/{position_side} original_filled={STATE.get('actual_filled_qty')} "
                      f"remaining={snap.qty} NativeSL=VERIFIED", "SUCCESS")
        return True
    except Exception as exc:
        return _recovery_hold(f"restore_exception:{type(exc).__name__}:{exc}",
                              symbol=str(_position_symbol(position) or "UNKNOWN"),
                              trade_id=(journal_row or {}).get("trade_id"))


_RECOVERY_RETRY_LOCK = threading.RLock()
_RECOVERY_RETRY_INTERVAL = max(10.0, safe_float(os.getenv("RECOVERY_RETRY_INTERVAL_SEC"), 20.0))

def _track_unmatched_exchange_exposure(snap, reason):
    """Expose live exchange facts without inventing original qty, SL or TP values."""
    STATE.update({"open": True, "current_symbol": snap.symbol, "side": snap.side,
                  "entry": snap.entry_price, "qty": 0.0, "initial_qty": 0.0,
                  "actual_filled_qty": 0.0, "remaining_qty": snap.qty,
                  "exchange_qty": snap.qty, "mark_price": snap.mark_price,
                  "roe_pct": snap.roe_pct, "margin": snap.margin,
                  "unrealized_pnl_usdt": snap.unrealized_pnl,
                  "leverage": snap.leverage, "exchange_state": "OPEN",
                  "recovery_hold": True, "recovery_reason": str(reason),
                  "synthetic_sl": 0.0, "sl": 0.0, "tp1_price": 0.0,
                  "synthetic_tp1": 0.0, "tp2_price": 0.0})
    TRADE_STATE.update({"in_position": True, "symbol": snap.symbol, "side": snap.side,
                        "entry": snap.entry_price, "qty": snap.qty})
    MEMORY["position_status"] = "OPEN_RECOVERY_PENDING"
    update_position_dashboard(snap.symbol, snap.side, snap.entry_price, snap.qty)


def _attempt_runtime_trade_recovery(symbol, position=None, *, force=False):
    """Re-attempt journal matching while exposure remains open; never clear Hold blindly."""
    global RECOVERY_BLOCK_ENTRIES
    if PAPER_MODE:
        return False
    now = time.time()
    with _RECOVERY_RETRY_LOCK:
        last = safe_float(STATE.get("recovery_last_attempt_ts"), 0.0)
        if not force and now - last < _RECOVERY_RETRY_INTERVAL:
            return False
        STATE["recovery_last_attempt_ts"] = now
    if position is None:
        status, found = fetch_position_tri_state(symbol)
        if status != "OPEN" or not isinstance(found, dict):
            return _recovery_hold("exchange_position_not_confirmed_for_recovery", symbol=symbol,
                                  details={"exchange_status": status})
        position = found
    journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
    if journal is None:
        try:
            snap = _snapshot_from_position(str(_position_symbol(position) or symbol), position)
            _track_unmatched_exchange_exposure(snap, "trade_journal_unavailable")
        except Exception:
            pass
        return _recovery_hold("trade_journal_unavailable", symbol=symbol)
    try:
        snap = _snapshot_from_position(str(_position_symbol(position) or symbol), position)
        rows = list(journal.open_trades())
        matches = [row for row in rows
                   if roro_canonical_symbol(row.get("symbol")) == roro_canonical_symbol(snap.symbol)
                   and str(row.get("side") or "").upper() == snap.side]
        if len(matches) != 1:
            reason = "journal_row_not_found" if not matches else "multiple_journal_rows_match_exposure"
            _track_unmatched_exchange_exposure(snap, reason)
            return _recovery_hold(reason, symbol=snap.symbol,
                                  details={"candidate_count": len(matches),
                                           "exchange_side": snap.side,
                                           "open_journal_rows": len(rows)})
        restored = _restore_trade_from_journal(position, matches[0])
        RECOVERY_BLOCK_ENTRIES = not restored
        return restored
    except Exception as exc:
        RECOVERY_BLOCK_ENTRIES = True
        return _recovery_hold(f"runtime_recovery_exception:{type(exc).__name__}:{exc}", symbol=symbol)


def _portfolio_reserve_recovery_slot(trade_id, symbol, state_hint):
    """Reserve a durable slot for a recovered position using trusted hints only."""
    allocator = globals().get("PORTFOLIO_ALLOCATOR")
    if allocator is None:
        return None
    try:
        hint = _portfolio_trusted_hint(state_hint)
        result = allocator.reserve(
            normalize_symbol(symbol), str(trade_id), class_hint=hint,
            metadata={"source": "STARTUP_RECOVERY"},
        )
    except Exception as exc:
        log_execution(f"[RECOVERY] slot reserve failed for {trade_id}: {exc}", "ERROR")
        return None
    return result if result.ok else None


def _portfolio_restore_paper_rows(rows):
    """Restore multiple verified PAPER journal rows into isolated runtime slots."""
    global RECOVERY_BLOCK_ENTRIES
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if runtime is None or len(rows) > portfolio_allocator.TOTAL_CAPACITY:
        RECOVERY_BLOCK_ENTRIES = True
        return False
    restored_ids = []
    reservations = []
    try:
        for row in rows:
            saved = row.get("state") if isinstance(row.get("state"), dict) else {}
            trade_id = str(saved.get("trade_id") or row.get("trade_id") or "")
            symbol = str(saved.get("current_symbol") or row.get("symbol") or "")
            side = str(saved.get("side") or row.get("side") or "").upper()
            entry = safe_float(saved.get("entry"), safe_float(row.get("entry_price")))
            initial_qty = safe_float(saved.get("initial_qty"), safe_float(row.get("entry_qty")))
            remaining_qty = safe_float(saved.get("remaining_qty"), safe_float(row.get("remaining_qty"), initial_qty))
            sl = safe_float(saved.get("synthetic_sl"), safe_float(saved.get("sl"), safe_float(row.get("sl_price"))))
            tp1 = safe_float(saved.get("tp1_price"))
            tp2 = safe_float(saved.get("tp2_price"))
            if not (trade_id and symbol and side in {"BUY", "SELL"} and entry > 0
                    and initial_qty > 0 and remaining_qty > 0 and sl > 0 and tp1 > 0 and tp2 > 0):
                raise ValueError(f"invalid_paper_recovery_state:{trade_id}")
            reservation = _portfolio_reserve_recovery_slot(trade_id, symbol, saved)
            if reservation is None:
                raise ValueError(f"slot_unavailable:{trade_id}")
            reservations.append(trade_id)
            STATE.update(saved)
            STATE.update({
                "open": True, "trade_id": trade_id, "current_symbol": symbol, "side": side,
                "entry": entry, "qty": initial_qty, "initial_qty": initial_qty,
                "remaining_qty": remaining_qty, "exchange_qty": remaining_qty,
                "synthetic_sl": sl, "sl": sl, "tp1_price": tp1, "tp2_price": tp2,
                "exchange_state": "OPEN", "recovery_hold": False,
                "execution_unresolved": False, "close_reconciliation_required": False,
                "close_verified": False,
            })
            TRADE_STATE.update({
                "in_position": True, "symbol": symbol, "side": side, "entry": entry,
                "qty": remaining_qty, "tp1_hit": bool(saved.get("tp1_hit")),
                "tp2_hit": bool(saved.get("tp2_hit")),
                "trail_on": bool(saved.get("trail_activated")), "last_update_ts": time.time(),
            })
            _live_manager.start_trade(symbol, side, entry, remaining_qty, sl, tp1, tp2)
            adopt = runtime.adopt(
                trade_id, normalize_symbol(symbol),
                class_hint=_portfolio_trusted_hint(saved),
            )
            if not adopt.ok:
                raise ValueError(f"runtime_adopt_failed:{trade_id}:{adopt.reason}")
            restored_ids.append(trade_id)
        _live_manager.lifecycle_state = TradeLifecycleState.LIVE
        DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.LIVE.value
        MEMORY["position_status"] = "OPEN"
        MEMORY["recovery_status"] = "PAPER_PORTFOLIO_RESTORED_FROM_JOURNAL"
        RECOVERY_BLOCK_ENTRIES = False
        _recompute_portfolio_globals()
        _mark_persisted_recovery_resolved("OPEN_RESTORED_FROM_JOURNAL", "PAPER_PORTFOLIO_RESTORED_FROM_JOURNAL")
        log_execution(
            f"[PAPER_RECOVERY] Restored {len(restored_ids)} trades into isolated slots: {restored_ids}",
            "SUCCESS",
        )
        return True
    except Exception as exc:
        RECOVERY_BLOCK_ENTRIES = True
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "PAPER_PORTFOLIO_RECOVERY_FAILED"
        for trade_id in reservations:
            if trade_id not in restored_ids:
                _portfolio_release_reservation(trade_id, "", "RECOVERY_ADOPT_FAILED")
        _recompute_portfolio_globals()
        log_execution(f"[PAPER_RECOVERY] Multi-position restore failed: {exc}", "ERROR")
        return False


def _portfolio_restore_live_rows(rows, positions):
    """Restore matching multiple exchange positions into isolated runtime slots."""
    global RECOVERY_BLOCK_ENTRIES
    runtime = globals().get("PORTFOLIO_RUNTIME")
    if runtime is None or len(positions) > portfolio_allocator.TOTAL_CAPACITY:
        RECOVERY_BLOCK_ENTRIES = True
        return False
    expected = {roro_canonical_symbol(_position_symbol(pos)) for pos in positions}
    if len(expected) != len(positions):
        MEMORY["position_status"] = "MULTIPLE_OPEN"
        MEMORY["recovery_status"] = "DUPLICATE_EXCHANGE_SYMBOLS"
        RECOVERY_BLOCK_ENTRIES = True
        return False
    restored_ids = []
    reservations = []
    try:
        for position in positions:
            symbol = str(_position_symbol(position) or "")
            snap = _snapshot_from_position(symbol, position)
            key = roro_canonical_symbol(symbol)
            candidates = [
                row for row in rows
                if roro_canonical_symbol(row.get("symbol")) == key
                and str(row.get("side") or "").upper() == snap.side
            ]
            if len(candidates) != 1:
                raise ValueError(f"journal_match_ambiguous:{key}:{len(candidates)}")
            row = candidates[0]
            saved = row.get("state") if isinstance(row.get("state"), dict) else {}
            trade_id = str(row.get("trade_id") or saved.get("trade_id") or "")
            reservation = _portfolio_reserve_recovery_slot(trade_id, symbol, saved)
            if reservation is None:
                raise ValueError(f"slot_unavailable:{trade_id}")
            reservations.append(trade_id)
            if not _restore_trade_from_journal(position, row):
                raise ValueError(f"journal_restore_incomplete:{trade_id}")
            adopt = runtime.adopt(
                trade_id, normalize_symbol(symbol),
                class_hint=_portfolio_trusted_hint(saved or STATE),
            )
            if not adopt.ok:
                raise ValueError(f"runtime_adopt_failed:{trade_id}:{adopt.reason}")
            restored_ids.append(trade_id)
        _live_manager.lifecycle_state = TradeLifecycleState.LIVE
        DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.LIVE.value
        MEMORY["position_status"] = "OPEN"
        MEMORY["recovery_status"] = "LIVE_PORTFOLIO_RESTORED_FROM_JOURNAL"
        RECOVERY_BLOCK_ENTRIES = False
        _recompute_portfolio_globals()
        _mark_persisted_recovery_resolved("OPEN_RESTORED_FROM_JOURNAL", "LIVE_PORTFOLIO_RESTORED_FROM_JOURNAL")
        log_execution(
            f"[RECOVERY] Restored {len(restored_ids)} exchange positions into isolated slots",
            "SUCCESS",
        )
        return True
    except Exception as exc:
        RECOVERY_BLOCK_ENTRIES = True
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "LIVE_PORTFOLIO_RECOVERY_FAILED"
        for trade_id in reservations:
            if trade_id not in restored_ids:
                _portfolio_release_reservation(trade_id, "", "RECOVERY_ADOPT_FAILED")
        _recompute_portfolio_globals()
        log_execution(f"[RECOVERY] Multi-position restore failed: {exc}", "ERROR")
        return False


def _startup_flat_orders_reconciled(rows):
    """Require order-level terminal proof before releasing a FLAT startup hold."""
    terminal_statuses = {"CLOSED", "FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
    checked = set()

    def verify_entry(symbol, order_id, client_order_id, already_terminal=False):
        key = ("ENTRY", str(order_id or ""), str(client_order_id or ""))
        if already_terminal:
            return True, "PERSISTED_TERMINAL"
        if key in checked:
            return True, "ALREADY_VERIFIED"
        checked.add(key)
        if not (order_id or client_order_id):
            return False, "ENTRY_ORDER_ID_UNAVAILABLE"
        return _cancel_and_verify_entry_order_terminal(
            symbol, order_id, client_order_id, cancel_if_open=True
        )

    def verify_close(symbol, order_id):
        key = ("CLOSE", str(order_id or ""))
        if key in checked:
            return True, "ALREADY_VERIFIED"
        checked.add(key)
        fetcher = getattr(ex, "fetch_order", None)
        if not order_id or not callable(fetcher):
            return False, "CLOSE_ORDER_LOOKUP_UNAVAILABLE"
        try:
            order = safe_api_call(fetcher, str(order_id), normalize_symbol(symbol),
                                  _api_max_attempts=1)
        except Exception as exc:
            return False, f"CLOSE_ORDER_FETCH_FAILED:{type(exc).__name__}"
        if not isinstance(order, dict):
            return False, "CLOSE_ORDER_NOT_FOUND"
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        status = str(order.get("status") or info.get("status")
                     or info.get("orderStatus") or "UNKNOWN").upper()
        return (status in terminal_statuses, status)

    durable_entry_hold = any(STATE.get(key) for key in (
        "entry_context_persistence_failed",
        "cross_event_persistence_failed",
        "entry_event_persistence_failed",
    ))
    if durable_entry_hold:
        return False, "DURABLE_ENTRY_PERSISTENCE_HOLD"
    symbol = str(STATE.get("current_symbol") or DEFAULT_SYMBOL)
    entry_id = STATE.get("entry_exchange_order_id")
    client_id = STATE.get("entry_client_order_id")
    entry_terminal = bool(STATE.get("entry_order_terminal"))
    pending_entry_states = {
        "ENTRY_PENDING", "EXECUTION_UNCERTAIN", "ENTRY_PRICE_UNKNOWN",
        "PARTIAL_FILL", "PROTECTION_HOLD",
    }
    entry_state = str(STATE.get("entry_execution_state") or "").upper()
    close_id = STATE.get("pending_close_order_id")
    # execution_unresolved is shared by entry and close paths. An explicitly
    # pending close must not be misclassified as an entry missing its order ID.
    # A separately persisted pending-entry state still requires entry identity.
    entry_execution_unresolved = bool(
        STATE.get("execution_unresolved")
        and not (STATE.get("close_reconciliation_required") or close_id)
    )
    if ((entry_execution_unresolved or entry_state in pending_entry_states)
            and not (entry_id or client_id) and not entry_terminal):
        return False, "ENTRY_OUTCOME_UNRESOLVED_WITHOUT_ORDER_ID"
    if entry_id or client_id:
        ok, reason = verify_entry(symbol, entry_id, client_id, entry_terminal)
        if not ok:
            return False, f"ENTRY_ORDER:{reason}"

    if STATE.get("close_reconciliation_required") and not close_id:
        return False, "CLOSE_OUTCOME_UNRESOLVED_WITHOUT_ORDER_ID"
    if close_id:
        ok, reason = verify_close(symbol, close_id)
        if not ok:
            return False, f"CLOSE_ORDER:{reason}"

    for row in rows or []:
        saved = row.get("state") if isinstance(row.get("state"), dict) else {}
        row_symbol = str(row.get("symbol") or saved.get("current_symbol") or symbol)
        row_entry_id = (saved.get("entry_exchange_order_id")
                        or row.get("entry_exchange_order_id")
                        or row.get("exchange_order_id"))
        row_client_id = (saved.get("entry_client_order_id")
                         or row.get("entry_client_order_id")
                         or row.get("client_order_id"))
        row_entry_terminal = bool(saved.get("entry_order_terminal"))
        row_entry_state = str(saved.get("entry_execution_state") or row.get("status") or "").upper()
        row_close_id = (saved.get("pending_close_order_id")
                        or row.get("pending_close_order_id"))
        row_entry_execution_unresolved = bool(
            saved.get("execution_unresolved")
            and not (saved.get("close_reconciliation_required") or row_close_id)
        )
        if ((row_entry_execution_unresolved or row_entry_state in pending_entry_states)
                and not (row_entry_id or row_client_id) and not row_entry_terminal):
            return False, "JOURNAL_ENTRY_OUTCOME_UNRESOLVED_WITHOUT_ORDER_ID"
        if row_entry_id or row_client_id:
            ok, reason = verify_entry(row_symbol, row_entry_id, row_client_id,
                                      row_entry_terminal)
            if not ok:
                return False, f"JOURNAL_ENTRY_ORDER:{reason}"
        if saved.get("close_reconciliation_required") and not row_close_id:
            return False, "JOURNAL_CLOSE_OUTCOME_UNRESOLVED_WITHOUT_ORDER_ID"
        if row_close_id:
            ok, reason = verify_close(row_symbol, row_close_id)
            if not ok:
                return False, f"JOURNAL_CLOSE_ORDER:{reason}"
    return True, "ALL_KNOWN_ORDERS_TERMINAL"


def startup_trade_recovery():
    """Reconcile persisted journal with BingX before any fresh entry is permitted."""
    global RECOVERY_BLOCK_ENTRIES
    if _persisted_recovery_hold_active():
        # A prior process refused exit because exposure was unverified. Keep the
        # inherited hold until this restart's engine verification clears it.
        RECOVERY_BLOCK_ENTRIES = True
        STATE["recovery_hold"] = True
        STATE["recovery_reason"] = "PERSISTED_RECOVERY_HOLD"
        MEMORY["recovery_status"] = "PERSISTED_RECOVERY_HOLD"
    if PAPER_MODE:
        rows = list(INSTITUTIONAL_TRADE_JOURNAL.open_trades()) if INSTITUTIONAL_TRADE_JOURNAL else []
        if not rows:
            if _persisted_recovery_hold_active():
                log_execution("[PAPER_RECOVERY] Persisted unverified recovery hold; entries remain blocked", "ERROR")
                return False
            paper["position"] = None
            MEMORY["position_status"] = "FLAT"
            MEMORY["recovery_status"] = "PAPER_RECONCILED_FLAT"
            RECOVERY_BLOCK_ENTRIES = False
            _mark_persisted_recovery_resolved("FLAT", "PAPER_RECONCILED_FLAT")
            return True
        if len(rows) > 1:
            return _portfolio_restore_paper_rows(rows)
        row = rows[0]
        saved = row.get("state") if isinstance(row.get("state"), dict) else {}
        if not saved:
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["recovery_status"] = "PAPER_JOURNAL_STATE_MISSING"
            RECOVERY_BLOCK_ENTRIES = True
            log_execution("[PAPER_RECOVERY] Open journal row has no state snapshot; entries blocked", "ERROR")
            return False
        STATE.update(saved)
        symbol = str(saved.get("current_symbol") or row.get("symbol") or "")
        saved_remaining = safe_float(saved.get("remaining_qty"), safe_float(row.get("remaining_qty")))
        if bool(saved.get("close_verified")) and saved_remaining <= max(safe_float(saved.get("initial_qty"), 0.0) * 0.002, 1e-10):
            # A crash may occur after a verified PAPER full close but before the
            # journal finalizer commits. Finish that one-time finalization on startup.
            STATE.update({"open": True, "current_symbol": symbol,
                          "trade_id": str(saved.get("trade_id") or row.get("trade_id") or ""),
                          "remaining_qty": 0.0, "close_verified": True})
            finalize_trade_with_reality(symbol, close_reason=str(saved.get("last_close_reason") or "PAPER_CLOSE_RECOVERED"))
            paper["position"] = None
            MEMORY["position_status"] = "FLAT"
            MEMORY["recovery_status"] = "PAPER_CLOSED_TRADE_FINALIZED_AFTER_RESTART"
            RECOVERY_BLOCK_ENTRIES = False
            _mark_persisted_recovery_resolved("FLAT", "PAPER_CLOSED_TRADE_FINALIZED_AFTER_RESTART")
            log_execution(f"[PAPER_RECOVERY] Finalized verified pre-restart close for {symbol}; no duplicate close action", "SUCCESS")
            return True
        side = str(saved.get("side") or row.get("side") or "").upper()
        entry = safe_float(saved.get("entry"), safe_float(row.get("entry_price")))
        initial_qty = safe_float(saved.get("initial_qty"), safe_float(row.get("entry_qty")))
        remaining_qty = safe_float(saved.get("remaining_qty"), safe_float(row.get("remaining_qty"), initial_qty))
        sl = safe_float(saved.get("synthetic_sl"), safe_float(saved.get("sl"), safe_float(row.get("sl_price"))))
        tp1 = safe_float(saved.get("tp1_price"))
        tp2 = safe_float(saved.get("tp2_price"))
        trade_id = str(saved.get("trade_id") or row.get("trade_id") or "")
        valid = bool(symbol and side in {"BUY", "SELL"} and entry > 0 and initial_qty > 0 and
                     remaining_qty > 0 and sl > 0 and tp1 > 0 and tp2 > 0 and trade_id)
        if not valid:
            STATE.update({"open": True, "current_symbol": symbol or row.get("symbol"),
                          "side": side or row.get("side"), "entry": entry,
                          "qty": initial_qty, "initial_qty": initial_qty,
                          "remaining_qty": remaining_qty, "recovery_hold": True,
                          "execution_unresolved": True})
            MEMORY["position_status"] = "OPEN_RECOVERY_PENDING"
            MEMORY["recovery_status"] = "PAPER_JOURNAL_CRITICAL_FIELDS_MISSING"
            RECOVERY_BLOCK_ENTRIES = True
            log_execution("[PAPER_RECOVERY] Critical trade-management fields missing; preserving state and blocking new entries", "ERROR")
            return False
        STATE.update({"open": True, "trade_id": trade_id, "current_symbol": symbol,
                      "side": side, "entry": entry, "qty": initial_qty,
                      "initial_qty": initial_qty, "remaining_qty": remaining_qty,
                      "exchange_qty": remaining_qty, "synthetic_sl": sl, "sl": sl,
                      "tp1_price": tp1, "tp2_price": tp2, "exchange_state": "OPEN",
                      "recovery_hold": False, "execution_unresolved": False,
                      "close_reconciliation_required": False, "close_verified": False})
        paper["position"] = {"side": side, "entry": entry, "qty": initial_qty,
                             "remaining_qty": remaining_qty, "recovered": True, "trade_id": trade_id}
        TRADE_STATE.update({"in_position": True, "symbol": symbol, "side": side,
                            "entry": entry, "qty": remaining_qty,
                            "tp1_hit": bool(STATE.get("tp1_hit")),
                            "tp2_hit": bool(STATE.get("tp2_hit")),
                            "trail_on": bool(STATE.get("trail_activated")),
                            "last_update_ts": time.time()})
        _live_manager.start_trade(symbol, side, entry, remaining_qty, sl, tp1, tp2)
        _live_manager.lifecycle_state = TradeLifecycleState.LIVE
        DASHBOARD_STATE["lifecycle_state"] = TradeLifecycleState.LIVE.value
        DASHBOARD_STATE["live_trade_mode"] = True
        MEMORY["position_status"] = "OPEN"
        MEMORY["current_position"] = {"symbol": symbol, "side": side, "qty": remaining_qty, "entry": entry}
        MEMORY["recovery_status"] = "PAPER_TRADE_RESTORED_FROM_JOURNAL"
        update_position_dashboard(symbol, side, entry, remaining_qty)
        RECOVERY_BLOCK_ENTRIES = False
        _mark_persisted_recovery_resolved("OPEN_RESTORED_FROM_JOURNAL", "PAPER_TRADE_RESTORED_FROM_JOURNAL")
        log_execution(f"[PAPER_RECOVERY] Restored trade={trade_id} {symbol} {side} remaining={remaining_qty:.10f} TP1={STATE.get('tp1_hit')} TP2={STATE.get('tp2_hit')} trail={STATE.get('trail_activated')}", "SUCCESS")
        return True
    RECOVERY_BLOCK_ENTRIES = True
    MEMORY["recovery_status"] = "CHECKING_EXCHANGE"
    status, positions = fetch_position_tri_state(None)
    if status == "UNKNOWN":
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "EXCHANGE_UNAVAILABLE"
        log_execution("[RECOVERY] BingX position state UNKNOWN; entries blocked until reconciliation succeeds", "ERROR")
        return False
    rows = list(INSTITUTIONAL_TRADE_JOURNAL.open_trades()) if INSTITUTIONAL_TRADE_JOURNAL else []
    positions = positions or []
    if status == "FLAT":
        orders_reconciled, order_reason = _startup_flat_orders_reconciled(rows)
        if not orders_reconciled:
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["recovery_status"] = "ORDER_OUTCOME_REQUIRES_RECONCILIATION"
            STATE["recovery_hold"] = True
            STATE["recovery_reason"] = str(order_reason)
            RECOVERY_BLOCK_ENTRIES = True
            log_execution(
                f"[RECOVERY] BingX is FLAT but order-level reconciliation is incomplete: {order_reason}; entries remain blocked",
                "CRITICAL", debounce_key="startup_flat_order_unresolved", debounce_sec=30,
            )
            return False
        for row in rows:
            try:
                saved = row.get("state") if isinstance(row.get("state"), dict) else {}
                diagnostic = _fetch_external_close_diagnostic(
                    row.get("symbol"), row.get("side"), row.get("entry_ts"),
                    row.get("native_sl_order_id") or saved.get("native_sl_order_id"),
                    row.get("sl_price") or saved.get("native_sl_original_price") or saved.get("sl"),
                    saved.get("bot_close_order_ids", []))
                diagnostic["detected_at"] = time.time()
                diagnostic["symbol"] = row.get("symbol")
                MEMORY["last_close_diagnostic"] = diagnostic
                trade_id = str(row.get("trade_id"))
                INSTITUTIONAL_TRADE_JOURNAL.finalize_once(
                    trade_id, diagnostic.get("reason", "EXTERNAL_CLOSE_UNKNOWN"),
                    diagnostic.get("close_price"), diagnostic.get("realized_pnl"),
                    confidence=str(diagnostic.get("confidence", "UNKNOWN")))
                INSTITUTIONAL_TRADE_JOURNAL.event(
                    trade_id, f"external-close-diagnostic:{int(diagnostic['detected_at'])}",
                    "EXTERNAL_CLOSE_DIAGNOSTIC", diagnostic)
                if diagnostic.get("reason") == "NATIVE_STOP_LOSS":
                    send_once(f"🛑 <b>CONFIRMED NATIVE STOP LOSS</b> on {row.get('symbol')}\nDetected during startup recovery; order={diagnostic.get('order_id')}",
                              f"recovery_sl_{row.get('symbol')}_{diagnostic.get('order_id')}", 30)
                    log_execution(f"[RECOVERY_STOP_DIAG] confirmed stop-loss after restart: {row.get('symbol')} order={diagnostic.get('order_id')}", "WARN")
                else:
                    log_execution(f"[RECOVERY_CLOSE_DIAG] {row.get('symbol')} reason={diagnostic.get('reason')} confidence={diagnostic.get('confidence')} evidence={diagnostic.get('evidence')}", "WARN")
            except Exception as exc:
                log_execution(f"[RECOVERY] close diagnosis/finalization pending: {exc}", "WARN")
        # Clear stale flags only after FLAT and order-level reconciliation.
        STATE.update({
            "open": False, "execution_unresolved": False,
            "close_reconciliation_required": False, "recovery_hold": False,
            "recovery_reason": None, "pending_close_order_id": None,
            "pending_close_reason": None, "pending_close_before_qty": 0.0,
            "pending_close_requested_qty": 0.0,
            "pending_close_target_remaining_qty": None,
            "pending_close_started_at": 0.0,
            "entry_execution_state": "RECONCILED_FLAT",
            "entry_client_order_id": None, "entry_exchange_order_id": None,
            "entry_order_terminal": False,
        })
        TRADE_STATE["in_position"] = False
        TRADE_STATE["qty"] = 0.0
        MEMORY["position_status"] = "FLAT"
        MEMORY["current_position"] = None
        MEMORY["recovery_status"] = "RECONCILED_FLAT"
        RECOVERY_BLOCK_ENTRIES = False
        if not _mark_persisted_recovery_resolved("FLAT", "RECONCILED_FLAT"):
            STATE["recovery_hold"] = True
            STATE["recovery_reason"] = "PERSISTED_RECOVERY_RESOLUTION_FAILED"
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["recovery_status"] = "RECOVERY_STATE_PERSISTENCE_FAILED"
            RECOVERY_BLOCK_ENTRIES = True
            return False
        log_execution(
            "[RECOVERY] Startup FLAT reconciled; stale recovery flags cleared after order-level verification",
            "SUCCESS",
        )
        return True
    if len(positions) > 1:
        return _portfolio_restore_live_rows(rows, positions)
    if not positions:
        MEMORY["position_status"] = "MULTIPLE_OPEN"
        MEMORY["recovery_status"] = "MANUAL_RECONCILIATION_REQUIRED"
        log_execution("[RECOVERY] Exchange status inconsistent (OPEN with no positions); entries blocked.", "ERROR")
        return False
    position = positions[0]
    symbol_key = roro_canonical_symbol(_position_symbol(position))
    exchange_snap = _snapshot_from_position(_position_symbol(position), position)
    candidates = [row for row in rows
                  if roro_canonical_symbol(row.get("symbol")) == symbol_key
                  and str(row.get("side") or "").upper() == exchange_snap.side]
    matched = candidates[0] if len(candidates) == 1 else None
    if matched:
        critical = _restore_trade_from_journal(position, matched)
        RECOVERY_BLOCK_ENTRIES = not critical
        if not critical:
            log_execution("[RECOVERY] Position restored but TP/SL state incomplete; new entries blocked and manual review required", "ERROR")
        else:
            log_execution(f"[RECOVERY] Restored {symbol_key} {STATE.get('side')} and management stages from SQLite", "SUCCESS")
            _mark_persisted_recovery_resolved("OPEN_RESTORED_FROM_JOURNAL", "LIVE_TRADE_RESTORED_FROM_JOURNAL")
        return critical
    # Exchange exposure exists but no unique durable trade state. Track exposure and stop all entries.
    if len(candidates) > 1:
        _recovery_hold("multiple_journal_rows_match_exposure", symbol=symbol_key,
                       details={"candidate_count": len(candidates)})
    try:
        snap = _snapshot_from_position(_position_symbol(position), position)
        STATE.update({"open": True, "current_symbol": snap.symbol, "side": snap.side,
                      "entry": snap.entry_price, "qty": snap.qty, "remaining_qty": snap.qty,
                      "mark_price": snap.mark_price, "roe_pct": snap.roe_pct,
                      "exchange_state": "OPEN", "recovery_hold": True,
                      "recovery_reason": "journal_row_not_found" if not candidates else "multiple_journal_rows_match_exposure",
                      "qty": 0.0, "initial_qty": 0.0, "actual_filled_qty": 0.0,
                      "synthetic_sl": 0.0, "tp1_price": 0.0, "tp2_price": 0.0})
        TRADE_STATE.update({"in_position": True, "symbol": snap.symbol, "side": snap.side,
                            "entry": snap.entry_price, "qty": snap.qty})
        MEMORY["position_status"] = "OPEN"
        MEMORY["recovery_status"] = "OPEN_POSITION_WITHOUT_JOURNAL"
        update_position_dashboard(snap.symbol, snap.side, snap.entry_price, snap.qty)
    except Exception as exc:
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "RECOVERY_PARSE_ERROR"
        log_execution(f"[RECOVERY] Could not reconstruct exchange position safely: {exc}", "ERROR")
    RECOVERY_BLOCK_ENTRIES = True
    log_execution("[RECOVERY] Open BingX position has no matching journal. Tracking exposure only; entries blocked pending manual review.", "ERROR")
    return False


# Replace ambiguous legacy position reads with an explicit tri-state status.
def fetch_position(symbol):
    status, pos = fetch_position_tri_state(symbol)
    return pos if status == "OPEN" else None


def _exchange_fetch_live_snapshot(self, symbol):
    if PAPER_MODE:
        self.last_fetch_status = "OPEN" if STATE.get("open") else "FLAT"
        return self._paper_snapshot(symbol)
    status, pos = fetch_position_tri_state(symbol)
    self.last_fetch_status = status
    if status != "OPEN":
        return None
    try:
        snap = _snapshot_from_position(symbol, pos)
        self._last_snapshot = snap
        return snap
    except Exception as exc:
        self.last_fetch_status = "UNKNOWN"
        log_execution(f"[SYNC] invalid exchange position snapshot: {exc}", "ERROR")
        return None


def _exchange_reconcile(self, symbol, local_state, *, force=False):
    """Tri-state reconciliation; force bypasses throttle, never bypasses safety."""
    global RECOVERY_BLOCK_ENTRIES
    now = time.time()
    lock = getattr(self, "_reconcile_lock", None)
    if lock is None:
        lock = threading.Lock()
        self._reconcile_lock = lock
    if not lock.acquire(blocking=False):
        return "BUSY"
    try:
        if not force and now - self._last_reconcile < 10:
            return "THROTTLED"
        self._last_reconcile = now
        try:
            snap = self.fetch_live_snapshot(symbol)
        except Exception as exc:
            status = "UNKNOWN"
            log_execution(
                f"[RECONCILIATION] {symbol} snapshot error; state UNKNOWN: {type(exc).__name__}: {exc}",
                "ERROR", debounce_key=f"exchange_reconcile_error_{symbol}", debounce_sec=30
            )
            snap = None
        else:
            status = getattr(self, "last_fetch_status", "UNKNOWN")

        if status == "UNKNOWN":
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["recovery_status"] = "EXCHANGE_STATE_UNKNOWN"
            if local_state.get("open"):
                STATE["recovery_hold"] = True
                STATE["recovery_reason"] = "EXCHANGE_POSITION_STATE_UNKNOWN"
                RECOVERY_BLOCK_ENTRIES = True
            log_execution(
                f"[RECONCILIATION] {symbol} state UNKNOWN; preserving local exposure; no close/finalize action",
                "ERROR", debounce_key=f"exchange_reconcile_unknown_{symbol}", debounce_sec=30
            )
            return "UNKNOWN"

        if status == "FLAT":
            if local_state.get("open"):
                _clear_confirmed_flat(symbol)
            MEMORY["position_status"] = "FLAT"
            return "FLAT"

        if status != "OPEN" or snap is None:
            MEMORY["position_status"] = "UNKNOWN"
            if local_state.get("open"):
                STATE["recovery_hold"] = True
                STATE["recovery_reason"] = "INVALID_EXCHANGE_SNAPSHOT"
                RECOVERY_BLOCK_ENTRIES = True
            return "UNKNOWN"

        if STATE.get("pending_close_order_id"):
            _resolve_pending_close_from_snapshot(symbol, snap)
        _apply_open_snapshot(symbol, snap)
        self._last_snapshot = snap
        self.event_bus.emit("reconciled", snap)
        return "OPEN"
    finally:
        lock.release()


ExchangeSyncService.fetch_live_snapshot = _exchange_fetch_live_snapshot
ExchangeSyncService.reconcile = _exchange_reconcile


_original_sync_position_state = sync_position_state
def sync_position_state(symbol=None):
    if PAPER_MODE:
        return _original_sync_position_state(symbol)
    if not symbol:
        symbol = STATE.get("current_symbol") if STATE.get("open") else TRADE_STATE.get("symbol")
    if not symbol:
        return None, None, None, None
    snap = _exchange_sync.fetch_live_snapshot(symbol)
    status = getattr(_exchange_sync, "last_fetch_status", "UNKNOWN")
    if status == "UNKNOWN":
        MEMORY["position_status"] = "UNKNOWN"
        log_execution(f"[POS_SYNC] {symbol} status UNKNOWN; local state retained", "WARN")
        return None, None, None, None
    if status == "FLAT":
        if STATE.get("open"):
            _clear_confirmed_flat(symbol)
        else:
            MEMORY["position_status"] = "FLAT"
            MEMORY["current_position"] = None
        return None, None, None, None
    if snap is None:
        MEMORY["position_status"] = "UNKNOWN"
        return None, None, None, None
    if STATE.get("pending_close_order_id"):
        _resolve_pending_close_from_snapshot(symbol, snap)
    _apply_open_snapshot(symbol, snap)
    if STATE.get("recovery_hold") or not STATE.get("trade_id"):
        _attempt_runtime_trade_recovery(symbol, position=None, force=False)
    if not STATE.get("recovery_hold") and not STATE.get("execution_unresolved"):
        _persist_trade_journal_snapshot()
    return snap.mark_price, snap.unrealized_pnl, snap.margin, snap.roe_pct



_RUNTIME_FLAT_RECOVERY_RECHECK_SEC = 10.0
_RUNTIME_FLAT_RECOVERY_LOCK = threading.Lock()
_RUNTIME_FLAT_RECOVERY_CACHE = {
    "checked_at": 0.0, "fingerprint": None, "ok": False, "reason": "NOT_CHECKED",
}


def _verify_runtime_flat_recovery_orders():
    """Reuse startup's order-level proof before releasing a runtime FLAT recovery hold.

    A stale recovery_hold flag is not, by itself, proof of an in-flight order.
    Conversely, a FLAT position snapshot does not resolve a known pending order,
    missing order identity, or failed durable entry-event persistence.
    """
    global RECOVERY_BLOCK_ENTRIES
    fingerprint_keys = (
        "recovery_hold", "execution_unresolved", "close_reconciliation_required",
        "pending_close_order_id", "entry_exchange_order_id", "entry_client_order_id",
        "entry_order_terminal", "entry_execution_state",
        "entry_context_persistence_failed", "cross_event_persistence_failed",
        "entry_event_persistence_failed",
    )
    fingerprint = tuple((key, repr(STATE.get(key))) for key in fingerprint_keys)
    now = time.time()

    with _RUNTIME_FLAT_RECOVERY_LOCK:
        cached = _RUNTIME_FLAT_RECOVERY_CACHE
        if (cached.get("fingerprint") == fingerprint
                and now - safe_float(cached.get("checked_at"), 0.0) < _RUNTIME_FLAT_RECOVERY_RECHECK_SEC):
            return bool(cached.get("ok")), str(cached.get("reason") or "CACHED_RESULT")

        try:
            journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
            rows = list(journal.open_trades()) if journal is not None else []
            ok, reason = _startup_flat_orders_reconciled(rows)
        except Exception as exc:
            ok, reason = False, f"ORDER_RECONCILIATION_FAILED:{type(exc).__name__}"

        if ok and not _mark_persisted_recovery_resolved(
                "FLAT", "RUNTIME_RECONCILED_FLAT"):
            ok, reason = False, "RECOVERY_STATE_PERSISTENCE_FAILED"

        _RUNTIME_FLAT_RECOVERY_CACHE.update({
            "checked_at": now, "fingerprint": fingerprint,
            "ok": bool(ok), "reason": str(reason),
        })
        return bool(ok), str(reason)


def _apply_runtime_flat_reconciliation(*, clear_trade_state=False):
    """Commit the in-memory FLAT state only after order and recovery persistence proof."""
    STATE.update({
        "open": False,
        "execution_unresolved": False,
        "close_reconciliation_required": False,
        "recovery_hold": False,
        "recovery_reason": None,
        "pending_close_order_id": None,
        "pending_close_reason": None,
        "pending_close_before_qty": 0.0,
        "pending_close_requested_qty": 0.0,
        "pending_close_target_remaining_qty": None,
        "pending_close_started_at": 0.0,
        "entry_execution_state": "RECONCILED_FLAT",
        "entry_client_order_id": None,
        "entry_exchange_order_id": None,
        "entry_order_terminal": False,
    })
    if clear_trade_state:
        TRADE_STATE["in_position"] = False
        TRADE_STATE["qty"] = 0.0
    MEMORY["position_status"] = "FLAT"
    MEMORY["current_position"] = None
    MEMORY["recovery_status"] = "RUNTIME_RECONCILED_FLAT"


def sync_all_states():
    global RECOVERY_BLOCK_ENTRIES
    runtime = globals().get("PORTFOLIO_RUNTIME")
    active_items = []
    if runtime is not None and hasattr(runtime, "status"):
        try:
            active_items = list(runtime.status().get("active", []))
        except Exception:
            active_items = []
    if PAPER_MODE:
        if len(active_items) > 1 and runtime is not None:
            for item in active_items:
                try:
                    with runtime.focus(str(item["trade_id"])):
                        MEMORY["position_status"] = "OPEN" if STATE.get("open") else "FLAT"
                except Exception as exc:
                    log_execution(
                        f"[PORTFOLIO_SYNC] {item.get('trade_id')}: {type(exc).__name__}: {exc}",
                        "ERROR",
                    )
            _recompute_portfolio_globals()
            MEMORY["total_pnl"] = PERF.get("total_pnl_usdt", 0.0)
            MEMORY["total_pnl_pct"] = PERF.get("total_pnl_pct", 0.0) * 100
            RECOVERY_BLOCK_ENTRIES = _portfolio_uncertain()
            return
        MEMORY["position_status"] = "OPEN" if STATE.get("open") else "FLAT"
        MEMORY["total_pnl"] = PERF.get("total_pnl_usdt", 0.0)
        MEMORY["total_pnl_pct"] = PERF.get("total_pnl_pct", 0.0) * 100
        RECOVERY_BLOCK_ENTRIES = False
        return
    if len(active_items) > 1 and runtime is not None:
        # Multiple positions are legitimate only when exchange exposure exactly
        # matches the registered set. Every sync cycle is focused on one trade.
        status, positions = fetch_position_tri_state(None, strict_account_snapshot=True)
        if status == "UNKNOWN" or _portfolio_exchange_unexpected(positions):
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["recovery_status"] = "EXCHANGE_SET_MISMATCH"
            RECOVERY_BLOCK_ENTRIES = True
            _recompute_portfolio_globals()
            _main_loop_progress("position_sync_complete", status="UNKNOWN")
            return
        statuses = []
        for item in active_items:
            try:
                with runtime.focus(str(item["trade_id"])):
                    sync_position_state(str(item.get("symbol") or ""))
                    statuses.append(str(MEMORY.get("position_status") or ""))
            except Exception as exc:
                statuses.append("UNKNOWN")
                log_execution(
                    f"[PORTFOLIO_SYNC] {item.get('trade_id')}: {type(exc).__name__}: {exc}",
                    "ERROR",
                )
        _recompute_portfolio_globals()
        if any(value == "UNKNOWN" for value in statuses):
            MEMORY["position_status"] = "UNKNOWN"
            RECOVERY_BLOCK_ENTRIES = True
        _main_loop_progress("position_sync_complete", status=str(MEMORY.get("position_status") or ""))
        return
    symbol = STATE.get("current_symbol") if STATE.get("open") else None
    _main_loop_blocked_phase(
        "position_sync_start", "EXCHANGE_POSITION_SYNC",
        symbol=str(symbol or "NO_SYMBOL"),
    )
    status = ""
    if symbol:
        sync_position_state(symbol)
        status = str(MEMORY.get("position_status") or "")
    else:
        status, positions = fetch_position_tri_state(None, strict_account_snapshot=True)
        if status == "UNKNOWN":
            MEMORY["position_status"] = "UNKNOWN"
            MEMORY["current_position"] = None
            RECOVERY_BLOCK_ENTRIES = True
        elif status == "FLAT":
            # Do not infer an unresolved order merely from a stale recovery_hold.
            # Run the same order/journal proof used by startup recovery; only
            # explicit pending execution state or failed persistence keeps entries blocked.
            recovery_gate_active = bool(
                RECOVERY_BLOCK_ENTRIES
                or STATE.get("recovery_hold")
                or STATE.get("execution_unresolved")
                or STATE.get("close_reconciliation_required")
                or STATE.get("pending_close_order_id")
                or STATE.get("entry_exchange_order_id")
                or STATE.get("entry_client_order_id")
                or STATE.get("entry_context_persistence_failed")
                or STATE.get("cross_event_persistence_failed")
                or STATE.get("entry_event_persistence_failed")
                or str(MEMORY.get("position_status") or "").upper() == "UNKNOWN"
            )
            if recovery_gate_active:
                reconciled, order_reason = _verify_runtime_flat_recovery_orders()
                if not reconciled:
                    MEMORY["position_status"] = "UNKNOWN"
                    MEMORY["current_position"] = None
                    MEMORY["recovery_status"] = "ORDER_OUTCOME_REQUIRES_RECONCILIATION"
                    STATE["recovery_hold"] = True
                    STATE["recovery_reason"] = str(order_reason)
                    RECOVERY_BLOCK_ENTRIES = True
                    log_execution(
                        "[RECOVERY] BingX reports FLAT, but order-level reconciliation is not complete "
                        f"({order_reason}); entries remain blocked",
                        "CRITICAL",
                        debounce_key=f"runtime_flat_recovery:{order_reason}",
                        debounce_sec=60,
                    )
                    status = "UNKNOWN"
                    _main_loop_progress("position_sync_complete", status=status)
                    return
                had_recovery_hold = bool(STATE.get("recovery_hold") or RECOVERY_BLOCK_ENTRIES)
                _apply_runtime_flat_reconciliation(clear_trade_state=not active_items)
                # Preserve any independent uncertainty held by another registered
                # portfolio slot; a single FLAT account snapshot cannot clear it.
                _recompute_portfolio_globals(clear_recovery=True)
                if had_recovery_hold and not RECOVERY_BLOCK_ENTRIES:
                    log_execution(
                        "[RECOVERY] Runtime FLAT reconciled; inherited hold released after "
                        "order-level and persistence verification",
                        "SUCCESS", debounce_key="runtime_flat_recovery_released", debounce_sec=60,
                    )
                elif had_recovery_hold and RECOVERY_BLOCK_ENTRIES:
                    log_execution(
                        "[RECOVERY] Runtime FLAT verified, but an independent portfolio "
                        "uncertainty still blocks entries",
                        "ERROR", debounce_key="runtime_flat_portfolio_hold", debounce_sec=60,
                    )
            else:
                MEMORY["position_status"] = "FLAT"
                MEMORY["current_position"] = None
        elif status == "OPEN" and positions:
            if len(positions) == 1:
                position = positions[0]
                entry_order_unresolved = bool(
                    STATE.get("execution_unresolved")
                    and str(STATE.get("entry_execution_state") or "").upper() in {
                        "EXECUTION_UNCERTAIN", "PARTIAL_FILL", "ENTRY_PRICE_UNKNOWN", "PROTECTION_HOLD",
                    }
                    and not STATE.get("entry_order_terminal")
                )
                if entry_order_unresolved:
                    if STATE.get("entry_client_order_id") or STATE.get("entry_exchange_order_id"):
                        terminal, order_status = _cancel_and_verify_entry_order_terminal(
                            str(_position_symbol(position)),
                            STATE.get("entry_exchange_order_id"),
                            STATE.get("entry_client_order_id"),
                            cancel_if_open=False,
                        )
                    else:
                        terminal, order_status = False, "ENTRY_ORDER_ID_UNAVAILABLE"
                    if terminal:
                        STATE["entry_order_terminal"] = True
                        STATE["entry_order_status"] = order_status
                        _attempt_runtime_trade_recovery(
                            _position_symbol(position), position=position, force=False,
                        )
                    else:
                        MEMORY["position_status"] = "UNKNOWN"
                        MEMORY["current_position"] = position
                        MEMORY["recovery_status"] = "ENTRY_ORDER_REMAINDER_UNRESOLVED"
                        STATE["recovery_hold"] = True
                        STATE["recovery_reason"] = str(order_status)
                        RECOVERY_BLOCK_ENTRIES = True
                        log_execution(
                            f"[RECOVERY] Position exists but entry order is not terminal "
                            f"({order_status}); holding management handoff and new entries",
                            "CRITICAL", debounce_key="entry_order_remainder_unresolved", debounce_sec=30,
                        )
                else:
                    _attempt_runtime_trade_recovery(
                        _position_symbol(position), position=position, force=False,
                    )
            else:
                MEMORY["position_status"] = "MULTIPLE_OPEN"
                RECOVERY_BLOCK_ENTRIES = True
    # A completed exchange sync is real progress even when the result is UNKNOWN.
    _main_loop_progress("position_sync_complete", status=status or str(MEMORY.get("position_status") or ""))
    if symbol:
        _main_loop_blocked_phase(
            "realized_pnl_start", "EXCHANGE_REALIZED_PNL", symbol=str(symbol),
        )
        try:
            real_pnl, real_pnl_pct = get_realized_pnl(symbol)
            MEMORY["total_pnl"] = real_pnl
            MEMORY["total_pnl_pct"] = real_pnl_pct
        except Exception as exc:
            # A failed read is not a completed phase. Report the terminal outcome
            # without advancing liveness; the next real iteration beat proves the
            # loop is still running.
            _main_loop_status("realized_pnl_failed", "EXCHANGE_REALIZED_PNL_FAILED",
                              symbol=str(symbol), error=type(exc).__name__)
            return
        _main_loop_progress("realized_pnl_complete", symbol=str(symbol))


# Serialize every entry-producing analysis path because the legacy entry
# context is process-global. The lock is acquired by the worker itself (not by
# its dispatcher), so a reentrant execute_entry call from that worker is safe.
_ENTRY_DISCOVERY_TASK_LOCK = threading.RLock()
_original_smart_opportunity_selection_for_entry_lock = smart_opportunity_selection
def smart_opportunity_selection(*args, **kwargs):
    if not _ENTRY_DISCOVERY_TASK_LOCK.acquire(blocking=False):
        log_execution("[ENTRY_SCAN] Institutional revalidation deferred; another entry cycle owns context",
                      "DEBUG", debounce_key="entry_scan_context_busy", debounce_sec=10)
        return False
    try:
        return _original_smart_opportunity_selection_for_entry_lock(*args, **kwargs)
    finally:
        _ENTRY_DISCOVERY_TASK_LOCK.release()


_original_sniper_engine_v2_for_entry_lock = sniper_engine_v2
def sniper_engine_v2(*args, **kwargs):
    if not _ENTRY_DISCOVERY_TASK_LOCK.acquire(blocking=False):
        log_execution("[ENTRY_SCAN] Sniper V2 deferred; another entry cycle owns context",
                      "DEBUG", debounce_key="entry_scan_context_busy", debounce_sec=10)
        return False
    try:
        return _original_sniper_engine_v2_for_entry_lock(*args, **kwargs)
    finally:
        _ENTRY_DISCOVERY_TASK_LOCK.release()


# Serialize final entry authorization so one closed-candle trigger cannot
# submit two orders from concurrent scanner paths. The current signal is checked
# inside the original entry function after gate calculation and before any order.
_ENTRY_EVENT_EXECUTION_LOCK = threading.RLock()
_original_execute_entry_for_journal = execute_entry
def execute_entry(*args, **kwargs):
    with _ENTRY_EVENT_EXECUTION_LOCK:
        if not _ENTRY_DISCOVERY_TASK_LOCK.acquire(blocking=False):
            log_execution("[ENTRY_BLOCKED] Another entry-producing cycle owns the shared context",
                          "WARN", debounce_key="entry_context_locked", debounce_sec=5)
            return False
        try:
            globals()["_LAST_EXECUTION_GATE_RESULT"] = {}
            result = _original_execute_entry_for_journal(*args, **kwargs)
            gate = globals().get("_LAST_EXECUTION_GATE_RESULT") or {}
            event_id = str(gate.get("durable_event_id") or "").strip()
            journal = globals().get("INSTITUTIONAL_TRADE_JOURNAL")
            if result and STATE.get("open"):
                ctx = globals().get("_INSTITUTIONAL_ENTRY_CONTEXT") or {}
                _persist_trade_journal_start(ctx.get("setup_type"))
                trade_id = str(STATE.get("trade_id") or "")
                entry_context = dict(ctx.get("persistent_entry_context") or STATE.get("entry_context_snapshot") or {})
                sizing_decision = dict(STATE.get("sizing_decision") or ctx.get("sizing_decision") or {})
                # The handoff is frozen after fill verification: planned values come
                # from the immutable sizing decision, actual values from the exchange
                # result (or the verified PAPER fill).
                entry_context.update({
                    "requested_entry_price": sizing_decision.get("entry_price"),
                    "requested_qty": sizing_decision.get("final_exchange_qty")
                    or sizing_decision.get("final_qty"),
                    "actual_entry_price": STATE.get("entry"),
                    "actual_filled_qty": STATE.get("initial_qty") or STATE.get("qty"),
                    "fill_source": "EXCHANGE_VERIFIED_FILL" if not PAPER_MODE else "PAPER_VERIFIED_FILL",
                })
                STATE["entry_context_snapshot"] = dict(entry_context)
                if journal is not None and entry_context:
                    try:
                        if not journal.persist_entry_context(trade_id, entry_context):
                            STATE["entry_context_persistence_failed"] = True
                            globals()["RECOVERY_BLOCK_ENTRIES"] = True
                            log_execution(
                                f"[ENTRY_CONTEXT] CRITICAL: verified entry {trade_id or 'NO_TRADE_ID'} "
                                "has no durable entry context; new entries blocked",
                                "ERROR",
                            )
                    except Exception as exc:
                        STATE["entry_context_persistence_failed"] = True
                        globals()["RECOVERY_BLOCK_ENTRIES"] = True
                        log_execution(
                            f"[ENTRY_CONTEXT] CRITICAL: persistence failed for {trade_id}: "
                            f"{type(exc).__name__}: {exc}; new entries blocked", "ERROR",
                        )
                cross_event_id = str(ctx.get("cross_event_id") or "").strip()
                if cross_event_id and journal is not None:
                    try:
                        if not journal.consume_cross_event(cross_event_id, trade_id):
                            STATE["cross_event_persistence_failed"] = True
                            globals()["RECOVERY_BLOCK_ENTRIES"] = True
                            log_execution(
                                f"[CROSS_EVENT] CRITICAL: verified entry {trade_id or 'NO_TRADE_ID'} "
                                f"but could not consume cross event {cross_event_id}; new entries blocked",
                                "ERROR",
                            )
                    except Exception as exc:
                        STATE["cross_event_persistence_failed"] = True
                        globals()["RECOVERY_BLOCK_ENTRIES"] = True
                        log_execution(
                            f"[CROSS_EVENT] CRITICAL: consumption failed for {cross_event_id}: "
                            f"{type(exc).__name__}: {exc}; new entries blocked", "ERROR",
                        )
                if event_id and journal is not None:
                    try:
                        if not trade_id or not journal.consume_event(event_id, trade_id):
                            STATE["entry_event_persistence_failed"] = True
                            globals()["RECOVERY_BLOCK_ENTRIES"] = True
                            log_execution(
                                f"[ENTRY_EVENT_GUARD] CRITICAL: verified entry {trade_id or 'NO_TRADE_ID'} "
                                f"but could not durably consume signal {event_id}; new entries blocked",
                                "ERROR",
                            )
                        else:
                            # The signal is consumed first; only a verified fill on
                            # a strictly later closed candle may release the wait.
                            _release_rf_reentry_block_after_fill(
                                journal, STATE.get("current_symbol"), STATE.get("side"), gate,
                            )
                    except Exception as exc:
                        STATE["entry_event_persistence_failed"] = True
                        globals()["RECOVERY_BLOCK_ENTRIES"] = True
                        log_execution(
                            f"[ENTRY_EVENT_GUARD] CRITICAL: durable signal consumption or RF block "
                            f"release failed for {event_id}: {type(exc).__name__}: {exc}; "
                            "new entries blocked",
                            "ERROR",
                        )
            return result
        finally:
            _ENTRY_DISCOVERY_TASK_LOCK.release()

_original_manage_live_trade_for_journal = LiveTradeManager.manage_live_trade
def _manage_live_trade_with_journal(self):
    result = _original_manage_live_trade_for_journal(self)
    _persist_trade_journal_snapshot()
    return result
LiveTradeManager.manage_live_trade = _manage_live_trade_with_journal

# Start-up recovery must complete before Scanner can authorize a new order.
_original_main_loop_sniper = main_loop_sniper
def main_loop_sniper():
    try:
        if not PAPER_MODE:
            try:
                safe_api_call(ex.load_markets)
            except Exception as exc:
                globals()["RECOVERY_BLOCK_ENTRIES"] = True
                MEMORY["position_status"] = "UNKNOWN"
                MEMORY["recovery_status"] = "MARKETS_UNAVAILABLE"
                log_execution(f"[RECOVERY] Cannot load BingX markets before startup recovery: {exc}", "ERROR")
            startup_trade_recovery()
        else:
            startup_trade_recovery()
    except Exception as exc:
        global RECOVERY_BLOCK_ENTRIES
        RECOVERY_BLOCK_ENTRIES = True
        MEMORY["position_status"] = "UNKNOWN"
        MEMORY["recovery_status"] = "STARTUP_RECOVERY_ERROR"
        log_execution(f"[RECOVERY] Startup recovery failed closed: {exc}", "ERROR")
    _main_loop_progress("startup_recovery_complete",
                        recovery_hold=bool(STATE.get("recovery_hold")))
    return _original_main_loop_sniper()


# safe_main_loop resolves this alias dynamically; point it at the recovery wrapper.
main_loop = main_loop_sniper

DASHBOARD_SERVERS = []
DASHBOARD_ACTIVE_PORTS = []

def start_dashboard_servers():
    """Start one-process Waitress servers before the trading supervisor.

    Startup is idempotent per port and transactional: if any port fails after
    another succeeded, every server started by this call is closed and joined
    before the error propagates. This prevents leaked WSGI threads or sockets.
    """
    from waitress import create_server
    hosted_port = os.getenv("PORT")
    if hosted_port:
        ports = [int(hosted_port)]
        bind_host = "0.0.0.0"
    else:
        raw_ports = os.getenv("DASHBOARD_LOCAL_PORTS", "8000,8080")
        ports = []
        for value in raw_ports.split(","):
            try:
                port = int(value.strip())
                if 1 <= port <= 65535 and port not in ports:
                    ports.append(port)
            except (TypeError, ValueError):
                continue
        bind_host = os.getenv("DASHBOARD_BIND_HOST", "127.0.0.1")
    if not ports:
        raise RuntimeError("No valid Dashboard port configured")

    original_servers = list(DASHBOARD_SERVERS)
    original_ports = list(DASHBOARD_ACTIVE_PORTS)
    started = [port for port in ports if port in DASHBOARD_ACTIVE_PORTS]
    added = []
    failures = []
    try:
        for port in ports:
            if port in DASHBOARD_ACTIVE_PORTS:
                continue
            try:
                # Waitress is a production WSGI server. Keep a single Python
                # process: the trading engine, instance lock and recovery
                # supervisor must never be forked into duplicate order workers.
                server = create_server(app, host=bind_host, port=port, threads=8)
                thread = threading.Thread(
                    target=server.run, daemon=True, name=f"ggp_dashboard_{port}")
                thread.start()
                DASHBOARD_SERVERS.append((server, thread))
                DASHBOARD_ACTIVE_PORTS.append(port)
                added.append((server, thread, port))
                started.append(port)
                log_execution(
                    f"[DASHBOARD] Waitress WSGI server started host={bind_host} port={port}",
                    "SUCCESS",
                )
            except (OSError, ValueError) as exc:
                failures.append((port, f"{type(exc).__name__}: {exc}"))
                log_execution(f"[DASHBOARD] Could not bind {bind_host}:{port}: {exc}", "ERROR")
                if hosted_port:
                    raise
        if failures:
            # Partial availability is not acceptable: leave no socket or WSGI
            # thread behind before the caller decides whether to restart.
            raise RuntimeError(
                "Dashboard could not bind every configured port: "
                + ", ".join(f"{port} ({reason})" for port, reason in failures)
            )
        if not started:
            raise RuntimeError("Dashboard could not bind any configured local port")
        return started
    except BaseException:
        for server, thread, _port in added:
            _stop_dashboard_server(server, thread)
        DASHBOARD_SERVERS[:] = original_servers
        DASHBOARD_ACTIVE_PORTS[:] = original_ports
        raise


if __name__ == "__main__":
    # A single process must own execution, recovery, journal writes and dashboard.
    # The lock is OS-released on process exit/crash; it is not a stale PID flag.
    from ggp_instance_lock import acquire_instance_lock, release_instance_lock
    _GGP_LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "ggpbot.instance.lock")
    _GGP_INSTANCE_LOCK = acquire_instance_lock(_GGP_LOCK_PATH)
    if _GGP_INSTANCE_LOCK is None:
        raise SystemExit("[SINGLE_INSTANCE] Another JGB_GGP process already owns the runtime lock. No dashboard or orders started.")
    print("[SINGLE_INSTANCE] Runtime lock acquired.")
    try:
        # Serve the local/hosted Dashboard independently while the trade
        # supervisor stays on the foreground thread. This avoids daemon-only
        # process exit.
        start_dashboard_servers()
        start_runtime_workers()
        start_naiso_services()
        run_trade_supervisor(lock_handle=_GGP_INSTANCE_LOCK, lock_path=_GGP_LOCK_PATH)
    except Exception:
        log_execution(f"[SHUTDOWN] Fatal supervisor error: {traceback.format_exc()}", "ERROR")
        raise
    finally:
        # Guarantee cleanup on startup failure as well as controlled exit.
        _stop_runtime_services()
        release_instance_lock(_GGP_INSTANCE_LOCK)
