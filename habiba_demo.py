"""Isolated EMA9 + Session VWAP lifecycle simulator for the HABIBA dashboard.

This module is deliberately exchange-free: it never imports CCXT, reads bot
positions, calls an exchange, or submits real orders. Every order/fill/position
returned here is explicitly marked as simulated.
"""
from __future__ import annotations

from copy import deepcopy
import random
import threading
import time
from typing import Any, Dict, List, Optional


GATE_A = [
    ("closed_candle_cross", "تأكيد تقاطع EMA9/VWAP على شمعة مغلقة"),
    ("unique_event_id", "هوية Event ID فريدة وغير مستهلكة"),
    ("fresh_market_data", "حداثة بيانات السوق وسلامة OHLCV"),
    ("base_adx_18", "BASE ADX >= 18 — بوابة مطلقة"),
    ("valid_direction", "اتجاه BUY/SELL صحيح ومتسق"),
    ("location_not_invalid", "موقع مؤسسي غير ملغى أو مرفوض"),
    ("stop_loss_calculated", "Stop Loss محسوب وصالح هندسيًا"),
    ("risk_capacity", "سعة المخاطر تسمح بالدخول"),
    ("no_duplicate_order", "لا يوجد أمر سابق لنفس الحدث"),
    ("runtime_ready", "حالة محرك Demo متزامنة وجاهزة"),
]

GATE_B = [
    ("closed_mss_pending", "انتظار MSS مغلق — خاص بإعداد Reversal"),
    ("post_liquidity_window_not_ready", "نافذة ما بعد حدث السيولة غير مكتملة"),
    ("post_sweep_window_not_ready", "نافذة ما بعد Sweep غير مكتملة"),
    ("valid_sweep_or_failed_auction_not_confirmed", "Sweep/Failed Auction — خاص بإعداد Reversal"),
    ("post_mss_retest_pending", "انتظار Retest بعد MSS — خاص بإعداد Reversal"),
    ("no_fresh_nearby_order_block_or_fvg", "لا توجد منطقة OB/FVG قريبة وطازجة"),
]

GATE_C = [
    ("banker_flow_alignment", "توافق Institutional Flow"),
    ("volume_expansion", "توسع الحجم مقارنة بالمتوسط"),
    ("golden_zone_confluence", "توافق OB/FVG وموقع القيمة"),
    ("ema50_context", "EMA50 سياق مساعد وليس محفز دخول"),
]

EXECUTION_STEPS = [
    ("PRECHECK", "تحقق ما قبل الأمر"),
    ("CLASSIFY", "تصنيف SCALP / TREND"),
    ("SIZE", "حساب حجم المركز — سياسة 20X"),
    ("PREPARE_SL", "تجهيز Stop Loss"),
    ("SUBMIT", "إرسال أمر محاكاة"),
    ("VERIFY_FILL", "التحقق من التعبئة المحاكاة"),
    ("PROTECT", "تثبيت الحماية والتحقق منها"),
    ("CONFIRM_POSITION", "تأكيد المركز داخل Demo"),
]

SCENARIOS = ("ready_trend", "ready_scalp", "low_adx", "bad_zone", "waiting", "expired")
SYMBOLS = ("BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "LINK/USDT")


class DemoLifecycleEngine:
    """Thread-safe, deterministic-enough demo engine with an isolated state store."""

    def __init__(self, seed: int = 28062026) -> None:
        self._lock = threading.RLock()
        self._seed = int(seed)
        self._rng = random.Random(self._seed)
        self._sequence = 0
        self._cycle = 0
        self._running = True
        self._events: Dict[str, Dict[str, Any]] = {}
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._closed: List[Dict[str, Any]] = []
        self._decision_log: Dict[str, Dict[str, Any]] = {}
        self._non_cross: List[Dict[str, Any]] = []
        self._simulated_order_ids: set[str] = set()
        self._scenario_cursor = 0
        self._stats: Dict[str, Any] = {}
        self._seed_preview()

    def _now(self) -> float:
        return time.time()

    def _next_scenario(self) -> str:
        scenario = SCENARIOS[self._scenario_cursor % len(SCENARIOS)]
        self._scenario_cursor += 1
        return scenario

    def _make_zone(self, scenario: str, side: str) -> Dict[str, Any]:
        invalid = scenario == "bad_zone"
        waiting = scenario == "waiting"
        decision = "REJECTED" if invalid else "WAITING" if waiting else self._rng.choice(("EXCELLENT", "GOOD", "MODERATE"))
        validity = "INVALID" if invalid else "PENDING" if waiting else "VALID" if decision in ("EXCELLENT", "GOOD") else "WEAK"
        aligned = not invalid and self._rng.random() > 0.12
        ob_state = "INVALIDATED" if invalid else "MITIGATED" if waiting else self._rng.choice(("FRESH", "TESTED", "MITIGATED"))
        fvg_state = "FULLY_FILLED" if invalid else "PARTIALLY_FILLED" if waiting else self._rng.choice(("UNFILLED", "PARTIALLY_FILLED", "FULLY_FILLED"))
        liquidity_state = "SWEEP" if scenario == "waiting" else self._rng.choice(("NONE", "EQUAL_HIGHS", "EQUAL_LOWS", "GRAB"))
        side_name = "BULLISH" if side == "BUY" else "BEARISH"
        evidence = [
            f"OB {ob_state}: {'متوافق' if aligned else 'غير متوافق'} مع اتجاه {side}",
            f"FVG {fvg_state}: تم قياس الامتلاء من شموع مغلقة",
            f"Market Structure: {side_name if aligned else 'TRANSITION'} — {'aligned' if aligned else 'not aligned'}",
            f"Liquidity: {liquidity_state} — دليل سياقي وليس شرطًا عامًا لكل Setup",
            f"Momentum: {'متوافق' if aligned else 'ضعيف'} مع Volume Ratio",
            "Entry Location: تم قياس المسافة من أقرب منطقة سببية بوحدة ATR",
            "SL Feasibility: يُرفض الدخول إذا تعذر حساب وقف صالح",
        ]
        return {
            "decision": decision,
            "validity": validity,
            "orderBlock": {"state": ob_state, "directionAligned": aligned, "distanceAtr": round(self._rng.uniform(0.15, 0.85), 2)},
            "fvg": {"state": fvg_state, "fillPct": 100 if fvg_state == "FULLY_FILLED" else 42 if fvg_state == "PARTIALLY_FILLED" else 0},
            "structure": {"label": "BOS_UP" if side == "BUY" else "BOS_DOWN", "aligned": aligned, "mss": liquidity_state == "SWEEP"},
            "liquidity": {"event": liquidity_state, "direction": side_name if liquidity_state in ("SWEEP", "GRAB") else "NEUTRAL"},
            "momentum": {"direction": side_name if aligned else "NEUTRAL", "score": round(self._rng.uniform(48, 92), 1), "volumeRatio": round(self._rng.uniform(0.72, 2.15), 2)},
            "entry": {"distanceAtr": round(self._rng.uniform(0.12, 0.9), 2), "rr": round(self._rng.uniform(1.35, 3.2), 2)},
            "evidence": evidence,
        }

    def _make_gates(self, *, adx: float, zone: Dict[str, Any], scenario: str, side: str) -> List[Dict[str, Any]]:
        checks = {
            "closed_candle_cross": (True, "عبور مثبت على آخر شمعة مغلقة"),
            "unique_event_id": (True, "Event ID جديد وغير مستهلك"),
            "fresh_market_data": (True, "العمر داخل نافذة الحداثة"),
            "base_adx_18": (adx >= 18.0, f"ADX={adx:.2f}; الحد الأدنى 18.00"),
            "valid_direction": (side in ("BUY", "SELL"), f"side={side}"),
            # WAITING is not REJECTED: a setup-specific B condition may still be pending.
            "location_not_invalid": (zone["decision"] != "REJECTED", f"zone={zone['decision']}"),
            "stop_loss_calculated": (zone["decision"] != "REJECTED", "وقف صالح محسوب من Extreme افتراضي" if zone["decision"] != "REJECTED" else "لا يوجد SL صالح لموقع مرفوض"),
            "risk_capacity": (scenario != "bad_zone", "سعة مخاطر Demo متاحة"),
            "no_duplicate_order": (True, "لا يوجد simulated_order_id سابق للحدث"),
            "runtime_ready": (True, "محرك Demo متزامن؛ لا اتصال بمنصة"),
        }
        result = []
        for code, label in GATE_A:
            passed, reason = checks[code]
            result.append({"code": code, "label": label, "class": "A", "outcome": "PASS" if passed else "FAIL", "reason": reason})
        return result

    def _new_event(self, scenario: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        self._sequence += 1
        side = self._rng.choice(("BUY", "SELL"))
        if scenario == "ready_trend":
            side, adx = "BUY", round(self._rng.uniform(23, 31), 2)
        elif scenario == "ready_scalp":
            side, adx = "SELL", round(self._rng.uniform(19, 26), 2)
        elif scenario == "low_adx":
            adx = round(self._rng.uniform(11, 17.9), 2)
        else:
            adx = round(self._rng.uniform(20, 32), 2)
        symbol = symbol or self._rng.choice(SYMBOLS)
        now = self._now()
        zone = self._make_zone(scenario, side)
        event_id = f"DEMO-{symbol.replace('/', '').replace(':', '')}-{side}-{int(now * 1000)}-{self._sequence:04d}"
        reference_prices = {
            "BTC/USDT": 92000.0, "ETH/USDT": 3200.0, "SOL/USDT": 145.0,
            "DOGE/USDT": 0.18, "BNB/USDT": 620.0, "XRP/USDT": 2.4,
            "ADA/USDT": 0.72, "LINK/USDT": 15.0,
        }
        base_price = reference_prices.get(symbol, 100.0)
        ema9 = round(base_price * self._rng.uniform(0.994, 1.006), 6)
        spread = max(base_price * 0.00015, ema9 * self._rng.uniform(0.00015, 0.0012))
        vwap = round(ema9 - spread if side == "BUY" else ema9 + spread, 6)
        prior_gap = spread * self._rng.uniform(0.25, 0.8)
        prev_ema9 = round(vwap - prior_gap if side == "BUY" else vwap + prior_gap, 6)
        candle_ts = int(now // 900 * 900 - 900)
        trade_mode = "TREND" if scenario == "ready_trend" else "SCALP" if scenario == "ready_scalp" else "TREND" if self._rng.random() > 0.4 else "SCALP"
        setup_type = "LIQUIDITY_SWEEP_REVERSAL" if scenario in ("waiting", "expired") else "TREND_CONTINUATION" if trade_mode == "TREND" else "MOMENTUM_SCALP"
        gates = self._make_gates(adx=adx, zone=zone, scenario=scenario, side=side)
        event = {
            "id": event_id, "eventId": event_id, "symbol": symbol, "side": side,
            "crossStatus": "CONFIRMED", "crossDirection": "BULLISH" if side == "BUY" else "BEARISH",
            "candleTs": candle_ts, "detectedTs": now,
            "ema9": ema9, "prevEma9": prev_ema9,
            "vwap": vwap, "ema50": round(ema9 * (0.997 if side == "BUY" else 1.003), 5),
            "adx": adx, "diPlus": round(self._rng.uniform(12, 42), 2), "diMinus": round(self._rng.uniform(9, 38), 2),
            "scoredByScanner": round(self._rng.uniform(35, 98), 2), "scenario": scenario,
            "tradeMode": trade_mode, "setupType": setup_type, "zone": zone,
            "slStatus": "CALCULATED" if zone["decision"] != "REJECTED" else "MISSING",
            "stopLoss": round(ema9 * (0.992 if side == "BUY" else 1.008), 5) if zone["decision"] != "REJECTED" else None,
            "entryReady": False, "authorized": False, "licence": gates,
            "watchStatus": "CROSS_CONFIRMED", "stage": "CROSS_CONFIRMED",
            "stageTicks": 0, "waitTicks": 0, "waitReason": None, "rejectionReason": None,
            "rejectionCode": None, "expiresAfter": 6 if scenario == "waiting" else 2 if scenario == "expired" else 0,
            "executionSteps": [{"key": key, "label": label, "status": "PENDING"} for key, label in EXECUTION_STEPS],
            "executionIndex": 0, "simulatedOrderId": None, "positionId": None,
            "createdAt": now, "updatedAt": now,
        }
        self._events[event_id] = event
        self._sync_decision(event)
        return event

    def _sync_decision(self, event: Dict[str, Any]) -> None:
        log = {
            "symbol": event["symbol"], "side": event["side"], "event_id": event["id"],
            "candle_timestamp": event["candleTs"], "adx": event["adx"],
            "cross_status": event["crossStatus"], "watchlist_status": event["watchStatus"],
            "zone_status": event["zone"]["decision"], "trade_mode": event["tradeMode"],
            "stop_loss_status": event["slStatus"], "gate_results": deepcopy(event["licence"]),
            "final_authorization": "AUTHORIZED" if event["authorized"] else "DENIED" if event["stage"] in ("REJECTED", "EXPIRED") else "PENDING",
            "rejection_reason": event["rejectionReason"] or event["waitReason"] or "",
            "timestamp": event["updatedAt"], "stage": event["stage"],
        }
        self._decision_log[event["id"]] = log

    def _reject(self, event: Dict[str, Any], code: str, reason: str) -> None:
        event.update({"stage": "REJECTED", "watchStatus": "REJECTED", "entryReady": False,
                      "authorized": False, "rejectionCode": code, "rejectionReason": reason,
                      "waitReason": None, "updatedAt": self._now()})
        self._sync_decision(event)

    def _advance_event(self, event: Dict[str, Any]) -> None:
        if event["stage"] in ("REJECTED", "EXPIRED", "POSITION_OPEN", "COMPLETED"):
            return
        event["stageTicks"] += 1
        event["waitTicks"] += 1
        event["updatedAt"] = self._now()

        if event["stage"] == "CROSS_CONFIRMED":
            event.update({"stage": "INSTITUTIONAL_ANALYSIS", "watchStatus": "INSTITUTIONAL_ANALYSIS"})
            self._sync_decision(event)
            return

        if event["stage"] == "INSTITUTIONAL_ANALYSIS":
            scenario = event["scenario"]
            if event["adx"] < 18.0:
                self._reject(event, "BASE_ADX_BELOW_18", f"BASE ADX={event['adx']:.2f} أقل من 18.00؛ لا يسمح أي Score بتجاوز هذه البوابة")
                return
            if scenario == "bad_zone":
                self._reject(event, "INSTITUTIONAL_ZONE_REJECTED", "المنطقة المؤسسية ملغاة/ممتلئة بالكامل ولا يوجد موقع دخول صالح")
                return
            if scenario == "waiting":
                event.update({"stage": "INSTITUTIONAL_ANALYSIS", "watchStatus": "INSTITUTIONAL_ANALYSIS",
                              "entryReady": False, "authorized": False,
                              "waitReason": "إعداد LIQUIDITY_SWEEP_REVERSAL ينتظر post-MSS retest؛ شرط B خاص بهذا النوع ولا يعمم على TREND"})
                self._sync_decision(event)
                return
            if scenario == "expired":
                event.update({"stage": "INSTITUTIONAL_ANALYSIS", "watchStatus": "INSTITUTIONAL_ANALYSIS",
                              "entryReady": False, "authorized": False,
                              "waitReason": "انتهت نافذة الحدث قبل استكمال تأكيد الإعداد"})
                self._sync_decision(event)
                return
            failed = [gate for gate in event["licence"] if gate["outcome"] != "PASS"]
            if failed:
                self._reject(event, "LICENCE_GATE_FAILED", "فشل شرط إلزامي: " + ", ".join(g["code"] for g in failed))
                return
            event.update({"stage": "READY", "watchStatus": "READY", "authorized": True,
                          "entryReady": True, "waitReason": None, "rejectionReason": None})
            self._sync_decision(event)
            return

        if event["stage"] == "READY":
            if not event["authorized"] or not all(g["outcome"] == "PASS" for g in event["licence"]):
                self._reject(event, "AUTHORIZATION_NOT_VALID", "لا يمكن بدء التنفيذ دون اجتياز جميع بوابات A")
                return
            if not event["stopLoss"] or event["slStatus"] != "CALCULATED":
                self._reject(event, "MISSING_STOP_LOSS", "تم منع التنفيذ لغياب Stop Loss محسوب")
                return
            if event["simulatedOrderId"]:
                self._reject(event, "DUPLICATE_ORDER_BLOCKED", "تم منع تكرار أمر Demo لنفس Event ID")
                return
            event.update({"stage": "EXECUTING", "watchStatus": "EXECUTING", "executionIndex": 0})
            event["executionSteps"][0]["status"] = "ACTIVE"
            self._sync_decision(event)
            return

        if event["stage"] == "EXECUTING":
            index = int(event["executionIndex"])
            if index < 0 or index >= len(event["executionSteps"]):
                self._reject(event, "EXECUTION_STATE_INVALID", "حالة خط تنفيذ Demo غير صالحة")
                return
            step = event["executionSteps"][index]
            if step["key"] == "SUBMIT":
                if event["simulatedOrderId"]:
                    self._reject(event, "DUPLICATE_ORDER_BLOCKED", "تم منع تكرار أمر Demo لنفس الحدث")
                    return
                order_id = f"SIM-{event['id'][-9:]}"
                if order_id in self._simulated_order_ids:
                    self._reject(event, "DUPLICATE_ORDER_BLOCKED", "معرف أمر Demo مستخدم مسبقًا")
                    return
                self._simulated_order_ids.add(order_id)
                event["simulatedOrderId"] = order_id
            step["status"] = "COMPLETE"
            next_index = index + 1
            event["executionIndex"] = next_index
            if next_index < len(event["executionSteps"]):
                event["executionSteps"][next_index]["status"] = "ACTIVE"
            else:
                self._open_simulated_position(event)
            self._sync_decision(event)

    def _open_simulated_position(self, event: Dict[str, Any]) -> None:
        if not event["stopLoss"] or not event["simulatedOrderId"]:
            self._reject(event, "EXECUTION_INCOMPLETE", "لا يمكن تأكيد المركز قبل SL وأمر Demo وتأكيد الحماية")
            return
        position_id = f"SIM-POS-{event['id'][-9:]}"
        entry = float(event["ema9"])
        side_sign = 1 if event["side"] == "BUY" else -1
        position = {
            "id": position_id, "eventId": event["id"], "symbol": event["symbol"], "side": event["side"],
            "tradeMode": event["tradeMode"], "entry": entry, "mark": entry * (1 + side_sign * 0.0012),
            "qty": 1.0, "remainingQty": 1.0, "pnlPct": round(self._rng.uniform(0.15, 0.9), 2),
            "stopLoss": float(event["stopLoss"]), "tp1": entry * (1 + side_sign * 0.004),
            "tp2": entry * (1 + side_sign * 0.008), "tp1Progress": 0, "tp2Progress": 0,
            "tp1Hit": False, "tp2Hit": False, "ageTicks": 0,
            "reconciliation": "SIMULATED_OPEN_CONFIRMED", "source": "DEMO_SIMULATION_ONLY",
            "openedAt": self._now(),
        }
        self._positions[position_id] = position
        event.update({"stage": "POSITION_OPEN", "watchStatus": "POSITION_OPEN", "positionId": position_id,
                      "slStatus": "VERIFIED_DEMO", "entryReady": True, "updatedAt": self._now()})
        self._sync_decision(event)

    def _manage_positions(self) -> None:
        for position_id, position in list(self._positions.items()):
            position["ageTicks"] += 1
            sign = 1 if position["side"] == "BUY" else -1
            position["mark"] = round(position["entry"] * (1 + sign * (0.0012 + position["ageTicks"] * 0.001)), 6)
            position["pnlPct"] = round((0.2 + position["ageTicks"] * 0.42), 2)
            if position["tradeMode"] == "SCALP":
                if not position["tp1Hit"] and position["ageTicks"] >= 2:
                    position["tp1Hit"] = True
                    position["tp1Progress"] = 100
                    position["remainingQty"] = 0.0
                    self._close_position(position_id, "SCALP_TP1_FULL_CLOSE", "TP1 closed 100% of simulated SCALP quantity")
            else:
                if not position["tp1Hit"] and position["ageTicks"] >= 2:
                    position["tp1Hit"] = True
                    position["tp1Progress"] = 100
                    position["remainingQty"] = 0.5
                    position["pnlPct"] = round(position["pnlPct"] * 0.5, 2)
                elif position["tp1Hit"] and not position["tp2Hit"] and position["ageTicks"] >= 4:
                    position["tp2Hit"] = True
                    position["tp2Progress"] = 100
                    position["remainingQty"] = 0.0
                    self._close_position(position_id, "TREND_TP2_REMAINDER_CLOSE", "TP1 closed 50%; TP2 closed the remaining 50%")

    def _close_position(self, position_id: str, reason: str, reconciliation: str) -> None:
        position = self._positions.pop(position_id, None)
        if not position:
            return
        position["closedAt"] = self._now()
        position["closeReason"] = reason
        position["reconciliation"] = "SIMULATED_CLOSED_CONFIRMED"
        position["reconciliationDetail"] = reconciliation
        self._closed.insert(0, position)
        event = self._events.get(position["eventId"])
        if event:
            event.update({"stage": "COMPLETED", "watchStatus": "COMPLETED", "updatedAt": self._now()})
            self._sync_decision(event)

    def _update_stats(self) -> None:
        events = list(self._events.values())
        self._stats = {
            "cycle": self._cycle,
            "scanned": self._rng.randint(36, 58),
            "confirmedCrosses": sum(1 for e in events if e["crossStatus"] == "CONFIRMED"),
            "buyCrosses": sum(1 for e in events if e["side"] == "BUY"),
            "sellCrosses": sum(1 for e in events if e["side"] == "SELL"),
            "adxQualified": sum(1 for e in events if e["adx"] >= 18),
            "rejected": sum(1 for e in events if e["stage"] == "REJECTED"),
            "expired": sum(1 for e in events if e["stage"] == "EXPIRED"),
            "stale": self._rng.choice((0, 0, 1, 2)),
            "errors": 0,
            "cycleDurationMs": round(self._rng.uniform(180, 640), 1),
            "perSymbolMs": round(self._rng.uniform(3.1, 11.8), 2),
        }

    def _pipeline(self) -> List[Dict[str, Any]]:
        events = list(self._events.values())
        return [
            {"key": "SCAN", "count": self._stats.get("scanned", 0), "state": "complete", "note": "رموز جرى مسحها"},
            {"key": "CROSS", "count": self._stats.get("confirmedCrosses", 0), "state": "complete", "note": "شموع مغلقة مؤكدة"},
            {"key": "WATCHLIST", "count": len(events), "state": "complete", "note": "أحداث مستقلة محفوظة"},
            {"key": "ZONE", "count": sum(e["stage"] == "INSTITUTIONAL_ANALYSIS" for e in events), "state": "active", "note": "تحليل موقع التقاطع"},
            {"key": "AUTHORIZE", "count": sum(e["authorized"] for e in events), "state": "active", "note": "اجتازت بوابة A"},
            {"key": "EXECUTE", "count": sum(e["stage"] == "EXECUTING" for e in events), "state": "active", "note": "تنفيذ محاكاة والتحقق"},
            {"key": "POSITION", "count": len(self._positions), "state": "complete" if self._positions else "idle", "note": "مراكز Demo مؤكدة"},
        ]

    def _append_non_cross(self) -> None:
        kinds = [
            ("APPROACHING", "EMA9 يقترب من Session VWAP دون عبور"),
            ("NEAR_CROSS", "تقارب فقط؛ لا يوجد عبور مؤكد"),
            ("EMA50_VWAP", "تقاطع EMA50 مع VWAP ليس محفز EMA9"),
            ("UNCLOSED_CANDLE", "عبور داخل شمعة غير مغلقة — مرفوض"),
        ]
        self._non_cross = [
            {"symbol": self._rng.choice(SYMBOLS), "side": self._rng.choice(("BUY", "SELL")),
             "kind": kind, "note": note, "distancePct": round(self._rng.uniform(0.015, 0.24), 3),
             "candleTs": int(self._now() // 900 * 900)}
            for kind, note in kinds
        ]

    def _seed_preview(self) -> None:
        scenarios = [
            ("ready_trend", "BTC/USDT"),
            ("low_adx", "ETH/USDT"),
            ("bad_zone", "SOL/USDT"),
            ("waiting", "DOGE/USDT"),
            ("expired", "BNB/USDT"),
            ("ready_scalp", "XRP/USDT"),
        ]
        created = [self._new_event(kind, symbol) for kind, symbol in scenarios]
        for event in created:
            if event["scenario"] == "ready_scalp":
                self._advance_event(event)
                self._advance_event(event)  # Keep one visibly authorized READY row on first load.
            elif event["scenario"] == "waiting":
                self._advance_event(event)
                self._advance_event(event)
            elif event["scenario"] == "expired":
                self._advance_event(event)
                self._advance_event(event)
                event.update({"stage": "EXPIRED", "watchStatus": "EXPIRED", "waitReason": "انتهت مدة صلاحية Cross Event قبل اكتمال الشروط", "entryReady": False, "authorized": False})
                self._sync_decision(event)
            else:
                for _ in range(12):
                    self._advance_event(event)
                    if event["stage"] in ("REJECTED", "EXPIRED", "POSITION_OPEN", "COMPLETED"):
                        break
        self._cycle = 1
        self._append_non_cross()
        self._update_stats()

    def step(self) -> Dict[str, Any]:
        with self._lock:
            self._cycle += 1
            for event in list(self._events.values()):
                if event["stage"] not in ("REJECTED", "EXPIRED", "POSITION_OPEN", "COMPLETED"):
                    if event["scenario"] == "expired" and event["waitTicks"] >= 2:
                        event.update({"stage": "EXPIRED", "watchStatus": "EXPIRED", "entryReady": False,
                                      "authorized": False, "waitReason": "انتهت صلاحية الحدث قبل الترخيص"})
                        event["updatedAt"] = self._now()
                        self._sync_decision(event)
                    else:
                        self._advance_event(event)
            # Keep a freshly discovered Cross Event visible for at least one UI cycle.
            if self._cycle % 3 == 0:
                self._new_event(self._next_scenario())
            self._manage_positions()
            self._append_non_cross()
            self._update_stats()
            return self.snapshot()

    def start(self) -> Dict[str, Any]:
        with self._lock:
            self._running = True
            return self.snapshot()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            self._running = False
            return self.snapshot()

    def regenerate(self) -> Dict[str, Any]:
        with self._lock:
            self._seed += 1
            self._rng = random.Random(self._seed)
            self._sequence = 0
            self._cycle = 0
            self._events.clear()
            self._positions.clear()
            self._closed.clear()
            self._decision_log.clear()
            self._simulated_order_ids.clear()
            self._scenario_cursor = 0
            self._seed_preview()
            self._running = True
            return self.snapshot()

    def force_cycle(self) -> Dict[str, Any]:
        with self._lock:
            for scenario, symbol in (("ready_trend", "LINK/USDT"), ("ready_scalp", "XRP/USDT"),
                                     ("low_adx", "ADA/USDT"), ("bad_zone", "SOL/USDT"),
                                     ("waiting", "DOGE/USDT"), ("expired", "BNB/USDT")):
                event = self._new_event(scenario, symbol)
                for _ in range(14):
                    if event["stage"] in ("REJECTED", "EXPIRED", "POSITION_OPEN", "COMPLETED"):
                        break
                    self._advance_event(event)
                if scenario == "expired" and event["stage"] not in ("REJECTED", "EXPIRED"):
                    event.update({"stage": "EXPIRED", "watchStatus": "EXPIRED", "entryReady": False,
                                  "authorized": False, "waitReason": "انتهت صلاحية الحدث قبل الترخيص"})
                    self._sync_decision(event)
            self._cycle += 1
            self._manage_positions()
            self._append_non_cross()
            self._update_stats()
            return self.snapshot()

    def control(self, action: str) -> Dict[str, Any]:
        action = str(action or "").lower().strip()
        if action == "start": return self.start()
        if action == "stop": return self.stop()
        if action == "step": return self.step()
        if action in ("regenerate", "reset"): return self.regenerate()
        if action in ("force", "force_cycle"): return self.force_cycle()
        raise ValueError(f"unsupported_demo_action:{action or 'empty'}")

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            events = sorted(self._events.values(), key=lambda item: item["detectedTs"], reverse=True)
            stats = deepcopy(self._stats)
            return {
                "mode": "DEMO_SIMULATION_ONLY",
                "banner": "DEMO — بيانات وعمليات محاكاة فقط؛ لا توجد أوامر أو اتصالات تداول حقيقية",
                "running": self._running,
                "cycle": self._cycle,
                "stats": stats,
                "pipeline": self._pipeline(),
                "events": deepcopy(events),
                "nonCross": deepcopy(self._non_cross),
                "positions": deepcopy(list(self._positions.values())),
                "closedPositions": deepcopy(self._closed[:60]),
                "decisionLog": deepcopy(sorted(self._decision_log.values(), key=lambda item: item["timestamp"], reverse=True)),
                "gateCatalog": {
                    "A": [{"code": code, "label": label, "blocksEntry": True} for code, label in GATE_A],
                    "B": [{"code": code, "label": label, "blocksEntry": False, "scope": "setup-specific"} for code, label in GATE_B],
                    "C": [{"code": code, "label": label, "blocksEntry": False, "scope": "optional-evidence"} for code, label in GATE_C],
                },
                "executionSteps": [{"key": key, "label": label} for key, label in EXECUTION_STEPS],
                "policy": {"baseAdxMinimum": 18, "leverageDisplay": "20X", "scalpClosePct": 100, "trendTp1ClosePct": 50, "trendTp2ClosePct": 50},
                "repairReport": {
                    "rootCause": [
                        "مسار OpportunityPipeline يعرّف EMA9/VWAP proximity بأنه display/ranking only؛ لذلك لا يملك Cross Event lifecycle مستقلًا.",
                        "بعض محركات Reversal تنتظر Sweep → MSS → Retest؛ تعميم أسبابها كمانع لكل Setup يوقف Trend Continuation بلا مبرر.",
                        "Watchlist الحية مجمعة أساسًا بحسب الرمز، بينما المطلوب طابور أحداث Cross مستقلة بهوية Event ID؛ لا يجوز استبدال ذلك بتعديل مفتاح المحرك الحي أثناء معاينة Demo.",
                    ],
                    "implemented": [
                        "محرك Demo مستقل يحتفظ بكل Cross Event بهوية مستقلة ولا يستدعي مسار تنفيذ المنصة.",
                        "فصل بوابات A الإلزامية عن شروط B الخاصة بنوع Setup وعن أدلة C الاختيارية.",
                        "BASE ADX >= 18 شرط مطلق، ولا توجد آلية Score لتجاوز الترخيص.",
                        "تنفيذ افتراضي من 8 مراحل مع منع التكرار والتحقق من SL والتعبئة والحماية قبل فتح مركز Demo.",
                    ],
                    "scope": "DEMO_ONLY — live scanner, entry, BingX execution, protection, recovery and trade management are not modified by this feature.",
                },
            }


DEMO_ENGINE = DemoLifecycleEngine()
