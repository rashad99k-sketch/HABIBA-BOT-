"""Strict, testable parsing for BingX native swap stop-loss verification.

BingX's raw swap v2 trade/openOrders endpoint returns conditional STOP_MARKET
orders. The inspected CCXT 4.5.92 unified fetchOpenOrders adapter also calls
the swap v2 open-orders route, but does not apply this verifier's explicit
type=STOP_MARKET filter and raw-field matching. The direct query is retained
for deterministic, fail-closed protection matching, not because the unified
method is documented here as incapable of returning trigger orders.
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterable, List, Optional


def _as_dict(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
    return None


def extract_open_order_records(payload: Any) -> List[Dict[str, Any]]:
    """Unwrap CCXT implicit BingX responses without mistaking metadata for orders."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    obj = _as_dict(payload)
    if not obj:
        return []

    # Raw BingX shape is usually {code: 0, data: [...]}; tolerate wrappers too.
    for key in ("data", "orders", "list", "rows", "result", "order"):
        value = obj.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        nested = _as_dict(value)
        if nested is not None:
            found = extract_open_order_records(nested)
            if found:
                return found

    # Some adapters return the order object directly.
    identifying = {"symbol", "type", "orderType", "origType", "stopPrice", "triggerPrice", "orderId", "orderID"}
    if identifying.intersection(obj.keys()):
        return [obj]
    return []


def _walk_contexts(order: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Yield root and known nested order/trigger dictionaries safely."""
    stack: List[Any] = [order]
    seen = set()
    while stack:
        raw = stack.pop()
        obj = _as_dict(raw)
        if obj is None or id(obj) in seen:
            continue
        seen.add(id(obj))
        yield obj
        for key in ("info", "data", "order", "stopLoss", "stop_loss", "trigger", "conditional"):
            child = obj.get(key)
            if isinstance(child, (dict, str)):
                stack.append(child)


def _values(order: Dict[str, Any], keys: Iterable[str]) -> List[Any]:
    wanted = set(keys)
    result: List[Any] = []
    for context in _walk_contexts(order):
        for key, value in context.items():
            if key in wanted and value not in (None, ""):
                result.append(value)
    return result


def _first_text(order: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for value in _values(order, keys):
        if isinstance(value, (str, int, float)):
            text = str(value).strip()
            if text:
                return text
    return None


def _canonical_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    text = text.split(":", 1)[0]
    text = text.replace("/", "-").replace("_", "-")
    for suffix in ("-PERPETUAL", "-PERP"):
        if text.endswith(suffix):
            text = text[:-len(suffix)]
    return text


def _positive_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
        if math.isfinite(number) and number > 0:
            return number
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def is_matching_native_stop_order(
    order: Dict[str, Any],
    *,
    symbol: str,
    position_side: str,
    close_side: str,
    trigger_price: float,
    required_qty: float,
    price_tolerance: float = 1e-5,
    price_tolerance_abs: float = 0.0,
    qty_tolerance: float = 1e-3,
) -> bool:
    """Return True only for an active, correctly scoped STOP_MARKET covering qty."""
    if not isinstance(order, dict):
        return False

    expected_symbol = _canonical_symbol(symbol)
    actual_symbol = _canonical_symbol(_first_text(order, ("symbol", "market", "contract")))
    if not expected_symbol or actual_symbol != expected_symbol:
        return False

    # CCXT's BingX parser intentionally normalizes STOP_MARKET to the unified
    # order type "market" and preserves the venue-native type in order.info.type.
    # Looking only at the first normalized field therefore rejects a real native
    # stop as MISSING. Inspect every raw/normalized type value and require the
    # explicit BingX STOP_MARKET value; do not infer protection from "market".
    type_values = [
        str(value).strip().upper().replace("-", "_")
        for value in _values(order, ("type", "orderType", "origType"))
        if value not in (None, "")
    ]
    # BingX/CCXT has emitted both STOP_MARKET and TRIGGER_MARKET for linear
    # swap conditional market orders across API/parser generations. CCXT maps
    # both raw types to unified "market". Accept either native conditional type
    # ONLY when every independent protection field below (symbol, close side,
    # position side, active status, exact trigger and covering quantity) matches
    # the journaled stop. A plain MARKET remains categorically rejected.
    if not (set(type_values) & {"STOP_MARKET", "TRIGGER_MARKET"}):
        return False

    actual_side = (_first_text(order, ("side", "orderSide")) or "").upper()
    if actual_side != str(close_side).upper():
        return False

    actual_position_side = (_first_text(order, ("positionSide", "posSide", "position_side")) or "").upper()
    if actual_position_side != str(position_side).upper():
        return False

    status = (_first_text(order, ("status", "state", "orderStatus")) or "").upper().replace(" ", "_")
    # No status is not proof of an active order. Reject ambiguous payloads.
    if status not in {"NEW", "OPEN", "PENDING", "PARTIALLY_FILLED", "PARTIALLYFILLED", "WORKING"}:
        return False

    expected_price = _positive_float(trigger_price)
    stop_prices = [_positive_float(v) for v in _values(order, (
        "stopPrice", "triggerPrice", "stopLossPrice", "stop_price", "trigger_price"
    ))]
    stop_prices = [v for v in stop_prices if v is not None]
    if expected_price is None or not stop_prices:
        return False
    # Keep the strict relative tolerance, but also allow only the exact
    # quantization delta produced by the exchange's own price_to_precision().
    # The caller supplies that delta; this is not a blanket one-tick allowance.
    tolerance = max(abs(expected_price) * float(price_tolerance),
                    max(float(price_tolerance_abs or 0.0), 0.0), 1e-10)
    if not any(abs(value - expected_price) <= tolerance for value in stop_prices):
        return False

    expected_qty = _positive_float(required_qty)
    quantities = [_positive_float(v) for v in _values(order, (
        "origQty", "quantity", "qty", "amount", "orig_qty", "origQuantity"
    ))]
    quantities = [v for v in quantities if v is not None]
    if expected_qty is None or not quantities:
        return False
    qty_floor = expected_qty * (1.0 - max(float(qty_tolerance), 0.0))
    if not any(value >= qty_floor for value in quantities):
        return False

    return True


def native_order_identifiers(order: Dict[str, Any]) -> set[str]:
    """Collect native conditional-order identifiers, including BingX triggerOrderId."""
    identifiers = set()
    for value in _values(order, ("id", "orderId", "orderID", "triggerOrderId", "trigger_order_id", "algoId", "algoOrderId", "clientOrderId")):
        text = str(value).strip()
        if text:
            identifiers.add(text)
    return identifiers


def native_stop_candidate_summary(order: Dict[str, Any]) -> Dict[str, Any]:
    """Return a secret-free diagnostic summary for a candidate protective order."""
    return {
        "id": _first_text(order, ("id", "orderId", "orderID", "triggerOrderId", "trigger_order_id")),
        "symbol": _first_text(order, ("symbol", "market", "contract")),
        "type": _first_text(order, ("type", "orderType", "origType")),
        "side": _first_text(order, ("side", "orderSide")),
        "positionSide": _first_text(order, ("positionSide", "posSide", "position_side")),
        "status": _first_text(order, ("status", "state", "orderStatus")),
        "triggerPrice": _first_text(order, ("stopPrice", "triggerPrice", "stopLossPrice", "stop_price", "trigger_price")),
        "quantity": _first_text(order, ("origQty", "quantity", "qty", "amount", "orig_qty", "origQuantity")),
    }


CONDITIONAL_ORDER_TYPES = ("STOP_MARKET", "TRIGGER_MARKET")


def _raw_query_succeeded(payload: Any) -> bool:
    """A raw API envelope is useful evidence only when its own status is success."""
    if payload is None:
        return False
    obj = _as_dict(payload)
    if obj is not None and "code" in obj and str(obj.get("code")).strip().lower() not in {"0", "200", "ok"}:
        return False
    if obj is not None and "success" in obj and obj.get("success") is False:
        return False
    if isinstance(payload, list):
        return True
    if obj is None:
        return False
    if "code" in obj or "success" in obj:
        return True
    return bool(extract_open_order_records(obj))


def fetch_native_stop_evidence(exchange: Any, symbol: str, *, api_call=None, logger=None):
    """Collect conditional-order evidence without treating an empty list as absence.

    Returns ``(records, authoritative_ok, details)``. ``authoritative_ok`` is True
    only when BOTH BingX conditional-order queries (STOP_MARKET and
    TRIGGER_MARKET) succeeded. A unified ``fetch_open_orders`` response may add
    matching records, but an empty or unavailable unified response never proves
    that a conditional stop is absent.
    """
    call = api_call or (lambda func, *args, **kwargs: func(*args, **kwargs))

    def _warn(message: str) -> None:
        if callable(logger):
            try:
                logger(message)
            except Exception:
                pass

    records: List[Dict[str, Any]] = []
    seen_ids = set()

    def _add_records(payload: Any) -> None:
        # The same conditional order appears in multiple endpoints. Collapse it
        # by native id so duplicate detection reflects real exchange orders.
        for row in extract_open_order_records(payload):
            identifiers = native_order_identifiers(row)
            if identifiers and identifiers & seen_ids:
                continue
            seen_ids.update(identifiers)
            records.append(row)

    unified_seen = False
    fetch_open = getattr(exchange, "fetch_open_orders", None)
    if callable(fetch_open):
        for args in ((symbol,), (symbol, None, None, {"type": "STOP_MARKET"})):
            try:
                payload = call(fetch_open, *args)
            except TypeError:
                continue
            except Exception as exc:
                _warn(f"unified open-order query failed: {type(exc).__name__}: {exc}")
                continue
            if payload is not None:
                unified_seen = True
                _add_records(payload)

    market_id = str(symbol)
    market = getattr(exchange, "market", None)
    if callable(market):
        try:
            market_payload = call(market, symbol)
            if isinstance(market_payload, dict) and market_payload.get("id"):
                market_id = str(market_payload["id"])
        except Exception as exc:
            _warn(f"market id query failed: {type(exc).__name__}: {exc}")

    conditional_ok = {}
    raw_method = getattr(exchange, "swapV2PrivateGetTradeOpenOrders", None)
    if callable(raw_method):
        for native_type in CONDITIONAL_ORDER_TYPES:
            try:
                payload = call(raw_method, {"symbol": market_id, "type": native_type})
            except Exception as exc:
                _warn(f"{native_type} conditional query failed: {type(exc).__name__}: {exc}")
                continue
            if _raw_query_succeeded(payload):
                conditional_ok[native_type] = True
                _add_records(payload)
            else:
                _warn(f"{native_type} conditional query returned an unsuccessful envelope")

    authoritative_ok = all(conditional_ok.get(kind, False) for kind in CONDITIONAL_ORDER_TYPES)
    return records, authoritative_ok, {
        "unified_rows_seen": unified_seen,
        "conditional_ok": conditional_ok,
        "authoritative_ok": authoritative_ok,
    }
