"""Explicit v1 event contracts for the first vertical slice."""

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation


class ContractError(ValueError):
    pass


FIELDS = {
    "stock_move": ("sku", "quantity", "from", "to", "reference"),
    "order": ("order", "line", "sku", "quantity", "unit_price", "discount", "tax", "total", "currency"),
    "order_link": ("order", "line", "origin_source", "origin_scope", "origin_order", "origin_line"),
    "payment": ("transaction", "order_source", "order_scope", "order", "amount", "currency", "kind", "status"),
    "reservation": ("reservation", "order_source", "order_scope", "order", "line", "sku", "location", "quantity", "action"),
    "delivery": ("shipment", "order_source", "order_scope", "order", "line", "quantity", "status"),
    "inventory_snapshot": ("snapshot", "sku", "location", "condition", "quantity", "as_of"),
    "return_request": ("return", "return_line", "order_source", "order_scope", "order", "line", "sku", "quantity", "reason"),
    "return_receipt": ("receipt", "return_source", "return_scope", "return_line", "sku", "location", "quantity"),
    "return_inspection": ("inspection", "receipt_source", "receipt_scope", "receipt", "sku", "location", "quantity", "condition"),
    "refund_allocation": ("allocation", "transaction", "return_source", "return_scope", "return_line", "amount", "currency"),
    "purchase_order_line": ("purchase_order", "purchase_line", "supplier", "sku", "location", "quantity", "unit_cost", "currency", "expected_at"),
    "purchase_receipt": ("receipt", "purchase_source", "purchase_scope", "purchase_line", "sku", "location", "quantity"),
    "purchase_inspection": ("inspection", "receipt_source", "receipt_scope", "receipt", "sku", "location", "quantity", "condition"),
    "settlement_line": ("settlement", "settlement_line", "order_source", "order_scope", "order", "currency", "gross", "refunds", "fees", "payout", "period_start", "period_end"),
    "settlement": ("settlement", "currency", "gross", "refunds", "fees", "payout", "line_count", "period_start", "period_end"),
    "payout_transfer": ("transaction", "settlement_source", "settlement_scope", "settlement", "amount", "currency", "status"),
    "event_rejection": ("target_source", "target_scope", "target_event_id", "reason"),
}


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.utcoffset() is None:
            raise ValueError("timezone required")
        return result
    except (ValueError, TypeError, AttributeError) as exc:
        raise ContractError("invalid timezone-aware timestamp") from exc


def money(value, signed=False):
    if not isinstance(value, str):
        raise ContractError("money must be a decimal string")
    try:
        result = Decimal(value)
        if not result.is_finite() or (result < 0 and not signed) or result != result.quantize(Decimal("0.01")):
            raise ContractError("money must be nonnegative with at most two decimal places")
        if abs(result) >= Decimal("1000000000000000000"):
            raise ContractError("money exceeds decimal(20,2) storage precision")
        return result
    except (InvalidOperation, ValueError) as exc:
        raise ContractError("invalid decimal amount") from exc


def validate(event):
    if not isinstance(event, dict):
        raise ContractError("event must be an object")
    for field in ("event_id", "source", "scope", "type", "occurred_at", "schema_version", "payload"):
        if field not in event:
            raise ContractError("missing " + field)
    for field in ("event_id", "source", "scope", "type"):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ContractError("invalid " + field)
    if type(event["schema_version"]) is not int or event["schema_version"] != 1:
        raise ContractError("unsupported schema_version")
    timestamp(event["occurred_at"])
    if "source_version" in event and (type(event["source_version"]) is not int or not 0 < event["source_version"] <= 9223372036854775807):
        raise ContractError("source_version must be a positive integer")
    if event["type"] not in FIELDS:
        raise ContractError("unsupported event type")
    if event["type"] not in ("order", "payment", "inventory_snapshot", "purchase_order_line", "settlement_line", "settlement", "payout_transfer") and event.get("source_version", 1) != 1:
        raise ContractError("this event type is append-only; revisions are not supported")
    p = event["payload"]
    if not isinstance(p, dict):
        raise ContractError("payload must be an object")
    for field in FIELDS[event["type"]]:
        if field not in p or p[field] is None:
            raise ContractError("missing payload." + field)
        if field not in ("quantity", "line_count", "from", "to") and (not isinstance(p[field], str) or not p[field]):
            raise ContractError("invalid payload." + field)
    if "quantity" in p:
        minimum = 0 if event["type"] == "inventory_snapshot" else 1
        if type(p["quantity"]) is not int or not minimum <= p["quantity"] <= 9223372036854775807:
            raise ContractError("invalid quantity for event type")
    if event["type"] == "inventory_snapshot":
        timestamp(p["as_of"])
    for field in ("unit_price", "unit_cost", "discount", "tax", "total", "amount", "gross", "refunds", "fees"):
        if field in p:
            money(p[field])
    if "currency" in p and p["currency"] not in ("SGD", "VND"):
        raise ContractError("unsupported currency")
    if event["type"] == "order":
        if Decimal(p["quantity"]) * money(p["unit_price"]) - money(p["discount"]) + money(p["tax"]) != money(p["total"]):
            raise ContractError("line total mismatch")
    if event["type"] == "stock_move":
        document_fields = ("document_type", "document_source", "document_scope", "document_id")
        if any(f in p for f in document_fields):
            if not all(isinstance(p.get(f), str) and p[f] for f in document_fields):
                raise ContractError("stock document reference requires type/source/scope/id")
            if p["document_type"] not in ("return_receipt", "return_inspection", "purchase_receipt", "purchase_inspection"):
                raise ContractError("unsupported stock document type")
        for side in ("from", "to"):
            endpoint = p[side]
            if not isinstance(endpoint, dict) or set(endpoint) != {"location", "condition"}:
                raise ContractError("invalid stock endpoint")
            if not all(isinstance(v, str) and v for v in endpoint.values()):
                raise ContractError("invalid stock endpoint value")
        if p["from"] == p["to"]:
            raise ContractError("identical stock endpoints")
    if event["type"] == "payment":
        if "refund_reason" in p and (p["kind"] != "refund" or p["refund_reason"] not in ("return", "other")):
            raise ContractError("invalid refund_reason")
        if p["kind"] not in ("charge", "refund") or p["status"] not in ("succeeded", "failed", "pending"):
            raise ContractError("invalid payment kind/status")
        if p["kind"] == "refund" and not p.get("original_transaction"):
            raise ContractError("refund requires original_transaction")
    if event["type"] == "reservation" and p["action"] not in ("reserve", "release", "consume"):
        raise ContractError("invalid reservation action")
    if event["type"] == "delivery" and p["status"] != "delivered":
        raise ContractError("only delivered is supported in the first slice")
    if event["type"] in ("return_inspection", "purchase_inspection") and p["condition"] not in ("sellable", "defective"):
        raise ContractError("unsupported inspection condition")
    if event["type"] == "purchase_order_line":
        timestamp(p["expected_at"])
    if event["type"] in ("settlement", "settlement_line"):
        if timestamp(p["period_start"]) >= timestamp(p["period_end"]):
            raise ContractError("invalid settlement period")
        if money(p["gross"]) - money(p["refunds"]) - money(p["fees"]) != money(p["payout"], signed=True):
            raise ContractError("settlement payout mismatch")
        if event["type"] == "settlement" and (type(p["line_count"]) is not int or p["line_count"] < 0):
            raise ContractError("invalid settlement line_count")
    if event["type"] == "payout_transfer" and p["status"] not in ("pending", "failed", "succeeded"):
        raise ContractError("invalid payout transfer status")
    if event["type"] == "event_rejection" and event["source"] != "pipeline_control":
        raise ContractError("rejection must be issued by pipeline_control")
    if event["type"] == "event_rejection" and p["target_source"] == "pipeline_control":
        raise ContractError("control audit events cannot be rejected")
    return event
