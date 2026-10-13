"""Link return documents and money to physical movements without applying stock twice."""

from collections import defaultdict
from decimal import Decimal
import json

from .contracts import ContractError, canonical_json, timestamp


def integrate_returns(events, lines, payments, delivered, catalog, pending, checks, stable_id):
    requests, receipts, inspections, allocations = {}, {}, {}, {}
    stock_lineage = []

    def wait(event, reason):
        pending.append(dict(event=event, reason=reason))

    def insert(target, key, value):
        if key in target and target[key] != value:
            raise ContractError("return document identity conflict: " + str(key))
        target[key] = value

    def origin(e, field):
        return e["source"], e["scope"], e["payload"][field]

    def base(e):
        return dict(event_id=e["event_id"], source=e["source"], scope=e["scope"],
                    raw_file=e["provenance"]["file"], batch=e["provenance"]["batch"], occurred_at=e["occurred_at"])

    for e in events:
        if e["type"] != "return_request":
            continue
        p = e["payload"]
        line_key = (p["order_source"], p["order_scope"], p["order"], p["line"])
        line = lines.get(line_key)
        if line is None:
            wait(e, "missing return order line")
            continue
        if line["sku_id"] != e["sku_id"]:
            checks.append(dict(check="return_sku_mismatch", result="failed", event_id=e["event_id"]))
            continue
        key = origin(e, "return_line")
        value = dict(**base(e), return_id=stable_id("RET-", key[:2] + (p["return"],)),
                     return_line_id=stable_id("RETL-", key), order_id=line["order_id"], line_id=line["line_id"],
                     sku_id=e["sku_id"], quantity=p["quantity"], reason=p["reason"], currency=line["currency"],
                     source_return=p["return"], source_return_line=p["return_line"], order_line_key=canonical_json(line_key))
        insert(requests, key, value)
    for e in events:
        if e["type"] != "return_receipt":
            continue
        p = e["payload"]
        parent = requests.get((p["return_source"], p["return_scope"], p["return_line"]))
        loc = catalog["locations"].get(p["location"])
        if parent is None or loc is None:
            wait(e, "missing return request or receipt location")
            continue
        if parent["sku_id"] != e["sku_id"]:
            checks.append(dict(check="return_receipt_sku", result="failed", event_id=e["event_id"]))
            continue
        if timestamp(e["occurred_at"]) < timestamp(parent["occurred_at"]):
            checks.append(dict(check="return_receipt_before_request", result="failed", event_id=e["event_id"]))
        key = origin(e, "receipt")
        insert(receipts, key, dict(**base(e), receipt_id=stable_id("RRC-", key), source_receipt=p["receipt"],
                                  return_line_id=parent["return_line_id"], order_id=parent["order_id"], line_id=parent["line_id"],
                                  sku_id=e["sku_id"], location=loc, quantity=p["quantity"]))
    for e in events:
        if e["type"] != "return_inspection":
            continue
        p = e["payload"]
        parent = receipts.get((p["receipt_source"], p["receipt_scope"], p["receipt"]))
        loc = catalog["locations"].get(p["location"])
        if parent is None or loc is None:
            wait(e, "missing return receipt or inspection location")
            continue
        if parent["sku_id"] != e["sku_id"] or parent["location"] != loc:
            checks.append(dict(check="return_inspection_identity", result="failed", event_id=e["event_id"]))
            continue
        if timestamp(e["occurred_at"]) < timestamp(parent["occurred_at"]):
            checks.append(dict(check="return_inspection_before_receipt", result="failed", event_id=e["event_id"]))
        key = origin(e, "inspection")
        insert(inspections, key, dict(**base(e), inspection_id=stable_id("RIN-", key), source_inspection=p["inspection"],
                                     receipt_id=parent["receipt_id"], return_line_id=parent["return_line_id"],
                                     sku_id=e["sku_id"], location=loc, quantity=p["quantity"], condition=p["condition"]))
    for e in events:
        if e["type"] != "refund_allocation":
            continue
        p = e["payload"]
        tx_key = (e["source"], e["scope"], p["transaction"])
        tx = payments.get(tx_key)
        parent = requests.get((p["return_source"], p["return_scope"], p["return_line"]))
        if tx is None or parent is None:
            wait(e, "missing refund transaction or return line")
            continue
        if tx["kind"] != "refund" or tx["currency"] != p["currency"] or parent["currency"] != p["currency"] or tx["order_id"] != parent["order_id"]:
            checks.append(dict(check="refund_allocation_identity", result="failed", event_id=e["event_id"]))
            continue
        key = origin(e, "allocation")
        insert(allocations, key, dict(**base(e), allocation_id=stable_id("RAL-", key), transaction=p["transaction"],
                                     return_line_id=parent["return_line_id"], order_id=parent["order_id"], line_id=parent["line_id"],
                                     amount=p["amount"], currency=p["currency"]))

    # Validate aggregate cardinalities; several receipts/inspections/refunds are allowed.
    requested_by_line, received, inspected = defaultdict(int), defaultdict(int), defaultdict(int)
    for row in requests.values():
        requested_by_line[row["order_line_key"]] += row["quantity"]
    for line_key, quantity in requested_by_line.items():
        key = tuple(json.loads(line_key))
        if key not in delivered:
            checks.append(dict(check="return_delivery", result="waiting", line_id=lines[key]["line_id"]))
        elif quantity > delivered[key]:
            checks.append(dict(check="return_exceeds_delivered", result="failed", line_id=lines[key]["line_id"]))
    for row in receipts.values():
        received[row["return_line_id"]] += row["quantity"]
    for row in requests.values():
        if received[row["return_line_id"]] > row["quantity"]:
            checks.append(dict(check="received_exceeds_requested", result="failed", return_line_id=row["return_line_id"]))
    for row in inspections.values():
        inspected[row["receipt_id"]] += row["quantity"]
    for row in receipts.values():
        if inspected[row["receipt_id"]] > row["quantity"]:
            checks.append(dict(check="inspected_exceeds_received", result="failed", receipt_id=row["receipt_id"]))
    allocated = defaultdict(lambda: Decimal("0"))
    for row in allocations.values():
        allocated[(row["source"], row["scope"], row["transaction"])] += Decimal(row["amount"])
    for key, tx in payments.items():
        if tx["kind"] == "refund" and (tx.get("refund_reason") == "return" or key in allocated):
            if key not in allocated:
                checks.append(dict(check="refund_allocation_total", result="waiting", transaction=key[2]))
            elif allocated[key] != Decimal(tx["amount"]):
                checks.append(dict(check="refund_allocation_total", result="waiting" if allocated[key] < Decimal(tx["amount"]) else "failed", transaction=key[2]))

    movement_quantities = defaultdict(int)
    for e in events:
        if e["type"] != "stock_move" or e["payload"].get("document_type") not in ("return_receipt", "return_inspection"):
            continue
        p = e["payload"]
        key = (p["document_source"], p["document_scope"], p["document_id"])
        docs = receipts if p["document_type"] == "return_receipt" else inspections
        doc = docs.get(key)
        if doc is None:
            wait(e, "missing stock return document")
            continue
        to = p["to"]
        from_ = p["from"]
        valid = e["sku_id"] == doc["sku_id"] and catalog["locations"].get(to["location"]) == doc["location"]
        if p["document_type"] == "return_receipt":
            valid = valid and from_["location"] == "external:customer" and to["condition"] == "inspection"
            doc_id = doc["receipt_id"]
        else:
            valid = valid and catalog["locations"].get(from_["location"]) == doc["location"] and from_["condition"] == "inspection" and to["condition"] == doc["condition"]
            doc_id = doc["inspection_id"]
        if not valid:
            checks.append(dict(check="return_stock_identity", result="failed", event_id=e["event_id"]))
            continue
        movement_quantities[(p["document_type"], key)] += p["quantity"]
        stock_lineage.append(dict(**base(e), movement_id=stable_id("MOV-", (e["source"], e["scope"], e["event_id"])),
                                  document_type=p["document_type"], document_id=doc_id,
                                  return_line_id=doc["return_line_id"], sku_id=doc["sku_id"], quantity=p["quantity"]))
    for kind, docs in (("return_receipt", receipts), ("return_inspection", inspections)):
        for key, row in docs.items():
            moved = movement_quantities.get((kind, key))
            if moved is None or moved != row["quantity"]:
                checks.append(dict(check="return_stock_quantity", result="waiting" if moved is None else "failed",
                                   document=key[2], expected=row["quantity"], actual=moved))
    inspected_by_return = defaultdict(int)
    refunded_by_return = defaultdict(lambda: Decimal("0"))
    for row in inspections.values():
        inspected_by_return[row["return_line_id"]] += row["quantity"]
    for row in allocations.values():
        tx = payments[(row["source"], row["scope"], row["transaction"])]
        if tx["status"] == "succeeded":
            refunded_by_return[row["return_line_id"]] += Decimal(row["amount"])
    for row in requests.values():
        rid = row["return_line_id"]
        row.update(received_quantity=received[rid], inspected_quantity=inspected_by_return[rid],
                   refunded_amount=format(refunded_by_return[rid], ".2f"))
    return dict(return_lines=list(requests.values()), return_receipts=list(receipts.values()),
                return_inspections=list(inspections.values()), refund_allocations=list(allocations.values()), stock_lineage=stock_lineage)
