"""Purchase documents, partial receipts, quality checks and stock provenance."""

from collections import defaultdict

from .contracts import ContractError, timestamp


def integrate_purchases(events, catalog, pending, checks, stable_id, history=None):
    orders, receipts, inspections = {}, {}, {}
    lineage = []
    created_at = {}
    for e in history if history is not None else events:
        if e["type"] == "purchase_order_line":
            key = e["source"], e["scope"], e["payload"]["purchase_line"]
            if key not in created_at or timestamp(e["occurred_at"]) < timestamp(created_at[key]):
                created_at[key] = e["occurred_at"]

    def base(e):
        return dict(source=e["source"], scope=e["scope"], event_id=e["event_id"], occurred_at=e["occurred_at"],
                    raw_file=e["provenance"]["file"], batch=e["provenance"]["batch"])

    def insert(target, key, value):
        if key in target and target[key] != value:
            raise ContractError("purchase document identity conflict: " + str(key))
        target[key] = value

    def wait(e, reason):
        pending.append(dict(event=e, reason=reason))

    for e in events:
        if e["type"] != "purchase_order_line":
            continue
        p = e["payload"]
        loc = catalog["locations"].get(p["location"])
        if loc is None:
            wait(e, "missing purchase destination mapping")
            continue
        key = e["source"], e["scope"], p["purchase_line"]
        insert(orders, key, dict(**base(e), purchase_order_id=stable_id("PO-", key[:2] + (p["purchase_order"],)),
                                purchase_line_id=stable_id("POL-", key), source_purchase_order=p["purchase_order"],
                                source_purchase_line=p["purchase_line"], sku_id=e["sku_id"], location=loc,
                                supplier=p["supplier"], quantity=p["quantity"], unit_cost=p["unit_cost"], currency=p["currency"],
                                expected_at=p["expected_at"], created_at=created_at[key], source_version=e["source_version"]))
    for e in events:
        if e["type"] != "purchase_receipt":
            continue
        p = e["payload"]
        parent = orders.get((p["purchase_source"], p["purchase_scope"], p["purchase_line"]))
        loc = catalog["locations"].get(p["location"])
        if parent is None or loc is None:
            wait(e, "missing purchase line or receipt location")
            continue
        if parent["sku_id"] != e["sku_id"] or parent["location"] != loc:
            checks.append(dict(check="purchase_receipt_identity", result="failed", event_id=e["event_id"]))
            continue
        if timestamp(e["occurred_at"]) < timestamp(parent["created_at"]):
            checks.append(dict(check="purchase_receipt_before_order", result="failed", event_id=e["event_id"]))
        key = e["source"], e["scope"], p["receipt"]
        insert(receipts, key, dict(**base(e), receipt_id=stable_id("PRC-", key), source_receipt=p["receipt"],
                                  purchase_line_id=parent["purchase_line_id"], sku_id=e["sku_id"], location=loc, quantity=p["quantity"]))
    for e in events:
        if e["type"] != "purchase_inspection":
            continue
        p = e["payload"]
        parent = receipts.get((p["receipt_source"], p["receipt_scope"], p["receipt"]))
        loc = catalog["locations"].get(p["location"])
        if parent is None or loc is None:
            wait(e, "missing purchase receipt or inspection location")
            continue
        if parent["sku_id"] != e["sku_id"] or parent["location"] != loc:
            checks.append(dict(check="purchase_inspection_identity", result="failed", event_id=e["event_id"]))
            continue
        if timestamp(e["occurred_at"]) < timestamp(parent["occurred_at"]):
            checks.append(dict(check="purchase_inspection_before_receipt", result="failed", event_id=e["event_id"]))
        key = e["source"], e["scope"], p["inspection"]
        insert(inspections, key, dict(**base(e), inspection_id=stable_id("PIN-", key), source_inspection=p["inspection"],
                                     receipt_id=parent["receipt_id"], purchase_line_id=parent["purchase_line_id"],
                                     sku_id=e["sku_id"], location=loc, quantity=p["quantity"], condition=p["condition"]))
    received, inspected, outcomes = defaultdict(int), defaultdict(int), defaultdict(int)
    for row in receipts.values():
        received[row["purchase_line_id"]] += row["quantity"]
    for row in inspections.values():
        inspected[row["receipt_id"]] += row["quantity"]
        outcomes[(row["purchase_line_id"], row["condition"])] += row["quantity"]
    for row in receipts.values():
        if inspected[row["receipt_id"]] > row["quantity"]:
            checks.append(dict(check="purchase_inspected_exceeds_received", result="failed", receipt_id=row["receipt_id"]))
    for row in orders.values():
        pid = row["purchase_line_id"]
        inspected_qty = outcomes[(pid, "sellable")] + outcomes[(pid, "defective")]
        row.update(received_quantity=received[pid], outstanding_quantity=max(row["quantity"] - received[pid], 0),
                   over_received_quantity=max(received[pid] - row["quantity"], 0), inspected_quantity=inspected_qty,
                   uninspected_quantity=max(received[pid] - inspected_qty, 0),
                   accepted_quantity=outcomes[(pid, "sellable")], defective_quantity=outcomes[(pid, "defective")])
        if row["over_received_quantity"]:
            checks.append(dict(check="purchase_received_exceeds_ordered", result="failed", purchase_line_id=pid,
                               ordered=row["quantity"], received=received[pid]))
    moved = defaultdict(int)
    for e in events:
        p = e["payload"]
        if e["type"] != "stock_move" or p.get("document_type") not in ("purchase_receipt", "purchase_inspection"):
            continue
        key = p["document_source"], p["document_scope"], p["document_id"]
        docs = receipts if p["document_type"] == "purchase_receipt" else inspections
        doc = docs.get(key)
        if doc is None:
            wait(e, "missing purchase stock document")
            continue
        from_, to = p["from"], p["to"]
        valid = e["sku_id"] == doc["sku_id"] and catalog["locations"].get(to["location"]) == doc["location"]
        if p["document_type"] == "purchase_receipt":
            valid = valid and from_["location"] == "external:supplier" and to["condition"] == "inspection"
            did = doc["receipt_id"]
        else:
            valid = valid and catalog["locations"].get(from_["location"]) == doc["location"] and from_["condition"] == "inspection" and to["condition"] == doc["condition"]
            did = doc["inspection_id"]
        if not valid:
            checks.append(dict(check="purchase_stock_identity", result="failed", event_id=e["event_id"]))
            continue
        moved[(p["document_type"], key)] += p["quantity"]
        lineage.append(dict(**base(e), movement_id=stable_id("MOV-", (e["source"], e["scope"], e["event_id"])),
                            document_type=p["document_type"], document_id=did, purchase_line_id=doc["purchase_line_id"],
                            sku_id=doc["sku_id"], quantity=p["quantity"]))
    for kind, docs in (("purchase_receipt", receipts), ("purchase_inspection", inspections)):
        for key, doc in docs.items():
            actual = moved.get((kind, key))
            if actual is None or actual != doc["quantity"]:
                checks.append(dict(check="purchase_stock_quantity", result="waiting" if actual is None else "failed",
                                   document=key[2], expected=doc["quantity"], actual=actual))
    return dict(purchase_lines=list(orders.values()), purchase_receipts=list(receipts.values()),
                purchase_inspections=list(inspections.values())), lineage
