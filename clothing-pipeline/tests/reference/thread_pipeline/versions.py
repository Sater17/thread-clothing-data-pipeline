"""Choose full-record revisions by source version, never arrival time."""

from collections import defaultdict

from .contracts import ContractError, canonical_json


def event_key(event):
    return event["source"], event["scope"], event["event_id"]


def entity_key(event):
    p = event["payload"]
    base = (event["source"], event["scope"], event["type"])
    if event["type"] == "order":
        return base + (p["order"], p["line"])
    if event["type"] == "payment":
        return base + (p["transaction"],)
    if event["type"] == "inventory_snapshot":
        return base + (p["snapshot"], p["sku"], p["location"], p["condition"])
    if event["type"] == "purchase_order_line":
        return base + (p["purchase_order"], p["purchase_line"])
    if event["type"] == "settlement_line":
        return base + (p["settlement"], p["settlement_line"])
    if event["type"] == "settlement":
        return base + (p["settlement"],)
    if event["type"] == "payout_transfer":
        return base + (p["transaction"],)
    return None


def select_current(events):
    groups = defaultdict(list)
    selected = set()
    rejected = {(e["payload"]["target_source"], e["payload"]["target_scope"], e["payload"]["target_event_id"])
                for e in events if e["type"] == "event_rejection"}
    for event in events:
        if event_key(event) in rejected:
            continue
        key = entity_key(event)
        if key is None:
            selected.add(event_key(event))
        else:
            groups[key].append(event)
    for key, revisions in sorted(groups.items()):
        versions = defaultdict(list)
        for revision in revisions:
            versions[revision.get("source_version", 1)].append(revision)
        for version, copies in versions.items():
            if len({canonical_json(e["payload"]) for e in copies}) != 1:
                raise ContractError("conflicting payloads for entity/version: " + str(key) + "/" + str(version))
        # Payment identity cannot be silently transferred to another order/currency.
        if key[2] == "payment":
            immutable = ("order_source", "order_scope", "order", "currency", "kind", "original_transaction")
            if len({tuple(e["payload"].get(field) for field in immutable) for e in revisions}) != 1:
                raise ContractError("payment identity changed across versions: " + str(key))
        if key[2] == "purchase_order_line":
            immutable = ("supplier", "sku", "location", "currency")
            if len({tuple(e["payload"][field] for field in immutable) for e in revisions}) != 1:
                raise ContractError("purchase line identity changed across versions: " + str(key))
        if key[2] in ("settlement_line", "payout_transfer"):
            immutable = ("order_source", "order_scope", "order", "currency") if key[2] == "settlement_line" else ("settlement_source", "settlement_scope", "settlement", "currency")
            if len({tuple(e["payload"][field] for field in immutable) for e in revisions}) != 1:
                raise ContractError("settlement/payment identity changed across versions: " + str(key))
        newest = sorted(versions[max(versions)], key=event_key)[0]
        selected.add(event_key(newest))
    return selected
