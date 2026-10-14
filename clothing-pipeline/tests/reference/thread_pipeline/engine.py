"""Durable event ledger and replayable reference implementation (single runner)."""

import hashlib
import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from . import VERSION
from .contracts import ContractError, canonical_json, timestamp, validate
from .versions import event_key, select_current
from .returns import integrate_returns
from .purchases import integrate_purchases
from .settlements import integrate_settlements


class BatchError(ValueError):
    pass


def read_batch(root, manifest_path):
    root = Path(root).resolve()
    m = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    for field in ("batch_id", "source", "scope", "schema_version", "received_at", "record_count", "files", "period_start", "period_end", "data_kind"):
        if field not in m:
            raise BatchError("manifest missing " + field)
    timestamp(m["received_at"])
    if timestamp(m["period_start"]) >= timestamp(m["period_end"]) or m["data_kind"] not in ("events", "snapshot_rows"):
        raise BatchError("invalid period or unsupported data_kind")
    if not isinstance(m["files"], list):
        raise BatchError("files must be a list")
    for field in ("batch_id", "source", "scope"):
        if not isinstance(m[field], str) or not m[field]:
            raise BatchError("invalid " + field)
    if m["schema_version"] != 1 or type(m["record_count"]) is not int or m["record_count"] < 0:
        raise BatchError("invalid manifest version/count")
    records = []
    seen_paths = set()
    for entry in m["files"]:
        path = (root / entry["path"]).resolve()
        if root not in path.parents or path in seen_paths:
            raise BatchError("unsafe or duplicate file path")
        seen_paths.add(path)
        if not path.is_file():
            raise BatchError("missing file: " + entry["path"])
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise BatchError("checksum mismatch: " + entry["path"])
        for line_no, line in enumerate(content.decode("utf-8").splitlines(), 1):
            if not line.strip():
                raise BatchError("blank JSONL line")
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                event = {"invalid_json": line}
            records.append((event, entry["path"], line_no))
    if len(records) != m["record_count"]:
        raise BatchError("record count mismatch")
    return m, records


def open_ledger(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path))
    db.execute("CREATE TABLE IF NOT EXISTS events (key TEXT PRIMARY KEY, body TEXT, provenance TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS batches (key TEXT PRIMARY KEY, digest TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS quarantine (key TEXT PRIMARY KEY, body TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS receptions (key TEXT PRIMARY KEY, body TEXT)")
    return db


def ingest(db, root):
    report = dict(accepted=0, duplicates=0, quarantined=0, batches_replayed=0, blocked=[])
    for path in sorted((Path(root) / "manifests").glob("*.json")):
        try:
            manifest, rows = read_batch(root, path)
        except (BatchError, ContractError, ValueError, KeyError, TypeError, OSError) as exc:
            report["blocked"].append(dict(manifest=path.name, reason=str(exc)))
            continue
        batch_key = canonical_json([manifest["source"], manifest["scope"], manifest["batch_id"]])
        digest = hashlib.sha256(canonical_json(manifest).encode()).hexdigest()
        previous = db.execute("SELECT digest FROM batches WHERE key=?", (batch_key,)).fetchone()
        if previous:
            if previous[0] != digest:
                report["blocked"].append(dict(manifest=path.name, reason="batch identity conflict"))
            else:
                report["batches_replayed"] += 1
            continue
        with db:
            for event, file, line in rows:
                provenance = dict(batch=manifest["batch_id"], file=file, line=line, received_at=manifest["received_at"])
                body = canonical_json(event)
                try:
                    validate(event)
                    if event["source"] != manifest["source"] or event["scope"] != manifest["scope"]:
                        raise ContractError("source differs from manifest")
                    key = canonical_json([event["source"], event["scope"], event["event_id"]])
                    old = db.execute("SELECT body FROM events WHERE key=?", (key,)).fetchone()
                    if old and old[0] != body:
                        raise ContractError("event identity conflict")
                    if old:
                        report["duplicates"] += 1
                    else:
                        db.execute("INSERT INTO events VALUES (?,?,?)", (key, body, canonical_json(provenance)))
                        report["accepted"] += 1
                    receipt = dict(event_key=key, provenance=provenance, duplicate=bool(old))
                    db.execute("INSERT INTO receptions VALUES (?,?)", (canonical_json([batch_key, file, line]), canonical_json(receipt)))
                except (ContractError, ValueError, TypeError) as exc:
                    record = dict(event=event, provenance=provenance, reason=str(exc))
                    qkey = canonical_json([batch_key, file, line])
                    db.execute("INSERT OR REPLACE INTO quarantine VALUES (?,?)", (qkey, canonical_json(record)))
                    report["quarantined"] += 1
            db.execute("INSERT INTO batches VALUES (?,?)", (batch_key, digest))
    return report


def resolve_sku(event, catalog, index=None):
    matches = []
    time = timestamp(event["payload"]["as_of"] if event["type"] == "inventory_snapshot" else event["occurred_at"])
    candidates = catalog["sku_mappings"] if index is None else index.get((event["source"], event["scope"], event["payload"]["sku"]), [])
    for item in candidates:
        if (item["source"], item["scope"], item["code"]) != (event["source"], event["scope"], event["payload"]["sku"]):
            continue
        if timestamp(item["valid_from"]) <= time and (item["valid_to"] is None or time < timestamp(item["valid_to"])):
            matches.append(item["canonical_id"])
    if len(matches) != 1:
        raise ContractError("missing or ambiguous SKU mapping")
    return matches[0]


def stable_id(prefix, key):
    return prefix + hashlib.sha256(canonical_json(key).encode()).hexdigest()[:20]


def integrate(db, catalog):
    standardized, pending, issues = [], [], []
    mapping_index = defaultdict(list)
    for mapping in catalog["sku_mappings"]:
        mapping_index[(mapping["source"], mapping["scope"], mapping["code"])].append(mapping)
    raw_events = []
    for body, provenance in db.execute("SELECT body, provenance FROM events ORDER BY key"):
        e = json.loads(body)
        e["provenance"] = json.loads(provenance)
        raw_events.append(e)
    selected = select_current(raw_events)
    rejected = {(e["payload"]["target_source"], e["payload"]["target_scope"], e["payload"]["target_event_id"])
                for e in raw_events if e["type"] == "event_rejection"}
    known = {event_key(e) for e in raw_events}
    for key in sorted(rejected - known):
        issues.append(dict(check="rejection_target_missing", result="waiting", target=list(key)))
    for e in raw_events:
        e["source_version"] = e.get("source_version", 1)
        e["is_current"] = event_key(e) in selected
        e["rejected"] = event_key(e) in rejected
        try:
            if "sku" in e["payload"]:
                e["sku_id"] = resolve_sku(e, catalog, mapping_index)
            standardized.append(e)
        except ContractError as exc:
            pending.append(dict(event=e, reason=str(exc)))
    current = [e for e in standardized if e["is_current"]]
    orders, lines = {}, {}
    for e in current:
        if e["type"] != "order":
            continue
        p = e["payload"]
        key = (e["source"], e["scope"], p["order"])
        oid = stable_id("ORD-", key)
        lid = stable_id("LINE-", key + (p["line"],))
        if key in orders and orders[key]["currency"] != p["currency"]:
            raise ContractError("order currency conflict")
        orders[key] = dict(order_id=oid, currency=p["currency"])
        linekey = key + (p["line"],)
        value = dict(order_id=oid, line_id=lid, sku_id=e["sku_id"], source_version=e["source_version"], **p)
        if linekey in lines and lines[linekey] != value:
            raise ContractError("order line identity conflict")
        lines[linekey] = value
    links = []
    for e in current:
        if e["type"] == "order_link":
            p = e["payload"]
            origin = (p["origin_source"], p["origin_scope"], p["origin_order"], p["origin_line"])
            if origin not in lines:
                pending.append(dict(event=e, reason="missing origin order line"))
            else:
                links.append(dict(source=e["source"], scope=e["scope"], order=p["order"], line=p["line"],
                                  order_id=lines[origin]["order_id"], line_id=lines[origin]["line_id"]))
    balances, holds = defaultdict(int), defaultdict(int)
    payments, delivered = {}, defaultdict(int)
    inventory_reconciliation = []
    # Snapshot at T includes all stock movements with occurred_at <= T.
    def timeline_key(e):
        snapshot = e["type"] == "inventory_snapshot"
        time = e["payload"]["as_of"] if snapshot else e["occurred_at"]
        return timestamp(time), 1 if snapshot else 0, event_key(e)
    for e in sorted(current, key=timeline_key):
        p = e["payload"]
        kind = e["type"]
        if kind == "inventory_snapshot":
            loc = catalog["locations"].get(p["location"])
            if not loc:
                pending.append(dict(event=e, reason="missing snapshot location mapping"))
                continue
            actual = balances.get((e["sku_id"], loc, p["condition"]), 0)
            row = dict(snapshot=p["snapshot"], source=e["source"], scope=e["scope"],
                       event_id=e["event_id"], raw_file=e["provenance"]["file"], batch=e["provenance"]["batch"],
                       source_version=e["source_version"], as_of=p["as_of"], sku_id=e["sku_id"], location=loc,
                       condition=p["condition"], expected=p["quantity"], actual=actual,
                       delta=actual - p["quantity"], result="passed" if actual == p["quantity"] else "failed")
            inventory_reconciliation.append(row)
            if actual != p["quantity"]:
                issues.append(dict(check="inventory_snapshot", **row))
        elif kind == "stock_move":
            endpoints = []
            for side in ("from", "to"):
                loc = p[side]["location"]
                if loc.startswith(("external:", "transit:", "outbound:")):
                    endpoints.append(loc)
                elif loc in catalog["locations"]:
                    endpoints.append(catalog["locations"][loc])
                else:
                    break
            if len(endpoints) != 2:
                pending.append(dict(event=e, reason="missing location mapping"))
                continue
            for side, sign, loc in zip(("from", "to"), (-1, 1), endpoints):
                balances[(e["sku_id"], loc, p[side]["condition"])] += sign * p["quantity"]
        elif kind in ("payment", "reservation", "delivery"):
            order_key = (p["order_source"], p["order_scope"], p["order"])
            if order_key not in orders:
                pending.append(dict(event=e, reason="missing order"))
                continue
            if kind != "payment" and order_key + (p["line"],) not in lines:
                pending.append(dict(event=e, reason="missing order line"))
                continue
            if kind == "payment":
                txkey = (e["source"], e["scope"], p["transaction"])
                value = dict(order_id=orders[order_key]["order_id"], source_version=e["source_version"], **p)
                if txkey in payments and payments[txkey] != value:
                    raise ContractError("payment transaction identity conflict")
                payments[txkey] = value
            elif kind == "reservation":
                loc = catalog["locations"].get(p["location"])
                if not loc:
                    pending.append(dict(event=e, reason="missing location mapping"))
                    continue
                holds[(p["reservation"], e["sku_id"], loc)] += p["quantity"] * (1 if p["action"] == "reserve" else -1)
            else:
                delivered[order_key + (p["line"],)] += p["quantity"]
    totals = defaultdict(lambda: Decimal("0"))
    order_totals = defaultdict(lambda: Decimal("0"))
    for key, p in payments.items():
        if p["kind"] == "refund":
            original = payments.get(key[:2] + (p["original_transaction"],))
            if not original:
                issues.append(dict(check="refund_reference", result="waiting", transaction=key[2]))
            elif original["kind"] != "charge" or original["currency"] != p["currency"] or original["order_id"] != p["order_id"]:
                issues.append(dict(check="refund_reference", result="failed", transaction=key[2]))
        if p["status"] == "succeeded":
            totals[(p["kind"], p["currency"])] += Decimal(p["amount"])
            order_totals[(p["order_id"], p["currency"], p["kind"])] += Decimal(p["amount"])
    for oid, currency in sorted({(k[0], k[1]) for k in order_totals}):
        if order_totals[(oid, currency, "refund")] > order_totals[(oid, currency, "charge")]:
            issues.append(dict(check="refund_exceeds_collected", result="failed", currency=currency, order_id=oid))
    for key, quantity in balances.items():
        if not key[1].startswith("external:") and quantity < 0:
            issues.append(dict(check="negative_inventory", result="failed", key=list(key)))
    for key, quantity in holds.items():
        if quantity < 0:
            issues.append(dict(check="negative_reservation", result="failed", reservation=key[0]))
    held_per_location = defaultdict(int)
    for (_, sku, loc), quantity in holds.items():
        held_per_location[(sku, loc)] += quantity
    for (sku, loc), quantity in held_per_location.items():
        if quantity > balances[(sku, loc, "sellable")]:
            issues.append(dict(check="reservation_exceeds_sellable", result="failed", sku=sku, location=loc))
    for key, quantity in delivered.items():
        if quantity > lines[key]["quantity"]:
            issues.append(dict(check="delivery_exceeds_order", result="failed", line=lines[key]["line_id"]))
    returns = integrate_returns(current, lines, payments, delivered, catalog, pending, issues, stable_id)
    purchases, purchase_lineage = integrate_purchases(current, catalog, pending, issues, stable_id, history=standardized)
    returns["stock_lineage"].extend(purchase_lineage)
    settlements = integrate_settlements(current, orders, payments, pending, issues, stable_id)
    purchase_fields = ("source", "scope", "source_purchase_line", "quantity", "received_quantity", "outstanding_quantity",
                       "accepted_quantity", "defective_quantity", "uninspected_quantity", "over_received_quantity")
    purchase_reconciliation = [{field: row[field] for field in purchase_fields} for row in purchases["purchase_lines"]]
    purchase_reconciliation.sort(key=lambda row: (row["source"], row["scope"], row["source_purchase_line"]))
    summary = dict(warehouse_sellable=balances.get(("SKU-0001", "LOC-0005", "sellable"), 0),
                   warehouse_defective=balances.get(("SKU-0001", "LOC-0005", "defective"), 0),
                   store_sellable=balances.get(("SKU-0001", "LOC-0006", "sellable"), 0),
                   reserved=sum(holds.values()), collected_SGD=format(totals[("charge", "SGD")], ".2f"),
                   refunded_SGD=format(totals[("refund", "SGD")], ".2f"))
    reconciliation = dict(order_count=len(orders), line_count=len(lines), reserved=sum(holds.values()),
                          money=[dict(kind=k[0], currency=k[1], amount=format(v, ".2f")) for k, v in sorted(totals.items()) if v],
                          inventory=[dict(sku_id=k[0], location=k[1], condition=k[2], quantity=v) for k, v in sorted(balances.items()) if v and not k[1].startswith("external:")])
    return dict(standardized=standardized, orders=list(orders.values()), order_lines=list(lines.values()),
                source_links=links, payments=list(payments.values()),
                inventory=[dict(sku_id=k[0], location=k[1], condition=k[2], quantity=v) for k, v in sorted(balances.items())],
                pending=pending, checks=issues, summary=summary, reconciliation=reconciliation,
                inventory_reconciliation=inventory_reconciliation, purchase_reconciliation=purchase_reconciliation, **returns, **purchases, **settlements)


def run(root, state, output, fail_before_publish=False, parquet=False):
    root, output = Path(root), Path(output)
    catalog = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
    with closing(open_ledger(state)) as db:
        intake = ingest(db, root)
        result = integrate(db, catalog)
        quarantine = [json.loads(row[0]) for row in db.execute("SELECT body FROM quarantine ORDER BY key")]
        receptions = [json.loads(row[0]) for row in db.execute("SELECT body FROM receptions ORDER BY key")]
        inputs = [dict(batch_key=key, digest=digest) for key, digest in db.execute("SELECT key,digest FROM batches ORDER BY key")]
    result["quarantine"] = quarantine
    result["receptions"] = receptions
    digest = hashlib.sha256(canonical_json(result).encode()).hexdigest()
    run_id = "run-" + digest[:20]
    snapshot = output / "runs" / run_id
    snapshot.mkdir(parents=True, exist_ok=True)
    (snapshot / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    parquet_manifest = None
    if parquet:
        from .parquet import export
        parquet_manifest = export(result, snapshot / "tables")
    audit = dict(version=VERSION, run_id=run_id, input_batches=inputs, catalog_sha256=hashlib.sha256(canonical_json(catalog).encode()).hexdigest(), intake=intake, pending=len(result["pending"]),
                 quarantined=len(quarantine), checks=result["checks"])
    (output / "audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    if fail_before_publish:
        raise RuntimeError("injected failure before publication")
    # Atomic pointer replacement locally. A consumer always follows this pointer.
    marker = output / "latest.tmp"
    marker.write_text(json.dumps(dict(run_id=run_id, result="runs/" + run_id + "/result.json", parquet=parquet_manifest,
                                     quality="issues" if quarantine or result["pending"] or result["checks"] or intake["blocked"] else "passed")), encoding="utf-8")
    marker.replace(output / "latest.json")
    return result, audit
