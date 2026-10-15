"""Deterministic source fixtures, hourly manifests and business oracle."""

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from thread_etl.contracts import canonical_json


def event(source, scope, event_id, event_type, time, **payload):
    return dict(source=source, scope=scope, event_id=event_id, type=event_type,
                occurred_at=time, schema_version=1, payload=payload)


def endpoint(location, condition="sellable"):
    return dict(location=location, condition=condition)


def events():
    result = []
    def move(eid, time, quantity, origin, destination, reference):
        result.append(event("wms", "sg_inventory", eid, "stock_move", time,
                            sku="TS001-BLK-M", quantity=quantity,
                            **{"from": origin, "to": destination, "reference": reference}))
    move("RCV-001", "2026-09-01T09:00:00+08:00", 80,
         endpoint("external:supplier"), endpoint("SG-WH-01", "inspection"), "POL-001")
    move("QC-GOOD", "2026-09-01T10:00:00+08:00", 78,
         endpoint("SG-WH-01", "inspection"), endpoint("SG-WH-01"), "RCV-001")
    move("QC-BAD", "2026-09-01T10:01:00+08:00", 2,
         endpoint("SG-WH-01", "inspection"), endpoint("SG-WH-01", "defective"), "RCV-001")
    move("TI-001", "2026-09-02T09:00:00+08:00", 20,
         endpoint("SG-WH-01"), endpoint("transit:TRF-001"), "TRF-001")
    move("TR-001", "2026-09-02T15:00:00+08:00", 20,
         endpoint("transit:TRF-001"), endpoint("SG-ST-01"), "TRF-001")
    result.append(event("website", "sg_web", "ORDER-001", "order", "2026-09-03T10:15:00+08:00",
                        order="WEB-SG-1001", line="WL-001", sku="WEB-TEE-BLACK-M", quantity=2,
                        unit_price="30.00", discount="6.00", tax="0.00", total="54.00", currency="SGD"))
    result.append(event("oms", "brand_oms", "LINK-001", "order_link", "2026-09-03T10:16:00+08:00",
                        order="OMS-9001", line="OL-9001", origin_source="website", origin_scope="sg_web",
                        origin_order="WEB-SG-1001", origin_line="WL-001"))
    ref = dict(order_source="website", order_scope="sg_web", order="WEB-SG-1001")
    result.append(event("payment", "sg_payment_account", "PAY-001", "payment", "2026-09-03T10:17:00+08:00",
                        transaction="PAY-001", amount="54.00", currency="SGD", kind="charge", status="succeeded", **ref))
    hold = dict(reservation="RS-001", line="WL-001", sku="TS001-BLK-M", location="SG-WH-01", quantity=2, **ref)
    result.append(event("oms", "brand_oms", "RS-RESERVE", "reservation", "2026-09-03T10:18:00+08:00", action="reserve", **hold))
    move("GI-001", "2026-09-03T14:00:00+08:00", 2,
         endpoint("SG-WH-01"), endpoint("outbound:SHIP-001"), "AL-001")
    result.append(event("oms", "brand_oms", "RS-CONSUME", "reservation", "2026-09-03T14:00:00+08:00", action="consume", **hold))
    move("DELIVERY-MOVE", "2026-09-04T16:30:00+08:00", 2,
         endpoint("outbound:SHIP-001"), endpoint("external:customer"), "SHIP-001")
    result.append(event("carrier", "sg_carrier", "DEL-001", "delivery", "2026-09-04T16:30:00+08:00",
                        shipment="SHIP-001", line="WL-001", quantity=2, status="delivered", **ref))
    move("RR-001", "2026-09-07T09:00:00+08:00", 1,
         endpoint("external:customer"), endpoint("SG-WH-01", "inspection"), "RL-001")
    move("QC-001", "2026-09-07T10:00:00+08:00", 1,
         endpoint("SG-WH-01", "inspection"), endpoint("SG-WH-01"), "RR-001")
    result.append(event("payment", "sg_payment_account", "RF-001", "payment", "2026-09-08T11:00:00+08:00",
                        transaction="RF-001", original_transaction="PAY-001", amount="27.00",
                        currency="SGD", kind="refund", refund_reason="return", status="succeeded", **ref))
    result.append(event("oms", "brand_oms", "RET-001", "return_request", "2026-09-06T09:00:00+08:00",
                        **{"return": "RET-001", "return_line": "RL-001", "line": "WL-001", "sku": "TS001-BLK-M",
                           "quantity": 1, "reason": "size", **ref}))
    result.append(event("wms", "sg_inventory", "RR-DOC-001", "return_receipt", "2026-09-07T09:00:00+08:00",
                        receipt="RR-001", return_source="oms", return_scope="brand_oms", return_line="RL-001",
                        sku="TS001-BLK-M", location="SG-WH-01", quantity=1))
    result.append(event("wms", "sg_inventory", "QC-DOC-001", "return_inspection", "2026-09-07T10:00:00+08:00",
                        inspection="QC-001", receipt_source="wms", receipt_scope="sg_inventory", receipt="RR-001",
                        sku="TS001-BLK-M", location="SG-WH-01", quantity=1, condition="sellable"))
    result.append(event("payment", "sg_payment_account", "RA-001", "refund_allocation", "2026-09-08T11:00:00+08:00",
                        allocation="RA-001", transaction="RF-001", return_source="oms", return_scope="brand_oms",
                        return_line="RL-001", amount="27.00", currency="SGD"))
    result.append(event("procurement", "brand_procurement", "PO-001", "purchase_order_line", "2026-08-31T09:00:00+08:00",
                        purchase_order="PO-001", purchase_line="POL-001", supplier="SUP-001", sku="TS001-BLK-M",
                        location="SG-WH-01", quantity=100, unit_cost="12.00", currency="SGD", expected_at="2026-09-01T09:00:00+08:00"))
    result.append(event("wms", "sg_inventory", "RCV-DOC-001", "purchase_receipt", "2026-09-01T09:00:00+08:00",
                        receipt="RCV-001", purchase_source="procurement", purchase_scope="brand_procurement", purchase_line="POL-001",
                        sku="TS001-BLK-M", location="SG-WH-01", quantity=80))
    for eid, quantity, condition, time in [("QC-GOOD", 78, "sellable", "2026-09-01T10:00:00+08:00"),
                                           ("QC-BAD", 2, "defective", "2026-09-01T10:01:00+08:00")]:
        result.append(event("wms", "sg_inventory", eid + "-DOC", "purchase_inspection", time,
                            inspection=eid, receipt_source="wms", receipt_scope="sg_inventory", receipt="RCV-001",
                            sku="TS001-BLK-M", location="SG-WH-01", quantity=quantity, condition=condition))
    for item in result:
        doc = {"RR-001": ("return_receipt", "RR-001"), "QC-001": ("return_inspection", "QC-001")}.get(item["event_id"])
        if doc:
            item["payload"].update(document_type=doc[0], document_source="wms", document_scope="sg_inventory", document_id=doc[1])
        if item["event_id"] in ("RCV-001", "QC-GOOD", "QC-BAD"):
            kind = "purchase_receipt" if item["event_id"] == "RCV-001" else "purchase_inspection"
            item["payload"].update(document_type=kind, document_source="wms", document_scope="sg_inventory", document_id=item["event_id"])
    return result


def catalog():
    mappings = []
    for source, scope, code in [("website", "sg_web", "WEB-TEE-BLACK-M"),
                                ("wms", "sg_inventory", "TS001-BLK-M"),
                                ("oms", "brand_oms", "TS001-BLK-M"), ("procurement", "brand_procurement", "TS001-BLK-M")]:
        mappings.append(dict(source=source, scope=scope, code=code, canonical_id="SKU-0001",
                             valid_from="2026-01-01T00:00:00Z", valid_to=None))
    return dict(sku_mappings=mappings,
                locations={"SG-WH-01": "LOC-0005", "SG-ST-01": "LOC-0006"})


EXPECTED = {"warehouse_sellable": 57, "warehouse_defective": 2, "store_sellable": 20,
            "reserved": 0, "collected_SGD": "54.00", "refunded_SGD": "27.00"}


def generate(root):
    root = Path(root)
    if root.exists():
        raise FileExistsError("input destination already exists; use a new directory")
    root.mkdir(parents=True)
    (root / "catalog.json").write_text(json.dumps(catalog(), indent=2), encoding="utf-8")
    (root / "expected.json").write_text(json.dumps(EXPECTED, indent=2), encoding="utf-8")
    groups = defaultdict(list)
    for item in events():
        groups[(item["source"], item["scope"], item["occurred_at"][:13])].append(item)
    manifests = root / "manifests"
    manifests.mkdir()
    for index, ((source, scope, hour), items) in enumerate(sorted(groups.items())):
        bid = "B%03d" % index
        path = Path("raw") / source / bid / "part-001.jsonl"
        (root / path).parent.mkdir(parents=True)
        content = ("\n".join(canonical_json(e) for e in items) + "\n").encode("utf-8")
        (root / path).write_bytes(content)
        start = datetime.fromisoformat(hour + ":00:00+08:00")
        end = start + timedelta(hours=1)
        manifest = dict(batch_id=bid, source=source, scope=scope, schema_version=1,
                        period_start=start.isoformat(), period_end=end.isoformat(), data_kind="events",
                        received_at=(end + timedelta(minutes=10)).isoformat(), record_count=len(items),
                        files=[dict(path=path.as_posix(), sha256=hashlib.sha256(content).hexdigest())])
        (manifests / (bid + ".json")).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return root
