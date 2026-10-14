import json

import pytest
from pyspark.sql import functions as F

from thread_etl.events import parse_rows, resolve_versions
from thread_etl.intake import file_plan, read_inputs
from thread_etl.transform import transform
from thread_etl.contracts import canonical_json
from tools.simulator.history import generate_history

pytestmark = pytest.mark.spark


def test_two_markets_snapshots_returns_purchases_and_settlements(spark, tmp_path):
    import hashlib
    folder = tmp_path / "input"
    generate_history(folder, days=2, orders_per_day=20, sku_count=3, seed=42, daily_snapshots=True)
    selected = [dict(name=path.name, digest=hashlib.sha256(canonical_json(json.loads(path.read_text())).encode()).hexdigest())
        for path in sorted((folder / "manifests").glob("*.json"))]
    history = parse_rows(read_inputs(spark, file_plan(folder.as_uri(), selected))).persist()
    assert history.filter("validation_error IS NOT NULL").count() == 0
    tables = transform(history, json.loads((folder / "catalog.json").read_text()))
    issues = tables["quality_issues"].collect()
    assert not issues, issues
    expected = json.loads((folder / "expected-history.json").read_text())
    assert tables["orders"].count() == expected["order_count"]
    assert tables["order_lines"].count() == expected["line_count"]
    actual_inventory = [row.asDict() for row in tables["inventory"].filter("quantity <> 0 AND NOT location LIKE 'external:%'").orderBy("sku_id", "location", "condition").collect()]
    assert actual_inventory == expected["inventory"]
    money = tables["payments"].filter("status = 'succeeded'").groupBy("kind", "currency").agg(F.sum("amount").alias("amount"))
    actual_money = [dict(kind=row.kind, currency=row.currency, amount=format(row.amount, ".2f")) for row in money.orderBy("kind", "currency").collect() if row.amount]
    assert actual_money == expected["money"]
    assert tables["inventory_reconciliation"].filter("result <> 'passed'").count() == 0
    assert tables["settlement_reconciliation"].filter("result NOT IN ('passed','carry')").count() == 0
    fields = ("source", "scope", "purchase_line", "quantity", "received_quantity", "outstanding_quantity", "accepted_quantity", "defective_quantity", "uninspected_quantity", "over_received_quantity")
    actual_purchases = [row.asDict() for row in tables["purchase_lines"].select(*fields).orderBy("source", "scope", "purchase_line").collect()]
    for row in actual_purchases:
        row["source_purchase_line"] = row.pop("purchase_line")
    assert actual_purchases == json.loads((folder / "expected-purchases.json").read_text())


def test_revision_conflict_rejection_and_newest_missing_mapping(spark):
    from test_spark_business import history_frame
    from tools.simulator.sample import events
    original = next(event for event in events() if event["type"] == "payment" and event["payload"]["kind"] == "charge")
    changed = json.loads(json.dumps(original))
    changed["event_id"] = "revised"
    changed["source_version"] = 2
    changed["payload"]["amount"] = "56.00"
    hist = history_frame(spark, [changed, original, original])
    _, current, issues = resolve_versions(hist)
    assert current.first().event_id == "revised"
    assert issues.count() == 0
    conflict = json.loads(json.dumps(changed))
    conflict["event_id"] = "conflict"
    conflict["payload"]["amount"] = "57.00"
    _, _, issues = resolve_versions(history_frame(spark, [original, changed, conflict]))
    assert issues.filter("check = 'entity_version_conflict'").count() == 1
    rejection = dict(source="pipeline_control", scope="brand", event_id="reject-conflict", type="event_rejection", occurred_at="2026-10-14T00:00:00Z", schema_version=1,
        payload=dict(target_source=conflict["source"], target_scope=conflict["scope"], target_event_id="conflict", reason="adjudicated"))
    _, _, issues = resolve_versions(history_frame(spark, [original, changed, conflict, rejection]))
    assert issues.count() == 0
