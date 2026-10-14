import json

import pytest

from thread_etl.events import parse_rows
from thread_etl.observability import RunMonitor
from thread_etl.pipeline import DataQualityError, execute
from thread_etl.storage import IcebergStore, read_publication
from tools.simulator.sample import catalog, events

pytestmark = pytest.mark.spark


def raw_frame(spark, invalid=False):
    rows = [dict(value=json.dumps(event), manifest_source=event["source"], manifest_scope=event["scope"],
        raw_file="raw/fixture.jsonl", batch="B1", received_at="2026-10-14T00:00:00Z") for event in events()]
    if invalid:
        rows.append(dict(value="{bad json", manifest_source="website", manifest_scope="sg_web",
            raw_file="raw/bad.jsonl", batch="BAD", received_at="2026-10-14T00:00:00Z"))
    return spark.createDataFrame(rows)


def test_bronze_retry_is_idempotent_and_publication_pins_old_snapshot(spark):
    store = IcebergStore(spark, "thread", "idempotency")
    incoming = parse_rows(raw_frame(spark)).filter("validation_error IS NULL")
    first = store.merge_bronze(incoming).count()
    assert store.merge_bronze(incoming).count() == first == 24
    original = spark.createDataFrame([("old", "SGD")], "order_id string, currency string")
    entry = store.publish_table("orders", original)
    store.publish_table("orders", spark.createDataFrame([("new", "SGD")], original.schema))
    published = read_publication(spark, {"tables": {"orders": entry}}, "orders")
    assert [row.order_id for row in published.collect()] == ["old"]


def test_quality_gate_retains_diagnostics_without_publication(spark):
    publications, reports = [], []
    store = IcebergStore(spark, "thread", "quality_gate")
    def report(body, quarantine, issues):
        reports.append((body, quarantine.count(), issues.count()))
    with pytest.raises(DataQualityError, match="quarantine=1"):
        execute(spark, raw_frame(spark, invalid=True), catalog(), store, RunMonitor("bad-run", "tests"), report, publications.append)
    assert not publications
    assert reports[0][0]["quality"] == "failed"
    assert reports[0][1] == 1
    assert store.spark.table(store.table("bronze_events")).count() == 24


def test_full_publication_has_typed_tables_and_snapshot_reader(spark):
    publications = []
    store = IcebergStore(spark, "thread", "full_pipeline")
    marker = execute(spark, raw_frame(spark), catalog(), store, RunMonitor("valid-run", "tests"), lambda *args: None, publications.append)
    assert marker["quality"] == "passed" and len(publications) == 1
    assert len(marker["tables"]) == 24
    payments = read_publication(spark, marker, "payments")
    assert payments.schema["amount"].dataType.simpleString() == "decimal(20,2)"
    assert read_publication(spark, marker, "pending").count() == 0
    assert read_publication(spark, marker, "stock_lineage").count() == 5
