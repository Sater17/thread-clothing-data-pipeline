"""Validate -> durable bronze -> distributed silver/gold -> gated publication."""
from pyspark.sql import functions as F

from .events import parse_rows
from .schema import project
from .transform import transform


class DataQualityError(RuntimeError):
    pass


def execute(spark, raw, catalog, store, monitor, save_report, publish):
    monitor.event("intake", "started")
    parsed = parse_rows(raw).persist()
    incoming = parsed.filter("validation_error IS NULL")
    quarantine = parsed.filter("validation_error IS NOT NULL")
    input_count, invalid_count = parsed.count(), quarantine.count()
    monitor.metrics(RecordsRead=input_count, RecordsQuarantined=invalid_count)
    monitor.event("bronze", "started")
    history = store.merge_bronze(incoming).persist()
    monitor.event("bronze", "completed")
    monitor.event("silver_gold", "started")
    tables = transform(history, catalog)
    tables["quarantine"] = quarantine
    issues = tables["quality_issues"].persist()
    issue_count = issues.count()
    pending_count = issues.filter("result = 'waiting'").count()
    metrics = dict(records_read=input_count, records_quarantined=invalid_count, quality_issues=issue_count, pending_references=pending_count)
    # Diagnostics are retained even when no new consumer publication is allowed.
    report = dict(run_token=monitor.token, quality="failed" if invalid_count or issue_count else "passed", metrics=metrics)
    save_report(report, project("quarantine", quarantine), issues)
    monitor.metrics(QualityFailures=issue_count + invalid_count, PendingReferences=pending_count)
    if invalid_count or issue_count:
        monitor.event("quality_gate", "failed", **metrics)
        raise DataQualityError("quality gate rejected publication: quarantine=%d issues=%d; see run audit" % (invalid_count, issue_count))
    monitor.event("publication", "started")
    snapshot_map = {}
    for name, frame in tables.items():
        output = project(name, frame)
        snapshot_map[name] = store.publish_table(name, output)
        monitor.event("table", "committed", table=name, snapshot_id=snapshot_map[name]["snapshot_id"])
    publication = dict(format_version=2, attempt=monitor.token, quality="passed", tables=snapshot_map, metrics=metrics)
    publish(publication)
    monitor.metrics(RunDurationSeconds=monitor.duration(), RunsSucceeded=1)
    monitor.event("publication", "completed", tables=len(snapshot_map))
    parsed.unpersist()
    history.unpersist()
    issues.unpersist()
    return publication
