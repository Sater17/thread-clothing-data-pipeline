"""Glue Spark entrypoint. Business rules are packaged in thread_runtime.zip."""
import argparse
import hashlib
import json
import sys
import time

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from thread_etl.contracts import canonical_json
from thread_etl.intake import file_plan, read_inputs, read_json
from thread_etl.observability import RunMonitor
from thread_etl.pipeline import execute
from thread_etl.storage import IcebergStore


def main():
    parser = argparse.ArgumentParser()
    for name in ("JOB_NAME", "bucket", "input-prefix", "output-prefix", "plan-key", "run-token", "catalog-database", "batch-table", "pipeline-name"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--max-files", type=int, default=1000)
    args, _ = parser.parse_known_args()
    spark_context = SparkContext.getOrCreate()
    context = GlueContext(spark_context)
    spark = context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark_context.setCheckpointDir("s3://" + args.bucket + "/" + args.output_prefix + "/checkpoints/" + args.run_token)
    job = Job(context)
    job.init(args.JOB_NAME, vars(args))
    monitor = RunMonitor(args.run_token, args.pipeline_name, boto3.client("cloudwatch"))
    s3 = boto3.client("s3")
    table = boto3.resource("dynamodb").Table(args.batch_table)
    root = "s3://" + args.bucket + "/" + args.input_prefix
    run_prefix = args.output_prefix + "/runs/" + args.run_token
    latest_key = args.output_prefix + "/latest.json"
    previous_etag = None
    try:
        previous_etag = s3.head_object(Bucket=args.bucket, Key=latest_key)["ETag"]
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    try:
        plan = read_json("s3://" + args.bucket + "/" + args.plan_key)
        if plan["run_token"] != args.run_token:
            raise ValueError("plan ownership mismatch")
        catalog = read_json(root + "/catalog.json")
        if hashlib.sha256(canonical_json(catalog).encode()).hexdigest() != plan["catalog_sha256"]:
            raise ValueError("catalog changed after planning")
        selected = []
        for manifest in plan["manifests"]:
            prefix = args.input_prefix + "/manifests/"
            if not manifest["key"].startswith(prefix) or "/" in manifest["key"][len(prefix):]:
                raise ValueError("manifest outside source prefix")
            selected.append(dict(name=manifest["key"][len(prefix):], digest=manifest["digest"]))
        raw = read_inputs(spark, file_plan(root, selected, args.max_files))
        store = IcebergStore(spark, "thread", args.catalog_database)

        def save_report(report, quarantine, issues):
            report.update(format_version=2, pipeline=args.pipeline_name)
            s3.put_object(Bucket=args.bucket, Key=run_prefix + "/audit.json", Body=json.dumps(report).encode(), ContentType="application/json")
            quarantine.write.mode("overwrite").parquet("s3://" + args.bucket + "/" + run_prefix + "/quarantine")
            issues.write.mode("overwrite").parquet("s3://" + args.bucket + "/" + run_prefix + "/quality_issues")

        def publish(publication):
            lease = table.get_item(Key={"pk": "PIPELINE#" + args.pipeline_name, "sk": "LEASE"}, ConsistentRead=True).get("Item", {})
            if lease.get("token") != args.run_token or int(lease.get("expires", 0)) <= time.time():
                raise ValueError("run no longer owns the publication lease")
            publication.update(audit_key=run_prefix + "/audit.json", run_id=args.run_token, pipeline=args.pipeline_name)
            condition = dict(IfMatch=previous_etag) if previous_etag else dict(IfNoneMatch="*")
            s3.put_object(Bucket=args.bucket, Key=latest_key, Body=json.dumps(publication).encode(), ContentType="application/json", **condition)

        execute(spark, raw, catalog, store, monitor, save_report, publish)
        job.commit()
    except Exception as exc:
        monitor.event("run", "failed", error_type=type(exc).__name__, error=str(exc)[:1500])
        monitor.metrics(RunsFailed=1, RunDurationSeconds=monitor.duration())
        raise


if __name__ == "__main__":
    main()
