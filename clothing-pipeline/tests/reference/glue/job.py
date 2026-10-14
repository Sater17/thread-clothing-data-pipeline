"""Glue Python Shell adapter. Build standalone script with scripts/build_glue.py.

One writer only. Full-ledger replay is intentional for the small first slice.
No Glue resources are created by this file.
"""

import argparse
import json
import tempfile
import uuid
import hashlib
import sqlite3
from pathlib import Path, PurePosixPath

from thread_pipeline.engine import run
from thread_pipeline.contracts import canonical_json


def safe_destination(root, relative):
    path = (root / relative).resolve()
    if root.resolve() not in path.parents:
        raise ValueError("S3 input path escapes local staging")
    return path


def download(s3, bucket, key, destination, allow_missing=False):
    from botocore.exceptions import ClientError
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if allow_missing and exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return False
        raise
    with response["Body"] as body:
        destination.write_bytes(body.read())
    return True


def execute(s3, bucket, input_prefix, output_prefix, parquet=False, plan_key=None, run_token=None, catalog_client=None, database=None, max_records=None):
    input_prefix, output_prefix = input_prefix.strip("/"), output_prefix.strip("/")
    if not input_prefix or not output_prefix or input_prefix == output_prefix:
        raise ValueError("distinct nonempty prefixes required")
    if input_prefix.startswith(output_prefix + "/") or output_prefix.startswith(input_prefix + "/"):
        raise ValueError("input/output prefixes cannot overlap")
    attempt = run_token or uuid.uuid4().hex
    if not attempt.isalnum():
        raise ValueError("invalid run token")
    run_prefix = output_prefix + "/runs/" + attempt
    with tempfile.TemporaryDirectory() as temp:
        work = Path(temp)
        source = work / "input"
        (source / "manifests").mkdir(parents=True)
        download(s3, bucket, input_prefix + "/catalog.json", source / "catalog.json")
        # Snapshot the manifest key list before downloading raw.
        keys, digests = [], {}
        input_records = 0
        if plan_key:
            if not plan_key.startswith(output_prefix + "/plans/"):
                raise ValueError("plan outside output prefix")
            response = s3.get_object(Bucket=bucket, Key=plan_key)
            with response["Body"] as body:
                frozen = json.loads(body.read())
            if frozen["run_token"] != attempt:
                raise ValueError("run token differs from plan")
            actual_catalog = hashlib.sha256(canonical_json(json.loads((source / "catalog.json").read_text(encoding="utf-8"))).encode()).hexdigest()
            if frozen.get("catalog_sha256", actual_catalog) != actual_catalog:
                raise ValueError("catalog changed after plan")
            keys = [m["key"] for m in frozen["manifests"]]
            digests = {m["key"]: m["digest"] for m in frozen["manifests"]}
        else:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=input_prefix + "/manifests/"):
                keys.extend(item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".json"))
        for key in sorted(keys):
            if not key.startswith(input_prefix + "/manifests/"):
                raise ValueError("manifest outside input prefix")
            local = safe_destination(source, key[len(input_prefix) + 1:])
            download(s3, bucket, key, local)
            manifest = json.loads(local.read_text(encoding="utf-8"))
            input_records += manifest["record_count"]
            if max_records is not None and input_records > max_records:
                raise ValueError("input exceeds configured record limit")
            if key in digests and hashlib.sha256(canonical_json(manifest).encode()).hexdigest() != digests[key]:
                raise ValueError("manifest changed after plan")
            for entry in manifest["files"]:
                relative = entry["path"]
                if PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts:
                    raise ValueError("unsafe manifest path")
                download(s3, bucket, input_prefix + "/" + relative,
                         safe_destination(source, relative), allow_missing=True)
        state = work / "ledger.sqlite"
        latest = work / "previous.json"
        if download(s3, bucket, output_prefix + "/latest.json", latest, allow_missing=True):
            marker = json.loads(latest.read_text())
            state_key = marker["state_key"]
            if not state_key.startswith(output_prefix + "/runs/"):
                raise ValueError("state pointer outside output prefix")
            download(s3, bucket, state_key, state)
        if max_records is not None and state.exists():
            connection = sqlite3.connect(str(state))
            try:
                existing = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            finally:
                connection.close()
            if existing + input_records > max_records:
                raise ValueError("ledger plus selected input exceeds configured record limit")
        result, audit = run(source, state, work / "output", parquet=parquet)
        # Checkpoint and outputs use a new prefix; publish pointer last.
        s3.put_object(Bucket=bucket, Key=run_prefix + "/ledger.sqlite", Body=state.read_bytes())
        for name, value in (("result.json", result), ("audit.json", audit)):
            s3.put_object(Bucket=bucket, Key=run_prefix + "/" + name,
                          Body=json.dumps(value, ensure_ascii=False).encode("utf-8"), ContentType="application/json")
        marker = json.loads((work / "output" / "latest.json").read_text())
        if marker.get("parquet"):
            tables = work / "output" / "runs" / marker["run_id"] / "tables"
            for table in marker["parquet"].values():
                s3.put_object(Bucket=bucket, Key=run_prefix + "/tables/" + table["file"],
                              Body=(tables / table["file"]).read_bytes())
            marker["tables_prefix"] = run_prefix + "/tables/"
            if catalog_client and database:
                from thread_pipeline.catalog import register_tables
                marker["catalog"] = register_tables(catalog_client, database, bucket, run_prefix, attempt, tables)
        marker.update(state_key=run_prefix + "/ledger.sqlite", result_key=run_prefix + "/result.json",
                      audit_key=run_prefix + "/audit.json", attempt=attempt)
        s3.put_object(Bucket=bucket, Key=output_prefix + "/latest.json",
                      Body=json.dumps(marker).encode(), ContentType="application/json")
        return marker


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input-prefix", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--parquet", action="store_true")
    parser.add_argument("--plan-key")
    parser.add_argument("--run-token")
    parser.add_argument("--catalog-database")
    parser.add_argument("--max-records", type=int)
    args, _ = parser.parse_known_args()  # Glue adds its own runtime arguments.
    print(json.dumps(execute(boto3.client("s3"), args.bucket, args.input_prefix, args.output_prefix, parquet=args.parquet,
                             plan_key=args.plan_key, run_token=args.run_token,
                             catalog_client=boto3.client("glue") if args.catalog_database else None, database=args.catalog_database, max_records=args.max_records)))


if __name__ == "__main__":
    main()
