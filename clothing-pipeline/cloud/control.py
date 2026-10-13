"""Manifest registration, immutable run plans, single-writer lease and completion."""

import hashlib
import json
import os
import time
import uuid
from pathlib import PurePosixPath

from thread_pipeline.contracts import canonical_json, timestamp


def inspect_manifest(s3, bucket, key, input_prefix):
    if not key.startswith(input_prefix + "/manifests/") or not key.endswith(".json"):
        raise ValueError("not an input manifest")
    relative = key[len(input_prefix + "/manifests/"):]
    if "/" in relative or "\\" in relative:
        raise ValueError("manifest must be directly under manifests/")
    response = s3.get_object(Bucket=bucket, Key=key)
    with response["Body"] as body:
        manifest = json.loads(body.read())
    for field in ("source", "scope", "batch_id", "files", "record_count", "period_start", "period_end", "received_at", "schema_version", "data_kind"):
        if field not in manifest:
            raise ValueError("manifest missing " + field)
    if timestamp(manifest["period_start"]) >= timestamp(manifest["period_end"]):
        raise ValueError("invalid period")
    timestamp(manifest["received_at"])
    if manifest["schema_version"] != 1 or manifest["data_kind"] not in ("events", "snapshot_rows"):
        raise ValueError("unsupported manifest schema/kind")
    if not all(isinstance(manifest[f], str) and manifest[f] for f in ("source", "scope", "batch_id")):
        raise ValueError("invalid manifest identity")
    if type(manifest["record_count"]) is not int or manifest["record_count"] < 0 or not isinstance(manifest["files"], list):
        raise ValueError("invalid count/files")
    paths = set()
    ready = True
    for entry in manifest["files"]:
        path = entry["path"]
        parts = PurePosixPath(path)
        if not path.startswith("raw/") or parts.is_absolute() or ".." in parts.parts or "\\" in path or path in paths:
            raise ValueError("unsafe or repeated raw path")
        if not isinstance(entry["sha256"], str) or len(entry["sha256"]) != 64 or any(c not in "0123456789abcdef" for c in entry["sha256"]):
            raise ValueError("invalid SHA256")
        paths.add(path)
        try:
            s3.head_object(Bucket=bucket, Key=input_prefix + "/" + path)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404", "NotFound"):
                ready = False
            else:
                raise
    if not manifest["files"] and manifest["record_count"]:
        raise ValueError("nonempty batch without files")
    digest = hashlib.sha256(canonical_json(manifest).encode()).hexdigest()
    batch = hashlib.sha256(canonical_json([manifest["source"], manifest["scope"], manifest["batch_id"]]).encode()).hexdigest()
    return dict(pk="BATCH#" + batch, manifest_key=key, digest=digest, status="READY" if ready else "WAITING",
                source=manifest["source"], scope=manifest["scope"], batch_id=manifest["batch_id"])


def register(s3, table, bucket, key, prefix):
    item = inspect_manifest(s3, bucket, key, prefix)
    previous = table.get_item(Key={"pk": item["pk"]}, ConsistentRead=True).get("Item")
    if previous and previous["digest"] != item["digest"]:
        raise ValueError("immutable batch identity conflict")
    if previous and previous["status"] in ("PROCESSING", "COMPLETED"):
        return previous
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk) OR (digest = :d AND #s <> :processing AND #s <> :done)",
                   ExpressionAttributeNames={"#s": "status"},
                   ExpressionAttributeValues={":d": item["digest"], ":processing": "PROCESSING", ":done": "COMPLETED"})
    return item


def all_items(table):
    request = {"ConsistentRead": True}
    while True:
        response = table.scan(**request)
        yield from response.get("Items", [])
        if "LastEvaluatedKey" not in response:
            break
        request["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def release_lease(table, token):
    table.delete_item(Key={"pk": "LOCK"}, ConditionExpression="#owner = :t",
                      ExpressionAttributeNames={"#owner": "token"}, ExpressionAttributeValues={":t": token})


def plan(s3, table, bucket, input_prefix, output_prefix, force=False):
    now, token = int(time.time()), uuid.uuid4().hex
    try:
        table.put_item(Item=dict(pk="LOCK", token=token, expires=now + 7200),
                       ConditionExpression="attribute_not_exists(pk) OR expires < :now", ExpressionAttributeValues={":now": now})
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return dict(skip=True, reason="another run owns the lease")
        raise
    try:
        response = s3.get_object(Bucket=bucket, Key=input_prefix + "/catalog.json")
        with response["Body"] as body:
            catalog_digest = hashlib.sha256(canonical_json(json.loads(body.read())).encode()).hexdigest()
        last_catalog = table.get_item(Key={"pk": "CATALOG"}, ConsistentRead=True).get("Item")
        force = force or (last_catalog is not None and last_catalog["digest"] != catalog_digest)
        selected = []
        for item in all_items(table):
            if not item["pk"].startswith("BATCH#") or item["status"] not in ("READY", "WAITING", "FAILED", "PROCESSING"):
                continue
            # An expired run can be recovered under the new lease.
            fresh = inspect_manifest(s3, bucket, item["manifest_key"], input_prefix)
            if fresh["digest"] != item["digest"]:
                raise ValueError("manifest changed after registration")
            if fresh["status"] == "READY":
                selected.append(fresh)
        if not selected and not force:
            release_lease(table, token)
            return dict(skip=True, reason="no ready batches")
        selected.sort(key=lambda item: item["manifest_key"])
        payload = dict(run_token=token, catalog_sha256=catalog_digest, manifests=[dict(key=i["manifest_key"], digest=i["digest"]) for i in selected])
        key = output_prefix + "/plans/" + token + ".json"
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode(), ContentType="application/json")
        for item in selected:
            item.update(status="PROCESSING", run_token=token)
            table.put_item(Item=item)
        table.put_item(Item=dict(pk="RUN#" + token, status="PROCESSING", plan_key=key, catalog_digest=catalog_digest,
                                batches=[i["pk"] for i in selected], started_at=now))
        return dict(skip=False, run_token=token, plan_key=key)
    except Exception:
        release_lease(table, token)
        raise


def complete(s3, table, bucket, output_prefix, token, failed=False):
    run = table.get_item(Key={"pk": "RUN#" + token}, ConsistentRead=True)["Item"]
    if run["status"] in ("COMPLETED", "FAILED"):
        lock = table.get_item(Key={"pk": "LOCK"}, ConsistentRead=True).get("Item")
        if lock and lock.get("token") == token:
            release_lease(table, token)
        return dict(run_token=token, quality=run["quality"])
    blocked = set()
    quality = "failed"
    if not failed:
        with s3.get_object(Bucket=bucket, Key=output_prefix + "/latest.json")["Body"] as body:
            marker = json.loads(body.read())
        if marker["attempt"] != token:
            raise ValueError("publication does not belong to this run")
        quality = marker["quality"]
        with s3.get_object(Bucket=bucket, Key=marker["audit_key"])["Body"] as body:
            audit = json.loads(body.read())
        blocked = {r["manifest"] for r in audit["intake"]["blocked"]}
    for key in run["batches"]:
        item = table.get_item(Key={"pk": key}, ConsistentRead=True)["Item"]
        if item.get("run_token") != token:
            raise ValueError("batch ownership changed")
        item.update(status="FAILED" if failed or PurePosixPath(item["manifest_key"]).name in blocked else "COMPLETED", quality=quality)
        table.put_item(Item=item)
    run.update(status="FAILED" if failed else "COMPLETED", quality=quality, completed_at=int(time.time()))
    table.put_item(Item=run)
    if not failed:
        table.put_item(Item=dict(pk="CATALOG", digest=run["catalog_digest"]))
    release_lease(table, token)
    return dict(run_token=token, quality=quality)


def handler(event, context):
    import boto3
    s3 = boto3.client("s3")
    table = boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"])
    bucket = os.environ["DATA_BUCKET"]
    input_prefix, output_prefix = "thread/input", "thread/output"
    if "Records" in event:
        failures = []
        for message in event["Records"]:
            try:
                body = json.loads(message["body"])
                if body.get("detail", {}).get("bucket", {}).get("name") != bucket:
                    raise ValueError("unexpected source bucket")
                key = body["detail"]["object"]["key"]  # EventBridge keys are already literal.
                register(s3, table, bucket, key, input_prefix)
            except Exception:
                failures.append({"itemIdentifier": message["messageId"]})
        return {"batchItemFailures": failures}
    if event.get("action") == "complete":
        return complete(s3, table, bucket, output_prefix, event["run_token"], event.get("failed", False))
    return plan(s3, table, bucket, input_prefix, output_prefix, event.get("force", False))
