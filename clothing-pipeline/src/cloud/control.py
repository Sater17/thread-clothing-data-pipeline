"""V2 coordinator: status-index queries, conditional claims and owned leases."""
import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import PurePosixPath

from thread_etl.contracts import canonical_json, timestamp

LOGGER = logging.getLogger("thread.control")
LOGGER.setLevel(logging.INFO)


def key(pipeline, record):
    return dict(pk="PIPELINE#" + pipeline, sk=record)


def indexed(item, status):
    return dict(item, status=status, status_pk=item["pk"] + "#" + status,
                status_sk=item.get("received_sort", "") + "#" + item["sk"])


def inspect_manifest(s3, bucket, object_key, prefix, pipeline):
    if not object_key.startswith(prefix + "/manifests/"):
        raise ValueError("unexpected manifest prefix")
    name = object_key[len(prefix + "/manifests/"):]
    if not name.endswith(".json") or "/" in name or "\\" in name:
        raise ValueError("manifest must be directly under manifests")
    with s3.get_object(Bucket=bucket, Key=object_key)["Body"] as stream:
        manifest = json.load(stream)
    for field in ("source", "scope", "batch_id", "files", "record_count", "period_start", "period_end", "received_at", "schema_version", "data_kind"):
        if field not in manifest:
            raise ValueError("missing manifest." + field)
    if manifest["schema_version"] != 1 or manifest["data_kind"] not in ("events", "snapshot_rows"):
        raise ValueError("unsupported manifest")
    if timestamp(manifest["period_start"]) >= timestamp(manifest["period_end"]):
        raise ValueError("invalid period")
    if type(manifest["record_count"]) is not int or manifest["record_count"] < 0:
        raise ValueError("invalid record count")
    if not all(isinstance(manifest[field], str) and manifest[field] for field in ("source", "scope", "batch_id")):
        raise ValueError("invalid batch identity")
    if not isinstance(manifest["files"], list) or len(manifest["files"]) > 1000 or (not manifest["files"] and manifest["record_count"]):
        raise ValueError("invalid files")
    paths, ready = set(), True
    for item in manifest["files"]:
        path = item["path"]
        if not path.startswith("raw/") or PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts or "\\" in path or path in paths:
            raise ValueError("unsafe raw path")
        if len(item["sha256"]) != 64 or any(char not in "0123456789abcdef" for char in item["sha256"]):
            raise ValueError("invalid checksum")
        paths.add(path)
        try:
            s3.head_object(Bucket=bucket, Key=prefix + "/" + path)
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") in ("404", "NotFound", "NoSuchKey"):
                ready = False
            else:
                raise
    identity = hashlib.sha256(canonical_json([manifest["source"], manifest["scope"], manifest["batch_id"]]).encode()).hexdigest()
    item = dict(**key(pipeline, "BATCH#" + identity), manifest_key=object_key,
        digest=hashlib.sha256(canonical_json(manifest).encode()).hexdigest(), received_at=manifest["received_at"],
        received_sort="%020d" % int(timestamp(manifest["received_at"]).timestamp()),
        source=manifest["source"], scope=manifest["scope"], batch_id=manifest["batch_id"], record_count=manifest["record_count"])
    return indexed(item, "READY" if ready else "WAITING")


def register(s3, table, bucket, object_key, prefix, pipeline):
    item = inspect_manifest(s3, bucket, object_key, prefix, pipeline)
    previous = table.get_item(Key=dict(pk=item["pk"], sk=item["sk"]), ConsistentRead=True).get("Item")
    if previous and previous["digest"] != item["digest"]:
        raise ValueError("immutable batch identity conflict")
    if previous and previous["status"] in ("PROCESSING", "COMPLETED", "QUARANTINED"):
        return previous
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(sk) OR (digest = :d AND #s <> :processing AND #s <> :done AND #s <> :quarantined)",
        ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":d": item["digest"], ":processing": "PROCESSING", ":done": "COMPLETED", ":quarantined": "QUARANTINED"})
    LOGGER.info(json.dumps(dict(event="batch_registered", batch=item["sk"], status=item["status"])))
    return item


def release(table, pipeline, token):
    lease = table.get_item(Key=key(pipeline, "LEASE"), ConsistentRead=True).get("Item")
    if lease and lease.get("token") == token:
        table.delete_item(Key=key(pipeline, "LEASE"), ConditionExpression="#owner = :token",
            ExpressionAttributeNames={"#owner": "token"}, ExpressionAttributeValues={":token": token})


def candidates(table, pipeline, status, limit):
    arguments = dict(IndexName="by_status", KeyConditionExpression="status_pk = :status",
        ExpressionAttributeValues={":status": "PIPELINE#" + pipeline + "#" + status}, Limit=min(limit, 100))
    seen = 0
    while seen < limit:
        response = table.query(**arguments)
        for item in response.get("Items", []):
            if seen >= limit:
                return
            seen += 1
            yield item
        if "LastEvaluatedKey" not in response:
            return
        arguments["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        arguments["Limit"] = min(limit - seen, 100)


def transact(table, operations):
    requests = []
    for operation in operations:
        kind, request = next(iter(operation.items()))
        request = dict(request, TableName=table.name)
        requests.append({kind: request})
    # The DynamoDB resource client already serializes Python values.
    table.meta.client.transact_write_items(TransactItems=requests)


def claim(table, pipeline, token, batches):
    for offset in range(0, len(batches), 80):
        operations = [{"ConditionCheck": dict(Key=key(pipeline, "LEASE"), ConditionExpression="#owner = :token AND expires > :now",
            ExpressionAttributeNames={"#owner": "token"}, ExpressionAttributeValues={":token": token, ":now": int(time.time())})}]
        for item in batches[offset:offset + 80]:
            operations.append({"Put": dict(Item=indexed(dict(item, run_token=token), "PROCESSING"),
                ConditionExpression="digest = :digest AND (#s = :ready OR #s = :waiting OR #s = :failed)",
                ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":digest": item["digest"], ":ready": "READY", ":waiting": "WAITING", ":failed": "FAILED"})})
        transact(table, operations)


def plan(s3, table, bucket, prefix, output_prefix, pipeline, force=False, max_batches=500):
    if not 1 <= max_batches <= 500:
        raise ValueError("max_batches must be 1..500")
    token, now = uuid.uuid4().hex, int(time.time())
    try:
        table.put_item(Item=dict(**key(pipeline, "LEASE"), token=token, expires=now + 7200),
            ConditionExpression="attribute_not_exists(sk) OR expires < :now", ExpressionAttributeValues={":now": now})
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return dict(skip=True, reason="another run owns the lease")
        raise
    selected = []
    run = dict(**key(pipeline, "RUN#" + token), status="PLANNING", started_at=now, run_token=token, batches=[])
    try:
        with s3.get_object(Bucket=bucket, Key=prefix + "/catalog.json")["Body"] as stream:
            digest = hashlib.sha256(canonical_json(json.load(stream)).encode()).hexdigest()
        previous = table.get_item(Key=key(pipeline, "CATALOG"), ConsistentRead=True).get("Item")
        force = force or (previous is not None and previous["digest"] != digest)
        seen = set()
        for status in ("READY", "FAILED", "WAITING"):
            if len(selected) >= max_batches:
                break
            for candidate in candidates(table, pipeline, status, max_batches - len(selected)):
                identity = dict(pk=candidate["pk"], sk=candidate["sk"])
                item = table.get_item(Key=identity, ConsistentRead=True).get("Item")
                if not item or item["sk"] in seen or item["status"] not in ("READY", "FAILED", "WAITING"):
                    continue
                seen.add(item["sk"])
                fresh = inspect_manifest(s3, bucket, item["manifest_key"], prefix, pipeline)
                if fresh["digest"] != item["digest"]:
                    raise ValueError("manifest changed after registration")
                if fresh["status"] == "READY":
                    selected.append(item)
        if not selected and not force:
            release(table, pipeline, token)
            return dict(skip=True, reason="no ready batches")
        selected.sort(key=lambda item: item["manifest_key"])
        payload = dict(format_version=2, run_token=token, catalog_sha256=digest,
            manifests=[dict(key=item["manifest_key"], digest=item["digest"]) for item in selected])
        plan_key = output_prefix + "/plans/" + token + ".json"
        s3.put_object(Bucket=bucket, Key=plan_key, Body=json.dumps(payload).encode(), ContentType="application/json")
        run.update(plan_key=plan_key, catalog_digest=digest, batches=[item["sk"] for item in selected])
        table.put_item(Item=run)
        claim(table, pipeline, token, selected)
        run["status"] = "PROCESSING"
        table.put_item(Item=run)
        LOGGER.info(json.dumps(dict(event="run_planned", run_token=token, batches=len(selected))))
        return dict(skip=False, run_token=token, plan_key=plan_key)
    except Exception:
        for item in selected:
            current = table.get_item(Key=dict(pk=item["pk"], sk=item["sk"]), ConsistentRead=True).get("Item")
            if current and current.get("run_token") == token:
                table.put_item(Item=indexed(current, "FAILED"))
        run.update(status="FAILED", quality="failed", completed_at=int(time.time()))
        table.put_item(Item=run)
        release(table, pipeline, token)
        raise


def complete(s3, table, bucket, output_prefix, pipeline, token, failed=False, error=None):
    run = table.get_item(Key=key(pipeline, "RUN#" + token), ConsistentRead=True)["Item"]
    if run["status"] in ("COMPLETED", "FAILED"):
        release(table, pipeline, token)
        return dict(run_token=token, quality=run["quality"])
    quality = "failed"
    if not failed:
        with s3.get_object(Bucket=bucket, Key=output_prefix + "/latest.json")["Body"] as stream:
            marker = json.load(stream)
        if marker.get("format_version") != 2 or marker["attempt"] != token or marker["quality"] != "passed":
            raise ValueError("run has no approved publication")
        quality = "passed"
    for offset in range(0, len(run["batches"]), 80):
        operations = []
        for batch in run["batches"][offset:offset + 80]:
            item = table.get_item(Key=key(pipeline, batch), ConsistentRead=True)["Item"]
            if item.get("run_token") != token:
                raise ValueError("batch ownership changed")
            operations.append({"Put": dict(Item=indexed(dict(item, quality=quality, completed_at=int(time.time())), "FAILED" if failed else "COMPLETED"),
                ConditionExpression="#owner = :token", ExpressionAttributeNames={"#owner": "run_token"}, ExpressionAttributeValues={":token": token})})
        transact(table, operations)
    run.update(status="FAILED" if failed else "COMPLETED", quality=quality, completed_at=int(time.time()))
    if error:
        run["error"] = canonical_json(error)[:10000]
    table.put_item(Item=run)
    if not failed:
        table.put_item(Item=dict(**key(pipeline, "CATALOG"), digest=run["catalog_digest"]))
    release(table, pipeline, token)
    LOGGER.info(json.dumps(dict(event="run_completed", run_token=token, status=run["status"], quality=quality)))
    return dict(run_token=token, quality=quality)


def handler(event, context):
    import boto3
    s3 = boto3.client("s3")
    table = boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"])
    bucket, pipeline = os.environ["DATA_BUCKET"], os.environ["PIPELINE_NAME"]
    prefix, output = os.environ["INPUT_PREFIX"], os.environ["OUTPUT_PREFIX"]
    if "Records" in event:
        failures = []
        for message in event["Records"]:
            try:
                body = json.loads(message["body"])
                if body.get("detail", {}).get("bucket", {}).get("name") != bucket:
                    raise ValueError("unexpected source bucket")
                register(s3, table, bucket, body["detail"]["object"]["key"], prefix, pipeline)
            except Exception as exc:
                LOGGER.error(json.dumps(dict(event="manifest_registration_failed", message_id=message["messageId"], error_type=type(exc).__name__, error=str(exc)[:1000])))
                failures.append({"itemIdentifier": message["messageId"]})
        return dict(batchItemFailures=failures)
    if event.get("action") == "complete":
        return complete(s3, table, bucket, output, pipeline, event["run_token"], event.get("failed", False), event.get("error"))
    return plan(s3, table, bucket, prefix, output, pipeline, event.get("force", False), int(os.environ.get("MAX_BATCHES", "500")))
