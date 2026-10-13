import copy
import hashlib
import io
import json
import unittest

from cloud.control import inspect_manifest, register, plan, complete
from thread_pipeline.contracts import canonical_json


class ServiceError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class S3:
    def __init__(self):
        self.objects = {}
    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ServiceError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key])}
    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ServiceError("404")
        return {}
    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body


class Table:
    def __init__(self):
        self.items = {}
    def get_item(self, Key, **kwargs):
        return {"Item": copy.deepcopy(self.items[Key["pk"]])} if Key["pk"] in self.items else {}
    def put_item(self, Item, **kwargs):
        previous = self.items.get(Item["pk"])
        if Item["pk"] == "LOCK" and previous and previous["expires"] >= kwargs.get("ExpressionAttributeValues", {}).get(":now", 0):
            raise ServiceError("ConditionalCheckFailedException")
        if previous and Item["pk"].startswith("BATCH#") and "ConditionExpression" in kwargs and previous["status"] in ("PROCESSING", "COMPLETED"):
            raise ServiceError("ConditionalCheckFailedException")
        self.items[Item["pk"]] = copy.deepcopy(Item)
    def scan(self, **kwargs):
        return {"Items": copy.deepcopy(list(self.items.values()))}
    def delete_item(self, Key, **kwargs):
        self.items.pop(Key["pk"], None)


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.s3, self.table = S3(), Table()
        self.prefix = "thread/input"
        self.key = self.prefix + "/manifests/B1.json"
        self.manifest = dict(source="web", scope="vn", batch_id="B1", schema_version=1, data_kind="events", record_count=1, files=[dict(path="raw/part.jsonl", sha256="a" * 64)],
                             period_start="2026-09-01T00:00:00Z", period_end="2026-09-01T01:00:00Z", received_at="2026-09-01T01:10:00Z")
        self.s3.objects[self.key] = json.dumps(self.manifest).encode()
        self.s3.objects[self.prefix + "/catalog.json"] = b"{}"
    def test_manifest_waits_then_recovers_and_duplicate_does_not_reset_completed(self):
        item = register(self.s3, self.table, "bucket", self.key, self.prefix)
        self.assertEqual("WAITING", item["status"])
        self.s3.objects[self.prefix + "/raw/part.jsonl"] = b"{}"
        item = register(self.s3, self.table, "bucket", self.key, self.prefix)
        self.assertEqual("READY", item["status"])
        item["status"] = "COMPLETED"
        self.table.put_item(Item=item)
        self.assertEqual("COMPLETED", register(self.s3, self.table, "bucket", self.key, self.prefix)["status"])
    def test_plan_freezes_inputs_excludes_later_arrivals_and_completion_releases_lease(self):
        self.s3.objects[self.prefix + "/raw/part.jsonl"] = b"{}"
        register(self.s3, self.table, "bucket", self.key, self.prefix)
        result = plan(self.s3, self.table, "bucket", self.prefix, "thread/output")
        frozen = json.loads(self.s3.objects[result["plan_key"]])
        self.assertEqual([self.key], [m["key"] for m in frozen["manifests"]])
        self.assertTrue(plan(self.s3, self.table, "bucket", self.prefix, "thread/output")["skip"])
        complete(self.s3, self.table, "bucket", "thread/output", result["run_token"], failed=True)
        self.assertNotIn("LOCK", self.table.items)
        self.assertEqual("FAILED", next(v for k, v in self.table.items.items() if k.startswith("BATCH#"))["status"])
    def test_changed_manifest_and_unsafe_path_are_rejected(self):
        register(self.s3, self.table, "bucket", self.key, self.prefix)
        self.manifest["files"][0]["path"] = "../secret"
        self.s3.objects[self.key] = json.dumps(self.manifest).encode()
        with self.assertRaises(ValueError):
            register(self.s3, self.table, "bucket", self.key, self.prefix)
