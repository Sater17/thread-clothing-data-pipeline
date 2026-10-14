import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from glue.job import execute
from v1_template import template
from thread_pipeline.contracts import canonical_json
from thread_pipeline.sample import generate, EXPECTED
from test_glue_adapter import FakeS3, ClientError
from test_control import Table
from v1_control import register, plan, complete
import sys
import types
from unittest.mock import patch

try:
    import pyarrow
except ImportError:
    pyarrow = None


class DeploymentTests(unittest.TestCase):
    def test_template_limits_cost_and_has_event_driven_control(self):
        spec = template()
        self.assertEqual("DISABLED", spec["Parameters"]["ScheduleState"]["Default"])
        job = spec["Resources"]["GlueJob"]["Properties"]
        self.assertEqual(1, job["ExecutionProperty"]["MaxConcurrentRuns"])
        self.assertEqual(0, job["MaxRetries"])
        self.assertEqual(10, job["Timeout"])
        self.assertEqual("Retain", spec["Resources"]["DataBucket"]["DeletionPolicy"])
        self.assertIn("ManifestRule", spec["Resources"])

    @unittest.skipIf(pyarrow is None, "install Parquet dependency")
    def test_frozen_plan_and_run_catalog_publish_together(self):
        with tempfile.TemporaryDirectory() as temp:
            root = generate(Path(temp) / "input")
            s3 = FakeS3()
            for path in root.rglob("*"):
                if path.is_file():
                    s3.objects["input/" + path.relative_to(root).as_posix()] = path.read_bytes()
            manifest_keys = sorted(k for k in s3.objects if k.startswith("input/manifests/"))
            frozen = dict(run_token="abc123", manifests=[dict(key=key, digest=hashlib.sha256(canonical_json(json.loads(s3.objects[key])).encode()).hexdigest()) for key in manifest_keys])
            s3.objects["output/plans/abc123.json"] = json.dumps(frozen).encode()
            # Invalid manifest arriving later must not be read by the frozen run.
            s3.objects["input/manifests/later.json"] = b"{}"
            stub = types.ModuleType("botocore.exceptions")
            stub.ClientError = ClientError
            class Catalog:
                def __init__(self):
                    self.tables = []
                def create_table(self, **kwargs):
                    self.tables.append(kwargs)
            client = Catalog()
            with patch.dict(sys.modules, {"botocore.exceptions": stub}):
                with self.assertRaises(ValueError):
                    execute(s3, "test", "input", "output", max_records=1)
                self.assertNotIn("output/latest.json", s3.objects)
                marker = execute(s3, "test", "input", "output", parquet=True, plan_key="output/plans/abc123.json",
                                 run_token="abc123", catalog_client=client, database="thread")
            self.assertEqual(21, len(client.tables))
            self.assertEqual(21, len(marker["catalog"]["tables"]))
            self.assertEqual(EXPECTED, json.loads(s3.objects[marker["result_key"]])["summary"])
            for item in client.tables:
                self.assertTrue(item["TableInput"]["Name"].endswith("_abc123"))
                self.assertTrue(item["TableInput"]["StorageDescriptor"]["Location"].endswith(".parquet"))

    @unittest.skipIf(pyarrow is None, "install Parquet dependency")
    def test_event_registration_plan_glue_and_completion_end_to_end(self):
        with tempfile.TemporaryDirectory() as temp:
            root = generate(Path(temp) / "input")
            class Store(FakeS3):
                def head_object(self, Bucket, Key):
                    if Key not in self.objects:
                        raise ClientError()
                    return {}
            s3, table = Store(), Table()
            for path in root.rglob("*"):
                if path.is_file():
                    s3.objects["thread/input/" + path.relative_to(root).as_posix()] = path.read_bytes()
            for key in sorted(s3.objects):
                if key.startswith("thread/input/manifests/"):
                    register(s3, table, "test", key, "thread/input")
            frozen = plan(s3, table, "test", "thread/input", "thread/output")
            stub = types.ModuleType("botocore.exceptions")
            stub.ClientError = ClientError
            with patch.dict(sys.modules, {"botocore.exceptions": stub}):
                marker = execute(s3, "test", "thread/input", "thread/output", parquet=True,
                                 plan_key=frozen["plan_key"], run_token=frozen["run_token"], max_records=10000)
            done = complete(s3, table, "test", "thread/output", frozen["run_token"])
            self.assertEqual("passed", done["quality"])
            self.assertTrue(all(item["status"] == "COMPLETED" for key, item in table.items.items() if key.startswith("BATCH#")))
            self.assertNotIn("LOCK", table.items)
            self.assertTrue(plan(s3, table, "test", "thread/input", "thread/output")["skip"])
            # A catalog change triggers replay even with no new batch.
            cat = json.loads(s3.objects["thread/input/catalog.json"])
            cat["new_metadata"] = "v2"
            s3.objects["thread/input/catalog.json"] = json.dumps(cat).encode()
            changed = plan(s3, table, "test", "thread/input", "thread/output")
            self.assertFalse(changed["skip"])
            self.assertEqual([], json.loads(s3.objects[changed["plan_key"]])["manifests"])
