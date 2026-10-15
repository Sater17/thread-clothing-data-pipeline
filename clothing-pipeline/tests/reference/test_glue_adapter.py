"""Exercise the real S3 adapter against an in-memory S3 boundary, no AWS calls."""

import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from glue.job import execute
from thread_pipeline.sample import EXPECTED, generate

try:
    import pyarrow.parquet
except ImportError:
    pyarrow = None


class ClientError(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail_key = None

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError()
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail_key and self.fail_key in Key:
            raise RuntimeError("injected upload failure")
        self.objects[Key] = Body

    def get_paginator(self, operation):
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)]}


class GlueAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = generate(Path(self.tmp.name) / "input")
        self.s3 = FakeS3()
        for path in root.rglob("*"):
            if path.is_file():
                self.s3.objects["input/" + path.relative_to(root).as_posix()] = path.read_bytes()
        stub = types.ModuleType("botocore.exceptions")
        stub.ClientError = ClientError
        context = patch.dict(sys.modules, {"botocore.exceptions": stub})
        context.start()
        self.addCleanup(context.stop)

    def test_two_runs_restore_checkpoint_and_reconcile(self):
        marker = execute(self.s3, "test", "input", "output")
        self.assertEqual("passed", marker["quality"])
        result = json.loads(self.s3.objects[marker["result_key"]])
        self.assertEqual(EXPECTED, result["summary"])
        second = execute(self.s3, "test", "input", "output")
        self.assertEqual(result, json.loads(self.s3.objects[second["result_key"]]))

    def test_upload_failure_leaves_previous_publication_intact(self):
        execute(self.s3, "test", "input", "output")
        pointer = self.s3.objects["output/latest.json"]
        self.s3.fail_key = "/result.json"
        with self.assertRaises(RuntimeError):
            execute(self.s3, "test", "input", "output")
        self.assertEqual(pointer, self.s3.objects["output/latest.json"])
        self.s3.fail_key = None
        marker = execute(self.s3, "test", "input", "output")
        self.assertEqual(EXPECTED, json.loads(self.s3.objects[marker["result_key"]])["summary"])

    def test_overlapping_prefixes_rejected(self):
        with self.assertRaises(ValueError):
            execute(self.s3, "test", "input", "input/output")

    @unittest.skipIf(pyarrow is None, "install optional Parquet dependency")
    def test_parquet_uploaded_before_pointer_and_failure_is_safe(self):
        marker = execute(self.s3, "test", "input", "output", parquet=True)
        self.assertEqual(21, len(marker["parquet"]))
        for metadata in marker["parquet"].values():
            self.assertIn(marker["tables_prefix"] + metadata["file"], self.s3.objects)
        previous = self.s3.objects["output/latest.json"]
        self.s3.fail_key = "/tables/"
        with self.assertRaises(RuntimeError):
            execute(self.s3, "test", "input", "output", parquet=True)
        self.assertEqual(previous, self.s3.objects["output/latest.json"])
