"""Verify Glue wiring against a real SparkContext and the documented Glue facade."""
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.spark


def test_checkpoint_uses_spark_context_not_glue_facade(spark, monkeypatch):
    import boto3
    from thread_etl.contracts import canonical_json
    import hashlib
    catalog = {"sku_mappings": [], "locations": {}}
    plan = {"run_token": "run-test", "catalog_sha256": hashlib.sha256(canonical_json(catalog).encode()).hexdigest(), "manifests": []}
    job = MagicMock()
    monkeypatch.setitem(sys.modules, "awsglue", ModuleType("awsglue"))
    monkeypatch.setitem(sys.modules, "awsglue.context", SimpleNamespace(GlueContext=lambda sc: SimpleNamespace(spark_session=spark)))
    monkeypatch.setitem(sys.modules, "awsglue.job", SimpleNamespace(Job=lambda context: job))
    sdk = MagicMock()
    sdk.head_object.return_value = {"ETag": '"old"'}
    monkeypatch.setattr(boto3, "client", lambda name: sdk)
    monkeypatch.setattr(boto3, "resource", lambda name: sdk)
    checkpoint = MagicMock()
    monkeypatch.setattr(spark.sparkContext, "setCheckpointDir", checkpoint)
    entry = Path(__file__).resolve().parents[2] / "jobs/etl.py"
    spec = importlib.util.spec_from_file_location("glue_entrypoint_test", entry)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "read_json", lambda path: catalog if path.endswith("catalog.json") else plan)
    monkeypatch.setattr(module, "file_plan", lambda *args: [])
    monkeypatch.setattr(module, "read_inputs", lambda *args: "raw-frame")
    monkeypatch.setattr(module, "IcebergStore", lambda *args: "store")
    execute = MagicMock()
    monkeypatch.setattr(module, "execute", execute)
    arguments = {"JOB_NAME": "test", "bucket": "bucket", "input-prefix": "thread/input/v2", "output-prefix": "thread/output/v2",
        "plan-key": "plan.json", "run-token": "run-test", "catalog-database": "test_db", "batch-table": "ledger", "pipeline-name": "tests"}
    monkeypatch.setattr(sys, "argv", ["etl.py"] + [value for name, argument in arguments.items() for value in ("--" + name, argument)])
    module.main()
    checkpoint.assert_called_once_with("s3://bucket/thread/output/v2/checkpoints/run-test")
    execute.assert_called_once()
    job.commit.assert_called_once()
