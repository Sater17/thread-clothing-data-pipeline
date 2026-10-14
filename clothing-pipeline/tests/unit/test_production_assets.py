import importlib.util
from pathlib import Path
import zipfile

from infra.template import template

ROOT = Path(__file__).resolve().parents[2]


def test_runtime_archive_contains_only_production_code(tmp_path):
    spec = importlib.util.spec_from_file_location("build_glue", ROOT / "scripts/build_glue.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    first, second = tmp_path / "first.zip", tmp_path / "second.zip"
    module.archive(ROOT / "src", first)
    module.archive(ROOT / "src", second)
    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as package:
        assert "thread_etl/transform.py" in package.namelist()
        assert "cloud/notify.py" in package.namelist()
        assert all(name.startswith(("thread_etl/", "cloud/")) for name in package.namelist())
        assert all("sqlite" not in package.read(name).decode().lower() for name in package.namelist())


def test_spark_job_and_email_infrastructure_have_safe_defaults():
    config = template()
    job = config["Resources"]["GlueJob"]["Properties"]
    assert job["Command"]["Name"] == "glueetl"
    assert job["GlueVersion"] == "5.0"
    assert job["WorkerType"] == "G.1X"
    assert job["MaxRetries"] == 0
    assert config["Parameters"]["ScheduleState"]["Default"] == "DISABLED"
    assert config["Resources"]["AlertEmail"]["Condition"] == "HasEmail"
    assert config["Resources"]["AlertEmail"]["Properties"]["Endpoint"] == {"Ref": "NotificationEmail"}
    for resource in config["Resources"].values():
        if resource["Type"] == "AWS::IAM::Role":
            assert resource["Properties"].get("PermissionsBoundary")
