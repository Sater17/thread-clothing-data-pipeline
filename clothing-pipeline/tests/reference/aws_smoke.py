"""Explicit small-fixture upload/run/inspection using the WSL AWS profile."""
import argparse
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
from scripts.aws_cli import call, linux_path
from scripts.upload_inputs import upload_plan


def outputs():
    stack = call(["cloudformation", "describe-stacks", "--stack-name", "thread-clothing-pipeline"])["Stacks"][0]
    if stack["StackStatus"] not in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        raise ValueError("stack is not ready: " + stack["StackStatus"])
    result = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    if result.get("CatalogDatabase") != "thread_pipeline":
        raise ValueError("this historical smoke tool supports V1 only")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "start", "inspect"])
    args = parser.parse_args()
    config = outputs()
    evidence = root / "data" / "aws-smoke"
    evidence.mkdir(parents=True, exist_ok=True)
    if args.action == "upload":
        items = upload_plan(evidence / "input", "thread/input")
        # Keep this tool restricted to the reviewed 24-event fixture.
        manifests = list((evidence / "input" / "manifests").glob("*.json"))
        count = sum(json.loads(path.read_text())["record_count"] for path in manifests)
        if count != 24 or sum(item["bytes"] for item in items) > 100000:
            raise ValueError("unexpected smoke input size")
        for item in items:
            try:
                call(["s3api", "put-object", "--bucket", config["DataBucket"], "--key", item["key"],
                      "--body", linux_path(item["path"]), "--if-none-match", "*", "--metadata", "sha256=" + item["sha256"]])
            except RuntimeError as exc:
                if "PreconditionFailed" not in str(exc):
                    raise
                existing = call(["s3api", "head-object", "--bucket", config["DataBucket"], "--key", item["key"]])
                if existing.get("Metadata", {}).get("sha256") != item["sha256"]:
                    raise ValueError("existing input has different contents")
        result = dict(uploaded_files=len(items), records=count, bucket=config["DataBucket"])
        (evidence / "upload.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result))
    elif args.action == "start":
        # Verify EventBridge -> SQS -> Lambda registered every manifest first.
        ledger = call(["dynamodb", "scan", "--table-name", config["BatchTable"]])["Items"]
        batches = [item for item in ledger if item["pk"]["S"].startswith("BATCH#")]
        expected = len(list((evidence / "input" / "manifests").glob("*.json")))
        if len(batches) != expected or any(item["status"]["S"] != "READY" for item in batches):
            raise ValueError("input registration is not ready")
        record = evidence / "execution.json"
        if record.exists():
            raise ValueError("smoke execution already started; inspect existing execution")
        result = call(["stepfunctions", "start-execution", "--state-machine-arn", config["WorkflowArn"],
                       "--name", "smoke-v1", "--input", "{}"])
        record.write_text(json.dumps(result, indent=2))
        print(json.dumps(result))
    else:
        record = json.loads((evidence / "execution.json").read_text())
        result = call(["stepfunctions", "describe-execution", "--execution-arn", record["executionArn"]])
        (evidence / "execution-status.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
        if result["status"] != "SUCCEEDED":
            return
        marker_path = evidence / "latest.json"
        call(["s3api", "get-object", "--bucket", config["DataBucket"], "--key", "thread/output/latest.json", linux_path(marker_path)])
        marker = json.loads(marker_path.read_text())
        assert marker["quality"] == "passed", "cloud quality checks did not pass"
        result_path = evidence / "result.json"
        call(["s3api", "get-object", "--bucket", config["DataBucket"], "--key", marker["result_key"], linux_path(result_path)])
        data = json.loads(result_path.read_text())
        from thread_pipeline.sample import EXPECTED
        assert data["summary"] == EXPECTED, "cloud result differs from business oracle"
        assert len(marker["catalog"]["tables"]) == 21, "missing catalog tables"
        ledger = call(["dynamodb", "scan", "--table-name", config["BatchTable"]])["Items"]
        batches = [item for item in ledger if item["pk"]["S"].startswith("BATCH#")]
        assert batches and all(item["status"]["S"] == "COMPLETED" for item in batches), "incomplete batch ledger"
        catalog = call(["glue", "get-tables", "--database-name", config["CatalogDatabase"], "--max-results", "100"])
        (evidence / "catalog-tables.json").write_text(json.dumps(catalog, indent=2))
        names = {item["Name"] for item in catalog["TableList"]}
        assert set(marker["catalog"]["tables"].values()).issubset(names), "catalog publication is incomplete"
        print(json.dumps(dict(oracle="passed", catalog_tables=21, summary=data["summary"]), indent=2))


if __name__ == "__main__":
    main()
