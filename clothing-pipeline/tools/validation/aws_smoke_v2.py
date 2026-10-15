"""Explicit, bounded cloud smoke operations; never part of the Glue runtime."""
import argparse
import json
import sys
import uuid
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.aws_cli import call, linux_path
from scripts.upload_inputs import upload_plan

EVIDENCE = ROOT / "data/aws-smoke-v2"
STACK = "thread-clothing-pipeline"


def config():
    stack = call(["cloudformation", "describe-stacks", "--stack-name", STACK])["Stacks"][0]
    if stack["StackStatus"] != "UPDATE_COMPLETE":
        raise ValueError("stack not ready: " + stack["StackStatus"])
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    if outputs.get("InputPrefix") != "thread/input/v2":
        raise ValueError("not a V2 deployment")
    return outputs


def save(name, value):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / name).write_text(json.dumps(value, indent=2), encoding="utf-8")


def batches(outputs):
    values = EVIDENCE / "query-values.json"
    save(values.name, {":p": {"S": "PIPELINE#" + STACK}, ":s": {"S": "BATCH#"}})
    return call(["dynamodb", "query", "--table-name", outputs["BatchTable"], "--consistent-read",
        "--key-condition-expression", "pk = :p AND begins_with(sk, :s)", "--expression-attribute-values", "file://" + str(values)])["Items"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "start", "retry", "inspect", "subscriptions", "alerts", "alert-test"])
    args = parser.parse_args()
    outputs = config()
    if args.action == "alerts":
        values = EVIDENCE / "alert-query-values.json"
        save(values.name, {":p": {"S": "ALERT#" + outputs["GlueJobName"]}})
        result = call(["dynamodb", "query", "--table-name", outputs["BatchTable"], "--consistent-read", "--key-condition-expression", "pk = :p",
            "--expression-attribute-values", "file://" + str(values)])
        save("alerts.json", result)
    elif args.action == "subscriptions":
        result = call(["sns", "list-subscriptions-by-topic", "--topic-arn", outputs["AlertsTopicArn"]])
    elif args.action == "upload":
        items = upload_plan(EVIDENCE / "input", outputs["InputPrefix"])
        manifests = list((EVIDENCE / "input/manifests").glob("*.json"))
        if sum(json.loads(path.read_text())["record_count"] for path in manifests) != 24 or sum(item["bytes"] for item in items) > 100000:
            raise ValueError("smoke restricted to 24-event fixture under 100 KB")
        for item in items:
            try:
                call(["s3api", "put-object", "--bucket", outputs["DataBucket"], "--key", item["key"], "--body", linux_path(item["path"]),
                    "--if-none-match", "*", "--metadata", "sha256=" + item["sha256"]])
            except RuntimeError as exc:
                if "PreconditionFailed" not in str(exc):
                    raise
                head = call(["s3api", "head-object", "--bucket", outputs["DataBucket"], "--key", item["key"]])
                if head.get("Metadata", {}).get("sha256") != item["sha256"]:
                    raise ValueError("existing input differs")
        result = dict(files=len(items), records=24)
        save("upload.json", result)
    elif args.action in ("start", "retry"):
        records = batches(outputs)
        manifests = list((EVIDENCE / "input/manifests").glob("*.json"))
        expected_status = "READY" if args.action == "start" else "FAILED"
        if len(records) != len(manifests) or any(item["status"]["S"] != expected_status for item in records):
            raise ValueError("not all smoke manifests are " + expected_status)
        if args.action == "retry":
            previous = json.loads((EVIDENCE / "execution.json").read_text())
            details = call(["stepfunctions", "describe-execution", "--execution-arn", previous["executionArn"]])
            if details["status"] not in ("FAILED", "TIMED_OUT", "ABORTED"):
                raise ValueError("previous smoke has not failed")
            save("previous-" + details["name"] + ".json", details)
        elif (EVIDENCE / "execution.json").exists():
            raise ValueError("smoke already started; inspect before another execution")
        result = call(["stepfunctions", "start-execution", "--state-machine-arn", outputs["WorkflowArn"], "--name", "smoke-v2-" + uuid.uuid4().hex, "--input", "{}"])
        save("execution.json", result)
    elif args.action == "alert-test":
        subscriptions = call(["sns", "list-subscriptions-by-topic", "--topic-arn", outputs["AlertsTopicArn"]])["Subscriptions"]
        if not any(item["Protocol"] == "email" and item["SubscriptionArn"].startswith("arn:") for item in subscriptions):
            raise ValueError("confirm the SNS email subscription before testing delivery")
        if (EVIDENCE / "alert-test.json").exists():
            raise ValueError("alert test already started")
        token = "alert-test-" + uuid.uuid4().hex
        parameters = EVIDENCE / "alert-test-arguments.json"
        save(parameters.name, {"--run-token": token, "--plan-key": outputs["OutputPrefix"] + "/plans/" + token + ".json"})
        result = call(["glue", "start-job-run", "--job-name", outputs["GlueJobName"], "--arguments", "file://" + str(parameters)])
        save("alert-test.json", result)
    else:
        execution = json.loads((EVIDENCE / "execution.json").read_text())
        result = dict(execution=call(["stepfunctions", "describe-execution", "--execution-arn", execution["executionArn"]]),
            glue=call(["glue", "get-job-runs", "--job-name", outputs["GlueJobName"], "--max-results", "5"]), batches=batches(outputs))
        save("inspection.json", result)
    if args.action == "inspect":
        result = dict(execution=result["execution"]["status"], error=result["execution"].get("cause"),
            glue=[{name: run.get(name) for name in ("Id", "JobRunState", "ErrorMessage", "ExecutionTime")} for run in result["glue"]["JobRuns"]],
            batches=dict(Counter(item["status"]["S"] for item in result["batches"])))
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
