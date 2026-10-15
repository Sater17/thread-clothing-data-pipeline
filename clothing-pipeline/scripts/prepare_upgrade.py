"""Create reviewable V2 change sets; never executes either change set."""
import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path

from aws_cli import call, linux_path
from deploy_release import verify_release

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "review"])
    args = parser.parse_args()
    if args.action == "review":
        saved = json.loads((ROOT / "data/deployment/upgrade-plans.json").read_text())
        for name, identity in saved["plans"].items():
            details = call(["cloudformation", "describe-change-set", "--change-set-name", identity])
            (ROOT / "data/deployment" / (name + "-change-set.json")).write_text(json.dumps(details, indent=2), encoding="utf-8")
            print(json.dumps(dict(plan=name, status=details["Status"], reason=details.get("StatusReason"), execution=details.get("ExecutionStatus"),
                changes=[{k: change["ResourceChange"].get(k) for k in ("Action", "LogicalResourceId", "ResourceType", "Replacement")} for change in details.get("Changes", [])]), indent=2), flush=True)
        return
    from infra.bootstrap import template as bootstrap
    account = call(["sts", "get-caller-identity"])["Account"]
    if account != "082585183737":
        raise ValueError("unexpected deployment account")
    dist = ROOT / "dist"
    email = json.loads((ROOT / "data/deployment/settings.json").read_text(encoding="utf-8-sig"))["notification_email"]
    old = call(["cloudformation", "get-template", "--stack-name", "thread-deployment-bootstrap"])["TemplateBody"]
    old = json.loads(old) if isinstance(old, str) else old
    boot = dist / "bootstrap-v2.json"
    boot.write_text(json.dumps(bootstrap(account, provider_exists="GitHubOidc" not in old["Resources"])), encoding="utf-8")
    revision = __import__("subprocess").check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    prefix = "thread-runtime/" + revision
    bucket = "thread-artifacts-" + account + "-ap-southeast-1"
    checksums = verify_release(dist)
    for name, digest in checksums.items():
        path = dist / name
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError("release changed during preparation")
        try:
            call(["s3api", "put-object", "--bucket", bucket, "--key", prefix + "/" + name,
                "--body", linux_path(path), "--if-none-match", "*", "--metadata", "sha256=" + digest])
        except RuntimeError as exc:
            if "PreconditionFailed" not in str(exc):
                raise
            existing = call(["s3api", "head-object", "--bucket", bucket, "--key", prefix + "/" + name])
            if existing.get("Metadata", {}).get("sha256") != digest:
                raise ValueError("immutable artifact differs: " + name)
    parameters = dist / "upgrade-parameters.json"
    parameters.write_text(json.dumps([dict(ParameterKey=k, ParameterValue=v) for k, v in {
        "ArtifactBucket": bucket, "ArtifactPrefix": prefix, "DatabaseName": "thread_pipeline_v2",
        "ScheduleState": "DISABLED", "SparkWorkers": "2", "MaxBatches": "500", "NotificationEmail": email}.items()]), encoding="utf-8")
    plans = {}
    for name, stack, file, capabilities, extra in (
        ("bootstrap", "thread-deployment-bootstrap", boot, "CAPABILITY_NAMED_IAM", []),
        ("pipeline", "thread-clothing-pipeline", dist / "cloudformation.json", "CAPABILITY_IAM", ["--parameters", "file://" + str(parameters)])):
        response = call(["cloudformation", "create-change-set", "--stack-name", stack, "--change-set-name", "release-v2-" + uuid.uuid4().hex,
            "--change-set-type", "UPDATE", "--template-body", "file://" + str(file), "--capabilities", capabilities] + extra)
        plans[name] = response["Id"]
        print(name + ": " + response["Id"], flush=True)
    (ROOT / "data/deployment/upgrade-plans.json").write_text(json.dumps(dict(revision=revision, plans=plans), indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
