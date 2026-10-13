"""Explicit AWS deployment entry point; not invoked by build or CI."""

import argparse
import hashlib
import json
import re
import sys
import uuid
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if (root / ".deps").is_dir():
    sys.path.insert(0, str(root / ".deps"))


def verify_release(directory):
    directory = Path(directory)
    expected = json.loads((directory / "release.json").read_text(encoding="utf-8"))
    if set(expected) != {"glue_job.py", "control.zip", "cloudformation.json"}:
        raise ValueError("unexpected release asset list")
    for filename, digest in expected.items():
        if hashlib.sha256((directory / filename).read_bytes()).hexdigest() != digest:
            raise ValueError("release checksum mismatch: " + filename)
    return expected


def deploy(session, directory, stack, bucket, prefix, mode):
    if mode not in ("plan-only", "apply"):
        raise ValueError("invalid deployment mode")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,80}", stack):
        raise ValueError("invalid stack name")
    if not re.fullmatch(r"thread-runtime/[A-Za-z0-9_-]+", prefix):
        raise ValueError("invalid immutable release prefix")
    expected = verify_release(directory)
    s3, cfn = session.client("s3"), session.client("cloudformation")
    for filename, digest in expected.items():
        body = (Path(directory) / filename).read_bytes()
        key = prefix + "/" + filename
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", Metadata={"sha256": digest})
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") not in ("PreconditionFailed", "412"):
                raise
            if s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("sha256") != digest:
                raise ValueError("release prefix already contains different code")
    kind = "CREATE"
    try:
        cfn.describe_stacks(StackName=stack)
        kind = "UPDATE"
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") != "ValidationError" or "does not exist" not in error.get("Message", ""):
            raise
    change_set = "release-" + uuid.uuid4().hex
    response = cfn.create_change_set(StackName=stack, ChangeSetName=change_set, ChangeSetType=kind,
        TemplateBody=(Path(directory) / "cloudformation.json").read_text(encoding="utf-8"), Capabilities=["CAPABILITY_IAM"],
        Parameters=[dict(ParameterKey="ArtifactBucket", ParameterValue=bucket), dict(ParameterKey="ArtifactPrefix", ParameterValue=prefix),
                    dict(ParameterKey="ScheduleState", ParameterValue="DISABLED")])
    waiter = cfn.get_waiter("change_set_create_complete")
    try:
        waiter.wait(ChangeSetName=response["Id"], WaiterConfig={"Delay": 5, "MaxAttempts": 120})
    except Exception:
        details = cfn.describe_change_set(ChangeSetName=response["Id"])
        reason = details.get("StatusReason", "")
        if "didn't contain changes" not in reason and "No updates are to be performed" not in reason:
            raise
        return dict(no_changes=True, reason=reason)
    details = cfn.describe_change_set(ChangeSetName=response["Id"])
    (Path(directory) / "change-set.json").write_text(json.dumps(details, default=str, indent=2), encoding="utf-8")
    if mode == "apply":
        cfn.execute_change_set(ChangeSetName=response["Id"])
        cfn.get_waiter("stack_create_complete" if kind == "CREATE" else "stack_update_complete").wait(
            StackName=stack, WaiterConfig={"Delay": 15, "MaxAttempts": 120})
    return dict(change_set=response["Id"], mode=mode, stack=stack)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stack", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--profile")
    parser.add_argument("--mode", choices=["plan-only", "apply"], default="plan-only")
    args = parser.parse_args()
    import boto3
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    print(json.dumps(deploy(session, root / "dist", args.stack, args.bucket, args.prefix, args.mode), indent=2))


if __name__ == "__main__":
    main()
