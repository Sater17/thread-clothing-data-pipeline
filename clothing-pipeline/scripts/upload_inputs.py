"""Plan input uploads by default; --apply is for the approved AWS deployment step."""

import argparse
import hashlib
import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
if (project_root / ".deps").is_dir():
    sys.path.insert(0, str(project_root / ".deps"))


def upload_plan(root, prefix):
    root = Path(root).resolve()
    files = []
    for path in sorted((root / "raw").rglob("*")):
        if path.is_file():
            files.append(path)
    files = [root / "catalog.json"] + files + sorted((root / "manifests").glob("*.json"))
    return [dict(path=str(p), key=prefix.strip("/") + "/" + p.relative_to(root).as_posix(),
                 bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="thread/input")
    parser.add_argument("--profile")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    items = upload_plan(args.input, args.prefix)
    print(json.dumps(dict(bucket=args.bucket, files=len(items), bytes=sum(i["bytes"] for i in items), apply=args.apply), indent=2))
    if not args.apply:
        return
    import boto3
    s3 = boto3.Session(profile_name=args.profile, region_name=args.region).client("s3")
    for item in items:
        body = Path(item["path"]).read_bytes()
        try:
            s3.put_object(Bucket=args.bucket, Key=item["key"], Body=body, IfNoneMatch="*",
                          Metadata={"sha256": item["sha256"]})
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") not in ("PreconditionFailed", "412"):
                raise
            existing = s3.head_object(Bucket=args.bucket, Key=item["key"])
            if existing.get("Metadata", {}).get("sha256") != item["sha256"]:
                raise ValueError("refusing to overwrite different input: " + item["key"])


if __name__ == "__main__":
    main()
