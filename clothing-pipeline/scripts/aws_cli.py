"""Invoke the authorized WSL AWS CLI without shell interpolation.

Usage on Windows: python scripts/aws_cli.py SERVICE OPERATION [OPTIONS].
JSON output and failures are forwarded; this script never reads credentials.
"""
import json
import subprocess
import sys
from pathlib import Path


def linux_path(path):
    path = Path(path).resolve()
    if not path.drive or len(path.drive) != 2:
        raise ValueError("expected a Windows drive path")
    return "/mnt/" + path.drive[0].lower() + path.as_posix()[2:]


def call(arguments):
    args = list(arguments)
    for index, arg in enumerate(args):
        if arg.startswith(("file://", "fileb://")):
            scheme, path = arg.split("://", 1)
            args[index] = scheme + "://" + linux_path(path)
    command = ["wsl.exe", "-d", "Ubuntu", "--exec", "/home/sater/.local/bin/aws"] + args + [
        "--profile", "aws-elt", "--region", "ap-southeast-1", "--output", "json", "--no-cli-pager"]
    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout) if result.stdout.strip() else {}


if __name__ == "__main__":
    try:
        print(json.dumps(call(sys.argv[1:]), indent=2, ensure_ascii=False))
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
