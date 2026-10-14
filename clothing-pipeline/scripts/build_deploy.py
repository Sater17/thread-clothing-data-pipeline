"""Reproducible release build; never uploads, deploys or includes test tooling."""
import hashlib
import json
import sys
from pathlib import Path

from build_glue import archive, main as build_spark


def main():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from infra.template import template
    build_spark()
    output = root / "dist"
    archive(root / "src", output / "control.zip", include={"cloud/__init__.py", "cloud/control.py", "cloud/notify.py", "thread_etl/__init__.py", "thread_etl/contracts.py"})
    (output / "cloudformation.json").write_bytes(json.dumps(template(), sort_keys=True, indent=2).encode())
    names = ("glue_job.py", "thread_runtime.zip", "control.zip", "cloudformation.json")
    checksums = {name: hashlib.sha256((output / name).read_bytes()).hexdigest() for name in names}
    (output / "release.json").write_bytes(json.dumps(checksums, sort_keys=True, indent=2).encode())
    print(json.dumps(dict(assets=str(output), checksums=checksums), indent=2))


if __name__ == "__main__":
    main()
