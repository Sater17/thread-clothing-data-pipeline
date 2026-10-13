"""Build CloudFormation and Lambda assets locally; never uploads or deploys."""

import hashlib
import json
import runpy
import sys
import zipfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from infra.template import template
    runpy.run_path(str(root / "scripts" / "build_glue.py"), run_name="__main__")
    output = root / "dist"
    with zipfile.ZipFile(output / "control.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for folder in ("cloud", "thread_pipeline"):
            for path in sorted((root / folder).glob("*.py")):
                archive.write(path, path.relative_to(root).as_posix())
    (output / "cloudformation.json").write_text(json.dumps(template(), indent=2), encoding="utf-8")
    files = ("glue_job.py", "control.zip", "cloudformation.json")
    checksums = {name: hashlib.sha256((output / name).read_bytes()).hexdigest() for name in files}
    (output / "release.json").write_text(json.dumps(checksums, indent=2), encoding="utf-8")
    print(json.dumps(dict(assets=str(output), checksums=checksums), indent=2))


if __name__ == "__main__":
    main()
