"""Build a standalone Glue script containing the shared package; no network."""

import base64
import io
import zipfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((root / "thread_pipeline").glob("*.py")):
            archive.write(path, path.relative_to(root).as_posix())
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    bootstrap = (
        "import base64, sys, tempfile\n"
        "from pathlib import Path\n"
        "_bundle_dir = tempfile.TemporaryDirectory()\n"
        "_bundle_path = Path(_bundle_dir.name) / 'thread_pipeline.zip'\n"
        "_bundle_path.write_bytes(base64.b64decode(" + repr(encoded) + "))\n"
        "sys.path.insert(0, str(_bundle_path))\n\n"
    )
    target = root / "dist" / "glue_job.py"
    target.parent.mkdir(exist_ok=True)
    target.write_text(bootstrap + (root / "glue" / "job.py").read_text(encoding="utf-8"), encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
