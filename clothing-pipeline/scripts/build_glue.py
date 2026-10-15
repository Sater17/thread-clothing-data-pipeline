"""Build a thin Spark entrypoint and production-only runtime archive."""
from pathlib import Path
import zipfile


def archive(source, target, include=None):
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as package:
        for path in sorted(source.rglob("*.py")):
            relative = path.relative_to(source).as_posix()
            if include is not None and relative not in include:
                continue
            info = zipfile.ZipInfo(relative, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            package.writestr(info, path.read_bytes().replace(b"\r\n", b"\n"))


def main():
    root = Path(__file__).resolve().parents[1]
    output = root / "dist"
    output.mkdir(exist_ok=True)
    (output / "glue_job.py").write_bytes((root / "jobs" / "etl.py").read_bytes().replace(b"\r\n", b"\n"))
    archive(root / "src", output / "thread_runtime.zip")


if __name__ == "__main__":
    main()
