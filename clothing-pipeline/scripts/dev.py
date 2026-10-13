"""Use project-local optional dependencies with isolated bundled Python runtimes."""

import runpy
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
if (root / ".deps").is_dir():
    sys.path.insert(0, str(root / ".deps"))
if len(sys.argv) > 1 and sys.argv[1] == "test":
    import unittest
    suite = unittest.defaultTestLoader.discover(str(root / "tests"))
    outcome = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if outcome.wasSuccessful() else 1)
sys.argv = ["thread_pipeline"] + sys.argv[1:]
runpy.run_module("thread_pipeline", run_name="__main__")
