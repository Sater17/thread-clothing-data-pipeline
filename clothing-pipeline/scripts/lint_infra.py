import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if sys.platform == "win32":
    sys.path.insert(0, str(root / ".deps"))
sys.path.insert(0, str(root))
from cfnlint.api import lint
from infra.template import template
from infra.bootstrap import template as bootstrap

findings = lint(json.dumps(template()), regions=["ap-southeast-1"])
findings += lint(json.dumps(bootstrap("082585183737")), regions=["ap-southeast-1"])
for finding in findings:
    print(finding)
raise SystemExit(1 if findings else 0)
