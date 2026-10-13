import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / ".deps"))
sys.path.insert(0, str(root))
from cfnlint.api import lint
from infra.template import template

findings = lint(json.dumps(template()), regions=["ap-southeast-1"])
for finding in findings:
    print(finding)
raise SystemExit(1 if findings else 0)
