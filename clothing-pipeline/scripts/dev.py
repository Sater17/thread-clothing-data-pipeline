"""Developer runner; simulation/reference code stays outside src and artifacts."""
import argparse
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "src"))
sys.path.insert(0, str(root))
if sys.platform == "win32" and (root / ".deps").is_dir():
    sys.path.insert(0, str(root / ".deps"))


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    tests = commands.add_parser("test")
    tests.add_argument("--suite", choices=["unit", "reference", "integration", "all"], default="unit")
    sample = commands.add_parser("sample")
    sample.add_argument("--output", type=Path, required=True)
    history = commands.add_parser("history")
    history.add_argument("--output", type=Path, required=True)
    history.add_argument("--days", type=int, default=30)
    history.add_argument("--orders-per-day", type=int, default=500)
    history.add_argument("--skus", type=int, default=500)
    history.add_argument("--seed", type=int, default=42)
    history.add_argument("--daily-snapshots", action="store_true")
    args = parser.parse_args()
    if args.command == "test":
        targets = [str(root / "tests")] if args.suite == "all" else [str(root / "tests" / args.suite)]
        import pytest
        raise SystemExit(pytest.main(targets + ["-q"]))
    if args.command == "sample":
        from tools.simulator.sample import generate
        print(generate(args.output))
    else:
        from tools.simulator.history import generate_history
        print(json.dumps(generate_history(args.output, args.days, args.orders_per_day, args.skus, args.seed, args.daily_snapshots), indent=2))


if __name__ == "__main__":
    main()
