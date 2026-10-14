import argparse
import json
from pathlib import Path

from .engine import run
from .sample import generate
from .history import generate_history


def main():
    parser = argparse.ArgumentParser(description="THREAD local pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    sample = commands.add_parser("sample")
    sample.add_argument("--output", type=Path, required=True)
    history = commands.add_parser("history")
    history.add_argument("--output", type=Path, required=True)
    history.add_argument("--days", type=int, default=30)
    history.add_argument("--orders-per-day", type=int, default=500)
    history.add_argument("--skus", type=int, default=500)
    history.add_argument("--seed", type=int, default=42)
    history.add_argument("--daily-snapshots", action="store_true")
    process = commands.add_parser("run")
    process.add_argument("--input", type=Path, required=True)
    process.add_argument("--state", type=Path, required=True)
    process.add_argument("--output", type=Path, required=True)
    process.add_argument("--check-sample", action="store_true")
    process.add_argument("--check-history", action="store_true")
    process.add_argument("--parquet", action="store_true")
    args = parser.parse_args()
    if args.command == "sample":
        print("Generated:", generate(args.output))
        return
    if args.command == "history":
        print(json.dumps(generate_history(args.output, args.days, args.orders_per_day, args.skus, args.seed, args.daily_snapshots), indent=2))
        return
    result, audit = run(args.input, args.state, args.output, parquet=args.parquet)
    print(json.dumps(dict(summary=result["summary"], order_count=result["reconciliation"]["order_count"],
                         money=result["reconciliation"]["money"], run_id=audit["run_id"], intake=audit["intake"],
                         pending=audit["pending"], quarantined=audit["quarantined"], checks=audit["checks"]), indent=2))
    if args.check_sample:
        expected = json.loads((args.input / "expected.json").read_text(encoding="utf-8"))
        if result["summary"] != expected or result["pending"] or result["quarantine"] or result["checks"] or audit["intake"]["blocked"]:
            raise SystemExit("Sample reconciliation FAILED")
        print("Sample reconciliation PASSED")
    if args.check_history:
        expected = json.loads((args.input / "expected-history.json").read_text(encoding="utf-8"))
        if result["reconciliation"] != expected or result["pending"] or result["quarantine"] or result["checks"] or audit["intake"]["blocked"]:
            raise SystemExit("History reconciliation FAILED")
        print("History reconciliation PASSED")
        purchase_oracle = args.input / "expected-purchases.json"
        if purchase_oracle.exists():
            expected_purchases = json.loads(purchase_oracle.read_text(encoding="utf-8"))
            if result["purchase_reconciliation"] != expected_purchases:
                raise SystemExit("Purchase reconciliation FAILED")
            print("Purchase reconciliation PASSED")


if __name__ == "__main__":
    main()
