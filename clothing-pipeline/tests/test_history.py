import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.history import generate_history


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_multi_market_oracle_and_replay(self):
        source = self.root / "history"
        generate_history(source, days=3, orders_per_day=30, sku_count=12, seed=42)
        expected = json.loads((source / "expected-history.json").read_text())
        result, _ = run(source, self.root / "state.sqlite", self.root / "output")
        self.assertEqual(expected, result["reconciliation"])
        expected_purchases = json.loads((source / "expected-purchases.json").read_text())
        self.assertEqual(expected_purchases, result["purchase_reconciliation"])
        self.assertEqual(24, len(result["purchase_lines"]))
        self.assertEqual(48, len(result["purchase_receipts"]))
        self.assertFalse(result["checks"] or result["pending"] or result["quarantine"])
        self.assertEqual({"VND", "SGD"}, {p["currency"] for p in result["payments"]})
        self.assertEqual({"website", "marketplace", "pos"}, {e["source"] for e in result["standardized"] if e["type"] == "order"})
        self.assertEqual(7, len({row["location"] for row in result["reconciliation"]["inventory"]}))
        self.assertEqual(12, len({row["sku_id"] for row in result["reconciliation"]["inventory"]}))
        again, _ = run(source, self.root / "state.sqlite", self.root / "output")
        self.assertEqual(result, again)

    def test_seed_reproduces_all_inputs(self):
        left, right = self.root / "left", self.root / "right"
        for path in (left, right):
            generate_history(path, days=1, orders_per_day=8, sku_count=5, seed=7)
        def hashes(path):
            return {p.relative_to(path).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in path.rglob("*") if p.is_file()}
        self.assertEqual(hashes(left), hashes(right))

    def test_nonpositive_configuration_rejected(self):
        with self.assertRaises(ValueError):
            generate_history(self.root / "bad", sku_count=0)

    def test_daily_snapshots_match_at_each_market_boundary(self):
        source = self.root / "snapshots"
        generate_history(source, days=3, orders_per_day=30, sku_count=12, seed=42, daily_snapshots=True)
        result, _ = run(source, self.root / "state.sqlite", self.root / "output")
        self.assertTrue(result["inventory_reconciliation"])
        self.assertTrue(all(row["result"] == "passed" for row in result["inventory_reconciliation"]))
        self.assertFalse(result["checks"] or result["pending"] or result["quarantine"])
        expected = json.loads((source / "expected-history.json").read_text())
        self.assertEqual(expected, result["reconciliation"])
