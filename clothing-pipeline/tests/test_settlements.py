import copy
import hashlib
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.history import generate_history
from thread_pipeline.sample import event


class SettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "input"
        generate_history(self.source, 2, 25, 12, 42)

    def process(self):
        return run(self.source, self.root / "ledger.sqlite", self.root / "output")

    def edit(self, kind, transform):
        for manifest in (self.source / "manifests").glob("*.json"):
            m = json.loads(manifest.read_text())
            path = self.source / m["files"][0]["path"]
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            for row in rows:
                if row["type"] == kind:
                    transform(row)
                    body = ("\n".join(json.dumps(v) for v in rows) + "\n").encode()
                    path.write_bytes(body)
                    m["files"][0]["sha256"] = hashlib.sha256(body).hexdigest()
                    manifest.write_text(json.dumps(m))
                    return row
        self.fail("missing fixture type")

    def append(self, e):
        relative = "raw/extra.jsonl"
        body = (json.dumps(e) + "\n").encode()
        (self.source / relative).write_bytes(body)
        m = dict(batch_id=e["event_id"], source=e["source"], scope=e["scope"], schema_version=1, data_kind="events",
                 period_start="2026-10-01T00:00:00Z", period_end="2026-10-02T00:00:00Z", received_at="2026-10-02T00:00:00Z",
                 record_count=1, files=[dict(path=relative, sha256=hashlib.sha256(body).hexdigest())])
        (self.source / "manifests" / "extra.json").write_text(json.dumps(m))

    def test_statements_match_money_and_bank_without_changing_sales_totals(self):
        result, _ = self.process()
        self.assertEqual(2, len(result["settlements"]))
        self.assertEqual(2, len(result["payout_transfers"]))
        self.assertTrue(all(r["result"] == "passed" for r in result["settlement_reconciliation"]))
        self.assertFalse(result["checks"] or result["pending"])
        self.assertEqual(json.loads((self.source / "expected-history.json").read_text()), result["reconciliation"])

    def test_wrong_gross_is_reported_without_rewriting_collected_money(self):
        def alter(e):
            e["payload"]["gross"] = format(Decimal(e["payload"]["gross"]) + 1, ".2f")
            e["payload"]["payout"] = format(Decimal(e["payload"]["payout"]) + 1, ".2f")
        self.edit("settlement_line", alter)
        result, _ = self.process()
        self.assertTrue(any(c["check"] == "settlement_gross" for c in result["checks"]))
        self.assertEqual(json.loads((self.source / "expected-history.json").read_text()), result["reconciliation"])

    def test_bank_transfer_shortfall_waits(self):
        self.edit("payout_transfer", lambda e: e["payload"].update(amount="0.00"))
        result, _ = self.process()
        self.assertTrue(any(c["check"] == "bank_payout" and c["result"] == "waiting" for c in result["checks"]))

    def test_same_version_conflict_can_be_adjudicated_without_deleting_history(self):
        original = self.edit("settlement_line", lambda e: None)
        bad = copy.deepcopy(original)
        bad["event_id"] += "-BAD"
        bad["payload"]["fees"] = format(Decimal(bad["payload"]["fees"]) + 1, ".2f")
        bad["payload"]["payout"] = format(Decimal(bad["payload"]["payout"]) - 1, ".2f")
        self.append(bad)
        with self.assertRaises(ValueError):
            self.process()
        control = event("pipeline_control", "operator", "REJECT-001", "event_rejection", "2026-10-02T00:00:00Z",
                        target_source=bad["source"], target_scope=bad["scope"], target_event_id=bad["event_id"], reason="confirmed invalid revision")
        # New immutable batch rather than replacing the bad batch.
        relative = "raw/control.jsonl"
        body = (json.dumps(control) + "\n").encode()
        (self.source / relative).write_bytes(body)
        m = dict(batch_id="REJECTION", source="pipeline_control", scope="operator", schema_version=1, data_kind="events",
                 period_start="2026-10-01T00:00:00Z", period_end="2026-10-03T00:00:00Z", received_at="2026-10-03T00:00:00Z",
                 record_count=1, files=[dict(path=relative, sha256=hashlib.sha256(body).hexdigest())])
        (self.source / "manifests" / "control.json").write_text(json.dumps(m))
        result, _ = self.process()
        self.assertFalse(result["checks"] or result["pending"])
        self.assertTrue(any(e["event_id"] == bad["event_id"] and e["rejected"] for e in result["standardized"]))
