import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.sample import EXPECTED, generate


class PurchaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = generate(self.root / "input")

    def process(self):
        return run(self.source, self.root / "ledger.sqlite", self.root / "output")

    def edit(self, eid, transform):
        for manifest in (self.source / "manifests").glob("*.json"):
            m = json.loads(manifest.read_text())
            path = self.source / m["files"][0]["path"]
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            for row in rows:
                if row["event_id"] == eid:
                    transform(row)
                    body = ("\n".join(json.dumps(v) for v in rows) + "\n").encode()
                    path.write_bytes(body)
                    m["files"][0]["sha256"] = hashlib.sha256(body).hexdigest()
                    manifest.write_text(json.dumps(m))
                    return row
        self.fail("fixture not found")

    def append(self, e):
        bid = "EXTRA-" + e["event_id"]
        relative = "raw/" + bid + ".jsonl"
        body = (json.dumps(e) + "\n").encode()
        (self.source / relative).write_bytes(body)
        m = dict(batch_id=bid, source=e["source"], scope=e["scope"], schema_version=1, data_kind="events",
                 period_start="2026-09-09T00:00:00+08:00", period_end="2026-09-10T00:00:00+08:00",
                 received_at="2026-09-10T00:10:00+08:00", record_count=1,
                 files=[dict(path=relative, sha256=hashlib.sha256(body).hexdigest())])
        (self.source / "manifests" / (bid + ".json")).write_text(json.dumps(m))

    def test_sample_outstanding_and_quality_match_stock_without_double_counting(self):
        result, _ = self.process()
        row = result["purchase_lines"][0]
        self.assertEqual((100, 80, 20, 78, 2, 0), (row["quantity"], row["received_quantity"], row["outstanding_quantity"],
                                                   row["accepted_quantity"], row["defective_quantity"], row["uninspected_quantity"]))
        self.assertEqual(EXPECTED, result["summary"])
        self.assertEqual(3, sum(r["document_type"].startswith("purchase_") for r in result["stock_lineage"]))
        self.assertFalse(result["checks"] or result["pending"])

    def test_second_receipt_fulfils_remaining_order(self):
        receipt = copy.deepcopy(self.edit("RCV-DOC-001", lambda e: None))
        receipt.update(event_id="RCV-DOC-002", occurred_at="2026-09-02T08:00:00+08:00")
        receipt["payload"].update(receipt="RCV-002", quantity=20)
        self.append(receipt)
        move = copy.deepcopy(self.edit("RCV-001", lambda e: None))
        move.update(event_id="RCV-002", occurred_at="2026-09-02T08:00:00+08:00")
        move["payload"].update(document_id="RCV-002", quantity=20)
        self.append(move)
        result, _ = self.process()
        row = result["purchase_lines"][0]
        self.assertEqual((100, 0, 20), (row["received_quantity"], row["outstanding_quantity"], row["uninspected_quantity"]))
        self.assertEqual(2, len(result["purchase_receipts"]))
        self.assertFalse(result["checks"] or result["pending"])

    def test_over_receipt_is_reported_not_hidden(self):
        self.edit("RCV-DOC-001", lambda e: e["payload"].update(quantity=101))
        result, _ = self.process()
        row = result["purchase_lines"][0]
        self.assertEqual(1, row["over_received_quantity"])
        self.assertTrue(any(c["check"] == "purchase_received_exceeds_ordered" for c in result["checks"]))

    def test_excess_inspection_and_wrong_condition_are_detected(self):
        self.edit("QC-GOOD-DOC", lambda e: e["payload"].update(quantity=81, condition="defective"))
        result, _ = self.process()
        names = {c["check"] for c in result["checks"]}
        self.assertIn("purchase_inspected_exceeds_received", names)
        self.assertIn("purchase_stock_identity", names)

    def test_missing_order_then_late_arrival_repairs_links(self):
        self.edit("RCV-DOC-001", lambda e: e["payload"].update(purchase_line="POL-LATE"))
        result, _ = self.process()
        self.assertTrue(result["pending"])
        order = copy.deepcopy(self.edit("PO-001", lambda e: None))
        order["event_id"] = "PO-LATE"
        order["payload"].update(purchase_order="PO-LATE", purchase_line="POL-LATE")
        self.append(order)
        result, _ = self.process()
        self.assertFalse(result["checks"] or result["pending"])
        self.assertEqual(EXPECTED, result["summary"])

    def test_duplicate_receipt_does_not_add_received_quantity(self):
        receipt = copy.deepcopy(self.edit("RCV-DOC-001", lambda e: None))
        self.append(receipt)
        result, audit = self.process()
        self.assertEqual(80, result["purchase_lines"][0]["received_quantity"])
        self.assertEqual(1, audit["intake"]["duplicates"])

    def test_new_order_version_changes_outstanding_without_changing_stock(self):
        before, _ = self.process()
        revised = copy.deepcopy(self.edit("PO-001", lambda e: None))
        revised.update(event_id="PO-V2", source_version=2, occurred_at="2026-09-02T09:00:00+08:00")
        revised["payload"]["quantity"] = 120
        self.append(revised)
        after, _ = self.process()
        self.assertEqual(40, after["purchase_lines"][0]["outstanding_quantity"])
        self.assertEqual(before["purchase_lines"][0]["purchase_line_id"], after["purchase_lines"][0]["purchase_line_id"])
        self.assertEqual(EXPECTED, after["summary"])
        self.assertFalse(after["checks"])

    def test_wrong_receipt_destination_is_not_linked(self):
        self.edit("RCV-DOC-001", lambda e: e["payload"].update(location="SG-ST-01"))
        result, _ = self.process()
        self.assertFalse(result["purchase_receipts"])
        self.assertTrue(any(c["check"] == "purchase_receipt_identity" for c in result["checks"]))
