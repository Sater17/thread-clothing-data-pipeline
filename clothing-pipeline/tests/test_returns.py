import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.sample import EXPECTED, generate


class ReturnTests(unittest.TestCase):
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
        file = "raw/" + bid + ".jsonl"
        body = (json.dumps(e) + "\n").encode()
        (self.source / file).write_bytes(body)
        manifest = dict(batch_id=bid, source=e["source"], scope=e["scope"], schema_version=1, data_kind="events",
                        period_start="2026-09-09T00:00:00+08:00", period_end="2026-09-10T00:00:00+08:00",
                        received_at="2026-09-10T00:10:00+08:00", record_count=1,
                        files=[dict(path=file, sha256=hashlib.sha256(body).hexdigest())])
        (self.source / "manifests" / (bid + ".json")).write_text(json.dumps(manifest))

    def test_complete_chain_is_traceable_and_does_not_duplicate_inventory(self):
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        request = result["return_lines"][0]
        self.assertEqual((1, 1, "27.00"), (request["received_quantity"], request["inspected_quantity"], request["refunded_amount"]))
        self.assertEqual(result["order_lines"][0]["line_id"], request["line_id"])
        self.assertEqual(request["return_line_id"], result["refund_allocations"][0]["return_line_id"])
        self.assertEqual(result["return_receipts"][0]["receipt_id"], result["return_inspections"][0]["receipt_id"])
        stock_links = [r for r in result["stock_lineage"] if r["document_type"].startswith("return_")]
        self.assertEqual(2, len(stock_links))
        self.assertTrue(all(row["return_line_id"] == request["return_line_id"] for row in stock_links))
        self.assertFalse(result["checks"] or result["pending"])

    def test_missing_receipt_doc_waits_and_recovers_on_arrival(self):
        doc = self.edit("RR-DOC-001", lambda e: None)
        manifest = next(p for p in (self.source / "manifests").glob("*.json")
                        if doc["event_id"] in (self.source / json.loads(p.read_text())["files"][0]["path"]).read_text())
        file = self.source / json.loads(manifest.read_text())["files"][0]["path"]
        saved = file.read_bytes()
        file.unlink()
        result, audit = self.process()
        self.assertTrue(audit["intake"]["blocked"])
        self.assertTrue(result["pending"])
        file.write_bytes(saved)
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(result["pending"] or result["checks"])

    def test_split_allocations_sum_to_one_refund(self):
        first = self.edit("RA-001", lambda e: e["payload"].update(amount="10.00"))
        second = copy.deepcopy(first)
        second.update(event_id="RA-002")
        second["payload"].update(allocation="RA-002", amount="17.00")
        self.append(second)
        result, _ = self.process()
        self.assertEqual(2, len(result["refund_allocations"]))
        self.assertEqual("27.00", result["return_lines"][0]["refunded_amount"])
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(result["checks"])

    def test_overallocation_fails(self):
        self.edit("RA-001", lambda e: e["payload"].update(amount="28.00"))
        result, _ = self.process()
        self.assertTrue(any(c["check"] == "refund_allocation_total" and c["result"] == "failed" for c in result["checks"]))

    def test_underallocation_waits_for_remaining_details(self):
        self.edit("RA-001", lambda e: e["payload"].update(amount="10.00"))
        result, _ = self.process()
        self.assertTrue(any(c["check"] == "refund_allocation_total" and c["result"] == "waiting" for c in result["checks"]))

    def test_multiple_receipts_and_mixed_inspection_results(self):
        self.edit("RET-001", lambda e: e["payload"].update(quantity=2))
        receipt = copy.deepcopy(self.edit("RR-DOC-001", lambda e: None))
        receipt.update(event_id="RR-DOC-002")
        receipt["payload"]["receipt"] = "RR-002"
        self.append(receipt)
        movement = copy.deepcopy(self.edit("RR-001", lambda e: None))
        movement.update(event_id="RR-002")
        movement["payload"]["document_id"] = "RR-002"
        self.append(movement)
        inspection = copy.deepcopy(self.edit("QC-DOC-001", lambda e: None))
        inspection.update(event_id="QC-DOC-002")
        inspection["payload"].update(inspection="QC-002", receipt="RR-002", condition="defective")
        self.append(inspection)
        movement = copy.deepcopy(self.edit("QC-001", lambda e: None))
        movement.update(event_id="QC-002")
        movement["payload"]["document_id"] = "QC-002"
        movement["payload"]["to"]["condition"] = "defective"
        self.append(movement)
        self.edit("RF-001", lambda e: e["payload"].update(amount="54.00"))
        self.edit("RA-001", lambda e: e["payload"].update(amount="54.00"))
        result, _ = self.process()
        row = result["return_lines"][0]
        self.assertEqual((2, 2, "54.00"), (row["received_quantity"], row["inspected_quantity"], row["refunded_amount"]))
        self.assertEqual(2, len(result["return_receipts"]))
        self.assertEqual({"sellable", "defective"}, {r["condition"] for r in result["return_inspections"]})
        self.assertEqual(3, result["summary"]["warehouse_defective"])
        self.assertFalse(result["checks"] or result["pending"])

    def test_return_quantity_cannot_exceed_delivered(self):
        self.edit("RET-001", lambda e: e["payload"].update(quantity=3))
        result, _ = self.process()
        self.assertTrue(any(c["check"] == "return_exceeds_delivered" for c in result["checks"]))

    def test_inspection_quantity_and_condition_must_match_receipt_movement(self):
        self.edit("QC-DOC-001", lambda e: e["payload"].update(quantity=2, condition="defective"))
        result, _ = self.process()
        names = {c["check"] for c in result["checks"]}
        self.assertIn("inspected_exceeds_received", names)
        self.assertIn("return_stock_identity", names)
        self.assertEqual(EXPECTED, result["summary"])

    def test_wrong_currency_allocation_is_rejected(self):
        self.edit("RA-001", lambda e: e["payload"].update(currency="VND"))
        result, _ = self.process()
        self.assertFalse(result["refund_allocations"])
        self.assertTrue(any(c["check"] == "refund_allocation_identity" for c in result["checks"]))

    def test_duplicate_delivery_of_receipt_does_not_add_document_or_stock(self):
        receipt = self.edit("RR-DOC-001", lambda e: None)
        self.append(receipt)
        result, audit = self.process()
        self.assertEqual(1, len(result["return_receipts"]))
        self.assertEqual(1, audit["intake"]["duplicates"])
        self.assertEqual(EXPECTED, result["summary"])
