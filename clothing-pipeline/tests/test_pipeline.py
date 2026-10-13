import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.sample import EXPECTED, generate


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.input = generate(self.root / "input")
        self.state = self.root / "state.sqlite"
        self.output = self.root / "output"

    def process(self, **kwargs):
        return run(self.input, self.state, self.output, **kwargs)

    def rewrite(self, path, rows):
        manifest = json.loads(path.read_text())
        file = self.input / manifest["files"][0]["path"]
        content = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
        file.write_bytes(content)
        manifest["files"][0]["sha256"] = hashlib.sha256(content).hexdigest()
        manifest["record_count"] = len(rows)
        path.write_text(json.dumps(manifest))

    def find(self, event_id):
        for path in (self.input / "manifests").glob("*.json"):
            manifest = json.loads(path.read_text())
            rows = [json.loads(line) for line in (self.input / manifest["files"][0]["path"]).read_text().splitlines()]
            if any(row["event_id"] == event_id for row in rows):
                return path, rows
        self.fail("missing fixture event")

    def test_business_oracle_and_replay(self):
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(result["pending"] or result["checks"] or result["quarantine"])
        self.assertEqual(1, len(result["orders"]))
        self.assertEqual(1, len(result["source_links"]))
        again, audit = self.process()
        self.assertEqual(result, again)
        self.assertGreater(audit["intake"]["batches_replayed"], 0)

    def test_duplicate_keeps_receipt_without_second_effect(self):
        path, rows = self.find("GI-001")
        self.rewrite(path, rows + [copy.deepcopy(rows[0])])
        result, audit = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertEqual(1, audit["intake"]["duplicates"])
        self.assertEqual(1, sum(receipt["duplicate"] for receipt in result["receptions"]))

    def test_identity_conflict_quarantined(self):
        path, rows = self.find("GI-001")
        conflict = copy.deepcopy(rows[0])
        conflict["payload"]["quantity"] = 99
        self.rewrite(path, rows + [conflict])
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertIn("conflict", result["quarantine"][0]["reason"])

    def test_missing_file_then_late_arrival(self):
        path, _ = self.find("RR-001")
        manifest = json.loads(path.read_text())
        file = self.input / manifest["files"][0]["path"]
        content = file.read_bytes()
        file.unlink()
        result, audit = self.process()
        self.assertEqual(57, result["summary"]["warehouse_sellable"])  # QC arrived before receipt, but inspection is negative
        self.assertTrue(audit["intake"]["blocked"])
        self.assertTrue(any(check["check"] == "negative_inventory" for check in result["checks"]))
        file.write_bytes(content)
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(result["checks"])

    def test_checksum_failure_blocks_whole_batch(self):
        path, _ = self.find("PAY-001")
        manifest = json.loads(path.read_text())
        (self.input / manifest["files"][0]["path"]).write_text("{}\n")
        result, audit = self.process()
        self.assertEqual("0.00", result["summary"]["collected_SGD"])
        self.assertIn("checksum", audit["intake"]["blocked"][0]["reason"])

    def test_missing_mapping_recovers_without_new_input(self):
        path = self.input / "catalog.json"
        original = json.loads(path.read_text())
        broken = copy.deepcopy(original)
        broken["sku_mappings"] = [m for m in broken["sku_mappings"] if m["source"] != "website"]
        path.write_text(json.dumps(broken))
        result, _ = self.process()
        self.assertTrue(result["pending"])
        path.write_text(json.dumps(original))
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(result["pending"])

    def test_invalid_quantity_and_money_are_quarantined(self):
        path, rows = self.find("ORDER-001")
        rows[0]["payload"]["unit_price"] = "NaN"
        self.rewrite(path, rows)
        result, _ = self.process()
        self.assertTrue(result["quarantine"])
        self.assertFalse(result["orders"])

    def test_failure_does_not_publish_and_retry_is_safe(self):
        with self.assertRaises(RuntimeError):
            self.process(fail_before_publish=True)
        self.assertFalse((self.output / "latest.json").exists())
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertTrue((self.output / "latest.json").exists())

    def test_zero_record_batch(self):
        manifest = dict(batch_id="EMPTY", source="pos", scope="sg_store_01", schema_version=1,
                        period_start="2026-09-03T11:00:00+08:00", period_end="2026-09-03T12:00:00+08:00", data_kind="events",
                        received_at="2026-09-03T12:00:00+08:00", record_count=0, files=[])
        (self.input / "manifests" / "empty.json").write_text(json.dumps(manifest))
        result, audit = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertFalse(audit["intake"]["blocked"])

    def test_manifest_cannot_escape_input_directory(self):
        path, _ = self.find("PAY-001")
        manifest = json.loads(path.read_text())
        manifest["files"][0]["path"] = "../outside.jsonl"
        path.write_text(json.dumps(manifest))
        _, audit = self.process()
        self.assertIn("unsafe", audit["intake"]["blocked"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
