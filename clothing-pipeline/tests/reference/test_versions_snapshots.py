import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from thread_pipeline.contracts import ContractError, validate
from thread_pipeline.engine import run
from thread_pipeline.sample import EXPECTED, event, generate


class RevisionSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = generate(self.root / "input")
        self.state = self.root / "ledger.sqlite"
        self.output = self.root / "output"

    def process(self):
        return run(self.source, self.state, self.output)

    def fixture(self, eid):
        for path in (self.source / "raw").rglob("*.jsonl"):
            for line in path.read_text().splitlines():
                row = json.loads(line)
                if row["event_id"] == eid:
                    return row
        self.fail("missing fixture")

    def add(self, e, bid):
        relative = "raw/revisions/" + bid + ".jsonl"
        path = self.source / relative
        path.parent.mkdir(exist_ok=True)
        content = (json.dumps(e) + "\n").encode()
        path.write_bytes(content)
        manifest = dict(batch_id=bid, source=e["source"], scope=e["scope"], schema_version=1, data_kind="events",
                        period_start="2026-09-09T00:00:00+08:00", period_end="2026-09-10T00:00:00+08:00",
                        received_at="2026-09-10T00:10:00+08:00", record_count=1,
                        files=[dict(path=relative, sha256=hashlib.sha256(content).hexdigest())])
        (self.source / "manifests" / (bid + ".json")).write_text(json.dumps(manifest))

    def snapshot(self, eid="SNAP-001", quantity=56, as_of="2026-09-04T00:00:00+08:00"):
        return event("wms", "sg_inventory", eid, "inventory_snapshot", "2026-09-09T00:00:00+08:00",
                     snapshot="DAILY-SG-20260903", sku="TS001-BLK-M", location="SG-WH-01",
                     condition="sellable", quantity=quantity, as_of=as_of)

    def test_snapshot_compares_historical_inventory_not_final_and_never_changes_stock(self):
        self.add(self.snapshot(), "SNAP")
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        row = result["inventory_reconciliation"][0]
        self.assertEqual((56, 56, "passed"), (row["expected"], row["actual"], row["result"]))
        self.assertFalse(result["checks"])

    def test_snapshot_at_same_timestamp_includes_movement(self):
        self.add(self.snapshot(as_of="2026-09-03T14:00:00+08:00"), "SNAP")
        result, _ = self.process()
        self.assertEqual(56, result["inventory_reconciliation"][0]["actual"])

    def test_mismatch_does_not_overwrite_stock(self):
        self.add(self.snapshot(quantity=999), "SNAP")
        result, _ = self.process()
        self.assertEqual(EXPECTED, result["summary"])
        self.assertEqual(-943, result["inventory_reconciliation"][0]["delta"])
        self.assertEqual("issues", json.loads((self.output / "latest.json").read_text())["quality"])

    def test_payment_newest_version_wins_even_if_older_arrives_later(self):
        newer = self.fixture("PAY-001")
        newer["event_id"], newer["source_version"] = "PAY-V3", 3
        newer["payload"]["status"] = "failed"
        self.add(newer, "NEW")
        result, _ = self.process()
        self.assertEqual("0.00", result["summary"]["collected_SGD"])
        older = self.fixture("PAY-001")
        older["event_id"], older["source_version"] = "PAY-V2", 2
        older["payload"]["status"] = "pending"
        self.add(older, "OLD-LATE")
        result, _ = self.process()
        charge = next(p for p in result["payments"] if p["kind"] == "charge")
        self.assertEqual((3, "failed"), (charge["source_version"], charge["status"]))
        self.assertEqual(3, sum(e["type"] == "payment" and e["payload"]["transaction"] == "PAY-001" for e in result["standardized"]))

    def test_order_revision_updates_line_but_keeps_stable_id(self):
        before, _ = self.process()
        revision = self.fixture("ORDER-001")
        revision.update(event_id="ORDER-V2", source_version=2)
        revision["payload"].update(discount="0.00", total="60.00")
        self.add(revision, "ORDER-REVISION")
        after, _ = self.process()
        self.assertEqual(before["order_lines"][0]["line_id"], after["order_lines"][0]["line_id"])
        self.assertEqual("60.00", after["order_lines"][0]["total"])
        self.assertEqual(2, len([e for e in after["standardized"] if e["type"] == "order"]))

    def test_same_version_conflict_prevents_new_publication(self):
        self.process()
        pointer = (self.output / "latest.json").read_bytes()
        revision = self.fixture("ORDER-001")
        revision.update(event_id="CONFLICT", source_version=1)
        revision["payload"].update(discount="0.00", total="60.00")
        self.add(revision, "CONFLICT")
        with self.assertRaises(ContractError):
            self.process()
        self.assertEqual(pointer, (self.output / "latest.json").read_bytes())

    def test_snapshot_revision_replaces_comparison_only(self):
        wrong = self.snapshot(quantity=999)
        self.add(wrong, "SNAP-V1")
        self.process()
        corrected = copy.deepcopy(wrong)
        corrected.update(event_id="SNAP-V2", source_version=2)
        corrected["payload"]["quantity"] = 56
        self.add(corrected, "SNAP-V2")
        result, _ = self.process()
        self.assertEqual(1, len(result["inventory_reconciliation"]))
        self.assertEqual("passed", result["inventory_reconciliation"][0]["result"])
        self.assertEqual(EXPECTED, result["summary"])

    def test_zero_snapshot_valid_and_negative_or_boolean_version_invalid(self):
        zero = self.snapshot(quantity=0)
        validate(zero)
        for bad in (-1, True):
            zero["source_version"] = bad
            with self.assertRaises(ContractError):
                validate(zero)

    def test_newest_revision_missing_mapping_never_falls_back_to_old(self):
        revision = self.fixture("ORDER-001")
        revision.update(event_id="ORDER-V2", source_version=2)
        revision["payload"]["sku"] = "UNKNOWN"
        self.add(revision, "UNMAPPED")
        result, _ = self.process()
        self.assertFalse(result["orders"])
        self.assertTrue(result["pending"])

    def test_missing_movement_then_late_arrival_repairs_snapshot_comparison(self):
        self.add(self.snapshot(), "SNAP")
        file = next(p for p in (self.source / "raw").rglob("*.jsonl") if '"event_id":"GI-001"' in p.read_text())
        saved = file.read_bytes()
        file.unlink()
        result, _ = self.process()
        self.assertEqual("failed", result["inventory_reconciliation"][0]["result"])
        file.write_bytes(saved)
        result, _ = self.process()
        self.assertEqual("passed", result["inventory_reconciliation"][0]["result"])
        self.assertEqual(EXPECTED, result["summary"])

    def test_append_only_movements_cannot_be_versioned(self):
        revision = self.fixture("GI-001")
        revision["source_version"] = 2
        with self.assertRaises(ContractError):
            validate(revision)
