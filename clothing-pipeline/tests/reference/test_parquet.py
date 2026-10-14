import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from thread_pipeline.engine import run
from thread_pipeline.sample import generate

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


@unittest.skipIf(pq is None, "install optional Parquet dependency")
class ParquetTests(unittest.TestCase):
    def test_roundtrip_preserves_decimal_and_empty_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = generate(root / "input")
            result, _ = run(source, root / "state.sqlite", root / "output", parquet=True)
            marker = json.loads((root / "output" / "latest.json").read_text())
            tables = root / "output" / "runs" / marker["run_id"] / "tables"
            payments = pq.read_table(tables / "payments.parquet")
            self.assertEqual([Decimal("54.00"), Decimal("27.00")], [row["amount"] for row in payments.to_pylist()])
            pending = pq.read_table(tables / "pending.parquet")
            self.assertEqual(0, pending.num_rows)
            self.assertEqual(["event_json", "reason"], pending.column_names)
            events = pq.read_table(tables / "standardized.parquet")
            self.assertEqual(len(result["standardized"]), events.num_rows)
            self.assertEqual("UTC", events.schema.field("occurred_at_utc").type.tz)
            for name, metadata in marker["parquet"].items():
                self.assertEqual(metadata["rows"], pq.read_table(tables / metadata["file"]).num_rows)
