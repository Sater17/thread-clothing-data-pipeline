"""Typed snapshot tables with explicit schemas, including empty tables."""

import hashlib
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from .contracts import canonical_json, timestamp


def export(result, destination):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Parquet requires the optional dependency: pip install '.[parquet]'") from exc
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    text, integer, amount = pa.string(), pa.int64(), pa.decimal128(20, 2)
    schemas = {
        "standardized": pa.schema([(n, text) for n in ("event_id", "source", "scope", "type", "sku_id", "payload_json", "provenance_json")]
                                  + [("occurred_at_utc", pa.timestamp("us", tz="UTC")), ("business_date", pa.date32()),
                                     ("source_version", integer), ("is_current", pa.bool_()), ("rejected", pa.bool_())]),
        "inventory": pa.schema([(n, text) for n in ("sku_id", "location", "condition")] + [("quantity", integer)]),
        "orders": pa.schema([("order_id", text), ("currency", text)]),
        "order_lines": pa.schema([(n, text) for n in ("order_id", "line_id", "sku_id", "order", "line", "currency")]
                                 + [("quantity", integer), ("source_version", integer)] + [(n, amount) for n in ("unit_price", "discount", "tax", "total")]),
        "payments": pa.schema([(n, text) for n in ("order_id", "order_source", "order_scope", "order", "transaction", "original_transaction", "currency", "kind", "status")]
                              + [("amount", amount), ("source_version", integer)]),
        "inventory_reconciliation": pa.schema([(n, text) for n in ("snapshot", "source", "scope", "event_id", "raw_file", "batch", "as_of", "sku_id", "location", "condition", "result")]
                                              + [(n, integer) for n in ("source_version", "expected", "actual", "delta")]),
        "source_links": pa.schema([(n, text) for n in ("source", "scope", "order", "line", "order_id", "line_id")]),
        "pending": pa.schema([("event_json", text), ("reason", text)]),
        "quarantine": pa.schema([("event_json", text), ("provenance_json", text), ("reason", text)]),
    }
    lineage = [(n, text) for n in ("event_id", "source", "scope", "raw_file", "batch", "occurred_at")]
    schemas.update({
        "return_lines": pa.schema(lineage + [(n, text) for n in ("return_id", "return_line_id", "order_id", "line_id", "sku_id", "reason", "currency", "source_return", "source_return_line", "order_line_key")]
                                  + [(n, integer) for n in ("quantity", "received_quantity", "inspected_quantity")] + [("refunded_amount", amount)]),
        "return_receipts": pa.schema(lineage + [(n, text) for n in ("receipt_id", "source_receipt", "return_line_id", "order_id", "line_id", "sku_id", "location")] + [("quantity", integer)]),
        "return_inspections": pa.schema(lineage + [(n, text) for n in ("inspection_id", "source_inspection", "receipt_id", "return_line_id", "sku_id", "location", "condition")] + [("quantity", integer)]),
        "refund_allocations": pa.schema(lineage + [(n, text) for n in ("allocation_id", "transaction", "return_line_id", "order_id", "line_id", "currency")] + [("amount", amount)]),
        "stock_lineage": pa.schema(lineage + [(n, text) for n in ("movement_id", "document_type", "document_id", "return_line_id", "purchase_line_id", "sku_id")] + [("quantity", integer)]),
        "purchase_lines": pa.schema(lineage + [(n, text) for n in ("purchase_order_id", "purchase_line_id", "source_purchase_order", "source_purchase_line", "sku_id", "location", "supplier", "currency", "expected_at", "created_at")]
                                    + [(n, integer) for n in ("quantity", "source_version", "received_quantity", "outstanding_quantity", "over_received_quantity", "inspected_quantity", "uninspected_quantity", "accepted_quantity", "defective_quantity")]
                                    + [("unit_cost", amount)]),
        "purchase_receipts": pa.schema(lineage + [(n, text) for n in ("receipt_id", "source_receipt", "purchase_line_id", "sku_id", "location")] + [("quantity", integer)]),
        "purchase_inspections": pa.schema(lineage + [(n, text) for n in ("inspection_id", "source_inspection", "receipt_id", "purchase_line_id", "sku_id", "location", "condition")] + [("quantity", integer)]),
    })
    schemas.update({
        "settlements": pa.schema(lineage + [(n, text) for n in ("settlement_id", "settlement", "currency", "period_start", "period_end")]
                                 + [(n, amount) for n in ("gross", "refunds", "fees", "payout")] + [("line_count", integer), ("source_version", integer)]),
        "settlement_lines": pa.schema(lineage + [(n, text) for n in ("settlement_id", "settlement_line_id", "settlement", "settlement_line", "order_id", "order_source", "order_scope", "order", "currency", "period_start", "period_end")]
                                      + [(n, amount) for n in ("gross", "refunds", "fees", "payout")] + [("source_version", integer)]),
        "payout_transfers": pa.schema(lineage + [(n, text) for n in ("settlement_id", "transaction", "settlement_source", "settlement_scope", "settlement", "currency", "status")]
                                      + [("amount", amount), ("source_version", integer)]),
        "settlement_reconciliation": pa.schema([(n, text) for n in ("settlement_id", "order_id", "check", "result")]
                                               + [(n, amount) for n in ("expected", "actual", "delta")]),
    })
    manifest = {}
    for name, schema in schemas.items():
        rows = []
        for original in result[name]:
            row = dict(original)
            if name == "standardized":
                time = timestamp(row["occurred_at"])
                row.update(occurred_at_utc=time.astimezone(timezone.utc), business_date=time.date(),
                           payload_json=canonical_json(row["payload"]), provenance_json=canonical_json(row["provenance"]))
            elif name in ("pending", "quarantine"):
                row["event_json"] = canonical_json(row["event"])
                if name == "quarantine":
                    row["provenance_json"] = canonical_json(row["provenance"])
            for field in schema:
                if pa.types.is_decimal(field.type) and field.name in row:
                    row[field.name] = Decimal(row[field.name])
            rows.append({field.name: row.get(field.name) for field in schema})
        table = pa.Table.from_pylist(rows, schema=schema)
        path = destination / (name + ".parquet")
        pq.write_table(table, path, compression="snappy")
        manifest[name] = dict(file=path.name, rows=len(rows), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return manifest
