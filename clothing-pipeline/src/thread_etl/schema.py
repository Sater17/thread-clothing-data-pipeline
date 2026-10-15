"""Stable, typed public tables. Internal payloads never leak into gold schemas."""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

LINEAGE = "event_id string, source string, scope string, raw_file string, batch string, occurred_at string"
SCHEMAS = {
    "standardized": "event_id string, source string, scope string, type string, sku_id string, payload_json string, provenance_json string, occurred_at_utc timestamp, business_date date, source_version long, is_current boolean, rejected boolean",
    "orders": "order_id string, currency string",
    "order_lines": "order_id string, line_id string, sku_id string, order string, line string, currency string, quantity long, source_version long, unit_price decimal(20,2), discount decimal(20,2), tax decimal(20,2), total decimal(20,2)",
    "payments": LINEAGE + ", order_id string, order_source string, order_scope string, order string, transaction string, original_transaction string, currency string, kind string, status string, amount decimal(20,2), source_version long",
    "inventory": "sku_id string, location string, condition string, quantity long",
    "inventory_reconciliation": "snapshot string, source string, scope string, event_id string, raw_file string, batch string, as_of string, sku_id string, location string, condition string, result string, source_version long, expected long, actual long, delta long",
    "source_links": "source string, scope string, order string, line string, order_id string, line_id string",
    "return_lines": LINEAGE + ", return_id string, return_line_id string, order_id string, line_id string, sku_id string, reason string, currency string, source_return string, source_return_line string, order_line_key string, quantity long, received_quantity long, inspected_quantity long, refunded_amount decimal(20,2)",
    "return_receipts": LINEAGE + ", receipt_id string, source_receipt string, return_line_id string, order_id string, line_id string, sku_id string, location string, quantity long",
    "return_inspections": LINEAGE + ", inspection_id string, source_inspection string, receipt_id string, return_line_id string, sku_id string, location string, condition string, quantity long",
    "refund_allocations": LINEAGE + ", allocation_id string, transaction string, return_line_id string, order_id string, line_id string, currency string, amount decimal(20,2)",
    "stock_lineage": LINEAGE + ", movement_id string, document_type string, document_id string, return_line_id string, purchase_line_id string, sku_id string, quantity long",
    "purchase_lines": LINEAGE + ", purchase_order_id string, purchase_line_id string, source_purchase_order string, source_purchase_line string, sku_id string, location string, supplier string, currency string, expected_at string, created_at string, quantity long, source_version long, received_quantity long, outstanding_quantity long, over_received_quantity long, inspected_quantity long, uninspected_quantity long, accepted_quantity long, defective_quantity long, unit_cost decimal(20,2)",
    "purchase_receipts": LINEAGE + ", receipt_id string, source_receipt string, purchase_line_id string, sku_id string, location string, quantity long",
    "purchase_inspections": LINEAGE + ", inspection_id string, source_inspection string, receipt_id string, purchase_line_id string, sku_id string, location string, condition string, quantity long",
    "settlements": LINEAGE + ", settlement_id string, settlement string, currency string, period_start string, period_end string, gross decimal(20,2), refunds decimal(20,2), fees decimal(20,2), payout decimal(20,2), line_count long, source_version long",
    "settlement_lines": LINEAGE + ", settlement_id string, settlement_line_id string, settlement string, settlement_line string, order_id string, order_source string, order_scope string, order string, currency string, period_start string, period_end string, gross decimal(20,2), refunds decimal(20,2), fees decimal(20,2), payout decimal(20,2), source_version long",
    "payout_transfers": LINEAGE + ", settlement_id string, transaction string, settlement_source string, settlement_scope string, settlement string, currency string, status string, amount decimal(20,2), source_version long",
    "settlement_reconciliation": "settlement_id string, order_id string, check string, result string, expected decimal(20,2), actual decimal(20,2), delta decimal(20,2)",
    "reservations": "reservation string, sku_id string, location string, quantity long",
    "deliveries": "line_key string, line_id string, delivered_quantity long",
    "quality_issues": "check string, result string, key string",
    "pending": "event_json string, provenance_json string, reason string",
    "quarantine": "event_json string, provenance_json string, reason string",
}
ALIASES = {"source_return": "return", "source_return_line": "return_line", "order_line_key": "line_key",
           "source_receipt": "receipt", "source_inspection": "inspection", "source_purchase_order": "purchase_order",
           "source_purchase_line": "purchase_line", "line_id": "_parent_line_id"}


def project(name, frame):
    if name in ("standardized", "pending", "quarantine"):
        frame = frame.withColumn("provenance_json", F.to_json(F.struct("raw_file", "batch", "received_at")))
    if name == "standardized":
        frame = frame.withColumn("business_date", F.to_date(F.substring("occurred_at", 1, 10)))
    if name == "pending":
        frame = frame.withColumn("reason", F.lit("SKU mapping missing or ambiguous"))
    if name == "quarantine":
        frame = frame.withColumn("event_json", F.col("value")).withColumnRenamed("validation_error", "reason")
    schema = StructType.fromDDL(SCHEMAS[name]) if hasattr(StructType, "fromDDL") else frame.sparkSession.createDataFrame([], SCHEMAS[name]).schema
    columns = []
    for field in schema:
        original = field.name if field.name in frame.columns else ALIASES.get(field.name)
        if original is None or original not in frame.columns:
            raise ValueError("public schema field missing: " + name + "." + field.name)
        columns.append(F.col(original).cast(field.dataType).alias(field.name))
    return frame.select(*columns)
