import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from thread_etl.events import parse_rows
from thread_etl.transform import transform
from thread_etl.sales import extract
from tools.simulator.sample import EXPECTED, catalog, events

pytestmark = pytest.mark.spark


def test_checkpointed_domain_branches_have_independent_column_identity(spark):
    mapped = history_frame(spark, events()).withColumn("sku_id", F.lit("SKU-0001")).checkpoint(eager=True)
    holds = extract(mapped, "reservation").checkpoint(eager=True)
    moves = extract(mapped, "stock_move").checkpoint(eager=True)
    # Glue's analyzer rejects shared attribute IDs across joins of checkpointed branches.
    def attribute_id(frame):
        return frame._jdf.queryExecution().analyzed().output().apply(frame.columns.index("sku_id")).exprId().id()
    assert attribute_id(holds) != attribute_id(moves)
    assert holds.join(moves.select("sku_id").distinct(), "sku_id").count() == holds.count()


def history_frame(spark, source_events):
    records = [dict(value=json.dumps(event), manifest_source=event["source"], manifest_scope=event["scope"],
                    raw_file="raw/test.jsonl", batch="B001", received_at="2026-10-01T00:00:00Z") for event in source_events]
    return parse_rows(spark.createDataFrame(records)).filter("validation_error IS NULL")


def test_full_purchase_sale_return_chain_matches_independent_oracle(spark):
    history = history_frame(spark, events()).persist()
    tables = transform(history, catalog())
    issues = tables["quality_issues"].collect()
    assert not issues, issues
    inventory = {(row.sku_id, row.location, row.condition): row.quantity for row in tables["inventory"].collect()}
    payments = tables["payments"].filter("status = 'succeeded'").groupBy("kind", "currency").agg(F.sum("amount").alias("amount"))
    money = {(row.kind, row.currency): row.amount for row in payments.collect()}
    assert inventory[("SKU-0001", "LOC-0005", "sellable")] == EXPECTED["warehouse_sellable"]
    assert inventory[("SKU-0001", "LOC-0005", "defective")] == EXPECTED["warehouse_defective"]
    assert inventory[("SKU-0001", "LOC-0006", "sellable")] == EXPECTED["store_sellable"]
    assert money[("charge", "SGD")] == Decimal(EXPECTED["collected_SGD"])
    assert money[("refund", "SGD")] == Decimal(EXPECTED["refunded_SGD"])
    purchase = tables["purchase_lines"].first()
    assert (purchase.received_quantity, purchase.outstanding_quantity, purchase.accepted_quantity, purchase.defective_quantity) == (80, 20, 78, 2)
    returned = tables["return_lines"].first()
    assert returned.refunded_amount == Decimal("27.00")
    assert tables["stock_lineage"].count() == 5
