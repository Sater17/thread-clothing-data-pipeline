"""Distributed order/payment/reservation/delivery and inventory transformations."""
from pyspark.sql import functions as F, Window

from .frames import amount, event_base, identifier, key, p, quantity


def extract(events, kind, fields=(), money=(), integers=()):
    return events.filter(F.col("type") == kind).select("*", *[p(name).alias(name) for name in fields],
        *[amount(name).alias(name) for name in money], *[quantity(name).alias(name) for name in integers])


def location(column, catalog, virtual=False):
    pairs = [item for source, target in catalog["locations"].items() for item in (F.lit(source), F.lit(target))]
    mapped = F.element_at(F.create_map(*pairs), column)
    if virtual:
        return F.when(column.rlike("^(external:|transit:|outbound:)"), column).otherwise(mapped)
    return mapped


def transform(events, catalog, quality):
    lines = extract(events, "order", ("order", "line", "currency"), ("unit_price", "discount", "tax", "total"), ("quantity",))
    lines = lines.withColumn("order_key", key("source", "scope", "order")).withColumn("line_key", key("source", "scope", "order", "line"))
    lines = lines.withColumn("order_id", identifier("ORD-", "source", "scope", "order")).withColumn("line_id", identifier("LINE-", "source", "scope", "order", "line"))
    quality.unique(lines, ["line_key"], "order_line_identity")
    orders = lines.select("order_key", "order_id", "currency").distinct()
    quality.unique(orders, ["order_key"], "order_currency_conflict")
    links = extract(events, "order_link", ("order", "line", "origin_source", "origin_scope", "origin_order", "origin_line"))
    links = links.withColumn("origin_key", key("origin_source", "origin_scope", "origin_order", "origin_line"))
    links = quality.link(links, lines, "origin_key", "line_key", ["order_id", "line_id"], "order_link")
    links = links.select("source", "scope", "order", "line", F.col("_parent_order_id").alias("order_id"), F.col("_parent_line_id").alias("line_id"))

    payments = extract(events, "payment", ("transaction", "order_source", "order_scope", "order", "original_transaction", "currency", "kind", "status", "refund_reason"), ("amount",))
    payments = payments.withColumn("order_key", key("order_source", "order_scope", "order"))
    payments = quality.link(payments, orders, "order_key", "order_key", ["order_id", "currency"], "payment_order", F.col("currency") == F.col("_parent_currency"))
    payments = payments.withColumnRenamed("_parent_order_id", "order_id").drop("_parent_currency")
    payments = payments.withColumn("transaction_key", key("source", "scope", "transaction"))
    payments = payments.withColumn("original_key", key("source", "scope", "original_transaction"))
    original = payments.select(F.col("transaction_key").alias("_original_key"), F.col("order_id").alias("_original_order"),
                              F.col("kind").alias("_original_kind"), F.col("currency").alias("_original_currency"))
    reference = payments.filter("kind = 'refund'").join(original, F.col("original_key") == F.col("_original_key"), "left")
    quality.add(reference, F.col("_original_key").isNull(), "refund_reference_missing", severity="waiting")
    quality.add(reference, F.col("_original_key").isNotNull() & ((F.col("_original_kind") != "charge") |
        (F.col("_original_currency") != F.col("currency")) | (F.col("_original_order") != F.col("order_id"))), "refund_reference_identity")
    cash = payments.filter("status = 'succeeded'").groupBy("order_id", "currency").agg(
        F.sum(F.when(F.col("kind") == "charge", F.col("amount")).otherwise(F.lit(0))).alias("gross"),
        F.sum(F.when(F.col("kind") == "refund", F.col("amount")).otherwise(F.lit(0))).alias("refunds"))
    quality.add(cash, F.col("refunds") > F.col("gross"), "refund_exceeds_collected", "order_id")

    delivered = extract(events, "delivery", ("shipment", "order_source", "order_scope", "order", "line"), integers=("quantity",))
    delivered = delivered.withColumn("line_key", key("order_source", "order_scope", "order", "line"))
    delivered = quality.link(delivered, lines, "line_key", "line_key", ["line_id", "quantity"], "delivery_line")
    totals = delivered.groupBy("line_key", "_parent_line_id", "_parent_quantity").agg(F.sum("quantity").alias("delivered_quantity"))
    quality.add(totals, F.col("delivered_quantity") > F.col("_parent_quantity"), "delivery_exceeds_order", "_parent_line_id")

    reservations = extract(events, "reservation", ("reservation", "order_source", "order_scope", "order", "line", "location", "action"), integers=("quantity",))
    reservations = reservations.withColumn("line_key", key("order_source", "order_scope", "order", "line"))
    reservations = quality.link(reservations, lines, "line_key", "line_key", ["sku_id"], "reservation_line", F.col("sku_id") == F.col("_parent_sku_id"))
    reservations = reservations.withColumn("location", location(F.col("location"), catalog))
    quality.add(reservations, F.col("location").isNull(), "reservation_location_missing", severity="waiting")
    holds = reservations.filter("location IS NOT NULL").groupBy("reservation", "sku_id", "location").agg(
        F.sum(F.col("quantity") * F.when(F.col("action") == "reserve", F.lit(1)).otherwise(F.lit(-1))).alias("quantity"))
    quality.add(holds, F.col("quantity") < 0, "negative_reservation", "reservation")

    moves = extract(events, "stock_move", ("reference", "document_type", "document_source", "document_scope", "document_id"), integers=("quantity",))
    for side in ("from", "to"):
        moves = moves.withColumn(side + "_source_location", p(side + ".location"))
        moves = moves.withColumn(side + "_location", location(p(side + ".location"), catalog, virtual=True))
        moves = moves.withColumn(side + "_condition", p(side + ".condition"))
    quality.add(moves, F.col("from_location").isNull() | F.col("to_location").isNull(), "stock_location_missing", severity="waiting")
    moves = moves.filter("from_location IS NOT NULL AND to_location IS NOT NULL")
    from_rows = moves.select("event_key", "sku_id", F.col("from_location").alias("location"), F.col("from_condition").alias("condition"),
                            "occurred_at_utc", (-F.col("quantity")).alias("delta"))
    to_rows = moves.select("event_key", "sku_id", F.col("to_location").alias("location"), F.col("to_condition").alias("condition"),
                          "occurred_at_utc", F.col("quantity").alias("delta"))
    movements = from_rows.unionByName(to_rows)
    inventory = movements.groupBy("sku_id", "location", "condition").agg(F.sum("delta").alias("quantity"))
    quality.add(inventory.withColumn("stock_key", key("sku_id", "location", "condition")),
        ~F.col("location").startswith("external:") & (F.col("quantity") < 0), "negative_inventory", "stock_key")
    held = holds.groupBy("sku_id", "location").agg(F.sum("quantity").alias("reserved"))
    sellable = inventory.filter("condition = 'sellable'").select("sku_id", "location", F.col("quantity").alias("sellable"))
    compared = held.join(sellable, ["sku_id", "location"], "left").fillna(0, ["sellable"]).withColumn("stock_key", key("sku_id", "location"))
    quality.add(compared, F.col("reserved") > F.col("sellable"), "reservation_exceeds_sellable", "stock_key")

    snapshots = extract(events, "inventory_snapshot", ("snapshot", "location", "condition", "as_of"), integers=("quantity",))
    snapshots = snapshots.withColumn("location", location(F.col("location"), catalog)).withColumn("as_of_utc", F.to_timestamp("as_of"))
    quality.add(snapshots, F.col("location").isNull(), "snapshot_location_missing", severity="waiting")
    timeline = movements.withColumn("entry_type", F.lit(0)).unionByName(snapshots.filter("location IS NOT NULL").select(
        "event_key", "sku_id", "location", "condition", F.col("as_of_utc").alias("occurred_at_utc"), F.lit(0).cast("long").alias("delta"), F.lit(1).alias("entry_type")))
    window = Window.partitionBy("sku_id", "location", "condition").orderBy("occurred_at_utc", "entry_type", "event_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    observed = timeline.withColumn("actual", F.sum("delta").over(window)).filter("entry_type = 1").select("event_key", "actual")
    reconciliation = snapshots.join(observed, "event_key").withColumnRenamed("quantity", "expected")
    reconciliation = reconciliation.withColumn("delta", F.col("actual") - F.col("expected")).withColumn("result", F.when(F.col("delta") == 0, "passed").otherwise("failed"))
    quality.add(reconciliation, F.col("delta") != 0, "inventory_snapshot_mismatch")
    return dict(orders=orders, order_lines=lines, source_links=links, payments=payments, cash=cash,
                deliveries=totals, reservations=holds, stock_moves=moves, inventory=inventory, inventory_reconciliation=reconciliation)
