"""Purchase and return document graphs expressed as joins and aggregations."""
from pyspark.sql import functions as F

from .frames import identifier, key, p
from .sales import extract, location


def documents(events, history, sales, catalog, quality):
    purchase = extract(events, "purchase_order_line", ("purchase_order", "purchase_line", "supplier", "location", "currency", "expected_at"), ("unit_cost",), ("quantity",))
    purchase = purchase.withColumn("document_key", key("source", "scope", "purchase_line"))
    purchase = purchase.withColumn("location", location(F.col("location"), catalog))
    quality.add(purchase, F.col("location").isNull(), "purchase_location_missing", severity="waiting")
    purchase = purchase.filter("location IS NOT NULL").withColumn("purchase_order_id", identifier("PO-", "source", "scope", "purchase_order"))
    purchase = purchase.withColumn("purchase_line_id", identifier("POL-", "source", "scope", "purchase_line"))
    earliest = history.filter("type = 'purchase_order_line'").withColumn("document_key", key("source", "scope", p("purchase_line"))).groupBy("document_key").agg(
        F.min(F.struct("occurred_at_utc", "occurred_at")).alias("created"))
    purchase = purchase.join(earliest, "document_key").withColumn("created_at", F.col("created.occurred_at")).withColumn("created_at_utc", F.col("created.occurred_at_utc")).drop("created")
    quality.unique(purchase, ["document_key"], "purchase_line_identity")

    returns = extract(events, "return_request", ("return", "return_line", "order_source", "order_scope", "order", "line", "reason"), integers=("quantity",))
    returns = returns.withColumn("line_key", key("order_source", "order_scope", "order", "line"))
    returns = quality.link(returns, sales["order_lines"], "line_key", "line_key", ["order_id", "line_id", "currency", "sku_id"],
        "return_order_line", F.col("sku_id") == F.col("_parent_sku_id"))
    returns = returns.withColumn("order_id", F.col("_parent_order_id")).withColumn("line_id", F.col("_parent_line_id")).withColumn("currency", F.col("_parent_currency"))
    returns = returns.withColumn("document_key", key("source", "scope", "return_line"))
    returns = returns.withColumn("return_id", identifier("RET-", "source", "scope", "return")).withColumn("return_line_id", identifier("RETL-", "source", "scope", "return_line"))
    quality.unique(returns, ["document_key"], "return_line_identity")
    requested = returns.groupBy("line_key").agg(F.sum("quantity").alias("requested"))
    requested = requested.join(sales["deliveries"].select("line_key", "delivered_quantity"), "line_key", "left")
    quality.add(requested, F.col("delivered_quantity").isNull(), "return_delivery_missing", "line_key", "waiting")
    quality.add(requested, F.col("requested") > F.col("delivered_quantity"), "return_exceeds_delivered", "line_key")

    def receipts(domain, parent):
        line = domain + "_line"
        frame = extract(events, domain + "_receipt", ("receipt", domain + "_source", domain + "_scope", line, "location"), integers=("quantity",))
        frame = frame.withColumn("parent_key", key(domain + "_source", domain + "_scope", line)).withColumn("location", location(F.col("location"), catalog))
        fields = [line + "_id", "sku_id", "quantity"]
        if domain == "purchase":
            fields += ["location", "created_at_utc"]
            identity = (F.col("sku_id") == F.col("_parent_sku_id")) & (F.col("location") == F.col("_parent_location"))
        else:
            fields += ["order_id", "line_id", "occurred_at_utc"]
            identity = (F.col("sku_id") == F.col("_parent_sku_id")) & F.col("location").isNotNull()
        frame = quality.link(frame, parent, "parent_key", "document_key", fields, domain + "_receipt", identity)
        timestamp = "_parent_created_at_utc" if domain == "purchase" else "_parent_occurred_at_utc"
        quality.add(frame, F.col("occurred_at_utc") < F.col(timestamp), domain + "_receipt_before_parent")
        frame = frame.withColumn(line + "_id", F.col("_parent_" + line + "_id"))
        if domain == "return":
            frame = frame.withColumn("order_id", F.col("_parent_order_id")).withColumn("line_id", F.col("_parent_line_id"))
        frame = frame.withColumn("document_key", key("source", "scope", "receipt"))
        frame = frame.withColumn("receipt_id", identifier("PRC-" if domain == "purchase" else "RRC-", "source", "scope", "receipt"))
        quality.unique(frame, ["document_key"], domain + "_receipt_identity_conflict")
        return frame

    def inspections(domain, parent):
        frame = extract(events, domain + "_inspection", ("inspection", "receipt_source", "receipt_scope", "receipt", "location", "condition"), integers=("quantity",))
        frame = frame.withColumn("parent_key", key("receipt_source", "receipt_scope", "receipt")).withColumn("location", location(F.col("location"), catalog))
        fields = ["receipt_id", domain + "_line_id", "sku_id", "location", "occurred_at_utc"]
        frame = quality.link(frame, parent, "parent_key", "document_key", fields, domain + "_inspection",
            (F.col("sku_id") == F.col("_parent_sku_id")) & (F.col("location") == F.col("_parent_location")))
        quality.add(frame, F.col("occurred_at_utc") < F.col("_parent_occurred_at_utc"), domain + "_inspection_before_receipt")
        frame = frame.withColumn("receipt_id", F.col("_parent_receipt_id")).withColumn(domain + "_line_id", F.col("_parent_" + domain + "_line_id"))
        frame = frame.withColumn("document_key", key("source", "scope", "inspection"))
        frame = frame.withColumn("inspection_id", identifier("PIN-" if domain == "purchase" else "RIN-", "source", "scope", "inspection"))
        quality.unique(frame, ["document_key"], domain + "_inspection_identity_conflict")
        inspected = frame.groupBy("receipt_id").agg(F.sum("quantity").alias("inspected"))
        compared = parent.select("receipt_id", "quantity").join(inspected, "receipt_id", "left")
        quality.add(compared, F.col("inspected") > F.col("quantity"), domain + "_inspection_exceeds_received", "receipt_id")
        return frame

    pr, rr = receipts("purchase", purchase), receipts("return", returns)
    pi, ri = inspections("purchase", pr), inspections("return", rr)

    def document_totals(domain, lines, received, inspected):
        line_id = domain + "_line_id"
        totals = received.groupBy(line_id).agg(F.sum("quantity").alias("received_quantity"))
        outcomes = inspected.groupBy(line_id).agg(F.sum("quantity").alias("inspected_quantity"),
            F.sum(F.when(F.col("condition") == "sellable", F.col("quantity")).otherwise(F.lit(0))).alias("accepted_quantity"),
            F.sum(F.when(F.col("condition") == "defective", F.col("quantity")).otherwise(F.lit(0))).alias("defective_quantity"))
        lines = lines.join(totals, line_id, "left").join(outcomes, line_id, "left").fillna(0, ["received_quantity", "inspected_quantity", "accepted_quantity", "defective_quantity"])
        quality.add(lines, F.col("received_quantity") > F.col("quantity"), domain + "_received_exceeds_ordered", line_id)
        if domain == "purchase":
            lines = lines.withColumn("outstanding_quantity", F.greatest(F.col("quantity") - F.col("received_quantity"), F.lit(0)))
            lines = lines.withColumn("over_received_quantity", F.greatest(F.col("received_quantity") - F.col("quantity"), F.lit(0)))
            lines = lines.withColumn("uninspected_quantity", F.greatest(F.col("received_quantity") - F.col("inspected_quantity"), F.lit(0)))
        return lines

    purchase = document_totals("purchase", purchase, pr, pi)
    returns = document_totals("return", returns, rr, ri)
    allocations = extract(events, "refund_allocation", ("allocation", "transaction", "return_source", "return_scope", "return_line", "currency"), ("amount",))
    allocations = allocations.withColumn("transaction_key", key("source", "scope", "transaction"))
    allocations = quality.link(allocations, sales["payments"], "transaction_key", "transaction_key", ["order_id", "currency", "kind", "status"], "refund_transaction")
    for field in ("order_id", "currency", "kind", "status"):
        allocations = allocations.withColumnRenamed("_parent_" + field, "_tx_" + field)
    allocations = allocations.withColumn("parent_key", key("return_source", "return_scope", "return_line"))
    allocations = quality.link(allocations, returns, "parent_key", "document_key", ["return_line_id", "order_id", "line_id", "currency"], "refund_return_line",
        (F.col("_tx_kind") == "refund") & (F.col("currency") == F.col("_tx_currency")) &
        (F.col("currency") == F.col("_parent_currency")) & (F.col("_tx_order_id") == F.col("_parent_order_id")))
    for field in ("return_line_id", "order_id", "line_id"):
        allocations = allocations.withColumn(field, F.col("_parent_" + field))
    allocations = allocations.withColumn("allocation_id", identifier("RAL-", "source", "scope", "allocation"))
    quality.unique(allocations, ["source", "scope", "allocation"], "refund_allocation_identity_conflict")
    allocated = allocations.groupBy("transaction_key").agg(F.sum("amount").alias("allocated"))
    refunds = sales["payments"].filter("kind = 'refund'").join(allocated, "transaction_key", "left")
    required = (F.col("refund_reason") == "return") | F.col("allocated").isNotNull()
    quality.add(refunds, required & (F.col("allocated").isNull() | (F.col("allocated") < F.col("amount"))), "refund_allocation_incomplete", severity="waiting")
    quality.add(refunds, F.col("allocated") > F.col("amount"), "refund_allocation_exceeds_refund")
    refunded = allocations.filter("_tx_status = 'succeeded'").groupBy("return_line_id").agg(F.sum("amount").alias("refunded_amount"))
    returns = returns.join(refunded, "return_line_id", "left").fillna(0, ["refunded_amount"])

    specs = [("purchase_receipt", pr, "receipt_id", "purchase_line_id", "external:supplier"),
             ("purchase_inspection", pi, "inspection_id", "purchase_line_id", None),
             ("return_receipt", rr, "receipt_id", "return_line_id", "external:customer"),
             ("return_inspection", ri, "inspection_id", "return_line_id", None)]
    document_frames = []
    for kind, frame, doc_id, line_id, external in specs:
        document_frames.append(frame.select("document_key", F.lit(kind).alias("document_type"), F.col(doc_id).alias("document_id"),
            "sku_id", "location", F.col("quantity").alias("expected_quantity"),
            (F.lit("inspection") if external else F.col("condition")).alias("condition"),
            F.lit(external).cast("string").alias("expected_origin"),
            (F.col(line_id) if line_id == "purchase_line_id" else F.lit(None).cast("string")).alias("purchase_line_id"),
            (F.col(line_id) if line_id == "return_line_id" else F.lit(None).cast("string")).alias("return_line_id")))
    stock_docs = document_frames[0]
    for frame in document_frames[1:]:
        stock_docs = stock_docs.unionByName(frame)
    moves = sales["stock_moves"].filter("document_type IS NOT NULL").withColumn("document_key", key("document_source", "document_scope", "document_id"))
    parent = stock_docs.select("document_key", "document_type", *[F.col(name).alias("_doc_" + name) for name in
        ("document_id", "sku_id", "location", "expected_quantity", "condition", "expected_origin", "purchase_line_id", "return_line_id")])
    moves = moves.join(parent, ["document_key", "document_type"], "left")
    quality.add(moves, F.col("_doc_document_id").isNull(), "stock_document_missing", severity="waiting")
    identity = (F.col("sku_id") == F.col("_doc_sku_id")) & (F.col("to_location") == F.col("_doc_location")) & (F.col("to_condition") == F.col("_doc_condition"))
    identity &= F.when(F.col("_doc_expected_origin").isNotNull(), F.col("from_source_location") == F.col("_doc_expected_origin")).otherwise(
        (F.col("from_location") == F.col("_doc_location")) & (F.col("from_condition") == "inspection"))
    quality.add(moves, F.col("_doc_document_id").isNotNull() & ~identity, "stock_document_identity")
    lineage = moves.filter(F.col("_doc_document_id").isNotNull() & identity)
    moved = lineage.groupBy("document_key", "document_type").agg(F.sum("quantity").alias("moved"))
    compared = stock_docs.join(moved, ["document_key", "document_type"], "left")
    quality.add(compared, F.col("moved").isNull(), "stock_document_movement_missing", "document_key", "waiting")
    quality.add(compared, F.col("moved") != F.col("expected_quantity"), "stock_document_quantity", "document_key")
    lineage = lineage.withColumn("movement_id", identifier("MOV-", "source", "scope", "event_id"))
    lineage = lineage.withColumn("document_id", F.col("_doc_document_id")).withColumn("purchase_line_id", F.col("_doc_purchase_line_id")).withColumn("return_line_id", F.col("_doc_return_line_id"))
    return dict(purchase_lines=purchase, purchase_receipts=pr, purchase_inspections=pi, return_lines=returns,
                return_receipts=rr, return_inspections=ri, refund_allocations=allocations, stock_lineage=lineage)
