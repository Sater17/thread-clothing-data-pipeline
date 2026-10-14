"""Distributed final-statement and bank payout reconciliation."""
from pyspark.sql import functions as F

from .frames import identifier, key
from .sales import extract


def transform(events, sales, quality):
    metrics = ("gross", "refunds", "fees", "payout")
    headers = extract(events, "settlement", ("settlement", "currency", "period_start", "period_end"), metrics, ("line_count",))
    headers = headers.withColumn("settlement_key", key("source", "scope", "settlement"))
    headers = headers.withColumn("settlement_id", identifier("SET-", "source", "scope", "settlement"))
    quality.unique(headers, ["settlement_key"], "settlement_header_identity")
    lines = extract(events, "settlement_line", ("settlement", "settlement_line", "order_source", "order_scope", "order", "currency", "period_start", "period_end"), metrics)
    lines = lines.withColumn("settlement_key", key("source", "scope", "settlement"))
    lines = quality.link(lines, headers, "settlement_key", "settlement_key", ["settlement_id", "currency", "period_start", "period_end"], "settlement_header",
        (F.col("currency") == F.col("_parent_currency")) & (F.col("period_start") == F.col("_parent_period_start")) & (F.col("period_end") == F.col("_parent_period_end")))
    lines = lines.withColumn("settlement_id", F.col("_parent_settlement_id")).drop("_parent_settlement_id", "_parent_currency", "_parent_period_start", "_parent_period_end")
    lines = lines.withColumn("order_key", key("order_source", "order_scope", "order"))
    lines = quality.link(lines, sales["orders"], "order_key", "order_key", ["order_id", "currency"], "settlement_order", F.col("currency") == F.col("_parent_currency"))
    lines = lines.withColumn("order_id", F.col("_parent_order_id")).drop("_parent_order_id", "_parent_currency")
    lines = lines.withColumn("settlement_line_id", identifier("SETL-", "source", "scope", "settlement", "settlement_line"))
    quality.unique(lines, ["order_key"], "multiple_final_statements_per_order")
    compared = lines.join(sales["cash"].select("order_id", "currency", F.col("gross").alias("cash_gross"), F.col("refunds").alias("cash_refunds")), ["order_id", "currency"], "left").fillna(0, ["cash_gross", "cash_refunds"])
    reconciliations = []
    for metric in ("gross", "refunds"):
        quality.add(compared, F.col(metric) != F.col("cash_" + metric), "settlement_" + metric + "_mismatch")
        reconciliations.append(compared.select("settlement_id", "order_id", F.lit(metric).alias("check"), F.col(metric).alias("expected"),
            F.col("cash_" + metric).alias("actual"), (F.col("cash_" + metric) - F.col(metric)).alias("delta"),
            F.when(F.col("cash_" + metric) == F.col(metric), "passed").otherwise("failed").alias("result")))
    totals = lines.groupBy("settlement_id").agg(F.count("settlement_line_id").alias("actual_lines"),
        *[F.sum(metric).alias("actual_" + metric) for metric in metrics])
    totals = headers.join(totals, "settlement_id", "left").fillna(0, ["actual_lines"] + ["actual_" + metric for metric in metrics])
    quality.add(totals, F.col("actual_lines") < F.col("line_count"), "settlement_lines_incomplete", severity="waiting")
    quality.add(totals, F.col("actual_lines") > F.col("line_count"), "settlement_lines_excess")
    for metric in metrics:
        quality.add(totals, F.col("actual_" + metric) != F.col(metric), "settlement_total_" + metric)
    transfers = extract(events, "payout_transfer", ("transaction", "settlement_source", "settlement_scope", "settlement", "currency", "status"), ("amount",))
    transfers = transfers.withColumn("settlement_key", key("settlement_source", "settlement_scope", "settlement"))
    transfers = quality.link(transfers, headers, "settlement_key", "settlement_key", ["settlement_id", "currency"], "payout_settlement", F.col("currency") == F.col("_parent_currency"))
    transfers = transfers.withColumn("settlement_id", F.col("_parent_settlement_id"))
    bank = transfers.filter("status = 'succeeded'").groupBy("settlement_id").agg(F.sum("amount").alias("bank"))
    bank = headers.join(bank, "settlement_id", "left").fillna(0, ["bank"])
    bank = bank.withColumn("result", F.when(F.col("payout") < 0, "carry").when(F.col("bank") == F.col("payout"), "passed").when(F.col("bank") < F.col("payout"), "waiting").otherwise("failed"))
    quality.add(bank, F.col("result") == "waiting", "bank_payout_incomplete", "settlement_id", "waiting")
    quality.add(bank, F.col("result") == "failed", "bank_payout_excess", "settlement_id")
    reconciliations.append(bank.select("settlement_id", F.lit(None).cast("string").alias("order_id"), F.lit("bank_payout").alias("check"),
        F.col("payout").alias("expected"), F.col("bank").alias("actual"), (F.col("bank") - F.col("payout")).alias("delta"), "result"))
    reconciliation = reconciliations[0]
    for frame in reconciliations[1:]:
        reconciliation = reconciliation.unionByName(frame)
    return dict(settlements=headers, settlement_lines=lines, payout_transfers=transfers, settlement_reconciliation=reconciliation)
