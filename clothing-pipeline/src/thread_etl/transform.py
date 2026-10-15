"""Stage orchestration; all business records stay in Spark DataFrames."""
from pyspark.sql import functions as F

from .documents import documents
from .events import apply_mappings, resolve_versions
from .frames import Quality
from .sales import transform as sales_transform
from .settlements import transform as settlements_transform


def transform(history, catalog):
    spark = history.sparkSession
    quality = Quality(spark)
    standardized, current, issues = resolve_versions(history)
    quality.extend(issues.withColumn("result", F.when(F.col("check") == "rejection_target_missing", "waiting").otherwise("failed")))
    current, pending = apply_mappings(current, catalog)
    quality.extend(pending.select(F.lit("sku_mapping_missing_or_ambiguous").alias("check"), F.lit("waiting").alias("result"), F.col("event_key").alias("key")))
    # Cut repeated join/UDF evaluation without moving records to the driver.
    current = current.checkpoint(eager=True)
    standardized, _ = apply_mappings(standardized, catalog, keep_unresolved=True)
    sales = sales_transform(current, catalog, quality)
    docs = documents(current, standardized, sales, catalog, quality)
    settlements = settlements_transform(current, sales, quality)
    tables = {name: sales[name] for name in ("orders", "order_lines", "source_links", "payments", "inventory", "inventory_reconciliation")}
    tables.update(docs)
    tables.update(settlements)
    tables["standardized"] = standardized
    tables["reservations"] = sales["reservations"]
    tables["deliveries"] = sales["deliveries"]
    tables["quality_issues"] = quality.result()
    tables["pending"] = pending
    return tables
