"""Row validation followed by distributed identity/version resolution.

Python executes only contract validation for one input row. History, duplicates,
revisions, rejections and mapping use Spark joins/windows, never driver replay.
"""
import hashlib
import json

from .contracts import canonical_json, validate

REVISION_FIELDS = {
    "order": ("order", "line"), "payment": ("transaction",),
    "inventory_snapshot": ("snapshot", "sku", "location", "condition"),
    "purchase_order_line": ("purchase_order", "purchase_line"),
    "settlement_line": ("settlement", "settlement_line"),
    "settlement": ("settlement",), "payout_transfer": ("transaction",),
}
IMMUTABLE_FIELDS = {
    "payment": ("order_source", "order_scope", "order", "currency", "kind", "original_transaction"),
    "purchase_order_line": ("supplier", "sku", "location", "currency"),
    "settlement_line": ("order_source", "order_scope", "order", "currency"),
    "payout_transfer": ("settlement_source", "settlement_scope", "settlement", "currency"),
}
ENVELOPE_DDL = """source string, scope string, event_id string, type string,
occurred_at string, source_version long, payload_json string, event_json string,
event_key string, entity_key string, immutable_key string, payload_hash string,
event_hash string, validation_error string"""


def decode_event(raw):
    empty = {field.strip().split()[0]: None for field in ENVELOPE_DDL.split(",")}
    try:
        event = validate(json.loads(raw))
        payload = event["payload"]
        key = [event["source"], event["scope"], event["event_id"]]
        entity = [event["source"], event["scope"], event["type"]]
        if event["type"] in REVISION_FIELDS:
            entity += [payload[field] for field in REVISION_FIELDS[event["type"]]]
        else:
            entity += [event["event_id"]]
        body = canonical_json(event)
        return dict(source=event["source"], scope=event["scope"], event_id=event["event_id"], type=event["type"],
                    occurred_at=event["occurred_at"], source_version=event.get("source_version", 1),
                    payload_json=canonical_json(payload), event_json=body, event_key=canonical_json(key),
                    entity_key=canonical_json(entity),
                    immutable_key=canonical_json([payload.get(field) for field in IMMUTABLE_FIELDS.get(event["type"], ())]),
                    payload_hash=hashlib.sha256(canonical_json(payload).encode()).hexdigest(),
                    event_hash=hashlib.sha256(body.encode()).hexdigest(), validation_error=None)
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        return dict(empty, validation_error=str(exc))


def parse_rows(raw_rows):
    from pyspark.sql import functions as F
    parser = F.udf(decode_event, ENVELOPE_DDL)
    rows = raw_rows.withColumn("parsed", parser("value")).select("*", "parsed.*").drop("parsed")
    rows = rows.withColumn("validation_error", F.when(
        F.col("validation_error").isNull() & ((F.col("source") != F.col("manifest_source")) | (F.col("scope") != F.col("manifest_scope"))),
        F.lit("source/scope differs from manifest")).otherwise(F.col("validation_error")))
    return rows.withColumn("occurred_at_utc", F.to_timestamp("occurred_at"))


def resolve_versions(history):
    from pyspark.sql import functions as F, Window
    checks = []
    identity = history.groupBy("event_key").agg(F.countDistinct("event_hash").alias("variants"))
    checks.append(identity.filter("variants > 1").select(F.lit("event_identity_conflict").alias("check"), F.col("event_key").alias("key")))
    rejected = history.filter("type = 'event_rejection'").select(F.to_json(F.array(
        F.get_json_object("payload_json", "$.target_source"), F.get_json_object("payload_json", "$.target_scope"),
        F.get_json_object("payload_json", "$.target_event_id"))).alias("event_key")).distinct()
    missing = rejected.join(history.select("event_key").distinct(), "event_key", "left_anti")
    checks.append(missing.select(F.lit("rejection_target_missing").alias("check"), F.col("event_key").alias("key")))
    active = history.join(rejected.withColumn("rejected", F.lit(True)), "event_key", "left").fillna(False, ["rejected"])
    usable = active.filter("NOT rejected")
    conflicts = usable.groupBy("entity_key", "source_version").agg(F.countDistinct("payload_hash").alias("variants"))
    checks.append(conflicts.filter("variants > 1").select(F.lit("entity_version_conflict").alias("check"), F.col("entity_key").alias("key")))
    immutable = usable.groupBy("entity_key").agg(F.countDistinct("immutable_key").alias("variants"))
    checks.append(immutable.filter("variants > 1").select(F.lit("immutable_identity_changed").alias("check"), F.col("entity_key").alias("key")))
    # Dedup before row_number; deterministic choice for identical entity/version copies.
    window = Window.partitionBy("entity_key").orderBy(F.desc("source_version"), "event_key", "raw_file", "batch")
    current = usable.withColumn("rank", F.row_number().over(window)).filter("rank = 1").drop("rank")
    selected = current.select("event_key").withColumn("is_current", F.lit(True))
    standardized = active.join(selected, "event_key", "left").fillna(False, ["is_current"])
    issues = checks[0]
    for frame in checks[1:]:
        issues = issues.unionByName(frame)
    # A rejected valid event is retained in bronze, but excluded from active identity conflicts.
    issues = issues.filter("check <> 'event_identity_conflict'").unionByName(
        usable.groupBy("event_key").agg(F.countDistinct("event_hash").alias("variants")).filter("variants > 1")
        .select(F.lit("event_identity_conflict").alias("check"), F.col("event_key").alias("key")))
    return standardized, current, issues


def apply_mappings(events, catalog, keep_unresolved=False):
    from pyspark.sql import functions as F
    spark = events.sparkSession
    mappings = spark.createDataFrame(catalog["sku_mappings"], "source string, scope string, code string, canonical_id string, valid_from string, valid_to string")
    needs_sku = events.withColumn("source_sku", F.get_json_object("payload_json", "$.sku"))
    needs_sku = needs_sku.withColumn("mapping_at", F.to_timestamp(F.when(F.col("type") == "inventory_snapshot",
        F.get_json_object("payload_json", "$.as_of")).otherwise(F.col("occurred_at"))))
    e, m = needs_sku.alias("e"), F.broadcast(mappings).alias("m")
    condition = (F.col("e.source") == F.col("m.source")) & (F.col("e.scope") == F.col("m.scope")) & (F.col("e.source_sku") == F.col("m.code"))
    condition &= (F.col("e.mapping_at") >= F.to_timestamp("m.valid_from")) & (F.col("m.valid_to").isNull() | (F.col("e.mapping_at") < F.to_timestamp("m.valid_to")))
    joined = e.join(m, condition, "left").select("e.*", F.col("m.canonical_id").alias("sku_id"))
    counts = joined.groupBy("event_key").agg(F.count("sku_id").alias("mapping_count"), F.first("sku_id").alias("sku_id"))
    resolved = needs_sku.join(counts, "event_key")
    pending = resolved.filter("source_sku IS NOT NULL AND mapping_count <> 1")
    clean = resolved if keep_unresolved else resolved.filter("source_sku IS NULL OR mapping_count = 1")
    clean = clean.withColumn("sku_id", F.when(F.col("mapping_count") == 1, F.col("sku_id"))).drop("mapping_count", "mapping_at", "source_sku")
    return clean, pending
