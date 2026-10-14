"""Iceberg state, immutable publication maps and snapshot-aware reads."""
import re
import uuid

from pyspark.sql import functions as F, Window


class IcebergStore:
    def __init__(self, spark, catalog, database):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", database) or not re.fullmatch(r"[a-z][a-z0-9_]*", catalog):
            raise ValueError("invalid catalog/database identifier")
        self.spark, self.namespace = spark, catalog + "." + database
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + self.namespace)

    def table(self, name):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("invalid table identifier")
        return self.namespace + "." + name

    def merge_bronze(self, incoming):
        target = self.table("bronze_events")
        window = Window.partitionBy("event_hash").orderBy("received_at", "raw_file", "batch")
        incoming = incoming.withColumn("duplicate_rank", F.row_number().over(window)).filter("duplicate_rank = 1").drop("duplicate_rank")
        if not self.spark.catalog.tableExists(target):
            incoming.writeTo(target).using("iceberg").partitionedBy(F.days("occurred_at_utc"), "source").tableProperty("format-version", "2").create()
        else:
            view = "incoming_" + uuid.uuid4().hex
            incoming.createOrReplaceTempView(view)
            try:
                self.spark.sql("MERGE INTO " + target + " t USING " + view + " s ON t.event_hash = s.event_hash WHEN NOT MATCHED THEN INSERT *")
            finally:
                self.spark.catalog.dropTempView(view)
        return self.spark.table(target)

    def publish_table(self, name, frame):
        target = self.table(name)
        if self.spark.catalog.tableExists(target):
            # Existing table is immutable in schema during v2. Schema migrations are explicit.
            frame.writeTo(target).overwrite(F.lit(True))
        else:
            writer = frame.writeTo(target).using("iceberg").tableProperty("format-version", "2").tableProperty("write.target-file-size-bytes", str(128 * 1024 * 1024))
            if name == "standardized":
                writer = writer.partitionedBy("business_date", "source")
            writer.create()
        snapshot = self.spark.sql("SELECT snapshot_id FROM " + target + ".snapshots ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1").first()
        return dict(identifier=target, snapshot_id=str(snapshot.snapshot_id) if snapshot else None, schema_json=frame.schema.json())


def read_publication(spark, publication, table):
    """Readers pin every table to the publication, never mix current snapshots."""
    import json
    from pyspark.sql.types import StructType
    entry = publication["tables"][table]
    schema = StructType.fromJson(json.loads(entry["schema_json"]))
    if entry["snapshot_id"] is None:
        return spark.createDataFrame([], schema)
    frame = spark.read.format("iceberg").option("snapshot-id", entry["snapshot_id"]).load(entry["identifier"])
    return frame.select(*[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema])
