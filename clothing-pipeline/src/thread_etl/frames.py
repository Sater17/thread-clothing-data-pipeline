"""Shared Spark expressions and quality checks across business domains."""
from pyspark.sql import functions as F


def p(field, prefix=""):
    return F.get_json_object(prefix + "payload_json", "$." + field)


def amount(field):
    return p(field).cast("decimal(20,2)")


def quantity(field="quantity"):
    return p(field).cast("long")


def key(*columns):
    return F.to_json(F.array(*[F.col(c) if isinstance(c, str) else c for c in columns]))


def identifier(prefix, *columns):
    return F.concat(F.lit(prefix), F.substring(F.sha2(key(*columns), 256), 1, 20))


def event_base():
    return [F.col(name) for name in ("event_id", "source", "scope", "raw_file", "batch", "occurred_at")]


class Quality:
    def __init__(self, spark):
        self.spark = spark
        self.frames = []

    def add(self, frame, condition, check, key_column="event_key", severity="failed"):
        self.frames.append(frame.filter(condition).select(F.lit(check).alias("check"), F.lit(severity).alias("result"),
            F.col(key_column).cast("string").alias("key")).checkpoint(eager=True))

    def extend(self, frame):
        self.frames.append(frame.checkpoint(eager=True))

    def result(self):
        result = self.spark.createDataFrame([], "check string, result string, key string")
        for frame in self.frames:
            result = result.unionByName(frame)
        return result.dropDuplicates()

    def link(self, child, parent, child_key, parent_key, fields, check, identity=None):
        """Resolve parent attributes without silently dropping missing references."""
        right = parent.select(F.col(parent_key).alias("_parent_key"), *[F.col(name).alias("_parent_" + name) for name in fields])
        joined = child.join(right, F.col(child_key) == F.col("_parent_key"), "left")
        self.add(joined, F.col("_parent_key").isNull(), check + "_missing", severity="waiting")
        if identity is not None:
            self.add(joined, F.col("_parent_key").isNotNull() & ~identity, check + "_identity")
        condition = F.col("_parent_key").isNotNull()
        if identity is not None:
            condition &= identity
        return joined.filter(condition).drop("_parent_key").checkpoint(eager=True)

    def unique(self, frame, identity_columns, check):
        collisions = frame.groupBy(*identity_columns).count().filter("count > 1")
        self.frames.append(collisions.select(F.lit(check).alias("check"), F.lit("failed").alias("result"),
            key(*identity_columns).alias("key")).checkpoint(eager=True))
        return frame
