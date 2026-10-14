"""Bounded manifest planning and distributed streaming file verification."""
import hashlib
import codecs
import json
import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

from .contracts import canonical_json, timestamp

FILE_DDL = "uri string, raw_file string, digest string, batch string, batch_key string, manifest_source string, manifest_scope string, received_at string, expected_records long"


def read_json(uri):
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        import boto3
        with boto3.client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"] as stream:
            return json.load(stream)
    return json.loads(Path(parsed.path if parsed.scheme == "file" else uri).read_text(encoding="utf-8"))


def file_plan(root_uri, selected, max_files=1000):
    files = []
    for entry in selected:
        manifest = read_json(root_uri.rstrip("/") + "/manifests/" + entry["name"])
        if hashlib.sha256(canonical_json(manifest).encode()).hexdigest() != entry["digest"]:
            raise ValueError("manifest changed after plan: " + entry["name"])
        if manifest["schema_version"] != 1 or manifest["data_kind"] not in ("events", "snapshot_rows"):
            raise ValueError("unsupported manifest")
        if type(manifest["record_count"]) is not int or manifest["record_count"] < 0 or timestamp(manifest["period_start"]) >= timestamp(manifest["period_end"]):
            raise ValueError("invalid manifest period/count")
        timestamp(manifest["received_at"])
        for item in manifest["files"]:
            path = item["path"]
            if not re.fullmatch(r"raw/[A-Za-z0-9_./-]+", path) or any(part in ("..", ".") for part in PurePosixPath(path).parts):
                raise ValueError("unsafe raw path")
            if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]):
                raise ValueError("invalid file checksum")
            files.append(dict(uri=root_uri.rstrip("/") + "/" + path, raw_file=path, digest=item["sha256"],
                batch=manifest["batch_id"], batch_key=canonical_json([manifest["source"], manifest["scope"], manifest["batch_id"]]),
                manifest_source=manifest["source"], manifest_scope=manifest["scope"], received_at=manifest["received_at"], expected_records=manifest["record_count"]))
            if len(files) > max_files:
                raise ValueError("input file planning limit exceeded; split the run")
        if not manifest["files"] and manifest["record_count"]:
            raise ValueError("nonempty manifest has no files")
    if len({item["uri"] for item in files}) != len(files):
        raise ValueError("raw object appears in multiple selected manifests")
    return files


def verify_partition(rows):
    s3 = None
    for row in rows:
        parsed = urlparse(row.uri)
        if parsed.scheme == "s3":
            if s3 is None:
                import boto3
                s3 = boto3.client("s3")
            stream = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"]
        else:
            stream = open(parsed.path if parsed.scheme == "file" else row.uri, "rb")
        digest, count, last = hashlib.sha256(), 0, None
        decoder = codecs.getincrementaldecoder("utf-8")("strict")
        with stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                decoder.decode(chunk, final=False)
                count += chunk.count(b"\n")
                last = chunk[-1]
            decoder.decode(b"", final=True)
        if last is not None and last != 10:
            count += 1
        yield row.batch_key, row.expected_records, count, digest.hexdigest() == row.digest


def read_inputs(spark, files):
    from pyspark.sql import functions as F
    metadata = spark.createDataFrame(files, FILE_DDL)
    if not files:
        return spark.createDataFrame([], FILE_DDL + ", value string")
    checked = spark.createDataFrame(metadata.rdd.mapPartitions(verify_partition), "batch_key string, expected long, actual long, checksum_ok boolean")
    counts = checked.groupBy("batch_key", "expected").agg(F.sum("actual").alias("actual"), F.min(F.col("checksum_ok").cast("int")).alias("checksum_ok"))
    failures = counts.filter("actual <> expected OR checksum_ok <> 1")
    if failures.limit(1).count():
        raise ValueError("raw checksum or record count mismatch; refusing intake")
    # Spark reads the records; only bounded file metadata was materialized on the driver.
    rows = spark.read.text([item["uri"] for item in files]).withColumn("uri", F.input_file_name())
    # Hadoop normalizes local file:/ to file:///; join through the normalized path.
    rows = rows.withColumn("uri", F.regexp_replace("uri", "^file:/+", "file:///"))
    rows = rows.withColumn("uri", F.regexp_replace("uri", "^s3a://", "s3://"))
    metadata = metadata.withColumn("uri", F.regexp_replace("uri", "^file:/+", "file:///"))
    if rows.select("uri").distinct().join(metadata.select("uri"), "uri", "left_anti").limit(1).count():
        raise ValueError("Spark file path does not match frozen metadata")
    return rows.join(F.broadcast(metadata), "uri", "inner")
