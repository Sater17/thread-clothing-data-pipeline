import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = (SparkSession.builder.master("local[2]").appName("thread-integration-tests")
        .config("spark.jars", os.environ.get("ICEBERG_JAR", "/opt/iceberg-runtime.jar"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.thread", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.thread.type", "hadoop")
        .config("spark.sql.catalog.thread.warehouse", str(tmp_path_factory.mktemp("iceberg-warehouse")))
        .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
        .config("spark.ui.retainedJobs", "20").config("spark.ui.retainedStages", "100")
        .config("spark.sql.ui.retainedExecutions", "20")
        .getOrCreate())
    session.sparkContext.setCheckpointDir(str(tmp_path_factory.mktemp("spark-checkpoints")))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
