"""Register immutable run-specific tables before publishing the run pointer."""


def register_tables(client, database, bucket, run_prefix, token, local_tables):
    import pyarrow as pa
    import pyarrow.parquet as pq

    def type_name(dtype):
        if pa.types.is_string(dtype):
            return "string"
        if pa.types.is_integer(dtype):
            return "bigint"
        if pa.types.is_boolean(dtype):
            return "boolean"
        if pa.types.is_decimal(dtype):
            return "decimal(%d,%d)" % (dtype.precision, dtype.scale)
        if pa.types.is_timestamp(dtype):
            return "timestamp"
        if pa.types.is_date(dtype):
            return "date"
        raise ValueError("unsupported catalog type: " + str(dtype))

    names = {}
    for path in sorted(local_tables.glob("*.parquet")):
        schema = pq.read_schema(path)
        name = path.stem + "_" + token
        table = dict(Name=name, TableType="EXTERNAL_TABLE", Parameters={"classification": "parquet", "EXTERNAL": "TRUE"},
                     StorageDescriptor=dict(Columns=[dict(Name=f.name, Type=type_name(f.type)) for f in schema],
                                            Location="s3://" + bucket + "/" + run_prefix + "/tables/" + path.name,
                                            InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                            OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                            SerdeInfo={"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}))
        client.create_table(DatabaseName=database, TableInput=table)
        names[path.stem] = name
    return dict(database=database, tables=names)
