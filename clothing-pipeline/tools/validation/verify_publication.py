"""Read exact published Iceberg snapshots and compare the 24-event smoke oracle."""
import json
import sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
if sys.platform == "win32":
    sys.path.insert(0, str(ROOT / ".deps"))
from scripts.aws_cli import call, linux_path

EVIDENCE = ROOT / "data/aws-smoke-v2"


def main():
    import fastavro
    import pyarrow.parquet as pq
    stack = call(["cloudformation", "describe-stacks", "--stack-name", "thread-clothing-pipeline"])["Stacks"][0]
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    bucket, prefix = outputs["DataBucket"], outputs["OutputPrefix"]
    execution = json.loads((EVIDENCE / "execution.json").read_text())
    details = call(["stepfunctions", "describe-execution", "--execution-arn", execution["executionArn"]])
    if details["status"] != "SUCCEEDED":
        raise ValueError("workflow has not succeeded")
    token = json.loads(details["output"])["Payload"]["run_token"]
    cache = EVIDENCE / ("snapshot-" + token)
    cache.mkdir(exist_ok=True)

    def get(key, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        call(["s3api", "get-object", "--bucket", bucket, "--key", key, linux_path(path)])
        return path

    publication = json.loads(get(prefix + "/latest.json", cache / "publication.json").read_text())
    assert publication["attempt"] == token and publication["quality"] == "passed"
    assert len(publication["tables"]) == 24
    assert publication["metrics"]["records_read"] == 24
    assert publication["metrics"]["quality_issues"] == publication["metrics"]["records_quarantined"] == 0
    warehouse_prefix = prefix + "/warehouse/"
    listing = call(["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", warehouse_prefix, "--max-keys", "1000", "--no-paginate"])
    objects = listing.get("Contents", [])
    if listing.get("IsTruncated") or len(objects) > 1000 or sum(item["Size"] for item in objects) > 20 * 1024 * 1024:
        raise ValueError("smoke warehouse exceeds download bound")
    destination = cache / "warehouse"
    destination.mkdir(exist_ok=True)
    call(["s3", "cp", "s3://" + bucket + "/" + warehouse_prefix, linux_path(destination), "--recursive", "--only-show-errors"])

    def local(uri):
        parsed = urlsplit(uri)
        if parsed.scheme != "s3" or parsed.netloc != bucket or not parsed.path.lstrip("/").startswith(warehouse_prefix):
            raise ValueError("snapshot reference outside smoke warehouse")
        relative = parsed.path.lstrip("/")[len(warehouse_prefix):]
        if ".." in Path(relative).parts:
            raise ValueError("unsafe snapshot path")
        return destination / relative

    catalog = call(["glue", "get-tables", "--database-name", outputs["CatalogDatabase"]])["TableList"]
    catalog = {table["Name"]: table for table in catalog}
    summaries, snapshots = {}, {}
    for name, entry in publication["tables"].items():
        metadata = json.loads(local(catalog[name]["Parameters"]["metadata_location"]).read_text())
        if entry["snapshot_id"] is None:
            summaries[name], snapshots[name] = 0, None
            continue
        snapshot = next(item for item in metadata["snapshots"] if str(item["snapshot-id"]) == entry["snapshot_id"])
        snapshots[name] = snapshot
        summaries[name] = int(snapshot["summary"]["total-records"])

    def rows(name):
        snapshot = snapshots[name]
        if snapshot is None:
            return []
        files = set()
        with local(snapshot["manifest-list"]).open("rb") as stream:
            manifests = list(fastavro.reader(stream))
        for manifest in manifests:
            with local(manifest["manifest_path"]).open("rb") as stream:
                for entry in fastavro.reader(stream):
                    if entry["status"] == 2:
                        continue
                    data = entry["data_file"]
                    if data.get("content", 0) != 0:
                        raise ValueError("delete files require a full Iceberg reader")
                    files.add(data["file_path"])
        result = [row for file in sorted(files) for row in pq.read_table(local(file)).to_pylist()]
        assert len(result) == summaries[name]
        return result

    expected = json.loads((EVIDENCE / "input/expected.json").read_text())
    inventory = {(row["sku_id"], row["location"], row["condition"]): row["quantity"] for row in rows("inventory")}
    assert inventory[("SKU-0001", "LOC-0005", "sellable")] == expected["warehouse_sellable"]
    assert inventory[("SKU-0001", "LOC-0005", "defective")] == expected["warehouse_defective"]
    assert inventory[("SKU-0001", "LOC-0006", "sellable")] == expected["store_sellable"]
    money = {}
    for payment in rows("payments"):
        if payment["status"] == "succeeded":
            key = payment["kind"] + "_" + payment["currency"]
            money[key] = money.get(key, Decimal("0")) + payment["amount"]
    assert money["charge_SGD"] == Decimal(expected["collected_SGD"])
    assert money["refund_SGD"] == Decimal(expected["refunded_SGD"])
    purchase, returned = rows("purchase_lines")[0], rows("return_lines")[0]
    assert (purchase["received_quantity"], purchase["outstanding_quantity"], purchase["accepted_quantity"], purchase["defective_quantity"]) == (80, 20, 78, 2)
    assert returned["refunded_amount"] == Decimal("27.00")
    assert summaries["stock_lineage"] == 5 and summaries["standardized"] == 24
    report = dict(quality="passed", run_token=token, table_rows=summaries, money=money,
        warehouse_sellable=inventory[("SKU-0001", "LOC-0005", "sellable")], warehouse_defective=inventory[("SKU-0001", "LOC-0005", "defective")],
        store_sellable=inventory[("SKU-0001", "LOC-0006", "sellable")], verified_from="published Iceberg manifest lists and data manifests")
    (EVIDENCE / "verification.json").write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
    print(json.dumps(report, default=str, indent=2))


if __name__ == "__main__":
    main()
