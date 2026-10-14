import io
import json
import os
import time
import uuid

import boto3
import pytest

from cloud.control import complete, key, plan, register


@pytest.fixture
def ledger():
    endpoint = os.environ.get("DYNAMODB_ENDPOINT")
    if not endpoint:
        pytest.skip("requires isolated DynamoDB Local service")
    db = boto3.resource("dynamodb", endpoint_url=endpoint, region_name="ap-southeast-1", aws_access_key_id="local", aws_secret_access_key="local")
    for attempt in range(40):
        try:
            db.meta.client.list_tables()
            break
        except Exception:
            time.sleep(0.25)
    table = db.create_table(TableName="ledger-" + uuid.uuid4().hex, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[dict(AttributeName=name, AttributeType="S") for name in ("pk", "sk", "status_pk", "status_sk")],
        KeySchema=[dict(AttributeName="pk", KeyType="HASH"), dict(AttributeName="sk", KeyType="RANGE")],
        GlobalSecondaryIndexes=[dict(IndexName="by_status", KeySchema=[dict(AttributeName="status_pk", KeyType="HASH"), dict(AttributeName="status_sk", KeyType="RANGE")], Projection=dict(ProjectionType="ALL"))])
    table.wait_until_exists()
    yield table
    table.delete()


class S3:
    def __init__(self):
        self.objects = {}
    def get_object(self, Bucket, Key):
        return dict(Body=io.BytesIO(self.objects[Key]))
    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}
    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body


def source(s3):
    manifest = dict(batch_id="B1", source="web", scope="sg", schema_version=1, data_kind="events", record_count=1,
        files=[dict(path="raw/one.jsonl", sha256="a" * 64)], period_start="2026-10-14T00:00:00Z", period_end="2026-10-14T01:00:00Z", received_at="2026-10-14T01:10:00Z")
    s3.objects["thread/input/v2/catalog.json"] = b"{}"
    s3.objects["thread/input/v2/manifests/B1.json"] = json.dumps(manifest).encode()
    s3.objects["thread/input/v2/raw/one.jsonl"] = b"{}"
    return "thread/input/v2/manifests/B1.json"


def test_query_claim_complete_and_retry_on_real_dynamodb(ledger):
    s3 = S3()
    object_key = source(s3)
    batch = register(s3, ledger, "bucket", object_key, "thread/input/v2", "tests")
    frozen = plan(s3, ledger, "bucket", "thread/input/v2", "thread/output/v2", "tests")
    assert not frozen["skip"]
    assert plan(s3, ledger, "bucket", "thread/input/v2", "thread/output/v2", "tests")["skip"]
    s3.objects["thread/output/v2/latest.json"] = json.dumps(dict(format_version=2, attempt=frozen["run_token"], quality="passed")).encode()
    complete(s3, ledger, "bucket", "thread/output/v2", "tests", frozen["run_token"])
    complete(s3, ledger, "bucket", "thread/output/v2", "tests", frozen["run_token"])
    assert ledger.get_item(Key=key("tests", "LEASE")).get("Item") is None
    assert ledger.get_item(Key=dict(pk=batch["pk"], sk=batch["sk"]))["Item"]["status"] == "COMPLETED"
    assert plan(s3, ledger, "bucket", "thread/input/v2", "thread/output/v2", "tests")["skip"]


def test_wrong_publication_keeps_owned_lease_for_recovery(ledger):
    s3 = S3()
    register(s3, ledger, "bucket", source(s3), "thread/input/v2", "tests")
    frozen = plan(s3, ledger, "bucket", "thread/input/v2", "thread/output/v2", "tests")
    s3.objects["thread/output/v2/latest.json"] = json.dumps(dict(format_version=2, attempt="other", quality="passed")).encode()
    with pytest.raises(ValueError, match="approved publication"):
        complete(s3, ledger, "bucket", "thread/output/v2", "tests", frozen["run_token"])
    assert ledger.get_item(Key=key("tests", "LEASE"))["Item"]["token"] == frozen["run_token"]
    complete(s3, ledger, "bucket", "thread/output/v2", "tests", frozen["run_token"], failed=True)


def test_email_deduplication_claim_on_real_dynamodb(ledger):
    from unittest.mock import MagicMock
    from cloud.notify import send_failure
    glue, states, sns = MagicMock(), MagicMock(), MagicMock()
    glue.get_job_run.return_value = {"JobRun": {"Id": "jr_failed", "JobRunState": "FAILED", "ErrorMessage": "invalid input"}}
    sns.publish.return_value = {"MessageId": "message-one"}
    event = {"source": "aws.glue", "detail": {"jobName": "test-normalize", "state": "FAILED", "jobRunId": "jr_failed"}}
    arguments = (event, glue, states, sns, ledger, "test-normalize", "workflow", "topic", "ap-southeast-1", "bucket", "thread/output/v2")
    assert send_failure(*arguments) == {"sent": True}
    assert send_failure(*arguments) == {"duplicate": True}
    sns.publish.assert_called_once()
