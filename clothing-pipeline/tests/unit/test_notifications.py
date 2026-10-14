from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from cloud.notify import inspect_failure, send_failure

JOB = "thread-test-normalize"
WORKFLOW = "arn:aws:states:ap-southeast-1:123456789012:stateMachine:thread-test"
EVENT = {"source": "aws.glue", "detail": {"jobName": JOB, "jobRunId": "jr_failed", "state": "FAILED"}}


def clients():
    glue, states, sns, table = [MagicMock() for _ in range(4)]
    glue.get_job_run.return_value = {"JobRun": {"Id": "jr_failed", "JobRunState": "FAILED",
        "ErrorMessage": "DataQualityError: missing SKU mapping", "Arguments": {"--run-token": "run-123"}}}
    sns.publish.return_value = {"MessageId": "message-123"}
    return glue, states, sns, table


def send(event, resources):
    return send_failure(event, *resources, JOB, WORKFLOW, "topic", "ap-southeast-1", "bucket", "thread/output/v2")


def test_failure_email_has_actual_error_and_run_diagnostics():
    resources = clients()
    assert send(EVENT, resources) == {"sent": True}
    message = resources[2].publish.call_args.kwargs
    assert "DataQualityError: missing SKU mapping" in message["Message"]
    assert "jr_failed" in message["Message"] and "run-123" in message["Message"]
    assert "/aws-glue/thread-test/error" in message["Message"]
    assert "thread/output/v2/runs/run-123/audit.json" in message["Message"]
    assert len(message["Subject"]) <= 100
    assert resources[3].put_item.call_args.kwargs["Item"]["alert_state"] == "SENT"


def test_duplicate_failure_does_not_send_again():
    resources = clients()
    resources[3].put_item.side_effect = ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
    resources[3].get_item.return_value = {"Item": {"alert_state": "SENT"}}
    assert send(EVENT, resources) == {"duplicate": True}
    resources[2].publish.assert_not_called()


def test_sns_failure_releases_owned_claim_for_retry():
    resources = clients()
    resources[2].publish.side_effect = RuntimeError("SNS unavailable")
    with pytest.raises(RuntimeError, match="SNS unavailable"):
        send(EVENT, resources)
    request = resources[3].delete_item.call_args.kwargs
    assert request["ConditionExpression"] == "#owner = :o"


@pytest.mark.parametrize("state", ["FAILED", "TIMEOUT", "STOPPED"])
def test_terminal_glue_states_are_supported(state):
    resources = clients()
    resources[0].get_job_run.return_value["JobRun"]["JobRunState"] = state
    event = {"source": "aws.glue", "detail": {**EVENT["detail"], "state": state}}
    assert send(event, resources) == {"sent": True}


def test_foreign_job_and_success_are_ignored():
    resources = clients()
    for detail in ({**EVENT["detail"], "jobName": "other"}, {**EVENT["detail"], "state": "SUCCEEDED"}):
        assert send({"source": "aws.glue", "detail": detail}, resources) == {"ignored": True}
    resources[0].get_job_run.assert_not_called()
    resources[2].publish.assert_not_called()


def test_workflow_and_glue_failure_share_deduplication_key():
    resources = clients()
    resources[1].describe_execution.return_value = {"executionArn": "execution", "status": "FAILED", "cause": '{"JobRunId":"jr_failed"}'}
    workflow_event = {"source": "aws.states", "detail": {"stateMachineArn": WORKFLOW, "executionArn": "execution", "status": "FAILED"}}
    assert inspect_failure(workflow_event, *resources[:2], JOB, WORKFLOW)["key"] == inspect_failure(EVENT, *resources[:2], JOB, WORKFLOW)["key"]
