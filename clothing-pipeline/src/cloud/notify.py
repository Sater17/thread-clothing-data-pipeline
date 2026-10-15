"""Rich failure emails for Glue and workflow events, deduplicated by job run."""
import json
import logging
import os
import time
import uuid

LOGGER = logging.getLogger("thread.alerts")


def inspect_failure(event, glue, states, job_name, workflow_arn):
    detail = event.get("detail", {})
    source = event.get("source")
    if source == "aws.glue":
        if detail.get("jobName") != job_name or detail.get("state") not in ("FAILED", "TIMEOUT", "STOPPED"):
            return None
        run = glue.get_job_run(JobName=job_name, RunId=detail["jobRunId"])["JobRun"]
        return dict(key="GLUE#" + run["Id"] + "#" + run["JobRunState"], job=job_name, job_run_id=run["Id"],
            job_state=run["JobRunState"], error=run.get("ErrorMessage", "No Glue error message available")[:2000],
            run_token=run.get("Arguments", {}).get("--run-token"), execution_arn=None)
    if source != "aws.states" or detail.get("stateMachineArn") != workflow_arn or detail.get("status") not in ("FAILED", "TIMED_OUT", "ABORTED"):
        return None
    execution = states.describe_execution(executionArn=detail["executionArn"])
    cause = execution.get("cause", execution.get("error", "Unknown workflow error"))
    try:
        parsed = json.loads(cause)
    except (TypeError, ValueError):
        parsed = {}
    job_id = parsed.get("Id") or parsed.get("JobRunId")
    token = None
    if not job_id:
        history = states.get_execution_history(executionArn=execution["executionArn"], maxResults=30, reverseOrder=True)
        for item in history["events"]:
            state = item.get("stateEnteredEventDetails", {})
            if state.get("name") in ("Complete", "MarkFailed", "Glue"):
                body = json.loads(state.get("input", "{}"))
                token = token or body.get("run_token")
                job_id = job_id or body.get("glue", {}).get("Id")
            task = item.get("taskSubmittedEventDetails", {})
            if task.get("resourceType") == "glue":
                job_id = job_id or json.loads(task.get("output", "{}" )).get("JobRunId")
    if job_id:
        run = glue.get_job_run(JobName=job_name, RunId=job_id)["JobRun"]
        if run["JobRunState"] in ("FAILED", "TIMEOUT", "STOPPED"):
            return dict(key="GLUE#" + job_id + "#" + run["JobRunState"], job=job_name, job_run_id=job_id,
                job_state=run["JobRunState"], error=run.get("ErrorMessage", cause)[:2000],
                run_token=run.get("Arguments", {}).get("--run-token", token), execution_arn=execution["executionArn"])
    return dict(key="EXEC#" + execution["executionArn"], job=job_name, job_run_id=job_id, job_state="workflow " + execution["status"],
        error=str(parsed.get("errorMessage", cause))[:2000], run_token=token, execution_arn=execution["executionArn"])


def send_failure(event, glue, states, sns, table, job_name, workflow_arn, topic_arn, region, bucket, output_prefix):
    failure = inspect_failure(event, glue, states, job_name, workflow_arn)
    if failure is None:
        return dict(ignored=True)
    now, owner = int(time.time()), uuid.uuid4().hex
    identity = dict(pk="ALERT#" + job_name, sk=failure["key"])
    try:
        table.put_item(Item=dict(**identity, alert_state="PUBLISHING", owner=owner, lease_until=now + 30, ttl=now + 7 * 86400),
            ConditionExpression="attribute_not_exists(pk) OR (#s = :publishing AND lease_until < :now)",
            ExpressionAttributeNames={"#s": "alert_state"}, ExpressionAttributeValues={":publishing": "PUBLISHING", ":now": now})
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        record = table.get_item(Key=identity, ConsistentRead=True).get("Item", {})
        if record.get("alert_state") == "SENT":
            return dict(duplicate=True)
        raise RuntimeError("notification lease is busy; retry delivery") from exc
    try:
        lines = ["Pipeline execution requires attention.", "", "Job: " + failure["job"], "State: " + failure["job_state"],
            "Glue run: " + str(failure["job_run_id"] or "not started"), "Pipeline run: " + str(failure["run_token"] or "unknown"),
            "", "Error:", failure["error"], "", "Glue console: https://" + region + ".console.aws.amazon.com/gluestudio/home?region=" + region + "#/job/" + job_name,
            "CloudWatch logs: /aws-glue/" + job_name.removesuffix("-normalize") + "/error"]
        if failure["execution_arn"]:
            lines.append("Execution: " + failure["execution_arn"])
        if failure["run_token"]:
            lines.append("Audit (when available): s3://" + bucket + "/" + output_prefix + "/runs/" + failure["run_token"] + "/audit.json")
        response = sns.publish(TopicArn=topic_arn, Subject=("[THREAD] " + failure["job_state"] + " - " + job_name)[:100], Message="\n".join(lines))
        table.put_item(Item=dict(**identity, alert_state="SENT", message_id=response["MessageId"], ttl=now + 7 * 86400))
        LOGGER.info(json.dumps(dict(event="failure_notification_sent", key=failure["key"], message_id=response["MessageId"])))
        return dict(sent=True)
    except Exception:
        table.delete_item(Key=identity, ConditionExpression="#owner = :o", ExpressionAttributeNames={"#owner": "owner"}, ExpressionAttributeValues={":o": owner})
        raise


def handler(event, context):
    import boto3
    table = boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"])
    return send_failure(event, boto3.client("glue"), boto3.client("stepfunctions"), boto3.client("sns"), table,
        os.environ["GLUE_JOB"], os.environ["WORKFLOW_ARN"], os.environ["TOPIC_ARN"], os.environ["AWS_REGION"],
        os.environ["DATA_BUCKET"], os.environ["OUTPUT_PREFIX"])
