"""Read-only operational status. Does not start jobs or modify AWS resources."""
import argparse
import json


def inspect(session, stack, run_token=None):
    cfn, glue, states, sns, s3 = [session.client(name) for name in ("cloudformation", "glue", "stepfunctions", "sns", "s3")]
    deployed = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    outputs = {item["OutputKey"]: item["OutputValue"] for item in deployed.get("Outputs", [])}
    report = {"stack_status": deployed["StackStatus"], "outputs": outputs,
        "glue_runs": glue.get_job_runs(JobName=outputs["GlueJobName"], MaxResults=5).get("JobRuns", []),
        "workflow_executions": states.list_executions(stateMachineArn=outputs["WorkflowArn"], maxResults=5).get("executions", [])}
    if "AlertsTopicArn" in outputs:
        subscriptions = []
        for page in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=outputs["AlertsTopicArn"]):
            subscriptions.extend(page.get("Subscriptions", []))
        report["email_subscriptions"] = [item for item in subscriptions if item["Protocol"] == "email"]
    prefix = outputs.get("OutputPrefix")
    if prefix:
        try:
            response = s3.get_object(Bucket=outputs["DataBucket"], Key=prefix + "/latest.json")
            with response["Body"] as stream:
                report["publication"] = json.load(stream)
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") != "NoSuchKey":
                raise
            report["publication"] = None
    if run_token:
        ledger = session.resource("dynamodb").Table(outputs["BatchTable"])
        report["run"] = ledger.get_item(Key={"pk": "PIPELINE#" + stack, "sk": "RUN#" + run_token}, ConsistentRead=True).get("Item")
        report["requested_run_is_latest"] = (report.get("publication") or {}).get("attempt") == run_token
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stack", default="thread-clothing-pipeline")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--profile")
    parser.add_argument("--run-token")
    args = parser.parse_args()
    import boto3
    result = inspect(boto3.Session(profile_name=args.profile, region_name=args.region), args.stack, args.run_token)
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
