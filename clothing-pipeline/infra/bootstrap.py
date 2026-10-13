"""Account-specific deployment bootstrap, separate from the pipeline stack."""
import re


def template(account, region="ap-southeast-1", repository="Sater17/thread-clothing-data-pipeline", provider_exists=False):
    if not re.fullmatch(r"\d{12}", account):
        raise ValueError("invalid account")
    if region != "ap-southeast-1" or repository != "Sater17/thread-clothing-data-pipeline":
        raise ValueError("unsupported deployment destination")
    stack = "thread-clothing-pipeline"
    bucket = "thread-artifacts-" + account + "-" + region
    def arn(service, resource):
        return "arn:aws:" + service + ":" + region + ":" + account + ":" + resource
    def allow(actions, resources, condition=None):
        item = dict(Effect="Allow", Action=actions, Resource=resources)
        if condition:
            item["Condition"] = condition
        return item
    provider = "arn:aws:iam::" + account + ":oidc-provider/token.actions.githubusercontent.com"
    boundary = "arn:aws:iam::" + account + ":policy/thread-runtime-boundary"
    policy = [
        allow(["cloudformation:CreateChangeSet", "cloudformation:DescribeChangeSet", "cloudformation:ExecuteChangeSet", "cloudformation:DeleteChangeSet", "cloudformation:DescribeStacks", "cloudformation:DescribeStackEvents", "cloudformation:ListStackResources", "cloudformation:GetTemplate"], [arn("cloudformation", "stack/" + stack + "/*"), arn("cloudformation", "changeSet/release-*/*")]),
        allow(["s3:GetObject", "s3:PutObject"], "arn:aws:s3:::" + bucket + "/thread-runtime/*"),
        allow(["s3:GetBucketLocation"], "arn:aws:s3:::" + bucket),
        allow(["s3:*"], ["arn:aws:s3:::" + stack + "-*", "arn:aws:s3:::" + stack + "-*/*"]),
        allow(["dynamodb:*"], arn("dynamodb", "table/" + stack + "-*")),
        allow(["lambda:*"], arn("lambda", "function:" + stack + "-*")),
        allow(["lambda:CreateEventSourceMapping", "lambda:GetEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping", "lambda:ListEventSourceMappings"], "*"),
        allow(["sqs:*"], arn("sqs", stack + "-*")),
        allow(["events:*"], arn("events", "rule/" + stack + "-*")),
        allow(["states:*"], [arn("states", "stateMachine:" + stack + "-*"), arn("states", "execution:" + stack + "-*:*")]),
        allow(["glue:*"], [arn("glue", "catalog"), arn("glue", "database/thread_pipeline"), arn("glue", "table/thread_pipeline/*"), arn("glue", "userDefinedFunction/thread_pipeline/*"), arn("glue", "job/" + stack + "-*")]),
        allow(["logs:*"], [arn("logs", "log-group:/aws/lambda/" + stack + "-*:*")]),
        allow(["cloudwatch:*"], arn("cloudwatch", "alarm:" + stack + "-*")),
        allow(["iam:CreateRole"], "arn:aws:iam::" + account + ":role/" + stack + "-*", {"StringEquals": {"iam:PermissionsBoundary": boundary}}),
        allow(["iam:GetRole", "iam:DeleteRole", "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies", "iam:UpdateAssumeRolePolicy", "iam:TagRole", "iam:UntagRole"], "arn:aws:iam::" + account + ":role/" + stack + "-*"),
        allow(["iam:GetPolicy"], boundary),
        allow(["iam:PassRole"], "arn:aws:iam::" + account + ":role/" + stack + "-*", {"StringEquals": {"iam:PassedToService": ["lambda.amazonaws.com", "glue.amazonaws.com", "states.amazonaws.com", "events.amazonaws.com"]}}),
    ]
    runtime = [
        allow(["s3:ListBucket"], "arn:aws:s3:::" + stack + "-*"),
        allow(["s3:GetObject"], ["arn:aws:s3:::" + stack + "-*/thread/*", "arn:aws:s3:::" + bucket + "/thread-runtime/*"]),
        allow(["s3:PutObject"], "arn:aws:s3:::" + stack + "-*/thread/output/*"),
        allow(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Scan"], arn("dynamodb", "table/" + stack + "-*")),
        allow(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], arn("sqs", stack + "-*")),
        allow(["lambda:InvokeFunction"], arn("lambda", "function:" + stack + "-*")),
        allow(["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], arn("glue", "job/" + stack + "-*")),
        allow(["glue:CreateTable", "glue:GetDatabase"], [arn("glue", "catalog"), arn("glue", "database/thread_pipeline"), arn("glue", "table/thread_pipeline/*")]),
        allow(["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], [arn("logs", "log-group:/aws/lambda/" + stack + "-*:*") , arn("logs", "log-group:/aws-glue/*")]),
        allow(["states:StartExecution"], arn("states", "stateMachine:" + stack + "-*")),
    ]
    resources = {
        "RuntimeBoundary": {"Type": "AWS::IAM::ManagedPolicy", "Properties": {"ManagedPolicyName": "thread-runtime-boundary", "PolicyDocument": {"Version": "2012-10-17", "Statement": runtime}}},
        "Artifacts": {"Type": "AWS::S3::Bucket", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
            "BucketName": bucket, "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            "PublicAccessBlockConfiguration": {k: True for k in ("BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets")}}},
        "DeployRole": {"Type": "AWS::IAM::Role", "Properties": {"RoleName": "thread-github-deploy", "MaxSessionDuration": 3600,
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Federated": provider}, "Action": "sts:AssumeRoleWithWebIdentity", "Condition": {"StringEquals": {
                "token.actions.githubusercontent.com:aud": "sts.amazonaws.com", "token.actions.githubusercontent.com:sub": "repo:Sater17@79832416/thread-clothing-data-pipeline@1416576645:ref:refs/heads/main"}}}]},
            "Policies": [{"PolicyName": "ThreadDeployment", "PolicyDocument": {"Version": "2012-10-17", "Statement": policy}}]}}
    }
    if not provider_exists:
        resources["GitHubOidc"] = {"Type": "AWS::IAM::OIDCProvider", "Properties": {"Url": "https://token.actions.githubusercontent.com", "ClientIdList": ["sts.amazonaws.com"]}}
        resources["DeployRole"]["DependsOn"] = "GitHubOidc"
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "THREAD GitHub OIDC and release bucket", "Resources": resources,
        "Outputs": {"ArtifactBucket": {"Value": bucket}, "DeployRoleArn": {"Value": {"Fn::GetAtt": ["DeployRole", "Arn"]}}}}
