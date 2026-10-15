"""CloudFormation definition; importing/building never calls AWS."""

def ref(name):
    return {"Ref": name}


def arn(name):
    return {"Fn::GetAtt": [name, "Arn"]}


def sub(value):
    return {"Fn::Sub": value}


def role(service, statements):
    return {"Type": "AWS::IAM::Role", "Properties": {
        "PermissionsBoundary": sub("arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/thread-runtime-boundary"),
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]},
        "Policies": [{"PolicyName": "PipelineAccess", "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}]}}


def allow(actions, resources):
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def template():
    resources = {}
    resources["DataBucket"] = {"Type": "AWS::S3::Bucket", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "VersioningConfiguration": {"Status": "Enabled"},
        "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        "PublicAccessBlockConfiguration": {k: True for k in ("BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets")},
        "NotificationConfiguration": {"EventBridgeConfiguration": {"EventBridgeEnabled": True}}}}
    resources["BucketPolicy"] = {"Type": "AWS::S3::BucketPolicy", "Properties": {"Bucket": ref("DataBucket"), "PolicyDocument": {
        "Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:*",
            "Resource": [arn("DataBucket"), sub("${DataBucket.Arn}/*")], "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}}}
    resources["BatchTable"] = {"Type": "AWS::DynamoDB::Table", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "BillingMode": "PAY_PER_REQUEST", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "SSESpecification": {"SSEEnabled": True}}}
    resources["DeadLetterQueue"] = {"Type": "AWS::SQS::Queue", "Properties": {"MessageRetentionPeriod": 1209600, "SqsManagedSseEnabled": True}}
    resources["InputQueue"] = {"Type": "AWS::SQS::Queue", "Properties": {"VisibilityTimeout": 360, "SqsManagedSseEnabled": True,
        "RedrivePolicy": {"deadLetterTargetArn": arn("DeadLetterQueue"), "maxReceiveCount": 5}}}
    resources["ManifestRule"] = {"Type": "AWS::Events::Rule", "Properties": {"EventPattern": {
        "source": ["aws.s3"], "detail-type": ["Object Created"], "detail": {"bucket": {"name": [ref("DataBucket")]},
        "object": {"key": [{"prefix": "thread/input/manifests/"}]}}}, "Targets": [{"Id": "Queue", "Arn": arn("InputQueue")}]}}
    resources["QueuePolicy"] = {"Type": "AWS::SQS::QueuePolicy", "Properties": {"Queues": [ref("InputQueue")], "PolicyDocument": {
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"},
        "Action": "sqs:SendMessage", "Resource": arn("InputQueue"), "Condition": {"ArnEquals": {"aws:SourceArn": arn("ManifestRule")}}}]}}}
    resources["ControlRole"] = role("lambda.amazonaws.com", [
        allow(["s3:GetObject"], sub("${DataBucket.Arn}/thread/*")), allow(["s3:PutObject"], sub("${DataBucket.Arn}/thread/output/plans/*")),
        allow(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Scan"], arn("BatchTable")),
        allow(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], arn("InputQueue")),
        allow(["logs:CreateLogStream", "logs:PutLogEvents"], sub("arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/${AWS::StackName}-control:*"))])
    resources["ControlFunction"] = {"Type": "AWS::Lambda::Function", "Properties": {
        "FunctionName": sub("${AWS::StackName}-control"), "Runtime": "python3.12", "Handler": "cloud.control.handler",
        "Role": arn("ControlRole"), "Timeout": 60, "MemorySize": 256,
        "Code": {"S3Bucket": ref("ArtifactBucket"), "S3Key": sub("${ArtifactPrefix}/control.zip")},
        "Environment": {"Variables": {"BATCH_TABLE": ref("BatchTable"), "DATA_BUCKET": ref("DataBucket")}}}}
    resources["ControlLogs"] = {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": sub("/aws/lambda/${AWS::StackName}-control"), "RetentionInDays": 7}}
    resources["QueueMapping"] = {"Type": "AWS::Lambda::EventSourceMapping", "Properties": {
        "EventSourceArn": arn("InputQueue"), "FunctionName": ref("ControlFunction"), "BatchSize": 5,
        "FunctionResponseTypes": ["ReportBatchItemFailures"], "Enabled": True}}
    resources["CatalogDatabase"] = {"Type": "AWS::Glue::Database", "Properties": {"CatalogId": ref("AWS::AccountId"), "DatabaseInput": {"Name": ref("DatabaseName")}}}
    resources["GlueRole"] = role("glue.amazonaws.com", [
        allow(["s3:ListBucket"], arn("DataBucket")), allow(["s3:GetObject"], [sub("${DataBucket.Arn}/thread/*"), sub("arn:${AWS::Partition}:s3:::${ArtifactBucket}/${ArtifactPrefix}/*")]),
        allow(["s3:PutObject"], sub("${DataBucket.Arn}/thread/output/*")),
        allow(["glue:CreateTable", "glue:GetDatabase"], [sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:catalog"),
             sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:database/${DatabaseName}"),
             sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:table/${DatabaseName}/*")]),
        allow(["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], sub("arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws-glue/*"))])
    resources["GlueJob"] = {"Type": "AWS::Glue::Job", "Properties": {
        "Name": sub("${AWS::StackName}-normalize"),
        "Role": arn("GlueRole"), "Command": {"Name": "pythonshell", "PythonVersion": "3.9", "ScriptLocation": sub("s3://${ArtifactBucket}/${ArtifactPrefix}/glue_job.py")},
        "ExecutionProperty": {"MaxConcurrentRuns": 1}, "MaxRetries": 0, "Timeout": 10, "MaxCapacity": ref("GlueCapacity"),
        "DefaultArguments": {"--bucket": ref("DataBucket"), "--input-prefix": "thread/input", "--output-prefix": "thread/output",
            "--parquet": "true", "--catalog-database": ref("DatabaseName"), "--library-set": "analytics",
            "--additional-python-modules": "numpy==1.26.4,pyarrow==12.0.1", "--max-records": ref("MaxRecords")}}}
    resources["WorkflowRole"] = role("states.amazonaws.com", [
        allow(["lambda:InvokeFunction"], arn("ControlFunction")),
        allow(["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:job/${GlueJob}"))])
    definition = {"StartAt": "Plan", "TimeoutSeconds": 900, "States": {
        "Plan": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"), "Payload.$": "$"}, "OutputPath": "$.Payload", "Next": "Ready"},
        "Ready": {"Type": "Choice", "Choices": [{"Variable": "$.skip", "BooleanEquals": True, "Next": "NothingToRun"}], "Default": "Glue"},
        "NothingToRun": {"Type": "Succeed"},
        "Glue": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {
            "JobName": ref("GlueJob"), "Arguments": {"--plan-key.$": "$.plan_key", "--run-token.$": "$.run_token"}},
            "ResultPath": "$.glue", "Next": "Complete", "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "MarkFailed"}]},
        "Complete": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"),
            "Payload": {"action": "complete", "run_token.$": "$.run_token", "failed": False}}, "End": True},
        "MarkFailed": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"),
            "Payload": {"action": "complete", "run_token.$": "$.run_token", "failed": True}}, "Next": "Failed"},
        "Failed": {"Type": "Fail", "Error": "GlueRunFailed"}}}
    resources["Workflow"] = {"Type": "AWS::StepFunctions::StateMachine", "Properties": {"StateMachineName": sub("${AWS::StackName}-workflow"), "RoleArn": arn("WorkflowRole"), "StateMachineType": "STANDARD", "Definition": definition}}
    resources["ScheduleRole"] = role("events.amazonaws.com", [allow(["states:StartExecution"], ref("Workflow"))])
    resources["DailySchedule"] = {"Type": "AWS::Events::Rule", "Properties": {"ScheduleExpression": "cron(0 19 * * ? *)", "State": ref("ScheduleState"),
        "Targets": [{"Id": "Workflow", "Arn": ref("Workflow"), "RoleArn": arn("ScheduleRole"), "Input": "{}"}]}}
    resources["FailuresAlarm"] = {"Type": "AWS::CloudWatch::Alarm", "Properties": {
        "Namespace": "AWS/States", "MetricName": "ExecutionsFailed", "Dimensions": [{"Name": "StateMachineArn", "Value": ref("Workflow")}],
        "Statistic": "Sum", "Period": 300, "EvaluationPeriods": 1, "Threshold": 1, "ComparisonOperator": "GreaterThanOrEqualToThreshold", "TreatMissingData": "notBreaching"}}
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "THREAD pipeline v1 - offline reviewed before deployment", "Parameters": {
        "ArtifactBucket": {"Type": "String", "MinLength": 3, "Description": "Existing same-region bucket for built code; provision only after approval"},
        "ArtifactPrefix": {"Type": "String", "Default": "thread-runtime/v1"},
        "DatabaseName": {"Type": "String", "Default": "thread_pipeline", "AllowedPattern": "[a-z][a-z0-9_]{0,62}"},
        "ScheduleState": {"Type": "String", "Default": "DISABLED", "AllowedValues": ["DISABLED", "ENABLED"]},
        "GlueCapacity": {"Type": "Number", "Default": 0.0625, "AllowedValues": [0.0625, 1]},
        "MaxRecords": {"Type": "String", "Default": "10000", "AllowedPattern": "[1-9][0-9]{0,6}"}},
        "Resources": resources, "Outputs": {"DataBucket": {"Value": ref("DataBucket")}, "BatchTable": {"Value": ref("BatchTable")},
            "WorkflowArn": {"Value": ref("Workflow")}, "GlueJobName": {"Value": ref("GlueJob")}, "CatalogDatabase": {"Value": ref("DatabaseName")}}}
