"""Production Spark/Iceberg pipeline with a queried ledger and rich alerts."""
import json


def ref(name):
    return {"Ref": name}


def arn(name):
    return {"Fn::GetAtt": [name, "Arn"]}


def sub(value):
    return {"Fn::Sub": value}


def allow(actions, resources, condition=None):
    statement = dict(Effect="Allow", Action=actions, Resource=resources)
    if condition:
        statement["Condition"] = condition
    return statement


def role(service, statements):
    return {"Type": "AWS::IAM::Role", "Properties": {
        "PermissionsBoundary": sub("arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/thread-runtime-boundary"),
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]},
        "Policies": [{"PolicyName": "PipelineAccess", "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}]}}


def template():
    r = {}
    r["DataBucket"] = {"Type": "AWS::S3::Bucket", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "VersioningConfiguration": {"Status": "Enabled"},
        "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        "PublicAccessBlockConfiguration": {name: True for name in ("BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets")},
        "NotificationConfiguration": {"EventBridgeConfiguration": {"EventBridgeEnabled": True}},
        "LifecycleConfiguration": {"Rules": [{"Id": "SparkScratch", "Status": "Enabled", "Prefix": "thread/output/v2/checkpoints/", "ExpirationInDays": 1,
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1}}]}}}
    r["BucketPolicy"] = {"Type": "AWS::S3::BucketPolicy", "Properties": {"Bucket": ref("DataBucket"), "PolicyDocument": {
        "Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": [arn("DataBucket"), sub("${DataBucket.Arn}/*")],
            "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}}}
    # Existing v1 resources are retained for rollback/history; v2 has separate state.
    r["BatchTable"] = {"Type": "AWS::DynamoDB::Table", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "BillingMode": "PAY_PER_REQUEST", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "SSESpecification": {"SSEEnabled": True}}}
    r["BatchLedgerV2"] = {"Type": "AWS::DynamoDB::Table", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "BillingMode": "PAY_PER_REQUEST", "AttributeDefinitions": [{"AttributeName": name, "AttributeType": "S"} for name in ("pk", "sk", "status_pk", "status_sk")],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "GlobalSecondaryIndexes": [{"IndexName": "by_status", "KeySchema": [{"AttributeName": "status_pk", "KeyType": "HASH"}, {"AttributeName": "status_sk", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}],
        "SSESpecification": {"SSEEnabled": True}, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": True}, "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True}}}
    r["DeadLetterQueue"] = {"Type": "AWS::SQS::Queue", "Properties": {"MessageRetentionPeriod": 1209600, "SqsManagedSseEnabled": True}}
    r["InputQueue"] = {"Type": "AWS::SQS::Queue", "Properties": {"VisibilityTimeout": 1800, "SqsManagedSseEnabled": True,
        "RedrivePolicy": {"deadLetterTargetArn": arn("DeadLetterQueue"), "maxReceiveCount": 5}}}
    r["ManifestRule"] = {"Type": "AWS::Events::Rule", "Properties": {"EventPattern": {"source": ["aws.s3"], "detail-type": ["Object Created"],
        "detail": {"bucket": {"name": [ref("DataBucket")]}, "object": {"key": [{"prefix": "thread/input/v2/manifests/"}]}}}, "Targets": [{"Id": "Queue", "Arn": arn("InputQueue")}]}}
    r["QueuePolicy"] = {"Type": "AWS::SQS::QueuePolicy", "Properties": {"Queues": [ref("InputQueue")], "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sqs:SendMessage", "Resource": arn("InputQueue"), "Condition": {"ArnEquals": {"aws:SourceArn": arn("ManifestRule")}}}]}}}
    ledger_resources = [arn("BatchLedgerV2"), sub("${BatchLedgerV2.Arn}/index/*")]
    r["ControlRole"] = role("lambda.amazonaws.com", [
        allow(["s3:GetObject"], sub("${DataBucket.Arn}/thread/*")), allow(["s3:PutObject"], sub("${DataBucket.Arn}/thread/output/v2/plans/*")),
        allow(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:ConditionCheckItem"], ledger_resources),
        allow(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], arn("InputQueue")),
        allow(["logs:CreateLogStream", "logs:PutLogEvents"], sub("arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/${AWS::StackName}-control:*"))])
    variables = {"BATCH_TABLE": ref("BatchLedgerV2"), "DATA_BUCKET": ref("DataBucket"), "PIPELINE_NAME": ref("AWS::StackName"),
        "INPUT_PREFIX": "thread/input/v2", "OUTPUT_PREFIX": "thread/output/v2", "MAX_BATCHES": ref("MaxBatches")}
    r["ControlFunction"] = {"Type": "AWS::Lambda::Function", "Properties": {"FunctionName": sub("${AWS::StackName}-control"), "Runtime": "python3.12",
        "Handler": "cloud.control.handler", "Role": arn("ControlRole"), "Timeout": 300, "MemorySize": 512,
        "Code": {"S3Bucket": ref("ArtifactBucket"), "S3Key": sub("${ArtifactPrefix}/control.zip")}, "Environment": {"Variables": variables}}}
    r["ControlLogs"] = {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": sub("/aws/lambda/${AWS::StackName}-control"), "RetentionInDays": 7}}
    r["QueueMapping"] = {"Type": "AWS::Lambda::EventSourceMapping", "Properties": {"EventSourceArn": arn("InputQueue"), "FunctionName": ref("ControlFunction"),
        "BatchSize": 5, "FunctionResponseTypes": ["ReportBatchItemFailures"], "Enabled": True}}
    r["CatalogDatabase"] = {"Type": "AWS::Glue::Database", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "CatalogId": ref("AWS::AccountId"), "DatabaseInput": {"Name": "thread_pipeline"}}}
    r["CatalogDatabaseV2"] = {"Type": "AWS::Glue::Database", "DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain", "Properties": {
        "CatalogId": ref("AWS::AccountId"), "DatabaseInput": {"Name": ref("DatabaseName")}}}
    catalog_resources = [sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:catalog"),
        sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:database/${DatabaseName}"), sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:table/${DatabaseName}/*")]
    r["GlueRole"] = role("glue.amazonaws.com", [allow(["s3:ListBucket"], arn("DataBucket")),
        allow(["s3:GetObject"], [sub("${DataBucket.Arn}/thread/*"), sub("arn:${AWS::Partition}:s3:::${ArtifactBucket}/${ArtifactPrefix}/*")]),
        allow(["s3:PutObject", "s3:DeleteObject"], sub("${DataBucket.Arn}/thread/output/v2/*")),
        allow(["dynamodb:GetItem"], arn("BatchLedgerV2")),
        allow(["glue:CreateTable", "glue:GetTable", "glue:UpdateTable", "glue:GetDatabase"], catalog_resources),
        allow(["cloudwatch:PutMetricData"], "*", {"StringEquals": {"cloudwatch:namespace": ["Thread/Pipeline", "Glue"]}}),
        allow(["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], sub("arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws-glue/${AWS::StackName}/*"))])
    configuration = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.thread=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.thread.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.thread.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.thread.warehouse=s3://${DataBucket}/thread/output/v2/warehouse --conf spark.sql.shuffle.partitions=16"
    r["GlueJob"] = {"Type": "AWS::Glue::Job", "Properties": {"Name": sub("${AWS::StackName}-normalize"), "Role": arn("GlueRole"), "GlueVersion": "5.0",
        "Command": {"Name": "glueetl", "PythonVersion": "3", "ScriptLocation": sub("s3://${ArtifactBucket}/${ArtifactPrefix}/glue_job.py")},
        "WorkerType": "G.1X", "NumberOfWorkers": ref("SparkWorkers"), "ExecutionProperty": {"MaxConcurrentRuns": 1}, "MaxRetries": 0, "Timeout": 15,
        "DefaultArguments": {"--bucket": ref("DataBucket"), "--input-prefix": "thread/input/v2", "--output-prefix": "thread/output/v2", "--catalog-database": ref("DatabaseName"),
            "--batch-table": ref("BatchLedgerV2"), "--pipeline-name": ref("AWS::StackName"), "--max-files": "1000", "--datalake-formats": "iceberg",
            "--extra-py-files": sub("s3://${ArtifactBucket}/${ArtifactPrefix}/thread_runtime.zip"), "--additional-python-modules": "boto3==1.43.113",
            "--enable-metrics": "true", "--enable-observability-metrics": "true", "--enable-spark-ui": "true", "--spark-event-logs-path": sub("s3://${DataBucket}/thread/output/v2/spark-events/"),
            "--custom-logGroup-prefix": sub("/aws-glue/${AWS::StackName}"), "--job-bookmark-option": "job-bookmark-disable", "--conf": sub(configuration)}}}
    for name, suffix in (("GlueErrorLogs", "error"), ("GlueOutputLogs", "output")):
        r[name] = {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": sub("/aws-glue/${AWS::StackName}/" + suffix), "RetentionInDays": 7}}
    r["WorkflowRole"] = role("states.amazonaws.com", [allow(["lambda:InvokeFunction"], arn("ControlFunction")),
        allow(["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:job/${GlueJob}"))])
    retry = [{"ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"], "IntervalSeconds": 2, "MaxAttempts": 3, "BackoffRate": 2}]
    definition = {"StartAt": "Plan", "TimeoutSeconds": 1500, "States": {
        "Plan": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"), "Payload.$": "$"}, "OutputPath": "$.Payload", "Retry": retry, "Next": "Ready"},
        "Ready": {"Type": "Choice", "Choices": [{"Variable": "$.skip", "BooleanEquals": True, "Next": "NothingToRun"}], "Default": "Glue"}, "NothingToRun": {"Type": "Succeed"},
        "Glue": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": ref("GlueJob"), "Arguments": {"--plan-key.$": "$.plan_key", "--run-token.$": "$.run_token"}},
            "ResultPath": "$.glue", "Next": "Complete", "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "MarkFailed"}]},
        "Complete": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"), "Payload": {"action": "complete", "run_token.$": "$.run_token", "failed": False}}, "Retry": retry, "End": True},
        "MarkFailed": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": arn("ControlFunction"), "Payload": {"action": "complete", "run_token.$": "$.run_token", "failed": True, "error.$": "$.error"}}, "Retry": retry, "Next": "Failed"},
        "Failed": {"Type": "Fail", "Error": "GlueRunFailed"}}}
    r["Workflow"] = {"Type": "AWS::StepFunctions::StateMachine", "Properties": {"StateMachineName": sub("${AWS::StackName}-workflow"), "RoleArn": arn("WorkflowRole"), "StateMachineType": "STANDARD", "Definition": definition}}
    r["ScheduleRole"] = role("events.amazonaws.com", [allow(["states:StartExecution"], ref("Workflow"))])
    r["DailySchedule"] = {"Type": "AWS::Events::Rule", "Properties": {"ScheduleExpression": "cron(0 19 * * ? *)", "State": ref("ScheduleState"),
        "Targets": [{"Id": "Workflow", "Arn": ref("Workflow"), "RoleArn": arn("ScheduleRole"), "Input": "{}"}]}}
    r["AlertsTopic"] = {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": sub("${AWS::StackName}-alerts")}}
    r["AlertsPolicy"] = {"Type": "AWS::SNS::TopicPolicy", "Properties": {"Topics": [ref("AlertsTopic")], "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "cloudwatch.amazonaws.com"}, "Action": "sns:Publish", "Resource": ref("AlertsTopic"),
        "Condition": {"StringEquals": {"aws:SourceAccount": ref("AWS::AccountId")}, "ArnLike": {"aws:SourceArn": sub("arn:${AWS::Partition}:cloudwatch:${AWS::Region}:${AWS::AccountId}:alarm:${AWS::StackName}-*")}}}]}}}
    r["AlertEmail"] = {"Type": "AWS::SNS::Subscription", "Condition": "HasEmail", "Properties": {"TopicArn": ref("AlertsTopic"), "Protocol": "email", "Endpoint": ref("NotificationEmail")}}
    r["AlertRole"] = role("lambda.amazonaws.com", [allow(["sns:Publish"], ref("AlertsTopic")),
        allow(["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"], arn("BatchLedgerV2"), {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["ALERT#*"]}}),
        allow(["glue:GetJobRun"], sub("arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:job/${GlueJob}")),
        allow(["states:DescribeExecution", "states:GetExecutionHistory"], sub("arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:execution:${AWS::StackName}-workflow:*")),
        allow(["logs:CreateLogStream", "logs:PutLogEvents"], sub("arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/${AWS::StackName}-alerts:*"))])
    r["AlertFunction"] = {"Type": "AWS::Lambda::Function", "Properties": {"FunctionName": sub("${AWS::StackName}-alerts"), "Runtime": "python3.12", "Handler": "cloud.notify.handler",
        "Role": arn("AlertRole"), "Timeout": 60, "MemorySize": 256, "Code": {"S3Bucket": ref("ArtifactBucket"), "S3Key": sub("${ArtifactPrefix}/control.zip")},
        "Environment": {"Variables": {"BATCH_TABLE": ref("BatchLedgerV2"), "DATA_BUCKET": ref("DataBucket"), "OUTPUT_PREFIX": "thread/output/v2",
            "GLUE_JOB": ref("GlueJob"), "WORKFLOW_ARN": ref("Workflow"), "TOPIC_ARN": ref("AlertsTopic")}}}}
    r["AlertLogs"] = {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": sub("/aws/lambda/${AWS::StackName}-alerts"), "RetentionInDays": 7}}
    r["AlertInvokeConfig"] = {"Type": "AWS::Lambda::EventInvokeConfig", "Properties": {"FunctionName": ref("AlertFunction"), "Qualifier": "$LATEST", "MaximumRetryAttempts": 2,
        "DestinationConfig": {"OnFailure": {"Destination": ref("AlertsTopic")}}}}
    patterns = {"GlueFailureRule": {"source": ["aws.glue"], "detail-type": ["Glue Job State Change"], "detail": {"jobName": [ref("GlueJob")], "state": ["FAILED", "TIMEOUT", "STOPPED"]}},
        "WorkflowFailureRule": {"source": ["aws.states"], "detail-type": ["Step Functions Execution Status Change"], "detail": {"stateMachineArn": [ref("Workflow")], "status": ["FAILED", "TIMED_OUT", "ABORTED"]}}}
    for name, pattern in patterns.items():
        r[name] = {"Type": "AWS::Events::Rule", "Properties": {"EventPattern": pattern, "Targets": [{"Id": "Notify", "Arn": arn("AlertFunction"), "RetryPolicy": {"MaximumRetryAttempts": 185, "MaximumEventAgeInSeconds": 86400}}]}}
        r[name + "Permission"] = {"Type": "AWS::Lambda::Permission", "Properties": {"Action": "lambda:InvokeFunction", "FunctionName": ref("AlertFunction"), "Principal": "events.amazonaws.com", "SourceArn": arn(name), "SourceAccount": ref("AWS::AccountId")}}

    def alarm(name, namespace, metric, dimensions, threshold=1, actions=False, statistic="Sum"):
        properties = dict(AlarmName=sub("${AWS::StackName}-" + name), Namespace=namespace, MetricName=metric, Dimensions=dimensions,
            Statistic=statistic, Period=300, EvaluationPeriods=1, Threshold=threshold, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching")
        if actions:
            properties["AlarmActions"] = [ref("AlertsTopic")]
        r[name] = {"Type": "AWS::CloudWatch::Alarm", "Properties": properties}
    state_dims = [{"Name": "StateMachineArn", "Value": ref("Workflow")}]
    alarm("FailuresAlarm", "AWS/States", "ExecutionsFailed", state_dims)
    alarm("TimeoutsAlarm", "AWS/States", "ExecutionsTimedOut", state_dims)
    alarm("QualityAlarm", "Thread/Pipeline", "QualityFailures", [{"Name": "Pipeline", "Value": ref("AWS::StackName")}], statistic="Maximum")
    alarm("DlqAlarm", "AWS/SQS", "ApproximateNumberOfMessagesVisible", [{"Name": "QueueName", "Value": {"Fn::GetAtt": ["DeadLetterQueue", "QueueName"]}}], actions=True, statistic="Maximum")
    alarm("BacklogAgeAlarm", "AWS/SQS", "ApproximateAgeOfOldestMessage", [{"Name": "QueueName", "Value": {"Fn::GetAtt": ["InputQueue", "QueueName"]}}], threshold=3600, actions=True, statistic="Maximum")
    alarm("AlertDeliveryAlarm", "AWS/Lambda", "Errors", [{"Name": "FunctionName", "Value": ref("AlertFunction")}], actions=True)
    dashboard = {"widgets": [{"type": "text", "x": 0, "y": 0, "width": 24, "height": 2, "properties": {"markdown": "# THREAD operations\nSchedule defaults to DISABLED. Empty metrics do not prove success. Inspect execution status, quality audit and published snapshot IDs."}},
        {"type": "metric", "x": 0, "y": 2, "width": 12, "height": 6, "properties": {"region": "${AWS::Region}", "title": "Workflow status", "stat": "Sum", "period": 300,
            "metrics": [["AWS/States", metric, "StateMachineArn", "${Workflow}"] for metric in ("ExecutionsSucceeded", "ExecutionsFailed", "ExecutionsTimedOut")]}},
        {"type": "metric", "x": 12, "y": 2, "width": 12, "height": 6, "properties": {"region": "${AWS::Region}", "title": "Data quality", "stat": "Maximum", "period": 300,
            "metrics": [["Thread/Pipeline", metric, "Pipeline", "${AWS::StackName}"] for metric in ("RecordsRead", "RecordsQuarantined", "QualityFailures", "PendingReferences")]}},
        {"type": "metric", "x": 0, "y": 8, "width": 12, "height": 6, "properties": {"region": "${AWS::Region}", "title": "Glue processing duration", "stat": "Maximum", "period": 300,
            "metrics": [["Thread/Pipeline", "RunDurationSeconds", "Pipeline", "${AWS::StackName}"]]}},
        {"type": "metric", "x": 12, "y": 8, "width": 12, "height": 6, "properties": {"region": "${AWS::Region}", "title": "Intake DLQ", "stat": "Maximum", "period": 300,
            "metrics": [["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", "${DeadLetterQueue.QueueName}"]]}}]}
    r["OperationsDashboard"] = {"Type": "AWS::CloudWatch::Dashboard", "Properties": {"DashboardName": sub("${AWS::StackName}-operations"), "DashboardBody": sub(json.dumps(dashboard))}}
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "THREAD v2 Spark ETL, Iceberg publication, queried ledger and failure emails", "Parameters": {
        "ArtifactBucket": {"Type": "String", "MinLength": 3}, "ArtifactPrefix": {"Type": "String", "Default": "thread-runtime/v2"},
        "DatabaseName": {"Type": "String", "Default": "thread_pipeline_v2", "AllowedValues": ["thread_pipeline_v2"]},
        "ScheduleState": {"Type": "String", "Default": "DISABLED", "AllowedValues": ["DISABLED", "ENABLED"]},
        "SparkWorkers": {"Type": "Number", "Default": 2, "MinValue": 2, "MaxValue": 10},
        "MaxBatches": {"Type": "String", "Default": "500", "AllowedPattern": "([1-9]|[1-9][0-9]|[1-4][0-9]{2}|500)"},
        "NotificationEmail": {"Type": "String", "Default": "", "AllowedPattern": "^$|^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"}},
        "Conditions": {"HasEmail": {"Fn::Not": [{"Fn::Equals": [ref("NotificationEmail"), ""]}]}}, "Resources": r,
        "Outputs": {"DataBucket": {"Value": ref("DataBucket")}, "BatchTable": {"Value": ref("BatchLedgerV2")}, "WorkflowArn": {"Value": ref("Workflow")},
            "GlueJobName": {"Value": ref("GlueJob")}, "CatalogDatabase": {"Value": ref("DatabaseName")}, "AlertsTopicArn": {"Value": ref("AlertsTopic")},
            "DashboardName": {"Value": ref("OperationsDashboard")}, "InputPrefix": {"Value": "thread/input/v2"}, "OutputPrefix": {"Value": "thread/output/v2"}}}
