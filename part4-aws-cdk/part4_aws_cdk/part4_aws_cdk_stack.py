from aws_cdk import (
    Stack,
    Duration,
    Tags,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as lambda_event_sources,
    BundlingOptions,
)
from constructs import Construct
import os


class Part4AwsCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Apply tags to all resources in this stack
        Tags.of(self).add("Project", "RearcDataQuest")
        Tags.of(self).add("Environment", "dev")
        
        # 1. Create S3 bucket
        data_bucket = s3.Bucket(self, "LambdaDataBucket",
        bucket_name="lambda-pipeline-data-bucket"
        )
        
        # 2. Define the ingestion Lambda function with dependencies bundled
        ingestion_lambda = _lambda.Function(
            self, "IngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_func.lambda_handler", 
            function_name="data-ingestion-lambda",
            code=_lambda.Code.from_asset(
                "lambda_functions/data_ingestion",
                bundling=BundlingOptions( 
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -r . /asset-output"
                    ],
                )
            ),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": data_bucket.bucket_name,
                "BLS_URL": "https://download.bls.gov/pub/time.series/pr/",
                "API_URL": "https://datausa.io/api/data?drilldowns=Nation&measures=Population",
                "BLS_PREFIX": "bls-data/",
                "POPULATION_PREFIX": "population-data/",
                "JSON_FILE_NAME": "population.json"
            }
        )
        
        # 3. Grant S3 permissions to Lambda
        data_bucket.grant_read_write(ingestion_lambda)
        
        # 4. Set up a daily trigger for the Lambda
        daily_schedule = events.Rule(
            self, "DailyLambdaSchedule",
            rule_name="daily-data-ingestion-trigger",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        daily_schedule.add_target(targets.LambdaFunction(ingestion_lambda))

        # 5. Create SQS queue
        processing_queue = sqs.Queue(
            self, "DataProcessingQueue",
            queue_name="data-processing-queue",
            visibility_timeout=Duration.minutes(5),
            tags={"Project": "RearcDataQuest", "Environment": "dev"}
            retention_period=Duration.days(14)
        )

        # 6. Add S3 event notification to push to SQS when the JSON is uploaded
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT,
            s3n.SqsDestination(processing_queue),
            s3.NotificationKeyFilter(
                prefix="population-data/",
                suffix="population.json"
            )
        )

        analytics_lambda = _lambda.Function(
            self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="lambda_func.lambda_handler",
            function_name="data-analysis-lambda",
            code=_lambda.Code.from_asset(
            "lambda_functions/data_analysis",
            bundling=BundlingOptions(
                image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                command=[
                    "bash", "-c",
                    "pip install --no-cache-dir --only-binary=:all: pandas numpy -t /asset-output && "
                    "cp lambda_func.py /asset-output"
                ],
                )
            ),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "BUCKET_NAME": data_bucket.bucket_name,
                "BLS_URL": "https://download.bls.gov/pub/time.series/pr/",
                "API_URL": "https://api.census.gov/data/2019/pep/population?get=POP,DATE_DESC&for=us:*",
                "BLS_PREFIX": "bls-data/",
                "POPULATION_PREFIX": "population-data/",
                "JSON_FILE_NAME": "population.json"
            }
        )

        # 8. Grant analytics Lambda permissions
        data_bucket.grant_read(analytics_lambda)
        processing_queue.grant_consume_messages(analytics_lambda)

        # 9. Connect SQS → Lambda
        analytics_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(processing_queue)
        )