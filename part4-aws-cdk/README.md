# **Infrastructure as Code (IaC) and Automated Data Pipeline (CDK)** 
CDK IaC Source Code : [CDK Stack](./part4_aws_cdk/part4_aws_cdk_stack.py)    

( All resources for this pipeline can be identified using tags: `Project: RearcDataQuest` and `Environment: dev`)

**AWS Resources Created**
This CDK deployment creates the following AWS resources:
- **S3 Bucket:** `lambda-pipeline-data-bucket` - Stores BLS data files and population JSON data
- **Lambda Functions:**
  `data-ingestion-lambda` - Downloads and processes data from external APIs daily
  `data-analysis-lambda` - Processes data when triggered by S3 events
- **SQS Queue:** `data-processing-queue` - Queues messages when new data is uploaded to S3
- **EventBridge Rule:** `daily-data-ingestion-trigger` - Triggers the ingestion Lambda daily
- **IAM Roles:** Auto-generated roles with appropriate permissions for Lambda execution  


Built a serverless data pipeline using CDK that automates:
- Data ingestion from the BLS and Population API (Part 1 and 2)
  - Ingestion Lambda function Source Code : [Ingestion Lambda Function](./lambda_functions/data_ingestion/lambda_func.py)
- Daily sync schedule using EventBridge
- Event-driven data processing using SQS and Lambda (Part 3)
  - Analysis Lambda function Source Code : [Analysis Lambda Function](./lambda_functions/data_analysis/lambda_func.py)

**Pipeline Architecture**  
| Resource | Purpose |
|----------|---------|
| S3 Bucket | Stores both raw BLS and population datasets |
| Lambda (Ingestion) | Fetches BLS + API data daily and uploads to S3 |
| EventBridge Rule | Triggers ingestion Lambda daily |
| SQS Queue | Gets triggered when new population JSON is uploaded to S3 |
| Lambda (Analytics) | Processes messages from SQS, reads both datasets, and logs analysis |

**Pipeline Flow (Implemented)**  
- **Daily Data Ingestion**: EventBridge triggers a Lambda function daily to fetch BLS data and population data from APIs, storing both datasets in S3 under separate prefixes.
- **Event-Driven Processing**: When new JSON files are uploaded to S3, an event notification triggers an SQS queue, which then invokes an analytics Lambda function.
- **Analytics & Reporting**: The analytics Lambda reads both datasets, computes population statistics (mean/std dev for 2013-2018), identifies the best performing year by series ID, creates a joined report for series `PRS30006032` with population data, and logs all results.

![Part4_pipeline](./resources/Part4_pipeline.png)

**Future Optimizations**  
- **Data Architecture**: I would implement `Bronze/Silver/Gold` data layers across separate S3 buckets for improved data quality and lineage tracking
- **Error Handling & Monitoring**: I would add `DLQ` for failed messages and `SNS` notifications for real-time pipeline failure alerts  
- **Reporting**: I would integrate `Amazon QuickSight` for interactive dashboards and user-friendly reporting
- **Security and Network Isolation**: I would deploy infrastructure in private `VPC` subnets for improved security and compliance

 ![Enhanced_Pipeline](./resources/Enhanced_Part4_Pipeline.png) 

**Outputs & Proof of Execution**  
I have included a pipeline architecture diagram based on the resources deployed by the CDK infrastructure. For verification of specific resource configurations, all CDK output images are available in the [resources](./resources) folder with the `Part4` prefix.

![Output_Proof](./resources/Output_Proof.jpeg)

