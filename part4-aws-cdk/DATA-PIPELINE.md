# **Infrastructure as Code (IaC) and Automated Data Pipeline (CDK)**  
Built a serverless data pipeline using CDK that automates:
- Data ingestion from the BLS and Population API (Part 1 and 2)
- Daily sync schedule using EventBridge
- Event-driven data processing using SQS and Lambda (Part 3)

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

![Part4_pipeline](https://github.com/SumaShruthika/Rearc-Data-Quest/blob/b43c701d9c01ecb9e5f2aa1dc4f24afb368f5b49/resources/Part4_pipeline.png)

**Future Optimizations**  
- **Data Architecture**: I would implement `Bronze/Silver/Gold` data layers across separate S3 buckets for improved data quality and lineage tracking
- **Error Handling & Monitoring**: I would add `DLQ` for failed messages and `SNS` notifications for real-time pipeline failure alerts  
- **Reporting**: I would integrate `Amazon QuickSight` for interactive dashboards and user-friendly reporting
- **Security and Network Isolation**: I would deploy infrastructure in private `VPC` subnets for improved security and compliance

 ![Enhanced_Pipeline](https://github.com/SumaShruthika/Rearc-Data-Quest/blob/458099ae1b596db925198fa5ac68ae17899294c9/resources/Enhanced_Part4_Pipeline.png) 

**Outputs & Proof of Execution**  
I have included a pipeline architecture diagram based on the resources deployed by the CDK infrastructure. For verification of specific resource configurations, all CDK outputs are available in the [resources](https://github.com/SumaShruthika/Rearc-Data-Quest/tree/9a0c6ae8dfb829088422105b4f1801195804d9cc/resources) folder with the `Part4` prefix.

https://github.com/SumaShruthika/Rearc-Data-Quest/blob/485539c33058b6d534df0f83da58da59ace74101/resources/Output_Proof.jpeg

