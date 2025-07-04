**BLS Data Sync to S3**

**S3 Data Links** : 
- [pr.series](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.series)
- [pr.txt](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.txt)
- [pr.measure](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.measure)
- [pr.period](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.period)
- [pr.seasonal](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.seasonal)
- [pr.sector](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.sector)
- [pr.duration](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.duration)
- [pr.footnote](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.footnote)
- [pr.contacts](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.contacts)
- [pr.data.0.Current](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.data.0.Current)
- [pr.data.1.AllData](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.data.1.AllData)
- [pr.class](https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/pr/pr.class)
  
**Source Code** : [bls_pr_sync.py](/part1-bls-sync/bls_pr_sync.py)

Public datasets are fetched from BLS URL and published to `rearc-data-quest-ssm` S3 bucket. The sync script ensures:
- Files are dynamically discovered
- Duplicates are avoided
- Files are streamed directly to S3 without local storage
- Setup does not throw 403 error and is compliant with BLS data access policies

**S3 Bucket Configurations**
- Chose General Purpose Bucket
- Added a RearcDataQuest project tag
- Enabled versioning for traceability and rollbacks
- Used SSE-S3 for encryption (since its safe for public data). Will switch to SSE-KMS for sensitive data.

**Future Optimization Ideas**
- I would add recursive search for nested directores inside the `pr/` directory. Currently the script parses the flat directory at `/pub/time.series/pr/` but I would implement recursive traversal for potential subfolders within `pr/` path
- I would use retry strategy for request Session. This would enable automatic retries on time-out issues or other common network errors.
- I would implement and store **log.csv** file in S3 to append metadata about uploads and structured events (like hash mismatch, upload success/failure, skipped files etc). This would improve auditing
- I would also add more file validation techniques. Currently the script only compares hash of files in source and datalake but I'd expand it to include `file size comparison`, `last modified timestamps` and possibly a `checksum` strategy for byte comparison.
- I would include a staging area like a `\tmp` folder to preprocess files or enable batch processing (If the files need to be zipped before upload or in other scenarios). But as of now, the script directly streams data into S3 since it is ideal for lightweight public datasets.

**Upload Result**

![BLS_data_bucket](/resources/bls_data_bucket.png)
  
