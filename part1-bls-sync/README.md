**BLS Data Sync to S3**

Public datasets are fetched from BLS URL and published to `rearc-data-quest-ssm` S3 bucket. The sync script ensures:
- Files are dynamically discovered
- Duplicates are avoided
- Files are streamed directly to S3 without local storage
- Setup does not throw 403 error and is compliant with BLS data access policies

**S3 Data Link** : https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/bls/
**Source Code** : 
