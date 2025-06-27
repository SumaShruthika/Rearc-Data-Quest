**Source Code** : [api_to_s3.py](https://github.com/sumashruthika/rearc-data-quest/blob/main/part2-json-api/api_to_s3.py)  
**S3 Bucket Link** : https://rearc-data-quest-ssm.s3.us-east-2.amazonaws.com/usa-api-sync/population/

Population data is fetched from the given API endpoint and is stored as `nation_population.json` in `usa-api-sync/population/` bucket in S3

**Future Optimization Ideas**
- Since Part 1 and Part 2 have the same functionality, I would implement the enhancements listed in `part 1` like retry for request Session, have a metadata logging file and possibly include a staging area
