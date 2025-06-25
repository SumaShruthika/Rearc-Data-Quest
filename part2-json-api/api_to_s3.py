# Import libraries
import json
import requests
import boto3
from config import API_URL, BUCKET_NAME, S3_PREFIX, JSON_FILE_NAME

# Initialize S3 client
s3_client = boto3.client("s3")

def fetch_api_data(api_url):
    """
    Fetch data from the API and return JSON.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def save_json_to_s3(data, bucket, key):
    """
    Save JSON data to S3 as a file.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"JSON file uploaded to s3://{bucket}/{key}")

def main():
    data = fetch_api_data(API_URL)
    s3_key = f"{S3_PREFIX}{JSON_FILE_NAME}"
    save_json_to_s3(data, BUCKET_NAME, s3_key)

if __name__ == "__main__":
    main()