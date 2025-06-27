import os
import json
import requests
import boto3

BUCKET_NAME = os.getenv("BUCKET_NAME")
POPULATION_PREFIX = os.getenv("POPULATION_PREFIX")
API_URL = os.getenv("API_URL")
JSON_FILE_NAME = os.getenv("JSON_FILE_NAME")

s3_client = boto3.client("s3")

def fetch_api_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def save_json_to_s3(data):
    s3_key = f"{POPULATION_PREFIX}{JSON_FILE_NAME}"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
