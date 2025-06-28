import os
import requests
import boto3
import hashlib
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Load environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")
BLS_URL = os.getenv("BLS_URL")
BLS_PREFIX = os.getenv("BLS_PREFIX")
HEADERS = {
    'User-Agent': 'RearcDataQuest/ (contact: sumashruthikamaheshkumar@gmail.com)'
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    # 'Accept-Language': 'en-US,en;q=0.5'
}

s3_client = boto3.client("s3")

def fetch_bls_file_list():
    response = requests.get(BLS_URL, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    return [
        a["href"].split("/")[-1]
        for a in soup.find_all("a", href=True)
        if a["href"].startswith("/pub/time.series/pr/pr.")
    ]

def calculate_md5(content):
    return hashlib.md5(content).hexdigest()

def get_s3_file_list():
    paginator = s3_client.get_paginator("list_objects_v2")
    existing_files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=BLS_PREFIX):
        for obj in page.get("Contents", []):
            file_key = obj['Key'].split("/")[-1]
            if file_key:
                existing_files.append(file_key)
    return existing_files

def upload_or_update_file(bls_file, s3_existing_files):
    file_url = urljoin(BLS_URL, bls_file)
    response = requests.get(file_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to download {bls_file}")
        return

    bls_content = response.content
    bls_md5 = calculate_md5(bls_content)
    s3_key = f"{BLS_PREFIX}{bls_file}"

    if bls_file in s3_existing_files:
        try:
            s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            s3_md5 = calculate_md5(s3_obj["Body"].read())

            if bls_md5 != s3_md5:
                s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=bls_content)
        except Exception as e:
            print(f"Error reading {bls_file} from S3: {e}")
            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=bls_content)
    else:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=bls_content)

def delete_removed_files(bls_files, s3_existing_files):
    to_delete = [f for f in s3_existing_files if f not in bls_files]
    for f in to_delete:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=f"{BLS_PREFIX}{f}")

def sync_bls_data():
    files = fetch_bls_file_list()
    existing_files = get_s3_file_list()
    for file in files:
        upload_or_update_file(file, existing_files)
    delete_removed_files(files, existing_files)
