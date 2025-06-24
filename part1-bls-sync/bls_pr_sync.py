# Import libraries
import os
import sys
import requests
from bs4 import BeautifulSoup
import boto3
from urllib.parse import urljoin

# Import configurations
from config import BLS_URL, BUCKET_NAME, S3_PREFIX, HEADERS

# Initialize S3 client
s3_client = boto3.client('s3')

def fetch_bls_file_list(BLS_URL):
    """
    Recursively scan bls dir and fetch the list of files from the BLS URL.
    Returns a list of file names.
    """
    response = requests.get(BLS_URL, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    file_names = [
        a["href"].split("/")[-1]
        for a in soup.find_all("a", href=True)
        if a["href"].startswith("/pub/time.series/pr/pr.")
    ]

    return file_names

def get_s3_file_list():
    """
    List all existing files in S3 bucket under specified prefix
    Returns list of current files in S3 bucket
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    existing_files = []

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            # Extract only the file name, not the full key
            existing_files.append(obj['Key'].split("/")[-1])

    return existing_files

# utility function that handles file uploads to S3
def upload_files_to_s3(filename):
    """
    Uploads files from the BLS URL to the specified S3 bucket.
    Only uploads files that do not already exist in the S3 bucket.
    """

    file_url = urljoin(BLS_URL, filename)

    response = requests.get(file_url, headers=HEADERS)
    if response.status_code == 200:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{S3_PREFIX}{filename}",
            Body=response.content
        )
        print(f"Uploaded: {filename}")
    else:
        print(f"Failed to download {filename} (Status {response.status_code})")

def sync_pr_data():
    """
    Syncs BLS files to S3, skips files that already exist.
    """
    files = fetch_bls_file_list(BLS_URL)
    existing_files = get_s3_file_list()

    for file in files:
        if file not in existing_files:
            upload_files_to_s3(file)
        else:
            print(f"File {file} already exists in S3, skipping upload.")

if __name__ == "__main__":
    sync_pr_data()
    print("Sync Complete!!")
