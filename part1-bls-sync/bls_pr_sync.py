# Import libraries
import os
import sys
import requests
import boto3
import hashlib
from urllib.parse import urljoin
from bs4 import BeautifulSoup


# Import configurations
from config import BLS_URL, BUCKET_NAME, S3_PREFIX, HEADERS

# Initialize S3 client
s3_client = boto3.client('s3')

def fetch_bls_file_list(BLS_URL):
    """
    scan bls dir and fetch the list of files from the BLS URL.
    Returns a list of file names.
    """
    try:
        response = requests.get(BLS_URL, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        file_names = [
            a["href"].split("/")[-1]
            for a in soup.find_all("a", href=True)
            if a["href"].startswith("/pub/time.series/pr/pr.") 
        ]
        return file_names   
    except requests.RequestException as e:
        print(f"Error fetching BLS file list: {e}")
        return None

def calculate_md5(content):
    """
    Returns the MD5 hash of given binary content.
    """
    return hashlib.md5(content).hexdigest()

def get_s3_file_list():
    """
    List all existing files in S3 bucket under specified prefix
    Returns list of current files in S3 bucket
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    existing_files = []

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            file_key = obj['Key'].split("/")[-1]
            if file_key:
                existing_files.append(file_key)

    return existing_files

# utility function that handles file uploads to S3
def upload_or_update_file(bls_file, s3_existing_files):
    """
    Performs upload operation for new or updated files from BLS to S3.
    If the file already exists in S3 and is up-to-date, it skips the upload
    If file exists but content differs, it updates the file in S3
    """

    file_url = urljoin(BLS_URL, bls_file)

    response = requests.get(file_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to download {bls_file} (Status {response.status_code})")
        return

    bls_content = response.content
    bls_md5 = calculate_md5(bls_content)

    if bls_file in s3_existing_files:
        print(f"{bls_file} already exists in S3, checking for updates...")
        try:
            s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}{bls_file}")
            s3_content = s3_obj["Body"].read()
            s3_md5 = calculate_md5(s3_content)

            # if file hashes don't match, then need to update the file in S3
            if bls_md5 != s3_md5:
                print(f"Updating {bls_file} in S3...")
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=f"{S3_PREFIX}{bls_file}",
                    Body=bls_content
                )
                print(f"Updated {bls_file} in S3.")
            else:
                print(f"Skipping {bls_file}, already up-to-date in S3.")

        except Exception as e:
            print(f"Error checking S3 file {bls_file}: {e}, uploading as fallback.")

            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=f"{S3_PREFIX}{bls_file}",
                Body=bls_content
            )
            print(f"Uploaded (fallback): {bls_file}")
    else:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{S3_PREFIX}{bls_file}",
            Body=bls_content
        )
        print(f"Uploaded {bls_file} to S3.")


def delete_removed_files(bls_files, s3_existing_files):
    """
    Deletes files from S3 that are no longer present in the BLS (source)
    """
    files_to_delete = [file for file in s3_existing_files if file not in bls_files]
    for file in files_to_delete:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}{file}")
        print(f"Deleted {file} from S3.")


def sync_pr_data():
    """
    Syncs BLS files to S3, whether they are new or updated or deleted.
    """
    files = fetch_bls_file_list(BLS_URL)
    print(f"BLS FILES LIST: {files}")
    existing_files = get_s3_file_list()
    print(f"S3 FILES LIST: {existing_files}")

    files_set = set(files)
    existing_files_set = set(existing_files)

    # Delete files in S3 that are no longer in the source
    files_to_delete = existing_files_set - files_set
    print(f"Files to delete from S3: {files_to_delete}")

    for file in files:
        upload_or_update_file(file, existing_files)

    if files_to_delete:
        print("Entering deletion phase...")
        for file in files_to_delete:
            print(f"File to delete : {file} is no longer in BLS, deleting from S3.")
            delete_removed_files(file, existing_files)


if __name__ == "__main__":
    sync_pr_data()
    print("Sync Complete!!")
