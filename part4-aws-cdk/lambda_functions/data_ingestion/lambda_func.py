from bls_utils import sync_bls_data
from api_utils import fetch_api_data, save_json_to_s3

def lambda_handler(event, context):
    print("Syncing BLS Data")
    sync_bls_data()

    print("Syncing Population API Data")
    data = fetch_api_data()
    save_json_to_s3(data)

    print("Data Ingestion Complete")
