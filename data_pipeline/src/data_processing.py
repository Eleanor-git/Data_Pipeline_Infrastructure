"""
TUTORIAL: Data Processing Pipeline
Download data from S3, clean it, transform it, and upload results back to S3.
"""

import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import tempfile


def process_ecommerce_data():
    """Download, process, and upload e-commerce data"""

    print("Starting data processing...")

    # Load environment variables
    load_dotenv()
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

    if not bucket_name:
        print("ERROR: AWS_S3_BUCKET_NAME not found in .env file!")
        return False

    print(f"Processing data from bucket: {bucket_name}")

    try:
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)

        # Step 1: Download data from S3
        print("\nStep 1: Downloading data from S3...")
        datasets = download_data_from_s3(s3_client, bucket_name)

        # Step 2: Clean and transform data
        print("\nStep 2: Cleaning and transforming data...")
        processed_datasets = transform_data(datasets)

        # Step 3: Create business metrics
        print("\nStep 3: Creating business metrics...")
        business_metrics = create_business_metrics(processed_datasets)

        # Step 4: Upload processed data back to S3
        print("\nStep 4: Uploading processed data to S3...")
        upload_success = upload_processed_data(s3_client, bucket_name, processed_datasets, business_metrics)

        if upload_success:
            print("\nSUCCESS: Data processing pipeline completed!")
            return True
        else:
            print("\nERROR: Failed to upload processed data")
            return False

    except Exception as e:
        print(f"ERROR: Data processing failed: {e}")
        return False


def download_data_from_s3(s3_client, bucket_name):
    dataset_dict = {}
    data_files = os.listdir("../../data")

    for f_name in data_files:
        try:
            print(f"Downloading {f_name}...")
            local_path = os.path.join("../../Download", f_name)

            s3_client.download_file(bucket_name, f"data/{f_name}", local_path)
            df = pd.read_csv(local_path)
            dict_key = f_name.replace(".csv", "")
            dataset_dict[dict_key] = df

            print(f"[SUCCESS] {dict_key} has been loaded with records: {len(df)}\n")
            os.remove(local_path)

        except Exception as e:
            print(f"Failed to download {f_name} with: {e}\n")
    return dataset_dict



if __name__ == "__main__":
    # TODO: Call process_ecommerce_data() and handle the results
    pass
