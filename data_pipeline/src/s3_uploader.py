import os
import boto3
from pathlib import Path
from dotenv import load_dotenv


def upload_data_to_s3():
    """Upload data to S3 bucket"""

    print("Starting data upload to S3...")

    # Load environment variables
    load_dotenv()

    # Get AWS credentials from .env file
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')  # os.getenv(key, default)

    # input validation for prefect
    if not bucket_name:  # if not None = True, then the block below will be run
        print("ERROR: AWS_S3_BUCKET_NAME not found in .env file!")
        return False

    print(f"Bucket: {bucket_name}")
    print(f"Region: {region}")

    try:
        # Instantiate S3 client
        s3 = boto3.client('s3', region_name=region)

        # Create a bucket if it doesn't exist
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' exists")
        except:
            print(f"Creating bucket '{bucket_name}'...")
            if region == 'us-east-1':  # "us-east-1" is the original region, and it doesn't need a location constraint while every other region requires specifying the constraints
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"Bucket: {bucket_name} created!")

        # Find CSV files in data
        data_dir = Path("../../data")

        if not data_dir.exists():
            print("Error: Data directory does not exist!")
            print("Check your data directory with csv files.")
            return False

        # Get list of CSV files
        csv_files = list(data_dir.glob("*.csv"))
        print(f"There are {len(csv_files)} csv files in the data directory!")

        if not csv_files:
            print(f"Error: No CSV files found in {data_dir}")
            return False

        # Upload each file
        uploaded_count = 0
        for file in csv_files:
            s3_key = f"data/{file.name}"  # data structure
            print(f"Uploading {file.name}...")

            try:
                s3.upload_file(str(file), bucket_name, s3_key)  # param: Filename(str), Bucket(str), Key(str)
                print(f"SUCCESS Uploaded to s3://{bucket_name}/{s3_key} ")
                uploaded_count += 1
            except Exception as upload_error:
                print(f"ERROR: Failed to upload {file.name}")

        if uploaded_count == len(csv_files):
            print(f"\nFULL SUCCESS: ALL {len(csv_files)} files uploaded to your data lake.")
            return True
        else:
            print(f"\nPartial Success: {uploaded_count}/{len(csv_files)} files uploaded.")
            return False

    except Exception as e:
        print("s3 client objection instantiation failed!")
        print(f"ERROR: {e}")
        return False


def verify_upload():
    '''A function to verify if upload is successful'''

    print("\nVerifying upload...")
    load_dotenv()
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    region = os.getenv("AWS_DEFAULT_REGION")

    try:
        s3 = boto3.client('s3', region_name=region)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="data/")

        if "Contents" not in response:
            print("ERROR: No files found in the bucket.")
            return False

        uploaded_files = response["Contents"]
        print(f"Found {len(uploaded_files)} files in the s3 bucket.")

        total_size_mb = 0
        for file in uploaded_files:
            size_mb = file["Size"] / (1024 * 1024)
            total_size_mb += size_mb
            print(f"file: {file["Key"]} ({size_mb:.2f} MB)")

        print("------------------------")
        print(f"Total data size: {total_size_mb:.2f} MB")
        print("Upload verification completed.")
        return True

    except Exception as e:
        print("Verification failed!")
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    # Run upload
    success = upload_data_to_s3()

    # Verify if upload was successful
    if success:
        verify_upload()

    print("\nNext step: Data processing and transformation!")