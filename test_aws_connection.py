import os, boto3
from dotenv import load_dotenv

def test_aws_connection():
    load_dotenv() # reads variables from a .env file and sets them in os.environ
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION')
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')

    print("Testing AWS connection")
    print(f"region: {region}")
    print(f"Bucket Name: {bucket_name}")

    try:
        # Instantiate object "S3 Client"
        s3_client = boto3.client('s3',
                          region_name=region,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key
                          )
        buckets = s3_client.list_buckets()  # listing buckets
        print(f"Connected successfully.\n There are {len(buckets["Buckets"])} s3 buckets.")
        return True

    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_aws_connection()
