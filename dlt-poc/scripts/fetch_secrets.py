import boto3
import os
import sys

def download_s3_file(bucket, key, local_path):
    print(f"Downloading s3://{bucket}/{key} to {local_path}")
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, key, local_path)
        print("Download successful")
    except Exception as e:
        print(f"Error downloading file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure these or pass as args
    BUCKET = "<your-db-drivers-bucket>"
    SECRETS_KEY = "dlt/secrets.toml"
    LOCAL_SECRETS_PATH = "/home/airflow/.dlt/secrets.toml"

    # Ensure directory exists
    os.makedirs(os.path.dirname(LOCAL_SECRETS_PATH), exist_ok=True)

    download_s3_file(BUCKET, SECRETS_KEY, LOCAL_SECRETS_PATH)