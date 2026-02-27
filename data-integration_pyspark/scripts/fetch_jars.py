import boto3
import os
import sys

def download_s3_file(bucket, key, local_path):
    print(f"Downloading s3://{bucket}/{key} to {local_path}")
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, key, local_path)
        print(f"Successfully downloaded {os.path.basename(local_path)}")
    except Exception as e:
        print(f"Error downloading file {key}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Resolve repo root
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    REPO_ROOT = os.path.dirname(SCRIPT_DIR)
    
    BUCKET = "poc-db-drivers"
    JARS_PREFIX = "jars/"
    LOCAL_JARS_DIR = os.path.join(REPO_ROOT, "jars")

    # List of JARs needed
    REQUIRED_JARS = [
        "mssql-jdbc-12.10.0.jre11.jar",
        "mysql-connector-j-9.3.0.jar",
        "hadoop-aws-3.3.2.jar",
        "aws-java-sdk-bundle-1.12.262.jar",
        "postgresql-42.7.7.jar"
    ]

    # Ensure local jars directory exists
    os.makedirs(LOCAL_JARS_DIR, exist_ok=True)

    for jar_name in REQUIRED_JARS:
        s3_key = f"{JARS_PREFIX}{jar_name}"
        local_path = os.path.join(LOCAL_JARS_DIR, jar_name)
        
        # Check if already present to save time/bandwidth, though usually safe to overwrite
        if not os.path.exists(local_path):
            download_s3_file(BUCKET, s3_key, local_path)
        else:
            print(f"File {jar_name} already exists locally. Skipping download.")
