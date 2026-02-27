import os
import sys
import tarfile
import urllib.request
import boto3

def download_file(url, local_path):
    print(f"Downloading {url} to {local_path}...")
    try:
        urllib.request.urlretrieve(url, local_path)
        print("Download successful.")
    except Exception as e:
        print(f"Error downloading from public URL: {e}")
        return False
    return True

def download_from_s3(bucket, key, local_path):
    print(f"Attempting to download s3://{bucket}/{key} to {local_path}...")
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, key, local_path)
        print("Download from S3 successful.")
        return True
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return False

def extract_tar_gz(tar_path, extract_path):
    print(f"Extracting {tar_path} to {extract_path}...")
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=extract_path)
        print("Extraction successful.")
    except Exception as e:
        print(f"Error extracting tarball: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Resolve repo root
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    REPO_ROOT = os.path.dirname(SCRIPT_DIR)
    
    JRE_DIR = os.path.join(REPO_ROOT, "jre")
    TAR_PATH = os.path.join(REPO_ROOT, "openjdk17.tar.gz")
    
    # Public URL for Eclipse Temurin OpenJDK 17 Linux x64
    PUBLIC_URL = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.8.1%2B1/OpenJDK17U-jre_x64_linux_hotspot_17.0.8.1_1.tar.gz"
    
    # S3 Fallback
    S3_BUCKET = "poc-db-drivers"
    S3_KEY = "jre/openjdk17.tar.gz"

    if os.path.exists(os.path.join(JRE_DIR, "bin", "java")):
        print("Portable JRE already exists. Skipping.")
        sys.exit(0)

    # Try Public URL first, then S3
    if not download_file(PUBLIC_URL, TAR_PATH):
        print("Public download failed, trying S3...")
        if not download_from_s3(S3_BUCKET, S3_KEY, TAR_PATH):
            print("CRITICAL: Failed to download JRE from both public URL and S3.")
            print(f"Please upload a Linux x64 JRE tarball to s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)

    # Extract
    temp_extract_dir = os.path.join(REPO_ROOT, "jre_temp")
    os.makedirs(temp_extract_dir, exist_ok=True)
    extract_tar_gz(TAR_PATH, temp_extract_dir)

    # Most JDK tarballs extract into a subfolder like 'jdk-17.0.8.1+1-jre'
    subfolders = [f for f in os.listdir(temp_extract_dir) if os.path.isdir(os.path.join(temp_extract_dir, f))]
    if subfolders:
        actual_content_path = os.path.join(temp_extract_dir, subfolders[0])
        os.rename(actual_content_path, JRE_DIR)
        print(f"Moved JRE contents from {subfolders[0]} to {JRE_DIR}")
    else:
        os.rename(temp_extract_dir, JRE_DIR)

    # Cleanup
    if os.path.exists(TAR_PATH):
        os.remove(TAR_PATH)
    if os.path.exists(temp_extract_dir):
        import shutil
        shutil.rmtree(temp_extract_dir)

    print("Portable JRE setup complete.")
