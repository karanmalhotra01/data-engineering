import boto3
from datetime import datetime, timedelta
import yaml




def copy_s3_files():
    # Configuration
    server_name = "xts_poc"
    database = "xts_db_poc"
    table_name = "tbl_order_details_poc"
    
    # Calculate date paths
    for i in range(0,560):
        trans_date = datetime.today() + timedelta(days=-i)
        year = trans_date.strftime('%Y')
        month = trans_date.strftime('%m')
        day = trans_date.strftime('%d')
    
        # Build paths (removing 's3a://' as boto3 needs bucket/path format)
        source_bucket = "poc-data-source"
        source_prefix = f"{server_name}/{database}/{table_name}/{year}/{month}/{day}/"
    
        dest_bucket = "poc-data-source"
        dest_prefix = f"{server_name}/{database}/{table_name}/{year}-{month}-{day}/"
    
        # Initialize S3 client
        s3 = boto3.client('s3')
    
        try:
            # List all objects in source
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)
        
            # Copy each file
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if not obj['Key'].endswith('/'):  # Skip directories
                            source_key = obj['Key']
                            # Build destination key by replacing the date format
                            dest_key = source_key.replace(
                                f"{year}/{month}/{day}/",
                                f"{year}-{month}-{day}/"
                            )
                        
                            # Perform copy
                            s3.copy_object(
                                CopySource={'Bucket': source_bucket, 'Key': source_key},
                                Bucket=dest_bucket,
                                Key=dest_key
                            )
                            print(f"Copied: {source_key} to {dest_key}")
        
            print("Copy operation completed successfully!")
    
        except Exception as e:
            print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    copy_s3_files()