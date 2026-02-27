# This is a one time job
import boto3
import json
sc.addPyFile("s3://poc-code-packages/pyyaml.zip")
import yaml

def load_yaml_from_s3(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(obj['Body'].read())

# Set your S3 config file path
bucket_name = "poc-data-source"
config_key = "configs/config_sqlserver_full_load.yml"
config = load_yaml_from_s3(bucket_name, config_key)


for job in config['jobs']:
    server_name = job["server_name"]
    database = job['database']
    table_name = job['table_name']

    

    glue = boto3.client('glue', region_name='ap-south-1')

# Create the Crawler
    response = glue.create_crawler(
        Name=server_name+"_"+database+"_"+table_name,
        Role='glue-crawler-role',
        DatabaseName=server_name,
        TablePrefix=database+"_",
        Targets={
            'S3Targets': [
                {
                    'Path': "s3://poc-data-source/"+server_name+"/"+database+"/"+table_name+"/",
                },
            ]
        },
            SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
)

# Step 2: Start the Crawler only for incremental run
    # glue.start_crawler(Name=server_name+"_"+database+"_"+table_name)