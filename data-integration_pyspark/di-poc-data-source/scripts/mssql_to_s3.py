from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date, date_format

import boto3
import json
import yaml
import os
import pytz

from slack_notifier import send_slack_webhook

# Resolve paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

# JAR paths - point to local jars/ directory within repo
JAR_BASE_PATH = os.path.join(REPO_ROOT, "jars")

mssql_db_jar = "mssql-jdbc-12.10.0.jre11.jar"
mysql_db_jar = "mysql-connector-j-9.3.0.jar"
hadoop_aws_jar = "hadoop-aws-3.3.2.jar"
aws_java_jar = "aws-java-sdk-bundle-1.12.262.jar"

mssql_driver = os.path.join(JAR_BASE_PATH, mssql_db_jar)
mysql_driver = os.path.join(JAR_BASE_PATH, mysql_db_jar)
hadoop_aws_driver = os.path.join(JAR_BASE_PATH, hadoop_aws_jar)
aws_java_driver = os.path.join(JAR_BASE_PATH, aws_java_jar)



# Create comma-separated list of all JAR files
all_jars = f"{mssql_driver},{mysql_driver},{hadoop_aws_driver},{aws_java_driver}"




# Configure SparkSession with variables
spark = SparkSession.builder \
    .appName("MSSQL TO S3") \
    .config("spark.jars", all_jars) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .getOrCreate()

# Load credentials from secret manager
def get_secret(secret_name, region_name='ap-south-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    
    # Secrets Manager returns a string, need to parse JSON
    secret = json.loads(response['SecretString'])
    return secret

# Read YAML file
yaml_path = os.path.join(os.path.dirname(SCRIPT_DIR), "configs", "config_sqlserver.yml")

with open(yaml_path, 'r') as file:
    config = yaml.safe_load(file)


bucket_name = "poc-data-source"

send_slack_webhook(f"✅ *Sql Server Job Started*")

# Loop through each job
for job in config['jobs']:
    secret = get_secret(job['server_name'])
    user_name = secret["username"]
    password = secret["password"]
    db_server = secret["db_server"]
    server_name = secret["server_name"]
    
    
    # JDBC Properties
    properties = {
        "user": user_name,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "encrypt": "true",
        "trustServerCertificate": "true"
    }


    database = job['database']
    table_name = job['table_name']
    incremental_date = job['incremental_date']
    n = job['n']
    
    jdbc_url = "jdbc:sqlserver://"+db_server+";databaseName="+database

    output_path = "s3a://poc-data-source/"+server_name+"/"+database+"/"+table_name+"/"



    for i in range(0,n):  #change this for historical load

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist)
        trans_date = datetime.today() + timedelta(days=-i)
        year = trans_date.strftime('%Y')
        month = trans_date.strftime('%m')
        day = trans_date.strftime('%d')
        
        formatted_date = trans_date.strftime('%Y-%m-%d')
        dt = datetime.strptime(formatted_date, "%Y-%m-%d")  # Convert to datetime object
        epoch_formatted_date = int(dt.timestamp())
        next_day = dt + timedelta(days=1)  # Add 1 day
        formatted_next_day = next_day.strftime("%Y-%m-%d")  # Convert back to string
        epoch_formatted_next_day = int(next_day.timestamp())
        

        if incremental_date == "nLogonLogoffTime":
            query = f"(SELECT * FROM dbo.{table_name} WHERE {incremental_date} >= '{epoch_formatted_date}' AND {incremental_date} < '{epoch_formatted_next_day}') AS tmp"
        else:
            query = f"(SELECT * FROM dbo.{table_name} WHERE CONVERT(DATE, {incremental_date}) >= '{formatted_date}' AND CONVERT(DATE, {incremental_date}) < '{formatted_next_day}') AS tmp"
        
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=properties
            )


            df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .parquet(f"{output_path}/{year}-{month}-{day}/")
        
            print(f"[✔] Wrote data for {database}.{table_name} - {formatted_date}")
        
        except Exception as e:
            error_msg = f"[❌] Failed job: {database}.{table_name} for {formatted_date}"
            print(error_msg)
            send_slack_webhook(error_msg)
        
# Step 2: Start the Crawler only for incremental run
    try:
        glue = boto3.client('glue', region_name='ap-south-1')
        crawler_name = f"{server_name}_{database}_{table_name}"
        glue.start_crawler(Name=crawler_name)
    except Exception as e:
        error_msg = f"❌ Glue crawler *{crawler_name}* failed"
        send_slack_webhook(error_msg)

send_slack_webhook(f"✅ *Sql Server Job Completed*")