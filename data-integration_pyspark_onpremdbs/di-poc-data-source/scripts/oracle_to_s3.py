from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import current_date, date_format

import boto3
import json
import yaml
import os
import pytz


jar_path = "jars/"
oracle_db_jar = "ojdbc8.jar" 
hadoop_aws_jar = "hadoop-aws-3.3.2.jar"
aws_java_jar = "aws-java-sdk-bundle-1.12.262.jar"
oracle_driver = os.path.join(os.environ['HOME'], jar_path, oracle_db_jar)
hadoop_aws_driver = os.path.join(os.environ['HOME'], jar_path,hadoop_aws_jar)
aws_java_driver = os.path.join(os.environ['HOME'], jar_path,aws_java_jar)



# Create comma-separated list of all JAR files
all_jars = f"{oracle_driver},{hadoop_aws_driver},{aws_java_driver}"

# Configure SparkSession with variables
spark = SparkSession.builder \
    .appName("ORACLE TO S3") \
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
relative_path = "configs/config_oracle.yml"
yaml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", relative_path)

with open(yaml_path, 'r') as file:
    config = yaml.safe_load(file)


bucket_name = "poc-data-source"


# Loop through each job
for job in config['jobs']:
    secret = get_secret(job['server_name'])
    user_name = secret["username"]
    password = secret["password"]
    db_server = secret["db_server"]
    port = secret["port"]
    service_name = secret["service_name"]
    server_name = secret["server_name"]

    
    
    # JDBC Properties
    properties = {
        "user": user_name,
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
        "oracle.jdbc.timezoneAsRegion": "false",
        "oracle.jdbc.defaultTimeZone": "IST",
        "fetchsize": "1000"
    }


    schema = job['schema']
    database = job['database']
    table_name = job['table_name']
    incremental_date = job['incremental_date']
    n = job['n']
    
    jdbc_url = f"jdbc:oracle:thin:@{db_server}:{port}/{service_name}"

    output_path = "s3a://poc-data-source/"+server_name+"/"+schema+"/"+table_name+"/"



    for i in range(0,n):  #change this for historical load

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist)
        trans_date = datetime.today() + timedelta(days=-i)
        year = trans_date.strftime('%Y')
        month = trans_date.strftime('%m')
        day = trans_date.strftime('%d')
        
        formatted_date = trans_date.strftime('%Y-%m-%d')


        query = f"(SELECT * FROM {schema}.{table_name} WHERE {incremental_date} = to_date('{formatted_date}','YYYY-MM-DD'))"
        try:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=properties
            )


            df.repartition(10).write \
                .mode("overwrite") \
                .option("header", "true") \
                .parquet(f"{output_path}/{year}-{month}-{day}/")
        
            print(f"[✔] Wrote data for {database}.{table_name} - {formatted_date}")
        
        except Exception as e:
            print(f"[⚠] Skipping {database}.{table_name} for {formatted_date}: {e}")
        
# Step 2: Start the Crawler only for incremental run
    glue = boto3.client('glue', region_name='ap-south-1')
    glue.start_crawler(Name=server_name+"_"+database+"_"+table_name)