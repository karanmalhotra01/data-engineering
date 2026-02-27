import dlt
from dlt.sources.sql_database import sql_database
from dlt.destinations.adapters import athena_partition, athena_adapter
import yaml
import os
import sys
from slack_notifier import send_slack_webhook
import boto3
import json

import urllib.parse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

def get_secret(secret_name, region_name='ap-south-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)    
    # Secrets Manager returns a string, need to parse JSON
    secret = json.loads(response['SecretString'])
    return secret

def get_connection_url(secret, database):
    """
    Construct PostgreSQL SQLAlchemy URL from secret
    """
    user = urllib.parse.quote_plus(str(secret.get('username', '')))
    password = urllib.parse.quote_plus(str(secret.get('password', '')))
    host = secret.get('host')
    port = secret.get('port')    
    # Add connection parameters for stability
    # Increased timeouts and keepalives for long-running extractions
    params = "connect_timeout=60&keepalives=1&keepalives_idle=60&keepalives_interval=10&keepalives_count=10"
    if port:
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?{params}"
    return f"postgresql+psycopg2://{user}:{password}@{host}/{database}?{params}"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def run_with_retry(pipeline, source, write_disposition):
    """
    Run the DLT pipeline with exponential backoff retries
    """
    return pipeline.run(source, write_disposition=write_disposition)

def run_pipeline():
    config_file = "config_postgres.yml"
    if not os.path.exists(config_file):
        print(f"❌ Config file {config_file} not found")
        return

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    send_slack_webhook("🚀 *Postgres to Athena DLT Job Started*")

    for job in config.get('jobs', []):
        server_name = job.get('server_name')
        database = job.get('database')
        table_name = job.get('table_name')
        schema_name = job.get('schema_name', 'public')
        load_strategy = job.get('load_strategy', 'full')
        incremental_field = job.get('incremental_field')
    

        print(f"Processing {database}.{schema_name}.{table_name}...")

        try:
            # Fetch secret from AWS Secrets Manager
            secret = get_secret(server_name)
            if not secret:
                raise ValueError(f"Secret {server_name} not found")
            
            # Construct connection URL
            conn_url = get_connection_url(secret, database)
            
            # Test connection before running pipeline
            from sqlalchemy import create_engine
            print(f"Testing connection to {secret.get('host')}...")
            engine = create_engine(conn_url)
            with engine.connect() as conn:
                print("✅ Connection test successful")
            engine.dispose()

            # Define pipeline
            # Dataset name matches Athena schema/database
            dataset_name = f"dlt_{database}" 
            pipeline_name = f"postgres_{database}_{table_name}"
            
            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='athena',
                dataset_name=dataset_name,
            )

            # Source configuration
            source = sql_database(
                credentials=conn_url,
                schema=schema_name
            ).with_resources(table_name)

            if load_strategy == "incr" and incremental_field:
                # Apply incremental hints
                source.resources[table_name].apply_hints(
                    incremental=dlt.sources.incremental(incremental_field)
                )
                # Wrap with athena_adapter for partitioning
                source = athena_adapter(
                    source, 
                    partition=[athena_partition.day(incremental_field)]
                )
                write_disposition = "merge"
            else:
                write_disposition = "replace"

            # Run pipeline with retry mechanism
            info = run_with_retry(pipeline, source, write_disposition)
            print(f"✅ Successfully processed {table_name}")
            print(info)

        except Exception as e:
            error_msg = f"❌ Failed Postgres DLT job: {database}.{table_name} - {str(e)}"
            print(error_msg)
            send_slack_webhook(error_msg)

    send_slack_webhook("✅ *Postgres to Athena DLT Job Completed*")

if __name__ == "__main__":
    run_pipeline()
