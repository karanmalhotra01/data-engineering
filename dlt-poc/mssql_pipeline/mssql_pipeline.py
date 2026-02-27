import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.destinations.adapters import athena_partition, athena_adapter
from sqlalchemy import create_engine, text as sa_text
import yaml
import os
import sys
from slack_notifier import send_slack_webhook
import boto3
import json
import urllib.parse
from datetime import datetime, timedelta
import pendulum
from decimal import Decimal
import time

def execute_athena_query(query, database, region='ap-south-1'):
    """
    Executes a SQL query in Athena and waits for completion.
    Uses environment variables for configuration if available.
    """
    client = boto3.client('athena', region_name=region)
    # Get output location from environment if possible, otherwise use a placeholder
    # User should ensure this is set in their environment
    output_location = os.environ.get('DESTINATION__ATHENA__QUERY_RESULT_BUCKET', 's3://<your-athena-results-bucket>/')
    if not output_location.startswith('s3://'):
         output_location = f"s3://{output_location}"
    if not output_location.endswith('/'):
         output_location += '/'
    print(f"Executing Athena query: {query}")
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']
        # Poll for completion
        while True:
            status = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED']:
                print(f"Query succeeded: {query_execution_id}")
                return True
            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"❌ Athena query {state}: {reason}")
                return False
            time.sleep(2)
    except Exception as e:
        print(f"❌ Error executing Athena query: {str(e)}")
        return False

def get_secret(secret_name, region_name='ap-south-1'):
    """
    Fetch secret from AWS Secrets Manager
    """
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        # Secrets Manager returns a string, need to parse JSON
        secret = json.loads(response['SecretString'])
        return secret
    except Exception as e:
        print(f"❌ Error fetching secret {secret_name}: {str(e)}")
        return None

def get_connection_url(secret, database):
    """
    Construct MSSQL SQLAlchemy URL from secret
    Using pymssql driver for better compatibility across environments
    """
    user = urllib.parse.quote_plus(str(secret.get('username', '')))
    password = urllib.parse.quote_plus(str(secret.get('password', '')))
    # db_server might contain host or host:port
    db_server = secret.get('db_server', '')
    # pymssql URL format: mssql+pymssql://user:password@host[:port]/database
    # If there's a port, db_server might be host,1433 (MSSQL style) or host:1433
    host = db_server.replace(',', ':')
    return f"mssql+pymssql://{user}:{password}@{host}/{database}"


def convert_numeric_to_string(item):
    """
    Traverses the dictionary and converts any Decimal or float values to strings.
    This prevents 'Rescaling Decimal value would cause data loss' errors in Athena/DLT
    by treating these fields as text. Integers and dates are preserved.
    """
    for key, value in item.items():
        if isinstance(value, (Decimal, float)):
            try:
                # Convert to string to preserve full precision and bypass Athena decimal limits
                item[key] = str(value)
            except Exception:
                pass
    return item


def run_pipeline(manual_config=None):
    if manual_config:
        config = manual_config
    else:
        config_file = "config_mssql.yml"
        if not os.path.exists(config_file):
            # Fallback to absolute path if needed, but relative should work in the script dir
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(script_dir, "config_mssql.yml")

        if not os.path.exists(config_file):
            print(f"❌ Config file {config_file} not found")
            return

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

    send_slack_webhook("🚀 *MSSQL to Athena DLT Job Started*")

    for job in config.get('jobs', []):
        server_name = job.get('server_name')
        database = job.get('database')
        table_name = job.get('table_name')
        schema_name = job.get('schema_name', 'dbo') # MSSQL default is dbo
        load_strategy = job.get('load_strategy', 'full')
        incremental_field = job.get('incremental_field')
        initial_value = job.get('initial_value')
        end_date = job.get('end_date')
        is_unix_timestamp = job.get('is_unix_timestamp', False)
        lookback_days = job.get('lookback_days', 1)  # For date_replace strategy

        print(f"Processing {database}.{schema_name}.{table_name}...")

        try:
            # Fetch secret from AWS Secrets Manager
            secret = get_secret(server_name)
            if not secret:
                raise ValueError(f"Secret {server_name} not found or invalid")

            # Construct connection URL
            conn_url = get_connection_url(secret, database)

            # Define pipeline
            # Dataset name matches Athena schema/database
            dataset_name = f"dlt_{server_name.lower()}"
            pipeline_name = f"mssql_{server_name.lower()}_{database.lower()}_{table_name.lower()}"

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='athena',
                dataset_name=dataset_name,
            )

            # ── DATE-REPLACE STRATEGY ─────────────────────────────────────────────
            # For tables where ALL rows in a day share the same midnight timestamp
            # (e.g. TRADEDATE = 2026-02-24 00:00:00.000000).
            # Incremental tracking fails because the cursor never advances within a day.
            # Instead, we query by exact date and do a full REPLACE of that partition.
            if load_strategy == "date_replace" and incremental_field:
                today = datetime.utcnow().date()
                window_start = today - timedelta(days=lookback_days - 1)
                # Build a date-windowed query so we only fetch the target day(s)
                date_query = (
                    f"SELECT * FROM [{schema_name}].[{table_name}] "
                    f"WHERE CAST([{incremental_field}] AS DATE) >= '{window_start}' "
                    f"AND CAST([{incremental_field}] AS DATE) <= '{today}'"
                )
                print(f"[date_replace] Querying window {window_start} \u2192 {today}: {date_query}")

                # Use raw SQLAlchemy — bypasses all DLT sql_database/sql_table API version issues
                athena_table_name = f"{database.lower()}_{table_name.lower()}"
                import dlt.common.normalizers.naming.snake_case as sc
                naming = sc.NamingConvention()
                normalized_key = naming.normalize_identifier(incremental_field)

                # 1. DELETE existing partition data first if table exists
                # We use DELETE for Iceberg or just try it. If it fails, we log and continue
                # since the user specifically asked for a delete query first.
                delete_query = (
                    f"DELETE FROM {dataset_name}.{athena_table_name} "
                    f"WHERE {normalized_key} >= DATE('{window_start}') "
                    f"AND {normalized_key} <= DATE('{today}')"
                )
                print(f"[date_replace] Running pre-load delete: {delete_query}")
                execute_athena_query(delete_query, dataset_name)

                # 2. Setup resource for APPEND
                @dlt.resource(
                    name=athena_table_name,
                    write_disposition="append" # Append after manual delete
                )
                def date_replace_resource(_url=conn_url, _sql=date_query):
                    engine = create_engine(_url)
                    with engine.connect() as conn:
                        result = conn.execute(sa_text(_sql))
                        cols = list(result.keys())
                        for row in result:
                            yield dict(zip(cols, row))

                # Apply numeric-to-string conversion
                date_replace_resource.add_map(convert_numeric_to_string)

                adapted = athena_adapter(
                    date_replace_resource,
                    partition=[athena_partition.day(normalized_key)]
                )

                info = pipeline.run(adapted, write_disposition="append")
                print(f"\u2705 [date_replace] Successfully processed {athena_table_name} for {window_start}\u2192{today}")
                print(info)
                continue  # Skip the rest of the loop for this job
            # ── END DATE-REPLACE STRATEGY ─────────────────────────────────────────

            # Check if a custom query is provided
            query = job.get('query')
            if query:
                print(f"Using custom query for {table_name}")
                source = sql_database(
                    credentials=conn_url,
                    queries={table_name: query}
                )
            else:
                # Source configuration restricted to the specific table
                source = sql_database(
                    credentials=conn_url,
                    schema=schema_name,
                    table_names=[table_name]
                )

            # Check if resource was found (e.g. if it's a view or table)
            if not source.resources:
                raise ValueError(f"Table/View {table_name} not found in schema {schema_name}")

            # Get actual resource name (handles normalization/casing)
            resource_name = list(source.resources.keys())[0]

            # Set destination table name with database prefix as requested by user
            # e.g., client_db_client_details
            athena_table_name = f"{database.lower()}_{table_name.lower()}"
            source.resources[resource_name].apply_hints(table_name=athena_table_name)
            # Add string conversion for numeric types for all strategies
            source.resources[resource_name].add_map(convert_numeric_to_string)

            if load_strategy == "incr" and incremental_field:
                # Handle Initial Value Conversion (casting string configuration to proper type)
                if initial_value and isinstance(initial_value, str):
                    if is_unix_timestamp:
                        try:
                            # Assume initial_value is in YYYY-MM-DD format
                            dt = datetime.strptime(initial_value, "%Y-%m-%d")
                            # Subtract 1 second to make the start inclusive (DLT defaults to > initial_value)
                            initial_value = int(dt.timestamp()) - 1
                            print(f"Converted initial_value {job.get('initial_value')} to epoch (with -1s offset): {initial_value}")
                        except ValueError:
                            print(f"⚠️ Could not convert initial_value {initial_value} to epoch. Using as-is.")
                    else:
                        # Try to convert string date to datetime object for correct comparison in DLT
                        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"):
                            try:
                                initial_value = datetime.strptime(initial_value, fmt) - timedelta(seconds=1)
                                print(f"Converted initial_value to datetime (with -1s offset): {initial_value}")
                                break
                            except ValueError:
                                continue

                # Use DLT's naming convention to find the normalized key
                import dlt.common.normalizers.naming.snake_case as sc
                naming = sc.NamingConvention()
                normalized_key = naming.normalize_identifier(incremental_field)

                # CLEAN SOLUTION: Determine partitioning field and register it if needed
                # Use normalized_key for partitioning to match DLT's schema column names
                partition_field = normalized_key
                columns_hint = {}

                if is_unix_timestamp:
                    # Use a dedicated shadow column for partitioning to avoid type conflicts with incremental tracking
                    partition_field = "_partition_date"
                    columns_hint[partition_field] = {"data_type": "timestamp"}

                def finalize_record_naming_and_types(item):
                    """
                    Ensures records contain the fields expected by BOTH partitioning
                    and DLT's incremental tracking (which is case-sensitive).
                    """
                    # 1. Fetch value from the record (checking original and normalized keys)
                    val = item.get(incremental_field)
                    if val is None:
                        val = item.get(normalized_key)
                    if val is None:
                        val = item.get(incremental_field.lower())

                    if val is not None:
                        # 2. Handle Unix Timestamp logic
                        if is_unix_timestamp:
                            try:
                                # Track incremental state using the original INTEGER field
                                item[incremental_field] = int(val)
                                # Partition Athena using a separate DATETIME field
                                item[partition_field] = datetime.fromtimestamp(int(val))
                            except (ValueError, TypeError):
                                pass
                        else:
                            # 3. Ensure original field is preserved for cases where DLT normalized it
                            item[incremental_field] = val
                    return item

                # Add map transformation for incremental field handling
                source.resources[resource_name].add_map(finalize_record_naming_and_types)

                # Register the shadow column (if any) and setup incremental tracking
                end_value = None
                if end_date:
                    # Parse and add 1 day to make the end date inclusive
                    # e.g., if end_date is 2025-12-03, we want to include records FROM 2025-12-03
                    # so we set cutoff to 2025-12-04 00:00:00
                    parsed_end = pendulum.parse(end_date).naive()

                    if is_unix_timestamp:
                        end_value = int(parsed_end.timestamp())
                    else:
                        end_value = parsed_end

                source.resources[resource_name].apply_hints(
                    columns=columns_hint,
                    incremental=dlt.sources.incremental(
                        incremental_field,
                        initial_value=initial_value,
                        end_value=end_value
                    )
                )

                # Wrap with athena_adapter for partitioning
                source = athena_adapter(
                    source,
                    partition=[athena_partition.day(partition_field)]
                )
                write_disposition = "merge"
            else:
                write_disposition = "replace"

            # Run pipeline
            info = pipeline.run(source, write_disposition=write_disposition)
            print(f"✅ Successfully processed {athena_table_name}")
            print(info)

        except Exception as e:
            error_msg = f"❌ Failed MSSQL DLT job: {database}.{table_name} - {str(e)}"
            print(error_msg)
            send_slack_webhook(error_msg)

    send_slack_webhook("✅ *MSSQL to Athena DLT Job Completed*")

if __name__ == "__main__":
    run_pipeline()