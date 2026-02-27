import json
import boto3
import urllib.parse
from sqlalchemy import create_engine, inspect

def get_secret(secret_name, region_name='ap-south-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def list_columns(server_name, database, table_name):
    secret = get_secret(server_name)
    user = secret.get("username")
    password = secret.get("password")
    host = secret.get("db_server")
    
    encoded_password = urllib.parse.quote_plus(password)
    # Using pymssql as we did in the pipeline
    connection_url = f"mssql+pymssql://{user}:{encoded_password}@{host}/{database}"
    
    engine = create_engine(connection_url)
    inspector = inspect(engine)
    
    print(f"Schema: dbo, Table: {table_name}")
    columns = inspector.get_columns(table_name, schema='dbo')
    
    if not columns:
        print("No columns found or table does not exist in 'dbo' schema.")
        # Try without schema just in case
        columns = inspector.get_columns(table_name)
        if columns:
            print(f"Found columns in default schema:")
    
    for column in columns:
        print(f"- {column['name']} ({column['type']})")

if __name__ == "__main__":
    # Parameters from user's config
    SERVER = "mssql_server_01"
    DB = "exchange_db_poc_01"
    TABLE = "settlement"
    
    try:
        list_columns(SERVER, DB, TABLE)
    except Exception as e:
        print(f"Error: {e}")
