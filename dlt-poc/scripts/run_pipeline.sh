#!/bin/bash

# Set error handling
set -euo pipefail

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Main execution
main() {
    log "Starting dlt-data-integration pipeline execution"

    log "Installing boto3 for secret fetching"
    pip install boto3 || { log "Error installing boto3"; exit 1; }

    log "Downloading DLT configurations from S3 using boto3"
    python scripts/fetch_secrets.py || { log "Error downloading secrets.toml"; exit 1; }

    # pipeline venv
    log "Setting up  postgres-dlt-data-integration virtual environment"
    python3 -m venv ~/venv/ postgres-dlt-data-integration || { log "Error creating venv"; exit 1; }
    . ~/venv/ postgres-dlt-data-integration/bin/activate

    log "Installing pipeline requirements"
    pip install -r postgres_pipeline/requirements.txt || { log "Error installing requirements"; exit 1;}

    log "Running postgres_pipeline.py"
    cd postgres_pipeline
    python postgres_pipeline.py || { log "postgres_pipeline.py run failed"; exit 1; }
    cd ..

    log "Pipeline execution completed successfully"

    deactivate

    # mssql pipeline
    log "Setting up mssql-dlt-data-integration virtual environment"
    python3 -m venv ~/venv/mssql-dlt-data-integration || { log "Error creating venv"; exit 1; }
    . ~/venv/mssql-dlt-data-integration/bin/activate

    log "Installing pipeline requirements"
    pip install -r mssql_pipeline/requirements.txt || { log "Error installing requirements"; exit 1;}

    log "Running mssql_pipeline.py"
    cd mssql_pipeline
    python mssql_pipeline.py || { log "mssql_pipeline.py run failed"; exit 1; }

    log "Pipeline execution completed successfully"
    cd ..

    deactivate

    # pipeline venv mongo
    log "Setting up dlt-data-integration-mongo virtual environment"
    python3 -m venv ~/venv/dlt-data-integration-mongo || { log "Error creating venv"; exit 1; }
    . ~/venv/dlt-data-integration-mongo/bin/activate

    log "Installing pipeline requirements"
    pip install -r mongodb_pipeline/requirements.txt || { log "Error installing requirements"; exit 1;}

    log "Running mongodb_pipeline.py"
    cd mongodb_pipeline
    python mongodb_pipeline.py || { log "mongodb_pipeline.py run failed"; exit 1; }
    cd ..

    deactivate
    log "Pipeline execution completed successfully"
}

main "$@"
