#!/bin/bash

# Set error handling
set -euo pipefail

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Main execution
main() {
    log "Starting data pipeline execution"

    # Resolve REPO_ROOT: Assume script is in <REPO_ROOT>/scripts/
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(dirname "$SCRIPT_DIR")"
    
    log "REPO_ROOT resolved to: $REPO_ROOT"

    # Try to set JAVA_HOME if not already set
    if [ -z "${JAVA_HOME:-}" ]; then
        log "JAVA_HOME not set, checking for local JRE..."
        
        # Ensure venv is active to have boto3 for fetch_jre.py or fetch_jars.py
        python3 -m venv ~/venv/data-integration || { log "Error creating venv"; exit 1; }
        . ~/venv/data-integration/bin/activate
        pip install boto3 >/dev/null 2>&1

        # 1. Download JRE (The Engine) if missing
        if [ ! -d "$REPO_ROOT/jre" ]; then
            log "Local JRE (Engine) missing, downloading portable OpenJDK..."
            python "$REPO_ROOT/scripts/fetch_jre.py" || { log "Failed to setup portable JRE"; exit 1; }
        fi

        if [ -d "$REPO_ROOT/jre" ]; then
            export JAVA_HOME="$REPO_ROOT/jre"
            export PATH="$JAVA_HOME/bin:$PATH"
            log "Using Portable JRE Engine: $JAVA_HOME"
        fi
    fi

    # 2. Download Driver JARs (The Libraries)
    log "Downloading database driver JARs from S3"
    python scripts/fetch_jars.py || { log "Error downloading JARs"; exit 1; }

    if [ -z "${JAVA_HOME:-}" ]; then
        log "CAUTION: JAVA_HOME still not set. PySpark will fail."
    else
        log "Final JAVA_HOME: $JAVA_HOME"
        $JAVA_HOME/bin/java -version 2>&1 | head -n 1 || log "Cannot execute java"
    fi

    log "Checking for ps command (procps):"
    command -v ps >/dev/null 2>&1 && log "ps command found" || log "WARNING: ps command NOT found. Spark might show minor warnings but should run."

    # First pipeline (data-integration)
    log "Setting up data-pipeline virtual environment"

    cd "$REPO_ROOT" || { log "Failed to change to integration directory"; exit 1; }
    
    # Create venv in a standard location

    log "Installing boto3 for JAR fetching"
    pip install boto3 || { log "Error installing boto3"; exit 1; }

    log "Downloading database driver JARs from S3"
    python scripts/fetch_jars.py || { log "Error downloading JARs"; exit 1; }

    log "Installing data-pipeline requirements"
    pip install -r "$REPO_ROOT/requirements.txt" || { log "Error installing requirements"; exit 1; }

    log "Running MSSQL to S3 export"
    python "$REPO_ROOT/di-poc-data-source/scripts/mssql_to_s3.py" || { log "MSSQL export failed"; exit 1; }

    log "Running MySQL to S3 export"
    python "$REPO_ROOT/di-poc-data-source/scripts/mysql_to_s3.py" || { log "MySQL export failed"; exit 1; }

    # log "Running PostgreSQL to S3 export"
    # python "$REPO_ROOT/di-poc-data-source/scripts/postgres_to_s3.py" || { log "PostgreSQL export failed"; exit 1; }


    deactivate

    log "Pipeline execution completed successfully"
}

main "$@"