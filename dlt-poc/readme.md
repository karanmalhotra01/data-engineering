# DLT Data Integration POC

This repository contains a Proof of Concept (POC) for building scalable and maintainable data integration pipelines using **dlt (data load tool)**. It demonstrates how to extract data from various sources (MSSQL, PostgreSQL, MongoDB) and load it into **AWS Athena** (backed by S3) with different loading strategies.

## 🚀 Features

- **Multi-Source Support**: Pipelines for MSSQL, PostgreSQL, and MongoDB.
- **Dynamic Loading Strategies**:
    - `full`: Complete data replacement.
    - `incr`: Incremental loading using cursor fields.
    - `date_replace`: Specialized strategy for replacing specific date partitions (useful for daily reporting data).
- **AWS Integration**:
    - **Athena**: Data is loaded into Athena for queryability.
    - **Secrets Manager**: Secure storage and retrieval of database credentials.
    - **S3**: Storage for DLT configurations, staging data, and query results.
- **Airflow Orchestration**: Includes a modular DAG for automating pipeline execution.
- **Automated Notifications**: Integrated Slack notifications for job status (Success/Failure).
- **Schema Management**: Handling of complex data types (e.g., Decimal-to-String conversion to prevent precision loss in Athena).

## 📁 Project Structure

```text
.
├── dags/                        # Airflow DAG definitions
│   └── dlt-data-integration.py  # Main orchestration DAG
├── mssql_pipeline/              # MSSQL to Athena pipeline
│   ├── config_mssql.yml         # Pipeline configuration
│   ├── mssql_pipeline.py        # Core logic
│   └── slack_notifier.py        # Shared notification logic
├── postgres_pipeline/           # PostgreSQL to Athena pipeline
│   ├── config_postgres.yml      # Pipeline configuration
│   └── postgres_pipeline.py     # Core logic
├── mongodb_pipeline/            # MongoDB to Athena pipeline
│   ├── config_mongo.yml         # Pipeline configuration
│   └── mongodb_pipeline.py      # Core logic
├── scripts/                     # Utility scripts
│   ├── run_pipeline.sh          # Bash script to run all pipelines
│   └── fetch_secrets.py         # AWS Secrets retrieval utility
└── readme.md                    # Project documentation
```

## 🛠️ Getting Started

### Prerequisites

- Python 3.8+
- AWS Account with Athena, S3, and Secrets Manager access.
- `dlt` library installed.

### Installation

1. Install DLT with Athena support:
   ```bash
   pip install "dlt[athena]"
   ```

2. Install other dependencies:
   ```bash
   pip install boto3 requests sqlalchemy pymssql psycopg2-binary pymongo
   ```

## ⚙️ Configuration

### DLT Secrets

The pipelines expect a `secrets.toml` file. This can be placed locally in `~/.dlt/` or fetched from S3 using the provided `scripts/fetch_secrets.py`.

Example `secrets.toml`:
```toml
[destination.athena]
database = "your_athena_database"
s3_staging_url = "s3://your-staging-bucket/dlt/"

[sources.mssql]
username = "your_user"
password = "your_password"
```

### Pipeline Configs

Each integration has a YAML config file (e.g., `config_mssql.yml`) to define jobs:
```yaml
jobs:
  - server_name: "mssql_server_01"
    database: "finance_db"
    table_name: "transactions"
    incremental_field: "updated_at"
    load_strategy: "incr"
```

## 🏃 Usage

### Running Locally

Use the `run_pipeline.sh` script to execute the integrated pipelines:
```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh
```

### Airflow Integration

Place the `dlt-data-integration.py` file in your Airflow DAGs directory. The DAG will handle:
1. Fetching secrets from S3.
2. Setting up virtual environments.
3. Executing the DLT pipelines sequentially.

## 📝 License

This project is intended for showcase and POC purposes.
