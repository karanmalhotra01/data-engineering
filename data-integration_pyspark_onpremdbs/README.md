# Data Integration Pipeline (PySpark)

A robust and scalable ETL pipeline designed to extract data from various on-premise relational databases and load it into AWS S3 using PySpark. This project is tailored for high-performance data ingestion into a modern data lake architecture.

## 🚀 Key Features

- **Multi-Source Support**: Built-in support for multiple RDBMS including **Microsoft SQL Server**, **MySQL**, **Postgres**, and **Oracle**.
- **Incremental Data Loading**: Efficiently fetches only new or updated records based on configurable date/timestamp columns, reducing extraction time and cost.
- **Dynamic Partitioning**: Automatically organizes extracted data in S3 using Hive-style partitioning (`/year/month/day/`), facilitating optimized querying via Athena.
- **AWS Integration**:
    - **Secrets Manager**: Securely retrieves database credentials and Slack webhook tokens.
    - **Glue Crawlers**: Automatically updates the data catalog after successful ingestion.
- **Alerting & Notifications**: Real-time failure alerts sent via Slack webhooks to ensure pipeline reliability.
- **Environment Agnostic**: Uses portable JRE and isolated JAR management for consistent execution across different computing environments (e.g., EC2, EKS, or on-premise).

## 🛠 Tech Stack

- **Engine**: Apache Spark (PySpark)
- **Language**: Python 3
- **Cloud Infrastructure**: AWS S3, AWS Secrets Manager, AWS Glue
- **Orchestration**: Airflow (DAG provided)
- **Monitoring**: Slack API

## 📁 Project Structure

```text
├── dags/                   # Airflow DAG definitions
├── di-poc-data-source/
│   ├── configs/            # YAML configuration for each source
│   └── scripts/            # Core PySpark extraction scripts
├── scripts/                # Utility scripts (fetch JRE, JARs, run pipeline)
├── jars/                   # (Managed) Database driver JARs
└── requirements.txt        # Python dependencies
```

## ⚙️ Configuration

The pipeline is driven by YAML configuration files. Each database source has its own config file (e.g., `config_sqlserver.yml`) defining:

- `server_name`: Identifier for AWS Secrets Manager.
- `database`: Target database name.
- `table_name`: Table to extract.
- `incremental_date`: Column used for date-based filtering.
- `n`: Number of days to look back for historical/incremental loads.

## 🏃 Execution Flow

1. **Environment Setup**: The `run_pipeline.sh` script initializes a virtual environment and ensures the correct JRE and database drivers are present.
2. **Metadata Loading**: Scripts load the job configuration from YAML and retrieve credentials from AWS Secrets Manager.
3. **Extraction & Transformation**:
    - PySpark connects to the source DB via JDBC.
    - Data is filtered incrementally and repartitioned if necessary.
4. **Loading**: Data is written to S3 in Parquet format.
5. **Post-Load**:
    - AWS Glue Crawlers are triggered to update the data catalog.
    - Slack notifications are sent summarizing the job status.

## 📊 Monitoring

Failures are immediately reported to Slack. The notification includes the database and table name that failed, allowing for quick troubleshooting.
