from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

os.environ['AWS_DEFAULT_REGION'] = 'ap-south-1'

default_args = {
    'owner': 'data-engineer-poc',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Resolve repo paths dynamically
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DAG_DIR)

with DAG(
    dag_id='data-integration',
    default_args=default_args,
    description='Run SQL Server, MySQL and postgres to S3 data integration pipeline',
    start_date=datetime(2026, 1, 28),
    schedule='0 19 * * *',
    catchup=False,
    tags=['sqlserver', 'mysql', 'postgres', 's3'],
) as dag:

    # 📁 Verify script path inside repo
    verify_script = BashOperator(
        task_id='verify_script_path',
        bash_command=f'''
        echo "REPO_ROOT: {REPO_ROOT}"
        echo "Checking if script exists:"
        ls -la "{REPO_ROOT}/scripts/run_pipeline.sh" || echo "Script not found"
        '''
    )

    # ▶️ Run data integration pipeline
    run_pipeline = BashOperator(
        task_id='run_data_integration_pipeline',
        bash_command=f'''
        cd "{REPO_ROOT}" && chmod +x ./scripts/run_pipeline.sh && ./scripts/run_pipeline.sh
        '''
    )

    # Task order
    verify_script >> run_pipeline
