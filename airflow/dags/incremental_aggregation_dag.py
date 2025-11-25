from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG runs every 4 seconds (minimum practical interval is 1 second)
with DAG(
    'incremental_aggregation',
    description='Run Spark incremental aggregation job',
    schedule_interval=timedelta(seconds=4),
    start_date=datetime(2025, 11, 20, 0, 0, 0),
    catchup=False,
    max_active_runs=1) as dag:
        BashOperator(
            task_id='run_incremental_spark',
            bash_command=(
                '/opt/spark/bin/spark-submit /opt/mnt/processing/incremental/incremental_aggregation.py'
            ),
        )