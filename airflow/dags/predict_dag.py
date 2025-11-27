from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG runs every 20 seconds
with DAG(
    'machine_learning_prediction',
    description='Run Spark ml prediction',
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2025, 11, 20, 0, 0, 0),
    catchup=False,
    max_active_runs=1) as dag:
        BashOperator(
            task_id='run_machine_learning_prediction_spark',
            bash_command=(
                '/opt/spark/bin/spark-submit /opt/mnt/processing/ml/inference_cached.py'
            ),
        )