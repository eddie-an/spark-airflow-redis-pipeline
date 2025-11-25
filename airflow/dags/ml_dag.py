from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG runs every 4 seconds (minimum practical interval is 1 second)
with DAG(
    'machine_learning_training',
    description='Run Spark ml training job',
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2025, 11, 20, 0, 0, 0),
    catchup=False,
    max_active_runs=1) as dag:
        BashOperator(
            task_id='run_machine_learning_training_spark',
            bash_command=(
                '/opt/spark/bin/spark-submit /opt/mnt/processing/ml/train.py'
            ),
        )