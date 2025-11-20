from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG runs every 4 seconds (minimum practical interval is 1 second)
with DAG(
    'incremental_aggregation',
    description='Run Spark incremental aggregation job',
    schedule='*/1 * * * *',  # Airflow cron cannot do 4 seconds natively
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1) as dag:
        BashOperator(
            task_id='run_incremental_spark',
            # bash_command=(
            #     'docker exec spark-master '
            #     '/opt/spark/bin/spark-submit '
            #     '/opt/mnt/processing/incremental/incremental_aggregation.py'
            # ),
            bash_command=(
                    'echo hello'
            )
        )