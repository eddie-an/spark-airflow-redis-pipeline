# spark-airflow-redis-pipeline
SENG 550 Assignment 3

Run the following to configure Redis, Apache Spark, and Airflow
```bash
docker compose up -d
```

If any changes have been made to the `docker-compose.yml` file, run the following command:
```bash
docker compose down
```

Then run `docker compose up -d` again.


To run full data aggregation, run 

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/airflow/processing/full/full_aggregation.py