# spark-airflow-redis-pipeline
SENG 550 Assignment 3

Run the following command in the direcotry containing `docker-compose.yml` to configure Redis, Apache Spark, and Airflow in Docker containers
```bash
docker compose up -d
```

If any changes have been made to the `docker-compose.yml` file, run the following command:
```bash
docker compose down
```

Then run `docker compose up -d` again.


To run the shell inside Docker, run the following
```bash
docker exec -it spark-master /bin/bash
```

To escape the shell inside Docker, run `exit` in the shell.

## Part 0
To split the data by day of week (order_dow) into 7 separate files and store them under different folders, run the following.
```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/airflow/data/split.py
```

The output CSV file is written to `data/raw/`

Note that the produced CSV file names are not clean. Make sure to rename it to assignment's guidelines. In each part1/data/raw/`#`/name.csv, rename it to part1/data/raw/`#`/orders_`#`.csv

## Part 1

To run full data aggregation, run 

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/airflow/processing/full/full_aggregation.py
```

The output CSV file is written to `data/processed/`
Note that the produced CSV file name is not clean. Make sure to rename it to `orders.csv`