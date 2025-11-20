# spark-airflow-redis-pipeline
## SENG 550 Assignment 3
### Members:
- Edward An UCID: 30142179
- Sahib Singh Thethi UCID: 30117033

# Files
#### Part 0
The file `split.py` uses Spark to partition the `orders.csv` data by the day of week (order_dow) column into 7 separate files and store them in `data/raw` folder.

#### Part 1
The file `full_aggregation.py` uses Spark to process the 7 raw data files created in part 0 of the assignment. To be specific, Spark is used to aggregate the order data into a smaller dataset showing the number of items sold for each category, grouped by day of week and hour of day. It is then stored in the `data/processed` folder.

# How to run the pipeline
Run the following command in the direcotry containing `docker-compose.yml` to configure Redis, Apache Spark, and Airflow in Docker containers
```bash
docker compose up -d
```

If any changes have been made to the `docker-compose.yml` file, run the following command:
```bash
docker compose down
```

Then run `docker compose up -d` again.


To run the shell inside spark container, run the following
```bash
docker exec -it spark-master /bin/bash
```

To escape the shell inside Docker, run `exit` in the shell.

## Part 0
To split the data by day of week (order_dow) into 7 separate files and store them under different folders, run the following.
```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/data/split.py
```

The output CSV file is written to `data/raw/`

Note that the produced CSV file names are not clean. Make sure to rename it to assignment's guidelines. In each part1/data/raw/`#`/name.csv, rename it to part1/data/raw/`#`/orders_`#`.csv

## Part 1

To run full data aggregation, run 

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/full/full_aggregation.py
```

The output CSV file is written to `data/processed/`
Note that the produced CSV file name is not clean. Make sure to rename it to `orders.csv`