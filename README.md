# spark-airflow-redis-pipeline
## TODO:
* Figure out what to do in part 3 and 4.
* Verify that we processed the data correctly (90% sure that we did)
* Develop an Airflow DAG for inference_cached.py

## SENG 550 Assignment 3
### Members:
- Edward An UCID: 30142179
- Sahib Singh Thethi UCID: 30117033

# Files
#### Part 0
The file [`split.py`](/data/split.py) uses Spark to partition the [`orders.csv`](/data/orders.csv) data by the day of week (order_dow) column into 7 separate CSV files and store them in `data/raw` folder.

#### Part 1
The file [`full_aggregation.py`](/processing/full/full_aggregation.py) uses Spark to process the 7 raw data files created in part 0 of the assignment. To be specific, Spark is used to aggregate the order data into a smaller dataset showing the number of items sold for each category, grouped by day of week and hour of day. The output CSV file is then stored in the `data/processed` folder.

#### Part 2
The file [`incremental_aggregation.py`](/processing/incremental/incremental_aggregation.py) does the same thing as part 1 except it only reads the unprocessed folders (new days) each time it runs. It keeps track of a key, named `processed_day` stored in Redis to determine which files should be aggregated. The key is updated each time the aggregation is run. Similar to part 1, the output CSV file is also stored in the `data/processed` folder. The [`incremental_aggregation_dag.py`](/airflow/dags/incremental_aggregation_dag.py) file is used to run the aggregation on a schedule.

#### Part 3
- [`train.py`](/processing/ml/train.py)
- [`train_dag.py`](/airflow/dags/train_dag.py)
- [`inference.py`](/processing/ml/inference.py)

#### Part 4
- [`inference_cached.py`](/processing/ml/inference_cached.py)


# How to run the pipeline
Before running the data processing pipeline, make sure to have Docker installed on the local machine. All other dependencies such as Apache Spark, Apache Airflow, and Redis will be handled within Docker containers. The only thing you need is Docker.

Run the following command in the directory containing `docker-compose.yml` to configure Redis, Apache Spark, and Airflow in Docker containers,
```bash
docker compose up -d
```

\* If any changes have been made to the `docker-compose.yml` file, run the following command:
```bash
docker compose down
```

Then run `docker compose up -d` again.

## Part 0
To split the data by day of week (order_dow) into 7 separate files and store them under different folders, run the following.
```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/data/split.py
```

The output CSV files appears in `data/raw/`, but Spark writes ugly filenames. Rename them to match the assignment requirements.
In each data/raw/`#`/file_name.csv, rename it to data/raw/`#`/orders_`#`.csv. 

So for example, `data/raw/0/part-0000-8b39fdsf.csv` should be renamed to `data/raw/0/orders_0.csv`.

If the CSV files are not renamed, there will be issues with running the next parts.

## Part 1

To execute full data aggregation, run the following command:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/full/full_aggregation.py
```

The output CSV file appears in `data/processed/`, but Spark writes ugly filenames. Make sure to rename it to `orders.csv`.

## Part 2
Since daily data arrival will be simulated to perform incremental aggregation, create folders (only the following three) and copy the raw data in `data/raw` directory from part 0. Make sure the file names are `orders_0.csv`, `orders_1.csv`, and `orders_2.csv`.
- data/incremental/raw/0/
- data/incremental/raw/1/
- data/incremental/raw/2/

The incremental aggregation can be simulated manually or the Airflow DAG can be used to schedule the aggregation. This README will cover both methods.

### Manually Running Incremental Data Aggregation
After the CSV files are copied over, run the following command to manually run incremental data aggregation:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/incremental/incremental_aggregation.py
```

The output CSV file appears in `data/processed/#/`, but Spark writes ugly filenames. This is fine. In addition to the output CSV file, a log file named `log.txt` is written to `data/processed/` and updated every time the incremental data aggregation is run.


### Scheduling Incremental Data Aggregation Using Airflow
There are two ways to schedule Airflow DAG.
- Command Line Interface
- Graphical User Interface

---

#### Command Line Interface
Run the shell inside the Airflow container in Docker with the following command:
```bash
docker exec -it airflow /bin/bash
```

Once inside the Airflow container in Docker, run the `airflow dags list` command to see all the airflow dags
```bash
airflow@53789362447e:/opt/mnt/data$ airflow dags list
dag_id                  | fileloc                                          | owners  | is_paused
========================+==================================================+=========+==========
incremental_aggregation | /opt/airflow/dags/incremental_aggregation_dag.py | airflow | True     
```

Every Airflow script located inside the `/opt/airflow/dags` folder within Docker container is listed as an Airflow DAG by default. Airflow pauses new DAGs by default, so you must unpause it before it will run.

To unpause the Airflow DAG, simply run the following command.
```bash
airflow dags unpause incremental_aggregation
```

Now the `incremental_aggregation.py` script should be run on a schedule.

---

#### Graphical User Interface
Go to [localhost:8085](http://localhost:8085/)

You will be prompted to a login page as shown below:
![Login Page](/assets/Airflow_login_page.png)

Login using 
- username: `admin`
- password: `admin`

You will be prompted to a home page as shown below:
![Home Page](/assets/Airflow_home_page.png)

Click on the button located on the left of the DAG name to pause/unpause the DAG.
![Pause/Unpause DAG](/assets/Airflow_unpause_DAG.png)

Once the incremental_aggregation DAG has been unpaused, the aggregation will run every 4 seconds as specified in the `incremental_aggregation_dag.py` file.

---

### Inspecting Redis Cache Contents
After running the incremental data aggregation either manually or automatically using a schedule, we can verify that data has been written to Redis.

To run the shell inside Redis container in Docker, run the following:
```bash
docker exec -it redis /bin/bash
```

Once inside the Redis container in Docker, run the `redis-cli` command to interact with Redis.
```bash
redis-cli
```

The `keys *` command can be used in redis-cli to see all keys stored in Redis.
```bash
keys *
```

The `get` command can be used in redis-cli to see the value of `processed_day` key in Redis.
```bash
get processed_day
```

The `del` command can be used in redis-cli to delete key-value pairs stored in Redis.
```bash
del processed_day
```

Note that if the `processed_day` key is deleted, the aggregation will run on ALL the CSV files inside the incremental/raw folders. Essentially, when Redis is cleared, the system reprocesses everything from scratch.

To escape the redis-cli and/or the Redis Docker container, run `exit` in the shell.


## Part 3
The Spark Machine Learning Random Forest model is trained on all the processed CSV datasets located in the `data/processed` folder. So as the more data comes in, the aggregation part adds it to the aggregation results and the
model will use it to be a more accurate model. The input features are `day_of_week`, `hour_of_day`, and `category` and the target feature is the `total_count`.

The model training can be done manually or the Airflow DAG can be used to schedule the training. This README will cover both methods.

### Manually Running Model Training
After completing Part 2, run the following command to manually run model training:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/ml/train.py
```

The machine learning model files appear in the `processing/ml/models` directory.

These files will be used for prediction later.


### Scheduling Model Training Using Airflow
Refer to the following section: [Scheduling Incremental Data Aggregation Using Airflow](#Scheduling-Incremental-Data-Aggregation-Using-Airflow). The process is essentially the same, except the DAG id is changed to `machine_learning_training`. 

After the Airflow DAG is unpaused, the machine learning model will be trained every 20 seconds as defined inside the `train_dag.py` file. As more data is processed through scheduled incremental aggregation in Part 2, each iteration of the model will become more accurate.

---

### Running Predictions Using Machine Learning Model
After the model has been trained, it can be used to predict the number of items in an order based on the day of week, hour of day, and category. The `inference.py` file does exactly this. Run the following command to run the prediction:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/ml/inference.py <day_of_week> <hour_of_day> <category>
```

\* *Notice the 3 command line arguments above (day of week, hour of day, category). Make sure to replace it with actual parameters such as `/opt/.../ml/inference.py 0 8 "fresh vegetables"`*

The predictions are written to a txt file named `output.txt` and stored in the `processing/ml` directory.

## Part 4
Passing thousands of inputs through a ML learning model for prediction can be quite slow. The `inference_cached.py` file speeds up the prediction process by using Redis to store the predictions. 

So the key, value pair in redis would be key: `day_of_week:hour_of_day:category` and the value would be the `predicted number of items`.