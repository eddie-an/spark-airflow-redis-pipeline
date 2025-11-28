# spark-airflow-redis-pipeline
## SENG 550 Assignment 3
### Members:
- Edward An UCID: 30142179
- Sahib Singh Thethi UCID: 30117033

# Files
#### Part 0
Splits the original dataset by order_dow so each day of the week gets its own CSV.

- [`split.py`](/data/split.py) reads [`orders.csv`](/data/orders.csv) and writes 7 separate day-based files into `data/raw/#` folder.

#### Part 1
Runs the full aggregation over all 7 raw day files.
- [`full_aggregation.py`](/processing/full/full_aggregation.py) uses Spark to compute total items sold per `(day_of_week, hour_of_day, category)` and writes the results to `data/processed` folder.


#### Part 2
Implements incremental aggregation so only new day folders are processed.

- [`incremental_aggregation.py`](/processing/incremental/incremental_aggregation.py) reads unprocessed day folders, aggregates them, appends the results, and updates the processed_day key in Redis.

- [`incremental_aggregation_dag.py`](/airflow/dags/incremental_aggregation_dag.py) is an Airflow DAG that schedules incremental processing.

#### Part 3
The machine learning pipeline lives here. This part trains a Random Forest regression model using the fully aggregated dataset and runs inference on the test split.

- [`train.py`](/processing/ml/train.py) trains the ML model using Spark ML and outputs the model files & test set.
- [`train_dag.py`](/airflow/dags/train_dag.py) is an Airflow DAG that retrains the model every 20 seconds.
- [`inference.py`](/processing/ml/inference.py) runs predictions on the test set produced by training and writes them out as a CSV.

#### Part 4
This part optimizes inference by caching predictions inside Redis, so machine learning predictions are stored in cache.

- [`inference_cached.py`](/processing/ml/inference_cached.py) generates all predictions using the trained model and stores them in Redis using the key-value pair of  (`day_of_week:hour_of_day:category`,`prediction`).
- [`predict_dag.py`](/airflow/dags/predict_dag.py) is an Airflow DAG that runs the cached prediction job on a schedule (every 20 seconds).


# Directory Layout
```
data/
├── raw/ <-- this will be created once commands are run
├── processed/ <-- this will be created once commands are run
processing/
├── full/
├── incremental/
└── ml/
airflow/
└── dags/
```

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

The output CSV files appears in `data/raw/#`.

## Part 1

To execute full data aggregation, run the following command:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/full/full_aggregation.py
```

The output CSV file appears in `data/processed/`.

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

The output CSV file appears in `data/processed/#/`. In addition to the output CSV file, a log file named `log.txt` is written to `data/processed/` and updated every time the incremental data aggregation is run.


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
model will use it to be a more accurate model. The input features are `day_of_week`, `hour_of_day`, and `category` and the target feature is the `total_count`. The model uses a 90/10 split to use 90% of the dataset for training and 10% of the dataset for testing. The test set is then saved as a csv file to the `processing/ml/test_set` directory, and it will be used for making predictions.

The model training can be done manually or the Airflow DAG can be used to schedule the training. This README will cover both methods.

### Manually Running Model Training
After completing Part 2, run the following command to manually run model training:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/ml/train.py
```

The machine learning model files are exported and appear in the `processing/ml/models` directory. The test set can be found in the `processing/ml/test_set` directory.

These files will be used for prediction later.


### Scheduling Model Training Using Airflow
Refer to the following section: [Scheduling Incremental Data Aggregation Using Airflow](#Scheduling-Incremental-Data-Aggregation-Using-Airflow). The process is essentially the same, except the DAG id is changed to `machine_learning_training`. 

After the Airflow DAG is unpaused, the machine learning model will be trained every 20 seconds as defined inside the `train_dag.py` file. As more data is processed through scheduled incremental aggregation in Part 2, each iteration of the model will become more accurate.

---

### Running Predictions Using Machine Learning Model
After the model has been trained, it can be used to predict the number of items in an order based on the day of week, hour of day, and category. The `inference.py` file does exactly this. The prediction is run on the test set created earlier from training. Run the following command to run the prediction:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/ml/inference.py
```

The predictions are written to a csv file stored in the `processing/ml/prediction` directory. They are stored in the following format:

```csv
order_dow,order_hour_of_day,category,items,prediction
0,0,fresh fruits,8,27
0,0,fresh vegetables,4,21
0,1,tortillas flat bread,1,3
0,10,breakfast bars pastries,1,5
0,10,energy granola bars,19,5
0,10,indian foods,1,5
0,10,packaged meat,3,5
0,10,packaged poultry,4,5
0,10,preserved dips spreads,3,5
0,11,candy chocolate,5,5
0,11,cleaning products,7,5
0,11,feminine care,1,5
0,11,fresh vegetables,149,42
0,11,hot dogs bacon sausage,13,5
0,11,muscles joints pain relief,2,5
```

## Part 4
Passing thousands of inputs through a ML learning model for prediction can be quite slow. The `inference_cached.py` file speeds up the prediction process by using Redis to store the predictions.

The key would be stored in the format of `day_of_week:hour_of_day:category` and the value would be the `predicted number of items`.

The model prediction can be done manually or the Airflow DAG can be used to schedule the training. This README will cover both methods.

### Manually Running Model Prediction
Run the following command to run the prediction:

```bash
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  /opt/mnt/processing/ml/inference_cached.py
```

The predictions are written to a csv file stored in the `processing/ml/prediction` directory.


### Scheduling Model Prediction Using Airflow
Refer to the following section: [Scheduling Incremental Data Aggregation Using Airflow](#Scheduling-Incremental-Data-Aggregation-Using-Airflow). The process is essentially the same, except the DAG id is changed to `machine_learning_prediction`. 

After the Airflow DAG is unpaused, the prediction will run on the test set every 20 seconds as defined inside the `predict_dag.py` file. As the model is trained on a schedule in part 3, the predictions will become more accurate over time. The new predictions will replace the old predictions stored inside Redis so that the prediction is up to date.

---

### Inspecting Redis Cache Contents (Again)
After running the cached implementation of inference either manually or automatically using a schedule, we can verify that data has been written to Redis.

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
The output will look something like this:
```bash
1018) "3:16:spreads"
1019) "3:8:frozen meals"
1020) "3:13:tortillas flat bread"
1021) "4:12:energy granola bars"
1022) "3:14:crackers"
1023) "1:11:refrigerated pudding desserts"
1024) "4:17:frozen meat seafood"
1025) "4:21:nuts seeds dried fruit"
1026) "0:16:condiments"
1027) "4:18:dish detergents"
1028) "6:19:grains rice dried goods"
1029) "3:11:frozen produce"
1030) "5:19:refrigerated"
```

The `get` command can be used in redis-cli to see the value of any of the keys in Redis.

For example:
```bash
127.0.0.1:6379> get 3:8:"frozen meals"
"4"
```

To escape the redis-cli and/or the Redis Docker container, run `exit` in the shell.


# Summary
This project brings together Spark, Airflow, and Redis to build a full data-processing and prediction pipeline from start to finish. Raw Instacart-style order data is first split by day of week, then fully and incrementally aggregated using Spark. Airflow orchestrates scheduled processing so new data is detected and merged automatically, with Redis keeping track of progress across runs.

A machine learning model is trained on the aggregated dataset using Spark ML, with scheduled retraining to keep predictions up to date as more data arrives. To optimize inference performance, all model predictions are cached in Redis, allowing the system to respond instantly to high-volume prediction requests without repeatedly running the ML model.

Overall, this pipeline demonstrates how batch processing, incremental loading, orchestration, caching, and machine learning can all work together in a scalable, production-style architecture—powered entirely through containerized Spark, Airflow, and Redis services.