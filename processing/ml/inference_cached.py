from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import pyspark.sql.functions as F
import os
import sys
import glob
import redis
from datetime import datetime


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Load saved models from train.py
    MODEL_DIR = os.path.join(BASE_DIR, "models")
    TEST_DIR = os.path.join(BASE_DIR, "test_set/*.csv")


    rf_model_path = os.path.join(MODEL_DIR, "rf_model")
    indexer_path = os.path.join(MODEL_DIR, "indexer")
    encoder_path = os.path.join(MODEL_DIR, "encoder")
    prediction_csv_path = os.path.join(BASE_DIR, "prediction")

    if not (os.path.exists(rf_model_path) and os.path.exists(indexer_path) and os.path.exists(encoder_path)):
        print("Model files not found. Please run train.py first.")
        sys.exit(1)

    if not glob.glob(TEST_DIR):
        print("Test set not found. Please ensure test_set CSV files are available.")
        sys.exit(1)

    rf_model = RandomForestRegressionModel.load(rf_model_path)
    indexer = StringIndexerModel.load(indexer_path)
    encoder = OneHotEncoderModel.load(encoder_path)


    test_df = spark.read.csv(TEST_DIR, header=True, sep=",")


    # Apply the saved indexer + encoder to match train.py
    test_df = indexer.transform(test_df)
    test_df = test_df.withColumn("order_dow", test_df["order_dow"].cast("integer"))
    test_df = test_df.withColumn("order_hour_of_day", test_df["order_hour_of_day"].cast("integer"))

    test_df = encoder.transform(test_df)

    # Rebuild the feature vector
    assembler = VectorAssembler(
        inputCols=["order_dow", "order_hour_of_day", "cat_vec"],
        outputCol="features"
    )

    test_df = assembler.transform(test_df)

    # Run prediction
    prediction_df = rf_model.transform(test_df)
    prediction_df = prediction_df.drop("cat_idx").drop("cat_vec").drop("features")
    prediction_df = prediction_df.withColumn(
        "prediction",
        F.round(F.col("prediction")).cast("int")
    )
    os.makedirs(prediction_csv_path, exist_ok=True)

    # Save each DataFrame as a CSV
    prediction_df.coalesce(1).write.mode("overwrite").csv(f"{prediction_csv_path}", header=True)
    prediction_df.rdd.mapPartitions(write_partition).collect()

    execution_timestamp = datetime.now()
    with open(f'{BASE_DIR}/inference_log.txt', "a") as logFile:
        logFile.write(f"[{execution_timestamp}] Inference output written to {prediction_csv_path} and Redis updated.\n")
    

def write_partition(partition):
    # Create a Redis client inside each executor
    r = redis.Redis(host='redis', port=6379, db=0)

    for row in partition:
        row = row.asDict()
        key = f"{row['order_dow']}:{row['order_hour_of_day']}:{row['category']}"
        value = row['prediction']

        r.set(key, value)

    # Return nothing, Spark just wants an iterator
    return iter([])



if __name__== "__main__":
    main()