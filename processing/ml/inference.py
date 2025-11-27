from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import pyspark.sql.functions as F
import os
import sys

spark = SparkSession.builder.master("local[*]").getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load saved models from train.py
MODEL_DIR = os.path.join(BASE_DIR, "models")
TEST_DIR = os.path.join(BASE_DIR, "test_set/*.csv")


rf_model_path = os.path.join(MODEL_DIR, "rf_model")
indexer_path = os.path.join(MODEL_DIR, "indexer")
encoder_path = os.path.join(MODEL_DIR, "encoder")
prediction_csv_path = os.path.join(BASE_DIR, "prediction")

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