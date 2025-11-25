from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import pyspark.sql.functions as F
import os
import sys
import redis


def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # CLI args: day_of_week, hour_of_day, category
    if len(sys.argv) != 4:
        print("Usage: python inference.py <day_of_week> <hour_of_day> <category>")
        sys.exit(1)

    day_of_week = int(sys.argv[1])
    hour_of_day = int(sys.argv[2])
    category = sys.argv[3]
    redisKey = f"{day_of_week}:{hour_of_day}:{category}"

    r = redis.Redis(host='redis', port=6379, db=0)
    exists = r.exists(redisKey)
    
    prediction = None
    if exists == 0:
        prediction = machineLearningInference(day_of_week, hour_of_day, category)
        r.set(redisKey, prediction)
        with open(f'{BASE_DIR}/output.txt', "a") as outputFile:
            outputFile.write(f"[Ran ML prediction] Predicted number of items for day_of_week: {day_of_week}, hour_of_day: {hour_of_day}, category: {category} is {round(prediction)}\n")

    else:
        prediction = (float) (r.get(redisKey))
        with open(f'{BASE_DIR}/output.txt', "a") as outputFile:
            outputFile.write(f"[Retrieved from Redis] Predicted number of items for day_of_week: {day_of_week}, hour_of_day: {hour_of_day}, category: {category} is {round(prediction)}\n")
    
    print(f"Predicted number of items: {round(prediction)}")



def machineLearningInference(day_of_week, hour_of_day, category):
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Load saved models from train.py
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    MODEL_DIR = os.path.join(BASE_DIR, "models")
    rf_model_path = os.path.join(MODEL_DIR, "rf_model")
    indexer_path = os.path.join(MODEL_DIR, "indexer")
    encoder_path = os.path.join(MODEL_DIR, "encoder")
    
    rf_model = RandomForestRegressionModel.load(rf_model_path)
    indexer = StringIndexerModel.load(indexer_path)
    encoder = OneHotEncoderModel.load(encoder_path)
    
    # Create a single-row Spark DataFrame
    input_df = spark.createDataFrame(
        [(day_of_week, hour_of_day, category)],
        ["order_dow", "order_hour_of_day", "category"]
    )

    # Apply the saved indexer + encoder to match train.py
    input_df = indexer.transform(input_df)
    input_df = input_df.withColumn("order_dow", input_df["order_dow"].cast("integer"))
    input_df = input_df.withColumn("order_hour_of_day", input_df["order_hour_of_day"].cast("integer"))

    input_df = encoder.transform(input_df)

    # Rebuild the feature vector
    assembler = VectorAssembler(
        inputCols=["order_dow", "order_hour_of_day", "cat_vec"],
        outputCol="features"
    )

    input_df = assembler.transform(input_df)

    # Run prediction
    prediction_df = rf_model.transform(input_df)

    pred = prediction_df.select("prediction").collect()[0][0]
    return pred


if __name__== "__main__":
    main()