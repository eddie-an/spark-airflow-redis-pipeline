from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import glob
import shutil


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    start_day = 0
    end_day = 6

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    input_csv_path = os.path.join(BASE_DIR, f'../../data/processed/{start_day}/*.csv')
    test_csv_path = os.path.join(BASE_DIR, "test_set")


    df = None
    if glob.glob(input_csv_path): # If there are .csv files inside the csv_path folder then create DataFrame
        df = spark.read.csv(input_csv_path, header=True, sep=",")
        print("combined day 0 file")


    for i in range(start_day+1, end_day+1):
        input_csv_path = os.path.join(BASE_DIR, f'../../data/processed/{i}/*.csv')
        if not glob.glob(input_csv_path): # If path doesn't exist, just skip it
            continue
        
        df_new = spark.read.csv(input_csv_path, header=True, sep=",")

        if df is None:
            df = df_new
        else:
            df = df.union(df_new)
        print(f"combined day {i} file")

    if df is None: # If there are no processed csv files to work with, just stop the process
        return
    
    # Train Test Data Split
    train_df, test_df = df.randomSplit([0.9, 0.1], seed=42)

    # Make sure the output directory exists (Spark won't overwrite unless you tell it)
    os.makedirs(test_csv_path, exist_ok=True)

    # Save each DataFrame as a CSV
    df.coalesce(1).write.mode("overwrite").csv(f"{test_csv_path}", header=True)

    # Assigns an arbitrary number to each category type
    indexer = StringIndexer(inputCol="category", outputCol="cat_idx")
    fitted_indexer = indexer.fit(train_df)
    train_df = fitted_indexer.transform(train_df)
    test_df = fitted_indexer.transform(test_df)

    # Numeric Columns are casted to integer
    for col in ["order_dow", "order_hour_of_day", "items"]:
        train_df = train_df.withColumn(col, train_df[col].cast("integer"))
        test_df = test_df.withColumn(col, test_df[col].cast("integer"))


    # One Hot Encoding on the category number
    encoder = OneHotEncoder(
        inputCols=["cat_idx"],
        outputCols=["cat_vec"]
    )
    fitted_encoder = encoder.fit(train_df)

    train_df = fitted_encoder.transform(train_df)
    test_df = fitted_encoder.transform(test_df)

    # Vector assembler to combine 3 columns into one array to be used in the ML model
    assembler = VectorAssembler(
        inputCols=["order_dow", "order_hour_of_day", "cat_vec"],
        outputCol="features"
    )

    train_df = assembler.transform(train_df)
    test_df = assembler.transform(test_df)

    # Random Forest
    rf = RandomForestRegressor(
        featuresCol="features", # Feature columns
        labelCol="items", # Output
        predictionCol="prediction", # Prediction
        numTrees=50,
        maxDepth=5
    )

    rf_model = rf.fit(train_df)
    predictions = rf_model.transform(test_df)

    predictions.select("items", "prediction").show(20)

    mse = predictions.withColumn(
        "sqErr", (F.col("items") - F.col("prediction"))**2
    ).agg({"sqErr": "avg"}).collect()[0][0]

    print("Test MSE =", mse)


    # Delete existing models directory if it already exists as Spark doesn't provide a way to overwrite it
    MODEL_DIR = os.path.join(BASE_DIR, "models")
    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR)

    rf_model.save(os.path.join(MODEL_DIR, "rf_model"))
    fitted_indexer.save(os.path.join(MODEL_DIR, "indexer"))
    fitted_encoder.save(os.path.join(MODEL_DIR, "encoder"))



if __name__ == "__main__":
    main()