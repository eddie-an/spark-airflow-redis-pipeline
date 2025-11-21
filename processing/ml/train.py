from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, "../../data/processed/orders.csv")

df = spark.read.csv(csv_path, header=True, sep=",")

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

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