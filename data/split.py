from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, "orders.csv")

df = spark.read.csv(csv_path, header=True, sep=",")

output_path = os.path.join(BASE_DIR, "raw")


# Make sure the directory exists (Spark won't overwrite unless you tell it)
os.makedirs(output_path, exist_ok=True)


for i in range(7):
    orders_i = df.filter(col("order_dow") == i) # Filter dataFrame
    orders_i.coalesce(1).write.mode("overwrite").csv(f"{output_path}/{i}", header=True) # Save each DataFrame as a CSV


# Note that the produced CSV file names are not clean. Make sure to rename it to assignment's guidelines