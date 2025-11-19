from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, "orders.csv")

df = spark.read.csv(csv_path, header=True, sep=",")

output_path = os.path.join(BASE_DIR, "data/raw")


# Make sure the directory exists (Spark won't overwrite unless you tell it)
os.makedirs(output_path, exist_ok=True)


orders_0 = df.filter(col("order_dow") == 0)
orders_1 = df.filter(col("order_dow") == 1)
orders_2 = df.filter(col("order_dow") == 2)
orders_3 = df.filter(col("order_dow") == 3)
orders_4 = df.filter(col("order_dow") == 4)
orders_5 = df.filter(col("order_dow") == 5)
orders_6 = df.filter(col("order_dow") == 6)

# Save each DataFrame as a CSV
orders_0.coalesce(1).write.mode("overwrite").csv(f"{output_path}/0", header=True)
orders_1.coalesce(1).write.mode("overwrite").csv(f"{output_path}/1", header=True)
orders_2.coalesce(1).write.mode("overwrite").csv(f"{output_path}/2", header=True)
orders_3.coalesce(1).write.mode("overwrite").csv(f"{output_path}/3", header=True)
orders_4.coalesce(1).write.mode("overwrite").csv(f"{output_path}/4", header=True)
orders_5.coalesce(1).write.mode("overwrite").csv(f"{output_path}/5", header=True)
orders_6.coalesce(1).write.mode("overwrite").csv(f"{output_path}/6", header=True)


# Note that the produced CSV file names are not clean. Make sure to rename it to assignment's guidelines