from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

df = spark.read.csv('orders.csv', header=True, sep=",")

output_dir = "./data/raw"

# Make sure the directory exists (Spark won't overwrite unless you tell it)
os.makedirs(output_dir, exist_ok=True)


orders_0 = df.filter(col("order_dow") == 0)
orders_1 = df.filter(col("order_dow") == 1)
orders_2 = df.filter(col("order_dow") == 2)
orders_3 = df.filter(col("order_dow") == 3)
orders_4 = df.filter(col("order_dow") == 4)
orders_5 = df.filter(col("order_dow") == 5)
orders_6 = df.filter(col("order_dow") == 6)
orders_6.show(5)

# Save each DataFrame as a CSV
orders_0.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/0/orders_0", header=True)
orders_1.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/1/orders_1", header=True)
orders_2.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/2/orders_2", header=True)
orders_3.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/3/orders_3", header=True)
orders_4.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/4/orders_4", header=True)
orders_5.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/5/orders_5", header=True)
orders_6.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/6/orders_6", header=True)
