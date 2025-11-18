from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

df = spark.read.csv('../../part1/orders.csv', header=True, sep=",")

output_dir = "../../part1/orders_by_dow"  # change as needed

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