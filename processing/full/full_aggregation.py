from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# spark = SparkSession.builder.master("local[*]").getOrCreate()

spark = ( # Delete this later
    SparkSession.builder
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(BASE_DIR, "../../data/processed")
csv_path = os.path.join(BASE_DIR, f'../../data/raw/0/orders_0.csv')

# Make sure the directory exists (Spark won't overwrite unless you tell it)
os.makedirs(output_path, exist_ok=True) 

# Get the first DataFrame
df_combined = spark.read.csv(csv_path, header=True, sep=",")

# Get the next six DataFrames and combine them
for i in range(1, 7):
    csv_path = os.path.join(BASE_DIR, f'../../data/raw/{i}/orders_{i}.csv')
    df_new = spark.read.csv(csv_path, header=True, sep=",")
    df_combined = df_combined.union(df_new)


category_columns = df_combined.columns[4:]
stack_expr = ", ".join([f"'{c}', `{c}`" for c in category_columns])

df_final = df_combined.select(
    "order_dow",
    "order_hour_of_day",
    expr(f"stack({len(category_columns)}, {stack_expr}) as (category, items)")
).filter("items > 0")

# Show the final DataFrame
df_final.groupBy("order_dow", "order_hour_of_day", "category").count().show(50, truncate=False)


# Save each DataFrame as a CSV
df_final.coalesce(1).write.mode("overwrite").csv(f"{output_path}", header=True)
