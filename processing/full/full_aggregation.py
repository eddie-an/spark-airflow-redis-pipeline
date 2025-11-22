from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark = (
#     SparkSession.builder
#         .appName("FullAggregation")
#         .master("spark://spark-master:7077")
#         .config("spark.driver.bindAddress", "0.0.0.0")
#         .config("spark.driver.host", "spark-master")
#         .getOrCreate()
# )

def loadCSV():
    csv_path = os.path.join(BASE_DIR, f'../../data/raw/0/orders_0.csv')
    
    # Get the first DataFrame
    df_combined = None
    if os.path.exists(csv_path):
        df_combined = spark.read.csv(csv_path, header=True, sep=",")

    # Get the next six DataFrames and combine them
    for i in range(1, 7):
        csv_path = os.path.join(BASE_DIR, f'../../data/raw/{i}/orders_{i}.csv')
        
        if not os.path.exists(csv_path): # If file doesn't exist, skip over it
            print(f"Skipping {csv_path} — file doesn't exist.")
            continue
        
        df_new = spark.read.csv(csv_path, header=True, sep=",")
        df_combined = df_combined.union(df_new)
    return df_combined

def processCSV(df):
    category_columns = df.columns[4:]
    stack_expr = ", ".join([f"'{c}', `{c}`" for c in category_columns])

    df_final = df.select(
        "order_dow",
        "order_hour_of_day",
        expr(f"stack({len(category_columns)}, {stack_expr}) as (category, items)")
    ).filter("items > 0")
    return df_final

def saveCSV(df):
    # Make sure the output directory exists (Spark won't overwrite unless you tell it)
    output_path = os.path.join(BASE_DIR, "../../data/processed")
    os.makedirs(output_path, exist_ok=True)

    # Save each DataFrame as a CSV
    df.coalesce(1).write.mode("overwrite").csv(f"{output_path}", header=True)

def main():
    df = loadCSV() # Load CSV files

    # If there is nothing to aggregate
    if df is None:
        print(f"The files don't exist")
        return

    df_processed = processCSV(df) # Process CSV so that the columns are changed

    # Show the final DataFrame
    df_processed.groupBy("order_dow", "order_hour_of_day", "category").count().show(50, truncate=False)


    saveCSV(df_processed)


if __name__== "__main__":
    main()