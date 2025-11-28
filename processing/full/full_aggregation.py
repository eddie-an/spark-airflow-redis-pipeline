from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import glob


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

def loadCSV(path):
    
    # Get the first DataFrame
    df_combined = None
    if glob.glob(path):
        df_combined = spark.read.csv(path, header=True, sep=",")

    # Get the next six DataFrames and combine them
    for i in range(1, 7):
        path = os.path.join(BASE_DIR, f'../../data/raw/{i}/*.csv')
        
        if not glob.glob(path): # If file doesn't exist, skip over it
            print(f"Skipping {path} — file doesn't exist.")
            continue
        
        df_new = spark.read.csv(path, header=True, sep=",")
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

def saveCSV(df, path):
    # Make sure the output directory exists (Spark won't overwrite unless you tell it)
    os.makedirs(path, exist_ok=True)

    # Save each DataFrame as a CSV
    df.coalesce(1).write.mode("overwrite").csv(f"{path}", header=True)

def main():
    csv_path = os.path.join(BASE_DIR, f'../../data/raw/0/*.csv')
    output_path = os.path.join(BASE_DIR, "../../data/processed")

    df = loadCSV(csv_path) # Load CSV files
    # If there is nothing to aggregate
    if df is None:
        print(f"The files don't exist")
        return

    df_processed = processCSV(df) # Process CSV so that the columns are changed
    saveCSV(df_processed, output_path)


if __name__== "__main__":
    main()