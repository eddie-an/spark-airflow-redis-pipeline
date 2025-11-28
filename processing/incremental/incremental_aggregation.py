from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import glob
import redis
from datetime import datetime

spark = SparkSession.builder.master("local[*]").getOrCreate() # Connect to Spark
r = redis.Redis(host='redis', port=6379, db=0) # Connect to Redis

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    execution_timestamp = datetime.now()
    start_day = None
    
    if r.exists("processed_day") == 0: # If key doesn't exist in Redis, start from day 0
        start_day = 0
    else:
        value = r.get('processed_day') # Get last processed day from Redis
        start_day = (int)(value.decode('utf-8'))+1 # Start from next day
    
    # Directories
    output_log_path = os.path.join(BASE_DIR, f"../../data/processed")
    input_csv_path = os.path.join(BASE_DIR, f'../../data/incremental/raw/{start_day}/*.csv')
    output_csv_path = os.path.join(BASE_DIR, f"../../data/processed/{start_day}")

    (df_combined, last_day) = loadCSV(input_csv_path, start_day)

    # If nothing to aggregate
    if df_combined is None:
        with open(f'{output_log_path}/log.txt', "a") as logFile:
            logFile.write(f'[{execution_timestamp}] Nothing has been aggregated.\n')
        return
    
    df_final = processCSV(df_combined) # Process CSV so that the columns are changed

    try:
        saveCSV(df_final, output_csv_path)
        r.set("processed_day", last_day)  # runs ONLY if write succeeds
        with open(f'{output_log_path}/log.txt', "a") as logFile:
            logFile.write(f"[{execution_timestamp}] Incremental Aggregation ran for days {start_day}-{last_day}. "
            f"Output CSV written to {output_csv_path}. Redis 'processed_day' updated to {last_day}.\n")
    except Exception as e:
        print("Write failed, not updating Redis:", e)

def loadCSV(path, start_day):
    # Get the first DataFrame
    df_combined = None
    if glob.glob(path):
        df_combined = spark.read.csv(path, header=True, sep=",")

    end_day = start_day
    # Get the next six DataFrames and combine them
    for i in range(start_day+1, 7):
        path = os.path.join(BASE_DIR, f'../../data/incremental/raw/{i}/*.csv')

        if not glob.glob(path): # If file doesn't exist, skip over it
            print(f"Skipping {path} — file doesn't exist.")
            continue
        
        end_day = i  # Update last day to be the last csv file (this is added to Redis)
        df_new = spark.read.csv(path, header=True, sep=",")
        df_combined = df_combined.union(df_new)

    return (df_combined, end_day)

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


if __name__== "__main__":
    main()