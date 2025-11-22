from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import redis
from datetime import datetime
from zoneinfo import ZoneInfo  

def main():
    execution_timestamp = datetime.now(ZoneInfo("Canada/Mountain"))
    r = redis.Redis(host='redis', port=6379, db=0)
    exists = r.exists("processed_day")

    start_day = None
    if exists == 0:
        start_day = 0
    else:
        value = r.get('processed_day')
        start_day = (int)(value.decode('utf-8'))+1

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Directories
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(BASE_DIR, f"../../data/processed/{start_day}")
    csv_path = os.path.join(BASE_DIR, f'../../data/incremental/raw/{start_day}/orders_{start_day}.csv')

    # Make sure the output directory exists (Spark won't overwrite unless you tell it)
    os.makedirs(output_path, exist_ok=True) 

    # Get the first DataFrame
    df_combined = None
    if os.path.exists(csv_path):
        df_combined = spark.read.csv(csv_path, header=True, sep=",")

    # Get the next six DataFrames and combine them
    last_day = start_day
    for i in range(start_day+1, 7):
        csv_path = os.path.join(BASE_DIR, f'../../data/incremental/raw/{i}/orders_{i}.csv')

        if not os.path.exists(csv_path): # If file doesn't exist, skip over it
            print(f"Skipping {csv_path} — file doesn't exist.")
            continue
        
        last_day = i  # Update last day to be the last csv file (this is added to Redis)
        df_new = spark.read.csv(csv_path, header=True, sep=",")
        df_combined = df_combined.union(df_new)

    # Nothing to aggregate
    if df_combined is None:
        with open(f'{output_path}/log.txt', "a") as logFile:
            logFile.write(f'[{execution_timestamp}] Nothing has been aggregated.\n')
        return
    
    category_columns = df_combined.columns[4:]
    stack_expr = ", ".join([f"'{c}', `{c}`" for c in category_columns])

    df_final = df_combined.select(
        "order_dow",
        "order_hour_of_day",
        expr(f"stack({len(category_columns)}, {stack_expr}) as (category, items)")
    ).filter("items > 0")

    try:
        df_final.groupBy("order_dow", "order_hour_of_day", "category").count().show(50, truncate=False) # Show the final DataFrame
        df_final.coalesce(1).write.mode("append").csv(output_path, header=True) # Append to csv
        r.set("processed_day", last_day)  # runs ONLY if write succeeds

        with open(f'{output_path}/log.txt', "a") as logFile:
            logFile.write(f"[{execution_timestamp}] Incremental Aggregation ran for days {start_day}-{last_day}. "
            f"Output CSV written to {output_path}. Redis 'processed_day' updated to {last_day}.\n")
    except Exception as e:
        print("Write failed, not updating Redis:", e)


if __name__=="__main__":
    main()