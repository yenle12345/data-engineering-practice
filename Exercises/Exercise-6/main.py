from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp,avg,to_date,count,to_timestamp,month,row_number,lit,expr,max
import zipfile
import os
import shutil
from pyspark.sql.window import Window
def extract_zip(zip_dir,extract_path):
    zip_files = [f for f in os.listdir(zip_dir) if f.endswith(".zip")]
    for zip_file in zip_files:
        zip_path = os.path.join(zip_dir, zip_file)
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            

def read_csv(spark, path):
    files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.csv')]
    df1 = spark.read.option("header", "true").csv(files[0])
    df2 = spark.read.option("header", "true").csv(files[1])
    df2 = df2.withColumnRenamed("started_at", "start_time").withColumnRenamed("ended_at", "end_time").withColumnRenamed("start_station_id", "from_station_id").withColumnRenamed("start_station_name", "from_station_name").withColumnRenamed("ride_id", "trip_id").withColumnRenamed("member_casual", "usertype")
    df1_columns = set(df1.columns)
    df2_columns = set(df2.columns)
    missing_columns1 = df1_columns - df2_columns
    missing_columns2 = df2_columns - df1_columns
    for col in missing_columns2:
        df1 = df1.withColumn(col, lit(None))
    for col in missing_columns1:
        df2 = df2.withColumn(col, lit(None))

    df = df1.unionByName(df2)
    return df




def avg_trip_duration(df):

    df = df.withColumn("start_time", unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("end_time", unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("trip_duration", (col("end_time").cast("long") - col("start_time").cast("long")) / 60)
    df = df.filter(col("tripduration").isNotNull())
    return df.groupBy("start_time").agg(avg("trip_duration").alias("avg_trip_duration"))
    
def count_trip_day(df):
    df = df.withColumn("date",to_date(to_timestamp(col("start_time"),"yyyy-MM-dd HH:mm:ss")))
    return df.groupBy("date").agg(count("trip_id").alias("trip_count"))

def from_station_month(df):
    df = df.withColumn("month",month(to_timestamp(col("start_time"),"yyyy-MM-dd HH:mm:ss")))
    count_id_month = df.groupBy("month","from_station_id").agg(count("*").alias("count_station"))
    window_spec = Window.partitionBy("month").orderBy(col("count_station").desc())
    ranked = count_id_month.withColumn("rank", row_number().over(window_spec))

    return ranked.filter(col("rank") == 1).select("month", "from_station_id", "count_station")

def top3_station_2wweeks(df):
    df = df.withColumn("timestamp", to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"))
    df = df.filter(expr("timestamp >= current_timestamp() - INTERVAL 14 DAYS"))
    df = df.withColumn("date", to_date(df.timestamp))
    station_counts = df.groupBy("date", "from_station_id").agg(count("*").alias("count_station"))
    window_spec = Window.partitionBy("date").orderBy(col("count_station").desc())
    ranked_stations = station_counts.withColumn("rank",row_number().over(window_spec))
    top_3_stations_per_day = ranked_stations.filter(col("rank") <= 3)
    return top_3_stations_per_day.select("date", "from_station_id", "count_station")

def max_male_female(df):
    df = df.groupBy("gender").agg(count("*").alias("count"))
    return df.orderBy("count", ascending=False).limit(1)

def top10_ages_longest_shortest(df):
    df = df.dropna(subset=["start_time", "end_time", "birthyear"])
    df = df.withColumn("start_time", unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("end_time", unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("trip_duration", col("end_time") - col("start_time"))
    top_longest = df.orderBy("trip_duration", ascending=False).select("birthyear", "trip_duration").limit(10)
    top_shortest = df.orderBy("trip_duration", ascending=True).select("birthyear", "trip_duration").limit(10)
    return top_longest.unionByName(top_shortest)

def save_csv(temp_path,path_final):
    for filename in os.listdir(temp_path):
        if filename.startswith("part-") and filename.endswith(".csv"):
            src_path = os.path.join(temp_path, filename)
            shutil.move(src_path,path_final)
            break
    shutil.rmtree(temp_path)

def main():
    spark = SparkSession.builder.appName("exercise-6").getOrCreate()
    
    temp_path = "/app/reports/temp_path"
    
    zip_dir = "/app/data"
    extract_dir = "/app/csv"
    extract_zip(zip_dir,extract_dir)

    df = read_csv(spark, extract_dir)

    rows = df.collect()
    for row in rows[-5:]:
        print(row)
    avg = avg_trip_duration(df)
    avg.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/avg_trip_duration.csv")

    count = count_trip_day(df)
    count.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/count_trip_day.csv")

    from_station_month(df).coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/max_from_station_month.csv")
    
    top3_station_2wweeks(df).coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/top3_station_2wweek.csv")
    
    max_male_female(df).coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/max_male_female.csv")

    top10_ages_longest_shortest(df).coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    save_csv(temp_path,"/app/reports/top10.csv")

if __name__ == "__main__":
    main()
