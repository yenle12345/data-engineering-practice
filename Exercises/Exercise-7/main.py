from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
import hashlib

def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(file_path)
    return df.withColumn("source_file", F.input_file_name())
    
def extract_file_date(df: DataFrame) -> DataFrame:
    # Extract date using regex and convert to timestamp
    df = df.withColumn("file_date_str", F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1))
    return df.withColumn("file_date", F.to_date("file_date_str", "yyyy-MM-dd")).drop("file_date_str")

def add_brand_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "brand",
        F.when(F.instr("model", " ") > 0, F.split("model", " ").getItem(0)).otherwise(F.lit("unknown"))
    )

def add_storage_ranking(df: DataFrame) -> DataFrame:
    model_capacity_df = df.groupBy("model").agg(F.max("capacity_bytes").alias("max_capacity_bytes"))
    window = Window.orderBy(F.desc("max_capacity_bytes"))
    ranked_df = model_capacity_df.withColumn("storage_ranking", F.dense_rank().over(window))

    return df.join(ranked_df.select("model", "storage_ranking"), on="model", how="left")

def add_primary_key(df: DataFrame) -> DataFrame:
    concat_cols = F.concat_ws("||", *df.columns)
    return df.withColumn("primary_key", F.sha2(concat_cols, 256))

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    file_path = "path/to/your/file.csv"  # replace with actual path
    df = read_data(spark, file_path)
    df = extract_file_date(df)
    df = add_brand_column(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)
    df.show(truncate=False)

if __name__ == "__main__":
    main()
