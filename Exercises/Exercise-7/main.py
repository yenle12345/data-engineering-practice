from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import hashlib
import zipfile
import os


def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    return os.path.join(extract_to, zip_ref.namelist()[0])


def read_csv_from_zip(spark, zip_path, extract_to="tmp"):
    csv_file_path = unzip_file(zip_path, extract_to)
    df = spark.read.option("header", True).csv(csv_file_path)
    source_file = os.path.basename(zip_path)
    df = df.withColumn("source_file", F.lit(source_file))
    return df


def extract_file_date(df):
    df = df.withColumn(
        "file_date",
        F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1))
    )
    return df


def extract_brand(df):
    df = df.withColumn(
        "brand",
        F.when(F.instr("model", " ") > 0, F.split("model", " ").getItem(0))
         .otherwise(F.lit("unknown"))
    )
    return df


def add_storage_ranking(df):
    capacity_ranking = (df
                        .select("model", "capacity_bytes")
                        .distinct()
                        .withColumn("capacity_bytes", F.col("capacity_bytes").cast("bigint"))
                        .withColumn("storage_ranking",F.dense_rank().over(Window.orderBy(F.col("capacity_bytes").desc())))
                        )
    df = df.join(capacity_ranking.select("model", "storage_ranking"), on="model", how="left")
    return df


def generate_primary_key(df):
    unique_cols = ["date", "serial_number", "model"]
    concat_cols = F.concat_ws("||", *[F.col(c) for c in unique_cols])
    df = df.withColumn("primary_key", F.sha2(concat_cols, 256))
    return df


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    
    # Path to the zip file
    zip_path = "data/hard-drive-2022-01-01-failures.csv.zip"
    
    df = read_csv_from_zip(spark, zip_path)
    df = extract_file_date(df)
    df = extract_brand(df)
    df = add_storage_ranking(df)
    df = generate_primary_key(df)
    df.head(10)


if __name__ == "__main__":
    main()
