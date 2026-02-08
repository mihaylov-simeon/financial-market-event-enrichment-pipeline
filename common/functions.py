from pyspark.sql import SparkSession, DataFrame
import os

def spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", False)
        .csv(path)
    )

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)