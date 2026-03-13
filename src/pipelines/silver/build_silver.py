from pyspark.sql.functions import (
    col, to_date
)
from pyspark.sql import SparkSession
from src.common.functions import read_parquet, write_parquet
from src.common import paths


def build_silver(spark: SparkSession) -> None:
    dividends = read_parquet(spark, paths.BRONZE_DIVIDENDS_PARQUET).select(
        col("symbol").alias("SYMBOL"),
        to_date("date").alias("DATE"),
        col("dividend").cast("double").alias("DIVIDENDS"),
    )

    earnings = read_parquet(spark, paths.BRONZE_EARNINGS_PARQUET).select(
        col("symbol").alias("SYMBOL"),
        to_date("date").alias("DATE"),
        col("qtr").alias("QUARTER"),
        col("eps_est").cast("double").alias("EPS_ESTIMATE"),
        col("eps").cast("double").alias("EPS"),
        col("release_time").alias("RELEASE_TIME"),
    )

    prices = read_parquet(spark, paths.BRONZE_PRICES_PARQUET).select(
        col("symbol").alias("SYMBOL"),
        to_date("date").alias("DATE"),
        col("open").cast("double").alias("OPEN_PRICE"),
        col("high").cast("double").alias("HIGH_PRICE"),
        col("low").cast("double").alias("LOW_PRICE"),
        col("close").cast("double").alias("CLOSE_PRICE"),
        col("close_adjusted").cast("double").alias("CLOSE_ADJUSTED_PRICE"),
        col("volume").cast("long").alias("VOLUME"),
        col("split_coefficient").cast("double").alias("SPLIT_COEFFICIENT"),
    )

    silver_df = prices.join(earnings, ["SYMBOL", "DATE"], "left").join(
        dividends, ["SYMBOL", "DATE"], "left"
    )

    # coalesce avoids the big shuffle that was causing your local OOM during write
    write_parquet(silver_df, paths.SILVER_PATH, partitions=4)
