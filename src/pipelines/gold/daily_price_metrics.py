from pyspark.sql.functions import (
    col, lag, when, avg
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Daily Price Metrics Pipeline
----------------------------

Computes fundamental daily price behavior metrics.

This pipeline measures price movement, gaps, and intraday volatility
relative to previous trading sessions, providing a baseline description
of how the stock price behaves day-to-day.
"""

def build_daily_price_metrics(spark: SparkSession) -> None:
    silver_df = (
            read_parquet(
            spark, 
            paths.SILVER_PATH)
            .select(
                "SYMBOL",
                "DATE",
                "CLOSE_PRICE",
                "OPEN_PRICE",
                "HIGH_PRICE",
                "LOW_PRICE",
            )
        )

    w = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc())
    twenty_days_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-20, -1)

    df = (
        silver_df
        .withColumn("PREV_CLOSE_PRICE", lag("CLOSE_PRICE").over(w))
        .withColumn(
            "DAILY_RETURN_PCT",
            when(
                col("PREV_CLOSE_PRICE").isNull() | (col("PREV_CLOSE_PRICE") == 0),
                None
            ).otherwise(
                ((col("CLOSE_PRICE") - col("PREV_CLOSE_PRICE")) / col("PREV_CLOSE_PRICE")) * 100
            )
        )
        .withColumn("GAP_AMOUNT", col("OPEN_PRICE") - col("PREV_CLOSE_PRICE"))
        .withColumn(
            "GAP_DIRECTION",
            when(col("OPEN_PRICE") > col("PREV_CLOSE_PRICE"), "UP")
             .when(col("OPEN_PRICE") < col("PREV_CLOSE_PRICE"), "DOWN")
             .otherwise("NO_GAP")
        )
        .withColumn(
            "DAILY_DIRECTION",
            when(col("CLOSE_PRICE") > col("PREV_CLOSE_PRICE"), "UP")
             .when(col("CLOSE_PRICE") < col("PREV_CLOSE_PRICE"), "DOWN")
             .otherwise("FLAT")
        )
        .withColumn("INTRADAY_RANGE_AMT", col("HIGH_PRICE") - col("LOW_PRICE"))
        .withColumn(
            "INTRADAY_RANGE_PCT",
            when(col("CLOSE_PRICE").isNull() | (col("CLOSE_PRICE") == 0), None)
             .otherwise((col("INTRADAY_RANGE_AMT") / col("CLOSE_PRICE")) * 100)
        )
        .withColumn(
            "20_DAY_AVG_INTRADAY_RANGE_PCT",
            avg("INTRADAY_RANGE_PCT").over(twenty_days_window)
        )
    )

    df = df.select(
        "SYMBOL",
        "DATE",
        "PREV_CLOSE_PRICE",
        "DAILY_RETURN_PCT",
        "GAP_AMOUNT",
        "GAP_DIRECTION",
        "DAILY_DIRECTION",
        "INTRADAY_RANGE_AMT",
        "INTRADAY_RANGE_PCT",
        "20_DAY_AVG_INTRADAY_RANGE_PCT"
    )

    write_parquet(df, paths.GOLD_DAILY_PRICE_METRICS_PATH, partitions=4)
