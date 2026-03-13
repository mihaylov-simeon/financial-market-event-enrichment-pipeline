from pyspark.sql.functions import (
    col, when, avg, lag
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Market Regime Pipeline
----------------------

Identifies the prevailing price trend and volatility environment.

This pipeline classifies whether a stock is operating in an upward,
downward, or neutral trend and evaluates the broader volatility
conditions influencing price behavior.
"""

def build_market_regime(spark: SparkSession) -> None:
    silver_df = (
        read_parquet(
            spark,
            paths.SILVER_PATH
        ).select(
            "SYMBOL",
            "DATE",
            "OPEN_PRICE",
            "CLOSE_PRICE",
        )
    )

    daily_price_metrics = (
        read_parquet(
            spark,
            paths.GOLD_DAILY_PRICE_METRICS_PATH
        ).select(
            "SYMBOL",
            "DATE",
            "INTRADAY_RANGE_PCT",
            "20_DAY_AVG_INTRADAY_RANGE_PCT",
        ).filter(
            (col("SYMBOL").isNotNull()) &
            (col("DATE").isNotNull())
        )
    )

    df = silver_df.join(
        daily_price_metrics,
        on=["SYMBOL", "DATE"],
        how="left")

    w = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc())
    thirty_days_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-30, -1)
    hundred_days_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-100, -1)
    six_months_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-180, -1)

    df = (
        df
        .withColumn("30_DAYS_AVG_SMA", avg("CLOSE_PRICE").over(thirty_days_window))
        .withColumn("100_DAYS_AVG_SMA", avg("CLOSE_PRICE").over(hundred_days_window))
        .withColumn("6_MONTHS_AVG_SMA", avg("CLOSE_PRICE").over(six_months_window))
        .withColumn("5_DAY_TREND_SLOPE", (
            col("30_DAYS_AVG_SMA") - lag("30_DAYS_AVG_SMA", 5).over(w)
            ) / 5
        )
        .withColumn("10_DAY_TREND_SLOPE", (
            col("30_DAYS_AVG_SMA") - lag("30_DAYS_AVG_SMA", 10).over(w)
            ) / 10
        )
        .withColumn("30_DAY_TREND_SLOPE", (
            col("30_DAYS_AVG_SMA") - lag("30_DAYS_AVG_SMA", 30).over(w)
            ) / 30
        )
        .withColumn("VOLATILITY_REGIME_FLAG", 
                    when((col("INTRADAY_RANGE_PCT").isNull())
                           | (col("INTRADAY_RANGE_PCT") == 0)
                           | (col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull())
                           | (col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0),
                           None)
                           .otherwise(
                               when(col("INTRADAY_RANGE_PCT") > 1.5 * col("20_DAY_AVG_INTRADAY_RANGE_PCT"), "HIGH")
                               .when(col("INTRADAY_RANGE_PCT") < 0.7 * col("20_DAY_AVG_INTRADAY_RANGE_PCT"), "LOW")
                               .otherwise("NORMAL")
                           )
        )
    )

    final_df = df.select(
        "SYMBOL",
        "DATE",
        "30_DAYS_AVG_SMA",
        "100_DAYS_AVG_SMA",
        "6_MONTHS_AVG_SMA",
        "5_DAY_TREND_SLOPE",
        "10_DAY_TREND_SLOPE",
        "30_DAY_TREND_SLOPE",
        "VOLATILITY_REGIME_FLAG",
    )

    write_parquet(final_df, paths.GOLD_MARKET_REGIME_PATH)
