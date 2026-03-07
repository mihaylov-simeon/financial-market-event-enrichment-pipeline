import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Risk Metrics Pipeline
---------------------

Calculates short-term risk characteristics of each stock.

This pipeline measures price instability (volatility), recent
declines (drawdown), and return efficiency relative to risk.

The resulting metrics provide a stability layer that helps determine
whether recent price performance occurred under stable or unstable
market conditions.
"""

def build_risk_metrics(spark: SparkSession) -> None:

    silver_df = (
        read_parquet(spark, paths.SILVER_PATH)
        .select("SYMBOL", "DATE", "CLOSE_PRICE")
        .filter(
            (F.col("SYMBOL").isNotNull()) &
            (F.col("DATE").isNotNull()) &
            (F.col("CLOSE_PRICE").isNotNull())
        )
    )

    daily_price_metrics = (
        read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
        .select("SYMBOL", "DATE", "DAILY_RETURN_PCT")
        .filter(
            (F.col("SYMBOL").isNotNull()) &
            (F.col("DATE").isNotNull())
        )
    )

    df = (
        silver_df
        .join(daily_price_metrics, on=["SYMBOL", "DATE"], how="left")
    )

    # Define 20-day rolling window
    twenty_day_window = (
        Window
        .partitionBy("SYMBOL")
        .orderBy(F.col("DATE").asc())
        .rowsBetween(-20, -1)
    )

    lag_window = (
        Window
        .partitionBy("SYMBOL")
        .orderBy(F.col("DATE").asc())
    )

    # Rolling Volatility
    df = (
        df
        .withColumn(
            "CLOSE_PRICE_20D",
            F.lag("CLOSE_PRICE", 20).over(lag_window)
        )
        .withColumn(
            "AVG_RETURN_20D",
            F.avg("DAILY_RETURN_PCT").over(twenty_day_window)
        )
        .withColumn(
            "VARIANCE_20D",
            F.avg(
                F.pow(F.col("DAILY_RETURN_PCT") - F.col("AVG_RETURN_20D"), 2)
            ).over(twenty_day_window)
        )
        .withColumn(
            "ROLLING_VOLATILITY_20D",
            F.sqrt("VARIANCE_20D")
        )
    )

    # Rolling Max Drawdown
    df = (
        df
        .withColumn(
            "ROLLING_MAX_CLOSE_20D",
            F.max("CLOSE_PRICE").over(twenty_day_window)
        )
        .withColumn(
            "DRAWDOWN_TODAY",
            (F.col("CLOSE_PRICE") / F.col("ROLLING_MAX_CLOSE_20D")) - 1
        )
        .withColumn(
            "MAX_DRAWDOWN_LOOKBACK",
            F.min("DRAWDOWN_TODAY").over(twenty_day_window)
        )
        .withColumn(
            "ROLLING_RETURN_20D",
            (F.col("CLOSE_PRICE") / F.col("CLOSE_PRICE_20D")) - 1
        )
        .withColumn(
            "RISK_ADJUSTED_RETURN_20D",
            F.when(
                F.col("ROLLING_VOLATILITY_20D") > 0,
                F.col("ROLLING_RETURN_20D") / F.col("ROLLING_VOLATILITY_20D")
            )
        )
    )

    final_df = (
        df
        .select(
            "SYMBOL",
            "DATE",
            "ROLLING_VOLATILITY_20D",
            "MAX_DRAWDOWN_LOOKBACK",
            "ROLLING_RETURN_20D",
            "RISK_ADJUSTED_RETURN_20D",
        )
    )

    write_parquet(final_df, paths.GOLD_RISK_METRICS)