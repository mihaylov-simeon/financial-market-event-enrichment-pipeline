from pyspark.sql.functions import (
    col, when, avg, countDistinct
)
from pyspark.sql import SparkSession
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Market Breadth Pipeline
-----------------------

Summarizes overall market conditions for each trading day by aggregating
stock-level metrics across all symbols.

The pipeline measures how broadly market movements are distributed by
calculating the percentage of stocks moving up or down, outperforming
the market, strengthening in relative performance, and the average
market volatility.

These indicators are then combined into a daily market breadth regime
classification describing whether the market environment is bullish,
neutral, or bearish.

"""

def build_market_breadth(spark: SparkSession) -> None:

    daily_price_metrics = (
        read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
        .select("SYMBOL", "DATE", "DAILY_DIRECTION")
        .filter(
                (col("SYMBOL").isNotNull()) &
                (col("DATE").isNotNull())
            )
    )
    risk_metrics = (
        read_parquet(spark, paths.GOLD_RISK_METRICS_PATH)
        .select("SYMBOL", "DATE", "ROLLING_VOLATILITY_20D")
        .filter(
                (col("SYMBOL").isNotNull()) &
                (col("DATE").isNotNull())
            )
    )
    relative_strength = (
        read_parquet(spark, paths.GOLD_RELATIVE_STRENGTH_PATH)
        .select("SYMBOL", "DATE", "RELATIVE_STRENGTH_DIRECTION", "RS_TREND")
        .filter(
                (col("SYMBOL").isNotNull()) &
                (col("DATE").isNotNull())
            )
    )

    join_key = ["SYMBOL", "DATE"]

    df = (
        daily_price_metrics
        .join(risk_metrics, on=join_key, how="left")
        .join(relative_strength, on=join_key, how="left")
        )

    df = (
        df
        .groupBy("DATE")
        .agg(
            countDistinct("SYMBOL").alias("TOTAL_STOCKS"),
            avg(
                when(
                    col("DAILY_DIRECTION") == "UP", 1)
                    .otherwise(0)
                ).alias("PCT_STOCKS_UP"),
            avg(
                when(
                    col("DAILY_DIRECTION") == "DOWN", 1)
                    .otherwise(0)
                ).alias("PCT_STOCKS_DOWN"),
            avg(when(
                    col("RELATIVE_STRENGTH_DIRECTION") == "OUTPERFORM", 1)
                    .otherwise(0)
                ).alias("PCT_STOCKS_OUTPERFORMING"),
            avg(
                when(
                    col("RS_TREND") == "STRENGTHENING", 1)
                    .otherwise(0)
                ).alias("PCT_RS_STRENGTHENING"),
            avg(
                    col("ROLLING_VOLATILITY_20D")
                ).alias("AVG_MARKET_VOLATILITY")
        )
        .withColumn(
            "BREADTH_REGIME",
            when(col("PCT_STOCKS_UP") >= 0.70, "STRONG_BULL")
            .when(col("PCT_STOCKS_UP") >= 0.50, "BULL")
            .when(col("PCT_STOCKS_UP") >= 0.40, "NEUTRAL")
            .when(col("PCT_STOCKS_UP") >= 0.25, "BEAR")
            .otherwise("STRONG_BEAR")
        )
    )

    final_df = (
        df
        .select(
            "DATE",
            "TOTAL_STOCKS",
            "PCT_STOCKS_UP",
            "PCT_STOCKS_DOWN",
            "PCT_STOCKS_OUTPERFORMING",
            "PCT_RS_STRENGTHENING",
            "AVG_MARKET_VOLATILITY",
            "BREADTH_REGIME"
        )
    )

    write_parquet(final_df, paths.GOLD_MARKET_BREADTH_PATH)
