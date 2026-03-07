import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from src.common.functions import read_parquet, write_parquet
from src.common import paths

def market_breadth(spark: SparkSession) -> None:

    daily_price_metrics = (
        read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
        .select("SYMBOL", "DATE", "DAILY_DIRECTION")
        .filter(
                (F.col("SYMBOL").isNotNull()) &
                (F.col("DATE").isNotNull())
            )
    )
    risk_metrics = (
        read_parquet(spark, paths.GOLD_RISK_METRICS_PATH)
        .select("SYMBOL", "DATE", "ROLLING_VOLATILITY_20D")
        .filter(
                (F.col("SYMBOL").isNotNull()) &
                (F.col("DATE").isNotNull())
            )
    )
    relative_strength = (
        read_parquet(spark, paths.GOLD_RELATIVE_STRENGTH_PATH)
        .select("SYMBOL", "DATE", "RELATIVE_STRENGTH_DIRECTION", "RS_TREND")
        .filter(
                (F.col("SYMBOL").isNotNull()) &
                (F.col("DATE").isNotNull())
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
            F.countDistinct("SYMBOL").alias("TOTAL_STOCKS"),
            F.avg(F.when(F.col("DAILY_DIRECTION") == "UP", 1).otherwise(0)).alias("PCT_STOCKS_UP"),
            F.avg(F.when(F.col("DAILY_DIRECTION") == "DOWN", 1).otherwise(0)).alias("PCT_STOCKS_DOWN"),
            F.avg(F.when(F.col("RELATIVE_STRENGTH_DIRECTION") == "OUTPERFORM", 1).otherwise(0)).alias("PCT_STOCKS_OUTPERFORMING"),
            F.avg(F.when(F.col("RS_TREND") == "STRENGTHENING", 1).otherwise(0)).alias("PCT_RS_STRENGTHENING"),
            F.avg(F.col("ROLLING_VOLATILITY_20D")).alias("AVG_MARKET_VOLATILITY")
        )
        .withColumn(
            "BREADTH_REGIME",
            F.when(F.col("PCT_STOCKS_UP") >= 0.70, "STRONG_BULL")
            .when(F.col("PCT_STOCKS_UP") >= 0.50, "BULL")
            .when(F.col("PCT_STOCKS_UP") >= 0.40, "NEUTRAL")
            .when(F.col("PCT_STOCKS_UP") >= 0.25, "BEAR")
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
