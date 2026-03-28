from pyspark.sql.functions import (
    col, when, lag, broadcast
)
from src.common.functions import read_parquet, write_parquet
from src.common import paths
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

"""
Relative Strength Pipeline
--------------------------

Compares each stock’s 20-day return to SPY’s 20-day return 
and classifies the stock as outperforming, underperforming, 
or neutral, then tracks whether that relative gap is strengthening 
or weakening.

This gives a quick signal of whether individual 
stocks are gaining or losing momentum relative to the broad market trend.
"""

def build_relative_strength(spark: SparkSession) -> None:
    risk_metrics_df = (
        read_parquet(spark, paths.GOLD_RISK_METRICS_PATH)
        .select("SYMBOL","DATE", "ROLLING_RETURN_20D")
        .filter(
            (col("SYMBOL").isNotNull()) &
            (col("DATE").isNotNull())
        )
    )

    index_df = (
        read_parquet(spark, paths.SILVER_PATH)
        .select("SYMBOL", "DATE", "CLOSE_PRICE")
        .filter(
                (col("SYMBOL") == "SPY") &
                (col("DATE").isNotNull()) &
                (col("CLOSE_PRICE").isNotNull())
            )
    )

    w = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc())

    index_df = (
        index_df
            .withColumn(
                "INDEX_CLOSE_20D_AGO",
                lag("CLOSE_PRICE", 20).over(w)
            )
            .withColumn(
                "INDEX_RETURN_20D",
                when(
                    (col("INDEX_CLOSE_20D_AGO").isNull()) |
                    (col("INDEX_CLOSE_20D_AGO") == 0),
                    None
                ).otherwise(
                    (col("CLOSE_PRICE") / col("INDEX_CLOSE_20D_AGO")) - 1
                )
            ).select(
                "DATE",
                "INDEX_RETURN_20D"
            )
        )
    
    df = risk_metrics_df.join(
            broadcast(index_df), 
            on="DATE", 
            how="left"
        )
    
    df = (
        df
        .withColumn("RELATIVE_STRENGTH_20D", col("ROLLING_RETURN_20D") - col("INDEX_RETURN_20D"))
        .withColumn("RELATIVE_STRENGTH_DIRECTION",
                        when(col("RELATIVE_STRENGTH_20D").isNull(), None)
                        .when(col("RELATIVE_STRENGTH_20D") > 0, "OUTPERFORM")
                        .when(col("RELATIVE_STRENGTH_20D") < 0, "UNDERPERFORM")
                        .when(col("RELATIVE_STRENGTH_20D") == 0, "NEUTRAL")
                    )
        .withColumn("PREV_RS_20D", lag("RELATIVE_STRENGTH_20D", 1).over(w))
        .withColumn("RS_TREND", 
                        when(col("PREV_RS_20D").isNull(), None)
                        .when(col("PREV_RS_20D") < col("RELATIVE_STRENGTH_20D"), "STRENGTHENING")
                        .when(col("PREV_RS_20D") > col("RELATIVE_STRENGTH_20D"), "WEAKENING")
                        .otherwise("STABLE")
                    )
        )
    
    final_df = df.select(
        "SYMBOL",
        "DATE",
        "INDEX_RETURN_20D",
        "ROLLING_RETURN_20D",
        "RELATIVE_STRENGTH_20D",
        "RELATIVE_STRENGTH_DIRECTION",
        "RS_TREND"
    )
    write_parquet(final_df, paths.GOLD_RELATIVE_STRENGTH_PATH)
