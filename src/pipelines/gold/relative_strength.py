import pyspark.sql.functions as F
from src.common.functions import read_parquet, write_parquet
from src.common import paths
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

def build_relative_strength(spark: SparkSession) -> None:
    risk_metrics_df = (
        read_parquet(spark, paths.GOLD_RISK_METRICS)
        .select("SYMBOL","DATE", "ROLLING_RETURN_20D")
        .filter(
            (F.col("SYMBOL").isNotNull()) &
            (F.col("DATE").isNotNull())
        )
    )

    index_df = (
        read_parquet(spark, paths.SILVER_PATH)
        .select("SYMBOL", "DATE", "CLOSE_PRICE")
        .filter(
                (F.col("SYMBOL") == "SPY") &
                (F.col("DATE").isNotNull()) &
                (F.col("CLOSE_PRICE").isNotNull())
            )
    )

    w = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc())
    lag_window = Window.orderBy(F.col("DATE").asc())

    index_df = (
        index_df
            .withColumn(
                "INDEX_CLOSE_20D_AGO",
                F.lag("CLOSE_PRICE", 20).over(lag_window)
            )
            .withColumn(
                "INDEX_RETURN_20D",
                F.when(
                    (F.col("INDEX_CLOSE_20D_AGO").isNull()) |
                    (F.col("INDEX_CLOSE_20D_AGO") == 0),
                    None
                ).otherwise(
                    (F.col("CLOSE_PRICE") / F.col("INDEX_CLOSE_20D_AGO")) - 1
                )
            ).select(
                "DATE",
                "INDEX_RETURN_20D"
            )
        )
    
    df = risk_metrics_df.join(
            F.broadcast(index_df), 
            on="DATE", 
            how="left"
        )
    
    df = (
        df
        .withColumn("RELATIVE_STRENGTH_20D", F.col("ROLLING_RETURN_20D") - F.col("INDEX_RETURN_20D"))
        .withColumn("RELATIVE_STRENGTH_DIRECTION",
                        F.when(F.col("RELATIVE_STRENGTH_20D").isNull(), None)
                        .when(F.col("RELATIVE_STRENGTH_20D") > 0, "OUTPERFORM")
                        .when(F.col("RELATIVE_STRENGTH_20D") < 0, "UNDERPERFORM")
                        .when(F.col("RELATIVE_STRENGTH_20D") == 0, "NEUTRAL")
                    )
        .withColumn("PREV_RS_20D", F.lag("RELATIVE_STRENGTH_20D", 1).over(w))
        .withColumn("RS_TREND", 
                        F.when(F.col("PREV_RS_20D").isNull(), None)
                        .when(F.col("PREV_RS_20D") < F.col("RELATIVE_STRENGTH_20D"), "STRENGTHENING")
                        .when(F.col("PREV_RS_20D") > F.col("RELATIVE_STRENGTH_20D"), "WEAKENING")
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
    write_parquet(final_df, paths.GOLD_RELATIVE_STRENGTH)
