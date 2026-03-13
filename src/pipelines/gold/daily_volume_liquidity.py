from pyspark.sql.functions import (
    col, when, lag, avg
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Daily Volume & Liquidity Pipeline
---------------------------------

Analyzes trading participation and liquidity conditions.

This pipeline evaluates how trading volume changes over time and
identifies abnormal volume activity that may confirm or weaken
observed price movements.
"""

def build_daily_volume_liquidity(spark: SparkSession) -> None:
    silver_df = (
            read_parquet(
            spark, 
            paths.SILVER_PATH)
            .select(
                "SYMBOL",
                "DATE",
                "VOLUME",
                "CLOSE_PRICE",
            )
        )

    w = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc())
    five_days_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-5, -1)
    twenty_days_window = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc()).rowsBetween(-20, -1)

    df = (
        silver_df
        .withColumn("PREV_VOLUME", lag("VOLUME").over(w))
        .withColumn(
            "VOLUME_CHANGE_PCT",
            when(col("PREV_VOLUME").isNull() | (col("PREV_VOLUME") == 0), None)
             .otherwise(((col("VOLUME") - col("PREV_VOLUME")) / col("PREV_VOLUME")) * 100)
        )
        .withColumn(
            "VOLUME_DIRECTION",
            when(col("VOLUME") > col("PREV_VOLUME"), "UP")
             .when(col("VOLUME") < col("PREV_VOLUME"), "DOWN")
             .otherwise("FLAT")
        )
        .withColumn("5_DAY_AVG_VOLUME", avg("VOLUME").over(five_days_window))
        .withColumn("20_DAY_AVG_VOLUME", avg("VOLUME").over(twenty_days_window))
    )

    final_df = df.select(
        "SYMBOL",
        "DATE",
        "PREV_VOLUME",
        "VOLUME_CHANGE_PCT",
        "VOLUME_DIRECTION",
        "5_DAY_AVG_VOLUME",
        "20_DAY_AVG_VOLUME",
    )

    write_parquet(final_df, paths.GOLD_DAILY_VOLUME_LIQUIDITY_PATH, partitions=4)