import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths


def build_daily_price_metrics(spark: SparkSession) -> None:
    silver_df = read_parquet(spark, paths.SILVER_PATH)

    w = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc())
    twenty_days_window = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc()).rowsBetween(-20, -1)

    df = (
        silver_df
        .withColumn("PREV_CLOSE_PRICE", F.lag("CLOSE_PRICE").over(w))
        .withColumn(
            "DAILY_RETURN_PCT",
            F.when(
                F.col("PREV_CLOSE_PRICE").isNull() | (F.col("PREV_CLOSE_PRICE") == 0),
                None
            ).otherwise(
                ((F.col("CLOSE_PRICE") - F.col("PREV_CLOSE_PRICE")) / F.col("PREV_CLOSE_PRICE")) * 100
            )
        )
        .withColumn("GAP_AMOUNT", F.col("OPEN_PRICE") - F.col("PREV_CLOSE_PRICE"))
        .withColumn(
            "GAP_DIRECTION",
            F.when(F.col("OPEN_PRICE") > F.col("PREV_CLOSE_PRICE"), "UP")
             .when(F.col("OPEN_PRICE") < F.col("PREV_CLOSE_PRICE"), "DOWN")
             .otherwise("NO_GAP")
        )
        .withColumn(
            "DAILY_DIRECTION",
            F.when(F.col("CLOSE_PRICE") > F.col("PREV_CLOSE_PRICE"), "UP")
             .when(F.col("CLOSE_PRICE") < F.col("PREV_CLOSE_PRICE"), "DOWN")
             .otherwise("FLAT")
        )
        .withColumn("INTRADAY_RANGE_AMT", F.col("HIGH_PRICE") - F.col("LOW_PRICE"))
        .withColumn(
            "INTRADAY_RANGE_PCT",
            F.when(F.col("CLOSE_PRICE").isNull() | (F.col("CLOSE_PRICE") == 0), None)
             .otherwise((F.col("INTRADAY_RANGE_AMT") / F.col("CLOSE_PRICE")) * 100)
        )
        .withColumn(
            "20_DAY_AVG_INTRADAY_RANGE_PCT",
            F.avg("INTRADAY_RANGE_PCT").over(twenty_days_window)
        )
    )

    write_parquet(df, paths.GOLD_DAILY_PRICE_METRICS_PATH, partitions=4)
