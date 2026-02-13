import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths


def build_earnings_reaction(spark: SparkSession) -> None:
    silver_df = read_parquet(spark, paths.SILVER_PATH)
    price_gold = read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH).select(
        "SYMBOL", "DATE", "INTRADAY_RANGE_PCT", "20_DAY_AVG_INTRADAY_RANGE_PCT"
    )
    volume_gold = read_parquet(spark, paths.GOLD_DAILY_VOLUME_LIQUIDITY_PATH).select(
        "SYMBOL", "DATE", "VOLUME_CHANGE_PCT", "VOLUME_DIRECTION", 
        "5_DAY_AVG_VOLUME", "20_DAY_AVG_VOLUME"
    )

    w = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc())

    df = (
        silver_df
        .join(volume_gold, ["SYMBOL", "DATE"], "left")
        .join(price_gold, ["SYMBOL", "DATE"], "left")
        .withColumn(
            "EPS_SURPRISE_AMT",
            F.when(F.col("EPS").isNull() | F.col("EPS_ESTIMATE").isNull(), None)
             .otherwise(F.col("EPS") - F.col("EPS_ESTIMATE"))
        )
        .withColumn(
            "EPS_SURPRISE_PCT",
            F.when(F.col("EPS").isNull() | F.col("EPS_ESTIMATE").isNull() | (F.col("EPS_ESTIMATE") == 0), None)
             .otherwise(((F.col("EPS") - F.col("EPS_ESTIMATE")) / F.col("EPS_ESTIMATE")) * 100)
        )
        .withColumn("PRE_EARNINGS_CLOSE", F.lag("CLOSE_PRICE").over(w))
        .withColumn("PRE_EARNINGS_VOLUME", F.lag("VOLUME").over(w))
        .withColumn("POST_EARNINGS_OPEN", F.lead("OPEN_PRICE").over(w))
        .withColumn("POST_EARNINGS_CLOSE", F.lead("CLOSE_PRICE").over(w))
        .withColumn("POST_EARNINGS_VOLUME", F.lead("VOLUME").over(w))
        .withColumn(
            "EARNING_DIRECTION",
            F.when(F.col("POST_EARNINGS_CLOSE").isNull() | F.col("PRE_EARNINGS_CLOSE").isNull(), None)
             .when(F.col("POST_EARNINGS_CLOSE") > F.col("PRE_EARNINGS_CLOSE"), "UP")
             .when(F.col("POST_EARNINGS_CLOSE") < F.col("PRE_EARNINGS_CLOSE"), "DOWN")
             .otherwise("FLAT")
        )
        .withColumn(
            "EARNINGS_RETURN_PCT",
            F.when(F.col("PRE_EARNINGS_CLOSE").isNull() | (F.col("PRE_EARNINGS_CLOSE") == 0) | F.col("POST_EARNINGS_CLOSE").isNull(), None)
             .otherwise(((F.col("POST_EARNINGS_CLOSE") - F.col("PRE_EARNINGS_CLOSE")) / F.col("PRE_EARNINGS_CLOSE")) * 100)
        )
        .withColumn(
            "VOLATILITY_SPIKE_FLAG",
            F.when(
                F.col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull() | (F.col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0) | F.col("INTRADAY_RANGE_PCT").isNull(),
                None
            ).when(
                F.col("INTRADAY_RANGE_PCT") > 1.5 * F.col("20_DAY_AVG_INTRADAY_RANGE_PCT"),
                "SPIKE"
            ).otherwise("NORMAL")
        )
        .withColumn(
            "EARNINGS_REACTION_STRENGTH",
            F.when(
                F.col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull() | (F.col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0) | F.col("EARNINGS_RETURN_PCT").isNull(),
                None
            ).otherwise(F.col("EARNINGS_RETURN_PCT") / F.col("20_DAY_AVG_INTRADAY_RANGE_PCT"))
        )
        .withColumn(
            "SURPRISE_DIRECTION",
            F.when(F.col("EPS_SURPRISE_PCT").isNull(), None)
             .when(F.col("EPS_SURPRISE_PCT") > 0, "POSITIVE")
             .when(F.col("EPS_SURPRISE_PCT") < 0, "NEGATIVE")
             .otherwise("NO_SURPRISE")
        )
        .withColumn(
            "REACTION_ALIGNMENT_FLAG",
            F.when(F.col("SURPRISE_DIRECTION").isNull() | F.col("EARNING_DIRECTION").isNull(), None)
             .when((F.col("SURPRISE_DIRECTION") == "POSITIVE") & (F.col("EARNING_DIRECTION") == "UP"), "ALIGNED")
             .when((F.col("SURPRISE_DIRECTION") == "NEGATIVE") & (F.col("EARNING_DIRECTION") == "DOWN"), "ALIGNED")
             .when((F.col("SURPRISE_DIRECTION") == "NO_SURPRISE") | (F.col("EARNING_DIRECTION") == "FLAT"), "NEUTRAL")
             .otherwise("MISALIGNED")
        )
    )

    write_parquet(df, paths.GOLD_EARNINGS_REACTION_PATH, partitions=4)
