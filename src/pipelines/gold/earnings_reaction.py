from pyspark.sql.functions import (
    col, lag, when, lead
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths

"""
Earnings Reaction Pipeline
--------------------------

Measures the market's reaction to earnings announcements.

This pipeline calculates earnings surprises and analyzes price and
volume behavior around the earnings event window to determine whether
the market response aligns with the reported results.
"""

def build_earnings_reaction(spark: SparkSession) -> None:

    silver_df = (
        read_parquet(spark, paths.SILVER_PATH)
        .select(
            "SYMBOL",
            "DATE",
            "OPEN_PRICE",
            "CLOSE_PRICE",
            "VOLUME",
            "EPS",
            "EPS_ESTIMATE",
        )
        .filter(
            (col("SYMBOL").isNotNull())
            & (col("DATE").isNotNull())
            & (col("OPEN_PRICE").isNotNull())
            & (col("CLOSE_PRICE").isNotNull())
        )
    )

    price_gold = (
        read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
        .select("SYMBOL", "DATE", "INTRADAY_RANGE_PCT", "20_DAY_AVG_INTRADAY_RANGE_PCT")
        .filter((col("SYMBOL").isNotNull()) & (col("DATE").isNotNull()))
    )

    volume_gold = (
        read_parquet(spark, paths.GOLD_DAILY_VOLUME_LIQUIDITY_PATH)
        .select(
            "SYMBOL",
            "DATE",
            "VOLUME_CHANGE_PCT",
            "VOLUME_DIRECTION",
            "5_DAY_AVG_VOLUME",
            "20_DAY_AVG_VOLUME",
        )
        .filter((col("SYMBOL").isNotNull()) & (col("DATE").isNotNull()))
    )


    df = (
        silver_df
        .join(volume_gold, ["SYMBOL", "DATE"], "left")
        .join(price_gold, ["SYMBOL", "DATE"], "left")
    )

    w = Window.partitionBy("SYMBOL").orderBy(col("DATE").asc())


    df = (
        df.withColumn(
            "EPS_SURPRISE_AMT",
            when(
                (col("EPS").isNull()) |
                (col("EPS_ESTIMATE").isNull()),
                None
            ).otherwise(col("EPS") - col("EPS_ESTIMATE")),
        )
        .withColumn(
            "EPS_SURPRISE_PCT",
            when(
                (col("EPS").isNull()) | 
                (col("EPS_ESTIMATE").isNull()) |
                (col("EPS_ESTIMATE") == 0),
                None,
            ).otherwise(
                ((col("EPS") - col("EPS_ESTIMATE")) / col("EPS_ESTIMATE")) * 100
            ),
        )
        .withColumn(
            "SURPRISE_DIRECTION",
            when(col("EPS_SURPRISE_PCT").isNull(), None)
            .when(col("EPS_SURPRISE_PCT") > 0, "POSITIVE")
            .when(col("EPS_SURPRISE_PCT") < 0, "NEGATIVE")
            .otherwise("NO_SURPRISE"),
        )
        .withColumn(
            "VOLUME_SPIKE_FLAG",
            when(
                (col("20_DAY_AVG_VOLUME").isNull()) |
                (col("20_DAY_AVG_VOLUME") == 0) |
                (col("VOLUME").isNull()),
                None,
            )
            .when(col("VOLUME") > 1.5 * col("20_DAY_AVG_VOLUME"), "SPIKE")
            .otherwise("NORMAL"),
        )
    )

    df = (
        df.withColumn("PRE_EARNINGS_CLOSE", lag("CLOSE_PRICE").over(w))
        .withColumn("PRE_EARNINGS_VOLUME", lag("VOLUME").over(w))
        .withColumn("POST_EARNINGS_OPEN", lead("OPEN_PRICE").over(w))
        .withColumn("POST_EARNINGS_CLOSE", lead("CLOSE_PRICE").over(w))
        .withColumn("POST_EARNINGS_VOLUME", lead("VOLUME").over(w))
    )


    df = (
        df.withColumn(
            "EARNING_DIRECTION",
            when(
                (col("POST_EARNINGS_CLOSE").isNull()) |
                (col("PRE_EARNINGS_CLOSE").isNull()),
                None,
            )
            .when(col("POST_EARNINGS_CLOSE") > col("PRE_EARNINGS_CLOSE"), "UP")
            .when(col("POST_EARNINGS_CLOSE") < col("PRE_EARNINGS_CLOSE"), "DOWN")
            .otherwise("FLAT"),
        )
        .withColumn(
            "EVENT_WINDOW_RETURN_PCT",
            when(
                (col("PRE_EARNINGS_CLOSE").isNull()) |
                (col("PRE_EARNINGS_CLOSE") == 0) |
                (col("POST_EARNINGS_CLOSE").isNull()),
                None,
            ).otherwise(
                (
                    (col("POST_EARNINGS_CLOSE") - col("PRE_EARNINGS_CLOSE")) / col("PRE_EARNINGS_CLOSE")
                )
                * 100
            ),
        )
        .withColumn(
            "EARNINGS_DAY_RETURN_PCT",
            when(
                (col("PRE_EARNINGS_CLOSE").isNull()) |
                (col("PRE_EARNINGS_CLOSE") == 0),
                None,
            ).otherwise(
                (
                    (col("CLOSE_PRICE") - col("PRE_EARNINGS_CLOSE")) / col("PRE_EARNINGS_CLOSE")
                )
                * 100
            ),
        )
    )

    df = (
        df.withColumn(
            "VOLATILITY_SPIKE_FLAG",
            when(
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()) |
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0) |
                (col("INTRADAY_RANGE_PCT").isNull()),
                None,
            )
            .when(
                col("INTRADAY_RANGE_PCT") > 1.5 * col("20_DAY_AVG_INTRADAY_RANGE_PCT"),
                "SPIKE",
            )
            .otherwise("NORMAL"),
        )
        .withColumn(
            "VOLATILITY_MULTIPLIER",
            when(
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()) |
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0) |
                (col("INTRADAY_RANGE_PCT").isNull()),
                None,
            ).otherwise(
                col("INTRADAY_RANGE_PCT") / col("20_DAY_AVG_INTRADAY_RANGE_PCT")
            ),
        )
        .withColumn(
            "EARNINGS_REACTION_STRENGTH",
            when(
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()) |
                (col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0) |
                (col("EVENT_WINDOW_RETURN_PCT").isNull()),
                None,
            ).otherwise(
                col("EVENT_WINDOW_RETURN_PCT") / col("20_DAY_AVG_INTRADAY_RANGE_PCT")
            ),
        )
        .withColumn(
            "NEXT_DAY_RETURN_PCT",
            (
                (col("POST_EARNINGS_CLOSE") - col("CLOSE_PRICE")) / col("CLOSE_PRICE")
            )
            * 100,
        )
    )

    df = (
        df.withColumn(
            "REACTION_ALIGNMENT_FLAG",
            when(
                (col("SURPRISE_DIRECTION").isNull()) |
                (col("EARNING_DIRECTION").isNull()),
                None,
            )
            .when(
                (col("SURPRISE_DIRECTION") == "POSITIVE") & 
                (col("EARNING_DIRECTION") == "UP"),
                "ALIGNED",
            )
            .when(
                (col("SURPRISE_DIRECTION") == "NEGATIVE") &
                (col("EARNING_DIRECTION") == "DOWN"),
                "ALIGNED",
            )
            .when(
                (col("SURPRISE_DIRECTION") == "NO_SURPRISE") |
                (col("EARNING_DIRECTION") == "FLAT"),
                "NEUTRAL",
            )
            .otherwise("MISALIGNED"),
        )
        .withColumn(
            "VOLUME_CONFIRMATION_FLAG",
            when(
                (col("VOLUME_DIRECTION").isNull()) |
                (col("VOLUME_SPIKE_FLAG").isNull()),
                "NEUTRAL",
            )
            .when(
                (col("EARNING_DIRECTION") != "FLAT") &
                (col("VOLUME_SPIKE_FLAG") == "SPIKE"),
                "CONFIRMED",
            )
            .when(
                (col("EARNING_DIRECTION") != "FLAT") &
                (col("VOLUME_SPIKE_FLAG") == "NORMAL"),
                "WEAK",
            )
            .otherwise("NEUTRAL"),
        )
        .withColumn(
            "DRIFT_DIRECTION",
            when(col("NEXT_DAY_RETURN_PCT").isNull(), None)
            .when(col("NEXT_DAY_RETURN_PCT") > 0, "UP")
            .when(col("NEXT_DAY_RETURN_PCT") == 0, "FLAT")
            .when(col("NEXT_DAY_RETURN_PCT") < 0, "DOWN"),
        )
        .withColumn(
            "DRIFT_ALIGNMENT_FLAG",
            when(
                (col("EARNING_DIRECTION").isNull()) |
                (col("DRIFT_DIRECTION").isNull()),
                None,
            )
            .when(
                (col("EARNING_DIRECTION") == "FLAT") | 
                (col("DRIFT_DIRECTION") == "FLAT"),
                "NEUTRAL",
            )
            .when(
                col("EARNING_DIRECTION") == col("DRIFT_DIRECTION"), "CONTINUATION"
            )
            .when(col("EARNING_DIRECTION") != col("DRIFT_DIRECTION"), "REVERSAL"),
        )
    )

    df = df.select(
        # Identity
        "SYMBOL",
        "DATE",
        # Market Data
        "OPEN_PRICE",
        "CLOSE_PRICE",
        "VOLUME",
        # Earnings Fundamentals
        "EPS",
        "EPS_ESTIMATE",
        # Surprise
        "EPS_SURPRISE_AMT",
        "EPS_SURPRISE_PCT",
        "SURPRISE_DIRECTION",
        # Event Window Context
        "PRE_EARNINGS_CLOSE",
        "PRE_EARNINGS_VOLUME",
        "POST_EARNINGS_OPEN",
        "POST_EARNINGS_CLOSE",
        "POST_EARNINGS_VOLUME",
        # Event Reaction
        "EARNING_DIRECTION",
        "EVENT_WINDOW_RETURN_PCT",
        "EARNINGS_DAY_RETURN_PCT",
        # Volatility
        "VOLATILITY_SPIKE_FLAG",
        "VOLATILITY_MULTIPLIER",
        "INTRADAY_RANGE_PCT",
        "20_DAY_AVG_INTRADAY_RANGE_PCT",
        "EARNINGS_REACTION_STRENGTH",
        "NEXT_DAY_RETURN_PCT",
        # Volume
        "VOLUME_CHANGE_PCT",
        "VOLUME_DIRECTION",
        "5_DAY_AVG_VOLUME",
        "20_DAY_AVG_VOLUME",
        "VOLUME_SPIKE_FLAG",
        "VOLUME_CONFIRMATION_FLAG",
        # Alignment
        "REACTION_ALIGNMENT_FLAG",
        # Drift
        "DRIFT_DIRECTION",
        "DRIFT_ALIGNMENT_FLAG",
    )

    write_parquet(df, paths.GOLD_EARNINGS_REACTION_PATH, partitions=4)
