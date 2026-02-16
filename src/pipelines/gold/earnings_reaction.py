import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.common.functions import read_parquet, write_parquet
from src.common import paths


def build_earnings_reaction(spark: SparkSession) -> None:

    # ======================================================
    # Read Silver and Gold Datasets
    # ======================================================

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
            (F.col("SYMBOL").isNotNull())
            & (F.col("DATE").isNotNull())
            & (F.col("OPEN_PRICE").isNotNull())
            & (F.col("CLOSE_PRICE").isNotNull())
        )
    )

    price_gold = (
        read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
        .select("SYMBOL", "DATE", "INTRADAY_RANGE_PCT", "20_DAY_AVG_INTRADAY_RANGE_PCT")
        .filter((F.col("SYMBOL").isNotNull()) & (F.col("DATE").isNotNull()))
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
        .filter((F.col("SYMBOL").isNotNull()) & (F.col("DATE").isNotNull()))
    )

    # ======================================================
    # Join Datasets
    # ======================================================

    df = silver_df.join(volume_gold, ["SYMBOL", "DATE"], "left").join(
        price_gold, ["SYMBOL", "DATE"], "left"
    )

    w = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc())

    # ======================================================
    # Earnings Surprise
    # ======================================================

    df = (
        df.withColumn(
            "EPS_SURPRISE_AMT",
            F.when(
                F.col("EPS").isNull() | F.col("EPS_ESTIMATE").isNull(), None
            ).otherwise(F.col("EPS") - F.col("EPS_ESTIMATE")),
        )
        .withColumn(
            "EPS_SURPRISE_PCT",
            F.when(
                (F.col("EPS").isNull() | F.col("EPS_ESTIMATE").isNull())
                | (F.col("EPS_ESTIMATE") == 0),
                None,
            ).otherwise(
                ((F.col("EPS") - F.col("EPS_ESTIMATE")) / F.col("EPS_ESTIMATE")) * 100
            ),
        )
        .withColumn(
            "SURPRISE_DIRECTION",
            F.when(F.col("EPS_SURPRISE_PCT").isNull(), None)
            .when(F.col("EPS_SURPRISE_PCT") > 0, "POSITIVE")
            .when(F.col("EPS_SURPRISE_PCT") < 0, "NEGATIVE")
            .otherwise("NO_SURPRISE"),
        )
        .withColumn(
            "VOLUME_SPIKE_FLAG",
            F.when(
                F.col("20_DAY_AVG_VOLUME").isNull() | F.col("20_DAY_AVG_VOLUME")
                == 0 | F.col("VOLUME").isNull(),
                None,
            )
            .when(F.col("VOLUME") > 1.5 * F.col("20_DAY_AVG_VOLUME"), "SPIKE")
            .otherwise("NORMAL"),
        )
    )

    # ======================================================
    # Event Window Context
    # ======================================================

    df = (
        df.withColumn("PRE_EARNINGS_CLOSE", F.lag("CLOSE_PRICE").over(w))
        .withColumn("PRE_EARNINGS_VOLUME", F.lag("VOLUME").over(w))
        .withColumn("POST_EARNINGS_OPEN", F.lead("OPEN_PRICE").over(w))
        .withColumn("POST_EARNINGS_CLOSE", F.lead("CLOSE_PRICE").over(w))
        .withColumn("POST_EARNINGS_VOLUME", F.lead("VOLUME").over(w))
    )

    # ======================================================
    # Event Window Reaction
    # ======================================================

    df = (
        df.withColumn(
            "EARNING_DIRECTION",
            F.when(
                F.col("POST_EARNINGS_CLOSE").isNull()
                | F.col("PRE_EARNINGS_CLOSE").isNull(),
                None,
            )
            .when(F.col("POST_EARNINGS_CLOSE") > F.col("PRE_EARNINGS_CLOSE"), "UP")
            .when(F.col("POST_EARNINGS_CLOSE") < F.col("PRE_EARNINGS_CLOSE"), "DOWN")
            .otherwise("FLAT"),
        )
        .withColumn(
            "EVENT_WINDOW_RETURN_PCT",
            F.when(
                F.col("PRE_EARNINGS_CLOSE").isNull()
                | (F.col("PRE_EARNINGS_CLOSE") == 0)
                | F.col("POST_EARNINGS_CLOSE").isNull(),
                None,
            ).otherwise(
                (
                    (F.col("POST_EARNINGS_CLOSE") - F.col("PRE_EARNINGS_CLOSE"))
                    / F.col("PRE_EARNINGS_CLOSE")
                )
                * 100
            ),
        )
        .withColumn(
            "EARNINGS_DAY_RETURN_PCT",
            F.when(
                F.col("PRE_EARNINGS_CLOSE").isNull() | F.col("PRE_EARNINGS_CLOSE") == 0,
                None,
            ).otherwise(
                (
                    (F.col("CLOSE_PRICE") - F.col("PRE_EARNINGS_CLOSE"))
                    / F.col("PRE_EARNINGS_CLOSE")
                )
                * 100
            ),
        )
    )

    # ======================================================
    # Volatility Context
    # ======================================================

    df = (
        df.withColumn(
            "VOLATILITY_SPIKE_FLAG",
            F.when(
                F.col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()
                | (F.col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0)
                | F.col("INTRADAY_RANGE_PCT").isNull(),
                None,
            )
            .when(
                F.col("INTRADAY_RANGE_PCT")
                > 1.5 * F.col("20_DAY_AVG_INTRADAY_RANGE_PCT"),
                "SPIKE",
            )
            .otherwise("NORMAL"),
        )
        .withColumn(
            "VOLATILITY_MULTIPLIER",
            F.when(
                F.col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()
                | F.col("20_DAY_AVG_INTRADAY_RANGE_PCT")
                == 0 | F.col("INTRADAY_RANGE_PCT").isNull(),
                None,
            ).otherwise(
                F.col("INTRADAY_RANGE_PCT") / F.col("20_DAY_AVG_INTRADAY_RANGE_PCT")
            ),
        )
        .withColumn(
            "EARNINGS_REACTION_STRENGTH",
            F.when(
                F.col("20_DAY_AVG_INTRADAY_RANGE_PCT").isNull()
                | (F.col("20_DAY_AVG_INTRADAY_RANGE_PCT") == 0)
                | F.col("EVENT_WINDOW_RETURN_PCT").isNull(),
                None,
            ).otherwise(
                F.col("EVENT_WINDOW_RETURN_PCT")
                / F.col("20_DAY_AVG_INTRADAY_RANGE_PCT")
            ),
        )
        .withColumn(
            "NEXT_DAY_RETURN_PCT",
            (
                (F.col("POST_EARNINGS_CLOSE") - F.col("CLOSE_PRICE"))
                / F.col("CLOSE_PRICE")
            )
            * 100,
        )
    )

    # ======================================================
    # Reaction Alignment
    # ======================================================

    df = (
        df.withColumn(
            "REACTION_ALIGNMENT_FLAG",
            F.when(
                F.col("SURPRISE_DIRECTION").isNull()
                | F.col("EARNING_DIRECTION").isNull(),
                None,
            )
            .when(
                (F.col("SURPRISE_DIRECTION") == "POSITIVE")
                & (F.col("EARNING_DIRECTION") == "UP"),
                "ALIGNED",
            )
            .when(
                (F.col("SURPRISE_DIRECTION") == "NEGATIVE")
                & (F.col("EARNING_DIRECTION") == "DOWN"),
                "ALIGNED",
            )
            .when(
                (F.col("SURPRISE_DIRECTION") == "NO_SURPRISE")
                | (F.col("EARNING_DIRECTION") == "FLAT"),
                "NEUTRAL",
            )
            .otherwise("MISALIGNED"),
        )
        .withColumn(
            "VOLUME_CONFIRMATION_FLAG",
            F.when(
                F.col("VOLUME_DIRECTION").isNull()
                | F.col("VOLUME_SPIKE_FLAG").isNull(),
                "NEUTRAL",
            )
            .when(
                (F.col("EARNING_DIRECTION") != "FLAT")
                & (F.col("VOLUME_SPIKE_FLAG") == "SPIKE"),
                "CONFIRMED",
            )
            .when(
                (F.col("EARNING_DIRECTION") != "FLAT")
                & (F.col("VOLUME_SPIKE_FLAG") == "NORMAL"),
                "WEAK",
            )
            .otherwise("NEUTRAL"),
        )
        .withColumn(
            "DRIFT_DIRECTION",
            F.when(F.col("NEXT_DAY_RETURN_PCT").isNull(), None)
            .when(F.col("NEXT_DAY_RETURN_PCT") > 0, "UP")
            .when(F.col("NEXT_DAY_RETURN_PCT") == 0, "FLAT")
            .when(F.col("NEXT_DAY_RETURN_PCT") < 0, "DOWN"),
        )
        .withColumn(
            "DRIFT_ALIGNMENT_FLAG",
            F.when(
                F.col("EARNING_DIRECTION").isNull() | F.col("DRIFT_DIRECTION").isNull(),
                None,
            )
            .when(
                (F.col("EARNING_DIRECTION") == "FLAT")
                | (F.col("DRIFT_DIRECTION") == "FLAT"),
                "NEUTRAL",
            )
            .when(
                F.col("EARNING_DIRECTION") == F.col("DRIFT_DIRECTION"), "CONTINUATION"
            )
            .when(F.col("EARNING_DIRECTION") != F.col("DRIFT_DIRECTION"), "REVERSAL"),
        )
    )

    # ======================================================
    # Final Ordered Schema
    # ======================================================

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
