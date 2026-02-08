import pyspark.sql.functions as F
from pyspark.sql.window import Window
from common.functions import (
    spark_session,
    read_csv,
    read_parquet,
    write_parquet
)


# Raw CSV inputs
BRONZE_DIVIDENDS_CSV = "data/bronze/dividents_latest.csv"
BRONZE_EARNINGS_CSV = "data/bronze/earnings_latest.csv"
BRONZE_PRICES_CSV = "data/bronze/stock_prices_latest.csv"

# Bronze parquet outputs
BRONZE_DIVIDENDS_PARQUET = "data/bronze/dividends"
BRONZE_EARNINGS_PARQUET = "data/bronze/earnings"
BRONZE_PRICES_PARQUET = "data/bronze/stock_prices"

# Silver output
SILVER_PATH = "data/silver/financial_market_events_silver"

# Gold output
GOLD_PATH_DAILY_PRICE_METRICS = "data/gold/daily_price_metrics_gold"
GOLD_PATH_VOLUME_LIQUIDITY_METRICS = "data/gold/daily_volume_liquidity_metrics_gold"
GOLD_PATH_EARNINGS_AND_EVENT_DRIVEN_MARKET_REACITION = "data/gold/earnings_impact_and_event_driven_market_reaction_gold"

def ingest_bronze(spark):
    write_parquet(
        read_csv(spark, BRONZE_DIVIDENDS_CSV),
        BRONZE_DIVIDENDS_PARQUET
    )

    write_parquet(
        read_csv(spark, BRONZE_EARNINGS_CSV),
        BRONZE_EARNINGS_PARQUET
    )

    write_parquet(
        read_csv(spark, BRONZE_PRICES_CSV),
        BRONZE_PRICES_PARQUET
    )

def build_silver(spark):
    dividends = (
        read_parquet(spark, BRONZE_DIVIDENDS_PARQUET)
        .select(
            F.col("symbol").alias("SYMBOL"),
            F.to_date("date").alias("DATE"),
            F.col("dividents").cast("double").alias("DIVIDENDS")
        )
    )

    earnings = (
        read_parquet(spark, BRONZE_EARNINGS_PARQUET)
        .select(
            F.col("symbol").alias("SYMBOL"),
            F.to_date("date").alias("DATE"),
            F.col("qtr").alias("QUARTER"),
            F.col("eps_est").cast("double").alias("EPS_ESTIMATE"),
            F.col("eps").cast("double").alias("EPS"),
            F.col("release_time").alias("RELEASE_TIME")
        )
    )

    prices = (
        read_parquet(spark, BRONZE_PRICES_PARQUET)
        .select(
            F.col("symbol").alias("SYMBOL"),
            F.to_date("date").alias("DATE"),
            F.col("open").cast("double").alias("OPEN_PRICE"),
            F.col("high").cast("double").alias("HIGH_PRICE"),
            F.col("low").cast("double").alias("LOW_PRICE"),
            F.col("close").cast("double").alias("CLOSE_PRICE"),
            F.col("close_adjusted").cast("double").alias("CLOSE_ADJUSTED_PRICE"),
            F.col("volume").cast("long").alias("VOLUME"),
            F.col("split_coefficient").cast("double").alias("SPLIT_COEFFICIENT")
        )
    )

    silver_df = (
        prices
        .join(earnings, ["SYMBOL", "DATE"], "left")
        .join(dividends, ["SYMBOL", "DATE"], "left")
    )


    write_parquet(silver_df, SILVER_PATH)

def build_gold(spark):
    silver_df = read_parquet(spark, SILVER_PATH)
    silver_df = silver_df.cache()

    w = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc())
    five_days_window = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc()).rowsBetween(-5, -1)
    twenty_days_window = Window.partitionBy("SYMBOL").orderBy(F.col("DATE").asc()).rowsBetween(-20, -1)

    # Calculation of daily price metrics gold layer
    daily_price_metrics_gold = (
        silver_df
        .withColumn("PREV_CLOSE_PRICE", F.lag("CLOSE_PRICE").over(w))
        .withColumn(
            "DAILY_RETURN_PCT",
            F.when(
                (F.col("PREV_CLOSE_PRICE").isNull()) | 
                (F.col("PREV_CLOSE_PRICE") == 0), None)
                .otherwise(((F.col("CLOSE_PRICE") - F.col("PREV_CLOSE_PRICE")) 
                            / F.col("PREV_CLOSE_PRICE"))* 100)
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
    )

    write_parquet(daily_price_metrics_gold, GOLD_PATH_DAILY_PRICE_METRICS)

    # Calculation of daily volume and liquidity metrics gold layer
    daily_volume_liquidity_metrics_gold = (
        silver_df
        .withColumn("PREV_VOLUME", F.lag("VOLUME").over(w))
        .withColumn("VOLUME_CHANGE_PCT",
                    F.when(
                        (F.col("PREV_VOLUME").isNull()) | 
                        (F.col("PREV_VOLUME") == 0), None)
                        .otherwise(
                            (F.col("VOLUME") - F.col("PREV_VOLUME")) 
                            / F.col("PREV_VOLUME") * 100)
        )
        .withColumn("VOLUME_DIRECTION",
                    F.when(F.col("VOLUME") > F.col("PREV_VOLUME"), "UP")
                    .when(F.col("VOLUME") < F.col("PREV_VOLUME"), "DOWN")
                    .otherwise("FLAT")

        )
        .withColumn("5_DAY_AVG_VOLUME", F.avg("VOLUME").over(five_days_window))
        .withColumn("20_DAY_AVG_VOLUME", F.avg("VOLUME").over(twenty_days_window))
    )
    
    write_parquet(daily_volume_liquidity_metrics_gold, GOLD_PATH_VOLUME_LIQUIDITY_METRICS)

    # Earnings Impact and Event-driven Market Reaction gold layer
    earnings_impact_and_event_driven_market_reaction_gold = (
        silver_df
        .join(
            daily_volume_liquidity_metrics_gold, 
            on=["SYMBOL", "DATE"], 
            how="left")
        .withColumn("EPS_SURPRISE_AMT",
                    F.when(
                        F.col("EPS").isNull() | F.col("EPS_ESTIMATE").isNull(), None
                    ).otherwise(
                        F.col("EPS") - F.col("EPS_ESTIMATE")
                    )
        )
        .withColumn("EPS_SURPRISE_PCT",
                    F.when(
                        (F.col("EPS").isNull()) | (F.col("EPS_ESTIMATE").isNull()), None
                    ).otherwise(
                        (F.col("EPS") - F.col("EPS_ESTIMATE")) 
                        / F.col("EPS_ESTIMATE") * 100)
        )
        
    )

    write_parquet(earnings_impact_and_event_driven_market_reaction_gold, GOLD_PATH_EARNINGS_AND_EVENT_DRIVEN_MARKET_REACITION)

def main():
    spark = spark_session("Financial Market Event Enrichment")

    ingest_bronze(spark)
    build_silver(spark)
    build_gold(spark)

if __name__ == "__main__":
    main()
