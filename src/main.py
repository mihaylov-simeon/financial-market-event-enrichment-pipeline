from src.common.functions import spark_session
from src.pipelines.bronze.ingest_bronze import ingest_bronze
from src.pipelines.silver.build_silver import build_silver
from src.pipelines.gold.daily_price_metrics import build_daily_price_metrics
from src.pipelines.gold.daily_volume_liquidity import build_daily_volume_liquidity
from src.pipelines.gold.earnings_reaction import build_earnings_reaction


def main():
    spark = spark_session("Financial Market Event Enrichment")

    ingest_bronze(spark)
    build_silver(spark)

    build_daily_price_metrics(spark)
    build_daily_volume_liquidity(spark)
    build_earnings_reaction(spark)


if __name__ == "__main__":
    main()
