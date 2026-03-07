from src.common.functions import spark_session
from src.pipelines.bronze import ingest_bronze
from src.pipelines.silver import build_silver
from src.pipelines.gold import (
    daily_price_metrics,
    daily_volume_liquidity,
    earnings_reaction,
    market_regime,
    risk_metrics,
    relative_strength,
) 


def main():
    spark = spark_session("Financial Market Event Enrichment")

    ingest_bronze.ingest_bronze(spark)

    build_silver(spark)

    daily_price_metrics.build_daily_price_metrics(spark)
    daily_volume_liquidity.build_daily_volume_liquidity(spark)
    earnings_reaction.build_earnings_reaction(spark)
    market_regime.build_market_regime(spark)
    risk_metrics.build_risk_metrics(spark)
    relative_strength.build_relative_strength(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()
