from src.common.functions import spark_session, read_parquet
import src.common.paths as paths
from pyspark.sql.functions import col

spark = spark_session("reader")

gold_daily_prices = read_parquet(spark, paths.GOLD_DAILY_PRICE_METRICS_PATH)
# gold_daily_prices.show(10, truncate=False)

gold_volume_liquidity = read_parquet(spark, paths.GOLD_DAILY_VOLUME_LIQUIDITY_PATH)
# gold_volume_liquidity.show(10, truncate=False)

gold_earnings_reaction = read_parquet(spark, paths.GOLD_EARNINGS_REACTION_PATH)
# gold_earnings_reaction.select(
#         "SYMBOL",
#         "DATE",
#         "EPS_SURPRISE_PCT",
#         "SURPRISE_DIRECTION",
#         "EARNINGS_DAY_RETURN_PCT",
#         "NEXT_DAY_RETURN_PCT",
#         "EVENT_WINDOW_RETURN_PCT",
#         "EARNINGS_REACTION_STRENGTH",
#         "VOLUME_CONFIRMATION_FLAG",
#         "DRIFT_DIRECTION",
#         "DRIFT_ALIGNMENT_FLAG"
#     ).show(10, truncate=False)

gold_market_breadth = read_parquet(spark, paths.GOLD_MARKET_BREADTH_PATH)
# gold_market_breadth.show(10, truncate=False)

gold_market_regime = read_parquet(spark, paths.GOLD_MARKET_REGIME_PATH)
# gold_market_regime.show(10, truncate=False)

gold_relative_strength = read_parquet(spark, paths.GOLD_RELATIVE_STRENGTH_PATH)
# gold_relative_strength.filter(
#     (col("INDEX_RETURN_20D").isNotNull()) &
#     (col("ROLLING_RETURN_20D").isNotNull()) &
#     (col("RELATIVE_STRENGTH_20D").isNotNull()) &
#     (col("RELATIVE_STRENGTH_DIRECTION").isNotNull()) &
#     (col("RS_TREND").isNotNull())
# ).dropDuplicates(["SYMBOL"]).show(10, truncate=False)

gold_risk_metrics = read_parquet(spark, paths.GOLD_RISK_METRICS_PATH)
# gold_risk_metrics.show(10, truncate=False)