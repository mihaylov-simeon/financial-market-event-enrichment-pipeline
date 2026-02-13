from pyspark.sql import SparkSession
from src.common.functions import read_csv, write_parquet
from src.common import paths


def ingest_bronze(spark: SparkSession) -> None:
    write_parquet(
        read_csv(spark, paths.BRONZE_DIVIDENDS_CSV),
        paths.BRONZE_DIVIDENDS_PARQUET,
        partitions=1,
    )
    write_parquet(
        read_csv(spark, paths.BRONZE_EARNINGS_CSV),
        paths.BRONZE_EARNINGS_PARQUET,
        partitions=1,
    )
    write_parquet(
        read_csv(spark, paths.BRONZE_PRICES_CSV),
        paths.BRONZE_PRICES_PARQUET,
        partitions=1,
    )
