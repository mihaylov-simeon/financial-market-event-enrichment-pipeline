from pyspark.sql import SparkSession, DataFrame

def spark_session(app_name: str) -> SparkSession:
    spark = (
       SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", False)
        .csv(path)
    )

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def write_parquet(df: DataFrame, path: str, partitions: int | None = None) -> None:
    writer = df
    
    if partitions is not None:
        writer = df.coalesce(partitions)

    writer.write.mode("overwrite").parquet(path)