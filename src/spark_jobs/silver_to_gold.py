"""
PySpark job: Silver -> Gold
---------------------------
- Reads Silver candles from GCS
- Produces:
    * gold/returns_daily
    * gold/volatility_30d
- Partitioning: asset_type / symbol / date

Args (env vars override defaults):
--gcs-bucket       GCS bucket name (default: market-lake-dev-eliott)
--silver-prefix    Prefix of silver root (default: silver)
--gold-prefix      Prefix of gold root (default: gold)
--date             Optional YYYY-MM-DD filter on candle date
"""

from argparse import ArgumentParser
from typing import Optional

from pyspark.sql import SparkSession, functions as F, Window


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--gcs-bucket", default="market-lake-dev-eliott")
    parser.add_argument("--silver-prefix", default="silver")
    parser.add_argument("--gold-prefix", default="gold")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD")
    return parser.parse_args()


def read_candles(spark: SparkSession, bucket: str, silver_prefix: str, date: Optional[str]):
    base_path = f"gs://{bucket}/{silver_prefix}/candles"
    path = base_path if date is None else f"{base_path}/date={date}"
    return spark.read.parquet(path)


def read_prices(spark: SparkSession, bucket: str, silver_prefix: str, date: Optional[str]):
    base_path = f"gs://{bucket}/{silver_prefix}/prices"
    df = spark.read.parquet(base_path)
    if date:
        df = df.filter(F.col("date") == F.lit(date))
    return df


def compute_returns_from_candles(candles_df):
    df = candles_df.filter(F.col("interval") == F.lit("1day"))
    w = Window.partitionBy("symbol").orderBy("ts_utc")
    df = df.withColumn("close_prev", F.lag("close").over(w))
    df = df.withColumn("return_1d", (F.col("close") / F.col("close_prev")) - F.lit(1.0))
    df = df.filter(F.col("return_1d").isNotNull())
    df = df.withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
    return df.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "return_1d",
        "close",
        "close_prev",
        "source",
    )


def compute_returns_from_prices(prices_df):
    # Keep USD when available to avoid mixing currencies
    prices_df = prices_df.withColumn("currency_l", F.lower(F.col("currency")))
    usd_df = prices_df.filter(F.col("currency_l") == "usd")
    non_usd_df = prices_df.filter(F.col("currency_l") != "usd")
    base_df = usd_df.unionByName(non_usd_df, allowMissingColumns=True)

    w = Window.partitionBy("symbol").orderBy("ts_utc")
    df = base_df.withColumn("close_prev", F.lag("price").over(w))
    df = df.withColumn("return_1d", (F.col("price") / F.col("close_prev")) - F.lit(1.0))
    df = df.filter(F.col("return_1d").isNotNull())
    df = df.withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
    return df.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "return_1d",
        F.col("price").alias("close"),
        "close_prev",
        "source",
    )


def compute_volatility(returns_df, window_days: int = 30, min_days: int = 10):
    w = Window.partitionBy("symbol").orderBy("ts_utc").rowsBetween(-(window_days - 1), 0)
    df = returns_df.withColumn("obs_count", F.count("return_1d").over(w))
    df = df.withColumn("volatility_raw", F.stddev_samp("return_1d").over(w))
    df = df.withColumn(
        "volatility",
        F.when(F.col("obs_count") >= min_days, F.col("volatility_raw")),
    )
    df = df.filter(F.col("volatility").isNotNull())
    df = df.withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
    df = df.withColumn("window_days", F.lit(window_days))
    return df.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "volatility",
        "window_days",
    )


def write_partitioned(df, path: str):
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("asset_type", "symbol", "date")
        .parquet(path)
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("silver_to_gold")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    candles_df = read_candles(spark, args.gcs_bucket, args.silver_prefix, args.date)
    prices_df = read_prices(spark, args.gcs_bucket, args.silver_prefix, args.date)

    returns_candles = compute_returns_from_candles(candles_df)
    returns_prices = compute_returns_from_prices(prices_df)
    returns_df = returns_candles.unionByName(returns_prices, allowMissingColumns=True)

    volatility_df = compute_volatility(returns_df, window_days=30, min_days=10)

    write_partitioned(
        returns_df, f"gs://{args.gcs_bucket}/{args.gold_prefix}/returns_daily"
    )
    write_partitioned(
        volatility_df, f"gs://{args.gcs_bucket}/{args.gold_prefix}/volatility_30d"
    )

    print(f"returns_daily rows: {returns_df.count()}")
    print(f"volatility_30d rows: {volatility_df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
