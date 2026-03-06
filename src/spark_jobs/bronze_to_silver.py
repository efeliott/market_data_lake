"""
PySpark job: Bronze -> Silver
--------------------------------
- Reads Bronze JSONL.gz on GCS
- Normalises into two Silver Parquet datasets:
    * silver/prices  (crypto spot + forex daily)
    * silver/candles (stocks/etf daily OHLCV)
- Partitioning: asset_type / symbol / date

Arguments (env vars override defaults):
--gcs-bucket       GCS bucket name (default: market-lake-dev-eliott)
--bronze-prefix    Prefix of bronze root inside bucket (default: bronze)
--silver-prefix    Prefix of silver root inside bucket (default: silver)
--date             Optional YYYY-MM-DD; processes only that date partition
--source           Optional filter (coingecko|frankfurter|twelvedata)

Designed for Dataproc Serverless (Spark 3.5+ Python 3.11).
"""

from argparse import ArgumentParser
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--gcs-bucket", default="market-lake-dev-eliott")
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--silver-prefix", default="silver")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--source", default=None, help="coingecko|frankfurter|twelvedata")
    return parser.parse_args()


def build_bronze_paths(bucket: str, bronze_prefix: str, date: Optional[str]) -> list:
    date_part = f"date={date}" if date else "date=*"
    # Two patterns: classic run_id and symbol=.../run_id
    p1 = f"gs://{bucket}/{bronze_prefix}/*/*/{date_part}/run_id=*/data.jsonl.gz"
    p2 = f"gs://{bucket}/{bronze_prefix}/*/*/{date_part}/symbol=*/run_id=*/data.jsonl.gz"
    return [p1, p2]


def read_bronze(spark: SparkSession, paths: list):
    # Load every JSONL.gz under the patterns; keep ingest timestamp + filename for lineage.
    return (
        spark.read.option("recursiveFileLookup", "true")
        .json(paths)
        .withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
        .withColumn("input_file", F.input_file_name())
    )


def transform_coingecko(bronze_df):
    df = bronze_df.filter(F.col("source") == F.lit("coingecko"))
    if df.rdd.isEmpty():
        return None

    # data is a map<asset, map<currency, price>> but Spark may have unified schemas
    # with other sources; re-parse to a clean map to avoid struct explosion errors.
    cg_map_schema = T.MapType(T.StringType(), T.MapType(T.StringType(), T.DoubleType()))
    parsed = df.select(
        "run_id",
        "ingest_ts",
        F.from_json(F.to_json(F.col("data")), cg_map_schema).alias("cg_map"),
    )

    exploded = (
        parsed.select("run_id", "ingest_ts", F.explode("cg_map").alias("asset_id", "quotes"))
        .select(
            "run_id",
            "ingest_ts",
            F.col("asset_id").alias("symbol"),
            F.explode("quotes").alias("currency", "price"),
        )
    )

    result = (
        exploded.withColumn("ts_utc", F.col("ingest_ts"))
        .withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
        .withColumn("asset_type", F.lit("crypto"))
        .withColumn("interval", F.lit("spot"))
        .withColumn("price", F.col("price").cast(T.DoubleType()))
        .withColumn("source", F.lit("coingecko"))
    )
    return result.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "interval",
        "currency",
        "price",
        "source",
        "run_id",
        "ingest_ts",
    )


def transform_coingecko_market_chart(bronze_df):
    df = bronze_df.filter(F.col("source") == F.lit("coingecko_market_chart"))
    if df.rdd.isEmpty():
        return None

    prices = (
        df.select("run_id", "ingest_ts", "coin", "vs_currency", F.col("data.prices").alias("prices"))
        .withColumn("price_struct", F.explode("prices"))
        .select(
            "run_id",
            "ingest_ts",
            "coin",
            "vs_currency",
            F.col("price_struct").getItem(0).alias("ts_ms"),
            F.col("price_struct").getItem(1).alias("price"),
        )
        .withColumn("ts_utc", F.to_timestamp(F.col("ts_ms") / 1000))
        .withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
        .withColumn("asset_type", F.lit("crypto"))
        .withColumn("symbol", F.col("coin"))
        .withColumn("interval", F.lit("1d"))  # daily aggregated prices
        .withColumn("currency", F.col("vs_currency"))
        .withColumn("source", F.lit("coingecko_market_chart"))
    )

    return prices.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "interval",
        "currency",
        F.col("price").cast(T.DoubleType()).alias("price"),
        "source",
        "run_id",
        "ingest_ts",
    )


def transform_frankfurter(bronze_df):
    df = bronze_df.filter(F.col("source") == F.lit("frankfurter"))
    if df.rdd.isEmpty():
        return None

    # Reparse rates to a map in case schema was widened with other sources
    rates_map = T.MapType(T.StringType(), T.DoubleType())
    parsed = df.select(
        "run_id",
        "ingest_ts",
        F.col("data.base").alias("base"),
        F.col("data.date").alias("fx_date"),
        F.from_json(F.to_json(F.col("data.rates")), rates_map).alias("rates_map"),
    )

    exploded = (
        parsed.select(
            "run_id",
            "ingest_ts",
            "base",
            "fx_date",
            F.explode("rates_map").alias("currency", "price"),
        )
    )

    ts_col = F.to_timestamp("fx_date")
    result = (
        exploded.withColumn("ts_utc", ts_col)
        .withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
        .withColumn("asset_type", F.lit("forex"))
        .withColumn("symbol", F.concat_ws("", F.col("base"), F.col("currency")))
        .withColumn("interval", F.lit("1d"))
        .withColumn("price", F.col("price").cast(T.DoubleType()))
        .withColumn("source", F.lit("frankfurter"))
    )
    return result.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "interval",
        "currency",
        "price",
        "source",
        "run_id",
        "ingest_ts",
    )


def transform_twelvedata(bronze_df):
    df = bronze_df.filter(F.col("source") == F.lit("twelvedata"))
    if df.rdd.isEmpty():
        return None

    # Filter status ok
    df_ok = df.filter(F.col("data.status") == F.lit("ok"))

    exploded = (
        df_ok.select(
            "run_id",
            "ingest_ts",
            "input_file",
            F.col("data.meta.symbol").alias("meta_symbol"),
            F.col("data.meta.currency").alias("meta_currency"),
            F.col("request.symbol").alias("req_symbol"),
            F.col("request.interval").alias("req_interval"),
            F.explode_outer("data.values").alias("val"),
        )
    )

    # Fallback: infer symbol from file path if payload misses it (handles symbol=... in folder)
    symbol_from_path = F.regexp_extract("input_file", r"symbol=([^/]+)", 1)
    symbol_col = F.coalesce("meta_symbol", "req_symbol", symbol_from_path, F.lit("UNKNOWN"))
    interval_col = F.coalesce("req_interval", F.lit("1day"))

    ts_raw = F.col("val.datetime")
    ts_col = F.when(
        F.length(ts_raw) == 10, F.to_timestamp(ts_raw, "yyyy-MM-dd")
    ).otherwise(F.to_timestamp(ts_raw))

    result = (
        exploded.withColumn("ts_utc", ts_col)
        .withColumn("date", F.date_format("ts_utc", "yyyy-MM-dd"))
        .withColumn("asset_type", F.lit("stock"))
        .withColumn("symbol", symbol_col)
        .withColumn("interval", interval_col)
        .withColumn("open", F.col("val.open").cast(T.DoubleType()))
        .withColumn("high", F.col("val.high").cast(T.DoubleType()))
        .withColumn("low", F.col("val.low").cast(T.DoubleType()))
        .withColumn("close", F.col("val.close").cast(T.DoubleType()))
        .withColumn(
            "volume",
            F.when(F.col("val.volume").isNull() | (F.col("val.volume") == ""), None).otherwise(
                F.col("val.volume").cast(T.DoubleType())
            ),
        )
        .withColumn("currency", F.col("meta_currency"))
        .withColumn("source", F.lit("twelvedata"))
    )

    result = result.filter(F.col("ts_utc").isNotNull())

    return result.select(
        "ts_utc",
        "date",
        "asset_type",
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "currency",
        "source",
        "run_id",
        "ingest_ts",
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
        SparkSession.builder.appName("bronze_to_silver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    bronze_paths = build_bronze_paths(args.gcs_bucket, args.bronze_prefix, args.date)
    bronze_df = read_bronze(spark, bronze_paths)

    if args.source:
        bronze_df = bronze_df.filter(F.col("source") == F.lit(args.source))

    # Debug counts per source
    src_counts = bronze_df.groupBy("source").count().collect()
    print("Bronze records per source:", {r['source']: r['count'] for r in src_counts})

    prices_parts = []
    candles_parts = []

    cg = transform_coingecko(bronze_df)
    if cg is not None:
        prices_parts.append(cg)

    ff = transform_frankfurter(bronze_df)
    if ff is not None:
        prices_parts.append(ff)

    cg_hist = transform_coingecko_market_chart(bronze_df)
    if cg_hist is not None:
        prices_parts.append(cg_hist)

    td = transform_twelvedata(bronze_df)
    if td is not None:
        print("TwelveData rows:", td.count(), "distinct symbols:", td.select("symbol").distinct().count())
        candles_parts.append(td)

    if prices_parts:
        prices_df = prices_parts[0]
        for part in prices_parts[1:]:
            prices_df = prices_df.unionByName(part, allowMissingColumns=True)
        write_partitioned(prices_df, f"gs://{args.gcs_bucket}/{args.silver_prefix}/prices")
        print(f"Wrote prices rows: {prices_df.count()}")
    else:
        print("No prices data to write.")

    if candles_parts:
        candles_df = candles_parts[0]
        for part in candles_parts[1:]:
            candles_df = candles_df.unionByName(part, allowMissingColumns=True)
        write_partitioned(candles_df, f"gs://{args.gcs_bucket}/{args.silver_prefix}/candles")
        print(f"Wrote candles rows: {candles_df.count()}")
    else:
        print("No candles data to write.")

    spark.stop()


if __name__ == "__main__":
    main()
