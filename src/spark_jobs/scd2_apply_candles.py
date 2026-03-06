"""
SCD Type 2 maintainer for Silver candles.

Inputs:
- silver/candles (non historised)
- silver_scd2/candles (existing history, optional)

Output columns:
    ts_utc, date, asset_type, symbol, interval, open, high, low, close,
    volume, currency, source, run_id, ingest_ts,
    valid_from, valid_to, is_current, hash_diff

Key: asset_type, symbol, interval, ts_utc
Hash diff: on OHLCV + currency
Partitioning: asset_type / symbol / date
"""

from argparse import ArgumentParser

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql import types as T


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--gcs-bucket", default="market-lake-dev-eliott")
    parser.add_argument("--silver-prefix", default="silver")
    parser.add_argument("--silver-scd2-prefix", default="silver_scd2")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD filter on input silver")
    return parser.parse_args()


def read_silver_candles(spark, bucket, silver_prefix, date):
    base = f"gs://{bucket}/{silver_prefix}/candles"
    df = spark.read.parquet(base)
    if date:
        df = df.filter(F.col("date") == F.lit(date))
    return df


def read_existing_scd2(spark, bucket, prefix):
    path = f"gs://{bucket}/{prefix}/candles"
    try:
        # Previous SCD2 state is optional; start fresh when missing.
        return spark.read.parquet(path)
    except Exception:
        return None


def compute_hash(df):
    # Hash every value that defines a change in the row; used to detect updates.
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast(T.StringType()), F.lit(""))
                for c in ["open", "high", "low", "close", "volume", "currency"]
            ]
        ),
        256,
    )


def dedupe_latest(df, key_cols):
    w = Window.partitionBy(*key_cols).orderBy(F.col("ingest_ts").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("scd2_apply_candles")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    key_cols = ["asset_type", "symbol", "interval", "ts_utc"]
    silver_df = read_silver_candles(spark, args.gcs_bucket, args.silver_prefix, args.date)
    silver_df = silver_df.withColumn("hash_diff", compute_hash(silver_df))
    silver_df = dedupe_latest(silver_df, key_cols)

    existing = read_existing_scd2(spark, args.gcs_bucket, args.silver_scd2_prefix)
    if existing is None:
        bootstrap = (
            silver_df.withColumn("valid_from", F.col("ingest_ts"))
            .withColumn("valid_to", F.lit(None).cast(T.TimestampType()))
            .withColumn("is_current", F.lit(True))
        )
        (
            bootstrap.write.mode("overwrite")
            .partitionBy("asset_type", "symbol", "date")
            .parquet(f"gs://{args.gcs_bucket}/{args.silver_scd2_prefix}/candles")
        )
        print(f"Bootstrap SCD2 candles rows: {bootstrap.count()}")
        spark.stop()
        return

    current = existing.filter(F.col("is_current") == F.lit(True))
    historical = existing.filter(F.col("is_current") == F.lit(False))

    join_cond = [silver_df[k] == current[k] for k in key_cols]
    joined = silver_df.join(current, join_cond, "left")

    changed = joined.filter(
        (current["hash_diff"].isNull()) | (silver_df["hash_diff"] != current["hash_diff"])
    ).select(silver_df["*"], current["ingest_ts"].alias("prev_ingest_ts"))

    to_close = (
        current.join(changed, [current[k] == changed[k] for k in key_cols], "inner")
        .select(current["*"], changed["ingest_ts"].alias("new_ingest_ts"))
        .withColumn("valid_to", F.col("new_ingest_ts"))
        .withColumn("is_current", F.lit(False))
    )

    new_rows = (
        changed.withColumn("valid_from", F.col("ingest_ts"))
        .withColumn("valid_to", F.lit(None).cast(T.TimestampType()))
        .withColumn("is_current", F.lit(True))
    )

    unchanged_currents = current.join(changed, [current[k] == changed[k] for k in key_cols], "left_anti")

    final_cols = existing.columns
    final_df = (
        historical.unionByName(unchanged_currents, allowMissingColumns=True)
        .unionByName(to_close.select(final_cols), allowMissingColumns=True)
        .unionByName(new_rows.select(final_cols), allowMissingColumns=True)
    )

    (
        final_df.write.mode("overwrite")
        .partitionBy("asset_type", "symbol", "date")
        .parquet(f"gs://{args.gcs_bucket}/{args.silver_scd2_prefix}/candles")
    )

    print(f"SCD2 candles rows total: {final_df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
