"""
SCD Type 2 maintainer for Silver prices.

Inputs:
- silver/prices (non historised)
- silver_scd2/prices (existing history, optional)

Output:
- silver_scd2/prices with columns:
    ts_utc, date, asset_type, symbol, interval, currency, price, source, run_id, ingest_ts,
    valid_from, valid_to, is_current, hash_diff

Keys: asset_type, symbol, currency, interval
Hash diff: on ts_utc + price
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


def read_silver_prices(spark, bucket, silver_prefix, date):
    base = f"gs://{bucket}/{silver_prefix}/prices"
    df = spark.read.parquet(base)
    if date:
        df = df.filter(F.col("date") == F.lit(date))
    return df


def read_existing_scd2(spark, bucket, prefix):
    path = f"gs://{bucket}/{prefix}/prices"
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def compute_hash(df):
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("ts_utc").cast(T.StringType()), F.lit("")),
            F.coalesce(F.col("price").cast(T.StringType()), F.lit("")),
        ),
        256,
    )


def dedupe_latest(df, key_cols):
    w = Window.partitionBy(*key_cols).orderBy(F.col("ingest_ts").desc(), F.col("ts_utc").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("scd2_apply_prices")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    key_cols = ["asset_type", "symbol", "currency", "interval"]
    silver_df = read_silver_prices(spark, args.gcs_bucket, args.silver_prefix, args.date)
    silver_df = silver_df.withColumn("hash_diff", compute_hash(silver_df))
    silver_df = dedupe_latest(silver_df, key_cols)

    existing = read_existing_scd2(spark, args.gcs_bucket, args.silver_scd2_prefix)
    if existing is None:
        # bootstrap
        bootstrap = (
            silver_df.withColumn("valid_from", F.col("ingest_ts"))
            .withColumn("valid_to", F.lit(None).cast(T.TimestampType()))
            .withColumn("is_current", F.lit(True))
        )
        (
            bootstrap.write.mode("overwrite")
            .partitionBy("asset_type", "symbol", "date")
            .parquet(f"gs://{args.gcs_bucket}/{args.silver_scd2_prefix}/prices")
        )
        print(f"Bootstrap SCD2 prices rows: {bootstrap.count()}")
        spark.stop()
        return

    existing = existing.withColumn("hash_diff", F.col("hash_diff"))
    current = existing.filter(F.col("is_current") == F.lit(True))
    historical = existing.filter(F.col("is_current") == F.lit(False))

    # Join new vs current
    join_cond = [silver_df[k] == current[k] for k in key_cols]
    joined = silver_df.join(current, join_cond, "left")

    changed = joined.filter(
        (current["hash_diff"].isNull()) | (silver_df["hash_diff"] != current["hash_diff"])
    ).select(silver_df["*"], current["ingest_ts"].alias("prev_ingest_ts"))

    # Close previous versions where applicable
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
        .parquet(f"gs://{args.gcs_bucket}/{args.silver_scd2_prefix}/prices")
    )

    print(f"SCD2 prices rows total: {final_df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
