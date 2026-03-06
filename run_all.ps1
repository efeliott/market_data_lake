# Orchestration script: full pipeline Bronze -> Silver -> Gold -> SCD2
# Usage (PowerShell):
#   $env:SA="544302055206-compute@developer.gserviceaccount.com"
#   $env:RUN_DATE="2026-03-04"           # optional; defaults to today's UTC date
#   ./run_all.ps1
#
# Prérequis: gcloud, gsutil disponibles, ADC ou auth gcloud active.

param()

$Bucket     = $env:GCS_BUCKET     | ForEach-Object { if ($_ -ne $null -and $_ -ne "") { $_ } else { "market-lake-dev-eliott" } }
$BronzePref = $env:BRONZE_PREFIX  | ForEach-Object { if ($_ -ne $null -and $_ -ne "") { $_ } else { "bronze" } }
$SilverPref = $env:SILVER_PREFIX  | ForEach-Object { if ($_ -ne $null -and $_ -ne "") { $_ } else { "silver" } }
$GoldPref   = $env:GOLD_PREFIX    | ForEach-Object { if ($_ -ne $null -and $_ -ne "") { $_ } else { "gold" } }
$SilverSCD2 = $env:SILVER_SCD2_PREFIX | ForEach-Object { if ($_ -ne $null -and $_ -ne "") { $_ } else { "silver_scd2" } }
$SA         = $env:SA
if (-not $SA) { Write-Error 'Set service account in $env:SA'; exit 1 }
$Date       = $env:RUN_DATE
# Historique optionally
$FxFrom     = $env:FX_FROM
$FxTo       = $env:FX_TO
$TdStart    = $env:TD_START_DATE
$TdEnd      = $env:TD_END_DATE
$TdOutput   = $env:TD_OUTPUTSIZE

# 1) Upload latest spark jobs to GCS deps
Write-Host "== Upload spark jobs to gs://$Bucket/dependencies/"
gsutil cp src/spark_jobs/bronze_to_silver.py    gs://$Bucket/dependencies/bronze_to_silver.py
gsutil cp src/spark_jobs/silver_to_gold.py      gs://$Bucket/dependencies/silver_to_gold.py
gsutil cp src/spark_jobs/scd2_apply_prices.py   gs://$Bucket/dependencies/scd2_apply_prices.py
gsutil cp src/spark_jobs/scd2_apply_candles.py  gs://$Bucket/dependencies/scd2_apply_candles.py

# 2) Ingestion runs (idempotent)
Write-Host "== Ingestion: CoinGecko"
python src/ingestion/crypto_coingecko_to_bronze_local.py
Write-Host "== Ingestion: Frankfurter"
if ($FxFrom -and $FxTo) {
  $env:FX_FROM=$FxFrom; $env:FX_TO=$FxTo
}
python src/ingestion/forex_frankfurter_to_bronze_local.py
Write-Host "== Ingestion: TwelveData"
if ($TdStart) { $env:TD_START_DATE=$TdStart }
if ($TdEnd)   { $env:TD_END_DATE=$TdEnd }
if ($TdOutput){ $env:TD_OUTPUTSIZE=$TdOutput }
python src/ingestion/stocks_twelvedata_to_bronze_local.py

# 3) Bronze -> Silver
$dateArg = ""
if ($Date) { $dateArg = "--date=$Date" }
Write-Host "== Dataproc: bronze_to_silver $dateArg"
gcloud dataproc batches submit pyspark gs://$Bucket/dependencies/bronze_to_silver.py `
  --project=$Bucket `
  --region=europe-west1 `
  --service-account=$SA `
  --version=2.1 `
  --properties spark.sql.shuffle.partitions=200,spark.sql.session.timeZone=UTC `
  -- `
  --gcs-bucket=$Bucket `
  --bronze-prefix=$BronzePref `
  --silver-prefix=$SilverPref `
  $dateArg

# 4) Silver -> Gold
Write-Host "== Dataproc: silver_to_gold"
gcloud dataproc batches submit pyspark gs://$Bucket/dependencies/silver_to_gold.py `
  --project=$Bucket `
  --region=europe-west1 `
  --service-account=$SA `
  --version=2.1 `
  --properties spark.sql.shuffle.partitions=200,spark.sql.session.timeZone=UTC `
  -- `
  --gcs-bucket=$Bucket `
  --silver-prefix=$SilverPref `
  --gold-prefix=$GoldPref

# 5) SCD2 Prices
Write-Host "== Dataproc: scd2_apply_prices"
gcloud dataproc batches submit pyspark gs://$Bucket/dependencies/scd2_apply_prices.py `
  --project=$Bucket `
  --region=europe-west1 `
  --service-account=$SA `
  --version=2.1 `
  --properties spark.sql.shuffle.partitions=200,spark.sql.session.timeZone=UTC `
  -- `
  --gcs-bucket=$Bucket `
  --silver-prefix=$SilverPref `
  --silver-scd2-prefix=$SilverSCD2 `
  $dateArg

# 6) SCD2 Candles
Write-Host "== Dataproc: scd2_apply_candles"
gcloud dataproc batches submit pyspark gs://$Bucket/dependencies/scd2_apply_candles.py `
  --project=$Bucket `
  --region=europe-west1 `
  --service-account=$SA `
  --version=2.1 `
  --properties spark.sql.shuffle.partitions=200,spark.sql.session.timeZone=UTC `
  -- `
  --gcs-bucket=$Bucket `
  --silver-prefix=$SilverPref `
  --silver-scd2-prefix=$SilverSCD2 `
  $dateArg

Write-Host "== Done"
