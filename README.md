# Market Data Lake – Quickstart

Small data lake prototype that ingests market data (stocks, crypto, forex) to GCS, builds Silver/Gold parquet layers on Dataproc Serverless, serves them via a FastAPI, and renders a React UI with a lightweight ML signal.

## Stack
- **Ingestion (local)**: Python scripts under `src/ingestion` push Bronze JSONL to GCS.
- **Transform (serverless)**: PySpark jobs under `src/spark_jobs` run on Dataproc Serverless to create Silver (`prices`, `candles`) and Gold (`returns_daily`, `volatility_30d`, SCD2).
- **API**: `src/tools/gold_api.py` (FastAPI) reads Gold from GCS and exposes `/returns`, `/volatility`, `/symbols`, `/recommendation`, `/predict`.
- **ML**: `src/tools/ml_predict.py` trains a simple lasso per asset and writes `gold/predictions/latest.json`.
- **Frontend**: `webapp` (Vite/React) visualizes series, recommendations, and ML predictions.

## Prerequisites
- Python 3.10+ with virtualenv
- Node 18+ (for the UI)
- Google Cloud SDK (`gcloud`, `gsutil`) authenticated with ADC:
  ```bash
  gcloud auth application-default login
  gcloud auth application-default set-quota-project market-lake-dev-eliott
  ```
- GCP project/bucket variables (defaults used below):
  ```
  GCS_BUCKET=market-lake-dev-eliott
  GOLD_PREFIX=gold
  SILVER_PREFIX=silver
  BRONZE_PREFIX=bronze
  ```

## Setup
```bash
python -m venv .venv
. .venv/Scripts/activate   # on Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Install UI deps:
```bash
cd webapp && npm install && cd ..
```

## End-to-end daily run (example for 2026-03-05)
```powershell
# Stocks (TwelveData)
$env:TD_SYMBOLS="AAPL,MSFT,GOOGL,AMZN"
$env:TD_INTERVAL="1day"
python src/ingestion/stocks_twelvedata_to_bronze_local.py

# Crypto spot (CoinGecko simple price)
python src/ingestion/crypto_coingecko_to_bronze_local.py

# Forex (Frankfurter)
python src/ingestion/forex_frankfurter_to_bronze_local.py

# Bronze -> Silver -> Gold (Dataproc Serverless)
./run_all.ps1   # uploads spark jobs then submits bronze_to_silver and silver_to_gold
```

## Backfill examples
- Stocks longer history:
  ```powershell
  $env:TD_START_DATE="2025-11-01"
  $env:TD_OUTPUTSIZE="5000"
  python src/ingestion/stocks_twelvedata_to_bronze_local.py
  ```
- Crypto full history (market_chart):
  ```powershell
  $env:COINGECKO_IDS="bitcoin,ethereum"
  $env:VS_CURRENCY="usd"
  $env:DAYS="max"
  python src/ingestion/crypto_coingecko_history_to_bronze_local.py
  ```
- Forex range:
  ```powershell
  $env:FX_FROM="2025-12-01"
  $env:FX_TO="2026-03-05"
  python src/ingestion/forex_frankfurter_to_bronze_local.py
  ```
Then rerun `./run_all.ps1` (or submit `bronze_to_silver.py` / `silver_to_gold.py` manually) without `--date` to recompute all partitions.

## SCD2 maintenance
Optional history tables:
```powershell
gcloud dataproc batches submit pyspark gs://$env:GCS_BUCKET/dependencies/scd2_apply_prices.py `
  --project=$env:GCP_PROJECT --region=europe-west1 --version=2.1 `
  --properties spark.sql.session.timeZone=UTC,spark.sql.files.ignoreMissingFiles=true `
  -- --gcs-bucket=$env:GCS_BUCKET --silver-prefix=silver --silver-scd2-prefix=silver_scd2
```
Repeat with `scd2_apply_candles.py`.

## ML predictions
```powershell
pip install scikit-learn      # if not already
python src/tools/ml_predict.py
```
Writes: `gs://<bucket>/gold/predictions/latest.json`.

## Run the API locally
```powershell
uvicorn src.tools.gold_api:app --reload --port 8000
```
Endpoints: `/returns`, `/volatility`, `/symbols`, `/recommendation`, `/predict`, `/healthz`.

## Run the frontend
```bash
cd webapp
npm run dev   # opens http://localhost:5173 (proxy /api to localhost:8000)
```

## Troubleshooting
- **ADC project mismatch**: ensure `gcloud auth application-default set-quota-project <project>`.
- **PyArrow “outside base dir”**: in local runs we use bucket-relative paths with filesystem set.
- **Missing predictions**: run `ml_predict.py`; API returns empty predictions if file absent.

## Repo structure (key parts)
```
src/
  ingestion/            # Bronze loaders (stocks, crypto, forex)
  spark_jobs/           # PySpark jobs (bronze_to_silver, silver_to_gold, SCD2)
  tools/                # gold_api (FastAPI), ml_predict, check_sources
webapp/                 # React UI (Vite)
run_all.ps1             # convenience orchestrator for daily batch
```
