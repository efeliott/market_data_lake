# Market Lake Runbook (GCP + Spark)

## Prérequis (à faire manuellement)
- Bucket GCS existant : `gs://market-lake-dev-eliott`.
- IAM : le service account Dataproc Serverless **et** Cloud Run Jobs doit avoir `storage.objectAdmin` sur le bucket (lecture/écriture Bronze/Silver/Gold).
- Secret/API keys : `TWELVEDATA_API_KEY` injecté dans Cloud Run Job; aucune clé commitée.
- Réseau : fournir `--subnet` si l’org le requiert; sinon Dataproc Serverless default subnet.

## Activer les APIs
```powershell
gcloud services enable dataproc.googleapis.com run.googleapis.com cloudscheduler.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com --project=market-lake-dev-eliott
```

## Ingestion (Cloud Run Jobs) – idempotent + manifest
- Scripts : `src/ingestion/*.py` écrivent `data.jsonl.gz` + `manifest.json` sous `bronze/<source>/<dataset>/date=YYYY-MM-DD/run_id=<uuid>/`.
- Idempotence : si un `manifest.json` `status=success` existe pour la date/dataset, le run loggue `SKIP ...` et sort proprement.
- Variables communes : `GCS_BUCKET`, `BRONZE_PREFIX` (default `bronze`).

## Soumettre les jobs Spark (Dataproc Serverless)
> Les fichiers peuvent être passés depuis la machine locale; Dataproc les upload automatiquement. Remplace `SERVICE_ACCOUNT` si besoin.

### Bronze -> Silver
```powershell
gcloud dataproc batches submit pyspark spark_jobs/bronze_to_silver.py `
  --project=market-lake-dev-eliott `
  --region=europe-west1 `
  --service-account=SERVICE_ACCOUNT `
  --properties spark.sql.shuffle.partitions=200 `
  -- `
  --gcs-bucket=market-lake-dev-eliott `
  --bronze-prefix=bronze `
  --silver-prefix=silver `
  --date=2026-02-27   # optionnel
```

### Silver -> Gold
```powershell
gcloud dataproc batches submit pyspark spark_jobs/silver_to_gold.py `
  --project=market-lake-dev-eliott `
  --region=europe-west1 `
  --service-account=SERVICE_ACCOUNT `
  --properties spark.sql.shuffle.partitions=200 `
  -- `
  --gcs-bucket=market-lake-dev-eliott `
  --silver-prefix=silver `
  --gold-prefix=gold
```

### SCD2 Prices
```powershell
gcloud dataproc batches submit pyspark spark_jobs/scd2_apply_prices.py `
  --project=market-lake-dev-eliott `
  --region=europe-west1 `
  --service-account=SERVICE_ACCOUNT `
  --properties spark.sql.shuffle.partitions=200 `
  -- `
  --gcs-bucket=market-lake-dev-eliott `
  --silver-prefix=silver `
  --silver-scd2-prefix=silver_scd2 `
  --date=2026-02-27   # optionnel
```

### SCD2 Candles
```powershell
gcloud dataproc batches submit pyspark spark_jobs/scd2_apply_candles.py `
  --project=market-lake-dev-eliott `
  --region=europe-west1 `
  --service-account=SERVICE_ACCOUNT `
  --properties spark.sql.shuffle.partitions=200 `
  -- `
  --gcs-bucket=market-lake-dev-eliott `
  --silver-prefix=silver `
  --silver-scd2-prefix=silver_scd2 `
  --date=2026-02-27   # optionnel
```

## Orchestration Cloud Scheduler -> Cloud Run Job
URL d’exécution d’un Cloud Run Job : `https://europe-west1-run.googleapis.com/v1/namespaces/market-lake-dev-eliott/jobs/<job-name>:run`

Exemple (ingestion crypto toutes les 5 minutes) :
```powershell
gcloud scheduler jobs create http ingest-crypto-5m `
  --location=europe-west1 `
  --schedule="*/5 * * * *" `
  --uri="https://europe-west1-run.googleapis.com/v1/namespaces/market-lake-dev-eliott/jobs/crypto-coingecko:run" `
  --http-method=POST `
  --oidc-service-account-email=SCHEDULER_SA `
  --oidc-token-audience="https://europe-west1-run.googleapis.com/"
```

Autres cadences (adapter le nom du job Cloud Run) :
- Forex daily : `0 6 * * *`
- Stocks daily : `0 5 * * *`
- bronze_to_silver hourly : `0 * * * *`
- silver_to_gold daily : `10 6 * * *`
- SCD2 daily : `20 6 * * *`

## Vérifications rapides
- Lister partitions : `gsutil ls gs://market-lake-dev-eliott/silver/prices/asset_type=crypto/symbol=bitcoin/`
- Compter objets : `python scripts/verify_gcs_outputs.py --bucket market-lake-dev-eliott --prefix silver/prices`
- Vérifier manifest : `gsutil cat gs://market-lake-dev-eliott/bronze/coingecko/crypto_simple_price/date=2026-02-27/run_id=.../manifest.json`

## Diagnostic
- Logs Dataproc batch : `gcloud dataproc batches describe <batch-id> --region=europe-west1 --format="value(state,stateHistory,stateMessage)"` puis suivre le `driverOutputResourceUri`.
- Logs Cloud Run Job : `gcloud run jobs executions list --job=<name> --region=europe-west1` puis `gcloud run jobs executions logs --execution=<id>`.

## Rappels
- Partitionnement Hive partout : `asset_type/symbol/date`.
- Tous les jobs PySpark sont timezone UTC (`spark.sql.session.timeZone=UTC`).
- Manifests assurent l’idempotence de l’ingestion; relancer la même date ne réécrit pas Bronze.
- SCD2 jobs sont idempotents : si aucun changement de `hash_diff`, aucune nouvelle ligne n’est ajoutée.
