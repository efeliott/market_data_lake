import gzip
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from dotenv import load_dotenv
from google.cloud import storage


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_prices(base_url: str, ids: str, vs_currencies: str) -> dict:
    params = {"ids": ids, "vs_currencies": vs_currencies}
    r = requests.get(base_url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def gcs_client():
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    return storage.Client(project=project)


def manifest_exists(client: storage.Client, bucket: str, prefix: str) -> bool:
    for blob in client.list_blobs(bucket, prefix=prefix):
        if blob.name.endswith("manifest.json"):
            content = json.loads(blob.download_as_text())
            if content.get("status") == "success":
                return True
    return False


def write_gcs_bytes(client: storage.Client, bucket: str, object_name: str, data: bytes):
    blob = client.bucket(bucket).blob(object_name)
    blob.upload_from_string(data)


def main():
    load_dotenv()

    base_url = os.environ.get("COINGECKO_URL", "https://api.coingecko.com/api/v3/simple/price")
    crypto_ids = os.environ.get("CRYPTO_IDS", "bitcoin,ethereum")
    vs = os.environ.get("VS_CURRENCIES", "usd,eur")
    bucket = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze")

    run_id = str(uuid.uuid4())        # identifiant unique du run (traçabilité)
    ingest_ts = utc_now_iso()
    date_str = utc_today()

    client = gcs_client()
    base_path = f"{bronze_prefix}/coingecko/crypto_simple_price/date={date_str}"
    manifest_prefix = base_path

    if manifest_exists(client, bucket, manifest_prefix):
        print(f"SKIP already ingested date={date_str} dataset=crypto_simple_price")
        return

    payload = fetch_prices(base_url, crypto_ids, vs)

    record: Dict[str, Any] = {
        "source": "coingecko",
        "dataset": "crypto_simple_price",
        "run_id": run_id,
        "ingest_ts": ingest_ts,
        "ids": crypto_ids.split(","),
        "vs_currencies": vs.split(","),
        "data": payload,
    }

    object_base = f"{base_path}/run_id={run_id}"
    data_object = f"{object_base}/data.jsonl.gz"
    manifest_object = f"{object_base}/manifest.json"

    line = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
    compressed = gzip.compress(line)
    write_gcs_bytes(client, bucket, data_object, compressed)

    manifest = {
        "run_id": run_id,
        "source": "coingecko",
        "dataset": "crypto_simple_price",
        "ingest_ts": ingest_ts,
        "date": date_str,
        "request_params": {"ids": crypto_ids, "vs_currencies": vs},
        "record_count": 1,
        "status": "success",
    }
    write_gcs_bytes(client, bucket, manifest_object, json.dumps(manifest).encode("utf-8"))

    print("OK crypto bronze -> GCS")
    print(f"data: gs://{bucket}/{data_object}")
    print(f"manifest: gs://{bucket}/{manifest_object}")


if __name__ == "__main__":
    main()
