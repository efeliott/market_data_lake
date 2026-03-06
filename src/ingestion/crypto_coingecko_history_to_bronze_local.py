import gzip
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from google.cloud import storage


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


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


def fetch_market_chart(base_url: str, coin_id: str, vs_currency: str, days: str, from_ts=None, to_ts=None):
    if from_ts and to_ts:
        url = f"{base_url}/coins/{coin_id}/market_chart/range"
        params = {"vs_currency": vs_currency, "from": from_ts, "to": to_ts}
    else:
        url = f"{base_url}/coins/{coin_id}/market_chart"
        params = {"vs_currency": vs_currency, "days": days}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def write_gcs_bytes(client: storage.Client, bucket: str, object_name: str, data: bytes):
    blob = client.bucket(bucket).blob(object_name)
    blob.upload_from_string(data)


def main():
    load_dotenv()
    base_url = os.environ.get("COINGECKO_URL", "https://api.coingecko.com/api/v3")
    ids_raw = os.environ.get("COINGECKO_IDS", "bitcoin,ethereum")
    vs_currency = os.environ.get("VS_CURRENCY", "usd")
    days = os.environ.get("DAYS", "90")  # or "max"
    from_ts = os.environ.get("FROM_TS")  # optional unix seconds
    to_ts = os.environ.get("TO_TS")      # optional unix seconds

    bucket = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze")

    coin_ids: List[str] = [c.strip() for c in ids_raw.split(",") if c.strip()]
    ingest_ts = utc_now_iso()
    date_str = utc_today()
    client = gcs_client()

    for coin in coin_ids:
        run_id = str(uuid.uuid4())
        base_path = f"{bronze_prefix}/coingecko/crypto_market_chart/date={date_str}/coin={coin}"
        if manifest_exists(client, bucket, base_path):
            print(f"SKIP already ingested coin={coin} date={date_str}")
            continue

        payload = fetch_market_chart(base_url, coin, vs_currency, days, from_ts, to_ts)
        record: Dict[str, Any] = {
            "source": "coingecko_market_chart",
            "dataset": "crypto_market_chart",
            "run_id": run_id,
            "ingest_ts": ingest_ts,
            "coin": coin,
            "vs_currency": vs_currency,
            "params": {"days": days, "from_ts": from_ts, "to_ts": to_ts},
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
            "source": "coingecko_market_chart",
            "dataset": "crypto_market_chart",
            "ingest_ts": ingest_ts,
            "date": date_str,
            "request_params": {"coin": coin, "vs_currency": vs_currency, "days": days, "from_ts": from_ts, "to_ts": to_ts},
            "record_count": len(payload.get("prices", [])),
            "status": "success",
        }
        write_gcs_bytes(client, bucket, manifest_object, json.dumps(manifest).encode("utf-8"))

        print(f"OK crypto history -> GCS coin={coin} prices={len(payload.get('prices', []))}")


if __name__ == "__main__":
    main()
