import gzip
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

import requests
from dotenv import load_dotenv
from google.cloud import storage


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def utc_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_fx(url: str, base: str, symbols: str) -> dict:
    params = {"base": base, "symbols": symbols}
    r = requests.get(url, params=params, timeout=30)
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


def ingest_one_day(client, bucket, bronze_prefix, base, symbols, url, date_str, ingest_ts):
    run_id = str(uuid.uuid4())
    base_path = f"{bronze_prefix}/frankfurter/forex_latest/date={date_str}"
    if manifest_exists(client, bucket, base_path):
        print(f"SKIP already ingested date={date_str} dataset=forex_latest")
        return

    url_for_date = url if "latest" not in url else url.replace("latest", date_str)
    payload = fetch_fx(url_for_date, base, symbols)

    record: Dict[str, Any] = {
        "source": "frankfurter",
        "dataset": "forex_latest",
        "run_id": run_id,
        "ingest_ts": ingest_ts,
        "request": {"base": base, "symbols": symbols.split(",")},
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
        "source": "frankfurter",
        "dataset": "forex_latest",
        "ingest_ts": ingest_ts,
        "date": date_str,
        "request_params": {"base": base, "symbols": symbols},
        "record_count": 1,
        "status": "success",
    }
    write_gcs_bytes(client, bucket, manifest_object, json.dumps(manifest).encode("utf-8"))

    print(f"OK forex bronze -> GCS date={date_str}")
    print(f"data: gs://{bucket}/{data_object}")


def main():
    load_dotenv()

    url = os.environ.get("FRANKFURTER_URL", "https://api.frankfurter.dev/v1/latest")
    base = os.environ.get("FX_BASE", "EUR")
    symbols = os.environ.get("FX_SYMBOLS", "USD,GBP")
    date_from = os.environ.get("FX_FROM")  # YYYY-MM-DD optional
    date_to = os.environ.get("FX_TO")      # YYYY-MM-DD optional
    bucket = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze")

    client = gcs_client()
    ingest_ts = utc_now_iso()

    if date_from and date_to:
        start = datetime.fromisoformat(date_from)
        end = datetime.fromisoformat(date_to)
        cur = start
        while cur <= end:
            ingest_one_day(client, bucket, bronze_prefix, base, symbols, url, cur.strftime("%Y-%m-%d"), ingest_ts)
            cur += timedelta(days=1)
    else:
        ingest_one_day(client, bucket, bronze_prefix, base, symbols, url, utc_today(), ingest_ts)


if __name__ == "__main__":
    main()
