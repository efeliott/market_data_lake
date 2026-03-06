import gzip
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from dotenv import load_dotenv
from google.cloud import storage


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def utc_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_time_series(api_key: str, symbol: str, interval: str, outputsize: str, start_date: str = None, end_date: str = None) -> dict:
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": api_key,
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
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


def main():
    load_dotenv()

    api_key = os.environ["TWELVEDATA_API_KEY"]
    symbols_raw = os.environ.get("TD_SYMBOLS") or os.environ.get("TD_SYMBOL", "AAPL")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    interval = os.environ.get("TD_INTERVAL", "1day")
    outputsize = os.environ.get("TD_OUTPUTSIZE", "30")
    start_date = os.environ.get("TD_START_DATE")  # YYYY-MM-DD optional
    end_date = os.environ.get("TD_END_DATE")      # YYYY-MM-DD optional
    bucket = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze")

    client = gcs_client()
    ingest_ts = utc_now_iso()
    date_str = utc_today()

    for symbol in symbols:
        run_id = str(uuid.uuid4())
        base_path = f"{bronze_prefix}/twelvedata/time_series/date={date_str}/symbol={symbol}"

        if manifest_exists(client, bucket, base_path):
            print(f"SKIP already ingested date={date_str} symbol={symbol}")
            continue

        payload = fetch_time_series(api_key, symbol, interval, outputsize, start_date, end_date)
        if payload.get("status") == "error":
            print(f"ERROR {symbol}: {payload}")
            continue

        record: Dict[str, Any] = {
            "source": "twelvedata",
            "dataset": "time_series",
            "run_id": run_id,
            "ingest_ts": ingest_ts,
            "request": {"symbol": symbol, "interval": interval, "outputsize": outputsize},
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
            "source": "twelvedata",
            "dataset": "time_series",
            "ingest_ts": ingest_ts,
            "date": date_str,
            "request_params": {"symbol": symbol, "interval": interval, "outputsize": outputsize},
            "record_count": 1,
            "status": "success",
        }
        write_gcs_bytes(client, bucket, manifest_object, json.dumps(manifest).encode("utf-8"))

        print(f"OK stocks bronze -> GCS symbol={symbol}")
        print(f"data: gs://{bucket}/{data_object}")
        print(f"manifest: gs://{bucket}/{manifest_object}")


if __name__ == "__main__":
    main()
