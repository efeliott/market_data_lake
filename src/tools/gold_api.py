"""
FastAPI micro-API to read Gold datasets from GCS and serve simple views/predictions.

Endpoints:
  GET /returns?asset_type=stock&symbol=AAPL&limit=100
  GET /volatility?asset_type=stock&symbol=AAPL&limit=100
  GET /recommendation?asset_type=stock&symbol=AAPL

Environment:
  GCP_PROJECT (optional)
  GCS_BUCKET (default: market-lake-dev-eliott)
  GOLD_PREFIX (default: gold)

Run locally:
  uvicorn src.tools.gold_api:app --reload --port 8000
"""

import os
import json
from functools import lru_cache
from typing import Optional

import numpy as np
import pandas as pd
import gcsfs
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


BUCKET = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "gold")
PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
GCSFS_PROJECT = os.environ.get("GCSFS_PROJECT")  # optional explicit override


def gcs_filesystem():
    # Avoid project mismatch; let ADC default project resolve unless explicitly overridden
    if GCSFS_PROJECT:
        return gcsfs.GCSFileSystem(project=GCSFS_PROJECT, token="google_default")
    return gcsfs.GCSFileSystem(token="google_default")


@lru_cache(maxsize=4)
def load_gold(dataset: str) -> pd.DataFrame:
    """Load a Gold parquet dataset from GCS with light caching to avoid re-reads."""
    fs = gcs_filesystem()
    rel_path = f"{BUCKET}/{GOLD_PREFIX}/{dataset}"
    full_path = f"gs://{rel_path}"
    try:
        return pd.read_parquet(rel_path, filesystem=fs)
    except FileNotFoundError:
        try:
            return pd.read_parquet(full_path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"Dataset not found: {full_path}") from exc


def load_predictions_json():
    fs = gcs_filesystem()
    path = f"{BUCKET}/{GOLD_PREFIX}/predictions/latest.json"
    try:
        with fs.open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        # If predictions are absent, return an empty payload instead of 404 so the UI can stay graceful.
        return {"generated_at": None, "predictions": []}


def filter_df(df: pd.DataFrame, asset_type: str, symbol: str, limit: int) -> pd.DataFrame:
    # Keep only the requested series, newest first, capped by limit
    filtered = df[(df["asset_type"] == asset_type) & (df["symbol"] == symbol)]
    filtered = filtered.sort_values("ts_utc", ascending=False).head(limit)
    return filtered


def make_recommendation(df_returns: pd.DataFrame) -> dict:
    # Tiny slope-based heuristic: positive slope => buy, negative => sell, else hold.
    if df_returns.empty:
        return {"action": "insufficient-data"}
    closes = df_returns.sort_values("ts_utc")["close"].tail(30)
    if len(closes) < 5:
        return {"action": "insufficient-data"}
    x = np.arange(len(closes))
    slope = float(np.polyfit(x, closes.values, 1)[0])
    last_close = float(closes.iloc[-1])
    # Simple rule-based signal on slope relative to price level
    rel_slope = slope / last_close if last_close else 0.0
    if rel_slope > 0.002:
        action = "buy"
    elif rel_slope < -0.002:
        action = "sell"
    else:
        action = "hold"
    return {"action": action, "slope": slope, "rel_slope": rel_slope, "last_close": last_close}


class SeriesResponse(BaseModel):
    ts_utc: list
    value: list


app = FastAPI(title="Market Lake Gold API", version="0.1.0")


@app.get("/returns")
def get_returns(asset_type: str, symbol: str, limit: int = Query(100, le=1000)):
    df = load_gold("returns_daily")
    out = filter_df(df, asset_type, symbol, limit)
    return out.to_dict(orient="records")


@app.get("/symbols")
def list_symbols(asset_type: Optional[str] = None):
    df = load_gold("returns_daily")
    if asset_type:
        df = df[df["asset_type"] == asset_type]
    uniq = df[["asset_type", "symbol"]].drop_duplicates().sort_values(["asset_type", "symbol"])
    return uniq.to_dict(orient="records")


@app.get("/predict")
def get_predictions(asset_type: Optional[str] = None, symbol: Optional[str] = None):
    data = load_predictions_json()
    preds = data.get("predictions", [])
    if asset_type:
        preds = [p for p in preds if p.get("asset_type") == asset_type]
    if symbol:
        preds = [p for p in preds if p.get("symbol") == symbol]
    return {"generated_at": data.get("generated_at"), "predictions": preds}


@app.get("/volatility")
def get_volatility(asset_type: str, symbol: str, limit: int = Query(100, le=1000)):
    df = load_gold("volatility_30d")
    out = filter_df(df, asset_type, symbol, limit)
    return out.to_dict(orient="records")


@app.get("/recommendation")
def recommendation(asset_type: str, symbol: str):
    df = load_gold("returns_daily")
    sub = filter_df(df, asset_type, symbol, 365)
    rec = make_recommendation(sub)
    rec["asset_type"] = asset_type
    rec["symbol"] = symbol
    return rec


@app.get("/healthz")
def health():
    return {"status": "ok"}
