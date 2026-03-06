import os
import json
from datetime import datetime, timezone
from typing import List, Dict

import numpy as np
import pandas as pd
import gcsfs
from sklearn.linear_model import Lasso


BUCKET = os.environ.get("GCS_BUCKET", "market-lake-dev-eliott")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "gold")
ALPHA = float(os.environ.get("ML_ALPHA", "0.001"))
BUY_THRESH = float(os.environ.get("ML_BUY_THRESH", "0.003"))
SELL_THRESH = float(os.environ.get("ML_SELL_THRESH", "-0.003"))
MIN_SAMPLES = int(os.environ.get("ML_MIN_SAMPLES", "20"))
LOOKBACK_RET = int(os.environ.get("ML_LOOKBACK_RET", "5"))
LOOKBACK_VOL = int(os.environ.get("ML_LOOKBACK_VOL", "3"))


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO8601."""
    return datetime.now(timezone.utc).isoformat()


def gcs_fs():
    # Let ADC pick the project; avoids mismatch errors when quota project differs.
    return gcsfs.GCSFileSystem(token="google_default")


def load_gold() -> pd.DataFrame:
    fs = gcs_fs()
    # With filesystem passed, use bucket-relative paths to avoid pyarrow "outside base dir" errors.
    ret_path = f"{BUCKET}/{GOLD_PREFIX}/returns_daily"
    vol_path = f"{BUCKET}/{GOLD_PREFIX}/volatility_30d"
    ret_files = fs.glob(f"{ret_path}/**/*.parquet")
    if not ret_files:
        print(f"No return parquet files found under {ret_path}")
        return pd.DataFrame()
    vol_files = fs.glob(f"{vol_path}/**/*.parquet")
    if not vol_files:
        print(f"No volatility parquet files found under {vol_path}")
        return pd.DataFrame()

    returns = pd.read_parquet(ret_files, filesystem=fs)
    vols = pd.read_parquet(vol_files, filesystem=fs)
    # merge on ts_utc/asset_type/symbol/date
    df = pd.merge(
        returns,
        vols[["ts_utc", "asset_type", "symbol", "volatility"]],
        on=["ts_utc", "asset_type", "symbol"],
        how="left",
    )
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values(["asset_type", "symbol", "ts_utc"])
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    # df grouped by asset_type+symbol, sorted
    frames = []
    for (atype, sym), g in df.groupby(["asset_type", "symbol"]):
        g = g.sort_values("ts_utc").reset_index(drop=True)
        for i in range(1, LOOKBACK_RET + 1):
            g[f"ret_lag_{i}"] = g["return_1d"].shift(i)
        for i in range(0, LOOKBACK_VOL):
            g[f"vol_lag_{i}"] = g["volatility"].shift(i)
        g["ret_mean_5"] = g["return_1d"].rolling(5).mean()
        g["ret_std_5"] = g["return_1d"].rolling(5).std()
        g["ret_mean_10"] = g["return_1d"].rolling(10).mean()
        g["ret_std_10"] = g["return_1d"].rolling(10).std()
        g["target"] = g["return_1d"]
        g["asset_type"] = atype
        g["symbol"] = sym
        frames.append(g)
    out = pd.concat(frames, ignore_index=True)
    feature_cols = [c for c in out.columns if c.startswith("ret_lag_") or c.startswith("vol_lag_") or c.startswith("ret_mean_") or c.startswith("ret_std_")]
    out = out.dropna(subset=feature_cols + ["target"])
    return out, feature_cols


def train_and_predict(df_feat: pd.DataFrame, feature_cols: List[str]) -> List[Dict[str, any]]:
    preds = []
    for (atype, sym), g in df_feat.groupby(["asset_type", "symbol"]):
        if len(g) < MIN_SAMPLES:
            continue
        g = g.sort_values("ts_utc")
        X = g[feature_cols].values
        y = g["target"].values
        model = Lasso(alpha=ALPHA)
        model.fit(X, y)
        x_last = X[-1].reshape(1, -1)
        pred = float(model.predict(x_last)[0])
        action = "hold"
        if pred > BUY_THRESH:
            action = "buy"
        elif pred < SELL_THRESH:
            action = "sell"
        preds.append({
            "asset_type": atype,
            "symbol": sym,
            "prediction_return": pred,
            "action": action,
            "last_ts": g["ts_utc"].iloc[-1].isoformat(),
            "last_close": float(g["close"].iloc[-1]) if "close" in g else None,
            "model": "lasso",
            "alpha": ALPHA,
            "samples": len(g),
        })
    return preds


def save_predictions(preds: List[Dict[str, any]]):
    fs = gcs_fs()
    path = f"gs://{BUCKET}/{GOLD_PREFIX}/predictions/latest.json"
    with fs.open(path, "w") as f:
        json.dump({"generated_at": utc_now_iso(), "predictions": preds}, f)
    print(f"Saved predictions: {path}, symbols={len(preds)}")


def main():
    df = load_gold()
    if df.empty:
        print("No Gold data found.")
        return
    df_feat, feat_cols = build_features(df)
    if df_feat.empty:
        print("No features available.")
        return
    preds = train_and_predict(df_feat, feat_cols)
    save_predictions(preds)


if __name__ == "__main__":
    main()
