import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from shared.features import FEATURE_COLUMNS, LOOKBACK_HOURS, compute_features, get_feature_vector

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gold_prediction")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PREDICTION_HORIZONS = [
    int(h) for h in os.getenv("PREDICTION_HORIZONS", "1,12,24,48,72").split(",")
]

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)

app = FastAPI(title="Gold Price Prediction API — Multi-Horizon")

_models = {}
_champions = {}


class PredictRequest(BaseModel):
    horizon: int = 12


class PredictResponse(BaseModel):
    horizon: int
    timestamp: str
    predicted_price: float
    model_name: str


class PredictAllResponse(BaseModel):
    timestamp: str
    predictions: list[PredictResponse]


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
        region_name="us-east-1",
        use_ssl=False,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def _fetch_yfinance_history() -> pd.DataFrame:
    tickers = "GC=F CL=F DX-Y.NYB EURUSD=X JPY=X"
    log.info("Fetching historical hourly data from yfinance...")
    raw = yf.download(tickers, period="10d", interval="1h", progress=False)
    data = {
        "gold_price": raw["Close"]["GC=F"],
        "oil_price": raw["Close"]["CL=F"],
        "dxy": raw["Close"]["DX-Y.NYB"],
        "eurusd": raw["Close"]["EURUSD=X"],
        "jpy": raw["Close"]["JPY=X"],
    }
    df = pd.DataFrame(data).reset_index()
    df = df.rename(columns={"Datetime": "timestamp", "Date": "timestamp", "index": "timestamp"})
    df["timestamp"] = df["timestamp"].astype(str)
    return df.tail(LOOKBACK_HOURS + 24)[[
        "timestamp", "gold_price", "oil_price", "dxy", "eurusd", "jpy",
    ]].dropna()


def _get_latest_price_from_garage() -> dict:
    try:
        s3 = _get_s3()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket="processed-data"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])

        if not keys:
            # fallback: try hourly-history
            for page in paginator.paginate(Bucket="hourly-history"):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        keys.append(obj["Key"])

        if not keys:
            log.warning("No Data in Garage, using yfinance only")
            return None

        latest_key = sorted(keys)[-1]
        bucket = "hourly-history" if "hourly-history" in str(keys) else "processed-data"
        resp = s3.get_object(Bucket=bucket, Key=latest_key)
        df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        last_row = df.iloc[-1]
        return {
            "timestamp": str(last_row.get("timestamp", "")),
            "gold_price": float(last_row["gold_price"]),
            "oil_price": float(last_row.get("oil_price", 0)),
            "dxy": float(last_row.get("dxy", 0)),
            "eurusd": float(last_row.get("eurusd", 0)),
            "jpy": float(last_row.get("jpy", 0)),
        }
    except Exception as e:
        log.warning(f"Could not get latest price from Garage: {e}")
        return None


def load_models():
    global _models, _champions
    s3 = _get_s3()

    for h in PREDICTION_HORIZONS:
        try:
            resp = s3.get_object(Bucket="models", Key=f"h={h}/champion/model.pkl")
            _models[h] = joblib.load(io.BytesIO(resp["Body"].read()))
            try:
                meta = s3.get_object(Bucket="models", Key=f"h={h}/champion/metadata.json")
                _champions[h] = json.loads(meta["Body"].read().decode())
            except Exception:
                _champions[h] = {"model_name": "unknown", "mae": 0.0}
            log.info(f"Loaded model h={h}: {_champions[h]['model_name']} (MAE={_champions[h]['mae']:.4f})")
        except Exception as e:
            log.warning(f"Model h={h} not found: {e}")


@app.on_event("startup")
def startup():
    load_models()


@app.get("/")
def root():
    return {
        "status": "ok",
        "horizons_loaded": list(_models.keys()),
        "champions": {h: c.get("model_name", "?") for h, c in _champions.items()},
    }


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    horizon = req.horizon
    if horizon not in _models:
        raise HTTPException(status_code=404, detail=f"No model for horizon {horizon}")

    history_df = _fetch_yfinance_history()
    latest_row = _get_latest_price_from_garage()
    if latest_row is None:
        latest_row = history_df.iloc[-1].to_dict()

    try:
        feature_dict = get_feature_vector(history_df, latest_row)
    except Exception as e:
        log.error(f"Feature computation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Feature error: {e}")

    feature_values = np.array([feature_dict.get(c, 0.0) for c in FEATURE_COLUMNS]).reshape(1, -1)
    pred = float(_models[horizon].predict(feature_values)[0])

    ts = datetime.now(timezone.utc).isoformat()
    model_name = _champions.get(horizon, {}).get("model_name", "unknown")

    try:
        with engine.connect() as conn:
            conn.execute(
                text("INSERT INTO predictions (timestamp, predicted_price, horizon, model_version) "
                     "VALUES (:ts, :price, :horizon, :version)"),
                {"ts": ts, "price": pred, "horizon": horizon, "version": model_name},
            )
            conn.commit()
    except Exception as e:
        log.error(f"PostgreSQL insert failed: {e}")

    return PredictResponse(
        horizon=horizon,
        timestamp=ts,
        predicted_price=round(pred, 2),
        model_name=model_name,
    )


@app.post("/predict/all", response_model=PredictAllResponse)
def predict_all():
    history_df = _fetch_yfinance_history()
    latest_row = _get_latest_price_from_garage()
    if latest_row is None:
        latest_row = history_df.iloc[-1].to_dict()

    try:
        feature_dict = get_feature_vector(history_df, latest_row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feature error: {e}")

    feature_values = np.array([feature_dict.get(c, 0.0) for c in FEATURE_COLUMNS]).reshape(1, -1)
    ts = datetime.now(timezone.utc).isoformat()
    predictions = []

    for h in PREDICTION_HORIZONS:
        if h not in _models:
            continue
        pred = float(_models[h].predict(feature_values)[0])
        model_name = _champions.get(h, {}).get("model_name", "unknown")

        try:
            with engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO predictions (timestamp, predicted_price, horizon, model_version) "
                         "VALUES (:ts, :price, :horizon, :version)"),
                    {"ts": ts, "price": pred, "horizon": h, "version": model_name},
                )
                conn.commit()
        except Exception as e:
            log.error(f"DB insert failed for h={h}: {e}")

        predictions.append(PredictResponse(
            horizon=h,
            timestamp=ts,
            predicted_price=round(pred, 2),
            model_name=model_name,
        ))

    return PredictAllResponse(timestamp=ts, predictions=predictions)


@app.get("/predictions")
def get_predictions(limit: int = 100, horizon: int = None):
    with engine.connect() as conn:
        if horizon is not None:
            rows = conn.execute(
                text("SELECT id, timestamp, actual_price, predicted_price, error, horizon, model_version "
                     "FROM predictions WHERE horizon = :h ORDER BY timestamp DESC LIMIT :l"),
                {"h": horizon, "l": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text("SELECT id, timestamp, actual_price, predicted_price, error, horizon, model_version "
                     "FROM predictions ORDER BY timestamp DESC LIMIT :l"),
                {"l": limit},
            ).fetchall()
    cols = ["id", "timestamp", "actual_price", "predicted_price", "error", "horizon", "model_version"]
    return [dict(zip(cols, row)) for row in rows]


@app.get("/metrics")
def get_metrics(limit: int = 20, horizon: int = None):
    with engine.connect() as conn:
        if horizon is not None:
            rows = conn.execute(
                text("SELECT id, timestamp, model_name, mae, rmse, r2, horizon "
                     "FROM model_metrics WHERE horizon = :h ORDER BY timestamp DESC LIMIT :l"),
                {"h": horizon, "l": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text("SELECT id, timestamp, model_name, mae, rmse, r2, horizon "
                     "FROM model_metrics ORDER BY timestamp DESC LIMIT :l"),
                {"l": limit},
            ).fetchall()
    cols = ["id", "timestamp", "model_name", "mae", "rmse", "r2", "horizon"]
    return [dict(zip(cols, row)) for row in rows]
