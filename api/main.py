import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from shared.features import FEATURE_COLUMNS

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

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)

app = FastAPI(title="Gold Price Prediction API")

_model = None
_model_version = "none"
_model_metadata = {}


class PredictRequest(BaseModel):
    timestamp: str
    features: dict


class PredictResponse(BaseModel):
    timestamp: str
    predicted_price: float
    model_version: str


def load_model_from_garage():
    global _model, _model_version, _model_metadata

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=GARAGE_ENDPOINT,
            aws_access_key_id=GARAGE_ACCESS_KEY,
            aws_secret_access_key=GARAGE_SECRET_KEY,
            region_name="us-east-1",
            use_ssl=False,
            config=boto3.session.Config(signature_version="s3v4"),
        )

        log.info("Downloading model from Garage...")
        resp = s3.get_object(Bucket="models", Key="latest/model.pkl")
        _model = joblib.load(io.BytesIO(resp["Body"].read()))

        try:
            meta_resp = s3.get_object(Bucket="models", Key="latest/metadata.json")
            _model_metadata = json.loads(meta_resp["Body"].read().decode())
            _model_version = _model_metadata.get("model_name", "unknown")
            log.info(f"Model: {_model_version}")
        except Exception:
            _model_version = "unknown"

        log.info("Model loaded successfully")
    except Exception as e:
        log.warning(f"Could not load model from Garage: {e}")
        log.warning("API will start without model. Predictions will fail until model is available.")


@app.on_event("startup")
def startup():
    load_model_from_garage()


@app.get("/")
def root():
    return {
        "status": "ok",
        "model_loaded": _model is not None,
        "model_version": _model_version,
    }


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded yet")

    try:
        features_dict = req.features
        feature_values = [features_dict.get(col, 0.0) for col in FEATURE_COLUMNS]
        X = np.array(feature_values).reshape(1, -1)

        raw_pred = _model.predict(X)[0]
        predicted_price = round(float(raw_pred), 2)

        ts = req.timestamp
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                INSERT INTO predictions (timestamp, predicted_price, model_version)
                VALUES (:ts, :price, :version)
                """
                ),
                {"ts": ts, "price": predicted_price, "version": _model_version},
            )
            conn.commit()

        log.info(f"Predicted: {predicted_price} (model: {_model_version})")
        return PredictResponse(
            timestamp=ts,
            predicted_price=predicted_price,
            model_version=_model_version,
        )
    except Exception as e:
        log.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictions")
def get_predictions(limit: int = 100):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, timestamp, actual_price, predicted_price, error, model_version "
                "FROM predictions ORDER BY timestamp DESC LIMIT :limit"
            ),
            {"limit": limit},
        ).fetchall()
    columns = ["id", "timestamp", "actual_price", "predicted_price", "error", "model_version"]
    return [dict(zip(columns, row)) for row in rows]


@app.get("/metrics")
def get_metrics(limit: int = 20):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, timestamp, model_name, mae, rmse, r2 "
                "FROM model_metrics ORDER BY timestamp DESC LIMIT :limit"
            ),
            {"limit": limit},
        ).fetchall()
    columns = ["id", "timestamp", "model_name", "mae", "rmse", "r2"]
    return [dict(zip(columns, row)) for row in rows]
