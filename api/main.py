from fastapi import FastAPI
from sqlalchemy import create_engine, text
import pandas as pd

app = FastAPI(title="Gold Price Prediction API")

DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/gold_prediction"
engine = create_engine(DATABASE_URL)


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/features/latest")
def latest_features():
    query = text("SELECT * FROM gold_features ORDER BY timestamp DESC LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()
    if not row:
        return {"error": "No data yet"}
    return {
        "id": row[0],
        "timestamp": str(row[1]),
        "lag1": row[2],
        "lag5": row[3],
        "return": row[4],
        "volatility": row[5],
        "dxy": row[6],
        "oil_price": row[7],
    }


@app.get("/features")
def all_features(limit: int = 100):
    query = text("SELECT * FROM gold_features ORDER BY timestamp DESC LIMIT :limit")
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()
    columns = ["id", "timestamp", "lag1", "lag5", "return", "volatility", "dxy", "oil_price"]
    return [dict(zip(columns, row)) for row in rows]


@app.get("/predictions")
def get_predictions(limit: int = 100):
    query = text("SELECT * FROM predictions ORDER BY timestamp DESC LIMIT :limit")
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()
    columns = ["id", "timestamp", "actual_price", "predicted_price", "error", "model_version"]
    return [dict(zip(columns, row)) for row in rows]


@app.get("/metrics")
def get_metrics(limit: int = 20):
    query = text("SELECT * FROM model_metrics ORDER BY timestamp DESC LIMIT :limit")
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()
    columns = ["id", "timestamp", "mae", "rmse", "r2"]
    return [dict(zip(columns, row)) for row in rows]
