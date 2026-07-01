import io
import json
import logging
import os
import asyncio
import concurrent.futures
import threading
import time
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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
_history_buffer = pd.DataFrame()
_processed_keys = set()

_mkt_cache = {"data": {}, "ts": 0}
MKT_CACHE_TTL = 60

_TICKER_MAP = {
    "GC=F": "gc_history",
    "CL=F": "oil_history",
    "DX-Y.NYB": "dxy_history",
    "EURUSD=X": "eurusd_history",
    "JPY=X": "jpy_history",
}


async def _warm_mkt_cache():
    try:
        ticker_str = " ".join(_TICKER_MAP.keys())
        raw = await asyncio.to_thread(yf.download, ticker_str, period="1d", interval="5m", progress=False)
        if not raw.empty:
            for ticker, field in _TICKER_MAP.items():
                try:
                    close = raw["Close"].get(ticker)
                    if close is not None:
                        close = close.dropna()
                        _mkt_cache["data"][field] = [
                            {"x": str(i.tz_convert("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")), "y": float(close[i])}
                            for i in close.index
                        ]
                    else:
                        _mkt_cache["data"][field] = []
                except Exception:
                    _mkt_cache["data"][field] = []
            _mkt_cache["ts"] = time.time()
            log.info("Market history cache warmed (%d tickers)", len(_TICKER_MAP))
    except Exception as e:
        log.warning(f"Cache warmup failed: {e}")


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
        config=boto3.session.Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=10,
            retries={"max_attempts": 1},
        ),
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


def _list_parquet_keys(bucket: str = "processed-data") -> list:
    s3 = _get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _load_one_parquet(bucket: str, key: str) -> pd.DataFrame:
    s3 = _get_s3()
    resp = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def _load_parquet_batch(keys: list[str], bucket: str = "processed-data", max_workers: int = 10) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_load_one_parquet, bucket, k): k for k in keys}
        dfs = []
        for f in concurrent.futures.as_completed(futures):
            try:
                dfs.append(f.result())
            except Exception as e:
                log.warning(f"Failed to load {futures[f]}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _rebuild_history() -> pd.DataFrame:
    global _processed_keys
    try:
        log.info("Loading initial history from Garage...")
        all_keys = sorted(_list_parquet_keys())
        if not all_keys:
            log.warning("No parquet files in Garage")
            return pd.DataFrame()

        all_keys = sorted(all_keys)
        load_keys = all_keys[-200:]
        df = _load_parquet_batch(load_keys)
        _processed_keys = set(all_keys)
        df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        log.info(f"Loaded {len(df)} historical rows from Garage ({len(all_keys)} total keys tracked)")
        return df
    except Exception as e:
        log.error(f"Failed to rebuild history from Garage: {e}")
        try:
            _processed_keys = set(_list_parquet_keys())
        except Exception:
            pass
        return pd.DataFrame()


def _check_new_data() -> pd.DataFrame:
    global _processed_keys
    try:
        all_keys = set(_list_parquet_keys())
        new_keys = sorted(all_keys - _processed_keys)
        if not new_keys:
            return pd.DataFrame()
        if len(new_keys) > 200:
            log.warning(f"Too many new keys ({len(new_keys)}), loading latest 200")
            _processed_keys.update(new_keys[:-200])
            new_keys = new_keys[-200:]
        else:
            _processed_keys.update(new_keys)
        df = _load_parquet_batch(new_keys)
        log.info(f"Added {len(df)} new rows from Garage")
        return df
    except Exception as e:
        log.warning(f"Check new data failed: {e}")
        return pd.DataFrame()


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


def _predict_and_save(latest_row: dict):
    global _history_buffer
    if _history_buffer.empty or len(_history_buffer) < 168:
        return

    try:
        buf = _history_buffer.copy()
        buf["timestamp"] = buf["timestamp"].astype(str)
        feature_dict = get_feature_vector(buf, latest_row)
    except Exception as e:
        log.warning(f"Feature computation failed: {e}")
        return

    feature_values = np.array([feature_dict.get(c, 0.0) for c in FEATURE_COLUMNS]).reshape(1, -1)
    ts = datetime.now(timezone.utc).isoformat()
    log.info(f"Predicting at {ts} — {len(_history_buffer)} history rows")

    actual_price = latest_row.get("gold_price")
    for h in PREDICTION_HORIZONS:
        if h not in _models:
            continue
        try:
            pred = float(_models[h].predict(feature_values)[0])
            model_name = _champions.get(h, {}).get("model_name", "unknown")
            with engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO predictions (timestamp, predicted_price, actual_price, horizon, model_version) "
                         "VALUES (:ts, :price, :actual, :h, :version)"),
                    {"ts": ts, "price": pred, "actual": actual_price, "h": h, "version": model_name},
                )
                conn.commit()
        except Exception as e:
            log.error(f"Predict/insert failed h={h}: {e}")


def _polling_loop():
    global _history_buffer
    log.info("Background polling started (interval=1s)")

    _history_buffer = _rebuild_history()
    last_refresh = time.time()
    last_rebuild = time.time()

    while True:
        try:
            if _history_buffer.empty and time.time() - last_rebuild > 30:
                _history_buffer = _rebuild_history()
                last_rebuild = time.time()

            if time.time() - last_refresh > 60:
                new_data = _check_new_data()
                if new_data is not None and not new_data.empty:
                    _history_buffer = pd.concat([_history_buffer, new_data], ignore_index=True)
                    _history_buffer = _history_buffer.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
                    if len(_history_buffer) > 1000:
                        _history_buffer = _history_buffer.tail(500)
                last_refresh = time.time()

            latest = _get_latest_price_from_garage()
            if latest is not None and not _history_buffer.empty:
                _predict_and_save(latest)
        except Exception as e:
            log.error(f"Polling error: {e}")
        time.sleep(1)


@app.on_event("startup")
def startup():
    t_models = threading.Thread(target=load_models, daemon=True)
    t_models.start()
    t_poll = threading.Thread(target=_polling_loop, daemon=True)
    t_poll.start()
    asyncio.create_task(_warm_mkt_cache())
    log.info("Startup complete — models loading in background")


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

    actual_price = latest_row.get("gold_price")
    try:
        with engine.connect() as conn:
            conn.execute(
                text("INSERT INTO predictions (timestamp, predicted_price, actual_price, horizon, model_version) "
                     "VALUES (:ts, :price, :actual, :horizon, :version)"),
                {"ts": ts, "price": pred, "actual": actual_price, "horizon": horizon, "version": model_name},
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
    actual_price = latest_row.get("gold_price")
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
                    text("INSERT INTO predictions (timestamp, predicted_price, actual_price, horizon, model_version) "
                         "VALUES (:ts, :price, :actual, :horizon, :version)"),
                    {"ts": ts, "price": pred, "actual": actual_price, "horizon": h, "version": model_name},
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


@app.get("/market/latest")
def get_market_latest():
    data = _get_latest_price_from_garage()
    if data is None:
        try:
            df = _fetch_yfinance_history()
            if not df.empty:
                last = df.iloc[-1]
                data = {
                    "timestamp": str(last.get("timestamp", "")),
                    "gold_price": float(last["gold_price"]),
                    "oil_price": float(last.get("oil_price", 0)),
                    "dxy": float(last.get("dxy", 0)),
                    "eurusd": float(last.get("eurusd", 0)),
                    "jpy": float(last.get("jpy", 0)),
                }
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Market data unavailable: {e}")
    if data is None:
        raise HTTPException(status_code=503, detail="No market data available")
    return data


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


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    log.info("WebSocket client connected")

    while True:
        try:
            market = _get_latest_price_from_garage()
            if market is None:
                history_df = _fetch_yfinance_history()
                if not history_df.empty:
                    last = history_df.iloc[-1]
                    market = {
                        "timestamp": str(last.get("timestamp", "")),
                        "gold_price": float(last["gold_price"]),
                        "oil_price": float(last.get("oil_price", 0)),
                        "dxy": float(last.get("dxy", 0)),
                        "eurusd": float(last.get("eurusd", 0)),
                        "jpy": float(last.get("jpy", 0)),
                    }

            preds = []
            try:
                with engine.connect() as conn:
                    rows = conn.execute(
                        text("SELECT horizon, predicted_price, actual_price, timestamp, model_version "
                             "FROM predictions ORDER BY timestamp DESC LIMIT 3000")
                    ).fetchall()
                    preds = [
                        {"horizon": r[0], "predicted_price": r[1], "actual_price": r[2], "timestamp": r[3].isoformat(), "model_version": r[4]}
                        for r in rows
                    ]
            except Exception:
                preds = []

            now = time.time()
            if not _mkt_cache["data"] or (now - _mkt_cache["ts"]) > MKT_CACHE_TTL:
                try:
                    ticker_str = " ".join(_TICKER_MAP.keys())
                    raw = await asyncio.to_thread(yf.download, ticker_str, period="1d", interval="5m", progress=False)
                    if not raw.empty:
                        for ticker, field in _TICKER_MAP.items():
                            try:
                                close = raw["Close"].get(ticker)
                                if close is not None:
                                    close = close.dropna()
                                    _mkt_cache["data"][field] = [
                                        {"x": str(i.tz_convert("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")), "y": float(close[i])}
                                        for i in close.index
                                    ]
                                else:
                                    _mkt_cache["data"][field] = []
                            except Exception:
                                _mkt_cache["data"][field] = []
                        _mkt_cache["ts"] = now
                except Exception as e:
                    log.warning(f"yfinance market history download failed: {e}")

            await websocket.send_json({
                "market": market,
                "predictions": preds,
                "gc_history": _mkt_cache["data"].get("gc_history", []),
                "dxy_history": _mkt_cache["data"].get("dxy_history", []),
                "eurusd_history": _mkt_cache["data"].get("eurusd_history", []),
                "jpy_history": _mkt_cache["data"].get("jpy_history", []),
                "oil_history": _mkt_cache["data"].get("oil_history", []),
            })
            await asyncio.sleep(1)
        except WebSocketDisconnect:
            log.info("WebSocket client disconnected")
            break
        except Exception as e:
            log.warning(f"WebSocket error: {e}")
            await asyncio.sleep(2)
