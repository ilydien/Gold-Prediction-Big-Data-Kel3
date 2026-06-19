import json
import os
from datetime import datetime, timezone

import boto3
import psycopg2

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gold_prediction")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

COUNT_THRESHOLD = int(os.getenv("RETRAIN_COUNT_THRESHOLD", "2000"))
PERF_WINDOW = int(os.getenv("RETRAIN_PERF_WINDOW", "100"))
PERF_FACTOR = float(os.getenv("RETRAIN_PERF_FACTOR", "1.5"))
PREDICTION_HORIZONS = [
    int(h) for h in os.getenv("PREDICTION_HORIZONS", "12,24,48,72,168,720").split(",")
]

STATE_BUCKET = "models"


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


def _get_pg():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


def load_training_state(horizon: int = None) -> dict:
    key = f"training-state/h={horizon}/latest.json" if horizon else "training-state/latest.json"
    try:
        s3 = _get_s3()
        resp = s3.get_object(Bucket=STATE_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode())
    except Exception:
        return {}


def save_training_state(state: dict, horizon: int):
    key = f"training-state/h={horizon}/latest.json"
    try:
        s3 = _get_s3()
        s3.put_object(Bucket=STATE_BUCKET, Key=key, Body=json.dumps(state, indent=2).encode())
        print(f"Training state saved to {STATE_BUCKET}/{key}")
    except Exception as e:
        print(f"Failed to save training state: {e}")


def _count_objects(bucket: str) -> int:
    s3 = _get_s3()
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        count += len(page.get("Contents", []))
    return count


def check_count_trigger(last_state: dict) -> tuple[bool, int]:
    prev_count = last_state.get("object_count", 0)
    curr_count = _count_objects("processed-data") + _count_objects("hourly-history")
    new_objects = curr_count - prev_count
    if new_objects >= COUNT_THRESHOLD:
        return True, new_objects
    return False, new_objects


def check_performance_trigger(last_state: dict) -> tuple[bool, float]:
    best_mae = last_state.get("best_mae")
    if best_mae is None:
        return False, 0.0
    try:
        conn = _get_pg()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT AVG(ABS(predicted_price - actual_price)) as rolling_mae
            FROM (
                SELECT predicted_price, actual_price
                FROM predictions
                WHERE actual_price IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT %s
            ) sub
            """,
            (PERF_WINDOW,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0] is not None:
            rolling_mae = float(row[0])
            if rolling_mae > PERF_FACTOR * best_mae:
                return True, rolling_mae
            return False, rolling_mae
    except Exception as e:
        print(f"Performance check failed: {e}")
    return False, 0.0


def should_retrain() -> tuple[bool, str, int]:
    any_dirty = False
    reasons = []

    for h in PREDICTION_HORIZONS:
        last_state = load_training_state(h)

        if not last_state:
            any_dirty = True
            reasons.append(f"h={h}: no previous state")
            continue

        if last_state.get("best_mae", 0) <= 0:
            any_dirty = True
            reasons.append(f"h={h}: invalid state (MAE=0)")
            continue

    new_objects = _count_objects("processed-data") + _count_objects("hourly-history")
    triggered_count, _ = check_count_trigger(load_training_state(PREDICTION_HORIZONS[0]) or {})

    if triggered_count:
        return True, f"Count trigger: {new_objects} new objects", new_objects
    if any_dirty:
        return True, "Invalid or missing training state: " + "; ".join(reasons), new_objects

    return False, f"No trigger: {new_objects} new objects", new_objects


def build_training_state(metadata: dict, object_count: int) -> dict:
    return {
        "last_training_ts": datetime.now(timezone.utc).isoformat(),
        "rows_used": metadata.get("n_samples", 0),
        "object_count": object_count,
        "best_mae": metadata["mae"],
        "best_model": metadata["model_name"],
        "horizon": metadata.get("horizon"),
    }
