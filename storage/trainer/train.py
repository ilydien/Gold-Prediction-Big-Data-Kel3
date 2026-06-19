import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import joblib
import pandas as pd
import psycopg2
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from shared.features import FEATURE_COLUMNS, TARGET_COLUMN

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gold_prediction")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

s3 = boto3.client(
    "s3",
    endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY,
    aws_secret_access_key=GARAGE_SECRET_KEY,
    region_name="us-east-1",
    use_ssl=False,
    config=boto3.session.Config(signature_version="s3v4"),
)

MODELS = {
    "linear_regression": Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LinearRegression()),
    ]),
    "knn": Pipeline([
        ("scaler", StandardScaler()),
        ("knn", KNeighborsRegressor(n_neighbors=5)),
    ]),
    "random_forest": Pipeline([
        ("scaler", StandardScaler()),
        ("rf", RandomForestRegressor(n_estimators=100, random_state=42)),
    ]),
}


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def load_features() -> pd.DataFrame:
    print("Loading features from Garage...")
    response = s3.get_object(Bucket="features", Key="latest/features.parquet")
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    print(f"Loaded {len(df)} rows")
    return df


def train_and_evaluate(model, model_name: str, X_train, X_test, y_train, y_test):
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return {
        "model_name": model_name,
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": mean_squared_error(y_test, y_pred) ** 0.5,
        "r2": r2_score(y_test, y_pred),
        "model": model,
    }


def save_best_model(best_result: dict):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    model_name = best_result["model_name"]
    key = f"{model_name}/{timestamp}/model.pkl"

    buffer = io.BytesIO()
    joblib.dump(best_result["model"], buffer)
    s3.put_object(Bucket="models", Key=key, Body=buffer.getvalue())

    metadata = {
        "model_name": model_name,
        "timestamp": timestamp,
        "mae": best_result["mae"],
        "rmse": best_result["rmse"],
        "r2": best_result["r2"],
        "features": FEATURE_COLUMNS,
    }
    s3.put_object(
        Bucket="models",
        Key=f"{model_name}/{timestamp}/metadata.json",
        Body=json.dumps(metadata).encode(),
    )

    s3.put_object(
        Bucket="models",
        Key="latest/model.pkl",
        Body=buffer.getvalue(),
    )
    s3.put_object(
        Bucket="models",
        Key="latest/metadata.json",
        Body=json.dumps(metadata).encode(),
    )

    print(f"Model saved to models/{key}")
    return metadata


def save_metrics_to_postgres(metadata: dict):
    try:
        conn = pg_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO model_metrics (timestamp, model_name, mae, rmse, r2)
            VALUES (%s, %s, %s, %s, %s)
        """,
            (
                datetime.now(timezone.utc),
                metadata["model_name"],
                metadata["mae"],
                metadata["rmse"],
                metadata["r2"],
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        print("Metrics saved to PostgreSQL")
    except Exception as e:
        print(f"Failed to save metrics to PostgreSQL: {e}")


def run_training():
    print("=== ML Training Pipeline ===")

    for attempt in range(30):
        try:
            df = load_features()
            break
        except Exception as e:
            print(f"Waiting for features ({attempt+1}/30): {e}")
            time.sleep(5)
    else:
        print("No features available. Exiting.")
        return

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )
    print(
        f"Train: {len(X_train)} rows, Test: {len(X_test)} rows"
    )

    results = []
    for name, model in MODELS.items():
        result = train_and_evaluate(model, name, X_train, X_test, y_train, y_test)
        results.append(result)
        print(f"  {name}: MAE={result['mae']:.4f}, RMSE={result['rmse']:.4f}, R²={result['r2']:.4f}")

    best = min(results, key=lambda r: r["mae"])
    print(f"Best model: {best['model_name']} (MAE={best['mae']:.4f})")

    metadata = save_best_model(best)
    save_metrics_to_postgres(metadata)
    print("=== Training Complete ===")


if __name__ == "__main__":
    run_training()
