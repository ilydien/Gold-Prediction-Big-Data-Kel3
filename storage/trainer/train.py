import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import joblib
import pandas as pd
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, train_test_split
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
        ("knn", KNeighborsRegressor()),
    ]),
    "random_forest": Pipeline([
        ("scaler", StandardScaler()),
        ("rf", RandomForestRegressor(random_state=42)),
    ]),
    "gradient_boosting": Pipeline([
        ("scaler", StandardScaler()),
        ("gbr", GradientBoostingRegressor(random_state=42)),
    ]),
}

PARAM_GRIDS = {
    "linear_regression": {},
    "knn": {
        "knn__n_neighbors": [3, 5, 7, 10],
        "knn__weights": ["uniform", "distance"],
    },
    "random_forest": {
        "rf__n_estimators": [50, 100, 200],
        "rf__max_depth": [None, 10, 20],
    },
    "gradient_boosting": {
        "gbr__n_estimators": [50, 100, 200],
        "gbr__learning_rate": [0.05, 0.1],
        "gbr__max_depth": [3, 5],
    },
}

FEATURE_IMPORTANCE_MODELS = {"random_forest": "rf", "gradient_boosting": "gbr"}


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
    param_grid = PARAM_GRIDS.get(model_name, {})
    if param_grid:
        tscv = TimeSeriesSplit(n_splits=3)
        search = GridSearchCV(
            model, param_grid, cv=tscv, scoring="neg_mean_absolute_error", n_jobs=1
        )
        search.fit(X_train, y_train)
        best_model = search.best_estimator_
        best_params = {
            k.split("__", 1)[-1]: v for k, v in search.best_params_.items()
        }
        print(f"  {model_name} best params: {best_params}")
    else:
        best_model = model
        best_model.fit(X_train, y_train)
        best_params = {}

    y_pred = best_model.predict(X_test)
    return {
        "model_name": model_name,
        "mae": mean_absolute_error(y_test, y_pred),
        "rmse": mean_squared_error(y_test, y_pred) ** 0.5,
        "r2": r2_score(y_test, y_pred),
        "model": best_model,
        "best_params": best_params,
    }


def extract_feature_importance(model, model_name: str) -> dict:
    step_name = FEATURE_IMPORTANCE_MODELS.get(model_name)
    if not step_name:
        return {}
    estimator = model.named_steps.get(step_name)
    if not hasattr(estimator, "feature_importances_"):
        return {}
    importances = estimator.feature_importances_
    pairs = sorted(
        zip(FEATURE_COLUMNS, importances), key=lambda x: x[1], reverse=True
    )
    return {feat: round(float(imp), 6) for feat, imp in pairs}


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
        "best_params": best_result.get("best_params", {}),
        "features": FEATURE_COLUMNS,
        "n_features": len(FEATURE_COLUMNS),
        "n_samples": best_result.get("n_samples", 0),
        "feature_importance": extract_feature_importance(
            best_result["model"], model_name
        ),
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
        result["n_samples"] = len(X_train)
        results.append(result)
        print(f"  {name}: MAE={result['mae']:.4f}, RMSE={result['rmse']:.4f}, R²={result['r2']:.4f}")

    best = min(results, key=lambda r: r["mae"])
    print(f"\nBest model: {best['model_name']} (MAE={best['mae']:.4f})")

    importance = extract_feature_importance(best["model"], best["model_name"])
    if importance:
        print("\nFeature importance:")
        for feat, imp in importance.items():
            bar = "█" * int(imp * 50) if imp > 0 else ""
            print(f"  {feat:20s} {imp:.4f} {bar}")

    metadata = save_best_model(best)
    save_metrics_to_postgres(metadata)
    print("\n=== Training Complete ===")


if __name__ == "__main__":
    run_training()
