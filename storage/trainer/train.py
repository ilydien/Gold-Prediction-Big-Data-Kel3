import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import joblib
import mlflow
import pandas as pd
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, train_test_split
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
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PREDICTION_HORIZONS = [
    int(h) for h in os.getenv("PREDICTION_HORIZONS", "12,24,48,72,168,720").split(",")
]

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
    "gradient_boosting": Pipeline([
        ("scaler", StandardScaler()),
        ("gbr", GradientBoostingRegressor(random_state=42)),
    ]),
}

PARAM_GRIDS = {
    "linear_regression": {},
    "gradient_boosting": {
        "gbr__n_estimators": [50, 100, 200],
        "gbr__learning_rate": [0.05, 0.1],
        "gbr__max_depth": [3, 5],
    },
}

FEATURE_IMPORTANCE_MODELS = {"gradient_boosting": "gbr"}


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


def load_features() -> pd.DataFrame:
    print("Loading features from Garage...")
    response = s3.get_object(Bucket="features", Key="latest/features.parquet")
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    print(f"Loaded {len(df)} rows")
    return df


def train_and_evaluate(model, model_name, X_train, X_test, y_train, y_test):
    param_grid = PARAM_GRIDS.get(model_name, {})
    if param_grid:
        tscv = TimeSeriesSplit(n_splits=3)
        search = GridSearchCV(model, param_grid, cv=tscv, scoring="neg_mean_absolute_error", n_jobs=1)
        search.fit(X_train, y_train)
        best_model = search.best_estimator_
        best_params = {k.split("__", 1)[-1]: v for k, v in search.best_params_.items()}
        print(f"    {model_name} best params: {best_params}")
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


def extract_feature_importance(model, model_name):
    step_name = FEATURE_IMPORTANCE_MODELS.get(model_name)
    if not step_name:
        return {}
    estimator = model.named_steps.get(step_name)
    if not hasattr(estimator, "feature_importances_"):
        return {}
    importances = estimator.feature_importances_
    pairs = sorted(zip(FEATURE_COLUMNS, importances), key=lambda x: x[1], reverse=True)
    return {feat: round(float(imp), 6) for feat, imp in pairs}


def _load_champion_meta(horizon):
    try:
        resp = s3.get_object(Bucket="models", Key=f"h={horizon}/champion/metadata.json")
        return json.loads(resp["Body"].read().decode())
    except Exception:
        return None


def _record_champion_event(horizon, version, event):
    try:
        resp = s3.get_object(Bucket="models", Key=f"h={horizon}/champion/rollback_history.json")
        history = json.loads(resp["Body"].read().decode())
    except Exception:
        history = []
    history.append({
        "version": version,
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    s3.put_object(
        Bucket="models",
        Key=f"h={horizon}/champion/rollback_history.json",
        Body=json.dumps(history, indent=2).encode(),
    )


def _update_champion(horizon, metadata, buffer):
    champion_meta = _load_champion_meta(horizon)
    version = f"{metadata['model_name']}/{metadata['timestamp']}"

    if champion_meta and metadata["mae"] >= champion_meta["mae"]:
        print(f"  Champion h={horizon} unchanged: "
              f"new MAE {metadata['mae']:.4f} >= current {champion_meta['mae']:.4f}")
        _record_champion_event(horizon, version, "skipped")
        return

    if champion_meta:
        print(f"  Champion h={horizon} promoted: "
              f"MAE {champion_meta['mae']:.4f} -> {metadata['mae']:.4f}")
    else:
        print(f"  Champion h={horizon} initialized: "
              f"{metadata['model_name']} MAE={metadata['mae']:.4f}")

    buffer.seek(0)
    model_bytes = buffer.read()
    s3.put_object(Bucket="models", Key=f"h={horizon}/champion/model.pkl", Body=model_bytes)
    s3.put_object(
        Bucket="models",
        Key=f"h={horizon}/champion/metadata.json",
        Body=json.dumps(metadata, indent=2).encode(),
    )
    _record_champion_event(horizon, version, "promoted_to_champion")


def _save_best_model(horizon, best_result):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    model_name = best_result["model_name"]
    path_prefix = f"h={horizon}/{model_name}/{timestamp}"
    key = f"{path_prefix}/model.pkl"

    buffer = io.BytesIO()
    joblib.dump(best_result["model"], buffer)
    s3.put_object(Bucket="models", Key=key, Body=buffer.getvalue())

    metadata = {
        "model_name": model_name,
        "horizon": horizon,
        "timestamp": timestamp,
        "mae": best_result["mae"],
        "rmse": best_result["rmse"],
        "r2": best_result["r2"],
        "best_params": best_result.get("best_params", {}),
        "features": FEATURE_COLUMNS,
        "n_features": len(FEATURE_COLUMNS),
        "n_samples": best_result.get("n_samples", 0),
        "feature_importance": extract_feature_importance(best_result["model"], model_name),
    }
    s3.put_object(
        Bucket="models",
        Key=f"{path_prefix}/metadata.json",
        Body=json.dumps(metadata).encode(),
    )

    _update_champion(horizon, metadata, buffer)
    print(f"  Model saved to models/{key}")
    return metadata


def save_metrics_to_postgres(metadata: dict):
    try:
        conn = pg_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO model_metrics (timestamp, model_name, mae, rmse, r2, horizon)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.now(timezone.utc),
                metadata["model_name"],
                metadata["mae"],
                metadata["rmse"],
                metadata["r2"],
                metadata.get("horizon", 0),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        print("  Metrics saved to PostgreSQL")
    except Exception as e:
        print(f"  PostgreSQL save failed: {e}")


def _configure_mlflow():
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("gold_price_prediction")
    except Exception as e:
        print(f"MLflow unavailable: {e}")


def _log_to_mlflow(result: dict, timestamp: str, horizon: int):
    try:
        name = result["model_name"]
        run_name = f"h={horizon}_{name}_{timestamp}"
        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({"horizon": horizon})
            if result.get("best_params"):
                mlflow.log_params(result["best_params"])
            mlflow.log_metrics({
                "mae": result["mae"], "rmse": result["rmse"], "r2": result["r2"],
            })
            if name in FEATURE_IMPORTANCE_MODELS:
                importance = extract_feature_importance(result["model"], name)
                for feat, imp in importance.items():
                    mlflow.log_metric(f"importance_{feat}", imp)
            mlflow.sklearn.log_model(result["model"], f"h={horizon}_{name}")
    except Exception as e:
        print(f"    MLflow skipped for {name} h={horizon}: {e}")


def run_training():
    print("=== MULTI-HORIZON ML TRAINING ===")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    _configure_mlflow()

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

    print(f"Data: {len(df)} rows, horizons: {PREDICTION_HORIZONS}\n")

    all_metadata = {}

    for horizon in PREDICTION_HORIZONS:
        print(f"{'='*60}")
        print(f"  HORIZON = {horizon}h")
        print(f"{'='*60}")

        target = df[TARGET_COLUMN].shift(-horizon)
        valid_idx = target.dropna().index
        if len(valid_idx) < 50:
            print(f"  SKIP — only {len(valid_idx)} valid rows (need 50+)")
            continue

        X = df.loc[valid_idx, FEATURE_COLUMNS]
        y = target.loc[valid_idx]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=False,
        )
        print(f"  Train: {len(X_train)} rows, Test: {len(X_test)} rows")

        results = []
        for name, model in MODELS.items():
            result = train_and_evaluate(model, name, X_train, X_test, y_train, y_test)
            result["n_samples"] = len(X_train)
            results.append(result)
            _log_to_mlflow(result, timestamp, horizon)
            print(f"    {name}: MAE={result['mae']:.4f}, "
                  f"RMSE={result['rmse']:.4f}, R²={result['r2']:.4f}")

        print(f"\n  {'Model':<22s} {'MAE':>10s}  {'RMSE':>10s}  {'R²':>8s}")
        print(f"  {'-'*53}")
        for r in sorted(results, key=lambda x: x["mae"]):
            print(f"  {r['model_name']:<22s} {r['mae']:>10.4f}  "
                  f"{r['rmse']:>10.4f}  {r['r2']:>8.4f}")

        best = min(results, key=lambda r: r["mae"])
        print(f"\n  Best: {best['model_name']} (MAE={best['mae']:.4f})")

        importance = extract_feature_importance(best["model"], best["model_name"])
        if importance:
            print(f"\n  Feature importance (top 5):")
            for feat, imp in list(importance.items())[:5]:
                bar = "█" * int(imp * 50) if imp > 0 else ""
                print(f"    {feat:20s} {imp:.4f} {bar}")

        metadata = _save_best_model(horizon, best)
        save_metrics_to_postgres(metadata)
        all_metadata[horizon] = metadata
        print()

    print("=" * 60)
    print("  TRAINING COMPLETE")
    print(f"  Horizons trained: {list(all_metadata.keys())}")
    print("=" * 60)
    return all_metadata


if __name__ == "__main__":
    run_training()
