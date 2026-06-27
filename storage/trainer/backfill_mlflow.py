"""Backfill first training run metrics into MLflow."""
import os

os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow:5000"

import mlflow
from mlflow.models import infer_signature
import pandas as pd

METRICS = [
    # (horizon, model_name, mae, rmse, r2)
    (12,  "linear_regression", 58.6549,  81.1523,   0.9239),
    (12,  "gradient_boosting", 355.8020, 448.0568, -1.3207),
    (24,  "linear_regression", 91.6008,  120.8821,  0.8309),
    (24,  "gradient_boosting", 384.3930, 474.8404, -1.6087),
    (48,  "linear_regression", 137.0415, 172.7162,  0.6544),
    (48,  "gradient_boosting", 446.5841, 536.9079, -2.3398),
    (72,  "linear_regression", 171.8563, 212.4120,  0.4768),
    (72,  "gradient_boosting", 472.1117, 559.4264, -2.6289),
    (168, "linear_regression", 206.0784, 272.2682,  0.1367),
    (168, "gradient_boosting", 407.2167, 504.8244, -1.9678),
    (720, "linear_regression", 320.5417, 377.8769, -0.6852),
    (720, "gradient_boosting", 514.4697, 589.2491, -3.0978),
]

mlflow.set_experiment("gold_price_prediction")

for horizon, model_name, mae, rmse, r2 in METRICS:
    run_name = f"h={horizon}_{model_name}_20260620_153112_backfill"
    with mlflow.start_run(run_name=run_name):
        mlflow.log_param("horizon", horizon)
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("run", "initial_backfill")
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        print(f"  {run_name}: MAE={mae:.4f}  RMSE={rmse:.4f}  R²={r2:.4f}")

print("\nBackfill complete — 12 runs logged to MLflow")
