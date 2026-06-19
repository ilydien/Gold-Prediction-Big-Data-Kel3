import os
import sys
import time

from prefect import flow, task

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "****" if os.getenv("GARAGE_ACCESS_KEY") else "NOT SET")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "NOT SET")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

_start_time = None


@task(name="Run Feature Engineering")
def run_feature_engineering():
    from storage.features.feature_pipeline import run_feature_pipeline

    run_feature_pipeline()
    return "Feature engineering complete"


@task(name="Run ML Training")
def run_ml_training():
    from storage.trainer.train import run_training

    run_training()
    return "Training complete"


@flow(name="Gold Prediction Pipeline", log_prints=True)
def gold_pipeline():
    global _start_time
    _start_time = time.time()

    print(f"\n{'='*50}")
    print(f"  GOLD PREDICTION PIPELINE")
    print(f"  Garage:  {GARAGE_ENDPOINT}")
    print(f"  Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}")
    print(f"{'='*50}\n")

    try:
        feat_result = run_feature_engineering()
        print(f"[OK] {feat_result}")
    except Exception as e:
        print(f"[FAIL] Feature engineering failed: {e}")
        raise

    try:
        train_result = run_ml_training()
        print(f"[OK] {train_result}")
    except Exception as e:
        print(f"[FAIL] ML training failed: {e}")
        raise

    elapsed = time.time() - _start_time
    print(f"\n{'='*50}")
    print(f"  PIPELINE COMPLETE ({elapsed:.1f}s)")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    state = gold_pipeline(return_state=True)
    if state.is_failed() or state.is_crashed():
        os._exit(1)
