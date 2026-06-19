import os
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

    return run_training()


@flow(name="Gold Prediction Pipeline", log_prints=True)
def gold_pipeline():
    global _start_time
    _start_time = time.time()

<<<<<<< HEAD
    from storage.trainer.retraining import should_retrain, save_training_state, build_training_state
=======
    from storage.trainer.retraining import (
        should_retrain,
        save_training_state,
        build_training_state,
    )
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    do_retrain, reason, object_count = should_retrain()

    print(f"\n{'='*50}")
    print(f"  GOLD PREDICTION PIPELINE")
    print(f"  Garage:  {GARAGE_ENDPOINT}")
    print(f"  Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}")
    print(f"  Retrain: {reason}")
    print(f"{'='*50}\n")

    if not do_retrain:
        print(f"[SKIP] {reason}")
        elapsed = time.time() - _start_time
        print(f"  Pipeline skipped ({elapsed:.1f}s)")
        return

    try:
        feat_result = run_feature_engineering()
        print(f"[OK] {feat_result}")
    except Exception as e:
        print(f"[FAIL] Feature engineering failed: {e}")
        raise

    try:
<<<<<<< HEAD
        metadata = run_ml_training()
        print(f"[OK] Training complete — best model: {metadata['model_name']}")
=======
        all_metadata = run_ml_training()
        trained = list(all_metadata.keys()) if all_metadata else []
        print(f"[OK] Training complete — horizons: {trained}")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    except Exception as e:
        print(f"[FAIL] ML training failed: {e}")
        raise

<<<<<<< HEAD
    state = build_training_state(metadata, object_count)
    save_training_state(state)
=======
    if all_metadata:
        for horizon, meta in all_metadata.items():
            state = build_training_state(meta, object_count)
            save_training_state(state, horizon)
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    elapsed = time.time() - _start_time
    print(f"\n{'='*50}")
    print(f"  PIPELINE COMPLETE ({elapsed:.1f}s)")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    state = gold_pipeline(return_state=True)
    if state.is_failed() or state.is_crashed():
        os._exit(1)
