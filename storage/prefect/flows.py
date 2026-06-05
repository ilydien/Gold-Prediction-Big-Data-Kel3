import os

from prefect import flow, task

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")


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


@flow(name="Gold Prediction Pipeline")
def gold_pipeline():
    feat_result = run_feature_engineering()
    print(feat_result)

    train_result = run_ml_training()
    print(train_result)


if __name__ == "__main__":
    gold_pipeline()
