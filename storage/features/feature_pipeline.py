import io
import os
import time

import boto3
import pandas as pd

from shared.features import FEATURE_COLUMNS, TARGET_COLUMN, compute_features

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
MAX_RETRIES = int(os.getenv("GARAGE_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("GARAGE_RETRY_DELAY", "5"))

s3 = boto3.client(
    "s3",
    endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY,
    aws_secret_access_key=GARAGE_SECRET_KEY,
    region_name="us-east-1",
    use_ssl=False,
    config=boto3.session.Config(signature_version="s3v4"),
)


def _retry_s3(fn, desc: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"  Retry {attempt}/{MAX_RETRIES} for {desc}: {e}")
            time.sleep(RETRY_DELAY * attempt)


def list_parquet_files(bucket: str) -> list[str]:
    keys = []

    def _list():
        nonlocal keys
        keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        return keys

    _retry_s3(_list, f"list {bucket}")
    return keys


def read_parquet_from_garage(bucket: str, key: str) -> pd.DataFrame:
    def _read():
        response = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(response["Body"].read()))

    return _retry_s3(_read, f"download {key}")


def write_parquet_to_garage(df: pd.DataFrame, bucket: str, prefix: str):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    key = f"{prefix}/features.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"Wrote {len(df)} rows to {bucket}/{key}")


def run_feature_pipeline():
    print("Reading processed data from Garage...")
    processed_files = list_parquet_files("processed-data")
    if not processed_files:
        print("No processed data found")
        return

    all_data = []
    for f in processed_files:
        print(f"  Downloading: {f}")
        df = read_parquet_from_garage("processed-data", f)
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    print(f"Loaded {len(combined)} rows from {len(processed_files)} files")

    features = compute_features(combined)
    features_clean = features.dropna(subset=FEATURE_COLUMNS).reset_index(drop=True)
    print(f"Computed features: {len(features_clean)} rows")

    write_parquet_to_garage(features_clean, "features", "latest")
    print("Feature pipeline complete!")


if __name__ == "__main__":
    run_feature_pipeline()
