import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gold_stream")
GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
PROCESSING_INTERVAL = os.getenv("PROCESSING_INTERVAL", "10")

schema = StructType(
    [
        StructField("timestamp", TimestampType()),
        StructField("gold_price", DoubleType()),
        StructField("oil_price", DoubleType()),
        StructField("dxy", DoubleType()),
        StructField("eurusd", DoubleType()),
        StructField("jpy", DoubleType()),
    ]
)

spark = (
    SparkSession.builder.appName("GoldStreamProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

s3_client = boto3.client(
    "s3",
    endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY,
    aws_secret_access_key=GARAGE_SECRET_KEY,
    region_name="us-east-1",
    use_ssl=False,
    config=boto3.session.Config(signature_version="s3v4"),
)


def _ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")


def write_raw_to_garage(df, epoch_id):
    if df.count() == 0:
        return
    pdf = df.toPandas()
    pdf["processing_time"] = datetime.now(timezone.utc).isoformat()
    buffer = pdf.to_parquet(index=False)
    filename = f"{_ts()}/raw.parquet"
    s3_client.put_object(Bucket="raw-data", Key=filename, Body=buffer)
    print(f"Wrote {len(pdf)} raw rows to raw-data/{filename}")


def write_processed_to_garage(df, epoch_id):
    if df.count() == 0:
        return
    pdf = df.toPandas()
    pdf["processing_time"] = datetime.now(timezone.utc).isoformat()

    pdf["_minute"] = pdf["timestamp"].dt.floor("1min")
    minutely = pdf.groupby("_minute", as_index=False).last()
    minutely = minutely.drop(columns=["_minute"])

    buffer = minutely.to_parquet(index=False)
    filename = f"{_ts()}/processed.parquet"
    s3_client.put_object(Bucket="processed-data", Key=filename, Body=buffer)
<<<<<<< HEAD
    print(f"Wrote {len(minutely)} minutely rows to processed-data/{filename}")
=======
    print(f"Wrote {len(pdf)} processed rows to processed-data/{filename}")


def write_hourly_to_garage(df, epoch_id):
    if df.count() == 0:
        return
    pdf = df.toPandas()
    now = datetime.now(timezone.utc)
    hour_key = now.strftime("%Y%m%d-%H")

    latest = pdf.iloc[-1]
    hourly_df = pd.DataFrame([{
        "timestamp": now.isoformat(),
        "gold_price": latest["gold_price"],
        "oil_price": latest["oil_price"],
        "dxy": latest["dxy"],
        "eurusd": latest["eurusd"],
        "jpy": latest["jpy"],
    }])
    buffer = hourly_df.to_parquet(index=False)
    s3_client.put_object(Bucket="hourly-history", Key=f"{hour_key}.parquet", Body=buffer)


def predict_via_fastapi(df, epoch_id):
    global _history_buffer
    if df.count() == 0:
        return
    pdf = df.toPandas()
    _history_buffer = pd.concat([_history_buffer, pdf], ignore_index=True)
    _history_buffer = _history_buffer.tail(50)
    features = compute_features(_history_buffer)
    last_row = features.dropna(subset=FEATURE_COLUMNS)
    if last_row.empty:
        return
    latest = last_row.iloc[-1]
    payload = {
        "timestamp": str(latest.get("timestamp", "")),
        "features": {col: latest[col] for col in FEATURE_COLUMNS},
    }
    try:
        resp = requests.post(FASTAPI_URL, json=payload, timeout=5)
        if resp.ok:
            print(f"Prediction sent: {resp.json()}")
        else:
            print(f"FastAPI error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"FastAPI request failed: {e}")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))


stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
)

parsed_df = (
    stream_df.select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(col("gold_price").isNotNull())
)

raw_query = (
    parsed_df.writeStream.foreachBatch(write_raw_to_garage)
    .outputMode("update")
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds")
    .start()
)
print("Streaming raw data to Garage...")

processed_query = (
    parsed_df.writeStream.foreachBatch(write_processed_to_garage)
    .outputMode("update")
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds")
    .start()
)
<<<<<<< HEAD
print("Streaming aggregated processed data to Garage...")
=======
print("Streaming processed data to Garage...")

hourly_query = (
    parsed_df.writeStream.foreachBatch(write_hourly_to_garage)
    .outputMode("update")
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds")
    .start()
)
print("Streaming hourly aggregation to Garage...")

predict_query = (
    parsed_df.writeStream.foreachBatch(predict_via_fastapi)
    .outputMode("update")
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds")
    .start()
)
print("Sending features to FastAPI...")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

spark.streams.awaitAnyTermination()
