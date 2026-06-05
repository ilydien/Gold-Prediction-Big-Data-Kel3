import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lag, stddev
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gold_stream")
GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000/predict")
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
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
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


def write_raw_to_garage(df, epoch_id):
    if df.count() == 0:
        return
    pdf = df.toPandas()
    pdf["processing_time"] = datetime.now(timezone.utc).isoformat()
    buffer = pdf.to_parquet(index=False)
    filename = f"batch={epoch_id}/data.parquet"
    s3_client.put_object(Bucket="raw-data", Key=filename, Body=buffer)
    print(f"Wrote {len(pdf)} raw rows to raw-data/{filename}")


def write_processed_to_garage(df, epoch_id):
    if df.count() == 0:
        return
    pdf = df.toPandas()
    window_spec = Window.orderBy("timestamp")
    pdf_spark = spark.createDataFrame(pdf)
    features = (
        pdf_spark.withColumn("lag_1", lag("gold_price", 1).over(window_spec))
        .withColumn("lag_5", lag("gold_price", 5).over(window_spec))
        .withColumn("lag_10", lag("gold_price", 10).over(window_spec))
    )

    pdf_result = features.toPandas().dropna()
    if len(pdf_result) == 0:
        return
    pdf_result["processing_time"] = datetime.now(timezone.utc).isoformat()
    buffer = pdf_result.to_parquet(index=False)
    filename = f"batch={epoch_id}/data.parquet"
    s3_client.put_object(Bucket="processed-data", Key=filename, Body=buffer)
    print(f"Wrote {len(pdf_result)} processed rows to processed-data/{filename}")


def predict_via_fastapi(df, epoch_id):
    if df.count() == 0:
        return
    latest = df.orderBy(col("timestamp").desc()).first()
    if latest is None:
        return
    payload = {
        "timestamp": latest["timestamp"].isoformat(),
        "gold_price": latest["gold_price"],
        "oil_price": latest["oil_price"],
        "dxy": latest["dxy"],
        "eurusd": latest["eurusd"],
        "jpy": latest["jpy"],
    }
    try:
        resp = requests.post(FASTAPI_URL, json=payload, timeout=5)
        if resp.ok:
            print(f"Prediction sent: {resp.json()}")
        else:
            print(f"FastAPI error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"FastAPI request failed: {e}")


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
print("Streaming processed data to Garage...")

predict_query = (
    parsed_df.writeStream.foreachBatch(predict_via_fastapi)
    .outputMode("update")
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds")
    .start()
)
print("Sending predictions to FastAPI...")

spark.streams.awaitAnyTermination()
