import json
import os
import random
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gold_stream")
MIN_INTERVAL = float(os.getenv("MIN_INTERVAL", "0.01"))
MAX_INTERVAL = float(os.getenv("MAX_INTERVAL", "0.05"))
DATA_PATH = os.getenv("DATA_PATH", "data/gold_data.csv")

print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Topic: {TOPIC}")
print(f"Interval: {MIN_INTERVAL}-{MAX_INTERVAL}s")
print(f"Data file: {DATA_PATH}")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Waiting for Kafka: {e}")
        time.sleep(5)

df = pd.read_csv(DATA_PATH)
total = len(df)
print(f"Loaded {total} rows from {DATA_PATH}")

sent = 0
for _, row in df.iterrows():
    data = {
        "timestamp": row["timestamp"],
        "gold_price": float(row["gold_price"]),
        "oil_price": float(row["oil_price"]),
        "dxy": float(row["dxy"]),
        "eurusd": float(row["eurusd"]),
        "jpy": float(row["jpy"]),
    }

    try:
        producer.send(TOPIC, value=data)
        sent += 1
        print(f"[{sent}/{total}] Sent: {data['timestamp']} gold={data['gold_price']}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

    if sent < total:
        sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        time.sleep(sleep_time)

producer.flush()
producer.close()
print(f"\nDone. Sent {sent} messages to topic '{TOPIC}'.")
