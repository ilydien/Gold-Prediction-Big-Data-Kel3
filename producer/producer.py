import json
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "gold_stream"
INTERVAL = 60

print(f"Connecting to Kafka at {KAFKA_BROKER}...")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Waiting for Kafka: {e}")
        time.sleep(5)

while True:
    try:
        gold = yf.Ticker("GC=F")
        oil = yf.Ticker("CL=F")
        dxy = yf.Ticker("DX-Y.NYB")

        gold_price = gold.history(period="1d", interval="1m")["Close"].iloc[-1]
        oil_price = oil.history(period="1d", interval="1m")["Close"].iloc[-1]
        dxy_price = dxy.history(period="1d", interval="1m")["Close"].iloc[-1]

        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "gold_price": round(float(gold_price), 2),
            "oil_price": round(float(oil_price), 2),
            "dxy": round(float(dxy_price), 2),
        }

        producer.send(TOPIC, value=data)
        print(f"Sent: {data}")

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(INTERVAL)
