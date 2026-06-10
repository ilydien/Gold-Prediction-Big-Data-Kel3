import json
import os
import random
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gold_stream")
MIN_INTERVAL = int(os.getenv("MIN_INTERVAL", "1"))
MAX_INTERVAL = int(os.getenv("MAX_INTERVAL", "5"))

TICKERS = {
    "gold_price": "GC=F",
    "oil_price": "CL=F",
    "dxy": "DX-Y.NYB",
    "eurusd": "EURUSD=X",
    "jpy": "JPY=X",
}

print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Topic: {TOPIC}")
print(f"Interval: {MIN_INTERVAL}-{MAX_INTERVAL}s")
print(f"Tickers: {list(TICKERS.values())}")

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

ticker_objects = {}
valid_tickers = []
for field, symbol in TICKERS.items():
    try:
        ticker = yf.Ticker(symbol)
        ticker.history(period="1d", interval="1m")
        ticker_objects[field] = ticker
        valid_tickers.append(field)
        print(f"  {field} ({symbol}): OK")
    except Exception as e:
        print(f"  {field} ({symbol}): FAILED - {e}")

if not valid_tickers:
    print("No tickers available. Exiting.")
    exit(1)

print(f"Valid tickers: {valid_tickers}")

while True:
    data = {"timestamp": datetime.now(timezone.utc).isoformat()}
    success = False

    for field in valid_tickers:
        try:
            price = (
                ticker_objects[field]
                .history(period="1d", interval="1m")["Close"]
                .iloc[-1]
            )
            data[field] = round(float(price), 5)
            success = True
        except Exception as e:
            print(f"Error fetching {field}: {e}")

    if success:
        try:
            producer.send(TOPIC, value=data)
            print(f"Sent: {data}")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
    else:
        print("No data fetched this cycle")

    sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
    time.sleep(sleep_time)
