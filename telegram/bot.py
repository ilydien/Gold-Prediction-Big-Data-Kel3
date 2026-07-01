import logging
import os
import time
from datetime import timezone, timedelta

import psycopg2
import requests

WIB = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gold_prediction")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "1"))
PRICE_THRESHOLD = float(os.getenv("PRICE_THRESHOLD", "10.0"))


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN:
        log.info(f"[Telegram would send]: {message}")
        return
    try:
        resp = requests.post(
            TELEGRAM_API,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
        if resp.ok:
            log.info("Telegram message sent")
        else:
            log.warning(f"Telegram API error: {resp.status_code} {resp.text}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


_last_prediction_id = None


def check_and_alert():
    global _last_prediction_id
    try:
        conn = pg_connect()
        cur = conn.cursor()

        cur.execute(
            "SELECT id, timestamp, predicted_price "
            "FROM predictions ORDER BY timestamp DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return

        pred_id, ts, predicted_price = row

        if pred_id == _last_prediction_id:
            return

        _last_prediction_id = pred_id

        cur.execute(
            "SELECT timestamp, predicted_price "
            "FROM predictions ORDER BY timestamp DESC LIMIT 1 OFFSET 1"
        )
        prev = cur.fetchone()

        cur.close()
        conn.close()

        ts_wib = ts.replace(tzinfo=timezone.utc).astimezone(WIB)
        msg = (
            f"💰 *Gold Price Alert*\n"
            f"Time: {ts_wib.strftime('%Y-%m-%d %H:%M:%S WIB')}\n"
            f"Predicted: ${predicted_price:.2f}"
        )

        if prev:
            prev_price = prev[1]
            change = abs(predicted_price - prev_price)
            if change >= PRICE_THRESHOLD:
                direction = "📈 Up" if predicted_price > prev_price else "📉 Down"
                msg += f"\nChange: {direction} ${change:.2f}"

        send_telegram(msg)

    except Exception as e:
        log.error(f"Check failed: {e}")


if __name__ == "__main__":
    log.info(f"Telegram bot started (interval: {CHECK_INTERVAL}s, threshold: ${PRICE_THRESHOLD})")

    send_telegram("🤖 *Gold Prediction Bot started*\nMonitoring gold prices...")

    while True:
        check_and_alert()
        time.sleep(CHECK_INTERVAL)
