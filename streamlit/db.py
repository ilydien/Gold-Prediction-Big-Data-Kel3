import os
from datetime import timezone, timedelta

import pandas as pd
import psycopg2

WIB = timezone(timedelta(hours=7))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gold_prediction")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_predictions(horizon=None, limit=500):
    conn = get_conn()
    if horizon:
        q = """
            SELECT timestamp, predicted_price, actual_price, error, model_version, horizon
            FROM predictions WHERE horizon = %s
            ORDER BY timestamp DESC LIMIT %s
        """
        df = pd.read_sql(q, conn, params=(horizon, limit))
    else:
        q = """
            SELECT timestamp, predicted_price, actual_price, error, model_version, horizon
            FROM predictions ORDER BY timestamp DESC LIMIT %s
        """
        df = pd.read_sql(q, conn, params=(limit,))
    conn.close()
    return df


def get_metrics(limit=20):
    conn = get_conn()
    q = """
        SELECT timestamp, model_name, mae, rmse, r2, horizon
        FROM model_metrics WHERE horizon > 0
        ORDER BY timestamp DESC LIMIT %s
    """
    df = pd.read_sql(q, conn, params=(limit,))
    conn.close()
    return df


def get_horizons():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT horizon FROM predictions ORDER BY horizon")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


def get_db_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM predictions")
    pred_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM model_metrics WHERE horizon > 0")
    metric_count = cur.fetchone()[0]
    cur.execute("""
        SELECT horizon, COUNT(*), MAX(timestamp)
        FROM predictions GROUP BY horizon ORDER BY horizon
    """)
    horizon_stats = cur.fetchall()
    cur.execute("SELECT MAX(timestamp) FROM predictions")
    latest_pred = cur.fetchone()[0]
    cur.execute("SELECT MAX(timestamp) FROM model_metrics")
    latest_metric = cur.fetchone()[0]
    cur.close()
    conn.close()

    if latest_pred:
        latest_pred = latest_pred.replace(tzinfo=timezone.utc).astimezone(WIB)
    if latest_metric:
        latest_metric = latest_metric.replace(tzinfo=timezone.utc).astimezone(WIB)

    return {
        "pred_count": pred_count,
        "metric_count": metric_count,
        "horizon_stats": horizon_stats,
        "latest_prediction": latest_pred,
        "latest_metrics": latest_metric,
    }
