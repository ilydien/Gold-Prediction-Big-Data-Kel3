import subprocess
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from db import get_db_stats, get_conn
from garage import GARAGE_ENDPOINT
from garage import get_bucket_stats, test_connection, list_buckets

st.set_page_config(page_title="System Dashboard", page_icon="⚙️", layout="wide")

st.title("⚙️ System Dashboard")
st.markdown("---")

col1, col2, col3 = st.columns(3)

try:
    stats = get_db_stats()
    col1.metric("Total Predictions", stats["pred_count"])
    col2.metric("Total Metrics", stats["metric_count"])
    latest = stats["latest_prediction"]
    col3.metric("Last Prediction", str(latest)[:19] if latest else "Never")
except Exception as e:
    col1.error(f"DB error: {e}")

st.markdown("---")
tab1, tab2, tab3, tab4 = st.tabs([
    "🖥️ Services", "🗄️ Garage Storage",
    "📡 API Health", "📊 Pipeline Flow",
])

with tab1:
    st.subheader("Container Status")

    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}|{{.Status}}|{{.Ports}}"],
            capture_output=True, text=True, timeout=10,
        )
        rows = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("|", 2)
            name = parts[0] if len(parts) > 0 else "?"
            status = parts[1] if len(parts) > 1 else "?"
            ports = parts[2] if len(parts) > 2 else ""
            is_running = "Up" in status
            rows.append({
                "Container": name,
                "Status": "✅ Running" if is_running else "❌ " + status,
                "Ports": ports,
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No containers found")
    except Exception as e:
        st.error(f"Docker not accessible: {e}")
        st.info("Install Docker or run this dashboard inside a Docker container with Docker socket mounted.")

with tab2:
    st.subheader("Garage Connection")

    ok, msg = test_connection()
    if ok:
        st.success(f"Garage connected: {GARAGE_ENDPOINT}")
    else:
        st.error(f"Garage unavailable: {msg}")

    try:
        buckets = list_buckets()
        st.markdown(f"**Buckets:** {', '.join(buckets)}")
    except Exception:
        buckets = []

    if ok:
        bucket_stats = get_bucket_stats()
        rows = []
        for name, s in bucket_stats.items():
            rows.append({
                "Bucket": name,
                "Objects": s.get("count", 0),
                "Size (MB)": s.get("size_mb", 0),
                "Last Modified": str(s.get("last_modified", "N/A"))[:19],
                "Status": "✅" if not s.get("error") else f"❌ {s['error']}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

with tab3:
    st.subheader("FastAPI Health")

    try:
        req = urllib.request.Request("http://fastapi:8000/")
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        st.success("FastAPI is up")

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Status", data.get("status", "?"))
        col_b.metric("Horizons Loaded", len(data.get("horizons_loaded", []))),
        champions = data.get("champions", {})
        col_c.metric("Models Loaded", len(champions))

        if champions:
            st.subheader("Loaded Champion Models")
            champ_df = pd.DataFrame([
                {"Horizon": h, "Model": m} for h, m in champions.items()
            ])
            st.dataframe(champ_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"FastAPI unreachable: {e}")

    st.markdown("---")
    st.subheader("Endpoint Check")

    endpoints = {
        "GET /": "http://fastapi:8000/",
        "GET /predictions": "http://fastapi:8000/predictions?limit=5",
        "GET /metrics": "http://fastapi:8000/metrics?limit=5",
    }
    for name, url in endpoints.items():
        try:
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=3)
            st.success(f"✅ {name} — {resp.status}")
        except Exception as e:
            st.error(f"❌ {name} — {e}")

with tab4:
    st.subheader("Data Pipeline Status")

    steps = {
        "Yahoo Finance → Kafka": ("🟢" if ok else "🔴", "Producer" if ok else "Check person1"),
        "Kafka → Spark Streaming": ("🟢" if ok else "🔴", "Spark job" if ok else "Check person2"),
        "Spark → Garage (processed-data)": ("🟢" if ok else "🔴", "Bucket: processed-data"),
        "Feature Pipeline": ("🟢" if ok else "🔴", "Compute 25 features"),
        "ML Training": ("🟢" if ok else "🔴", "Train 2 models × 6 horizons"),
        "PostgreSQL (predictions)": ("🟢" if stats["pred_count"] > 0 else "🔴", f"{stats['pred_count']} rows"),
        "PostgreSQL (model_metrics)": ("🟢" if stats["metric_count"] > 0 else "🔴", f"{stats['metric_count']} rows"),
        "FastAPI Serving": ("🟢" if resp.status == 200 else "🔴", "API running"),
        "Streamlit Dashboard": ("🟢", "You're here!"),
    }

    for step, (icon, detail) in steps.items():
        cols = st.columns([2, 1, 4])
        cols[0].markdown(f"{icon} **{step}**")
        cols[1].markdown(f"`{detail}`")

    st.markdown("---")
    st.subheader("PostgreSQL Details")

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT horizon, COUNT(*) as cnt,
                   ROUND(AVG(predicted_price)::numeric, 2) as avg_price,
                   MAX(timestamp)::text as last_time
            FROM predictions GROUP BY horizon ORDER BY horizon
        """)
        rows = cur.fetchall()
        if rows:
            horizon_df = pd.DataFrame(rows, columns=["Horizon", "Count", "Avg Price", "Last Prediction"])
            st.dataframe(horizon_df, use_container_width=True, hide_index=True)
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"DB query failed: {e}")

st.markdown("---")
st.caption("Gold Prediction Dashboard | System Page | Big Data Kelompok 3")
