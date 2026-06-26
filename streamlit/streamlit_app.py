import streamlit as st

from db import get_db_stats, get_conn
from garage import test_connection

st.set_page_config(
    page_title="Gold Price Prediction",
    page_icon="💰",
    layout="wide",
)

st.title("💰 Gold Price Prediction Dashboard")
st.markdown("Welcome to the **Gold Price Prediction System** — Big Data Kelompok 3")
st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Model Dashboard")
    st.markdown("""
    - Prediction history & trends
    - Model performance (MAE, RMSE, R²)
    - Actual vs predicted scatter plot
    - Champion models & feature importance
    """)

with col2:
    st.subheader("⚙️ System Dashboard")
    st.markdown("""
    - Service health monitoring
    - Garage storage overview
    - API endpoint status
    - Pipeline flow status
    """)

st.markdown("---")
col_left, col_mid, col_right = st.columns(3)

try:
    stats = get_db_stats()
    col_left.metric("Predictions in DB", stats["pred_count"])
    col_mid.metric("Model Metrics", stats["metric_count"])
except Exception as e:
    col_left.error(f"DB: {e}")

try:
    ok, _ = test_connection()
    col_right.metric("Garage", "✅ Connected" if ok else "❌ Disconnected")
except Exception:
    col_right.metric("Garage", "❌ Error")

st.markdown("---")
st.info("👈 Navigate between pages using the sidebar")

st.markdown("---")
st.caption("Gold Prediction Dashboard | Big Data Kelompok 3")
