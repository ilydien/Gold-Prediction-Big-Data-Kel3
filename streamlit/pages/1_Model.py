import json
import subprocess
import urllib.request
from datetime import datetime, timezone, timedelta

import altair as alt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from db import get_db_stats, get_horizons, get_metrics, get_predictions
from garage import test_connection

WIB = timezone(timedelta(hours=7))

st.set_page_config(page_title="Model Dashboard", page_icon="📊", layout="wide")

st.title("📊 Model Dashboard")
st.markdown("---")


def check_services():
    statuses = {}

    try:
        req = urllib.request.Request("http://fastapi:8000/")
        urllib.request.urlopen(req, timeout=5)
        statuses["api"] = True
    except Exception:
        statuses["api"] = False

    try:
        ok, _ = test_connection()
        statuses["garage"] = ok
    except Exception:
        statuses["garage"] = False

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5,
        )
        containers = [c for c in result.stdout.strip().split("\n") if c]
        statuses["docker"] = len(containers) > 0
    except Exception:
        statuses["docker"] = False

    up_count = sum(1 for v in statuses.values() if v)
    if up_count == 3:
        statuses["level"] = "✅ All Operational"
    elif up_count >= 1:
        statuses["level"] = "⚠️ Partial Outage"
    else:
        statuses["level"] = "🔴 Major Outage"

    return statuses


# ---- fetch data ----
try:
    req = urllib.request.Request("http://fastapi:8000/market/latest")
    resp = urllib.request.urlopen(req, timeout=5)
    market = json.loads(resp.read().decode())
except Exception:
    market = None

try:
    df_pred = get_predictions(limit=1)
    latest_pred = df_pred.iloc[0]["predicted_price"] if not df_pred.empty else None
except Exception:
    latest_pred = None

try:
    db_stats = get_db_stats()
except Exception:
    db_stats = {}

try:
    horizons = get_horizons()
except Exception:
    horizons = []

try:
    df_all = get_predictions(limit=100)
    avg_price = df_all["predicted_price"].mean() if not df_all.empty else None
except Exception:
    avg_price = None

try:
    df_metrics = get_metrics(limit=1)
    best_mae_row = df_metrics.iloc[0] if not df_metrics.empty else None
except Exception:
    best_mae_row = None

alerts = check_services()

# ---- ROW 1: Market Data (6 individual cards) ----
st.caption("📡 Market Data")
c11, c12, c13, c14, c15, c16 = st.columns(6)

with c11:
    with st.container(border=True):
        if market:
            st.metric("Gold Price", f"${market['gold_price']:.2f}")
        else:
            st.metric("Gold Price", "N/A")

with c12:
    with st.container(border=True):
        if market:
            st.metric("Gold Prediction", f"${latest_pred:.2f}" if latest_pred else "N/A")
        else:
            st.metric("Gold Prediction", "N/A")

with c13:
    with st.container(border=True):
        if market:
            st.metric("DXY", f"{market['dxy']:.2f}")
        else:
            st.metric("DXY", "N/A")

with c14:
    with st.container(border=True):
        if market:
            st.metric("EUR/USD", f"{market['eurusd']:.4f}")
        else:
            st.metric("EUR/USD", "N/A")

with c15:
    with st.container(border=True):
        if market:
            st.metric("JPY", f"{market['jpy']:.2f}")
        else:
            st.metric("JPY", "N/A")

with c16:
    with st.container(border=True):
        if market:
            st.metric("CL=F", f"${market['oil_price']:.2f}")
        else:
            st.metric("CL=F", "N/A")


# ---- ROW 2: Key Metrics (4 individual cards) ----
st.caption("📊 Key Metrics")
m1, m2, m3, m4 = st.columns(4)

with m1:
    with st.container(border=True):
        try:
            latest_row = df_pred.iloc[0] if not df_pred.empty else None
            if latest_row is not None:
                st.metric(
                    "Latest Prediction",
                    f"${latest_row['predicted_price']:.2f}",
                    latest_row["model_version"],
                )
            else:
                st.metric("Latest Prediction", "No data")
        except Exception:
            st.metric("Latest Prediction", "?")

with m2:
    with st.container(border=True):
        st.metric("Avg Prediction", f"${avg_price:.2f}" if avg_price else "No data")

with m3:
    with st.container(border=True):
        st.metric("Total Predictions", db_stats.get("pred_count", "?"))

with m4:
    with st.container(border=True):
        if best_mae_row is not None:
            st.metric(
                "Best MAE",
                f"{best_mae_row['mae']:.2f}",
                f"{best_mae_row['model_name']} h={best_mae_row['horizon']}",
            )
        else:
            st.metric("Best MAE", "No data")


# ---- ROW 3: Status & Alerts (4 individual cards) ----
st.caption("🔔 Status & Alerts")
s1, s2, s3, s4 = st.columns(4)

with s1:
    with st.container(border=True):
        st.metric("Total Metrics", db_stats.get("metric_count", "?"))

with s2:
    with st.container(border=True):
        if db_stats and db_stats.get("latest_prediction"):
            last_update = str(db_stats["latest_prediction"])[:19]
        else:
            last_update = "Never"
        st.metric("Last Update", last_update)

with s3:
    with st.container(border=True):
        st.metric("Alert Status", alerts["level"])

with s4:
    with st.container(border=True):
        st.metric("Last Alert", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


st.markdown("---")


# ---- Tabs ----
tab1, tab2 = st.tabs([
    "📈 Prediction History",
    "🎯 Actual vs Predicted",
])

with tab1:
    try:
        df_ts = get_predictions(limit=1000)
        if not df_ts.empty:
            df_ts["timestamp"] = pd.to_datetime(df_ts["timestamp"], utc=True).dt.tz_convert(WIB).dt.tz_localize(None)
            df_ts = df_ts.sort_values("timestamp")

            horizon_opts = sorted(df_ts["horizon"].unique())
            selected_horizon = st.selectbox(
                "Filter by Horizon",
                options=horizon_opts,
                index=len(horizon_opts) - 1,
                key="tab1_horizon",
            )
            df_h = df_ts[df_ts["horizon"] == selected_horizon]


            if not df_h.empty:
                df_chart = df_h.dropna(subset=["actual_price", "predicted_price"])

                if not df_chart.empty:
                    df_melted = df_chart.melt(
                        id_vars=["timestamp"],
                        value_vars=["actual_price", "predicted_price"],
                        var_name="series",
                        value_name="price",
                    )
                    df_melted["series"] = df_melted["series"].replace({
                        "actual_price": "Actual",
                        "predicted_price": "Predicted",
                    })

                    color_scale = alt.Scale(domain=["Actual", "Predicted"], range=["#1f77b4", "#ff7f0e"])

                    line = (
                        alt.Chart(df_melted)
                        .mark_line(strokeWidth=2)
                        .encode(
                            x=alt.X("timestamp:T", title="Time"),
                            y=alt.Y("price:Q", title="Price (USD)", scale=alt.Scale(zero=False)),
                            color=alt.Color("series:N", scale=color_scale, legend=alt.Legend(title=None)),
                            strokeDash=alt.condition(
                                alt.datum.series == "Predicted",
                                alt.value([6, 4]),
                                alt.value([]),
                            ),
                            tooltip=[
                                alt.Tooltip("timestamp:T", title="Time"),
                                alt.Tooltip("price:Q", format="$.2f"),
                                alt.Tooltip("series:N", title="Type"),
                            ],
                        )
                    )

                    combined = (
                        line
                        .properties(height=400)
                        .configure_axis(grid=True, gridDash=[2, 4], gridOpacity=0.3)
                        .configure_view(strokeWidth=0)
                        .configure_legend(
                            orient="top",
                            labelFontSize=12,
                            symbolStrokeWidth=3,
                        )
                    )

                    st.altair_chart(combined, use_container_width=True)

                    col_h, col_m = st.columns(2)
                    with col_h:
                        st.metric(
                            "Horizon displayed",
                            f"{selected_horizon}h" if selected_horizon else "All",
                        )
                    with col_m:
                        st.metric("Data points", len(df_chart))
                else:
                    st.info("No data with actual prices for selected horizon")
            else:
                st.info("No data for selected horizon")
        else:
            st.info("No prediction data yet. Run a prediction first!")
    except Exception as e:
        st.error(f"Failed to load prediction chart: {e}")

with tab2:
    time_range = st.selectbox(
        "Lookback Period",
        options=["1d", "5d", "1mo", "3mo", "6mo", "1y", "ytd"],
        index=2,
        key="tab2_range",
    )

    if time_range == "1d":
        interval = "5m"
    elif time_range in ("5d", "1mo"):
        interval = "15m"
    elif time_range == "3mo":
        interval = "1h"
    else:
        interval = "1d"

    try:
        ticker = yf.Ticker("GC=F")
        gc = ticker.history(period=time_range, interval=interval)
        if gc.empty:
            st.warning("No historical data from Yahoo Finance")
        else:
            fig = go.Figure()

            # Plot GC=F historical Close prices with a clean green line and NO zero fill
            fig.add_trace(
                go.Scatter(
                    x=gc.index,
                    y=gc["Close"],
                    mode="lines",
                    line=dict(color="#00cc96", width=2.0),
                    hovertemplate="<b>%{x|%b %d, %H:%M}</b><br>Price: $%{y:.2f}<extra></extra>",
                ),
            )

            try:
                df_preds = get_predictions(limit=3000)
                if not df_preds.empty:
                    df_preds["timestamp"] = pd.to_datetime(df_preds["timestamp"], utc=True).dt.tz_convert(WIB)
                    df_preds = df_preds.dropna(subset=["predicted_price"])
                    df_preds = df_preds.sort_values("timestamp")

                    horizon_opts = sorted(df_preds["horizon"].unique())
                    sel_h = st.selectbox(
                        "Prediction Horizon",
                        options=horizon_opts,
                        format_func=lambda h: f"{h}h",
                        index=len(horizon_opts) - 1,
                        key="tab2_horizon",
                    )
                    df_ph = df_preds[df_preds["horizon"] == sel_h]

                    if not df_ph.empty:
                        min_t = gc.index.min().tz_convert(WIB)
                        max_t = max(gc.index.max().tz_convert(WIB), df_ph["timestamp"].max())
                        df_ph = df_ph[(df_ph["timestamp"] >= min_t) & (df_ph["timestamp"] <= max_t)]

                        if not df_ph.empty:
                            df_ph["minute"] = df_ph["timestamp"].dt.floor("1min")
                            df_ph = df_ph.groupby("minute", as_index=False).last()

                        if not df_ph.empty:
                            customdata = np.column_stack([
                                [f"${v:.2f}" if pd.notna(v) else "N/A" for v in df_ph["actual_price"]],
                            ])

                            fig.add_trace(
                                go.Scatter(
                                    x=df_ph["timestamp"],
                                    y=df_ph["predicted_price"],
                                    mode="markers",
                                    marker=dict(
                                        size=8,
                                        color="#ff7f0e",
                                        symbol="circle",
                                        line=dict(width=1, color="white"),
                                    ),
                                    hovertemplate="<b>Prediction</b><br>"
                                                   "Time: %{x|%b %d, %H:%M}<br>"
                                                   "Predicted: $%{y:.2f}<br>"
                                                   "Actual: %{customdata[0]}"
                                                   "<extra></extra>",
                                    customdata=customdata,
                                ),
                            )
            except Exception:
                pass

            fig.update_layout(
                xaxis=dict(type="date"),
                yaxis=dict(
                    title="Price (USD)",
                    fixedrange=False,
                    tickprefix="$",
                    showgrid=True,
                    gridcolor="rgba(128,128,128,0.15)",
                ),
                hovermode="x unified",
                dragmode="pan",
                template="plotly_white",
                height=550,
                margin=dict(l=0, r=0, t=30, b=0),
                showlegend=False,
            )

            st.plotly_chart(fig, use_container_width=True)

            col_a, col_b = st.columns(2)
            with col_a:
                st.caption("🔵 Line: Yahoo Finance GC=F &nbsp;|&nbsp; 🟠 Dots: Model Predictions")
            with col_b:
                st.caption(f"Interval: {interval} &nbsp;·&nbsp; Data points: {len(gc)}")
    except Exception as e:
        st.error(f"Failed to load chart: {e}")

st.markdown("---")
st.caption("Gold Prediction Dashboard | Model Page | Big Data Kelompok 3")
