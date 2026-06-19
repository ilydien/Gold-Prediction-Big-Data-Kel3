import pandas as pd

FEATURE_COLUMNS = [
    # Lag features (7)
    "lag_1",
<<<<<<< HEAD
    "lag_5",
    "lag_10",
    "rolling_mean_5",
    "rolling_std_5",
    "return_gold",
    "return_oil",
    "return_dxy",
    "return_eurusd",
    "return_jpy",
    "lag_1_oil",
    "lag_1_dxy",
=======
    "lag_3",
    "lag_6",
    "lag_12",
    "lag_24",
    "lag_72",
    "lag_168",

    # Rolling statistics (6)
    "rolling_mean_24",
    "rolling_std_24",
    "rolling_mean_72",
    "rolling_std_72",
    "rolling_mean_168",
    "rolling_std_168",

    # Returns (5)
    "return_1h",
    "return_24h",
    "return_168h",
    "return_oil_24h",
    "return_dxy_24h",

    # Cross-asset (5)
    "lag_1_oil",
    "lag_1_dxy",
    "lag_1_eurusd",
    "gold_dxy_ratio",
    "oil_dxy_ratio",

    # Calendar (2)
    "hour_of_day",
    "day_of_week",
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
]

TARGET_COLUMN = "gold_price"
FEATURE_COUNT = len(FEATURE_COLUMNS)
LOOKBACK_HOURS = 168


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp").reset_index(drop=True)
    result = df.copy()

    # Lag features
    result["lag_1"] = result["gold_price"].shift(1)
    result["lag_3"] = result["gold_price"].shift(3)
    result["lag_6"] = result["gold_price"].shift(6)
    result["lag_12"] = result["gold_price"].shift(12)
    result["lag_24"] = result["gold_price"].shift(24)
    result["lag_72"] = result["gold_price"].shift(72)
    result["lag_168"] = result["gold_price"].shift(168)

    # Rolling statistics
    result["rolling_mean_24"] = result["gold_price"].rolling(24, min_periods=1).mean()
    result["rolling_std_24"] = result["gold_price"].rolling(24, min_periods=1).std()
    result["rolling_mean_72"] = result["gold_price"].rolling(72, min_periods=1).mean()
    result["rolling_std_72"] = result["gold_price"].rolling(72, min_periods=1).std()
    result["rolling_mean_168"] = result["gold_price"].rolling(168, min_periods=1).mean()
    result["rolling_std_168"] = result["gold_price"].rolling(168, min_periods=1).std()

<<<<<<< HEAD
    result["return_gold"] = result["gold_price"].pct_change()
    result["return_oil"] = result["oil_price"].pct_change()
    result["return_dxy"] = result["dxy"].pct_change()
    result["return_eurusd"] = result["eurusd"].pct_change()
    result["return_jpy"] = result["jpy"].pct_change()

    result["lag_1_oil"] = result["oil_price"].shift(1)
    result["lag_1_dxy"] = result["dxy"].shift(1)
=======
    # Returns
    result["return_1h"] = result["gold_price"].pct_change(1)
    result["return_24h"] = result["gold_price"].pct_change(24)
    result["return_168h"] = result["gold_price"].pct_change(168)
    result["return_oil_24h"] = result["oil_price"].pct_change(24)
    result["return_dxy_24h"] = result["dxy"].pct_change(24)

    # Cross-asset
    result["lag_1_oil"] = result["oil_price"].shift(1)
    result["lag_1_dxy"] = result["dxy"].shift(1)
    result["lag_1_eurusd"] = result["eurusd"].shift(1)
    result["gold_dxy_ratio"] = result["gold_price"] / result["dxy"]
    result["oil_dxy_ratio"] = result["oil_price"] / result["dxy"]

    # Calendar
    timestamps = pd.to_datetime(result["timestamp"], format="mixed")
    result["hour_of_day"] = timestamps.dt.hour
    result["day_of_week"] = timestamps.dt.dayofweek
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    return result


def get_feature_vector(
    history: pd.DataFrame, latest_row: dict
) -> dict:
    """
    Compute 25 features for inference.
    - history: hourly data (min 168 rows) with columns:
        timestamp, gold_price, oil_price, dxy, eurusd, jpy
    - latest_row: current gold price from Dio (treated as current hour)
        {"timestamp": "2026-06-19T15:00:00", "gold_price": 2355.0, ...}
    """
    latest_df = pd.DataFrame([latest_row])
    df = pd.concat([history, latest_df], ignore_index=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    features = compute_features(df)
    last_row = features.iloc[-1]

    return {
        col: float(last_row[col])
        for col in FEATURE_COLUMNS
        if pd.notna(last_row.get(col))
    }
