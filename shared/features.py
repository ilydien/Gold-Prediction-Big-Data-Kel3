import pandas as pd

FEATURE_COLUMNS = [
    "lag_1",
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
]

TARGET_COLUMN = "gold_price"


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("timestamp").reset_index(drop=True)

    result = df.copy()

    result["lag_1"] = result["gold_price"].shift(1)
    result["lag_5"] = result["gold_price"].shift(5)
    result["lag_10"] = result["gold_price"].shift(10)

    result["rolling_mean_5"] = (
        result["gold_price"].rolling(window=5, min_periods=1).mean()
    )
    result["rolling_std_5"] = (
        result["gold_price"].rolling(window=5, min_periods=1).std()
    )

    result["return_gold"] = result["gold_price"].pct_change()
    result["return_oil"] = result["oil_price"].pct_change()
    result["return_dxy"] = result["dxy"].pct_change()
    result["return_eurusd"] = result["eurusd"].pct_change()
    result["return_jpy"] = result["jpy"].pct_change()

    result["lag_1_oil"] = result["oil_price"].shift(1)
    result["lag_1_dxy"] = result["dxy"].shift(1)

    return result


def get_feature_vector(row: dict, history: list[dict]) -> dict:
    df = pd.DataFrame(history + [row])
    df = df.sort_values("timestamp").reset_index(drop=True)
    features = compute_features(df)
    last_row = features.iloc[-1]
    return {col: last_row[col] for col in FEATURE_COLUMNS if pd.notna(last_row.get(col))}
