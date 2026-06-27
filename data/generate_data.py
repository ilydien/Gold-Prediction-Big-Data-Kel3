import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

ROWS = 5000
SEED = 42
INTERVAL_MIN = 1
INTERVAL_MAX = 5

START_GOLD = 2350.0
START_OIL = 78.0
START_DXY = 104.0
START_EURUSD = 1.08
START_JPY = 150.0

START_TIME = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

GOLD_DRIFT = 0.00005
OIL_DRIFT = 0.00002
DXY_DRIFT = -0.00003
EURUSD_DRIFT = 0.00002
JPY_DRIFT = 0.00004

GOLD_VOL = 0.0015
OIL_VOL = 0.0020
DXY_VOL = 0.0005
EURUSD_VOL = 0.0006
JPY_VOL = 0.0008

rng = np.random.default_rng(SEED)

timestamps = []
gold_prices = []
oil_prices = []
dxys = []
eurusds = []
jpy_prices = []

current_time = START_TIME
gold = START_GOLD
oil = START_OIL
dxy = START_DXY
eurusd = START_EURUSD
jpy = START_JPY

for i in range(ROWS):
    interval = random.uniform(INTERVAL_MIN, INTERVAL_MAX)
    current_time += timedelta(seconds=interval)

    common_shock = rng.normal(0, 1)

    gold_ret = GOLD_DRIFT + GOLD_VOL * (0.7 * common_shock + 0.3 * rng.normal(0, 1))
    gold *= np.exp(gold_ret)

    oil_ret = OIL_DRIFT + OIL_VOL * (0.5 * common_shock + 0.5 * rng.normal(0, 1))
    oil *= np.exp(oil_ret)

    dxy_ret = DXY_DRIFT + DXY_VOL * (-0.6 * common_shock + 0.4 * rng.normal(0, 1))
    dxy *= np.exp(dxy_ret)

    eurusd_ret = EURUSD_DRIFT + EURUSD_VOL * (0.5 * -dxy_ret + 0.5 * rng.normal(0, 1))
    eurusd *= np.exp(eurusd_ret)

    jpy_ret = JPY_DRIFT + JPY_VOL * (0.4 * -dxy_ret + 0.6 * rng.normal(0, 1))
    jpy *= np.exp(jpy_ret)

    timestamps.append(current_time)
    gold_prices.append(round(gold, 2))
    oil_prices.append(round(oil, 2))
    dxys.append(round(dxy, 4))
    eurusds.append(round(eurusd, 5))
    jpy_prices.append(round(jpy, 3))

df = pd.DataFrame({
    "timestamp": timestamps,
    "gold_price": gold_prices,
    "oil_price": oil_prices,
    "dxy": dxys,
    "eurusd": eurusds,
    "jpy": jpy_prices,
})

output_path = "data/gold_data.csv"
df.to_csv(output_path, index=False)
print(f"Generated {len(df)} rows -> {output_path}")
print(f"Date range: {df['timestamp'].iloc[0]} to {df['timestamp'].iloc[-1]}")
print(f"Gold price range: {df['gold_price'].min()} - {df['gold_price'].max()}")
print(f"Oil price range:  {df['oil_price'].min()} - {df['oil_price'].max()}")
print(f"DXY range:        {df['dxy'].min()} - {df['dxy'].max()}")
print(f"EUR/USD range:    {df['eurusd'].min()} - {df['eurusd'].max()}")
print(f"USD/JPY range:    {df['jpy'].min()} - {df['jpy'].max()}")
