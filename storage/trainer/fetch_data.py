import os
import time

import pandas as pd
import yfinance as yf

TICKERS = {
    "gold_price": "GC=F",
    "oil_price": "CL=F",
    "dxy": "DX-Y.NYB",
    "eurusd": "EURUSD=X",
    "jpy": "JPY=X",
}

YF_PERIOD = os.getenv("YF_PERIOD", "730d")
YF_INTERVAL = os.getenv("YF_INTERVAL", "1h")
MAX_RETRIES = int(os.getenv("YF_RETRIES", "3"))


def fetch_yfinance(period: str = None, interval: str = None) -> pd.DataFrame:
    period = period or YF_PERIOD
    interval = interval or YF_INTERVAL
    ticker_str = " ".join(TICKERS.values())

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Downloading {ticker_str} from yfinance "
                  f"(period={period}, interval={interval})...")
            raw = yf.download(
                ticker_str,
                period=period,
                interval=interval,
                progress=False,
            )
            if raw.empty:
                raise ValueError("yfinance returned empty DataFrame")

            data = {}
            for name, ticker in TICKERS.items():
                if len(TICKERS) == 1:
                    data[name] = raw["Close"].rename(name)
                else:
                    data[name] = raw["Close"].get(ticker, raw.get(("Close", ticker)))

            combined = pd.DataFrame(data)
            combined = combined.reset_index()
            combined = combined.rename(columns={
                "Date": "timestamp",
                "Datetime": "timestamp",
                "index": "timestamp",
            })
            combined["timestamp"] = combined["timestamp"].astype(str)
            combined = combined[[
                "timestamp", "gold_price", "oil_price",
                "dxy", "eurusd", "jpy",
            ]].dropna()
            print(f"Fetched {len(combined)} hourly rows from yfinance")
            return combined

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"  yfinance attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(5 * attempt)


if __name__ == "__main__":
    df = fetch_yfinance()
    print(f"Shape: {df.shape}")
    print(df.head())
    print(df.tail())
