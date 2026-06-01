# Gold Price Prediction Pipeline

Real-time gold price prediction system using Kafka, Spark, PostgreSQL, and scikit-learn.

## Architecture

```
yfinance → Producer → Kafka → Spark → PostgreSQL → API (FastAPI)
                              ↘ Trainer (scikit-learn) → Garage (S3)
```

| Service | Role |
|---------|------|
| **kafka** | Message broker for gold price stream |
| **postgres** | Stores raw data, features, predictions, and metrics |
| **grafana** | Dashboard visualization from PostgreSQL |
| **garage** | S3-compatible storage for trained models |
| **prefect** | Workflow orchestration for training pipeline |
| **spark-master** | Spark cluster master |
| **spark-worker** | Spark cluster worker |
| **producer** | Fetches gold/oil/DXY prices from yfinance → Kafka |
| **trainer** | Trains ML model, saves to Garage, logs metrics |
| **api** | FastAPI serving predictions from PostgreSQL |

## Quick Start

```bash
docker compose up --watch
```
```bash
```

## Services & Ports

| Service | Port |
|---------|------|
| API | `8000` |
| Grafana | `3000` |
| Prefect | `4200` |
| Garage S3 | `3900` |
| Kafka | `9092` |
| PostgreSQL | `5432` |
| Spark Master | `7077` (cluster), `8080` (web) |

## Database Tables (PostgreSQL)

| Table | Contents |
|-------|----------|
| `gold_stream` | Raw price data (gold, oil, DXY) |
| `gold_features` | Engineered features (lag, return, volatility) |
| `predictions` | Model predictions vs actual |
| `model_metrics` | MAE, RMSE, R² per training run |

## Development

Rebuild a specific service:

```bash
docker compose build --no-cache producer
```

View logs:

```bash
docker compose logs -f api producer
```

Stop all:

```bash
docker compose down
```
