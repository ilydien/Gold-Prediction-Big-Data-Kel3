# Real-Time Gold Price Prediction and Monitoring System

Big Data Streaming project — monitoring dan prediksi harga emas dunia secara real-time menggunakan Kafka, Spark, Garage, scikit-learn, FastAPI, Grafana, dan Telegram.

## Team

| Person | Nama | Role | Service |
|--------|------|------|---------|
| 1 | Yoeke | Data Ingestion | kafka, yahoo-fetcher |
| 2 | Dio | Stream Processing | spark-master, spark-worker, processing-job |
| 3 | Fatih | ML & MLOps | prefect, ml-training |
| 4 | Angel | Serving & Monitoring | fastapi, postgres, grafana, telegram-bot |

## Architecture

```mermaid
graph TD
    subgraph "Person 1 - Yoeke"
        YF[Yahoo Finance<br/>GC=F, CL=F, DXY, EURUSD, JPY] --> PR[Producer]
        PR -->|1-5s interval| KF[Kafka<br/>topic: gold_stream]
    end

    subgraph "Person 2 - Dio"
        KF -->|consume| SS[Spark Structured Streaming]
        SS -->|parquet| GB1[Garage (local)<br/>raw-data]
        SS -->|parquet| GB2[Garage (local)<br/>processed-data]
        SS -->|HTTP POST| FA[FastAPI]
    end

    subgraph "Person 3 - Fatih"
        GB2 -.->|remote read| PF[Prefect Flow]
        PF -->|shared/features.py| GB3[Garage (remote)<br/>features]
        GB3 --> TR[ML Training<br/>LR + KNN + RF]
        TR -->|MAE/RMSE/R²| GB4[Garage (remote)<br/>models]
        TR -.->|remote write| PG[(PostgreSQL<br/>metrics)]
    end

    subgraph "Person 4 - Angel"
        GB4 -->|load at startup| FA[FastAPI]
        FA -->|predictions| PG
        PG --> G[Grafana Dashboard]
        PG --> TB[Telegram Alert Bot]
    end
```

## Data Flow

1. **Yahoo Finance Producer** (Yoeke) mengambil data 5 ticker setiap 1-5 detik dan kirim ke Kafka.
2. **Spark Structured Streaming** (Dio) consume dari Kafka, simpan raw + processed data ke Garage.
3. **Spark** juga POST feature vector ke FastAPI untuk prediksi real-time.
4. **Prefect** (Fatih) menjalankan feature engineering batch dari processed-data ke features bucket.
5. **ML Training** (Fatih) membaca features dari Garage (via Tailscale ke Dio), train 3 model (LR, KNN, RF), pilih terbaik, simpan ke Garage.
6. **FastAPI** (Angel) load model dari Garage saat startup (via Tailscale ke Dio), simpan di RAM.
7. **FastAPI** menerima features → predict → simpan ke PostgreSQL.
8. **Grafana** visualisasi dari PostgreSQL.
9. **Telegram Bot** kirim alert jika harga berubah > threshold.

## Data Source

| Ticker | Deskripsi |
|--------|-----------|
| GC=F | Gold Futures (target) |
| CL=F | Crude Oil (feature) |
| DX-Y.NYB | US Dollar Index (feature) |
| EURUSD=X | EUR/USD Forex (feature) |
| JPY=X | USD/JPY Forex (feature) |

## Feature Engineering

Semua feature dihitung dengan `shared/features.py` (kode SAMA untuk training dan inference).

- `lag_1`, `lag_5`, `lag_10` — harga emas sebelumnya
- `rolling_mean_5`, `rolling_std_5` — rolling statistics
- `return_gold`, `return_oil`, `return_dxy` — persentase perubahan

## Models

1. Linear Regression
2. K-Nearest Neighbors (KNN)
3. Random Forest Regressor

Evaluasi: MAE, RMSE, R². Model terbaik otomatis disimpan ke Garage.

## Quick Start (Local — all on one machine)

```bash
# 1. Clone repo
git clone <repo-url>
cd Gold-Prediction-Big-Data-Kel3

# 2. Copy .env
cp .env.example .env

# 3. Start all services
docker compose -f docker-compose.local.yaml up -d
```

## Distributed Deployment (via Tailscale)

Setiap anggota menjalankan service di laptop masing-masing.

### Prerequisites

1. Install [Tailscale](https://tailscale.com/download) di semua laptop
2. Semua join ke Tailnet yang sama
3. Catat IP masing-masing: `tailscale ip`

### Setup

```bash
# 1. Copy template .env sesuai peran
cp .env.person3 .env   # Contoh untuk Fatih

# 2. Edit .env — isi Tailscale IP anggota lain

# 3. Jalankan compose file masing-masing
docker compose -f docker-compose-person3.yaml up -d   # Fatih
```

Detail: lihat `TAILSCALE-SETUP.md`

## Person-Specific Compose Files

| File | Untuk | Services |
|------|-------|----------|
| `docker-compose-person1.yaml` | Yoeke | kafka, yahoo-fetcher |
| `docker-compose-person2.yaml` | Dio | spark-master, spark-worker, processing-job, garage, garage-init |
| `docker-compose-person3.yaml` | Fatih | prefect, ml-training |
| `docker-compose-person4.yaml` | Angel | postgres, fastapi, grafana, telegram-bot |
| `docker-compose.local.yaml` | Local | all services on one machine |

## Services & Ports

| Service | Port | Exposed By |
|---------|------|------------|
| Kafka | `9092` | Person 1 |
| Garage S3 API | `3900` | Person 2 |
| Garage Web | `3902` | Person 2 |
| Prefect UI | `4200` | Person 3 |
| FastAPI | `8000` | Person 4 |
| PostgreSQL | `5432` | Person 4 |
| Grafana | `3000` | Person 4 |
| Spark Master | `7077`, `8080` | Person 2 |

## Inter-Service Communication (Tailscale)

| From | To | Address |
|------|----|---------|
| Spark (Dio) | Kafka (Yoeke) | `100.x.x.1:9092` |
| Spark (Dio) | Garage (Dio) | `garage:3900` (local) |
| Spark (Dio) | FastAPI (Angel) | `100.x.x.4:8000` |
| Trainer (Fatih) | PostgreSQL (Angel) | `100.x.x.4:5432` |
| FastAPI (Angel) | Garage (Dio) | `100.x.x.2:3900` |

## Retraining Strategy

Sistem menggunakan **Conditional Retraining** dengan 2 trigger:

1. **Count-based**: Retrain jika data baru di Garage >= 10.000 rows sejak training terakhir
2. **Performance-based**: Retrain jika rolling MAE (100 prediksi terakhir) > 1.5x best MAE

Keduanya diorkestrasi oleh Prefect.

## Database Tables (PostgreSQL)

| Table | Contents |
|-------|----------|
| `predictions` | Predicted prices + model version |
| `model_metrics` | MAE, RMSE, R² per training run |

## Storage (Garage Object Storage)

| Bucket | Contents |
|--------|----------|
| `raw-data` | Raw price data (parquet) |
| `processed-data` | Preprocessed data (parquet) |
| `features` | Engineered features (parquet) |
| `models` | Trained model files (.pkl) + metadata (.json) |

## Development

```bash
# Build specific service
docker compose -f docker-compose.local.yaml build fastapi

# View logs
docker compose -f docker-compose.local.yaml logs -f fastapi

# Stop all
docker compose -f docker-compose.local.yaml down

# Stop individual person
docker compose -f docker-compose-person3.yaml down
```
