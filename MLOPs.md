# MLOps — Rencana & Progress Pengembangan ML

**Person 3: Fatih Dzaki Nabhani (L0224042)**  
**Role: ML & MLOps**  
**Services: Prefect, ml-training**

---

## 1. Arsitektur (Person 3 Perspective)

```
┌─────────────────────────────────────────────────────────┐
│ Person 2 (Dio) — Garage                                 │
│  processed-data/  features/  models/                    │
└──────────┬────────────────────────────┬─────────────────┘
           │ read                       │ write
           ▼                            ▼
┌──────────────────────┐    ┌──────────────────────────┐
│ Feature Pipeline     │    │ ML Training               │
│ feature_pipeline.py  │───▶│ train.py                   │
│                      │    │ - 4 model (LR,KNN,RF,GBR) │
│ - baca processed-data│    │ - GridSearchCV tuning     │
│ - compute 12 fitur   │    │ - Feature importance      │
│ - tulis ke features/ │    │ - Simpan model ke Garage  │
└──────────────────────┘    │ - Simpan metrics ke PG    │
                            └──────────┬────────────────┘
                                       │
                            ┌──────────▼────────────────┐
                            │ Person 4 (Angel)          │
                            │ FastAPI → PostgreSQL       │
                            │ Grafana → Telegram         │
                            └───────────────────────────┘
```

---

## 2. Rencana Pengembangan ML — Fase

### Fase 1: Arsitektur Pipeline ✅
**Tujuan:** Memperbaiki gap di mana ml-training container hanya menjalankan `train.py` tanpa feature engineering.

**Perubahan:**
- `storage/trainer/Dockerfile`: CMD dari `train.py` → `flows.py`
- `storage/prefect/flows.py`: startup banner, error handling per-step, state tracking
- Pipeline sekarang: feature engineering → training, bukan training saja

### Fase 2: Feature Engineering + Scaling ✅
**Tujuan:** Melengkapi fitur sesuai LAPORAN dan memperbaiki KNN yang bias.

**Perubahan:**
- `shared/features.py`: 4 fitur baru → total 12 fitur
  - `return_eurusd`, `return_jpy` — pergerakan valas (LAPORAN Bab 2.4)
  - `lag_1_oil`, `lag_1_dxy` — momentum prediktor utama
- `storage/trainer/train.py`: Semua model dibungkus `Pipeline([StandardScaler(), model])`
  - KNN tidak lagi bias ke fitur high-magnitude
  - Pipeline disimpan utuh → FastAPI `predict()` otomatis scaling

### Fase 3: Hyperparameter Tuning + GBR + Feature Importance ✅
**Tujuan:** Optimasi model, menambah Gradient Boosting, dan menjawab rumusan masalah #4.

**Perubahan:**
- `storage/trainer/train.py`:
  - **GradientBoostingRegressor** sebagai model ke-4 (sesuai LAPORAN Bab 3.1.1)
  - **GridSearchCV** dengan `TimeSeriesSplit(n_splits=3)` dan `scoring=neg_mean_absolute_error`
  - **Feature importance** diekstrak dari RF dan GBR → ranking + bar chart
  - **Metadata** diperkaya: `best_params`, `n_samples`, `n_features`, `feature_importance`

### Fase 4: Conditional Retraining ✅
**Tujuan:** Implementasi trigger-based retraining sesuai README.

**Perubahan:**
- `storage/trainer/retraining.py` (NEW): modul evaluasi trigger retraining
  - **Count trigger**: cek jumlah objek baru di `processed-data` bucket (default >= 2000)
  - **Performance trigger**: cek rolling MAE 100 prediksi terakhir vs 1.5x best MAE
  - State disimpan di Garage `models/training-state/latest.json`
- `storage/prefect/flows.py`: integrasi `should_retrain()` → skip atau lanjut
- `storage/trainer/train.py`: `run_training()` return metadata

---

## 3. Detail Teknis

### 3.1 Feature Engineering (12 fitur)

| Fitur | Rumus | Keterangan |
|-------|-------|------------|
| `lag_1` | `gold_price.shift(1)` | Harga emas 1 periode lalu |
| `lag_5` | `gold_price.shift(5)` | Harga emas 5 periode lalu |
| `lag_10` | `gold_price.shift(10)` | Harga emas 10 periode lalu |
| `rolling_mean_5` | `gold_price.rolling(5).mean()` | Rata-rata bergerak 5 periode |
| `rolling_std_5` | `gold_price.rolling(5).std()` | Volatilitas 5 periode |
| `return_gold` | `gold_price.pct_change()` | % perubahan emas |
| `return_oil` | `oil_price.pct_change()` | % perubahan minyak |
| `return_dxy` | `dxy.pct_change()` | % perubahan DXY |
| `return_eurusd` | `eurusd.pct_change()` | % perubahan EUR/USD |
| `return_jpy` | `jpy.pct_change()` | % perubahan USD/JPY |
| `lag_1_oil` | `oil_price.shift(1)` | Harga minyak 1 periode lalu |
| `lag_1_dxy` | `dxy.shift(1)` | DXY 1 periode lalu |

**Target:** `gold_price` (GC=F)

### 3.2 Model

| # | Model | Library | Hyperparameter Tuning |
|---|-------|---------|----------------------|
| 1 | Linear Regression | `sklearn.linear_model.LinearRegression` | `fit_intercept` |
| 2 | K-Nearest Neighbors | `sklearn.neighbors.KNeighborsRegressor` | `n_neighbors` [3,5,7,10], `weights` [uniform,distance] |
| 3 | Random Forest | `sklearn.ensemble.RandomForestRegressor` | `n_estimators` [50,100,200], `max_depth` [None,10,20] |
| 4 | Gradient Boosting | `sklearn.ensemble.GradientBoostingRegressor` | `n_estimators` [50,100,200], `learning_rate` [0.05,0.1], `max_depth` [3,5] |

**Pipeline:** `StandardScaler` → model (disimpan sebagai satu object via `joblib`)

**CV:** `TimeSeriesSplit(n_splits=3)` — time-series-aware cross validation

**Metrik evaluasi:** MAE, RMSE, R² Score

**Seleksi model terbaik:** MAE terendah

### 3.3 Feature Importance

Dianalisis menggunakan `feature_importances_` dari Random Forest dan Gradient Boosting. Hasil di-ranking menurun dan disimpan ke metadata.json:

```json
{
  "feature_importance": {
    "lag_1": 0.5234,
    "rolling_mean_5": 0.1823,
    "rolling_std_5": 0.0912,
    ...
  }
}
```

### 3.4 Retraining Triggers

| Trigger | Config Env Var | Default | Cara Kerja |
|---------|---------------|---------|------------|
| Count-based | `RETRAIN_COUNT_THRESHOLD` | 2000 | Bandingkan jumlah objek di `processed-data` bucket dengan state terakhir |
| Performance-based | `RETRAIN_PERF_WINDOW` | 100 | Hitung AVG error dari N prediksi terakhir di PostgreSQL |
| Performance factor | `RETRAIN_PERF_FACTOR` | 1.5 | Trigger jika rolling MAE > factor × best MAE |

### 3.5 Training State

Disimpan di Garage: `models/training-state/latest.json`

```json
{
  "last_training_ts": "2026-06-19T10:00:00+00:00",
  "rows_used": 5000,
  "object_count": 1200,
  "best_mae": 2.48,
  "best_model": "gradient_boosting"
}
```

### 3.6 Model Output

| Path di Garage | Isi |
|----------------|-----|
| `models/{model_name}/{timestamp}/model.pkl` | Model versi (full Pipeline) |
| `models/{model_name}/{timestamp}/metadata.json` | Metadata versi |
| `models/latest/model.pkl` | Model terbaru (untuk FastAPI) |
| `models/latest/metadata.json` | Metadata terbaru |
| `models/training-state/latest.json` | State retraining |

### 3.7 PostgreSQL

Tabel `model_metrics` (ditulis oleh Person 3):

| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| `timestamp` | TIMESTAMP | Waktu training |
| `model_name` | VARCHAR | Nama model terbaik |
| `mae` | DOUBLE | Mean Absolute Error |
| `rmse` | DOUBLE | Root Mean Squared Error |
| `r2` | DOUBLE | R² Score |

---

## 4. Struktur File Person 3

```
storage/
├── prefect/
│   └── flows.py              # Prefect flow: orchestrator pipeline
├── features/
│   └── feature_pipeline.py   # Baca processed-data → compute fitur → tulis features
└── trainer/
    ├── Dockerfile             # Container definition
    ├── train.py               # ML training: load, train, evaluate, save
    └── retraining.py          # Conditional retraining: trigger checks + state

shared/
└── features.py                # Feature engineering (digunakan training & inference)

docker-compose-person3.yaml    # Compose: prefect server + ml-training container
docker-compose.person2+3.yaml  # Compose: hybrid Person 2 + Person 3
.env                           # Konfigurasi Tailscale IP + credentials
```

---

## 5. Deployment

### Independent (Person 3 only)
```bash
cp .env.person3 .env          # Isi Tailscale IP
docker compose -f docker-compose-person3.yaml up -d
```

### Hybrid (Person 2 + Person 3 di 1 laptop)
```bash
# Init Garage dulu
docker compose -f docker-compose.person2+3.yaml up garage-init

# Jalanin semua
docker compose -f docker-compose.person2+3.yaml up -d
```

### Ports
| Service | Port |
|---------|------|
| Prefect UI | `4200` |

---

## 6. Environment Variables

```ini
# Garage (Person 2 via Tailscale)
GARAGE_ENDPOINT=http://100.69.198.116:3900
GARAGE_ACCESS_KEY=...
GARAGE_SECRET_KEY=...

# PostgreSQL (Person 4 via Tailscale)
POSTGRES_HOST=100.99.143.55
POSTGRES_PORT=5432
POSTGRES_DB=gold_prediction
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Retraining (optional, ada default)
RETRAIN_COUNT_THRESHOLD=2000
RETRAIN_PERF_WINDOW=100
RETRAIN_PERF_FACTOR=1.5
```

---

## 7. Progress

| Fase | Deskripsi | Status |
|------|-----------|--------|
| 1 | Arsitektur pipeline (Dockerfile + flows.py) | ✅ |
| 2 | Feature engineering (12 fitur) + StandardScaler | ✅ |
| 3 | Hyperparameter tuning + GBR + feature importance | ✅ |
| 4 | Conditional retraining (count + performance triggers) | ✅ |

---

## 8. Alur Lengkap Pipeline

```
docker compose up ml-training
  │
  ▼
flows.py: gold_pipeline()
  │
  ├─ should_retrain()
  │   ├─ Cek count trigger (processed-data bucket)
  │   ├─ Cek performance trigger (PostgreSQL predictions)
  │   └─ Return: (retrain?, reason, object_count)
  │
  ├─ [SKIP] jika tidak perlu retrain
  │
  ├─ feature_pipeline.py
  │   ├─ List semua file di processed-data/
  │   ├─ Baca & concat semua parquet
  │   ├─ compute_features() → 12 fitur
  │   └─ Tulis ke features/latest/features.parquet
  │
  ├─ train.py: run_training()
  │   ├─ Load features dari Garage
  │   ├─ Train/test split (80/20, time-series)
  │   ├─ GridSearchCV per model (TimeSeriesSplit)
  │   ├─ Evaluasi: MAE, RMSE, R²
  │   ├─ Pilih best model (MAE terendah)
  │   ├─ Ekstrak feature importance
  │   ├─ Simpan model + metadata ke models/
  │   └─ Simpan metrics ke PostgreSQL model_metrics
  │
  ├─ build_training_state()
  └─ save_training_state() → models/training-state/latest.json
```
