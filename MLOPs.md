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
│                      │    │ - 2 model (LR, GBR)   │
│ - baca processed-data│    │ - GridSearchCV tuning     │
│ - compute 12 fitur   │    │ - Feature importance      │
│ - tulis ke features/ │    │ - MLflow logging          │
└──────────────────────┘    │ - Simpan model ke Garage  │
                            │ - Champion auto-guard     │
                            │ - Simpan metrics ke PG    │
                            └──────────┬────────────────┘
                                       │
┌──────────────────┐    ┌──────────────▼──────────────┐
│ MLflow (port 5000)│    │ Person 4 (Angel)            │
│ - Experiment track│    │ FastAPI → PostgreSQL        │
│ - Artifact store  │    │ Streamlit → Telegram        │
│ - Model registry  │    │ (baca dari champion/)      │
└──────────────────┘    └─────────────────────────────┘
         │
    ┌────▼────┐    ┌─────────────┐
    │ CI/CD   │    │ Scheduler   │
    │ GitHub  │    │ Prefect cron│
    │ Actions │    │ (03:00 WIB) │
    └─────────┘    └─────────────┘
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
**Tujuan:** Melengkapi fitur sesuai LAPORAN.

**Perubahan:**
- `shared/features.py`: 4 fitur baru → total 12 fitur
  - `return_eurusd`, `return_jpy` — pergerakan valas (LAPORAN Bab 2.4)
  - `lag_1_oil`, `lag_1_dxy` — momentum prediktor utama
- `storage/trainer/train.py`: semua model dibungkus `Pipeline([StandardScaler(), model])` agar scaling konsisten
  - Pipeline disimpan utuh → FastAPI `predict()` otomatis scaling

### Fase 3: Hyperparameter Tuning + GBR + Feature Importance ✅
**Tujuan:** Optimasi model, menambah Gradient Boosting, dan menjawab rumusan masalah #4.

**Perubahan:**
- `storage/trainer/train.py`:
  - **GradientBoostingRegressor** sebagai model ke-4 (sesuai LAPORAN Bab 3.1.1)
  - **GridSearchCV** dengan `TimeSeriesSplit(n_splits=3)` dan `scoring=neg_mean_absolute_error`
  - **Feature importance** diekstrak dari GBR → ranking + bar chart
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

### Fase 5: MLflow Experiment Tracking
**Tujuan:** Mencatat semua eksperimen training secara sistematis agar bisa dibandingkan.

**Perubahan:**
- `storage/trainer/Dockerfile`: tambah `mlflow` dependency
- `storage/trainer/train.py`:
  - `_configure_mlflow()` → set tracking URI + experiment
  - `_log_to_mlflow()` → log params, metrics, feature importance, model artifact per run
  - Graceful fallback: jika MLflow server unreachable, training tetap jalan
- `docker-compose-person3.yaml`:
  - Service `mlflow` (port 5000) — tracking server dengan S3 artifact store ke Garage
  - MLflow UI dashboard di `localhost:5000`

### Fase 6: Pipeline Scheduling
**Tujuan:** Training berjalan otomatis tiap hari tanpa perlu trigger manual.

**Perubahan:**
- `storage/prefect/flows.py`:
  - Env `PREFECT_SCHEDULED="true"` → gunakan Prefect Runner dengan `CronSchedule`
  - Default: tiap jam 3 pagi WIB (`0 3 * * *`)
- `docker-compose-person3.yaml`:
  - Service `ml-training-scheduled` (profile `scheduled`) — mode daemon dengan `restart: unless-stopped`

### Fase 7: Model Tests
**Tujuan:** Validasi otomatis model sebelum deploy untuk mencegah model "gila" masuk production.

**Perubahan:**
- `tests/__init__.py`, `tests/test_model.py` (NEW):
  - **Sanity checks**: prediksi tidak negatif, tidak NaN/Inf, return tipe float
  - **Naive baseline**: model harus mengalahkan `DummyRegressor` (MAE + R²)
  - **Shape validation**: model menerima 12 fitur, reject input salah dimensi
  - **Range check**: prediksi dalam rentang harga emas wajar (1500–5000)
  - **Feature count**: assert `FEATURE_COLUMNS` berjumlah 12

### Fase 8: CI/CD (GitHub Actions)
**Tujuan:** Otomatis run tests + build Docker image setiap kali kode ML berubah.

**Perubahan:**
- `.github/workflows/ml-pipeline.yml` (NEW):
  - **Trigger**: push/PR ke `storage/trainer/**`, `storage/prefect/**`, `shared/**`, `tests/**`
  - **Job test**: install deps → `pytest tests/ -v`
  - **Job build** (hanya main/master): build Docker image ml-training

### Fase 9: Rollback Strategy
**Tujuan:** Bisa mengembalikan model ke versi sebelumnya jika model baru lebih buruk.

**Perubahan:**
- `storage/trainer/rollback.py` (NEW):
  - `list_versions()` — list semua versi model di Garage
  - `rollback(version)` — copy model versi tertentu ke `champion/`
  - `get_current_champion()` — info model champion saat ini
  - `get_rollback_history()` — history rollback
- `storage/trainer/train.py`:
  - `_update_champion()` — champion hanya di-update jika MAE model baru LEBIH KECIL
  - `_record_champion_event()` — catat setiap event (promoted/skipped) ke history
- `models/champion/model.pkl` — model yang dipakai FastAPI (bukan `latest/`)

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
| 2 | Gradient Boosting | `sklearn.ensemble.GradientBoostingRegressor` | `n_estimators` [50,100,200], `learning_rate` [0.05,0.1], `max_depth` [3,5] |

**Pipeline:** `StandardScaler` → model (disimpan sebagai satu object via `joblib`)

**CV:** `TimeSeriesSplit(n_splits=3)` — time-series-aware cross validation

**Metrik evaluasi:** MAE, RMSE, R² Score

**Seleksi model terbaik:** MAE terendah

### 3.3 Feature Importance

Dianalisis menggunakan `feature_importances_` dari Gradient Boosting. Hasil di-ranking menurun dan disimpan ke metadata.json:

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
| `models/latest/model.pkl` | Model terbaru (untuk fallback) |
| `models/latest/metadata.json` | Metadata terbaru |
| `models/champion/model.pkl` | Model terbaik (untuk FastAPI) |
| `models/champion/metadata.json` | Metadata champion |
| `models/champion/rollback_history.json` | History champion update + rollback |
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

### 3.8 MLflow — Experiment Tracking (Fase 5)

MLflow server berjalan di port 5000, menyimpan metadata ke SQLite dan artifacts ke Garage (S3-compatible).

```
train.py
  │
  ├─ _configure_mlflow()         ← set_tracking_uri + set_experiment
  ├─ per model:
  │   mlflow.start_run(f"{name}_{timestamp}")
  │   ├─ log_params(best_params)
  │   ├─ log_metrics({mae, rmse, r2})
  │   ├─ log_metrics({importance_lag_1, ...})   ← jika RF/GBR
  │   └─ sklearn.log_model(model, name)
  │
  └─ Fallback: jika MLflow unreachable, training tetap jalan
```

**Environment variables:**
| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | Alamat MLflow tracking server |

### 3.9 Pipeline Scheduling (Fase 6)

Arsitektur: **Prefect Server + Worker**.

```
prefect server (port 4200)     ml-training worker
┌───────────────────┐          ┌──────────────────────┐
│ UI + API           │  assign  │ daemon, nunggu tugas  │
│ deployment registry│─────────▶│ polling server tiap 2s│
│ cron schedule      │          │ eksekusi flow         │
│ 03:00 WIB          │          │ akses Garage Dio      │
└───────────────────┘          └──────────────────────┘
```

**Cara kerja:**
1. Startup: `deploy.py` daftarkan flow + schedule ke server
2. Worker start: `prefect worker start --pool gold-pool`
3. Server trigger worker: via cron 03:00 WIB, atau klik **Run** di UI

**Cara menjalankan:**
```bash
# Deploy daemon (server + worker)
docker compose -f docker-compose-person3.yaml up -d

# Manual trigger via UI
buka http://localhost:4200 → Deployments → gold-training-daily → Run

# Manual trigger via terminal (one-shot, bypass server)
docker compose -f docker-compose-person3.yaml run ml-training python -m storage.prefect.flows
```

### 3.10 Model Tests (Fase 7)

Test dijalankan via `pytest tests/ -v`:

| Test | Kategori | Deskripsi |
|------|----------|-----------|
| `test_prediction_not_negative` | Sanity | Prediksi tidak boleh negatif |
| `test_prediction_is_finite` | Sanity | Tidak ada NaN atau Inf |
| `test_single_prediction_returns_float` | Sanity | Satu prediksi = satu float |
| `test_mae_beats_naive` | Baseline | MAE model < MAE naive (constant) |
| `test_r2_beats_naive` | Baseline | R² model > R² naive (mean) |
| `test_accepts_correct_feature_count` | Shape | Model menerima tepat 12 fitur |
| `test_rejects_wrong_feature_count` | Shape | Model reject input ≠12 kolom |
| `test_predictions_in_reasonable_range` | Range | Prediksi antara 1500–5000 |
| `test_predictions_close_to_input` | Range | Deviasi rata-rata < 500 |
| `test_feature_column_count` | Config | `FEATURE_COLUMNS` = 12 |
| `test_target_column_exists` | Config | `TARGET_COLUMN` = "gold_price" |

Test tidak memerlukan koneksi ke Garage saat CI (menggunakan `_make_dummy_pipeline()` sebagai fallback).

### 3.11 CI/CD — GitHub Actions (Fase 8)

```
.github/workflows/ml-pipeline.yml

on: push/PR ke storage/trainer/, storage/prefect/, shared/, tests/

jobs:
  test:
    - install pandas, numpy, scikit-learn, joblib, boto3, mlflow, pytest
    - pytest tests/ -v

  build (only main/master):
    - docker build -f storage/trainer/Dockerfile -t ml-training .
```

### 3.12 Rollback Strategy (Fase 9)

Dua mekanisme:
1. **Auto-guard**: `train.py` hanya meng-update `champion/` jika MAE baru < MAE champion saat ini
2. **Manual rollback**: `rollback.py` untuk rollback ke versi spesifik

**Model path:**
```
models/champion/model.pkl    ← dipakai FastAPI (diarahkan oleh Person 4)
models/latest/model.pkl      ← model terbaru (bisa belum tentu terbaik)
models/{name}/{ts}/model.pkl ← semua versi (full history)
```

**Penggunaan rollback.py:**
```bash
# List semua versi model
python rollback.py list

# Rollback ke versi tertentu
python rollback.py rollback gradient_boosting/20260619_103000

# Lihat champion saat ini
python rollback.py champion

# Lihat history rollback
python rollback.py history
```

---

## 4. Struktur File Person 3

```
storage/
├── prefect/
│   └── flows.py              # Prefect flow: orchestrator + scheduler
├── features/
│   └── feature_pipeline.py   # Baca processed-data → compute fitur → tulis features
├── trainer/
│   ├── Dockerfile             # Container definition (+mlflow)
│   ├── train.py               # ML training: load, train, evaluate, save + mlflow + champion
│   ├── retraining.py          # Conditional retraining: trigger checks + state
│   └── rollback.py            # Model rollback: list, rollback, champion info
├── mlflow/
│   └── (data volume — mlflow.db + artifacts)

shared/
└── features.py                # Feature engineering (digunakan training & inference)

tests/
├── __init__.py
└── test_model.py              # Model tests: sanity, baseline, shape, range

.github/
└── workflows/
    └── ml-pipeline.yml        # CI/CD: test + build Docker image

docker-compose-person3.yaml    # Compose: prefect + mlflow + ml-training (one-shot & scheduled)
docker-compose.person2+3.yaml  # Compose: hybrid Person 2 + Person 3
.env                           # Konfigurasi Tailscale IP + credentials
```

---

## 5. Deployment

### Independent (Person 3 only)
```bash
cp .env.person3 .env          # Isi Tailscale IP
docker compose -f docker-compose-person3.yaml up -d
# MLflow UI: http://localhost:5000
# Prefect UI: http://localhost:4200
# Training otomatis: tiap jam 3 pagi WIB
```

### Jalankan training manual via UI
1. Buka http://localhost:4200
2. Pilih **Deployments** → **gold-training-daily** → klik **Run**

### Jalankan training manual via terminal (one-shot, log real-time)
```bash
docker compose -f docker-compose-person3.yaml run ml-training python -m storage.prefect.flows
```

### Rollback model
```bash
docker compose -f docker-compose-person3.yaml run ml-training python -m storage.trainer.rollback list
docker compose -f docker-compose-person3.yaml run ml-training python -m storage.trainer.rollback rollback <version>
```

### Jalankan tests (lokal)
```bash
pip install pytest pandas numpy scikit-learn joblib
pytest tests/ -v
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
| MLflow UI | `5000` |
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

# Network resilience (optional, ada default)
GARAGE_RETRIES=3
GARAGE_RETRY_DELAY=5

# MLflow (optional, ada default)
MLFLOW_TRACKING_URI=http://mlflow:5000

# Scheduling (PREFECT_SCHEDULED=true = daemon cron 03:00, false = one-shot)
PREFECT_SCHEDULED=true
```

---

## 7. Progress

| Fase | Deskripsi | Status |
|------|-----------|--------|
| 1 | Arsitektur pipeline (Dockerfile + flows.py) | ✅ |
| 2 | Feature engineering (12 fitur) + StandardScaler | ✅ |
| 3 | Hyperparameter tuning + GBR + feature importance | ✅ |
| 4 | Conditional retraining (count + performance triggers) | ✅ |
| 5 | MLflow experiment tracking | ✅ |
| 6 | Pipeline scheduling (Prefect cron) | ✅ |
| 7 | Model tests (sanity, naive, shape, range) | ✅ |
| 8 | CI/CD (GitHub Actions) | ✅ |
| 9 | Rollback strategy (champion model + rollback.py) | ✅ |

---

## 8. Alur Lengkap Pipeline

```
docker compose up ml-training       (one-shot)
atau
docker compose --profile scheduled up ml-training-scheduled  (cron 03:00 WIB)
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
  │   ├─ _configure_mlflow() → connect ke MLflow server
  │   ├─ Train/test split (80/20, time-series)
  │   ├─ GridSearchCV per model (TimeSeriesSplit)
  │   ├─ Evaluasi: MAE, RMSE, R²
  │   ├─ _log_to_mlflow() → log params, metrics, model artifact
  │   ├─ Pilih best model (MAE terendah)
  │   ├─ Ekstrak feature importance
  │   ├─ Simpan model + metadata ke models/{name}/{ts}/
  │   ├─ _update_champion() → update champion hanya jika MAE lebih baik
  │   ├─ Simpan ke models/latest/
  │   └─ Simpan metrics ke PostgreSQL model_metrics
  │
  ├─ build_training_state()
  └─ save_training_state() → models/training-state/latest.json
```
