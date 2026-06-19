import io
import os

import numpy as np
import pytest
from sklearn.dummy import DummyRegressor
<<<<<<< HEAD
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

FEATURE_COUNT = 12

SAMPLE_DATA = np.array([
    [2300, 2295, 2280, 2298, 12.5, 0.002, 0.001, -0.003, 0.001, -0.002, 72.5, 103.2],
    [2310, 2300, 2285, 2302, 10.1, 0.004, -0.002, 0.001, -0.001, 0.003, 73.0, 103.5],
    [2320, 2305, 2290, 2308, 11.3, 0.003, 0.000, -0.001, 0.002, 0.001, 72.8, 103.0],
    [2330, 2310, 2295, 2315, 13.2, 0.005, 0.003, 0.002, 0.003, -0.001, 73.2, 104.0],
    [2340, 2315, 2300, 2322, 14.0, 0.001, -0.001, 0.003, -0.002, 0.004, 73.5, 103.8],
], dtype=float)

SAMPLE_TARGET = np.array([2310, 2320, 2330, 2340, 2350], dtype=float)


def _load_model():
    import joblib
    import boto3

    endpoint = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("GARAGE_ACCESS_KEY", ""),
        aws_secret_access_key=os.getenv("GARAGE_SECRET_KEY", ""),
        region_name="us-east-1",
        use_ssl=False,
        config=boto3.session.Config(signature_version="s3v4"),
    )
    resp = s3.get_object(Bucket="models", Key="latest/model.pkl")
    return joblib.load(io.BytesIO(resp["Body"].read()))


def _make_dummy_pipeline():
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LinearRegression

=======
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from shared.features import FEATURE_COLUMNS, FEATURE_COUNT, TARGET_COLUMN

SAMPLE_DATA = np.random.RandomState(42).randn(100, FEATURE_COUNT)
SAMPLE_TARGET = SAMPLE_DATA[:, 0] * 10 + SAMPLE_DATA[:, 10] * 3 + np.random.randn(100) * 5


def _make_dummy_pipeline():
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    return Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LinearRegression()),
    ]).fit(SAMPLE_DATA, SAMPLE_TARGET)


class TestModelSanity:
<<<<<<< HEAD
    """Sanity checks — basic behavioral validation."""

    def test_prediction_not_negative(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        assert np.all(preds >= 0), "Model predicted negative gold prices"

    def test_prediction_is_finite(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        assert np.all(np.isfinite(preds)), "Model produced NaN or Inf predictions"
=======
    def test_prediction_is_finite(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        assert np.all(np.isfinite(preds)), "Model produced NaN or Inf"
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    def test_single_prediction_returns_float(self):
        model = _make_dummy_pipeline()
        pred = model.predict(SAMPLE_DATA[:1])
<<<<<<< HEAD
        assert isinstance(pred[0], (float, np.floating)), "Single prediction is not a float"


class TestModelAgainstNaive:
    """Model must outperform a naive baseline (predict yesterday's price)."""

=======
        assert isinstance(pred[0], (float, np.floating))


class TestModelAgainstNaive:
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    def test_mae_beats_naive(self):
        model = _make_dummy_pipeline()
        naive = DummyRegressor(strategy="constant", constant=SAMPLE_TARGET.mean())
        naive.fit(SAMPLE_DATA, SAMPLE_TARGET)
<<<<<<< HEAD

        model_mae = mean_absolute_error(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_mae = mean_absolute_error(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))

        assert model_mae <= naive_mae, (
            f"Model MAE ({model_mae:.4f}) worse than naive ({naive_mae:.4f})"
        )
=======
        model_mae = mean_absolute_error(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_mae = mean_absolute_error(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))
        assert model_mae <= naive_mae, f"MAE {model_mae:.2f} > naive {naive_mae:.2f}"
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    def test_r2_beats_naive(self):
        model = _make_dummy_pipeline()
        naive = DummyRegressor(strategy="mean")
        naive.fit(SAMPLE_DATA, SAMPLE_TARGET)
<<<<<<< HEAD

        model_r2 = r2_score(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_r2 = r2_score(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))

        assert model_r2 >= naive_r2, (
            f"Model R² ({model_r2:.4f}) worse than naive ({naive_r2:.4f})"
        )


class TestModelShape:
    """Model input/output shape validation."""

=======
        model_r2 = r2_score(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_r2 = r2_score(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))
        assert model_r2 >= naive_r2, f"R² {model_r2:.2f} < naive {naive_r2:.2f}"


class TestModelShape:
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    def test_accepts_correct_feature_count(self):
        model = _make_dummy_pipeline()
        model.predict(SAMPLE_DATA)
        assert True

    def test_rejects_wrong_feature_count(self):
        model = _make_dummy_pipeline()
        wrong_shape = SAMPLE_DATA[:, :10]
        with pytest.raises(ValueError):
            model.predict(wrong_shape)


<<<<<<< HEAD
class TestModelRange:
    """Prediction magnitude checks."""

    def test_predictions_in_reasonable_range(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        assert np.all(preds >= 1500), "Predictions below reasonable gold price floor"
        assert np.all(preds <= 5000), "Predictions above reasonable gold price ceiling"

    def test_predictions_close_to_input(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        deviation = np.abs(preds - SAMPLE_TARGET).mean()
        assert deviation < 500, (
            f"Predictions deviate too much: avg {deviation:.1f} vs target"
        )


class TestFeatureCount:
    """Ensure shared feature definition matches model expectation."""

    def test_feature_column_count(self):
        from shared.features import FEATURE_COLUMNS

        assert len(FEATURE_COLUMNS) == FEATURE_COUNT, (
            f"Expected {FEATURE_COUNT} features, got {len(FEATURE_COLUMNS)}"
        )

    def test_target_column_exists(self):
        from shared.features import TARGET_COLUMN

        assert TARGET_COLUMN == "gold_price", (
            f"Expected target 'gold_price', got '{TARGET_COLUMN}'"
        )
=======
class TestFeatureConfig:
    def test_feature_count(self):
        assert len(FEATURE_COLUMNS) == FEATURE_COUNT, \
            f"Expected {FEATURE_COUNT} features, got {len(FEATURE_COLUMNS)}"

    def test_target_column(self):
        assert TARGET_COLUMN == "gold_price"

    def test_all_features_numeric(self):
        sample = _make_dummy_pipeline().predict(SAMPLE_DATA[:1])[0]
        assert isinstance(sample, (float, np.floating))
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
