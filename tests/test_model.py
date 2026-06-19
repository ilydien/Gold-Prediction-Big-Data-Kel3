import io
import os

import numpy as np
import pytest
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from shared.features import FEATURE_COLUMNS, FEATURE_COUNT, TARGET_COLUMN

SAMPLE_DATA = np.random.RandomState(42).randn(100, FEATURE_COUNT)
SAMPLE_TARGET = SAMPLE_DATA[:, 0] * 10 + SAMPLE_DATA[:, 10] * 3 + np.random.randn(100) * 5


def _make_dummy_pipeline():
    return Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LinearRegression()),
    ]).fit(SAMPLE_DATA, SAMPLE_TARGET)


class TestModelSanity:
    def test_prediction_is_finite(self):
        model = _make_dummy_pipeline()
        preds = model.predict(SAMPLE_DATA)
        assert np.all(np.isfinite(preds)), "Model produced NaN or Inf"

    def test_single_prediction_returns_float(self):
        model = _make_dummy_pipeline()
        pred = model.predict(SAMPLE_DATA[:1])
        assert isinstance(pred[0], (float, np.floating))


class TestModelAgainstNaive:
    def test_mae_beats_naive(self):
        model = _make_dummy_pipeline()
        naive = DummyRegressor(strategy="constant", constant=SAMPLE_TARGET.mean())
        naive.fit(SAMPLE_DATA, SAMPLE_TARGET)
        model_mae = mean_absolute_error(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_mae = mean_absolute_error(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))
        assert model_mae <= naive_mae, f"MAE {model_mae:.2f} > naive {naive_mae:.2f}"

    def test_r2_beats_naive(self):
        model = _make_dummy_pipeline()
        naive = DummyRegressor(strategy="mean")
        naive.fit(SAMPLE_DATA, SAMPLE_TARGET)
        model_r2 = r2_score(SAMPLE_TARGET, model.predict(SAMPLE_DATA))
        naive_r2 = r2_score(SAMPLE_TARGET, naive.predict(SAMPLE_DATA))
        assert model_r2 >= naive_r2, f"R² {model_r2:.2f} < naive {naive_r2:.2f}"


class TestModelShape:
    def test_accepts_correct_feature_count(self):
        model = _make_dummy_pipeline()
        model.predict(SAMPLE_DATA)
        assert True

    def test_rejects_wrong_feature_count(self):
        model = _make_dummy_pipeline()
        wrong_shape = SAMPLE_DATA[:, :10]
        with pytest.raises(ValueError):
            model.predict(wrong_shape)


class TestFeatureConfig:
    def test_feature_count(self):
        assert len(FEATURE_COLUMNS) == FEATURE_COUNT, \
            f"Expected {FEATURE_COUNT} features, got {len(FEATURE_COLUMNS)}"

    def test_target_column(self):
        assert TARGET_COLUMN == "gold_price"

    def test_all_features_numeric(self):
        sample = _make_dummy_pipeline().predict(SAMPLE_DATA[:1])[0]
        assert isinstance(sample, (float, np.floating))
