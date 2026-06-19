CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    predicted_price DOUBLE PRECISION NOT NULL,
    actual_price DOUBLE PRECISION,
    error DOUBLE PRECISION,
    horizon INTEGER DEFAULT 0,
    model_version VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    model_name VARCHAR(100),
    mae DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    horizon INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_horizon ON predictions(horizon);
CREATE INDEX IF NOT EXISTS idx_model_metrics_timestamp ON model_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_model_metrics_horizon ON model_metrics(horizon);
