CREATE TABLE IF NOT EXISTS gold_stream (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    gold_price DOUBLE PRECISION,
    oil_price DOUBLE PRECISION,
    dxy DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_features (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    lag1 DOUBLE PRECISION,
    lag5 DOUBLE PRECISION,
    return DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    dxy DOUBLE PRECISION,
    oil_price DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    actual_price DOUBLE PRECISION,
    predicted_price DOUBLE PRECISION,
    error DOUBLE PRECISION,
    model_version VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    mae DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    r2 DOUBLE PRECISION
);
