FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple \
    pandas numpy scikit-learn joblib boto3 psycopg2-binary prefect mlflow yfinance

COPY shared/ /app/shared/
COPY storage/features/ /app/storage/features/
COPY storage/trainer/ /app/storage/trainer/
COPY storage/prefect/ /app/storage/prefect/

ENV PYTHONPATH=/app

CMD ["python", "-m", "storage.prefect.flows"]
