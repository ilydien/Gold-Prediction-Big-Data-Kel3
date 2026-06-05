FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas numpy scikit-learn joblib boto3 psycopg2-binary prefect

COPY shared/ /app/shared/
COPY storage/features/ /app/storage/features/
COPY storage/trainer/ /app/storage/trainer/

ENV PYTHONPATH=/app

CMD ["python", "-m", "storage.trainer.train"]
