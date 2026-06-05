FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3

COPY storage/garage/init-garage.py .

CMD ["python", "init-garage.py"]
