FROM bitnamilegacy/spark:3.5

USER root

RUN pip install --no-cache-dir boto3 pandas numpy requests

COPY spark/stream.py /app/stream.py
COPY shared/ /app/shared/

WORKDIR /app

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "stream.py"]
