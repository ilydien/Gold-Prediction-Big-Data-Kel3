FROM python:3.12-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY producer/producer.py .

CMD ["python", "producer.py"]
