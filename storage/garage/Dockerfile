FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple boto3

COPY storage/garage/init-garage.py .

CMD ["python", "init-garage.py"]
