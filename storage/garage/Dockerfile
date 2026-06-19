FROM dxflrs/garage:v2.3.0 AS garage-bin

FROM python:3.12-slim

WORKDIR /app

COPY --from=garage-bin /garage /usr/local/bin/garage

RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple boto3

COPY init-garage.py .

CMD ["python", "init-garage.py"]
