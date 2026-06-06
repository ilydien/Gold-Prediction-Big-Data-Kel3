FROM python:3.12-slim

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY api/main.py .
COPY shared/ shared/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
