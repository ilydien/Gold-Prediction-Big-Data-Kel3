FROM python:3.12-slim

WORKDIR /app

COPY telegram/requirements.txt .
RUN pip install --no-cache-dir --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY telegram/bot.py .

CMD ["python", "bot.py"]
