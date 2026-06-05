FROM python:3.12-slim

WORKDIR /app

COPY telegram/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY telegram/bot.py .

CMD ["python", "bot.py"]
