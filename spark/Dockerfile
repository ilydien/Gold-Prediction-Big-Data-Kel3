FROM bitnamilegacy/spark:3.5

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --prefer-binary --timeout 120 -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . .
