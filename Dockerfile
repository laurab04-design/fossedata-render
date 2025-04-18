FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates fonts-liberation libasound2 \
    libatk-bridge2.0-0 libcups2 libdbus-1-3 libgbm1 libnspr4 \
    libnss3 libxcomposite1 libxdamage1 libxrandr2 xdg-utils \
    libgtk-3-0 libu2f-udev && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PLAYWRIGHT_BROWSERS_PATH=0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps chromium

COPY . .

CMD ["sh", "-c", "npx playwright install chromium && uvicorn main:app --host 0.0.0.0 --port 10000"]
