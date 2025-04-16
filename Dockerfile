FROM mcr.microsoft.com/playwright/python:v1.41.2-jammy

WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install the browser binaries
RUN playwright install --with-deps chromium

CMD ["python", "main.py"]
