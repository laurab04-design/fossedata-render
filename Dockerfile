# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libgtk-3-0 \
    libu2f-udev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variable for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=0

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -r requirements.txt && playwright install chromium

# Install Playwright browsers (Chromium only) in the default path
RUN playwright install --with-deps chromium

# Debugging step: List installed browsers in the default path
RUN echo "Listing Playwright installation path during build:" && ls -la /opt/render/.cache/ms-playwright

# Copy the rest of your code
COPY . .

# Debugging step: List files in the application directory
RUN echo "Listing application directory files..." && ls -la /app

# Force Playwright browser installation at runtime in case it's missing
CMD ["sh", "-c", "playwright install chromium && ls -la /opt/render/.cache/ms-playwright && python main.py"]
