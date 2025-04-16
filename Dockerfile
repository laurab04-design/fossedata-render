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

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create the directory and set permissions
RUN mkdir -p /ms-playwright && chmod -R 777 /ms-playwright

# Install Playwright browsers (Chromium only)
RUN PLAYWRIGHT_BROWSERS_PATH=/ms-playwright playwright install --with-deps chromium

# Debugging step: List installed browsers
RUN ls -la /ms-playwright
RUN ls -la /root/.cache/ms-playwright

# Copy the rest of your code
COPY . .

# Debugging step: List files in the application directory
RUN ls -la /app

# Set the default command to run your script
CMD ["python", "main.py"]
