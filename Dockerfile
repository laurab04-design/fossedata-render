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

from pathlib import Path
import os

# Step 1: Debugging - Check Chromium installation path
playwright_path = Path("/opt/render/.cache/ms-playwright")
print("Checking Playwright installation path...")

if playwright_path.exists():
    print("Playwright installation path exists! Listing contents:")
    print(list(playwright_path.glob("**/*")))
else:
    print("Playwright installation path does NOT exist.")

# Check for Chromium specifically
chromium_path = list(playwright_path.glob("chromium-*"))
if chromium_path:
    print("Chromium detected at:", chromium_path)
    headless_shell = chromium_path[0] / "chrome-linux" / "headless_shell"
    print("Chromium exists:", headless_shell.exists())
else:
    print("Chromium folder NOT detected.")

# Rest of your imports and logic
from playwright.sync_api import sync_playwright

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.fossedata.co.uk/shows/Shows-To-Enter.aspx")
        print(page.title())
        browser.close()

if __name__ == "__main__":
    main()

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium only) in the default path
RUN playwright install --with-deps chromium

# Debugging step: List installed browsers in the default path
RUN echo "Checking Playwright browser installation..." && ls -la /opt/render/.cache/ms-playwright

# Copy the rest of your code
COPY . .

# Debugging step: List files in the application directory
RUN echo "Listing application directory files..." && ls -la /app

# Force Playwright browser installation at runtime in case it's missing
CMD ["sh", "-c", "playwright install chromium && python main.py"]
