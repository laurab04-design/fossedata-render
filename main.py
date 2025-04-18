from fastapi import FastAPI
import uvicorn
from pathlib import Path
import subprocess
import os

# === Set environment for Playwright ===
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# === Debug: Check if Chromium is installed ===
chromium_exec = Path("/opt/render/.cache/ms-playwright/chromium")
if chromium_exec.exists():
    print("Chromium is installed.")
else:
    print("Chromium not found, installing...")

# === Force-install Chromium if not already installed ===
try:
    subprocess.run(["playwright", "install", "chromium"], check=True)
    print("Chromium installation attempted.")
except Exception as e:
    print(f"Chromium install error: {e}")

# === Web trigger setup ===
app = FastAPI()

@app.get("/run")
def trigger_run():
    from fossedata_core import full_run
    result = full_run()
    return {"status": "completed", "shows": len(result)}

# === Optional CLI trigger ===
if __name__ == "__main__":
    from fossedata_core import full_run
    full_run()
