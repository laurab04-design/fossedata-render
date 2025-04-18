#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
import asyncio

from fastapi import FastAPI, HTTPException
from fossedata_core import full_run

# Ensure Playwright uses its vendored browsers
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# Debug: Check if Chromium is already installed
chromium_exec = Path("/opt/render/.cache/ms-playwright/chromium")
if chromium_exec.exists():
    print("Chromium is installed.")
else:
    print("Chromium not found, installing...")

# Force-install Chromium if not already present
try:
    subprocess.run(["playwright", "install", "chromium"], check=True)
    print("Chromium installation attempted.")
except Exception as e:
    print(f"Chromium install error: {e}")

# --- FastAPI web trigger setup ---
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData Render service is running."}

@app.get("/run")
async def trigger_run():
    try:
        # Bump timeout to 120s so we don’t keep cutting off
        shows = await asyncio.wait_for(full_run(), timeout=120.0)
        return {"status": "completed", "shows": len(shows)}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Run timed out after 120 seconds")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {e}")
