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

@app.get("/run")
async def trigger_run():
    try:
        # Auto‑cancel the full_run() after 60 seconds
        shows = await asyncio.wait_for(full_run(), timeout=60.0)
        return {"status": "completed", "shows": len(shows)}
    except asyncio.TimeoutError:
        # Return a 504 if it runs too long
        raise HTTPException(status_code=504, detail="Run timed out after 60 seconds")

# Add this to make sure it runs if launched directly (like locally, not on Render)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
