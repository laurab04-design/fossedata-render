#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
import asyncio

from fastapi import FastAPI, HTTPException, BackgroundTasks
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

# ─── Now define the FastAPI app ────────────────────────────────────────
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData Render service is running."}

@app.post("/run")
async def run_blocking():
    """
    Synchronous-style run: call full_run on a thread and wait for it to finish.
    """
    results = await asyncio.to_thread(full_run)
    return {"processed": len(results)}

@app.get("/run")
async def run_background(background_tasks: BackgroundTasks):
    """
    Asynchronous kick-off: fire full_run in the background immediately.
    """
    try:
        loop = asyncio.get_running_loop()
        # schedule full_run() but return immediately
        background_tasks.add_task(loop.create_task, full_run())
        return {"status": "started", "message": "Background scrape started."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {e}")
