#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
import asyncio

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fossedata_core import full_run  # <-- your async runner

# — make sure Playwright uses its vendored browsers —
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# check/install Chromium at startup (optional, can also be in Dockerfile)
CHROMIUM = Path("/opt/render/.cache/ms-playwright/chromium")
if not CHROMIUM.exists():
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
        print("Chromium installed.")
    except Exception as e:
        print(f"Chromium install error: {e}")

# — now define your FastAPI app —
app = FastAPI()


@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData service is running."}


@app.post("/run_sync")
async def run_sync():
    """
    Wait for full_run() to finish, then return how many shows were processed.
    Because full_run() is async you can simply await it here.
    """
    try:
        results = await full_run()
        return {"processed": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run_bg")
async def run_bg(background_tasks: BackgroundTasks):
    """
    Kick off full_run() in the background and return immediately.
    We schedule an asyncio task after sending the response.
    """
    # schedule the scrape on the running loop, but non-blocking
    background_tasks.add_task(asyncio.create_task, full_run())
    return {"status": "started", "message": "Background scrape started."}
