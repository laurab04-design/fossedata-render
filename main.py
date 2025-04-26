#!/usr/bin/env python3
import os
import subprocess
import asyncio
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fossedata_core import full_run   # <-- make sure this exists!

# ensure Playwright uses vendored browsers
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# install Chromium if missing (you can also bake this into your Dockerfile)
CHROMIUM = Path("/opt/render/.cache/ms-playwright/chromium")
if not CHROMIUM.exists():
    subprocess.run(["playwright", "install", "chromium"], check=False)

# --- instantiate app BEFORE any @app.<method> ---
app = FastAPI()


@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData is up"}


@app.post("/run")
async def run_sync():
    """
    Blocks until your full_run() finishes. Returns how many shows processed.
    """
    try:
        results = await full_run()
        return {"processed": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run_bg")
async def run_bg(background_tasks: BackgroundTasks):
    """
    Kicks off full_run() in the background (non-blocking).
    """
    background_tasks.add_task(asyncio.create_task, full_run())
    return {"status": "started", "message": "Background scrape kicked off"}
