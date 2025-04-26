#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
import asyncio

from fastapi import FastAPI, HTTPException, BackgroundTasks

# — make sure Playwright uses its vendored browsers —
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# check/install Chromium
CHROMIUM = Path("/opt/render/.cache/ms-playwright/chromium")
if not CHROMIUM.exists():
    try:
        subprocess.run(["playwright", "install", "chromium"], check=True)
        print("Chromium installed.")
    except Exception as e:
        print(f"Chromium install error: {e}")

# — now define your FastAPI app —
app = FastAPI()

def _run_full_scrape():
    """
    Sync wrapper to call your async full_run() in its own fresh event loop.
    That way asyncio.run() never runs inside FastAPI’s loop.
    """
    from fossedata_core import full_run
    return asyncio.run(full_run())

@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData service is running."}

@app.get("/run")
async def trigger_run(background_tasks: BackgroundTasks):
    """
    Kicks off the scrape in the background immediately.
    Returns 200 while the work runs in a separate thread.
    """
    background_tasks.add_task(_run_full_scrape)
    return {"status": "started", "message": "Background scrape started."}

@app.post("/run")
async def run_endpoint():
    """
    Blocks until full_run() finishes, then returns how many shows processed.
    We offload to a thread so that asyncio.run() happens outside the main loop.
    """
    try:
        results = await asyncio.to_thread(_run_full_scrape)
        return {"processed": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
