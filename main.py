#!/usr/bin/env python3
import os, subprocess, asyncio
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fossedata_core import full_run

# make sure Playwright uses its vendored browsers
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

# install Chromium if missing
CHROMIUM = Path("/opt/render/.cache/ms-playwright/chromium")
if not CHROMIUM.exists():
    subprocess.run(["playwright", "install", "chromium"], check=False)

# instantiate the app **before** any @app.*
app = FastAPI()


@app.get("/")
async def root():
    return {"status": "ok", "message": "FosseData is up"}


@app.post("/run")      # now POST /run really exists
async def run_sync():
    try:
        results = await full_run()
        return {"processed": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run_bg")
async def run_bg(background_tasks: BackgroundTasks):
    # background scrape
    background_tasks.add_task(asyncio.create_task, full_run())
    return {"status": "started", "message": "Background scrape kicked off"}
