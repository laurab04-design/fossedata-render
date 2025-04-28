#!/usr/bin/env python3
import os
import subprocess
import asyncio
import uvicorn
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

# The default port is 8000, but on Render, the port is assigned dynamically
port = os.getenv("PORT", 10000)  # Render expects this port, or it will use 10000 by default

if __name__ == "__main__":
    # Make sure to bind to 0.0.0.0 so it's accessible externally (not just localhost)
    uvicorn.run(app, host="0.0.0.0", port=int(port))

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
    background_tasks.add_task(full_run)  # Directly pass the function here, not wrapped in asyncio.create_task
    return {"status": "started", "message": "Background scrape kicked off"}
