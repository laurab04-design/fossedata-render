from fossedata_core import save_results, upload_to_google_drive, processed_shows
import os
import subprocess
import re
import asyncio
import datetime
import uvicorn
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks
from typing import List
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
    #Kicks off full_run() in the background (non-blocking).
    # This is an asynchronous task, so let's add it to the background
    background_tasks.add_task(run_full_run)
    return {"status": "started", "message": "Background scrape kicked off"}

async def run_full_run():
    # This function will run the full_run() and handle all steps in the background.
    try:
        print("Starting background process...")
        # Await the results from the full_run coroutine
        results = await full_run()

        # Handle other processes
        save_results(results, processed_shows)

        # Upload to Google Drive
        upload_to_google_drive()

        print("Processing complete.")
    except Exception as e:
        print(f"Error during background run: {e}")
