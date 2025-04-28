#!/usr/bin/env python3
from fossedata_core import detect_clashes, detect_overnight_pairs, save_results, upload_to_google_drive, travel_cache, processed_shows
import os
import subprocess
import asyncio
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

# This is a function to read the existing links in aspx_links.txt
def read_existing_links():
    try:
        if os.path.exists("aspx_links.txt"):
            with open("aspx_links.txt", "r") as f:
                return set(f.read().splitlines())
    except Exception as e:
        print(f"[ERROR] Failed to read aspx_links.txt: {e}")
    return set()  # Return empty set if file does not exist or error occurs

# This is a function to save the updated links to aspx_links.txt
def save_links(links):
    try:
        with open("aspx_links.txt", "w") as f:
            for link in links:
                f.write(f"{link}\n")
    except Exception as e:
        print(f"[ERROR] Failed to write to aspx_links.txt: {e}")

# Assuming this is your current fetch_show_list method from earlier
async def fetch_show_list(page) -> List[dict]:
    """
    Scrape the Fosse Data site for upcoming shows.
    Returns a list of shows with ID, name, date, venue, and type.
    """
    shows = []
    await page.goto("https://www.fossedata.co.uk/shows.aspx", timeout=60000)
    content = await page.content()

    show_entries = re.findall(
        r"(?P<date>\d{1,2} \w+ 20\d{2}).+?(?P<name>[A-Z][^<]+Show)[^<]*(?P<venue>[A-Z][^<]+)(?P<link>ShowID=\d+)?",
        content,
        flags=re.DOTALL
    )

    existing_links = read_existing_links()  # Read existing links from the file
    new_shows = []  # List to hold new shows

    for date_str, name, venue, showid in show_entries:
        show_id = showid.strip() if showid else f"{name}_{date_str}"
        
        # Skip shows already in aspx_links.txt
        if show_id in existing_links:
            continue
        
        # Add new show to list
        new_shows.append({
            "id": show_id,
            "show_name": name.strip(),
            "date": datetime.datetime.strptime(date_str, "%d %B %Y").date(),
            "venue": venue.strip(),
            "type": "Championship" if "Championship Show" in name else "Open"
        })
        
        # Add show ID to the set of existing links
        existing_links.add(show_id)

    # Save the updated links back to the file
    save_links(existing_links)

    return new_shows

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
    # This is an asynchronous task, so let's add it to the background
    background_tasks.add_task(run_full_run)
    return {"status": "started", "message": "Background scrape kicked off"}

async def run_full_run():
    """
    This function will run the full_run() and handle all steps in the background.
    """
    try:
        print("Starting background process...")
        # Await the results from the full_run coroutine
        results = await full_run()

        # Now that full_run() is finished, detect clashes
        clashes = detect_clashes(results)

        # Handle other processes like overnights and saving results
        overnights = detect_overnight_pairs(results, travel_cache)
        save_results(results, clashes, overnights, travel_cache, processed_shows)

        # Upload to Google Drive
        upload_to_google_drive()

        print("Processing complete.")
    except Exception as e:
        print(f"Error during background run: {e}")
