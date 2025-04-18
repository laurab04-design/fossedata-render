import os
import re
import csv
import json
import time
import datetime
import base64
import requests
import pdfplumber
import asyncio
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# (…all your existing imports and config…)

# —————————————
# 1. Fetch the official Kennel Club breed list
# —————————————
async def fetch_kc_breeds():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(
            "https://www.thekennelclub.org.uk/search/breeds-a-to-z/",
            wait_until="networkidle"
        )
        # Wait for the UL of results to appear
        await page.wait_for_selector("#searchResultsList li a", timeout=15000)
        elems = await page.query_selector_all("#searchResultsList li a")
        breeds = [await e.inner_text() for e in elems]
        await browser.close()
        # Normalize to lowercase
        return {b.lower().strip() for b in breeds}

# —————————————
# 2. Helper to extract the “base” breed name from a show URL
# —————————————
def extract_show_breed(raw_url):
    # e.g. "The-British-Chihuahua-Club-May-2025.aspx"
    fname = raw_url.rsplit("/", 1)[-1].replace(".aspx", "")
    parts = fname.split("-")
    # find first numeric/year segment
    idx = next((i for i,p in enumerate(parts) if re.fullmatch(r"[A-Za-z]+", p) is None), len(parts))
    base = " ".join(parts[:idx]).lower()
    return base

# —————————————
# (Your existing async save/load state, extract_text_from_pdf, get_postcode, etc.)
# —————————————

async def full_run():
    global travel_cache

    # —————————————
    # Fetch KC breeds ONCE
    # —————————————
    kc_breeds = await fetch_kc_breeds()
    print(f"[INFO] Loaded {len(kc_breeds)} KC breeds")

    # Get our show URLs…
    urls = fetch_aspx_links()
    if not urls:
        print("[WARN] No show URLs found.")
        return []

    # Load cache, etc…
    travel_cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)

    shows = []
    for url in urls:
        base_breed = extract_show_breed(url)
        # Skip single-breed shows that are in KC list but not golden
        if base_breed in kc_breeds and "golden" not in base_breed:
            print(f"[SKIP] {url} — single‑breed ({base_breed}), not golden")
            continue

        # …then your existing logic to download PDFs, parse, etc…
        pdf = await download_schedule_playwright(url)
        if not pdf:
            continue
        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} — no 'golden'")
            continue
        # (rest of your calculation + appending to shows)

    # (save JSON/CSV, find clashes and combos, return)
    find_clashes_and_combos(shows)
    # …write out results.json and results.csv…
    return shows
