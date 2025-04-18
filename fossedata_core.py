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

# Decode and write the Google service account credentials from env
creds_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_BASE64")
if creds_b64:
    with open("credentials.json", "wb") as f:
        f.write(base64.b64decode(creds_b64))
else:
    print("GOOGLE_SERVICE_ACCOUNT_BASE64 is not set.")

# Configuration
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
CACHE_FILE = "travel_cache.json"
DOG_DOB = datetime.datetime.strptime(os.environ.get("DOG_DOB", "2024-05-15"), "%Y-%m-%d")
DOG_NAME = os.environ.get("DOG_NAME", "Delia")
MPG = float(os.environ.get("MPG", 40))
OVERNIGHT_THRESHOLD_HOURS = float(os.environ.get("OVERNIGHT_THRESHOLD_HOURS", 3))
OVERNIGHT_COST = float(os.environ.get("OVERNIGHT_COST", 100))
ALWAYS_INCLUDE_CLASS = os.environ.get("ALWAYS_INCLUDE_CLASS", "").split(",")
CLASS_EXCLUSIONS = [x.strip() for x in os.environ.get("DOG_CLASS_EXCLUSIONS", "").split(",")]

def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        print(f"[ERROR] PDF extract failed for {file_path}: {e}")
        return ""

def get_postcode(text):
    match = re.search(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?) ?\d[A-Z]{2}\b", text)
    return match.group(0) if match else None

def get_drive(from_pc, to_pc, cache):
    key = f"{from_pc}_TO_{to_pc}"
    if key in cache:
        return cache[key]
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/distancematrix/json", params={
            "origins": from_pc,
            "destinations": to_pc,
            "mode": "driving",
            "key": GOOGLE_MAPS_API_KEY
        })
        e = r.json()["rows"][0]["elements"][0]
        result = {"duration": e["duration"]["value"], "distance": e["distance"]["value"] / 1000}
        cache[key] = result
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        return result
    except Exception as e:
        print(f"[ERROR] Travel lookup failed: {e}")
        return None

def get_diesel_price():
    try:
        soup = BeautifulSoup(requests.get("https://www.globalpetrolprices.com/diesel_prices/").text, "html.parser")
        uk_row = soup.find("td", string=re.compile("United Kingdom")).find_parent("tr")
        return float(uk_row.find_all("td")[2].text.strip().replace("£", ""))
    except:
        return 1.60

def estimate_cost(distance_km, duration_s):
    miles = distance_km * 0.621371
    price = get_diesel_price()
    gallons = miles / MPG
    fuel = gallons * 4.54609 * price
    return fuel + OVERNIGHT_COST if duration_s > OVERNIGHT_THRESHOLD_HOURS * 3600 else fuel

def extract_judges(text):
    sections = text.lower().split("retriever (golden)")
    result = {}
    for sec in sections[1:]:
        sub = sec[:1000]
        dogs = re.search(r"dogs.*judge.*?:\s*([A-Z][a-z].+)", sub, re.I)
        bitches = re.search(r"bitches.*judge.*?:\s*([A-Z][a-z].+)", sub, re.I)
        all_in_one = re.search(r"judge.*?:\s*([A-Z][a-z].+)", sub, re.I)
        if dogs:
            result["dogs"] = dogs.group(1).strip()
        if bitches:
            result["bitches"] = bitches.group(1).strip()
        if not result and all_in_one:
            result["all"] = all_in_one.group(1).strip()
        break
    return result or None

def get_show_date(text):
    match = re.search(r"Date Of Show:\s*([A-Za-z]+,\s*\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if match:
        try:
            return datetime.datetime.strptime(match.group(1), "%A, %d %B %Y").date()
        except:
            return None
    return None

def jw_points(text):
    txt = text.lower()
    if "open show" in txt:
        return 1
    if "championship show" in txt:
        return 9
    return 0

def find_clashes_and_combos(results):
    date_map = {}
    for show in results:
        date = show["date"]
        if date:
            date_map.setdefault(date, []).append(show)

    for same_day in date_map.values():
        if len(same_day) > 1:
            for s in same_day:
                s["clash"] = True

    for i, a in enumerate(results):
        if not a["postcode"] or a["duration_hr"] <= 3:
            continue
        for j, b in enumerate(results):
            if i == j or not b["postcode"] or b["duration_hr"] <= 3:
                continue
            da = datetime.datetime.strptime(a["date"], "%Y-%m-%d")
            db = datetime.datetime.strptime(b["date"], "%Y-%m-%d")
            if abs((da - db).days) != 1:
                continue
            inter = get_drive(a["postcode"], b["postcode"], travel_cache)
            if inter and inter["duration"] <= 75 * 60:
                a.setdefault("combo_with", []).append(b["show"])
                b.setdefault("combo_with", []).append(a["show"])

def should_include_class(name):
    if any(c.lower() in name.lower() for c in CLASS_EXCLUSIONS):
        return False
    if "golden" in name.lower() or any(a.lower() in name.lower() for a in ALWAYS_INCLUDE_CLASS):
        return True
    return False

async def download_schedule_playwright(show_url):
    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            print(f"[INFO] Navigating to {show_url}...")
            await page.goto(show_url)
            await page.wait_for_load_state("networkidle")
            print(f"[INFO] Page loaded, waiting for 'Schedule' button...")

            async with page.expect_download() as download_info:
                await page.click('input[type="submit"][value*="Schedule"]', timeout=15000)

            download = await download_info.value
            filename = download.suggested_filename
            await download.save_as(filename)
            await browser.close()
            print(f"[INFO] Downloaded and saved: {filename}")
            return filename

    except Exception as e:
        print(f"[ERROR] Playwright failed for {show_url}: {e}")
        return None

def fetch_aspx_links():
    try:
        print("[INFO] Fetching FosseData show links...")
        soup = BeautifulSoup(requests.get("https://www.fossedata.co.uk/shows.aspx").text, "html.parser")

        all_links = [a.get("href") for a in soup.select("a[href$='.aspx']") if a.get("href")]
        print(f"[DEBUG] Found {len(all_links)} .aspx links total.")

        filtered = []
        for href in all_links:
            if re.match(r"^/shows/[^/]+\.aspx$", href):
                print(f"[DEBUG] Accepted show link: {href}")
                filtered.append("https://www.fossedata.co.uk" + href)
            else:
                print(f"[DEBUG] Rejected link: {href}")

        with open("aspx_links.txt", "w") as f:
            f.write("\n".join(filtered))

        print(f"[INFO] Returning {len(filtered)} filtered show links.")
        return filtered

    except Exception as e:
        print(f"[ERROR] Error fetching ASPX links: {e}")
        return []

async def full_run():
    global travel_cache
    urls = fetch_aspx_links()
    if not urls:
        print("[WARN] No show URLs found.")
        return []

    print(f"[DEBUG] Starting schedule scrape for {len(urls)} show URLs.")

    travel_cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)

    shows = []
    for url in urls:
        pdf = await download_schedule_playwright(url)
        if not pdf:
            continue
        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} — no 'golden'")
            continue
        postcode = get_postcode(text)
        travel = get_drive(HOME_POSTCODE, postcode, travel_cache) if postcode else None
        cost = estimate_cost(travel["distance"], travel["duration"]) if travel else None
        judges = extract_judges(text)
        show_date = get_show_date(text)
        shows.append({
            "show": url,
            "pdf": pdf,
            "date": show_date.isoformat() if show_date else None,
            "postcode": postcode,
            "duration_hr": round(travel["duration"] / 3600, 2) if travel else None,
            "distance_km": round(travel["distance"], 1) if travel else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "points": jw_points(text),
            "judge": judges,
        })

    find_clashes_and_combos(shows)

    with open("results.json", "w") as f:
        json.dump(shows, f, indent=2)

    with open("results.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Show", "Date", "Postcode", "Distance (km)", "Time (hr)",
            "Estimated Cost", "JW Points", "Golden Judge(s)", "Clash", "Combos"
        ])
        for s in shows:
            judge = s.get("judge", {})
            judge_text = ", ".join(f"{k}: {v}" for k, v in judge.items()) if judge else ""
            combos = "; ".join(s.get("combo_with", [])) if "combo_with" in s else ""
            writer.writerow([
                s["show"],
                s["date"],
                s["postcode"],
                s.get("distance_km"),
                s.get("duration_hr"),
                s.get("cost_estimate"),
                s["points"],
                judge_text,
                "Yes" if s.get("clash") else "",
                combos
            ])
    print(f"[INFO] Processed {len(shows)} shows with Golden Retriever classes.")
    return shows
