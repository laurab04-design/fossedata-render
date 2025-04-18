import os
import re
import csv
import json
import time
import datetime
import requests
import pdfplumber
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Configuration
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
CACHE_FILE = "travel_cache.json"
DOG_DOB = datetime.datetime.strptime("2024-05-15", "%Y-%m-%d")
DOG_NAME = "Delia"
MPG = 40
OVERNIGHT_THRESHOLD_HOURS = 3
OVERNIGHT_COST = 100

BASE_URL = "https://www.fossedata.co.uk/shows.aspx"
OUTPUT_FILE = "aspx_links.txt"

def fetch_aspx_links():
    try:
        response = requests.get(BASE_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.select("a[href$='.aspx']"):
            href = a.get("href")
            if href and "/shows/" in href.lower():
                links.append("https://www.fossedata.co.uk" + href)
        with open(OUTPUT_FILE, "w") as f:
            f.write("\n".join(links))
        return links
    except Exception as e:
        print(f"[ERROR] Failed to fetch ASPX links: {e}")
        return []

def download_schedule_playwright(show_url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(show_url)
            page.wait_for_load_state("networkidle")
            with page.expect_download() as download_info:
                page.click('input[type="submit"][value*="Schedule"]', timeout=5000)
                download = download_info.value
                filename = download.suggested_filename
                download.save_as(filename)
            return filename
    except Exception as e:
        print(f"[ERROR] Playwright failed for {show_url}: {e}")
        return None

def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        print(f"[ERROR] PDF extraction failed: {e}")
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
        data = r.json()
        e = data["rows"][0]["elements"][0]
        result = {
            "duration": e["duration"]["value"],
            "distance": e["distance"]["value"] / 1000,
        }
        cache[key] = result
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        return result
    except Exception as e:
        print(f"[ERROR] Travel API failed: {e}")
        return None

def get_diesel_price():
    try:
        r = requests.get("https://www.globalpetrolprices.com/diesel_prices/")
        soup = BeautifulSoup(r.text, "html.parser")
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
        if dogs: result["dogs"] = dogs.group(1).strip()
        if bitches: result["bitches"] = bitches.group(1).strip()
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
    if "open show" in text.lower():
        return 1
    if "championship show" in text.lower():
        return 9
    return 0

def find_clashes_and_combos(results, travel_cache):
    date_map = {}
    for show in results:
        if show["date"]:
            date_map.setdefault(show["date"], []).append(show)

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

def full_run():
    links = fetch_aspx_links()
    if not links:
        print("No show URLs found.")
        return []

    travel_cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)

    results = []

    for url in links:
        pdf = download_schedule_playwright(url)
        if not pdf:
            continue
        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            continue
        postcode = get_postcode(text)
        drive = get_drive(HOME_POSTCODE, postcode, travel_cache) if postcode else None
        cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
        judges = extract_judges(text)
        show_date = get_show_date(text)

        results.append({
            "show": url,
            "pdf": pdf,
            "date": show_date.isoformat() if show_date else None,
            "postcode": postcode,
            "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "points": jw_points(text),
            "judge": judges,
        })

    find_clashes_and_combos(results, travel_cache)

    with open("results.json", "w") as f:
        json.dump(results, f, indent=2)

    with open("results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Show", "Date", "Postcode", "Distance (km)", "Time (hr)",
            "Estimated Cost", "JW Points", "Golden Judge(s)", "Clash", "Combos"
        ])
        for s in results:
            judge_text = ", ".join(f"{k}: {v}" for k, v in s.get("judge", {}).items()) if s.get("judge") else ""
            combos = "; ".join(s.get("combo_with", [])) if "combo_with" in s else ""
            writer.writerow([
                s["show"], s["date"], s["postcode"], s.get("distance_km"),
                s.get("duration_hr"), s.get("cost_estimate"), s["points"],
                judge_text, "Yes" if s.get("clash") else "", combos
            ])

    print(f"Processed {len(results)} shows with Golden Retriever classes.")
    return results
