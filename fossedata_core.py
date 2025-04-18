import os
import re
import json
import csv
import datetime
import requests
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# === CONFIGURATION ===
HOME_POSTCODE       = os.environ.get("HOME_POSTCODE")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
CACHE_FILE          = "travel_cache.json"

# === HELPERS ===

BASE_URL    = "https://www.fossedata.co.uk/shows.aspx"
OUTPUT_FILE = "aspx_links.txt"

def fetch_aspx_links():
    """Scrape the main shows.aspx page and write + return all .aspx show URLs."""
    try:
        r = requests.get(BASE_URL)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.select("a[href$='.aspx']"):
            href = a.get("href")
            if href and "/shows/" in href.lower():
                full = urljoin("https://www.fossedata.co.uk", href)
                links.append(full)
        with open(OUTPUT_FILE, "w") as f:
            f.write("\n".join(links))
        return links
    except Exception as e:
        print(f"[ERROR] fetch_aspx_links: {e}")
        return []

def download_schedule_playwright(show_url):
    """Use Playwright to click the Schedule button and download the PDF."""
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page    = browser.new_page()
            page.goto(show_url)
            page.wait_for_load_state("networkidle")
            with page.expect_download() as dl_info:
                page.click('input[type="submit"][value*="Schedule"]', timeout=5000)
            download = dl_info.value
            fn = download.suggested_filename
            download.save_as(fn)
            browser.close()
            return fn
    except Exception as e:
        print(f"[ERROR] Playwright failed for {show_url}: {e}")
        return None

def extract_text_from_pdf(path):
    try:
        text = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                text.append(t)
        joined = "\n".join(text)
        print(f"Extracted {len(joined)} chars from {path}")
        return joined
    except Exception as e:
        print(f"[ERROR] PDF extract failed for {path}: {e}")
        return ""

def get_postcode(text):
    m = re.search(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?) ?\d[A-Z]{2}\b", text)
    return m.group(0) if m else None

def get_drive(orig, dest, cache):
    key = f"{orig}_TO_{dest}"
    if key in cache:
        return cache[key]
    try:
        res = requests.get(
            "https://maps.googleapis.com/maps/api/distancematrix/json",
            params={"origins": orig, "destinations": dest,
                    "mode": "driving", "key": GOOGLE_MAPS_API_KEY}
        )
        data = res.json()
        e = data["rows"][0]["elements"][0]
        result = {"duration": e["duration"]["value"],
                  "distance": e["distance"]["value"] / 1000.0}
        cache[key] = result
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        return result
    except Exception as e:
        print(f"[ERROR] get_drive: {e}")
        return None

def get_diesel_price():
    try:
        r = requests.get("https://www.globalpetrolprices.com/diesel_prices/")
        soup = BeautifulSoup(r.text, "html.parser")
        row = soup.find("td", string=re.compile("United Kingdom")).find_parent("tr")
        price = row.find_all("td")[2].text.strip().replace("£", "")
        return float(price)
    except:
        return 1.60

MPG = 40
OVERNIGHT_THRESHOLD_HOURS = 3
OVERNIGHT_COST = 100.0

def estimate_cost(distance_km, duration_s):
    miles   = distance_km * 0.621371
    price   = get_diesel_price()
    gallons = miles / MPG
    fuel    = gallons * 4.54609 * price
    if duration_s > OVERNIGHT_THRESHOLD_HOURS * 3600:
        fuel += OVERNIGHT_COST
    return fuel

def extract_judges(text):
    """Only Golden Retriever section."""
    sec = text.lower().split("retriever (golden)")
    if len(sec) < 2:
        return {}
    block = sec[1][:1000]
    res = {}
    for key, pat in [("dogs",   r"dogs.*judge.*?:\s*([A-Z][a-z].+)"),
                     ("bitches",r"bitches.*judge.*?:\s*([A-Z][a-z].+)"),
                     ("all",    r"judge.*?:\s*([A-Z][a-z].+)")]:
        m = re.search(pat, block, re.I)
        if m:
            res[key] = m.group(1).strip()
    return res

def get_show_date(text):
    m = re.search(r"Date Of Show:\s*([A-Za-z]+,\s*\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if not m:
        return None
    try:
        return datetime.datetime.strptime(m.group(1), "%A, %d %B %Y").date()
    except:
        return None

def jw_points(text):
    tl = text.lower()
    if "championship show" in tl:
        return 9
    if "open show" in tl:
        return 1
    return 0

def find_clashes_and_combos(shows):
    date_map = {}
    for s in shows:
        d = s["date"]
        if d:
            date_map.setdefault(d, []).append(s)
    # same-day clashes
    for same in date_map.values():
        if len(same) > 1:
            for s in same:
                s["clash"] = True
    # back‑to‑back combos
    for i, a in enumerate(shows):
        for j, b in enumerate(shows):
            if i == j or not a["date"] or not b["date"]:
                continue
            da = datetime.datetime.fromisoformat(a["date"])
            db = datetime.datetime.fromisoformat(b["date"])
            if abs((da - db).days) != 1:
                continue
            inter = get_drive(a["postcode"], b["postcode"], travel_cache)
            if inter and inter["duration"] <= 75 * 60:
                a.setdefault("combo_with", []).append(b["show"])
                b.setdefault("combo_with", []).append(a["show"])

# === FULL RUN ===

def full_run():
    urls = fetch_aspx_links()
    if not urls:
        print("No show URLs found.")
        return []

    # load cache
    global travel_cache
    travel_cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)

    shows = []
    for url in urls:
        pdf = download_schedule_playwright(url)
        if not pdf:
            continue
        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            continue

        pc     = get_postcode(text)
        drive  = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
        cost   = estimate_cost(drive["distance"], drive["duration"]) if drive else None
        judge  = extract_judges(text)
        date   = get_show_date(text)

        shows.append({
            "show":        url,
            "pdf":         pdf,
            "date":        date.isoformat() if date else None,
            "postcode":    pc,
            "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost":        round(cost, 2) if cost else None,
            "points":      jw_points(text),
            "judge":       judge,
        })

    find_clashes_and_combos(shows)

    # write JSON
    with open("results.json", "w") as f:
        json.dump(shows, f, indent=2)

    # write CSV
    with open("results.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "Show","Date","Postcode","Distance (km)","Duration (hr)",
            "Cost","Points","Judge(s)","Clash","Combos"
        ])
        for s in shows:
            combos = ";".join(s.get("combo_with", []))
            judges = ";".join(f"{k}:{v}" for k,v in s["judge"].items())
            w.writerow([
                s["show"], s["date"], s["postcode"],
                s["distance_km"], s["duration_hr"],
                s["cost"], s["points"], judges,
                "Yes" if s.get("clash") else "", combos
            ])

    print(f"Processed {len(shows)} shows.")
    return shows
