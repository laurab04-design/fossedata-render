import os
import re
import requests
import json
import time
import datetime
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# === CONFIGURATION ===
HOME_POSTCODE = os.environ.get("HOME_POSTCODE")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
CACHE_FILE = "travel_cache.json"
DOG_DOB = datetime.datetime.strptime("2024-05-15", "%Y-%m-%d")
DOG_NAME = "Delia"
MPG = 40
OVERNIGHT_THRESHOLD_HOURS = 3
OVERNIGHT_COST = 100

# === STEP 1: Get .aspx Show URLs ===
def fetch_aspx_links():
    base_url = "https://www.fossedata.co.uk/show-schedules.aspx"
    response = requests.get(base_url, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    links = []

    for a in soup.select("a[href$='.aspx']"):
        href = a.get("href")
        if href and "shows/" in href.lower():
            full_url = urljoin(base_url, href)
            links.append(full_url)

    with open("aspx_links.txt", "w") as f:
        f.write("\n".join(links))

    return links

# === STEP 2: Download and Extract PDF ===
def download_schedule(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        link = soup.find("a", href=lambda href: href and ".pdf" in href.lower())
        if not link:
            return None
        pdf_url = urljoin(url, link['href'])
        filename = pdf_url.split("/")[-1]
        pdf_response = requests.get(pdf_url)
        with open(filename, "wb") as f:
            f.write(pdf_response.content)
        return filename
    except Exception as e:
        print(f"Download failed for {url}: {e}")
        return None

def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        print(f"PDF text extraction failed: {e}")
        return ""

# === STEP 3: Extract Info and Calculate Travel ===
def get_postcode_from_text(text):
    match = re.search(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?) ?\d[A-Z]{2}\b", text)
    return match.group(0) if match else None

def get_drive_time(from_postcode, to_postcode, travel_cache):
    cache_key = f"{from_postcode}_TO_{to_postcode}"
    if cache_key in travel_cache:
        return travel_cache[cache_key]

    try:
        url = "https://maps.googleapis.com/maps/api/distancematrix/json"
        params = {
            "origins": from_postcode,
            "destinations": to_postcode,
            "mode": "driving",
            "key": GOOGLE_MAPS_API_KEY,
        }
        r = requests.get(url, params=params)
        data = r.json()
        element = data["rows"][0]["elements"][0]
        duration = element["duration"]["value"]  # in seconds
        distance_km = element["distance"]["value"] / 1000
        travel_cache[cache_key] = {"duration": duration, "distance": distance_km}
        with open(CACHE_FILE, "w") as f:
            json.dump(travel_cache, f)
        return travel_cache[cache_key]
    except Exception as e:
        print(f"Travel time lookup failed: {e}")
        return None

def get_latest_diesel_price():
    try:
        r = requests.get("https://www.globalpetrolprices.com/diesel_prices/")
        soup = BeautifulSoup(r.text, "html.parser")
        uk_row = soup.find("td", string=re.compile("United Kingdom")).find_parent("tr")
        price = uk_row.find_all("td")[2].text.strip().replace("£", "")
        return float(price)
    except:
        return 1.60  # fallback

def estimate_cost(distance_km, duration_sec):
    miles = distance_km * 0.621371
    diesel_price = get_latest_diesel_price()
    gallons = miles / MPG
    diesel_cost = gallons * 4.54609 * diesel_price
    return diesel_cost + OVERNIGHT_COST if duration_sec > OVERNIGHT_THRESHOLD_HOURS * 3600 else diesel_cost

def jw_points_possible(text):
    if "open show" in text.lower():
        return 1
    if "championship show" in text.lower():
        return 9
    return 0

# === MAIN PROCESS ===
def main():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)
    else:
        travel_cache = {}

    show_urls = fetch_aspx_links()
    results = []

    for url in show_urls:
        filename = download_schedule(url)
        if not filename:
            continue
        text = extract_text_from_pdf(filename)
        if "retriever (golden)" not in text.lower():
            continue

        postcode = get_postcode_from_text(text)
        travel = get_drive_time(HOME_POSTCODE, postcode, travel_cache) if postcode else None
        points = jw_points_possible(text)
        cost = estimate_cost(travel["distance"], travel["duration"]) if travel else None

        results.append({
            "show": url,
            "pdf": filename,
            "postcode": postcode,
            "travel_time_hr": round(travel["duration"]/3600, 2) if travel else "?",
            "distance_km": round(travel["distance"], 1) if travel else "?",
            "points_possible": points,
            "estimated_cost": round(cost, 2) if cost else "?",
        })

    with open("results.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"Done! Processed {len(results)} Retriever (Golden) schedules.")

if __name__ == "__main__":
    main()
