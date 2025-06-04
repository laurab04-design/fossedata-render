# Introducing attempt number ? fossedata_core.py

import os
import io
import re
import csv
import json
import base64
import requests
import datetime
import pdfplumber
import asyncio
from pathlib import Path
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dateutil.parser import parse as date_parse
from playwright.async_api import async_playwright
from typing import List, Tuple, Optional
from collections import defaultdict
from kc_breeds import KC_BREEDS
from fossedata_results import scrape_all_results
from higham_links import fetch_higham_show_links

load_dotenv()

# ===== Load Environment Variables Correctly =====
google_service_account_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
gdrive_folder_id = os.getenv("GDRIVE_FOLDER_ID")
BREED_KEYWORDS = [b.lower() for b in KC_BREEDS]
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
HOME_POSTCODE = os.getenv("HOME_POSTCODE")
MPG = os.getenv("MPG")
OVERNIGHT_THRESHOLD_HOURS = os.getenv("OVERNIGHT_THRESHOLD_HOURS")
OVERNIGHT_COST = os.getenv("OVERNIGHT_COST")
MAX_PAIR_GAP_MINUTES = os.getenv("MAX_PAIR_GAP_MINUTES")

# Build Drive client exactly as before
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

if google_service_account_key:
    try:
        # Decode the base64 string into bytes
        decoded_key = base64.b64decode(google_service_account_key)

        # Convert bytes to a JSON string and then load it into a dictionary
        service_account_info = json.loads(decoded_key.decode("utf-8"))

        # Authenticate with the Google API using the decoded key
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/drive.file"]
        )

        # Build the drive service client
        drive_service = build("drive", "v3", credentials=credentials)
        print("[INFO] Google Drive client connected.")

    except Exception as e:
        print(f"[ERROR] Failed to decode or authenticate with service account: {e}")
else:
    print("[ERROR] GOOGLE_SERVICE_ACCOUNT_BASE64 environment variable is not set.")

def download_from_drive(filename, mime_type="application/json"):
    try:
        if not gdrive_folder_id:
            print("[ERROR] GDRIVE_FOLDER_ID not set for download.")
            return

        res = drive_service.files().list(
            q=f"name='{filename}' and trashed=false and '{gdrive_folder_id}' in parents",
            spaces="drive",
            fields="files(id, name)"
        ).execute()

        if not res["files"]:
            print(f"[INFO] {filename} not found in Drive. Skipping download.")
            return  # <- ADD THIS LINE

        file_id = res["files"][0]["id"]
        request = drive_service.files().get_media(fileId=file_id)
        with open(filename, "wb") as fh:
            fh.write(request.execute())
        print(f"[INFO] Downloaded {filename} from Drive.")

    except Exception as e:
        print(f"[ERROR] Failed to download {filename}: {e}")

# ===== Constants =====
PROCESSED_SHOWS_FILE = "processed_shows.json"
STORAGE_STATE_FILE = "storage_state.json"
RESULTS_CSV = "results.csv"
RESULTS_JSON = "results.json"
ASPX_LINKS = "aspx_links.txt"
TRAVEL_CACHE_FILE = "travel_cache.json"
CLASH_OVERNIGHT_CSV = "clashes_overnight.csv"
WINS_LOG_FILE = "wins.json"
GOLDEN_RESULTS_FILE="golden_results.csv"
HIGHAM_LINKS_FILE="higham_links.txt"

LITERS_PER_GALLON = 4.54609
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
DOG_DOB = datetime.datetime.strptime(os.environ.get("DOG_DOB", "2024-05-15"), "%Y-%m-%d").date()
MPG = float(os.environ.get("MPG", 40))
OVERNIGHT_THRESHOLD_HOURS = float(os.environ.get("OVERNIGHT_THRESHOLD_HOURS", 3))
OVERNIGHT_COST = float(os.environ.get("OVERNIGHT_COST", 100))

download_from_drive("processed_shows.json")
download_from_drive("storage_state.json")
download_from_drive("aspx_links.txt")
download_from_drive("travel_cache.json")
download_from_drive("wins.json")
download_from_drive("clashes_overnight.csv")
download_from_drive("golden_results.csv")

# ===== Travel Cache Configuration =====
travel_updated = False  # Global flag to track if travel cache was changed

def load_travel_cache():
    #Loads the travel cache from travel_cache.json.
    if Path(TRAVEL_CACHE_FILE).exists():
        try:
            with open(TRAVEL_CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load travel cache: {e}")
    return {}

def save_travel_cache(cache):
    #Saves the travel cache to travel_cache.json.
    try:
        with open(TRAVEL_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
        print("[INFO] Travel cache saved.")
    except Exception as e:
        print(f"[ERROR] Failed to save travel cache: {e}")

# ===== Load Cache =====
processed_shows = set()  # <- Always define it, even if the file is missing

if os.path.isfile(PROCESSED_SHOWS_FILE):
    try:
        with open(PROCESSED_SHOWS_FILE, "r") as f:
            data = json.load(f)
            processed_shows = set(data if isinstance(data, list) else data.keys())
    except Exception as e:
        print(f"Warning: Could not load {PROCESSED_SHOWS_FILE}: {e}")
        {}

# ===== Diesel Price =====
def fetch_gov_diesel_price():
    url = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1254009/weekly-road-fuel-prices.csv"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            csv_text = resp.content.decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_text))
            rows = list(reader)
            latest_row = rows[-1]
            diesel_price_ppl = latest_row.get('Diesel', '').replace('p', '').strip()
            if diesel_price_ppl:
                return float(diesel_price_ppl) / 100.0
    except Exception as e:
        print(f"Warning: Gov fuel price fetch failed: {e}")
    return 1.57

diesel_price_per_litre = fetch_gov_diesel_price()
print(f"Gov diesel price: £{diesel_price_per_litre:.2f} per litre")

def read_existing_links() -> List[str]:
    #Read show URLs from aspx_links.txt if present
    try:
        with open("aspx_links.txt", "r") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []
        
def save_links(links: set):
    #Save show URLs to aspx_links.txt
    if not links:
        print("[WARNING] save_links() called with empty set.")
        return
    with open("aspx_links.txt", "w") as f:
        for link in sorted(links):
            f.write(f"{link}\n")
    print(f"[INFO] Wrote {len(links)} links to aspx_links.txt")
    
async def fetch_show_list(page) -> List[dict]:
    # Scrape the FosseData 'Shows to Enter' page for all .aspx links and show details.
    await page.goto("https://fossedata.co.uk/shows/Shows-To-Enter.aspx", timeout=60000)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    rows = soup.select("tr.tableRow, tr.alternateRow, tr.tableRow.redBg, tr.alternateRow.redBg")
    existing_links = set(read_existing_links())
    new_links = set(existing_links)
    shows = []

    for row in rows:
        try:
            show_name_div = row.find("div", class_="showName")
            date_td = row.find_all("td")[1]
            link_tag = row.find("a", string="Details")

            if not show_name_div or not date_td or not link_tag:
                continue

            show_name = show_name_div.get_text(strip=True)
            if show_name.upper().startswith("NEW"):
                show_name = show_name[3:].strip()

            # Skip any show with a breed/group keyword in the name
            if any(keyword in show_name.lower() for keyword in BREED_KEYWORDS):
                print(f"[SKIP] Excluding breed-specific show: {show_name}")
                continue

            show_url = f"https://www.fossedata.co.uk/{link_tag['href']}"
            date_text = date_td.get_text(strip=True)

            # Handle date range
            if " - " in date_text:
                start_date_str = date_text.split(" - ")[0]
            else:
                start_date_str = date_text

            try:
                show_date = datetime.datetime.strptime(start_date_str.strip(), "%d %b %Y").date()
            except ValueError:
                try:
                    show_date = datetime.datetime.strptime(start_date_str.strip(), "%d %B %Y").date()
                except ValueError:
                    continue

            if show_url in existing_links:
                continue

            name_lower = show_name.lower()
            if "championship" in name_lower:
                show_type = "Championship"
            elif "premier" in name_lower:
                show_type = "Premier Open"
            elif "open" in name_lower:
                show_type = "Open"
            else:
                show_type = "Unknown"

            show_info = {
                "id": show_url,
                "show_name": show_name,
                "date": show_date,
                "venue": "",  # Venue not available on listing page
                "type": show_type,
                "url": show_url
            }

            shows.append(show_info)
            new_links.add(show_url)

        except Exception as e:
            print(f"[WARN] Skipped one row due to error: {e}")
            continue

    save_links(new_links)
    print(f"[INFO] Collected {len(shows)} new shows")
    return shows

def download_schedule_via_post(show_url: str, schedule_pdf_path: str) -> Optional[Tuple[str, str]]:
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        # Step 1: Get the form page to extract hidden fields
        resp = session.get(show_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # === Extract venue from schema.org PostalAddress block ===
        address_block = soup.find("span", itemprop="address")
        venue_parts = []

        if address_block:
            street = address_block.find("span", itemprop="streetAddress")
            locality = address_block.find("span", itemprop="addressLocality")
            region = address_block.find("span", itemprop="addressRegion")
            postcode = address_block.find("span", itemprop="postalCode")

            if street:
                venue_parts.append(street.get_text(strip=True))
            if locality:
                venue_parts.append(locality.get_text(strip=True))
            if region:
                venue_parts.append(region.get_text(strip=True))
            if postcode:
                venue_parts.append(postcode.get_text(strip=True))

        venue = ", ".join(venue_parts)

        # === Extract hidden form fields ===
        viewstate = soup.find("input", {"id": "__VIEWSTATE"}).get("value", "")
        viewstategen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"}).get("value", "")
        event_validation_tag = soup.find("input", {"id": "__EVENTVALIDATION"})
        event_validation = event_validation_tag.get("value", "") if event_validation_tag else ""

        form_data = {
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategen,
            "__EVENTVALIDATION": event_validation,
            "ctl00$ContentPlaceHolder$btnDownloadSchedule": "Schedule",
        }

        # === Download the PDF via POST ===
        post_resp = session.post(show_url, data=form_data)
        if post_resp.status_code == 200 and b"%PDF" in post_resp.content[:1024]:
            with open(schedule_pdf_path, "wb") as f:
                f.write(post_resp.content)
            print(f"[INFO] Downloaded schedule via POST: {schedule_pdf_path}")
            return schedule_pdf_path, venue
        else:
            print(f"[ERROR] POST failed or not a PDF. Status: {post_resp.status_code}")
            return None, ""

    except Exception as e:
        print(f"[ERROR] POST schedule download failed for {show_url}: {e}")
        return None, ""
    
def get_travel_info(destination: str, travel_cache: dict) -> dict:
    global travel_updated

    if destination in travel_cache:
        return travel_cache[destination]

    if not GOOGLE_MAPS_API_KEY:
        print("[ERROR] No Google Maps API key configured.")
        return {}

    base_url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": HOME_POSTCODE,
        "destinations": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "units": "imperial"
    }

    try:
        response = requests.get(base_url, params=params)
        data = response.json()

        if data["status"] == "OK" and data["rows"][0]["elements"][0]["status"] == "OK":
            distance_text = data["rows"][0]["elements"][0]["distance"]["text"]
            duration_text = data["rows"][0]["elements"][0]["duration"]["text"]
            distance_miles = float(distance_text.replace(" mi", "").replace(",", ""))
            duration_hours = float(data["rows"][0]["elements"][0]["duration"]["value"]) / 3600

            estimated_cost = calculate_diesel_cost(distance_miles, diesel_price_per_litre, MPG)
            overnight = duration_hours > OVERNIGHT_THRESHOLD_HOURS

            travel_info = {
                "distance_miles": distance_miles,
                "duration_hours": round(duration_hours, 2),
                "estimated_cost": round(estimated_cost, 2),
                "overnight_required": overnight,
                "overnight_cost": OVERNIGHT_COST if overnight else 0
            }

            travel_cache[destination] = travel_info
            travel_updated = True
            return travel_info

        else:
            print(f"[ERROR] Google Maps API error: {data['rows'][0]['elements'][0]['status']}")
            return {}

    except Exception as e:
        print(f"[ERROR] Failed to fetch travel info for {destination}: {e}")
        return {}

def get_between_travel_info(origin: str, destination: str, cache: dict) -> dict:
    # Get travel time between two venues. Uses cache if available, fetches if missing.
    if not origin or not destination:
        return {"distance_miles": 0, "drive_time_minutes": 9999}

    key = f"{origin}||{destination}"
    if 'between' not in cache:
        cache['between'] = {}

    if key in cache['between']:
        return cache['between'][key]

    try:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        params = {
            "origin": origin,
            "destination": destination,
            "key": api_key,
            "units": "imperial",
        }
        resp = requests.get("https://maps.googleapis.com/maps/api/directions/json", params=params, timeout=10)
        data = resp.json()
        if data["status"] == "OK":
            leg = data["routes"][0]["legs"][0]
            miles = float(leg["distance"]["text"].replace(" mi", ""))
            minutes = leg["duration"]["value"] // 60
            result = {"distance_miles": miles, "drive_time_minutes": minutes}
            cache['between'][key] = result
            return result
        else:
            print(f"Google Maps API error between {origin} and {destination}: {data['status']}")
    except Exception as e:
        print(f"Error fetching between-venue travel info from {origin} to {destination}: {e}")

    # Cache the failure so it doesn't retry repeatedly
    cache['between'][key] = {"distance_miles": 0, "drive_time_minutes": 9999}
    return cache['between'][key]

def calculate_diesel_cost(distance_miles: float, price_per_litre: float, mpg: float) -> float:
    # Calculates round-trip diesel cost for given distance, diesel price, and mpg.
    if mpg <= 0:
        return 0.0
    gallons_needed = (distance_miles * 2) / mpg  # Round trip
    litres_needed = gallons_needed * LITERS_PER_GALLON
    return round(litres_needed * price_per_litre, 2)

def parse_pdf_for_info(pdf_path: str, show_name:str) -> Optional[dict]:
    #Extract relevant information from the schedule PDF.
    #Includes entry fees, catalogue prices, judges, and whether eligible classes are present.
    text = ""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(pdf_path)
        for page in doc:
            text += page.get_text()
        doc.close()
    except Exception:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print(f"Failed to read PDF {pdf_path}: {e}")
            return None

    text_lower = text.lower()
    if "golden" not in text_lower:
        return None  # Skip if Golden Retrievers not mentioned

    info = {
        "first_entry_fee": extract_fee(r"First\s+Entry[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text),
        "subsequent_entry_fee": extract_fee(r"Subsequent[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text),
        "catalogue_price": extract_fee(r"Catalogue[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text),
    }
    
    info["type"] = extract_show_type_from_schedule(text)

    judge_dogs, judge_bitches = extract_judges(text,show_name)
    info["judge_dogs"] = judge_dogs
    info["judge_bitches"] = judge_bitches

    eligible_classes = [
        "Puppy", "Junior", "Yearling", "Special Beginners",
        "Undergraduate", "Tyro", "Novice", "Minor Puppy"
    ]
    info["eligible_classes_found"] = any(cls.lower() in text_lower for cls in eligible_classes)

    return info

def extract_show_type_from_schedule(text: str) -> str:
    text = text.lower()
    show_type_keywords = [
        ("championship show", "Championship"),
        ("premier open show", "Premier Open"),
        ("limited show", "Limited"),
        ("open show", "Open")
    ]
    first_found = None
    first_pos = len(text) + 1

    for keyword, label in show_type_keywords:
        index = text.find(keyword)
        if 0 <= index < first_pos:
            first_pos = index
            first_found = label

    return first_found or "Unknown"

def extract_fee(pattern: str, text: str) -> Optional[float]:
    #Extract a fee amount using the given regex pattern
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

import re
from typing import Tuple, Optional

def extract_judges(text: str, show_name: str = "") -> Tuple[Optional[str], Optional[str]]:
    # Split into lines for structured parsing
    lines = text.splitlines()
    text_lower = text.lower()

    is_single_breed = (
        "golden retriever club" in show_name.lower()
        or (
            "golden retriever" in show_name.lower()
            and not any(
                breed in text_lower
                for breed in KC_BREEDS
                if breed != "golden retriever"
            )
        )
    )

    # Recognise breed label variants
    golden_variants = [
        r"golden retriever",
        r"retriever\s*\(golden\)",
        r"retriever\s*-\s*golden",
        r"retriever\s+golden"
    ]

    # Judge name pattern
    judge_name_pattern = re.compile(
        r"\b(Mr|Mrs|Ms|Miss|Dr)\s+[A-Z][a-zA-Z]+(?:\s+[A-Z][a-z]+)*(?:\s+\([^)]+\))?",
        re.IGNORECASE
    )

    judge_dogs = None
    judge_bitches = None
    
    # === SINGLE BREED STRATEGY ===
    if is_single_breed:
        dog_match = re.search(r"(Dogs?:)\s*(.+)", text, flags=re.IGNORECASE)
        bitch_match = re.search(r"(Bitches?:)\s*(.+)", text, flags=re.IGNORECASE)
        if dog_match:
            judge_dogs = dog_match.group(2).strip(" .\n\r\t")
        if bitch_match:
            judge_bitches = bitch_match.group(2).strip(" .\n\r\t")

        if not judge_dogs and not judge_bitches:
            # Look for "Judge:" or "Judges:" label
            for line in lines:
                if re.search(r"^\s*judge[s]?:", line, re.IGNORECASE):
                    judge_match = judge_name_pattern.search(line)
                    if judge_match:
                        name = judge_match.group(0).strip()
                        judge_dogs = judge_bitches = name
                        break

        if not judge_dogs and not judge_bitches:
            # Fallback: grab first judge-looking line
            for line in lines:
                judge_match = judge_name_pattern.search(line)
                if judge_match:
                    name = judge_match.group(0).strip()
                    judge_dogs = judge_bitches = name
                    break

        return judge_dogs, judge_bitches

    # === MULTI BREED STRATEGY ===
    for i, line in enumerate(lines):
        for variant in golden_variants:
            if re.search(variant, line, re.IGNORECASE):
                inline_match = judge_name_pattern.search(line)
                if inline_match:
                    name = inline_match.group(0).strip()
                    judge_dogs = judge_bitches = name
                    return judge_dogs, judge_bitches

                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    next_line_match = judge_name_pattern.search(next_line)
                    if next_line_match:
                        name = next_line_match.group(0).strip()
                        judge_dogs = judge_bitches = name
                        return judge_dogs, judge_bitches

    return judge_dogs, judge_bitches
    
def detect_clashes(results: List[dict]) -> List[dict]:
    """
    Detect same-day show clashes (ignoring if same postcode).
    """
    clashes = []
    shows_by_date = defaultdict(list)

    for r in results:
        show_date = r.get("show_date") or r.get("date")
        if show_date:
            shows_by_date[show_date].append(r)

    for date, show_list in shows_by_date.items():
        if len(show_list) > 1:
            for i in range(len(show_list)):
                for j in range(i + 1, len(show_list)):
                    s1 = show_list[i]
                    s2 = show_list[j]
                    pc1 = extract_postcode(s1.get("venue", ""))
                    pc2 = extract_postcode(s2.get("venue", ""))
                    if pc1 and pc2 and pc1.upper() == pc2.upper():
                        continue  # Same postcode = allowed
                    clashes.append({
                        "type": "Clash",
                        "date": date,
                        "show1": s1["show_name"],
                        "show2": s2["show_name"]
                    })
    return clashes
    
def detect_overnight_pairs(
    results: List[dict],
    travel_cache: dict
) -> List[dict]:

    #Detect overnight stay chains:
    # Shows on consecutive days
    # Both >3h from home
    # Consecutive pairs within MAX_PAIR_GAP_MINUTES of each other
    # Allows multi-day chaining
    
    overnights = []
    max_pair_gap_minutes = int(os.getenv("MAX_PAIR_GAP_MINUTES", "75"))
    max_shows = os.getenv("MAX_SHOWS")
    max_shows = int(max_shows) if max_shows and max_shows.isdigit() else None  # Unlimited if unset

    results_by_date = sorted(
        [r for r in results if r.get("show_date") or r.get("date")],
        key=lambda x: date_parse(x.get("show_date") or x.get("date")).date()
    )

    for i, show_a in enumerate(results_by_date):
        chain = [show_a]
        travel_times = []
        date_a = date_parse(show_a.get("show_date") or show_a.get("date")).date()
        time_from_home = show_a.get("drive_time_minutes", 0)
        if time_from_home < 180:
            continue  # Only care about shows over 3h away

        current_date = date_a
        current_show = show_a

        while True:
            next_day = current_date + datetime.timedelta(days=1)
            next_day_shows = [
                r for r in results_by_date
                if date_parse(r.get("show_date") or r.get("date")).date() == next_day
            ]
            found_next = False
            for show_b in next_day_shows:
                venue_a = current_show['venue']
                venue_b = show_b['venue']
                if not venue_a or not venue_b:
                    continue

                travel_ab = get_between_travel_info(venue_a, venue_b, travel_cache)
                time_ab = travel_ab.get('drive_time_minutes', 9999)
                if time_ab <= max_pair_gap_minutes:
                    chain.append(show_b)
                    travel_times.append(time_ab)
                    current_date = next_day
                    current_show = show_b
                    found_next = True
                    break  # Only chain to one next show per day

            if not found_next:
                break  # No link in the chain

            if max_shows and len(chain) >= max_shows:
                break  # Hit max chain length if defined

        if len(chain) > 1:  # Only flag chains with at least two shows
            overnights.append({
                "type": "Overnight Suggestion",
                "dates": [s.get("show_date") or s.get("date") for s in chain],
                "shows": [s['show_name'] for s in chain],
                "chain_length": len(chain),
                "between_travel_times": travel_times
            })

    return overnights

def load_wins_log() -> list:
    #Load the wins log JSON file.
    #Returns an empty list if file not found or unreadable.

    if not os.path.isfile(WINS_LOG_FILE):
        print(f"No wins log found at {WINS_LOG_FILE}.")
        return []

    try:
        with open(WINS_LOG_FILE, "r") as f:
            wins = json.load(f)
            if isinstance(wins, list):
                return wins
            else:
                print(f"Warning: {WINS_LOG_FILE} is not a list.")
                return []
    except Exception as e:
        print(f"Error loading {WINS_LOG_FILE}: {e}")
        return []
    
def save_results(results, processed_shows):
    #Save results and caches to local files
    # Sort results by date
    for r in results:
        if r.get('date'):
            try:
                r['_date_obj'] = datetime.datetime.strptime(r['date'], "%Y-%m-%d").date()
            except Exception:
                r['_date_obj'] = None
        else:
            r['_date_obj'] = None
    results.sort(key=lambda x: (x.get('_date_obj') or datetime.date.max))

    # Write JSON and CSV
    with open(RESULTS_JSON, "w") as jf:
        json.dump(results, jf, indent=2, default=str)
    with open(RESULTS_CSV, "w", newline='') as cf:
        writer = csv.writer(cf)
        if results:
            header = [k for k in results[0].keys() if k != '_date_obj']
            writer.writerow(header)
            for r in results:
                row = [r.get(col, "") for col in header]
                writer.writerow(row)

    # Save processed shows cache
    try:
        with open(PROCESSED_SHOWS_FILE, "w") as pf:
            json.dump(sorted(list(processed_shows)), pf, indent=2)
    except Exception as e:
        print(f"Warning: Could not save {PROCESSED_SHOWS_FILE}: {e}")

def upload_to_google_drive():
    #Upload output and cache files to Google Drive using the already-initialised service account
    if not drive_service:
        print("[ERROR] Google Drive client not initialised.")
        return
    if not gdrive_folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID environment variable is not set.")
        return

    def upload_file(file_path, mime_type):
        #Uploads a file to Google Drive in the specified folder
        name = os.path.basename(file_path)
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=False)

        # Check if the file already exists in the folder
        query = f"'{gdrive_folder_id}' in parents and name='{name}'"
        result = drive_service.files().list(q=query, fields="files(id,name)").execute()
        
        if result.get("files"):
            # If the file exists, update it
            file_id = result['files'][0]['id']
            drive_service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[INFO] Updated {name} on Drive.")
        else:
            # If the file doesn't exist, create it
            file_metadata = {
                'name': name,
                'parents': [gdrive_folder_id]
            }
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"[INFO] Uploaded {name} to Drive.")

    try:
        # Upload the files
        upload_file(RESULTS_JSON, "application/json")
        upload_file(RESULTS_CSV, "text/csv")
        upload_file(PROCESSED_SHOWS_FILE, "application/json")
        upload_file(TRAVEL_CACHE_FILE,"application/json")
        upload_file(GOLDEN_RESULTS_FILE, "text/csv")
        upload_file(HIGHAM_LINKS_FILE,"text/plain")
        for pdf_file in Path(".").glob("schedule_*.pdf"):
            upload_file(str(pdf_file), "application/pdf")
        upload_file(ASPX_LINKS, "text/plain")
        if os.path.exists(STORAGE_STATE_FILE):
            upload_file(STORAGE_STATE_FILE, "application/json")

    except Exception as e:
        print(f"[ERROR] Google Drive upload failed: {e}")
        
async def fetch_postal_close_date(show_url: str) -> Optional[datetime.date]:
    #Scrape the postal close date for a show from its main aspx page.
    #Returns a date object if found, else None.
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            context = await browser.new_context(storage_state=STORAGE_STATE_FILE if os.path.exists(STORAGE_STATE_FILE) else None)
            page = await context.new_page()
            await page.goto(show_url, timeout=30000)
            html = await page.content()
            await context.storage_state(path=STORAGE_STATE_FILE)
            await browser.close()

        return parse_postal_close_date_from_html(html)

    except Exception as e:
        print(f"Warning: Failed to fetch postal close date for show {show_url}: {e}")
        return None
        
def parse_postal_close_date_from_html(html: str) -> Optional[datetime.date]:
    # Extract postal or online close date from a modern FosseData show page.
    # Looks for keywords in TDs and parses the following sibling's text as a date.
    soup = BeautifulSoup(html, "html.parser")
    td_elements = soup.find_all("td")
    
    postal_date = None
    online_date = None

    for i, td in enumerate(td_elements[:-1]):  # Stop before last to avoid index error
        label = td.get_text(strip=True).lower()
        next_text = td_elements[i + 1].get_text(strip=True)
        
        match = re.search(r"\d{1,2} \w+ 20\d{2}", next_text)
        if not match:
            continue

        try:
            parsed_date = datetime.datetime.strptime(match.group(), "%d %B %Y").date()
        except ValueError:
            continue

        if "postal entries close" in label:
            postal_date = parsed_date
        elif "online entries close" in label:
            online_date = parsed_date

    # Prefer postal if found, otherwise fall back to online
    if postal_date:
        return postal_date
    elif online_date:
        print("[INFO] Postal close date missing, using online close date instead.")
        return online_date
    else:
        return None
        
def run_golden_scrape():
    scrape_all_results(start_year=2007, output_csv="golden_results.csv")
    
def run_higham_links():
    links = fetch_higham_show_links()
    with open(HIGHAM_LINKS_FILE, "w") as f:
        for url, start, end, close in links:
            f.write(f"{url}\t{start}\t{end}\t{close}\n")
    print(f"[INFO] Saved {len(links)} Higham show links.")
        
async def main_processing_loop(show_list: list):
    global processed_shows
    results = []
    travel_cache = load_travel_cache()  # Load cache at start
    global travel_updated

    for show in show_list:
        show_url = show.get("url")
        if not show_url or show_url in processed_shows:
            continue

        print(f"Processing show: {show.get('show_name')} on {show.get('date')}")

        # === Fetch postal close date ===
        postal_close_date = await fetch_postal_close_date(show_url)

        # === Download schedule via POST to .aspx ===
        safe_id = re.sub(r"[^\w\-]", "_", show_url.split("/")[-1])
        schedule_pdf_path = f"schedule_{safe_id}.pdf"
        pdf_path, venue = download_schedule_via_post(show_url, schedule_pdf_path)

        # --- Travel cache integration ---
        if venue and venue not in travel_cache:
            travel_cache[venue] = {}  # Placeholder for travel data
            travel_updated = True

        if not pdf_path:
            print(f"Skipping {show.get('show_name')} (no schedule PDF)")
            continue

        # === Parse the PDF for Golden info ===
        info = parse_pdf_for_info(pdf_path, show.get("show_name", ""))
        if not info:
            print(f"Skipping {show.get('show_name')} (Golden Retriever not mentioned)")
            continue

        # === Trust title show type unless it's Unknown ===
        if show.get("type", "Unknown") == "Unknown" and "type" in info:
            show["type"] = info["type"]

        # === Travel data ===
        travel_info = get_travel_info(venue, travel_cache) if venue else {}

        result = {
            "show_url": show_url,
            "show_name": show.get("show_name"),
            "show_date": show.get("date").isoformat() if isinstance(show.get("date"), datetime.date) else show.get("date"),
            "type": show.get("type"),
            "judge_dogs": info.get("judge_dogs"),
            "judge_bitches": info.get("judge_bitches"),
            "venue": venue,
            "first_entry_fee": info.get("first_entry_fee"),
            "subsequent_entry_fee": info.get("subsequent_entry_fee"),
            "catalogue_fee": info.get("catalogue_price"),
            "entry_close": postal_close_date.isoformat() if postal_close_date else None,
            "distance_miles": travel_info.get("distance_miles"),
            "duration_hours": travel_info.get("duration_hours"),
            "estimated_cost": travel_info.get("estimated_cost"),
            "overnight_required": travel_info.get("overnight_required"),
            "overnight_cost": travel_info.get("overnight_cost"),
        }

        results.append(result)
        processed_shows.add(show_url)

        if len(results) % 5 == 0:
            # Patch in missing drive time from cache before saving
            for r in results:
                venue = r.get("venue")
                if venue and "drive_time_minutes" not in r:
                    cached = travel_cache.get(venue)
                    if cached and "duration_hours" in cached:
                        r["drive_time_minutes"] = round(cached["duration_hours"] * 60)

            save_results(results, processed_shows)

    # Final patch before last save
    for r in results:
        venue = r.get("venue")
        if venue and "drive_time_minutes" not in r:
            cached = travel_cache.get(venue)
            if cached and "duration_hours" in cached:
                r["drive_time_minutes"] = round(cached["duration_hours"] * 60)

    if travel_updated:
        save_travel_cache(travel_cache)

    upload_to_google_drive()
    print("Processing loop complete.")
    return results

async def full_run():
    run_golden_scrape()
    run_higham_links()
    # Fetch the list of shows
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page()
        show_list = await fetch_show_list(page)
        await browser.close()

    # Process each show
    results = await main_processing_loop(show_list)

    # Save results after all processing
    save_results(results, processed_shows)

    # Detect and write clashes and overnights
    clashes = detect_clashes(results)
    overnights = detect_overnight_pairs(results, load_travel_cache())

    with open(CLASH_OVERNIGHT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Type", "Date", "Show 1", "Show 2", "Chain Length", "Between Travel Times"])
        for c in clashes:
            writer.writerow([c["type"], c["date"], c["show1"], c["show2"], "", ""])
        for o in overnights:
            writer.writerow([
                o["type"],
                ", ".join(o["dates"]),
                o["shows"][0],
                o["shows"][-1],
                o["chain_length"],
                ", ".join(str(t) for t in o["between_travel_times"])
            ])

    return results


if __name__ == "__main__":
    final = asyncio.run(full_run())  # Execute full_run() asynchronously
    print(f"Processed {len(final)} shows.")
