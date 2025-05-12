# Introducing attempt number # fossedata_core.py

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
import PyPDF2
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

load_dotenv()

def fetch_aspx_links() -> List[str]:
    """
    Scrapes the FosseData shows page for all show URLs (old and new format)
    and saves them to 'aspx_links.txt'.
    Returns a list of full show URLs.
    """
    url = "https://www.fossedata.co.uk/shows.aspx"
    response = requests.get(url)
    show_links = []

    if response.status_code == 200:
        content = response.text

        # Match both legacy and pretty URLs
        classic_links = re.findall(r'href="(/show\.asp\?ShowID=\d+)"', content)
        pretty_links = re.findall(r'href="(/shows/[^"]+\.aspx)"', content)

        all_links = classic_links + pretty_links
        show_links = [f"https://www.fossedata.co.uk{link}" for link in all_links]

        # Save to file
        save_links(show_links)

    return show_links

# ===== Load Environment Variables Correctly =====
DOG_NAME = os.getenv("DOG_NAME")
DOG_DOB = date_parse(os.getenv("DOG_DOB")).date()
MPG = int(os.getenv("MPG"))
MAX_PAIR_GAP_MINUTES = int(os.getenv("MAX_PAIR_GAP_MINUTES"))
google_service_account_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
gdrive_folder_id = os.getenv("GDRIVE_FOLDER_ID")

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
LITERS_PER_GALLON = 4.54609
PUPPY_CUTOFF = DOG_DOB.replace(year=DOG_DOB.year + 1)
JW_CUTOFF = DOG_DOB + datetime.timedelta(days=548)

PROCESSED_SHOWS_FILE = "processed_shows.json"
TRAVEL_CACHE_FILE = "travel_cache.json"
STORAGE_STATE_FILE = "storage_state.json"
RESULTS_CSV = "results.csv"
RESULTS_JSON = "results.json"
CLASH_OVERNIGHT_CSV = "clashes_overnight.csv"
ASPX_LINKS = ("aspx_links.txt")

travel_cache = {}

download_from_drive("processed_shows.json")
download_from_drive("travel_cache.json")
download_from_drive("storage_state.json")
download_from_drive("wins_log.json")
download_from_drive("aspx_links.txt")

# These come in as strings, so we convert:
HANDLER_HAS_CC = os.getenv("HANDLER_HAS_CC", "false").lower() == "true"
DOG_HAS_SGWC = os.getenv("DOG_HAS_SGWC", "false").lower() == "true"
DOG_HAS_GCDS = os.getenv("DOG_HAS_GCDS", "false").lower() == "true"

# These are CSV strings, so we split them:
CC_TRIGGER_WORDS = [w.strip().lower() for w in os.getenv("CC_TRIGGER_WORDS", "").split(",") if w.strip()]
RCC_TRIGGER_WORDS = [w.strip().lower() for w in os.getenv("RCC_TRIGGER_WORDS", "").split(",") if w.strip()]

# ===== Load Cache =====
processed_shows = set()  # <- Always define it, even if the file is missing

if os.path.isfile(PROCESSED_SHOWS_FILE):
    try:
        with open(PROCESSED_SHOWS_FILE, "r") as f:
            data = json.load(f)
            processed_shows = set(data if isinstance(data, list) else data.keys())
    except Exception as e:
        print(f"Warning: Could not load {PROCESSED_SHOWS_FILE}: {e}")
if os.path.isfile(TRAVEL_CACHE_FILE):
    try:
        with open(TRAVEL_CACHE_FILE, "r") as f:
            travel_cache = json.load(f)
    except Exception as e:
        print(f"Warning: Could not load {TRAVEL_CACHE_FILE}: {e}")
        travel_cache = {}

if not isinstance(travel_cache, dict):
    travel_cache = {}

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
    """Read show IDs from aspx_links.txt if present."""
    try:
        with open("aspx_links.txt", "r") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        return []

def save_links(links: set):
    """Save show IDs to aspx_links.txt."""
    with open("aspx_links.txt", "w") as f:
        for link in sorted(links):
            f.write(f"{link}\n")

async def fetch_show_list(page) -> List[dict]:
    """
    Scrapes updated FosseData shows.aspx for all show listings (modern format).
    Extracts name, date, and full URL. Venue no longer available on this page.
    """
    await page.goto("https://www.fossedata.co.uk/shows.aspx", timeout=60000)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    existing_links = set(read_existing_links())
    new_links = set(existing_links)
    shows = []

    forms = soup.select("form[action^='/shows/']")
    for form in forms:
        action = form.get("action", "")
        show_url = f"https://www.fossedata.co.uk{action}"
        if show_url in existing_links:
            continue

        # Find the <h2> with the show name
        name_elem = form.select_one("h2")
        show_name = name_elem.get_text(strip=True) if name_elem else "Unknown Show"

        # Find the first <td> that looks like a date
        td_elems = form.select("td")
        show_date = None
        for td in td_elems:
            match = re.search(r"\b\d{1,2} \w+ 20\d{2}\b", td.text)
            if match:
                try:
                    show_date = datetime.datetime.strptime(match.group(0), "%d %B %Y").date()
                    break
                except Exception:
                    continue

        # Determine show type from name
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
            "venue": "",  # Venue not available anymore
            "type": show_type,
            "url": show_url
        }

        shows.append(show_info)
        new_links.add(show_url)

    save_links(new_links)
    return shows
 
async def download_schedule_for_show(context, show: dict) -> Optional[str]:
    """
    Download the schedule PDF for a given show via Playwright (with POST fallback).
    Returns the local PDF file path, or None if failed.
    """
    # Handle both legacy ID and new vanity URL
    target_url = show.get("url") or show.get("id")
    if not target_url:
        return None

    # Use a clean filename
    safe_id = re.sub(r'[^\w\-]', '_', target_url.split('/')[-1])
    schedule_pdf_path = f"schedule_{safe_id}.pdf"

    try:
        page = await context.new_page()
        await page.goto(target_url, timeout=30000)

        download_link_elem = await page.query_selector("a:text(\"Schedule\")")
        download_link = await download_link_elem.get_attribute("href") if download_link_elem else None

        if download_link:
            download_task = page.wait_for_event("download")
            await page.goto(download_link)
            download = await download_task
            await download.save_as(schedule_pdf_path)
        else:
            try:
                download_task = page.wait_for_event("download")
                download_button = await page.query_selector("text=Download Schedule")
                if download_button:
                    await download_button.click()
                else:
                    fallback_link = await page.query_selector("a[href*='Schedule']")
                    if fallback_link:
                        await fallback_link.click()
                download = await download_task
                await download.save_as(schedule_pdf_path)
            except Exception as e:
                raise e

        await page.close()
        return schedule_pdf_path

    except Exception:
        # POST fallback if Playwright failed
        try:
            if show_id and show_id.isdigit():
                pdf_response = requests.post(
                    "https://www.fossedata.co.uk/downloadSchedule.asp",
                    data={"ShowID": show_id},
                    timeout=15
                )
                if pdf_response.status_code == 200:
                    with open(schedule_pdf_path, "wb") as f:
                        f.write(pdf_response.content)
                    print(f"Used fallback POST to download schedule for {show.get('show_name')}")
                    return schedule_pdf_path
        except Exception as e2:
            print(f"Error downloading schedule for {show.get('show_name')}: {e2}")

    return None
    
def parse_pdf_for_info(pdf_path: str) -> Optional[dict]:
    """
    Extract relevant information from the schedule PDF.
    Includes entry fees, catalogue prices, judges, and whether eligible classes are present.
    """
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

    judge_dogs, judge_bitches = extract_judges(text)
    info["judge_dogs"] = judge_dogs
    info["judge_bitches"] = judge_bitches

    eligible_classes = [
        "Puppy", "Junior", "Yearling", "Special Beginners",
        "Undergraduate", "Tyro", "Novice", "Minor Puppy"
    ]
    info["eligible_classes_found"] = any(cls.lower() in text_lower for cls in eligible_classes)

    return info

def extract_fee(pattern: str, text: str) -> Optional[float]:
    """Extract a fee amount using the given regex pattern."""
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def extract_judges(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract judges for Dogs and Bitches from schedule text."""
    judge_dogs = judge_bitches = None
    judge_section_match = re.search(r"Golden Retriever[^\n]*Dogs?:\s*([^\n]+)", text)
    if judge_section_match:
        line = judge_section_match.group(0)
        if 'Bitches' in line:
            parts = re.split(r"Dogs?:|Bitches:", line)
            judge_dogs = parts[1].strip(' ,;\n') if len(parts) >= 2 else None
            judge_bitches = parts[2].strip(' ,;\n') if len(parts) >= 3 else None
        else:
            judge_dogs = judge_section_match.group(1).strip(' ,;\n')
    else:
        judge_lines = re.findall(r"Judge[^:\n]*:\s*([^\n]+)", text)
        if judge_lines:
            if len(judge_lines) == 1:
                judge_dogs = judge_bitches = judge_lines[0].strip(' ,;\n')
            elif len(judge_lines) >= 2:
                judge_dogs = judge_lines[0].strip(' ,;\n')
                judge_bitches = judge_lines[1].strip(' ,;\n')

    return judge_dogs, judge_bitches
    
def save_results(results, clashes, overnights, travel_cache, processed_shows):
    """Save results and caches to local files."""
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

    # Write clash/overnight CSV
    with open(CLASH_OVERNIGHT_CSV, "w", newline='') as cf:
        writer = csv.writer(cf)
        writer.writerow(["type", "date", "show1", "show2", "travel_time_minutes"])
        for item in clashes + overnights:
            writer.writerow([
                item.get('type', ''),
                item.get('date', item.get('dates', '')),
                item.get('show1', item.get('shows', [])[0] if isinstance(item.get('shows', []), list) else item.get('show1', '')),
                item.get('show2', item.get('shows', [])[1] if isinstance(item.get('shows', []), list) and len(item.get('shows', [])) > 1 else item.get('show2', '')),
                item.get('drive_time_minutes', item.get('between_travel_times', ''))
            ])

    # Save processed shows cache
    try:
        with open(PROCESSED_SHOWS_FILE, "w") as pf:
            json.dump(sorted(list(processed_shows)), pf, indent=2)
    except Exception as e:
        print(f"Warning: Could not save {PROCESSED_SHOWS_FILE}: {e}")

    # Save travel cache
    try:
        with open(TRAVEL_CACHE_FILE, "w") as tf:
            json.dump(travel_cache, tf, indent=2)
    except Exception as e:
        print(f"Warning: Could not save {TRAVEL_CACHE_FILE}: {e}")


def upload_to_google_drive():
    """Upload output and cache files to Google Drive using the already-initialised service account."""
    if not drive_service:
        print("[ERROR] Google Drive client not initialised.")
        return
    if not gdrive_folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID environment variable is not set.")
        return

    def upload_file(file_path, mime_type):
        """Uploads a file to Google Drive in the specified folder."""
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
        upload_file(CLASH_OVERNIGHT_CSV, "text/csv")
        upload_file(TRAVEL_CACHE_FILE, "application/json")
        upload_file(PROCESSED_SHOWS_FILE, "application/json")
        upload_file(ASPX_LINKS, "text/plain")
        if os.path.exists(STORAGE_STATE_FILE):
            upload_file(STORAGE_STATE_FILE, "application/json")

    except Exception as e:
        print(f"[ERROR] Google Drive upload failed: {e}")
        
def get_eligible_classes(
    text: str,
    show_type: str,
    show_date: datetime.date,
    wins_log: List[dict],
    postal_close_date: datetime.date,
    class_exclusions: List[str],
    manual_exclusions: List[str],
    always_include: List[str]
) -> Tuple[List[str], List[str]]:
    """
    Returns two lists:
      - all_eligible: all classes Delia could enter under KC rules
      - to_enter:      same list minus any in class_exclusions+manual_exclusions
    """
    t = text.lower()
    age_mo = (show_date.year - DOG_DOB.year) * 12 + (show_date.month - DOG_DOB.month)

    # ===== Fixed win logic with correct exclusions =====
    valid_wins = filter_wins_for_eligibility(wins_log, postal_close_date)
    first_prizes = sum(
        1 for w in valid_wins
        if w["award"].lower() == "1st"
        and not any(
            ex in w["class"].lower()
            for ex in [
                "minor puppy", "special minor puppy", "puppy", "special puppy",
                "baby puppy",
                "variety", "av", "a.v."
            ]
        )
    )
    cc_count  = sum(1 for w in valid_wins if w["award"] in CC_TRIGGER_WORDS)
    rcc_count = sum(1 for w in valid_wins if w["award"] in RCC_TRIGGER_WORDS)
    got_cc    = (cc_count >= 3) or (cc_count >= 2 and rcc_count >= 5)

    codes = set()

    # ===== Age‐based eligibility =====
    if 4 <= age_mo < 6    and "baby puppy"    in t: codes.add("baby puppy")
    if 6 <= age_mo < 9    and "minor puppy"   in t: codes.add("minor puppy")
    if 6 <= age_mo < 12   and "puppy"         in t: codes.add("puppy")
    if 6 <= age_mo < 18   and "junior"        in t: codes.add("junior")
    if 12 <= age_mo < 24  and "yearling"      in t: codes.add("yearling")

    # ===== Win‐based eligibility =====
    if not got_cc and first_prizes == 0 and "maiden"        in t: codes.add("maiden")
    if not got_cc and first_prizes < 3  and "novice"        in t: codes.add("novice")
    if not got_cc and first_prizes < 3  and "undergraduate" in t: codes.add("undergraduate")
    if not got_cc and first_prizes < 4  and "graduate"      in t: codes.add("graduate")
    if not got_cc and first_prizes < 5  and "post graduate" in t: codes.add("post graduate")
    if not got_cc and first_prizes < 3  and "mid limit"     in t: codes.add("mid limit")
    if not got_cc and first_prizes < 7  and "limit"         in t: codes.add("limit")

    # ===== Always‐open classes =====
    if "open" in t: codes.add("open")

    # ===== Veteran classes =====
    if age_mo >= 84  and "veteran"         in t: codes.add("veteran")
    if age_mo >= 120 and "special veteran" in t: codes.add("special veteran")

    # ===== Special classes =====
    if not HANDLER_HAS_CC and "special beginners" in t: codes.add("special beginners")
    if DOG_HAS_SGWC        and "special working"   in t: codes.add("special working")
    if DOG_HAS_GCDS        and "kc good citizen"  in t: codes.add("kc good citizen")

    # ===== Always‐include overrides =====
    for c in always_include:
        if c.lower() in t:
            codes.add(c.lower())

    # ===== Prepare outputs =====
    all_eligible = sorted(codes)
    exclusions = {c.lower() for c in class_exclusions + manual_exclusions}
    to_enter    = sorted(c for c in codes if c not in exclusions)

    return all_eligible, to_enter
    
def calculate_jw_points(
    show_type: str,
    show_date: datetime.date,
    eligible_classes_count: int,
    jw_cutoff: datetime.date
) -> int:
    """
    Calculate potential Junior Warrant points.

    Rules:
    - Only applies if show_date is before or on the JW cutoff.
    - Championship = 3 points per eligible class.
    - Open / Premier Open  = 1 point per eligible class.
    - Other types = 0.
    """
    if show_date > jw_cutoff:
        return 0
    if eligible_classes_count <= 0:
        return 0

    if show_type == "Championship":
        return 3 * eligible_classes_count
    if show_type in ["Open", "Premier Open"]:
        return eligible_classes_count

    return 0
    
def extract_postcode(text: str) -> Optional[str]:
    """
    Extracts a UK postcode from a given string.
    Matches standard UK postcode formats like YO8 9NA.
    """
    match = re.search(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b", text, flags=re.IGNORECASE)
    return match.group(0).upper() if match else None
    
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
    """
    Detect overnight stay chains:
    - Shows on consecutive days
    - Both >3h from home
    - Consecutive pairs within MAX_PAIR_GAP_MINUTES of each other
    - Allows multi-day chaining
    """
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
    
 
WINS_LOG_FILE = "wins_log.json"

def load_wins_log() -> list:
    """
    Load the wins log JSON file.
    Returns an empty list if file not found or unreadable.
    """
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
        
async def fetch_postal_close_date(show_id: str) -> Optional[datetime.date]:
    """
    Scrape the postal close date for a show from its main aspx page.
    Returns a date object if found, else None.
    """
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            context = await browser.new_context(storage_state=STORAGE_STATE_FILE if os.path.exists(STORAGE_STATE_FILE) else None)
            page = await context.new_page()

            target_url = f"https://www.fossedata.co.uk/show.asp?ShowID={show_id}"
            await page.goto(target_url, timeout=30000)
            html = await page.content()
            await context.storage_state(path=STORAGE_STATE_FILE)
            await browser.close()

        return parse_postal_close_date_from_html(html)

    except Exception as e:
        print(f"Warning: Failed to fetch postal close date for show {show_id}: {e}")
        return None
    
def parse_postal_close_date_from_html(html: str) -> Optional[datetime.date]:
    """Extract postal close date from show page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    postal_date = None
    online_date = None
    
    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        label = cells[0].get_text(strip=True).lower()
        date_text = cells[1].get_text(strip=True)
        date_match = re.search(r"(\d{1,2} \w+ 20\d{2})", date_text)

        if date_match:
            parsed_date = None
            try:
                parsed_date = datetime.datetime.strptime(date_match.group(1), "%d %B %Y").date()
            except ValueError:
                continue

            if "postal entries close" in label:
                postal_date = parsed_date
            elif "online entries close" in label:
                online_date = parsed_date

    # Use postal if available, else fallback to online
    if postal_date:
        return postal_date
    elif online_date:
        print("[INFO] Postal close date missing, using online close date instead.")
        return online_date
    else:
        return None
        
async def main_processing_loop(show_list: list):
    global processed_shows
    """
    Main loop to process shows, check eligibility, calculate costs, JW points, etc.
    """
    wins_log = load_wins_log()
    results = []

    for show in show_list:
        show_id = show.get("id")
        if not show_id or show_id in processed_shows:
            continue

        print(f"Processing show: {show.get('show_name')} on {show.get('date')}")
        
        postal_close_date = await fetch_postal_close_date(show_id)

        # ===== Schedule Download =====
        async def download_and_parse():
            async with async_playwright() as pw:
                browser = await pw.chromium.launch()
                context = await browser.new_context(storage_state=STORAGE_STATE_FILE if os.path.exists(STORAGE_STATE_FILE) else None)
                pdf_path = await download_schedule_for_show(context, show)
                await context.storage_state(path=STORAGE_STATE_FILE)
                await browser.close()
                return pdf_path

        pdf_path = await download_and_parse()  # Await async function
        if not pdf_path:
            print(f"Skipping {show.get('show_name')} (no schedule PDF)")
            continue

        info = parse_pdf_for_info(pdf_path)
        if not info:
            print(f"Skipping {show.get('show_name')} (Golden Retriever not mentioned)")
            continue

        # ===== Eligibility =====
        with open(pdf_path, "rb") as f:
            pdf_text = extract_text_from_pdf(f)

        eligible_all, eligible_to_enter = get_eligible_classes(
            text=pdf_text,
            show_type=show["type"],
            show_date=show["date"],
            wins_log=wins_log,
            postal_close_date=postal_close_date,
            class_exclusions=[],
            manual_exclusions=[],
            always_include=[]
        )

        # ===== JW Points =====
        jw_points = calculate_jw_points(
            show_type=show["type"],
            show_date=show["date"],
            eligible_classes_count=len(eligible_to_enter),
            jw_cutoff=JW_CUTOFF
        )

        # ===== Travel and Cost Calculation =====
        venue = show.get("venue", "")
        travel_info = get_travel_info(venue, travel_cache)
        diesel_cost = calculate_diesel_cost(travel_info.get("distance_miles", 0), diesel_price_per_litre, MPG)

        first_fee = info.get("first_entry_fee") or 0
        subsequent_fee = info.get("subsequent_entry_fee") or 0
        catalogue_fee = info.get("catalogue_price") or 0

        total_entry_fee = first_fee + (max(len(eligible_to_enter) - 1, 0) * subsequent_fee)
        total_cost = total_entry_fee + catalogue_fee + diesel_cost

        # ===== Build Result Row =====
        result = {
            "show_id": show_id,
            "show_name": show.get("show_name"),
            "show_date": show.get("date").isoformat() if isinstance(show.get("date"), datetime.date) else show.get("date"),
            "venue": venue,
            "type": show["type"],
            "judge_dogs": info.get("judge_dogs"),
            "judge_bitches": info.get("judge_bitches"),
            "eligible_classes": ", ".join(eligible_to_enter),
            "jw_points": jw_points,
            "first_entry_fee": first_fee,
            "subsequent_entry_fee": subsequent_fee,
            "catalogue_fee": catalogue_fee,
            "diesel_cost": diesel_cost,
            "total_cost": total_cost,
            "drive_distance_miles": travel_info.get("distance_miles"),
            "drive_time_minutes": travel_info.get("drive_time_minutes")
        }

        results.append(result)
        processed_shows.add(show_id)

        # Periodic save
        if len(results) % 5 == 0:
            save_results(results, [], [], travel_cache, processed_shows)

    # ===== Clashes and Overnights =====
    clashes = detect_clashes(results)
    overnights = detect_overnight_pairs(results, travel_cache)
    save_results(results, clashes, overnights, travel_cache, processed_shows)

    # ===== Upload to Drive =====
    upload_to_google_drive()

    print("Processing loop complete.")
    return results
    
def filter_wins_for_eligibility(wins_log: list, postal_close_date: Optional[datetime.date]) -> list:
    """
    Filters the wins log to only include wins dated before the postal close date.
    If no postal_close_date is given, all wins are considered valid.
    """
    if not postal_close_date:
        return wins_log
    return [
        w for w in wins_log
        if "show_date" in w and date_parse(w["show_date"]).date() <= postal_close_date
    ]
    
    
def extract_text_from_pdf(file_obj) -> str:
    """
    Extract text from a PDF file object.
    Tries PyMuPDF first, falls back to pdfplumber.
    """
    text = ""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=file_obj.read(), filetype="pdf")
        for page in doc:
            text += page.get_text()
        doc.close()
    except Exception:
        try:
            file_obj.seek(0)
            with pdfplumber.open(file_obj) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print(f"Failed to extract text from PDF: {e}")
    return text

def get_travel_info(destination: str, cache: dict) -> dict:
    """
    Get travel distance and duration from home to destination.
    Uses cached results if available.
    """
    if not destination:
        return {"distance_miles": 0, "drive_time_minutes": 0}

    if destination in cache:
        return cache[destination]

    try:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        origin = os.getenv("HOME_POSTCODE")
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
            miles = leg["distance"]["text"].replace(" mi", "")
            minutes = leg["duration"]["value"] // 60
            result = {"distance_miles": float(miles), "drive_time_minutes": int(minutes)}
            cache[destination] = result
            return result
        else:
            print(f"Google Maps API error for {destination}: {data['status']}")
    except Exception as e:
        print(f"Error fetching travel info for {destination}: {e}")

    return {"distance_miles": 0, "drive_time_minutes": 0}
    
def get_between_travel_info(origin: str, destination: str, cache: dict) -> dict:
    """
    Get travel time between two venues. Uses cache if available, fetches if missing.
    """
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

def calculate_diesel_cost(distance_miles: float, price_per_litre: float, mpg: int) -> float:
    """
    Calculates round-trip diesel cost for given distance, diesel price, and mpg.
    """
    if mpg <= 0:
        return 0
    gallons_needed = (distance_miles * 2) / mpg  # Round trip
    litres_needed = gallons_needed * LITERS_PER_GALLON
    return round(litres_needed * price_per_litre, 2)

# Modify full_run to await main_processing_loop
async def full_run():
    """Fetch shows, process them, detect clashes/overnights, save & upload."""
    # 1) fetch the list of shows
    async def _get_shows():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            page = await browser.new_page()
            shows = await fetch_show_list(page)  # Fetch show links and details
            await browser.close()
            return shows

    show_list = await _get_shows()  # Fetch the list of shows

    # 2) process each show
    results = await main_processing_loop(show_list)  # Await async function to process shows

    # 3) detect clashes & overnight chains
    clashes = detect_clashes(results)
    overnights = detect_overnight_pairs(results, travel_cache)

    # 4) save & upload
    save_results(results, clashes, overnights, travel_cache, processed_shows)
    upload_to_google_drive()  # Upload to Google Drive

    return results  # Return processed results

if __name__ == "__main__":
    final = asyncio.run(full_run())  # Execute full_run() asynchronously
    print(f"Processed {len(final)} shows.")
