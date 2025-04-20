import os
import re
import csv
import json
import datetime
import base64
import requests
import pdfplumber
import asyncio
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ———————————————————————————————————————————
# Decode & write Google service account creds
# ———————————————————————————————————————————
creds_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_BASE64")
if creds_b64:
    with open("credentials.json", "wb") as f:
        f.write(base64.b64decode(creds_b64))
else:
    print("GOOGLE_SERVICE_ACCOUNT_BASE64 is not set.")

# Build Drive client
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
credentials = service_account.Credentials.from_service_account_file(
    "credentials.json", scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)

def upload_to_drive(local_path, mime_type):
    fname = os.path.basename(local_path)
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")

    if not os.path.exists(local_path):
        print(f"[ERROR] File not found for upload: {local_path}")
        return

    if not folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID is not set.")
        return

    try:
        # Check if file already exists in the *correct* folder
        res = drive_service.files().list(
            q=f"name='{fname}' and trashed=false and '{folder_id}' in parents",
            spaces="drive",
            fields="files(id, name)"
        ).execute()

        if res["files"]:
            file_id = res["files"][0]["id"]
            drive_service.files().update(
                fileId=file_id,
                media_body=MediaFileUpload(local_path, mimetype=mime_type)
            ).execute()
            print(f"[INFO] Updated {fname} in shared Drive folder.")
        else:
            file = drive_service.files().create(
                body={"name": fname, "parents": [folder_id]},
                media_body=MediaFileUpload(local_path, mimetype=mime_type),
                fields="id, webViewLink"
            ).execute()
            print(f"[INFO] Uploaded {fname} to shared Drive folder.")
            print(f"[LINK] View: {file['webViewLink']}")

    except Exception as e:
        print(f"[ERROR] Failed to upload {fname}: {e}")

# ———————————————————————————————————————————
# Caching functions
# ———————————————————————————————————————————
# Define the cache file to store previously processed shows
CACHE_FILE = "processed_shows.json"

# Load the existing processed shows cache (if any)
def load_processed_shows():
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load processed cache: {e}")
    return {}

# Save the processed show data to the cache
def save_processed_shows(shows_data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(shows_data, f, indent=2)
        print(f"[INFO] Saved cache with {len(shows_data)} shows.")
    except Exception as e:
        print(f"[ERROR] Failed to save processed cache: {e}")

# Function to check if the show has already been processed
def is_show_processed(show_url, processed_shows):
    return show_url in processed_shows and isinstance(processed_shows[show_url], dict)
# -------------------------------------------
# Travel cache functions
# ———————————————————————————————————————————
travel_updated = False  # Global flag to track changes

def load_travel_cache():
    if Path("travel_cache.json").exists():
        try:
            with open("travel_cache.json", "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load travel cache: {e}")
    return {}

def save_travel_cache(cache):
    try:
        with open("travel_cache.json", "w") as f:
            json.dump(cache, f, indent=2)
        print("[INFO] Travel cache saved.")
    except Exception as e:
        print(f"[ERROR] Failed to save travel cache: {e}")

# ———————————————————————————————————————————
# Configuration
# ———————————————————————————————————————————
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
CACHE_FILE = "travel_cache.json"
DOG_DOB = datetime.datetime.strptime(
    os.environ.get("DOG_DOB", "2024-05-15"), "%Y-%m-%d"
)
DOG_NAME = os.environ.get("DOG_NAME", "Delia")
MPG = float(os.environ.get("MPG", 40))
OVERNIGHT_THRESHOLD_HOURS = float(os.environ.get("OVERNIGHT_THRESHOLD_HOURS", 3))
OVERNIGHT_COST = float(os.environ.get("OVERNIGHT_COST", 100))
ALWAYS_INCLUDE_CLASS = os.environ.get("ALWAYS_INCLUDE_CLASS", "").split(",")
CLASS_EXCLUSIONS = [x.strip() for x in os.environ.get("DOG_CLASS_EXCLUSIONS", "").split(",")]
# Define the cache directory to store downloaded PDF files
CACHE_DIR = "downloaded_pdfs"

# Ensure the cache directory exists
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ———————————————————————————————————————————
# Hardcoded Kennel Club breed list (simplified, lowercase, realistic show formats)
# ———————————————————————————————————————————
KC_BREEDS = {
    "affenpinscher", "afghan hound", "airedale terrier", "akita", "alaskan malamute",
    "american cocker spaniel", "australian cattle dog", "australian shepherd",
    "australian silky terrier", "basenji", "basset griffon vendeen", "basset hound",
    "beagle", "bearded collie", "beauceron", "bedlington terrier", "belgian shepherd dog",
    "bernese mountain dog", "bichon frise", "bloodhound", "border collie", "border terrier",
    "borzoi", "boston terrier", "bouvier des flandres", "boxer", "bracco italiano",
    "briard", "brittany", "briquet griffon vendeen", "bull terrier", "bull terrier miniature",
    "bulldog", "bullmastiff", "cairn terrier", "canaan dog", "canadian eskimo dog",
    "cavalier king charles spaniel", "cesky terrier", "chesapeake bay retriever",
    "chihuahua", "chow chow", "clumber spaniel", "cocker spaniel", "collie rough",
    "collie smooth", "curly coated retriever", "dachshund miniature long haired",
    "dachshund miniature smooth haired", "dachshund miniature wire haired",
    "dachshund long haired", "dachshund smooth haired", "dachshund wire haired",
    "dalmatian", "dandie dinmont terrier", "deerhound", "dobermann", "english setter",
    "english springer spaniel", "english toy terrier", "field spaniel", "finnish lapphund",
    "finnish spitz", "flatcoated retriever", "fox terrier", "french bulldog",
    "german pinscher", "german shepherd dog", "german shorthaired pointer",
    "german spitz klein", "german spitz mittel", "glen of imaal terrier", "golden retriever",
    "gordon setter", "great dane", "greyhound", "griffon bruxellois", "hamiltonstovare",
    "harrier", "heeler", "hound", "hovawart", "hungarian kuvasz", "hungarian puli",
    "hungarian vizsla", "hungarian wire haired vizsla", "irish red and white setter",
    "irish setter", "irish terrier", "irish water spaniel", "irish wolfhound",
    "italian greyhound", "jack russell terrier", "japanese akita inu",
    "japanese chin", "japanese shiba inu", "kerry blue terrier", "king charles spaniel",
    "klee kai", "komondor", "kooikerhondje", "kuvasz", "lagotto romagnolo",
    "labrador retriever", "lakeland terrier", "leonberger", "lhasa apso",
    "lowchen", "maltese", "manchester terrier", "mastiff", "miniature pinscher",
    "miniature schnauzer", "neapolitan mastiff", "newfoundland", "norfolk terrier",
    "norwegian buhund", "norwegian elkhound", "norwegian lundehund",
    "norwich terrier", "old english sheepdog", "otterhound", "papillon",
    "parson russell terrier", "pekingese", "perro de presa canario", "pharaoh hound",
    "pointer", "polish lowland sheepdog", "pomeranian", "poodle miniature",
    "poodle standard", "poodle toy", "portuguese podengo", "portuguese water dog",
    "pug", "pyrenean mountain dog", "pyrenean sheepdog", "redbone coonhound",
    "retriever curly coated", "retriever flatcoated", "retriever golden",
    "retriever labrador", "rhodesian ridgeback", "rottweiler", "russian black terrier",
    "saluki", "samoyed", "schipperke", "schnauzer", "scottish terrier",
    "sealyham terrier", "setter english", "setter gordon", "setter irish",
    "setter irish red and white", "shar pei", "shetland sheepdog", "shih tzu",
    "siberian husky", "skye terrier", "sloughi", "soft coated wheaten terrier",
    "spaniel american cocker", "spaniel clumber", "spaniel cocker",
    "spaniel field", "spaniel irish water", "spaniel sussex", "spaniel welsh springer",
    "spaniel tibetan", "spinone italiano", "staffordshire bull terrier", "swedish vallhund",
    "terrier", "thai ridgeback", "tibetan mastiff", "tibetan spaniel",
    "tibetan terrier", "toy", "vizsla hungarian", "vizsla wire haired hungarian",
    "volpino italiano", "weimaraner", "welsh corgi cardigan",
    "welsh corgi pembroke", "welsh springer spaniel", "welsh terrier",
    "west highland white terrier", "whippet", "wire haired fox terrier", "xoloitzcuintle",
    "yorkshire terrier"
}

# ———————————————————————————————————————————
# Playwright storage persistence
# ———————————————————————————————————————————
async def save_storage_state(page, state_file="storage_state.json"):
    storage = await page.context.storage_state()
    with open(state_file, "w") as f:
        json.dump(storage, f)

async def load_storage_state(context, state_file="storage_state.json"):
    if Path(state_file).exists():
        with open(state_file, "r") as f:
            storage = json.load(f)
            if storage.get("cookies"):
                await context.add_cookies(storage["cookies"])
        print(f"[INFO] Loaded storage state from {state_file}")
    else:
        print(f"[INFO] No storage state found, starting fresh.")

# ———————————————————————————————————————————
# PDF text extraction
# ———————————————————————————————————————————
def extract_text_from_pdf(path):
    try:
        with pdfplumber.open(path) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        print(f"[ERROR] PDF extract failed for {path}: {e}")
        return ""

# ———————————————————————————————————————————
# Utilities: postcode, driving, cost, judges, date, points
# ———————————————————————————————————————————
def get_postcode(text):
    m = re.search(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?) ?\d[A-Z]{2}\b", text)
    return m.group(0) if m else None

def get_drive(from_pc, to_pc, cache):
    key = f"{from_pc}_TO_{to_pc}"
    if key in cache:
        return cache[key]
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/distancematrix/json",
            params={
                "origins": from_pc,
                "destinations": to_pc,
                "mode": "driving",
                "key": GOOGLE_MAPS_API_KEY
            }
        ).json()
        e = r["rows"][0]["elements"][0]
        res = {"duration": e["duration"]["value"], "distance": e["distance"]["value"]/1000}
        cache[key] = res
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        return res
    except Exception as e:
        print(f"[ERROR] Travel lookup failed: {e}")
        return None

def get_diesel_price():
    try:
        soup = BeautifulSoup(
            requests.get("https://www.globalpetrolprices.com/diesel_prices/").text,
            "html.parser"
        )
        row = soup.find("td", string=re.compile("United Kingdom")).find_parent("tr")
        return float(row.find_all("td")[2].text.strip().replace("£",""))
    except:
        return 1.60

def estimate_cost(dist_km, dur_s):
    round_trip_miles = dist_km * 2 * 0.621371
    price = get_diesel_price()
    gal = round_trip_miles / MPG
    fuel = gal * 4.54609 * price
    return fuel + OVERNIGHT_COST if dur_s > OVERNIGHT_THRESHOLD_HOURS * 3600 else fuel

def get_show_type(text):
    lines = text.lower().splitlines()
    for line in lines[:20]:  # Top of the file only
        if "championship show" in line:
            return "Championship"
        if "premier open show" in line or "open show" in line:
            return "Open"
    return "Unknown"

def extract_golden_retriever_section(text: str) -> str:
    pattern = r'retriever\s*\(golden\).*?(?=(\n[A-Z][^\n]{0,60}\n|$))'  # stops at next uppercase block
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return match.group(0) if match else text

def extract_judges(text: str, is_single_breed: bool) -> dict:
    judges = {"dogs": None, "bitches": None, "both": None}
    affixes = {"dogs": None, "bitches": None, "both": None}

    def split_judge_affix(raw: str):
        name = raw.strip()
        affix_match = re.search(r'\(([^)]+)\)', name)
        affix = affix_match.group(1).strip() if affix_match else None
        name = re.sub(r'\(.*?\)', '', name).strip()
        name = re.split(r',|\d+|puppy|junior|novice|graduate|post ?graduate|open|limit|yearling', name, flags=re.IGNORECASE)[0].strip()
        return name, affix

    def is_valid_judge_line(line: str) -> bool:
        if len(line.split()) > 12:
            return False
        blacklist = ["refund", "policy", "expenses", "secretary", "schedule", "disqualification"]
        return not any(bad_word in line.lower() for bad_word in blacklist)

    if is_single_breed:
        dog_match = re.search(r'dogs:\s*(.+)', text, re.IGNORECASE)
        bitch_match = re.search(r'bitches:\s*(.+)', text, re.IGNORECASE)
        both_match = re.search(r'judge:\s*(.+)', text, re.IGNORECASE)

        if dog_match and is_valid_judge_line(dog_match.group(1)):
            name, affix = split_judge_affix(dog_match.group(1))
            judges["dogs"] = name
            affixes["dogs"] = affix
        if bitch_match and is_valid_judge_line(bitch_match.group(1)):
            name, affix = split_judge_affix(bitch_match.group(1))
            judges["bitches"] = name
            affixes["bitches"] = affix
        elif both_match and is_valid_judge_line(both_match.group(1)):
            name, affix = split_judge_affix(both_match.group(1))
            judges["both"] = name
            affixes["both"] = affix

    else:
        # Find all lines containing the word "golden"
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if "golden" in line.lower():
                for j in range(i + 1, min(i + 6, len(lines))):
                    next_line = lines[j].strip()
                    if not is_valid_judge_line(next_line):
                        continue
                    # Try to parse a judge name from the line
                    if re.search(r'judge[s]?:', next_line, re.IGNORECASE):
                        match = re.search(r'judge[s]?:\s*(.+)', next_line, re.IGNORECASE)
                        if match:
                            name, affix = split_judge_affix(match.group(1))
                        else:
                            continue
                    else:
                        name, affix = split_judge_affix(next_line)

                    # Assign to appropriate slot
                    if "bitch" in next_line.lower():
                        judges["bitches"] = name
                        affixes["bitches"] = affix
                    elif "dog" in next_line.lower():
                        judges["dogs"] = name
                        affixes["dogs"] = affix
                    elif not judges["both"]:
                        judges["both"] = name
                        affixes["both"] = affix

    return {"names": judges, "affixes": affixes}
def get_show_date(text):
    m = re.search(r"Date Of Show:\s*([A-Za-z]+,\s*\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%A, %d %B %Y").date()
        except:
            return None
    return None

def get_show_date_from_title(aspx_url):
    try:
        r = requests.get(aspx_url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string if soup.title else ""
        m = re.search(r"on\s+([A-Za-z]+,\s+\d{1,2}\s+[A-Za-z]+\s+\d{4})", title)
        if m:
            return datetime.datetime.strptime(m.group(1), "%A, %d %B %Y").date()
    except Exception as e:
        print(f"[WARN] Failed to extract show date from title: {e}")
    return None

def jw_points(text, show_type):
    eligible_classes = ["sbb", "pb", "jb", "ugb", "tb"]
    golden_section = extract_golden_retriever_section(text)

    count = 0
    for line in golden_section.lower().splitlines():
        for cls in eligible_classes:
            if cls in line:
                count += 1
                break

    if show_type.lower() == "championship":
        return count * 3
    elif "open" in show_type.lower():
        return 1 if count > 0 else 0
    return 0
    
def find_clashes_and_combos(results):
    by_date = {}
    missing_dates = []

    for s in results:
        d = s.get("date")
        if not isinstance(d, str):
            missing_dates.append(s.get("show"))
            continue
        by_date.setdefault(d, []).append(s)

    for group in by_date.values():
        if len(group) > 1:
            for s in group:
                s["clash"] = True

    for i, a in enumerate(results):
        if not a.get("postcode") or a.get("duration_hr", 0) <= 3:
            continue
        for b in results[i + 1:]:
            if not b.get("postcode") or b.get("duration_hr", 0) <= 3:
                continue
            if not isinstance(a.get("date"), str) or not isinstance(b.get("date"), str):
                continue
            try:
                da = datetime.datetime.fromisoformat(a["date"])
                db = datetime.datetime.fromisoformat(b["date"])
            except Exception:
                continue
            if abs((da - db).days) == 1:
                inter = get_drive(a["postcode"], b["postcode"], travel_cache)
                if inter and inter["duration"] <= 75 * 60:
                    a.setdefault("combo_with", []).append(b["show"])
                    b.setdefault("combo_with", []).append(a["show"])

    if missing_dates:
        with open("missing_dates.txt", "w") as f:
            f.write("\n".join(missing_dates))
        print(f"[WARN] {len(missing_dates)} shows skipped due to missing or invalid dates.")

def should_include_class(name):
    name_l = name.lower()
    if any(exc.lower() in name_l for exc in CLASS_EXCLUSIONS): return False
    if "golden" in name_l or any(inc.lower() in name_l for inc in ALWAYS_INCLUDE_CLASS):
        return True
    return False

# ———————————————————————————————————————————
# Fetch only proper show links
# ———————————————————————————————————————————
def fetch_aspx_links():
    try:
        print("[INFO] Fetching show links from Shows‑To‑Enter only…")
        r = requests.get("https://www.fossedata.co.uk/shows/Shows-To-Enter.aspx")
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        seen_ids = set()

        EXCLUDED_BREED_TERMS = {
            "terrier", "bull terrier", "border collie", "collie", "pointer",
            "german shorthaired pointer", "weimaraner", "heeler", "mastiff",
            "spaniel", "cocker spaniel", "king charles spaniel", "tibetan spaniel",
            "setter", "english setter", "hound", "toy", "pinscher",
            "flatcoated retriever", "labrador retriever", "bullmastiff",
            "dobermann", "lagotto romagnolo", "bernese mountain dog",
            "japanese shiba inu", "shiba", "fox terrier", "yorkshire terrier",
            "poodle", "akita", "schnauzer", "dachshund", "bulldog"
        }

        for a in soup.select("a[href$='.aspx']"):
            href = a["href"]
            if not href.startswith("/shows/") or href in (
                "/shows/Shows-To-Enter.aspx",
                "/shows/Shows-Starting-Soon.aspx"
            ):
                continue

            full_url = "https://www.fossedata.co.uk" + href
            match = re.search(r'ShowID=(\d+)', full_url)
            if match:
                show_id = match.group(1)
                if show_id in seen_ids:
                    continue
                seen_ids.add(show_id)

            link_text = a.text.lower()
            url_text = href.split("/")[-1].replace(".aspx", "").replace("-", " ").lower()

            # Always include if 'golden' is in the visible link text or the normalised URL
            if "golden" in link_text or "golden" in url_text:
                links.append(full_url)
                continue

            # Create a unified string for matching: cleaned visible text + cleaned URL text
            link_text = a.text.lower().strip()
            url_text = (
                href.split("/")[-1]
                .replace(".aspx", "")
                .replace("-", " ")
                .replace("_", " ")
                .lower()
                .strip()
            )
            text_for_matching = f"{link_text} {url_text}"

            # Check if it matches exactly one non-golden breed
            breed_matches = [breed for breed in KC_BREEDS if breed in text_for_matching]
            if len(breed_matches) == 1 and "golden" not in breed_matches[0]:
                print(f"[INFO] Skipping single-breed show: {link_text.strip()} ({breed_matches[0]})")
                continue

            # Also skip if it matches *any* term in the excluded set
            if any(term in text_for_matching for term in EXCLUDED_BREED_TERMS):
                print(f"[INFO] Skipping based on excluded term: {link_text.strip()}")
                continue

            links.append(full_url)

        print(f"[INFO] Found {len(links)} filtered show links.")
        with open("aspx_links.txt", "w") as f:
            f.write("\n".join(links))
        return links
    except Exception as e:
        print(f"[ERROR] Error fetching ASPX links: {e}")
        return []
# _________________________________________        
# playwright download
# ——————————————————————————————————————————
async def download_schedule_playwright(show_url, processed_shows):
    if is_show_processed(show_url, processed_shows):
        cached_pdf_path = processed_shows[show_url]
        if os.path.exists(cached_pdf_path):
            print(f"[INFO] Skipping {show_url} — already processed.")
            return cached_pdf_path
        else:
            print(f"[WARN] Cached file for {show_url} missing, re-downloading...")

    cache_filename = os.path.join(CACHE_DIR, f"{show_url.split('/')[-1].replace('.aspx', '.pdf')}")

    if os.path.exists(cache_filename):
        print(f"[INFO] Skipping {show_url} - already downloaded.")
        processed_shows[show_url] = cache_filename
        save_processed_shows(processed_shows)
        return cache_filename

    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.route("**/*", lambda route: route.abort() if any(ext in route.request.url for ext in [
                "google-analytics.com", "images", ".css", ".woff2", ".woff", ".js", "trackers"
            ]) else route.continue_())

            if Path("storage_state.json").exists():
                await load_storage_state(page.context)
                print("[INFO] Loaded session state.")

            try:
                await page.goto(show_url, wait_until="networkidle", timeout=20000)
            except Exception as e:
                print(f"[ERROR] Page load timeout for {show_url}: {e}")
                await browser.close()
                return None

            await page.evaluate("""() => {
                const o = document.getElementById('cookiescript_injected_wrapper');
                if (o) o.remove();
            }""")

            # Step 1 — Extract entry close dates from the page
            entry_close_postal = None
            entry_close_online = None
            try:
                rows = await page.query_selector_all("table tr")
                for row in rows:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue
                    key = (await cells[0].inner_text()).strip().lower()
                    val = (await cells[1].inner_text()).strip()

                    if "postal entries close" in key and "closed" not in val.lower():
                        entry_close_postal = val
                    elif "online entries close" in key and "closed" not in val.lower():
                        entry_close_online = val
            except Exception as e:
                print(f"[WARN] Failed to extract entry close dates: {e}")

            # Step 2 — Attempt PDF download via button
            try:
                await page.wait_for_selector("#ctl00_ContentPlaceHolder_btnDownloadSchedule", timeout=5000)
                async with page.expect_download(timeout=10000) as dl:
                    await page.click("#ctl00_ContentPlaceHolder_btnDownloadSchedule")
                download = await dl.value
                fname = download.suggested_filename
                file_path = os.path.join(CACHE_DIR, fname)
                await download.save_as(file_path)
                await save_storage_state(page)
                await browser.close()
                print(f"[INFO] Downloaded: {file_path}")
            except Exception as e:
                print(f"[WARN] Download click failed for {show_url}: {e}")
                print("[INFO] Attempting fallback POST...")

                # Fallback PDF via form POST
                form_data = await page.evaluate("""() => {
                    const data = {};
                    for (const [k,v] of new FormData(document.querySelector('#aspnetForm'))) {
                        data[k] = v;
                    }
                    return data;
                }""")
                resp = await page.context.request.post(show_url, data=form_data)
                ct = resp.headers.get("content-type", "")
                if resp.ok and "application/pdf" in ct:
                    pdfb = await resp.body()
                    fname = show_url.rsplit("/", 1)[-1].replace(".aspx", ".pdf")
                    file_path = os.path.join(CACHE_DIR, fname)
                    with open(file_path, "wb") as f:
                        f.write(pdfb)
                    await save_storage_state(page)
                    await browser.close()
                    print(f"[INFO] Fallback PDF saved: {file_path}")
                else:
                    print(f"[ERROR] Fallback POST failed: {resp.status} {ct}")
                    await browser.close()
                    return None

              try:
                # Step 3 — Extract data from the PDF
                text = extract_text_from_pdf(file_path)
                pc = get_postcode(text)
                drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
                cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
                judge = extract_judges(text, is_single_breed="single breed" in text.lower())
                dt = get_show_date(text)
                show_type = get_show_type(text)

                # Step 4 — Save full show data
                show_data = {
                    "show": show_url,
                    "pdf": file_path,
                    "date": dt.isoformat() if dt else None,
                    "postcode": pc,
                    "duration_hr": round(drive["duration"]/3600, 2) if drive else None,
                    "distance_km": round(drive["distance"], 1) if drive else None,
                    "cost_estimate": round(cost, 2) if cost else None,
                    "points": jw_points(text, show_type),
                    "judge": judge,
                    "entry_close_postal": entry_close_postal,
                    "entry_close_online": entry_close_online,
                }

                processed_shows[show_url] = show_data
                save_processed_shows(processed_shows)
                return file_path

            except Exception as e:
                print(f"[ERROR] Playwright crashed entirely for {show_url}: {e}")
                print(f"[ERROR] Failed to process {show_url}: {e}")
                processed_shows[show_url] = {"error": str(e)}
                save_processed_shows(processed_shows)
                return None

# ———————————————————————————————————————————
# full_run orchestrator
# ———————————————————————————————————————————

# Save the processed show data to the cache
def save_processed_shows(shows_data):
    try:
        with open("processed_shows.json", "w") as f:
            json.dump(shows_data, f, indent=2)
        print(f"[INFO] Saved cache with {len(shows_data)} shows.")
        print(f"[DEBUG] Writing to: {os.path.abspath('processed_shows.json')}")
        print(f"[DEBUG] File exists after write? {Path('processed_shows.json').exists()}")
    except Exception as e:
        print(f"[ERROR] Failed to save processed cache: {e}")


# Download a file from Google Drive
def download_from_drive(filename, mime_type="application/json"):
    try:
        folder_id = os.environ.get("GDRIVE_FOLDER_ID")
        if not folder_id:
            print("[ERROR] GDRIVE_FOLDER_ID not set for download.")
            return

        res = drive_service.files().list(
            q=f"name='{filename}' and trashed=false and '{folder_id}' in parents",
            spaces="drive",
            fields="files(id, name)"
        ).execute()

        if not res["files"]:
            print(f"[INFO] {filename} not found in Drive.")
            return

        file_id = res["files"][0]["id"]
        request = drive_service.files().get_media(fileId=file_id)
        fh = open(filename, "wb")
        fh.write(request.execute())
        fh.close()
        print(f"[INFO] Downloaded {filename} from Drive.")

    except Exception as e:
        print(f"[ERROR] Failed to download {filename}: {e}")


# Main async orchestration
async def full_run():
    global travel_cache

    urls = fetch_aspx_links()
    if not urls:
        print("[WARN] No show URLs found.")
        return []

    travel_cache = {}
    download_from_drive("travel_cache.json")
    if Path("travel_cache.json").exists():
        with open("travel_cache.json", "r") as f:
            travel_cache = json.load(f)

    # Restore the last known processed_shows cache
    download_from_drive("processed_shows.json")

    processed_shows = load_processed_shows()
    shows = []
    
    for url in urls:
        if is_show_processed(url, processed_shows):
            print(f"[INFO] Skipping {url} — already processed (early skip).")
            continue

        try:
            pdf = await asyncio.wait_for(download_schedule_playwright(url, processed_shows), timeout=60)
        except asyncio.TimeoutError:
            print(f"[TIMEOUT] Skipping {url} — download took too long.")
            processed_shows[url] = {"error": "timeout"}
            save_processed_shows(processed_shows)
            upload_to_drive("processed_shows.json", "application/json")
            continue

        if not pdf:
            continue

        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} — no 'golden'")
            processed_shows[url] = pdf
            save_processed_shows(processed_shows)
            upload_to_drive("processed_shows.json", "application/json")
            continue

        pc = get_postcode(text)
        drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
        cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
        judge = extract_judges(text, is_single_breed="single breed" in text.lower())
        dt = get_show_date(text) or get_show_date_from_title(url)
        show_type = get_show_type(text)

        shows.append({
            "show": url,
            "pdf": pdf,
            "date": dt.isoformat() if dt else None,
            "postcode": pc,
            "duration_hr": round(drive["duration"]/3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "show_type": show_type,
            "points": jw_points(text, show_type),
            "judge": judge,
        })
        
    if len(shows) % 30 == 0:
        save_travel_cache(travel_cache)
        upload_to_drive("travel_cache.json", "application/json")
        save_processed_shows(processed_shows)
        upload_to_drive("processed_shows.json", "application/json")

    find_clashes_and_combos(shows)

    # Final upload of processed cache no matter what
    save_processed_shows(processed_shows)
    upload_to_drive("processed_shows.json", "application/json")

    if shows:
        with open("results.json", "w") as f:
            json.dump(shows, f, indent=2)

        # Write results to CSV
        with open("results.csv", "w", newline="") as cf:
            w = csv.writer(cf)
            # Write header row
            w.writerow([
                "Show", "Date", "Postcode", "Show Type", 
                "Postal Close", "Online Close", 
                "Distance (km)", "Time (hr)", 
                "Estimated Cost", "JW Points", 
                "Golden Judge(s)", "Judge Affix(es)", "Clash", "Combos"
            ])
            for s in shows:
                # Flatten the 'judge' dictionary
                judge_data = s.get("judge", {})
                judges_flat = {
                    "dogs": judge_data.get("dogs", ""),
                    "bitches": judge_data.get("bitches", ""),
                    "both": judge_data.get("both", ""),
                }

                # Flatten 'judge' and 'affix' data
                jt = ", ".join(f"{k}: {v}" for k, v in judges_flat.items() if v)

                # Flatten 'judge_affix' if available
                judge_affix_data = s.get("judge_affix", {})
                affixes_flat = {
                    "dogs": judge_affix_data.get("dogs", ""),
                    "bitches": judge_affix_data.get("bitches", ""),
                    "both": judge_affix_data.get("both", ""),
                }

                at = ", ".join(f"{k}: {v}" for k, v in affixes_flat.items() if v)

                # Add combo shows
                combos = "; ".join(s.get("combo_with", []))

                # Write the row to the CSV file
                w.writerow([
                    s["show"], s["date"], s["postcode"], s.get("show_type", "") or "",
                    s.get("entry_close_postal", "") or "",
                    s.get("entry_close_online", "") or "",
                    s.get("distance_km"), s.get("duration_hr"),
                    s.get("cost_estimate"), s["points"],
                    jt, at, "Yes" if s.get("clash") else "", combos
                ])

        upload_to_drive("results.json", "application/json")
        upload_to_drive("results.csv", "text/csv")
    else:
        print("[INFO] No Golden Retriever shows processed — skipping results upload.")

    if travel_updated:
        save_travel_cache(travel_cache)
        upload_to_drive("travel_cache.json", "application/json")

    print(f"[INFO] Processed {len(shows)} shows with Golden Retriever classes.")
    return shows
