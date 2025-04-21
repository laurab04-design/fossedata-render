import os
import re
import csv
import json
import base64
import requests
import datetime
import asyncio
import pdfplumber
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from PyPDF2 import PdfReader

# Decode and write service account credentials
creds_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_BASE64")
if creds_b64:
    with open("credentials.json", "wb") as f:
        f.write(base64.b64decode(creds_b64))
else:
    print("GOOGLE_SERVICE_ACCOUNT_BASE64 is not set.")

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
credentials = service_account.Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
drive_service = build("drive", "v3", credentials=credentials)

def upload_to_drive(local_path, mime_type):
    fname = os.path.basename(local_path)
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    if not os.path.exists(local_path):
        print(f"[ERROR] File not found: {local_path}")
        return
    if not folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID is not set.")
        return
    try:
        res = drive_service.files().list(
            q=f"name='{fname}' and trashed=false and '{folder_id}' in parents",
            spaces="drive", fields="files(id, name)"
        ).execute()
        if res["files"]:
            file_id = res["files"][0]["id"]
            drive_service.files().update(
                fileId=file_id,
                media_body=MediaFileUpload(local_path, mimetype=mime_type)
            ).execute()
            print(f"[INFO] Updated {fname} in Drive.")
        else:
            file = drive_service.files().create(
                body={"name": fname, "parents": [folder_id]},
                media_body=MediaFileUpload(local_path, mimetype=mime_type),
                fields="id, webViewLink"
            ).execute()
            print(f"[INFO] Uploaded {fname} to Drive.")
            print(f"[LINK] View: {file['webViewLink']}")
    except Exception as e:
        print(f"[ERROR] Drive upload failed: {e}")


# âââââââââââââââââââââââââââââââââââââââââââ
# Cache loading and saving
# âââââââââââââââââââââââââââââââââââââââââââ
CACHE_FILE = "processed_shows.json"

def load_processed_shows():
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load processed cache: {e}")
    return {}

def save_processed_shows(shows_data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(shows_data, f, indent=2)
        print(f"[INFO] Saved cache with {len(shows_data)} shows.")
    except Exception as e:
        print(f"[ERROR] Failed to save processed cache: {e}")

def is_show_processed(show_url, processed_shows):
    return show_url in processed_shows and isinstance(processed_shows[show_url], dict)

# âââââââââââââââââââââââââââââââââââââââââââ
# Env config
# âââââââââââââââââââââââââââââââââââââââââââ
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
DOG_DOB = datetime.datetime.strptime(os.environ.get("DOG_DOB", "2024-05-15"), "%Y-%m-%d").date()
DOG_NAME = os.environ.get("DOG_NAME", "Delia")
MPG = float(os.environ.get("MPG", 40))
OVERNIGHT_THRESHOLD_HOURS = float(os.environ.get("OVERNIGHT_THRESHOLD_HOURS", 3))
OVERNIGHT_COST = float(os.environ.get("OVERNIGHT_COST", 100))
ALWAYS_INCLUDE_CLASS = os.environ.get("ALWAYS_INCLUDE_CLASS", "").split(",")
CLASS_EXCLUSIONS = [x.strip() for x in os.environ.get("DOG_CLASS_EXCLUSIONS", "").split(",")]
CACHE_DIR = "downloaded_pdfs"
os.makedirs(CACHE_DIR, exist_ok=True)


# âââââââââââââââââââââââââââââââââââââââââââ
# Travel cache
# âââââââââââââââââââââââââââââââââââââââââââ
travel_updated = False

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

# âââââââââââââââââââââââââââââââââââââââââââ
# JW point logic
# âââââââââââââââââââââââââââââââââââââââââââ
def calculate_jw_points(text, show_type, show_date):
    if not text or not show_type:
        return 0

    golden_section = extract_golden_retriever_section(text)
    eligible_codes = ["sbb", "ugb", "tb", "yb"]
    points = 0

    if show_date < datetime.date(2025, 5, 15):
        eligible_codes.append("pb")
    if datetime.date(2025, 5, 15) <= show_date < datetime.date(2026, 5, 15):
        eligible_codes.append("jb")

    class_lines = golden_section.lower().splitlines()
    match_count = 0
    for line in class_lines:
        for code in eligible_codes:
            if re.search(rf'\b{re.escape(code)}\b', line):
                match_count += 1
                break

    if show_type.lower() == "championship":
        return match_count * 3
    elif "open" in show_type.lower():
        return 1 if match_count > 0 else 0
    return 0

# âââââââââââââââââââââââââââââââââââââââââââ
# Show type extraction
# âââââââââââââââââââââââââââââââââââââââââââ
def get_show_type(text, file_path=None):
    lines = text.lower().splitlines()
    for line in lines[:20]:
        if "championship" in line:
            return "Championship"
        if "premier open" in line or "open show" in line:
            return "Open"
        if "limit" in line or "limited show" in line:
            return "Limit"

    if file_path:
        try:
            reader = PdfReader(file_path)
            first_page = reader.pages[0].extract_text().lower()
            if "championship" in first_page:
                return "Championship"
            if "premier open" in first_page or "open show" in first_page:
                return "Open"
            if "limit" in first_page or "limited show" in first_page:
                return "Limit"
        except Exception as e:
            print(f"[WARN] get_show_type fallback failed: {e}")
    return "Unknown"

def extract_golden_retriever_section(text: str) -> str:
    pattern = r'retriever\s*\(golden\).*?(?=(\n[A-Z][^\n]{0,60}\n|$))'
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return match.group(0) if match else text


def extract_text_from_pdf(path):
    try:
        with pdfplumber.open(path) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        print(f"[ERROR] PDF extract failed for {path}: {e}")
        return ""

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
        save_travel_cache(cache)
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
        return float(row.find_all("td")[2].text.strip().replace("Â£", ""))
    except:
        return 1.60

def estimate_cost(dist_km, dur_s):
    round_trip_miles = dist_km * 2 * 0.621371
    price = get_diesel_price()
    gal = round_trip_miles / MPG
    fuel = gal * 4.54609 * price
    return fuel + OVERNIGHT_COST if dur_s > OVERNIGHT_THRESHOLD_HOURS * 3600 else fuel


def extract_judges(text: str, is_single_breed: bool) -> dict:
    judges = {"dogs": None, "bitches": None, "both": None}
    affixes = {"dogs": None, "bitches": None, "both": None}

    def split_judge_affix(raw: str):
        name = raw.strip()
        affix_match = re.search(r'\(([^)]+)\)', name)
        affix = affix_match.group(1).strip() if affix_match else None
        name = re.sub(r'\(.*?\)', '', name).strip()
        name = re.split(r',|\d+|puppy|junior|novice|graduate|post ?graduate|open|limit|yearling',
                        name, flags=re.IGNORECASE)[0].strip()
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
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if "golden" in line.lower():
                context = lines[max(0, i - 2): i + 6]
                for ctx_line in context:
                    ctx_line = ctx_line.strip()
                    if not is_valid_judge_line(ctx_line):
                        continue
                    if re.search(r'judge[s]?:', ctx_line, re.IGNORECASE):
                        match = re.search(r'judge[s]?:\s*(.+)', ctx_line, re.IGNORECASE)
                        judge_raw = match.group(1) if match else None
                    else:
                        judge_raw = ctx_line
                    if not judge_raw:
                        continue
                    name, affix = split_judge_affix(judge_raw)
                    if "bitch" in ctx_line.lower():
                        judges["bitches"] = name
                        affixes["bitches"] = affix
                    elif "dog" in ctx_line.lower():
                        judges["dogs"] = name
                        affixes["dogs"] = affix
                    elif not judges["both"]:
                        judges["both"] = name
                        affixes["both"] = affix

    return {"names": judges, "affixes": affixes}

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


async def download_schedule_playwright(show_url, processed_shows):
    if is_show_processed(show_url, processed_shows):
        cached = processed_shows[show_url]
        if isinstance(cached, dict) and "pdf" in cached and os.path.exists(cached["pdf"]):
            print(f"[INFO] Skipping {show_url} â already processed.")
            return cached["pdf"]
        else:
            print(f"[WARN] Cached file missing or invalid for {show_url}, re-downloading...")

    filename = show_url.split("/")[-1].replace(".aspx", ".pdf")
    local_path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(local_path):
        print(f"[INFO] Skipping {show_url} â already downloaded.")
        processed_shows[show_url] = {"pdf": local_path}
        save_processed_shows(processed_shows)
        return local_path

    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.route("**/*", lambda route: route.abort()
                             if any(ext in route.request.url for ext in [".css", ".woff", ".jpg", "analytics"])
                             else route.continue_())

            if Path("storage_state.json").exists():
                await load_storage_state(page.context)

            try:
                await asyncio.wait_for(page.goto(show_url, wait_until="networkidle"), timeout=30)
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] Page load failed for {show_url}")
                await browser.close()
                return None

            await page.evaluate("""() => {
                const o = document.getElementById('cookiescript_injected_wrapper');
                if (o) o.remove();
            }""")

            # Extract entry close dates
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
                print(f"[WARN] Failed to parse entry close table: {e}")


            # Try PDF download
            try:
                await page.wait_for_selector("#ctl00_ContentPlaceHolder_btnDownloadSchedule", timeout=5000)
                async with page.expect_download(timeout=10000) as dl:
                    await page.click("#ctl00_ContentPlaceHolder_btnDownloadSchedule")
                download = await dl.value
                await download.save_as(local_path)
                await save_storage_state(page)
                await browser.close()
                print(f"[INFO] Downloaded: {local_path}")
            except Exception as e:
                print(f"[WARN] Button download failed: {e}")
                print("[INFO] Attempting fallback POST...")
                try:
                    form_data = await page.evaluate("""() => {
                        const data = {};
                        for (const [k, v] of new FormData(document.querySelector('#aspnetForm'))) {
                            data[k] = v;
                        }
                        return data;
                    }""")

                    resp = await page.context.request.post(show_url, data=form_data)
                    if resp.ok and "application/pdf" in resp.headers.get("content-type", ""):
                        content = await resp.body()
                        with open(local_path, "wb") as f:
                            f.write(content)
                        await save_storage_state(page)
                        await browser.close()
                        print(f"[INFO] Fallback PDF saved: {local_path}")
                    else:
                        print(f"[ERROR] Fallback POST failed: {resp.status}")
                        await browser.close()
                        return None
                except Exception as e:
                    print(f"[ERROR] Fallback crash: {e}")
                    await browser.close()
                    return None

            # Extract and cache
            try:
                text = extract_text_from_pdf(local_path)
                pc = get_postcode(text)
                drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
                cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
                judge = extract_judges(text, is_single_breed="single breed" in text.lower())
                dt = get_show_date(text) or get_show_date_from_title(show_url)
                show_type = get_show_type(text, file_path=local_path)
                points = calculate_jw_points(text, show_type, dt) if dt else 0

                show_data = {
                    "show": show_url,
                    "pdf": local_path,
                    "date": dt.isoformat() if dt else None,
                    "postcode": pc,
                    "duration_hr": round(drive["duration"]/3600, 2) if drive else None,
                    "distance_km": round(drive["distance"], 1) if drive else None,
                    "cost_estimate": round(cost, 2) if cost else None,
                    "points": points,
                    "judge": judge,
                    "entry_close_postal": entry_close_postal,
                    "entry_close_online": entry_close_online,
                    "show_type": show_type
                }

                processed_shows[show_url] = show_data
                save_processed_shows(processed_shows)
                return local_path
            except Exception as e:
                print(f"[ERROR] Final crash after download: {e}")
                processed_shows[show_url] = {"error": str(e)}
                save_processed_shows(processed_shows)
                return None

    except Exception as e:
        print(f"[ERROR] Uncaught error for {show_url}: {e}")
        return None


async def full_run():
    global travel_cache
    travel_cache = load_travel_cache()
    download_from_drive("processed_shows.json")
    download_from_drive("travel_cache.json")

    processed_shows = load_processed_shows()
    urls = fetch_aspx_links()
    results = []

    for url in urls:
        if is_show_processed(url, processed_shows):
            print(f"[INFO] Skipping {url} â already processed.")
            continue

        try:
            pdf = await asyncio.wait_for(download_schedule_playwright(url, processed_shows), timeout=90)
        except asyncio.TimeoutError:
            print(f"[TIMEOUT] {url} took too long.")
            processed_shows[url] = {"error": "timeout"}
            save_processed_shows(processed_shows)
            continue

        if not pdf:
            continue

        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} â no 'golden'")
            processed_shows[url] = {"pdf": pdf}
            save_processed_shows(processed_shows)
            continue

        pc = get_postcode(text)
        drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
        cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
        judge = extract_judges(text, is_single_breed="single breed" in text.lower())
        dt = get_show_date(text) or get_show_date_from_title(url)
        show_type = get_show_type(text, file_path=pdf)
        points = calculate_jw_points(text, show_type, dt) if dt else 0

        results.append({
            "show": url,
            "pdf": pdf,
            "date": dt.isoformat() if dt else None,
            "postcode": pc,
            "duration_hr": round(drive["duration"]/3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "points": points,
            "judge": judge,
            "show_type": show_type
        })

    save_travel_cache(travel_cache)
    save_processed_shows(processed_shows)
    upload_to_drive("processed_shows.json", "application/json")
    upload_to_drive("travel_cache.json", "application/json")

    if results:
        with open("results.json", "w") as f:
            json.dump(results, f, indent=2)
        upload_to_drive("results.json", "application/json")
        print(f"[INFO] Processed {len(results)} Golden Retriever shows.")
    else:
        print("[INFO] No valid Golden shows processed.")
