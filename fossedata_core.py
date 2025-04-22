# ---------------------------------------------
# GOOGLE DRIVE SETUP
# ---------------------------------------------
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
from dateutil.parser import parse as date_parse

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

# ---------------------------------------------
# GOOGLE DRIVE UPLOAD / DOWNLOAD
# ---------------------------------------------
def upload_to_drive(local_path, mime_type="application/json"):
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

def download_from_drive(filename):
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    if not folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID is not set.")
        return
    try:
        res = drive_service.files().list(
            q=f"name='{filename}' and trashed=false and '{folder_id}' in parents",
            spaces="drive", fields="files(id, name)"
        ).execute()
        if res["files"]:
            file_id = res["files"][0]["id"]
            request = drive_service.files().get_media(fileId=file_id)
            with open(filename, "wb") as f:
                downloader = MediaFileUpload(filename)
                downloader = build("drive", "v3", credentials=credentials).files().get_media(fileId=file_id)
                request.execute()
            print(f"[INFO] Downloaded {filename} from Drive.")
    except Exception as e:
        print(f"[ERROR] Failed to download {filename} from Drive: {e}")

# ---------------------------------------------
# ENV CONFIG AND CONSTANTS
# ---------------------------------------------
HOME_POSTCODE = os.environ.get("HOME_POSTCODE", "YO8 9NA")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
DOG_DOB = datetime.datetime.strptime(os.environ.get("DOG_DOB", "2024-05-15"), "%Y-%m-%d").date()
DOG_NAME = os.environ.get("DOG_NAME", "Delia")
MPG = float(os.environ.get("MPG", 40))
OVERNIGHT_THRESHOLD_HOURS = float(os.environ.get("OVERNIGHT_THRESHOLD_HOURS", 3))
OVERNIGHT_COST = float(os.environ.get("OVERNIGHT_COST", 100))
CACHE_FILE = "processed_shows.json"
TRAVEL_CACHE_FILE = "travel_cache.json"
CACHE_DIR = "downloaded_pdfs"
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------------------------------------------
# CONTINUED: FULL CLEANED SCRIPT (PRICING EXTRACTION AND COST CALCULATION LOGIC)
# ---------------------------------------------
def extract_entry_pricing(text):
    """Extracts entry pricing and catalogue costs from schedule text. Assumes non-member rates."""
    lines = text.lower().splitlines()
    pricing = {
        "first_entry": None,
        "additional_entry": None,
        "catalogue": None
    }

    for line in lines:
        if "first entry" in line and re.search(r"£?\d+(\.\d{2})?", line):
            match = re.search(r"£?\s*(\d+(\.\d{2})?)", line)
            if match:
                pricing["first_entry"] = float(match.group(1))
        elif ("each subsequent" in line or "subsequent entries" in line) and re.search(r"£?\d+(\.\d{2})?", line):
            match = re.search(r"£?\s*(\d+(\.\d{2})?)", line)
            if match:
                pricing["additional_entry"] = float(match.group(1))
        elif "catalogue" in line and re.search(r"£?\d+(\.\d{2})?", line):
            match = re.search(r"£?\s*(\d+(\.\d{2})?)", line)
            if match:
                pricing["catalogue"] = float(match.group(1))

    # Set defaults if not found
    if pricing["first_entry"] is None:
        pricing["first_entry"] = 5.0
    if pricing["additional_entry"] is None:
        pricing["additional_entry"] = pricing["first_entry"]
    if pricing["catalogue"] is None:
        pricing["catalogue"] = 3.0

    return pricing

def calculate_entry_cost(text, show_type, show_date):
    """Calculates the total entry cost including catalogue, based on class eligibility."""
    pricing = extract_entry_pricing(text)
    eligible_classes = get_eligible_classes(text, show_type, show_date)
    num_classes = len(eligible_classes)

    if num_classes == 0:
        return {
            "entry_cost": 0,
            "catalogue_cost": pricing["catalogue"],
            "total_cost": pricing["catalogue"]
        }

    first_entry = pricing["first_entry"]
    additional_entry = pricing["additional_entry"]
    entry_cost = first_entry + additional_entry * (num_classes - 1)
    total_cost = entry_cost + pricing["catalogue"]

    return {
        "entry_cost": round(entry_cost, 2),
        "catalogue_cost": pricing["catalogue"],
        "total_cost": round(total_cost, 2)
    }

def get_eligible_classes(text, show_type, show_date):
    """Determines eligible classes for Delia based on age and show type."""
    text = text.lower()
    eligible_classes = []
    eligible_codes = ["sbb", "ugb", "tb", "yb"]
    age_in_months = (show_date.year - DOG_DOB.year) * 12 + (show_date.month - DOG_DOB.month)

    if show_date < datetime.date(2025, 5, 15):
        eligible_codes.append("pb")
    if datetime.date(2025, 5, 15) <= show_date < datetime.date(2026, 5, 15):
        eligible_codes.append("jb")

    golden_section = extract_golden_retriever_section(text).lower()

    if show_type.lower() == "championship":
        for line in golden_section.splitlines():
            for code in eligible_codes:
                if re.search(rf'\b{re.escape(code)}\b', line):
                    eligible_classes.append(code)
                    break
    else:  # Open / Premier Open / Limited
        found_mixed = False
        for keyword in ["puppy", "junior", "yearling"]:
            if keyword in golden_section:
                eligible_classes.append(keyword)
                found_mixed = True
        if not found_mixed:
            for line in golden_section.splitlines():
                for code in eligible_codes:
                    if re.search(rf'\b{re.escape(code)}\b', line):
                        eligible_classes.append(code)
                        break

    return list(set(eligible_classes))  # remove duplicates

# ---------------------------------------------
# CONTINUED: SHOW SCRAPING AND PROCESSING LOGIC
# ---------------------------------------------

async def download_schedule_playwright(show_url, processed_shows, travel_cache):
    """Downloads and processes the schedule PDF using Playwright."""
    if is_show_processed(show_url, processed_shows):
        cached = processed_shows[show_url]
        if isinstance(cached, dict) and "pdf" in cached and os.path.exists(cached["pdf"]):
            print(f"[INFO] Skipping {show_url} — already processed.")
            return cached["pdf"], cached
        else:
            print(f"[WARN] Cached file missing or invalid for {show_url}, re-downloading...")

    filename = show_url.split("/")[-1].replace(".aspx", ".pdf")
    local_path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(local_path):
        print(f"[INFO] Skipping {show_url} — already downloaded.")
        return local_path, processed_shows[show_url]

    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            if Path("storage_state.json").exists():
                await load_storage_state(context)
            page = await context.new_page()

            await page.route("**/*", lambda route: route.abort() if any(ext in route.request.url for ext in [".css", ".woff", ".jpg", "analytics"]) else route.continue_())

            try:
                await asyncio.wait_for(page.goto(show_url, wait_until="networkidle"), timeout=30)
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] Page load failed for {show_url}")
                await browser.close()
                return None, None

            await page.evaluate("""() => { const o = document.getElementById('cookiescript_injected_wrapper'); if (o) o.remove(); }""")

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
                    form_data = await page.evaluate("""() => { const data = {}; for (const [k, v] of new FormData(document.querySelector('#aspnetForm'))) { data[k] = v; } return data; }""")
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
                        return None, None
                except Exception as e:
                    print(f"[ERROR] Fallback crash: {e}")
                    await browser.close()
                    return None, None

            # Process PDF
            try:
                text = extract_text_from_pdf(local_path)
                pc = get_postcode(text)
                drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
                cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
                judge = extract_judges(text, is_single_breed="single breed" in text.lower())
                dt = get_show_date(text) or get_show_date_from_title(show_url)
                show_type = get_show_type(text, file_path=local_path)
                points = calculate_jw_points(text, show_type, dt) if dt else 0

                entry_costs = calculate_entry_cost(text, show_type, dt)
                total_cost = (cost or 0) + entry_costs["total_cost"]

                show_data = {
                    "show": show_url,
                    "pdf": local_path,
                    "date": dt.isoformat() if dt else None,
                    "postcode": pc,
                    "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
                    "distance_km": round(drive["distance"], 1) if drive else None,
                    "cost_estimate": round(cost, 2) if cost else None,
                    "diesel_cost": round(cost, 2) if cost else None,
                    "entry_cost": round(entry_costs["entry_cost"], 2) if entry_costs else None,
                    "catalogue_cost": round(entry_costs["catalogue_cost"], 2) if entry_costs else None,
                    "total_cost": round(total_cost, 2) if total_cost else None,
                    "points": points,
                    "judge": judge,
                    "entry_close_postal": entry_close_postal,
                    "entry_close_online": entry_close_online,
                    "show_type": show_type
                }

                processed_shows[show_url] = show_data
                save_processed_shows(processed_shows)
                return local_path, show_data
            except Exception as e:
                print(f"[ERROR] Processing failed after download for {show_url}: {e}")
                processed_shows[show_url] = {"error": str(e)}
                save_processed_shows(processed_shows)
                return None, None

    except Exception as e:
        print(f"[ERROR] Uncaught error for {show_url}: {e}")
        return None, None

# ---------------------------------------------
# CONTINUED: FULL RUN LOOP AND OUTPUT GENERATION
# ---------------------------------------------

async def full_run():
    """Main async runner: fetches shows, processes them, and outputs CSV/JSON every 5 shows."""
    global travel_cache
    travel_cache = load_travel_cache()
    download_from_drive(CACHE_FILE)
    download_from_drive(TRAVEL_CACHE_FILE)

    processed_shows = load_processed_shows()
    urls = fetch_aspx_links()
    results = []
    counter = 0

    for url in urls:
        if is_show_processed(url, processed_shows):
            print(f"[INFO] Skipping {url} — already processed.")
            continue

        try:
            pdf, show_data = await asyncio.wait_for(download_schedule_playwright(url, processed_shows, travel_cache), timeout=120)
        except asyncio.TimeoutError:
            print(f"[TIMEOUT] {url} took too long.")
            processed_shows[url] = {"error": "timeout"}
            save_processed_shows(processed_shows)
            continue

        if not pdf or not show_data:
            continue

        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} — no 'golden' found in schedule text.")
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
        entry_costs = calculate_entry_cost(text, show_type, dt)
        total_cost = (cost or 0) + (entry_costs["total_cost"] if entry_costs else 0)

        result = {
            "show": url,
            "pdf": pdf,
            "date": dt.isoformat() if dt else None,
            "postcode": pc,
            "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "diesel_cost": round(cost, 2) if cost else None,
            "entry_cost": round(entry_costs["entry_cost"], 2) if entry_costs else None,
            "catalogue_cost": round(entry_costs["catalogue_cost"], 2) if entry_costs else None,
            "total_cost": round(total_cost, 2) if total_cost else None,
            "points": points,
            "judge": judge,
            "entry_close_postal": show_data.get("entry_close_postal"),
            "entry_close_online": show_data.get("entry_close_online"),
            "show_type": show_type
        }

        results.append(result)
        processed_shows[url] = result
        counter += 1

        # Update outputs every 5 processed shows
        if counter % 5 == 0:
            print(f"[INFO] Processed {counter} shows — updating outputs...")
            clashes, overnight_pairs = detect_clashes_and_overnight_combos(results)
            save_travel_cache(travel_cache)
            save_processed_shows(processed_shows)
            save_results_to_outputs(results, clashes, overnight_pairs)

    # Final output save
    print("[INFO] Final save of outputs...")
    clashes, overnight_pairs = detect_clashes_and_overnight_combos(results)
    save_travel_cache(travel_cache)
    save_processed_shows(processed_shows)
    save_results_to_outputs(results, clashes, overnight_pairs)

    print(f"[INFO] Completed processing {len(results)} Golden Retriever shows.")

# ---------------------------------------------
# CONTINUED: ASYNC FETCH LINKS AND SCRAPE LOGIC
# ---------------------------------------------
async def fetch_aspx_links():
    """Scrape show page links from the FosseData main listing."""
    print("[INFO] Fetching show page links...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.fossedata.co.uk/shows.aspx", wait_until="networkidle")

        links = []
        anchors = await page.query_selector_all("a")
        for anchor in anchors:
            href = await anchor.get_attribute("href")
            if href and href.endswith(".aspx") and "Shows-To-Enter" not in href:
                links.append(f"https://www.fossedata.co.uk/{href.lstrip('/')}")
        await browser.close()
    print(f"[INFO] Found {len(links)} links.")
    return links

# ---------------------------------------------
# SCHEDULE DOWNLOAD LOGIC
# ---------------------------------------------
async def download_schedule_playwright(show_url, processed_shows):
    if is_show_processed(show_url, processed_shows):
        cached = processed_shows[show_url]
        if "pdf" in cached and os.path.exists(cached["pdf"]):
            print(f"[INFO] Skipping {show_url} — already processed.")
            return cached["pdf"]
        else:
            print(f"[WARN] Cached file missing or invalid for {show_url}, re-downloading...")

    filename = show_url.split("/")[-1].replace(".aspx", ".pdf")
    local_path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(local_path):
        print(f"[INFO] Skipping {show_url} — already downloaded.")
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

            processed_shows[show_url] = {"pdf": local_path}
            save_processed_shows(processed_shows)
            return local_path
    except Exception as e:
        print(f"[ERROR] Uncaught error for {show_url}: {e}")
        return None

# ----------------- CONTINUED NEXT CHUNK -----------------

# ---------------------------------------------
# PRICING EXTRACTION AND COST CALCULATION LOGIC
# ---------------------------------------------

def extract_entry_pricing(text):
    """Extract entry fees and catalogue costs from schedule text (non-member rates assumed)."""
    lines = text.lower().splitlines()
    entry_first, entry_additional, catalogue = None, None, None

    for line in lines:
        if "first entry" in line and re.search(r"£\s*\d+(\.\d{2})?", line):
            m = re.search(r"£\s*(\d+(\.\d{2})?)", line)
            if m:
                entry_first = float(m.group(1))
        elif ("each subsequent" in line or "subsequent entries" in line or "additional entry" in line) and re.search(r"£\s*\d+(\.\d{2})?", line):
            m = re.search(r"£\s*(\d+(\.\d{2})?)", line)
            if m:
                entry_additional = float(m.group(1))
        elif "catalogue" in line and re.search(r"£\s*\d+(\.\d{2})?", line):
            m = re.search(r"£\s*(\d+(\.\d{2})?)", line)
            if m:
                catalogue = float(m.group(1))

    if not entry_first:
        print("[WARN] First entry fee not found — assuming £5.0")
        entry_first = 5.0
    if not entry_additional:
        entry_additional = entry_first
    if not catalogue:
        catalogue = 3.0

    return {"entry_first": entry_first, "entry_additional": entry_additional, "catalogue": catalogue}

# ---------------------------------------------
# ELIGIBLE CLASSES DETECTION
# ---------------------------------------------

def detect_eligible_classes(text, show_type, show_date):
    golden_section = extract_golden_retriever_section(text).lower()
    eligible_codes = ["sbb", "ugb", "tb", "yb"]

    if show_date < datetime.date(2025, 5, 15):
        eligible_codes.append("pb")
    if datetime.date(2025, 5, 15) <= show_date < datetime.date(2026, 5, 15):
        eligible_codes.append("jb")

    mixed_sex_names = ["puppy", "junior", "yearling"]
    match_count = 0

    if show_type.lower() == "championship":
        for line in golden_section.splitlines():
            if any(re.search(rf'\b{code}\b', line) for code in eligible_codes):
                match_count += 1
    else:
        found_mixed = False
        for line in golden_section.splitlines():
            if any(name in line for name in mixed_sex_names):
                match_count += 1
                found_mixed = True
        if not found_mixed:
            for line in golden_section.splitlines():
                if any(re.search(rf'\b{code}\b', line) for code in eligible_codes):
                    match_count += 1

    return match_count

# ---------------------------------------------
# ENTRY COST CALCULATION
# ---------------------------------------------

def calculate_entry_cost(text, show_type, show_date):
    fees = extract_entry_pricing(text)
    eligible_class_count = detect_eligible_classes(text, show_type, show_date)

    if eligible_class_count == 0:
        return {"entry_cost": 0, "catalogue_cost": fees["catalogue"], "total_cost": fees["catalogue"], "breakdown": {}}

    first = fees["entry_first"]
    subsequent = fees["entry_additional"]
    entry_cost = first + subsequent * (eligible_class_count - 1)
    total_cost = entry_cost + fees["catalogue"]

    breakdown = {
        "matched_classes": eligible_class_count,
        "first_entry": first,
        "additional_entries": subsequent,
        "catalogue": fees["catalogue"]
    }

    return {"entry_cost": entry_cost, "catalogue_cost": fees["catalogue"], "total_cost": total_cost, "breakdown": breakdown}

# ---------------------------------------------
# DIESEL COST CALCULATION
# ---------------------------------------------

def estimate_diesel_cost(dist_km):
    round_trip_miles = dist_km * 2 * 0.621371
    price = get_diesel_price()
    gal = round_trip_miles / MPG
    fuel = gal * 4.54609 * price
    return round(fuel, 2)
# ---------------------------------------------
# FULL COST BREAKDOWN (ENTRY FEES + DIESEL)
# ---------------------------------------------

def full_cost_breakdown(dist_km, dur_s, text, show_type, show_date):
    diesel_cost = estimate_diesel_cost(dist_km)
    entry_costs = calculate_entry_cost(text, show_type, show_date)

    total = diesel_cost + entry_costs["total_cost"]

    return {
        "diesel_cost": diesel_cost,
        "entry_cost": round(entry_costs["entry_cost"], 2),
        "catalogue_cost": round(entry_costs["catalogue_cost"], 2),
        "total_cost": round(total, 2),
        "entry_breakdown": entry_costs["breakdown"]
    }

# ---------------------------------------------
# JUNIOR WARRANT (JW) POINT CALCULATION
# ---------------------------------------------

def calculate_jw_points(text, show_type, show_date):
    golden_section = extract_golden_retriever_section(text)
    eligible_codes = ["sbb", "ugb", "tb", "yb"]
    if show_date < datetime.date(2025, 5, 15):
        eligible_codes.append("pb")
    if datetime.date(2025, 5, 15) <= show_date < datetime.date(2026, 5, 15):
        eligible_codes.append("jb")

    points = 0
    class_lines = golden_section.lower().splitlines()
    match_count = sum(1 for line in class_lines if any(re.search(rf'\b{code}\b', line) for code in eligible_codes))

    if show_type.lower() == "championship":
        points = match_count * 3
    elif "open" in show_type.lower() or "premier open" in show_type.lower() or "limit" in show_type.lower():
        points = 1 if match_count > 0 else 0
    return points

# ---------------------------------------------
# OVERNIGHT STAY DETECTION AND CLASH DETECTION
# ---------------------------------------------

def detect_clashes(results):
    clashes = []
    for i, show1 in enumerate(results):
        for j, show2 in enumerate(results):
            if i >= j:
                continue
            if show1["date"] == show2["date"] and show1["postcode"] != show2["postcode"]:
                clashes.append((show1["show"], show2["show"]))
    return clashes

def detect_overnight_pairs(results):
    combos = []
    for i, show1 in enumerate(results):
        for j, show2 in enumerate(results):
            if i >= j:
                continue
            if not show1["date"] or not show2["date"]:
                continue
            days_apart = abs((date_parse(show1["date"]).date() - date_parse(show2["date"]).date()).days)
            if days_apart in [0, 1]:
                duration1 = show1.get("duration_hr", 0)
                duration2 = show2.get("duration_hr", 0)
                if duration1 >= OVERNIGHT_THRESHOLD_HOURS or duration2 >= OVERNIGHT_THRESHOLD_HOURS:
                    combos.append((show1["show"], show2["show"]))
    return combos

# ---------------------------------------------
# PLAYWRIGHT PDF DOWNLOAD + FALLBACK LOGIC
# ---------------------------------------------
async def download_schedule_playwright(show_url, processed_shows):
    if is_show_processed(show_url, processed_shows):
        cached = processed_shows[show_url]
        if isinstance(cached, dict) and "pdf" in cached and os.path.exists(cached["pdf"]):
            print(f"[INFO] Skipping {show_url} — already processed.")
            return cached["pdf"]
        else:
            print(f"[WARN] Cached file missing or invalid for {show_url}, re-downloading...")

    filename = show_url.split("/")[-1].replace(".aspx", ".pdf")
    local_path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(local_path):
        print(f"[INFO] Skipping {show_url} — already downloaded.")
        processed_shows[show_url] = {"pdf": local_path}
        save_processed_shows(processed_shows)
        return local_path

    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.route("**/*", lambda route: route.abort() if any(ext in route.request.url for ext in [".css", ".woff", ".jpg", "analytics"]) else route.continue_())

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

            try:
                text = extract_text_from_pdf(local_path)
                pc = get_postcode(text)
                drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
                cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
                judge = extract_judges(text, is_single_breed="single breed" in text.lower())
                dt = get_show_date(text) or get_show_date_from_title(show_url)
                show_type = get_show_type(text, file_path=local_path)
                points = calculate_jw_points(text, show_type, dt) if dt else 0
                entry_fees = extract_entry_pricing(text, show_type)
                eligible_classes = get_eligible_classes(text, show_type, dt)
                entry_cost_data = calculate_entry_cost(text, show_type, dt)
                total_cost_data = full_cost_breakdown(drive["distance"], drive["duration"], len(eligible_classes), entry_fees) if drive else None

                show_data = {
                    "show": show_url,
                    "pdf": local_path,
                    "date": dt.isoformat() if dt else None,
                    "postcode": pc,
                    "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
                    "distance_km": round(drive["distance"], 1) if drive else None,
                    "cost_estimate": round(cost, 2) if cost else None,
                    "points": points,
                    "judge": judge,
                    "entry_close_postal": entry_close_postal,
                    "entry_close_online": entry_close_online,
                    "show_type": show_type,
                    "diesel_cost": total_cost_data["diesel_cost"] if total_cost_data else None,
                    "entry_cost": total_cost_data["entry_cost"] if total_cost_data else None,
                    "catalogue_cost": total_cost_data["catalogue_cost"] if total_cost_data else None,
                    "total_cost": total_cost_data["total_cost"] if total_cost_data else None
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

# ---------------------------------------------
# MAIN ASYNC FULL RUN LOGIC WITH BATCH SAVING
# ---------------------------------------------
async def full_run():
    global travel_cache
    travel_cache = load_travel_cache()
    download_from_drive("processed_shows.json")
    download_from_drive("travel_cache.json")

    processed_shows = load_processed_shows()
    urls = fetch_aspx_links()
    results = []
    counter = 0

    for url in urls:
        if is_show_processed(url, processed_shows):
            print(f"[INFO] Skipping {url} — already processed.")
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
            print(f"[INFO] Skipping {pdf} — no 'golden' found.")
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
        entry_fees = extract_entry_pricing(text, show_type)
        eligible_classes = get_eligible_classes(text, show_type, dt)
        entry_cost_data = calculate_entry_cost(text, show_type, dt)
        total_cost_data = full_cost_breakdown(drive["distance"], drive["duration"], len(eligible_classes), entry_fees) if drive else None

        result = {
            "show": url,
            "pdf": pdf,
            "date": dt.isoformat() if dt else None,
            "postcode": pc,
            "duration_hr": round(drive["duration"] / 3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "points": points,
            "judge": judge,
            "show_type": show_type,
            "entry_close_postal": processed_shows[url].get("entry_close_postal"),
            "entry_close_online": processed_shows[url].get("entry_close_online"),
            "diesel_cost": total_cost_data["diesel_cost"] if total_cost_data else None,
            "entry_cost": total_cost_data["entry_cost"] if total_cost_data else None,
            "catalogue_cost": total_cost_data["catalogue_cost"] if total_cost_data else None,
            "total_cost": total_cost_data["total_cost"] if total_cost_data else None
        }

        results.append(result)
        processed_shows[url] = result
        counter += 1

        # Save and upload every 5 processed shows
        if counter % 5 == 0:
            save_travel_cache(travel_cache)
            save_processed_shows(processed_shows)
            clashes, overnight_pairs = detect_clashes_and_overnight_combos(results)
            save_results_to_outputs(results, clashes, overnight_pairs)

    # Final save after loop
    save_travel_cache(travel_cache)
    save_processed_shows(processed_shows)
    clashes, overnight_pairs = detect_clashes_and_overnight_combos(results)
    save_results_to_outputs(results, clashes, overnight_pairs)

    if results:
        print(f"[INFO] Finished processing {len(results)} Golden Retriever shows.")
    else:
        print("[INFO] No valid Golden Retriever shows found.")

# ---------------------------------------------
# END OF SCRIPT
# ---------------------------------------------
