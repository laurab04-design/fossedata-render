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

# ===== Load Environment Variables Correctly =====
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
PROCESSED_SHOWS_FILE = "processed_shows.json"
STORAGE_STATE_FILE = "storage_state.json"
RESULTS_CSV = "results.csv"
RESULTS_JSON = "results.json"
ASPX_LINKS = "aspx_links.txt"

download_from_drive("processed_shows.json")
download_from_drive("storage_state.json")
download_from_drive("aspx_links.txt")


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
    
    #Scrape the FosseData 'Shows to Enter' page for all .aspx links and show details.
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
            show_url = f"https://www.fossedata.co.uk/shows{link_tag['href']}"
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

async def download_schedule_for_show(context, show: dict) -> Optional[str]:
    show_url = show.get("url")
    if not show_url:
        return None

    safe_id = re.sub(r"[^\w\-]", "_", show_url.split("/")[-1])
    schedule_pdf_path = f"schedule_{safe_id}.pdf"

    page = await context.new_page()
    try:
        await page.goto(show_url, timeout=60000)
        
        # Wait for the Schedule button and click it while catching the download
        with page.expect_download(timeout=15000) as download_info:
            await page.click("input#ctl00_ContentPlaceHolder_btnDownloadSchedule")
        download = await download_info.value
        await download.save_as(schedule_pdf_path)

        print(f"[INFO] Downloaded schedule: {schedule_pdf_path}")
        return schedule_pdf_path

    except Exception as e:
        print(f"[ERROR] Playwright download failed for {show_url}: {e}")
        return None
    finally:
        await page.close()
        
def parse_pdf_for_info(pdf_path: str) -> Optional[dict]:
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
    #Extract a fee amount using the given regex pattern
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def extract_judges(text: str) -> Tuple[Optional[str], Optional[str]]:
    #Extract judges for Dogs and Bitches from schedule text
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
        
async def main_processing_loop(show_list: list):
    global processed_shows
    results = []

    for show in show_list:
        show_url = show.get("url")
        if not show_url or show_url in processed_shows:
            continue

        print(f"Processing show: {show.get('show_name')} on {show.get('date')}")

        # === Fetch postal close date ===
        postal_close_date = await fetch_postal_close_date(show_url)

        # === Download schedule using Playwright PDF interception ===
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            context = await browser.new_context(
                storage_state=STORAGE_STATE_FILE if os.path.exists(STORAGE_STATE_FILE) else None
            )
            pdf_path = await download_schedule_for_show(context, show)
            await context.storage_state(path=STORAGE_STATE_FILE)
            await browser.close()

        if not pdf_path:
            print(f"Skipping {show.get('show_name')} (no schedule PDF)")
            continue

        # === Parse the PDF for Golden info ===
        info = parse_pdf_for_info(pdf_path)
        if not info:
            print(f"Skipping {show.get('show_name')} (Golden Retriever not mentioned)")
            continue

        result = {
            "show_url": show_url,
            "show_name": show.get("show_name"),
            "show_date": show.get("date").isoformat() if isinstance(show.get("date"), datetime.date) else show.get("date"),
            "type": show.get("type"),
            "judge_dogs": info.get("judge_dogs"),
            "judge_bitches": info.get("judge_bitches"),
            "venue": show.get("venue"),
            "first_entry_fee": info.get("first_entry_fee"),
            "subsequent_entry_fee": info.get("subsequent_entry_fee"),
            "catalogue_fee": info.get("catalogue_price"),
            "entry_close": postal_close_date.isoformat() if postal_close_date else None,
        }

        results.append(result)
        processed_shows.add(show_url)

        if len(results) % 5 == 0:
            save_results(results, processed_shows)

    upload_to_google_drive()
    print("Processing loop complete.")
    return results
    

def extract_text_from_pdf(file_obj) -> str:
    #Extract text from a PDF file object.
    #Tries PyMuPDF first, falls back to pdfplumber.
    
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

async def full_run():
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
    return results


if __name__ == "__main__":
    final = asyncio.run(full_run())  # Execute full_run() asynchronously
    print(f"Processed {len(final)} shows.")
