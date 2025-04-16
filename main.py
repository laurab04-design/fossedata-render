import os
import re
import requests
import json
import time
import datetime
import fitz  # PyMuPDF
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# === CONFIGURATION ===
HOME_POSTCODE = os.environ.get("HOME_POSTCODE")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
DRIVE_FOLDER_NAME = "FosseData Automation"

# === SETUP ===
with open("fossedata_links.txt") as f:
    show_urls = [line.strip() for line in f if line.strip()]

# === LOAD CACHE ===
CACHE_FILE = "travel_cache.json"
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        travel_cache = json.load(f)
else:
    travel_cache = {}

# === DRIVE SETUP ===
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = 'credentials.json'
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

def ensure_drive_folder(folder_name):
    results = drive_service.files().list(q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'",
                                         spaces='drive', fields="files(id, name)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    else:
        folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
        folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')

drive_folder_id = ensure_drive_folder(DRIVE_FOLDER_NAME)

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

def get_postcode_from_text(text):
    match = re.search(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?) ?\d[A-Z]{2}\b", text)
    return match.group(1) if match else None

def get_drive_time(from_postcode, to_postcode):
    cache_key = f"{from_postcode}_TO_{to_postcode}"
    if cache_key in travel_cache:
        return travel_cache[cache_key]

    try:
        url = f"https://maps.googleapis.com/maps/api/distancematrix/json"
        params = {
            "origins": from_postcode,
            "destinations": to_postcode,
            "mode": "driving",
            "key": GOOGLE_MAPS_API_KEY,
        }
        r = requests.get(url, params=params)
        data = r.json()
        duration = data['rows'][0]['elements'][0]['duration']['value']  # seconds
        distance_km = data['rows'][0]['elements'][0]['distance']['value'] / 1000  # in km
        travel_cache[cache_key] = {
            "seconds": duration,
            "km": distance_km
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(travel_cache, f)
        return travel_cache[cache_key]
    except Exception as e:
        print(f"Travel time lookup failed: {e}")
        return None

def upload_to_drive(local_path, folder_id):
    file_metadata = {
        "name": os.path.basename(local_path),
        "parents": [folder_id]
    }
    media = MediaFileUpload(local_path, resumable=True)
    file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return file.get("id")

def get_schedule_metadata(text):
    metadata = {}
    metadata["judges"] = []
    metadata["classes"] = []
    for line in text.splitlines():
        if "Retriever (Golden)" in line:
            metadata["golden"] = True
        if re.search(r"Judge[:\-]", line, re.IGNORECASE):
            metadata["judges"].append(line.strip())
        if any(word in line for word in ["Minor", "Junior", "Graduate", "Special Beginners", "Limit", "Open"]):
            metadata["classes"].append(line.strip())
        if "Entries Close" in line or "Entry Closes" in line:
            date_match = re.search(r"(\d{1,2} \w+ 20\d\d)", line)
            if date_match:
                metadata["entries_close"] = date_match.group(1)
    return metadata

def jw_points_possible(text):
    if "Open Show" in text:
        return 1
    if "Championship Show" in text:
        return 9
    return 0

def estimate_cost(distance_km, travel_time_sec):
    miles = distance_km * 0.621371
    fuel_cost_per_litre = get_average_diesel_price()
    gallons = miles / 40
    diesel_cost = gallons * 4.54609 * fuel_cost_per_litre
    accommodation_cost = 100 if travel_time_sec > 3 * 3600 else 0
    return diesel_cost + accommodation_cost

def get_average_diesel_price():
    try:
        r = requests.get("https://www.globalpetrolprices.com/diesel_prices/")
        soup = BeautifulSoup(r.text, "html.parser")
        uk_row = soup.find("td", string=re.compile("United Kingdom")).find_parent("tr")
        price = uk_row.find_all("td")[2].text.strip().replace("£", "")
        return float(price)
    except:
        return 1.60  # fallback

results = []
for url in show_urls:
    schedule_filename = download_schedule(url)
    if not schedule_filename:
        continue

    text = extract_text_from_pdf(schedule_filename)
    if "Retriever (Golden)" not in text and "retriever (golden)" not in text.lower():
        continue

    metadata = get_schedule_metadata(text)
    postcode = get_postcode_from_text(text)
    if postcode:
        travel = get_drive_time(HOME_POSTCODE, postcode)
    else:
        travel = None

    jw_pts = jw_points_possible(text)
    est_cost = estimate_cost(travel["km"], travel["seconds"]) if travel else "?"
    file_id = upload_to_drive(schedule_filename, drive_folder_id)

    results.append({
        "show": url,
        "file": schedule_filename,
        "google_drive_id": file_id,
        "postcode": postcode,
        "travel_time_hr": round(travel["seconds"] / 3600, 2) if travel else None,
        "distance_km": round(travel["km"], 1) if travel else None,
        "estimated_cost": round(est_cost, 2) if isinstance(est_cost, float) else "?",
        "points_possible": jw_pts,
        "entries_close": metadata.get("entries_close"),
        "classes": metadata["classes"],
        "judges": metadata["judges"],
    })

with open("results.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"Completed. Saved {len(results)} filtered show entries.")
