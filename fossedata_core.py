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
    res = drive_service.files().list(
        q=f"name='{fname}' and trashed=false",
        spaces="drive",
        fields="files(id)"
    ).execute()
    if res["files"]:
        file_id = res["files"][0]["id"]
        drive_service.files().update(
            fileId=file_id,
            media_body=MediaFileUpload(local_path, mimetype=mime_type)
        ).execute()
    else:
        drive_service.files().create(
            body={"name": fname},
            media_body=MediaFileUpload(local_path, mimetype=mime_type),
            fields="id"
        ).execute()
    print(f"[INFO] Uploaded {fname} to Google Drive.")

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
    miles = dist_km * 0.621371
    price = get_diesel_price()
    gal = miles / MPG
    fuel = gal * 4.54609 * price
    return fuel + OVERNIGHT_COST if dur_s > OVERNIGHT_THRESHOLD_HOURS * 3600 else fuel

def extract_judges(text):
    secs = text.lower().split("retriever (golden)")
    out = {}
    for sec in secs[1:]:
        dogs = re.search(r"dogs.*judge.*?:\s*([A-Z][a-z].+)", sec, re.I)
        bitches = re.search(r"bitches.*judge.*?:\s*([A-Z][a-z].+)", sec, re.I)
        anyj = re.search(r"judge.*?:\s*([A-Z][a-z].+)", sec, re.I)
        if dogs: out["dogs"] = dogs.group(1).strip()
        if bitches: out["bitches"] = bitches.group(1).strip()
        if not out and anyj: out["all"] = anyj.group(1).strip()
        break
    return out or None

def get_show_date(text):
    m = re.search(r"Date Of Show:\s*([A-Za-z]+,\s*\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%A, %d %B %Y").date()
        except:
            return None
    return None

def jw_points(text):
    txt = text.lower()
    if "championship show" in txt: return 9
    if "open show" in txt: return 1
    return 0

def find_clashes_and_combos(results):
    by_date = {}
    for s in results:
        d = s.get("date")
        if d: by_date.setdefault(d, []).append(s)
    for group in by_date.values():
        if len(group)>1:
            for s in group: s["clash"]=True
    for i,a in enumerate(results):
        if not a.get("postcode") or a.get("duration_hr",0)<=3: continue
        for b in results[i+1:]:
            if not b.get("postcode") or b.get("duration_hr",0)<=3: continue
            da = datetime.datetime.fromisoformat(a["date"])
            db = datetime.datetime.fromisoformat(b["date"])
            if abs((da-db).days)==1:
                inter = get_drive(a["postcode"], b["postcode"], travel_cache)
                if inter and inter["duration"]<=75*60:
                    a.setdefault("combo_with",[]).append(b["show"])
                    b.setdefault("combo_with",[]).append(a["show"])

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
        for a in soup.select("a[href$='.aspx']"):
            href = a["href"]
            if href.startswith("/shows/") and href not in (
                "/shows/Shows-To-Enter.aspx",
                "/shows/Shows-Starting-Soon.aspx"
            ):
                links.append("https://www.fossedata.co.uk" + href)
        print(f"[INFO] Found {len(links)} show links.")
        with open("aspx_links.txt", "w") as f:
            f.write("\n".join(links))
        return links
    except Exception as e:
        print(f"[ERROR] Error fetching ASPX links: {e}")
        return []

# ———————————————————————————————————————————
# Playwright download logic
# ———————————————————————————————————————————
async def download_schedule_playwright(show_url):
    try:
        print(f"[INFO] Launching Playwright for: {show_url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Block Google Analytics requests to clean up logs
            await page.route("**/*", lambda route: route.abort() if "google-analytics.com" in route.request.url else route.continue_())

            # Block images to speed up page load
            await page.route("**/*", lambda route: route.abort() if "images" in route.request.url else route.continue_())
            
            # Check for stored session (cookies, local storage) and load if available
            if Path("storage_state.json").exists():
                await load_storage_state(page.context)
            else:
                print("[INFO] No saved storage state found, starting fresh.")
           
            def on_request_failed(req):
                try:
                    print(f"[REQUEST FAILED] {req.url} -> {req.failure}")
                except:
                    print(f"[REQUEST FAILED] {req.url} -> <no details>")
            page.on("requestfailed", on_request_failed)
            
            # Set a timeout for page load (e.g., 30 seconds)
            await page.goto(show_url, wait_until="networkidle", timeout=30000)  # 30 seconds timeout
            await load_storage_state(page.context)
            await page.evaluate("""() => {
                const o = document.getElementById('cookiescript_injected_wrapper');
                if (o) o.remove();
            }""")

            try:
                async with page.expect_download() as dl:
                    await page.click("#ctl00_ContentPlaceHolder_btnDownloadSchedule", timeout=30000)
                download = await dl.value
                fname = download.suggested_filename
                await download.save_as(fname)
                await save_storage_state(page)
                await browser.close()
                print(f"[INFO] Downloaded: {fname}")
                return fname

            except Exception:
                print("[WARN] download click failed — falling back to POST…")
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
                    out = show_url.rsplit("/",1)[-1].replace(".aspx",".pdf")
                    with open(out, "wb") as f: f.write(pdfb)
                    await save_storage_state(page)
                    await browser.close()
                    print(f"[INFO] Fallback PDF saved: {out}")
                    return out
                else:
                    print(f"[ERROR] Fallback POST failed: {resp.status} {ct}")
                    await browser.close()
                    return None

    except Exception as e:
        print(f"[ERROR] Playwright failed for {show_url}: {e}")
        return None

# ———————————————————————————————————————————
# full_run orchestrator
# ———————————————————————————————————————————
async def full_run():
    global travel_cache

    urls = fetch_aspx_links()
    if not urls:
        print("[WARN] No show URLs found.")
        return []

    travel_cache = {}
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "r") as f:
            travel_cache = json.load(f)

    shows = []
    for url in urls:
        pdf = await download_schedule_playwright(url)
        if not pdf:
            continue
        text = extract_text_from_pdf(pdf)
        if "golden" not in text.lower():
            print(f"[INFO] Skipping {pdf} — no 'golden'")
            continue
        pc = get_postcode(text)
        drive = get_drive(HOME_POSTCODE, pc, travel_cache) if pc else None
        cost = estimate_cost(drive["distance"], drive["duration"]) if drive else None
        judge = extract_judges(text)
        dt = get_show_date(text)
        shows.append({
            "show": url,
            "pdf": pdf,
            "date": dt.isoformat() if dt else None,
            "postcode": pc,
            "duration_hr": round(drive["duration"]/3600, 2) if drive else None,
            "distance_km": round(drive["distance"], 1) if drive else None,
            "cost_estimate": round(cost, 2) if cost else None,
            "points": jw_points(text),
            "judge": judge,
        })

    find_clashes_and_combos(shows)

    with open("results.json", "w") as f:
        json.dump(shows, f, indent=2)
    with open("results.csv", "w", newline="") as cf:
        w = csv.writer(cf)
        w.writerow([
            "Show","Date","Postcode","Distance (km)","Time (hr)",
            "Estimated Cost","JW Points","Golden Judge(s)","Clash","Combos"
        ])
        for s in shows:
            jt = ", ".join(f"{k}: {v}" for k, v in (s.get("judge") or {}).items())
            combos = "; ".join(s.get("combo_with", []))
            w.writerow([
                s["show"], s["date"], s["postcode"],
                s.get("distance_km"), s.get("duration_hr"),
                s.get("cost_estimate"), s["points"],
                jt, "Yes" if s.get("clash") else "", combos
            ])

    upload_to_drive("results.json", "application/json")
    upload_to_drive("results.csv", "text/csv")

    print(f"[INFO] Processed {len(shows)} shows with Golden Retriever classes.")
    return shows
