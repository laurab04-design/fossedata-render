import os
import asyncio
import base64
import json
from playwright.async_api import async_playwright
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

google_service_account_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
gdrive_folder_id = os.getenv("GDRIVE_FOLDER_ID")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def get_drive_service():
    if not google_service_account_key:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_BASE64 env var not set")
    if not gdrive_folder_id:
        raise RuntimeError("GDRIVE_FOLDER_ID env var not set")

    decoded_key = base64.b64decode(google_service_account_key)
    service_account_info = json.loads(decoded_key.decode("utf-8"))

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )
    service = build("drive", "v3", credentials=credentials)
    return service

async def fetch_kc_breeds():
    url = "https://www.thekennelclub.org.uk/search/breeds-a-to-z/"
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        breeds = await page.eval_on_selector_all(".breed-card__title", "els => els.map(e => e.textContent.trim().toLowerCase())")
        await browser.close()

    # Save locally first
    filename = "kc_breeds.txt"
    with open(filename, "w") as f:
        for breed in sorted(breeds):
            f.write(breed + "\n")
    print(f"[INFO] Saved {len(breeds)} breeds to {filename}")

    # Upload to Google Drive
    try:
        drive_service = get_drive_service()
        media = MediaFileUpload(filename, mimetype="text/plain")
        # Check if file exists
        query = f"'{gdrive_folder_id}' in parents and name='{filename}'"
        result = drive_service.files().list(q=query, fields="files(id, name)").execute()
        if result.get("files"):
            file_id = result["files"][0]["id"]
            drive_service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[INFO] Updated {filename} on Google Drive.")
        else:
            file_metadata = {'name': filename, 'parents': [gdrive_folder_id]}
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"[INFO] Uploaded {filename} to Google Drive.")
    except Exception as e:
        print(f"[ERROR] Google Drive upload failed: {e}")

if __name__ == "__main__":
    asyncio.run(fetch_kc_breeds())
