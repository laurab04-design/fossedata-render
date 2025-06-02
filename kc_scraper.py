# kc_scraper.py
# One-time script to fetch and save KC breeds to kc_breeds.txt

import asyncio
from playwright.async_api import async_playwright
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

async def fetch_kc_breeds():
    url = "https://www.thekennelclub.org.uk/search/breeds-a-to-z/"
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        breeds = await page.eval_on_selector_all(".breed-card__title", "els => els.map(e => e.textContent.trim().toLowerCase())")
        with open("kc_breeds.txt", "w") as f:
            for breed in sorted(breeds):
                f.write(breed + "\n")
        await browser.close()
        print(f"[INFO] Saved {len(breeds)} breeds to kc_breeds.txt")

if __name__ == "__main__":
    asyncio.run(fetch_kc_breeds())
