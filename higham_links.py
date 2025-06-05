import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def fetch_higham_show_links():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.highampress.co.uk/shows")

        all_shows = []
        visited_pages = set()

        # Identify how many pages exist via pagination buttons
        await page.wait_for_selector("button[aria-label^='Go to page']")
        pagination_buttons = page.locator("button[aria-label^='Go to page']")
        count = await pagination_buttons.count()

        for i in range(count):
            button = pagination_buttons.nth(i)
            label = await button.get_attribute("aria-label")
            if not label or not label.lower().startswith("go to page"):
                continue

            # Avoid re-clicking the current page
            page_num = label.split()[-1]
            if page_num in visited_pages:
                continue
            visited_pages.add(page_num)

            await button.click()
            await page.wait_for_timeout(1000)  # Allow Livewire update

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            anchors = soup.find_all("a", href=True)
            for anchor in anchors:
                href = anchor["href"]
                if "/shows/" not in href or "class=" not in str(anchor):
                    continue

                parent = anchor.find_parent("div", class_="grid")
                if parent and "Entries are now closed" in parent.text:
                    continue

                full_url = href if href.startswith("http") else f"https://www.highampress.co.uk{href}"

                start_date = end_date = entry_close = None
                time_tags = parent.find_all("time") if parent else []

                for tag in time_tags:
                    date_str = tag.get("datetime", "").strip()
                    try:
                        parsed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue

                    label_text = tag.find_previous_sibling("span")
                    label = label_text.get_text(strip=True).lower() if label_text else ""

                    if "entry closing date" in label:
                        entry_close = parsed_date
                    elif not start_date:
                        start_date = parsed_date
                    elif parsed_date != start_date:
                        end_date = parsed_date

                all_shows.append((
                    full_url,
                    start_date.isoformat() if start_date else "",
                    end_date.isoformat() if end_date else "",
                    entry_close.isoformat() if entry_close else ""
                ))

        await browser.close()
        return all_shows

async def save_higham_links_to_file(output_file="higham_links.txt"):
    show_links = await fetch_higham_show_links()
    with open(output_file, "w") as f:
        for url, start, end, close in show_links:
            f.write(f"{url}\t{start}\t{end}\t{close}\n")
    print(f"Saved {len(show_links)} shows to {output_file}")
