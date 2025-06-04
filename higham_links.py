import requests
from bs4 import BeautifulSoup
import datetime

def fetch_higham_show_links():
    url = "https://www.highampress.co.uk/shows"
    response = requests.get(url)
    response.raise_for_status()
    html = response.text

    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    shows = []

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

        shows.append((
            full_url,
            start_date.isoformat() if start_date else "",
            end_date.isoformat() if end_date else "",
            entry_close.isoformat() if entry_close else ""
        ))

    return shows

def save_higham_links_to_file(output_file="higham_links.txt"):
    show_links = fetch_higham_show_links()
    with open(output_file, "w") as f:
        for url, start, end, close in show_links:
            f.write(f"{url}\t{start}\t{end}\t{close}\n")
    print(f"Saved {len(show_links)} shows to {output_file}")
