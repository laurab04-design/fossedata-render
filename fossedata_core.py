# Introducing attempt number # fossedata_core.py
import asyncio
import datetime
import math
import os
import re
import json
import csv
import shutil

from pathlib import Path

# Required external libraries: playwright (for async web scraping), PyMuPDF (for PDF parsing), requests (for HTTP requests).
# Ensure these are installed in your environment before running the script.

from playwright.async_api import async_playwright

# If PyMuPDF (fitz) is not installed, install it
try:
    import fitz  # PyMuPDF for PDF text extraction
except ImportError:
    import subprocess
    subprocess.run(['pip', 'install', 'PyMuPDF'], check=True)
    import fitz

import requests

# Hardcoded full Kennel Club breed list (KC_BREEDS)
KC_BREEDS = [
    # Hound Group
    "Afghan Hound", "Basenji", "Basset Fauve de Bretagne",
    "Grand Basset Griffon Vendeen", "Petit Basset Griffon Vendeen", "Basset Hound",
    "Beagle", "Bloodhound", "Borzoi", "Cirneco dell'Etna", "Dachshund",
    "Dachshund (Long Haired)", "Dachshund (Miniature Long Haired)",
    "Dachshund (Smooth Haired)", "Dachshund (Miniature Smooth Haired)",
    "Dachshund (Wire Haired)", "Dachshund (Miniature Wire Haired)",
    "Scottish Deerhound", "Finnish Spitz", "Greyhound", "Hamiltonstovare",
    "Harrier", "Ibizan Hound", "Irish Wolfhound", "Norwegian Elkhound",
    "Otterhound", "Pharaoh Hound", "Portuguese Podengo", "Rhodesian Ridgeback",
    "Saluki", "Sloughi", "Whippet",
    # Gundog Group
    "Brittany", "Bracco Italiano", "German Shorthaired Pointer",
    "German Longhaired Pointer", "German Wirehaired Pointer", "Gordon Setter",
    "Hungarian Vizsla", "Hungarian Wirehaired Vizsla", "Italian Spinone",
    "Irish Red and White Setter", "Irish Setter", "English Setter", "Pointer",
    "Weimaraner", "Large Munsterlander", "Small Munsterlander", "Lagotto Romagnolo",
    "Kooikerhondje", "Spanish Water Dog", "Golden Retriever", "Labrador Retriever",
    "Flat Coated Retriever", "Curly Coated Retriever", "Chesapeake Bay Retriever",
    "Nova Scotia Duck Tolling Retriever", "English Springer Spaniel",
    "Welsh Springer Spaniel", "Cocker Spaniel", "Clumber Spaniel", "Field Spaniel",
    "Irish Water Spaniel", "Sussex Spaniel", "American Cocker Spaniel",
    "Slovakian Rough Haired Pointer", "Braque d'Auvergne",
    # Terrier Group
    "Airedale Terrier", "American Hairless Terrier", "Australian Terrier",
    "Bedlington Terrier", "Border Terrier", "Bull Terrier", "Miniature Bull Terrier",
    "Cairn Terrier", "Cesky Terrier", "Dandie Dinmont Terrier", "Smooth Fox Terrier",
    "Wire Fox Terrier", "Glen of Imaal Terrier", "Irish Terrier", "Jack Russell Terrier",
    "Kerry Blue Terrier", "Lakeland Terrier", "Manchester Terrier", "Norfolk Terrier",
    "Norwich Terrier", "Parson Russell Terrier", "Scottish Terrier", "Sealyham Terrier",
    "Skye Terrier", "Soft Coated Wheaten Terrier", "Staffordshire Bull Terrier",
    "Welsh Terrier", "West Highland White Terrier",
    # Utility Group
    "Akita", "Japanese Akita Inu", "Boston Terrier", "Bulldog", "Chow Chow",
    "Dalmatian", "French Bulldog", "German Spitz", "Japanese Spitz", "Keeshond",
    "Lhasa Apso", "Schipperke", "Schnauzer", "Miniature Schnauzer", "Shar Pei",
    "Shih Tzu", "Tibetan Spaniel", "Tibetan Terrier", "Xoloitzcuintle", "Xoloitzcuintli",
    "Mexican Hairless", "Poodle", "Standard Poodle", "Miniature Poodle", "Toy Poodle",
    "Shiba Inu", "Japanese Shiba Inu", "Canaan Dog", "Eurasier",
    # Pastoral Group
    "Anatolian Shepherd Dog", "Kangal Shepherd Dog", "Australian Cattle Dog",
    "Australian Shepherd", "Bearded Collie", "Belgian Shepherd Dog", "Belgian Malinois",
    "Belgian Tervueren", "Belgian Groenendael", "Belgian Laekenois", "Border Collie",
    "Briard", "Catalan Sheepdog", "Rough Collie", "Smooth Collie", "German Shepherd Dog",
    "Komondor", "Kuvasz", "Finnish Lapphund", "Icelandic Sheepdog", "Norwegian Buhund",
    "Old English Sheepdog", "Polish Lowland Sheepdog", "Pyrenean Mountain Dog",
    "Shetland Sheepdog", "Swedish Vallhund", "Cardigan Welsh Corgi", "Pembroke Welsh Corgi",
    "Samoyed", "White Swiss Shepherd Dog",
    # Working Group
    "Alaskan Malamute", "Bernese Mountain Dog", "Bouvier des Flandres", "Boxer",
    "Bullmastiff", "Canadian Eskimo Dog", "Cane Corso", "Dobermann", "Dogue de Bordeaux",
    "Estrela Mountain Dog", "Great Dane", "Greater Swiss Mountain Dog", "Greenland Dog",
    "Hovawart", "Leonberger", "Mastiff", "Neapolitan Mastiff", "Newfoundland",
    "Portuguese Water Dog", "Rottweiler", "Russian Black Terrier", "St. Bernard",
    "Siberian Husky", "Tibetan Mastiff", "Giant Schnauzer", "German Pinscher",
    # Toy Group
    "Affenpinscher", "Bichon Frise", "Bolognese", "Cavalier King Charles Spaniel",
    "Chihuahua", "English Toy Terrier", "Griffon Bruxellois", "Havanese",
    "Italian Greyhound", "Japanese Chin", "King Charles Spaniel", "Lowchen",
    "Maltese", "Miniature Pinscher", "Papillon", "Pekingese", "Pomeranian", "Pug",
    "Russian Toy", "Yorkshire Terrier", "Chinese Crested", "Coton de Tulear"
]
# Deduplicate and unify breed list (e.g., remove exact duplicates, ensure consistent case)
KC_BREEDS = sorted(set(KC_BREEDS), key=str.lower)

# Configurable constants
HOME_LOCATION = "Bury, England, UK"  # Home location or postcode for travel calculations
MPG = 40.0  # fuel efficiency in miles per gallon
LITERS_PER_GALLON = 4.54609  # UK gallon to liters

# Dog information
DOG_NAME = "Delia"
DOG_DOB = datetime.date(2024, 5, 15)  # Delia's date of birth
PUPPY_CUTOFF = DOG_DOB.replace(year=DOG_DOB.year + 1)  # not eligible for Puppy class after this date
JW_CUTOFF = DOG_DOB + datetime.timedelta(days=548)  # 18 months ~ 548 days

# Class codes indicating eligibility for Delia (for JW points or particular interest classes) at Open shows
ELIGIBLE_CLASS_CODES = ["PB", "JB", "YB", "SBB", "UGB", "TB", "MPB", "NB"]
# We'll also detect sex-neutral class names for these categories if present (e.g., "Puppy", "Junior", etc.)

# File paths for data persistence
PROCESSED_SHOWS_FILE = "processed_shows.json"
TRAVEL_CACHE_FILE = "travel_cache.json"
STORAGE_STATE_FILE = "storage_state.json"

# Output files
RESULTS_CSV = "results.csv"
RESULTS_JSON = "results.json"
CLASH_OVERNIGHT_CSV = "clashes_overnight.csv"

# Initialize global variables
processed_shows = set()
travel_cache = {}
diesel_price_per_liter = None

# Load processed_shows if exists
if os.path.isfile(PROCESSED_SHOWS_FILE):
    try:
        with open(PROCESSED_SHOWS_FILE, "r") as f:
            processed_data = json.load(f)
            if isinstance(processed_data, list):
                processed_shows = set(processed_data)
            elif isinstance(processed_data, dict):
                processed_shows = set(processed_data.keys())
    except Exception as e:
        print(f"Warning: Could not load {PROCESSED_SHOWS_FILE}: {e}")

# Load travel_cache if exists
if os.path.isfile(TRAVEL_CACHE_FILE):
    try:
        with open(TRAVEL_CACHE_FILE, "r") as f:
            travel_cache = json.load(f)
    except Exception as e:
        print(f"Warning: Could not load {TRAVEL_CACHE_FILE}: {e}")
        travel_cache = {}

if not isinstance(travel_cache, dict):
    travel_cache = {}

async def get_diesel_price():
    '''Fetch the current average diesel price in £/liter (or use fallback).'''
    try:
        url = "https://www.rac.co.uk/drive/advice/fuel-watch/"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            text = resp.text
            match = re.search(r"Diesel\s*£?([0-9]+\.?[0-9]+)p", text)
            if match:
                price_pence = float(match.group(1))
                return price_pence / 100.0
            match = re.search(r"Diesel[^0-9]+([0-9]+\.?[0-9]+)p", text)
            if match:
                return float(match.group(1)) / 100.0
    except Exception as e:
        print(f"Warning: Diesel price fetch failed: {e}")
    return 1.50  # default £1.50 per liter

async def fetch_show_list(page):
    '''Scrape the Fosse Data site for upcoming shows and return a list of shows with details.'''
    shows = []
    await page.goto("https://www.fossedata.co.uk/shows.aspx", timeout=60000)
    content = await page.content()
    show_entries = re.findall(r"(?P<date>\d{1,2} \w+ 20\d{2}).+?(?P<name>[A-Z][^<]+Show)[^<]*(?P<venue>[A-Z][^<]+)(?P<link>ShowID=\d+)?", content, flags=re.DOTALL)
    for match in show_entries:
        date_str, name, venue, showid = match
        try:
            show_date = datetime.datetime.strptime(date_str, "%d %B %Y").date()
        except:
            try:
                show_date = datetime.datetime.strptime(date_str, "%d %b %Y").date()
            except:
                show_date = None
        show_name = name.strip()
        venue = venue.strip()
        show_type = "Unknown"
        if "Championship Show" in show_name or "Ch. Show" in show_name:
            show_type = "Championship"
        elif "Open Show" in show_name or "Open Show" in show_name:
            show_type = "Open"
            if "Premier" in show_name:
                show_type = "Premier Open"
        elif "Open Show" in show_name:
            show_type = "Open"
        if showid:
            show_id = showid.strip()
        else:
            show_id = f"{show_name}_{show_date}"
        shows.append({
            "id": show_id,
            "name": show_name,
            "date": show_date,
            "venue": venue,
            "type": show_type
        })
    return shows

async def download_schedule_for_show(context, show):
    '''Download the schedule PDF for a given show, using Playwright (with fallback to requests).'''
    show_id = show.get("id")
    schedule_pdf_path = f"schedule_{show_id}.pdf" if show_id else "schedule_temp.pdf"
    try:
        page = await context.new_page()
        if show_id and show_id.isdigit():
            await page.goto(f"https://www.fossedata.co.uk/show.asp?ShowID={show_id}", timeout=30000)
        else:
            await page.goto(f"https://www.fossedata.co.uk", timeout=20000)
        download_link = None
        try:
            download_link_elem = await page.query_selector("a:text(\"Schedule\")")
        except:
            download_link_elem = None
        if download_link_elem:
            href = await download_link_elem.get_attribute("href")
            if href:
                download_link = href
        if download_link:
            download_task = page.wait_for_event("download")
            await page.goto(download_link)
            download = await download_task
            await download.save_as(schedule_pdf_path)
        else:
            try:
                download_task = page.wait_for_event("download")
                download_button = await page.query_selector("text=Download Schedule")
                if download_button:
                    await download_button.click()
                else:
                    link = await page.query_selector("a[href*='Schedule']")
                    if link:
                        await link.click()
                download = await download_task
                await download.save_as(schedule_pdf_path)
            except Exception as e:
                raise e
        await page.close()
        return schedule_pdf_path
    except Exception as e:
        try:
            if show_id and show_id.isdigit():
                pdf_response = requests.post("https://www.fossedata.co.uk/downloadSchedule.asp", data={"ShowID": show_id}, timeout=15)
            else:
                pdf_response = None
            if pdf_response and pdf_response.status_code == 200:
                with open(schedule_pdf_path, "wb") as f:
                    f.write(pdf_response.content)
                print(f"Used fallback POST to download schedule for {show.get('name')}")
                return schedule_pdf_path
        except Exception as e2:
            print(f"Error downloading schedule for {show.get('name')}: {e2}")
    return None

def parse_pdf_for_info(pdf_path):
    '''Extract relevant information from the schedule PDF (already downloaded to pdf_path).'''
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text += page.get_text()
        doc.close()
    except Exception as e:
        print(f"Failed to read PDF {pdf_path}: {e}")
        return None
    text_lower = text.lower()
    if 'golden' not in text_lower:
        return None
    info = {}
    m = re.search(r"First\s+Entry[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text, flags=re.IGNORECASE)
    if m:
        try:
            info['first_entry_fee'] = float(m.group(1))
        except:
            info['first_entry_fee'] = m.group(1)
    else:
        info['first_entry_fee'] = None
    m2 = re.search(r"Subsequent[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text, flags=re.IGNORECASE)
    if m2:
        try:
            info['subsequent_entry_fee'] = float(m2.group(1))
        except:
            info['subsequent_entry_fee'] = m2.group(1)
    else:
        info['subsequent_entry_fee'] = None
    m3 = re.search(r"Catalogue[^£]*£\s*([0-9]+(?:\.[0-9]{1,2})?)", text, flags=re.IGNORECASE)
    if m3:
        try:
            info['catalogue_price'] = float(m3.group(1))
        except:
            info['catalogue_price'] = m3.group(1)
    else:
        info['catalogue_price'] = None
    judge_dogs = None
    judge_bitches = None
    judge_section_match = re.search(r"Golden Retriever[^\n]*Dogs?:\s*([^\n]+)", text)
    if judge_section_match:
        line = judge_section_match.group(0)
        if 'Bitches' in line:
            parts = re.split(r"Dogs?:|Bitches:", line)
            if len(parts) >= 2:
                judge_dogs = parts[1].strip().strip(' ,;')
            if len(parts) >= 3:
                judge_bitches = parts[2].strip().strip(' ,;')
        else:
            judge_dogs = judge_section_match.group(1).strip().strip(' ,;')
            next_line_idx = text.find(judge_section_match.group(0)) + len(judge_section_match.group(0))
            next_line_end = text.find('\n', next_line_idx)
            if next_line_end != -1:
                next_line = text[next_line_idx:next_line_end]
                if 'Bitches:' in next_line:
                    jb = re.search(r"Bitches:\s*([^\n]+)", next_line)
                    if jb:
                        judge_bitches = jb.group(1).strip().strip(' ,;')
    else:
        judge_lines = re.findall(r"Judge[^:\n]*:\s*([^\n]+)", text)
        if judge_lines:
            if len(judge_lines) == 1:
                judge_bitches = judge_dogs = judge_lines[0].strip().strip(' ,;')
            elif len(judge_lines) >= 2:
                judge_dogs = judge_lines[0].strip().strip(' ,;')
                judge_bitches = judge_lines[1].strip().strip(' ,;')
    info['judge_dogs'] = judge_dogs
    info['judge_bitches'] = judge_bitches
    eligible_classes_found = False
    class_keywords = ["Puppy", "Junior", "Yearling", "Special Beginners", "Undergraduate", "Tyro", "Novice", "Minor Puppy"]
    for ck in class_keywords:
        if ck.lower() in text_lower:
            eligible_classes_found = True
            break
    info['eligible_classes_found'] = eligible_classes_found
    return info

async def calculate_travel_info(page, origin, destination):
    '''Get travel distance (miles) and time (minutes) using Google Maps.'''
    key = (origin, destination)
    if 'between' in travel_cache and f"{origin}||{destination}" in travel_cache['between']:
        return travel_cache['between'][f"{origin}||{destination}"]
    if 'destinations' in travel_cache and destination in travel_cache['destinations']:
        if origin == HOME_LOCATION:
            return travel_cache['destinations'][destination]
    query_url = f"https://www.google.com/maps/dir/{origin.replace(' ', '+')}/{destination.replace(' ', '+')}"
    await page.goto(query_url, timeout=60000)
    try:
        consent_btn = await page.query_selector("button:text(\"Accept all\")")
        if consent_btn:
            await consent_btn.click()
            await page.wait_for_timeout(1000)
    except:
        pass
    try:
        await page.wait_for_selector("div[aria-label*='mi']", timeout=10000)
    except:
        pass
    content = await page.content()
    dist_match = re.search(r"([0-9]+\.?[0-9]*)\s*mi", content)
    time_match = re.search(r"([0-9]+\s*hours?\s*[0-9]*\s*mins?)", content)
    distance_text = dist_match.group(1) if dist_match else None
    time_text = time_match.group(1) if time_match else None
    distance_miles = float(distance_text) if distance_text else 0.0
    travel_minutes = 0
    if time_text:
        h_match = re.search(r"(\d+)\s*hour", time_text)
        m_match = re.search(r"(\d+)\s*min", time_text)
        if h_match:
            travel_minutes += int(h_match.group(1)) * 60
        if m_match:
            travel_minutes += int(m_match.group(1))
    if origin == HOME_LOCATION:
        travel_cache.setdefault('destinations', {})[destination] = {"distance": distance_miles, "time": travel_minutes}
    else:
        combined_key = f"{origin}||{destination}"
        travel_cache.setdefault('between', {})[combined_key] = {"distance": distance_miles, "time": travel_minutes}
    return {"distance": distance_miles, "time": travel_minutes}

async def main():
    global diesel_price_per_liter
    diesel_price_per_liter = await get_diesel_price()
    results = []
    clash_list = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = None
        if os.path.exists(STORAGE_STATE_FILE):
            context = await browser.new_context(storage_state=STORAGE_STATE_FILE)
        else:
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto("https://www.google.com", timeout=15000)
                consent = await page.query_selector("button:text(\"Accept all\")")
                if consent:
                    await consent.click()
                    await page.wait_for_timeout(2000)
            except Exception as e:
                pass
            await page.close()
            await context.storage_state(path=STORAGE_STATE_FILE)
        page = await context.new_page()
        shows = await fetch_show_list(page)
        filtered_shows = []
        for show in shows:
            name_lower = show['name'].lower()
            skip = False
            for breed in KC_BREEDS:
                if breed.lower() in name_lower:
                    if "golden" in breed.lower():
                        skip = False
                        break
                    else:
                        skip = True
                        break
            if 'spanish' in name_lower and 'golden' not in name_lower:
                skip = True
            if not skip:
                filtered_shows.append(show)
        count = 0
        for show in filtered_shows:
            show_id = show.get("id")
            show_name = show.get("name")
            show_date = show.get("date")
            show_type = show.get("type")
            unique_id = show_id or f"{show_name}_{show_date}"
            if unique_id in processed_shows:
                continue
            pdf_path = await download_schedule_for_show(context, show)
            if not pdf_path or not os.path.isfile(pdf_path):
                continue
            info = parse_pdf_for_info(pdf_path)
            try:
                os.remove(pdf_path)
            except:
                pass
            if not info:
                processed_shows.add(unique_id)
                continue
            travel_info = {}
            try:
                travel_info = await calculate_travel_info(page, HOME_LOCATION, show['venue'])
            except Exception as e:
                print(f"Travel calculation failed for {show_name}: {e}")
                travel_info = {"distance": 0.0, "time": 0}
            distance = travel_info.get("distance", 0.0)
            travel_minutes = travel_info.get("time", 0)
            fuel_needed_gallons = distance / MPG if MPG > 0 else 0
            fuel_needed_liters = fuel_needed_gallons * LITERS_PER_GALLON
            fuel_cost = round(fuel_needed_liters * diesel_price_per_liter, 2)
            jw_point = 0
            if show_type in ["Open", "Premier Open"] and show_date and show_date <= JW_CUTOFF:
                if info.get('eligible_classes_found'):
                    jw_point = 1
            result = {
                "date": show_date.strftime("%Y-%m-%d") if show_date else "",
                "show_name": show_name,
                "show_type": show_type,
                "venue": show.get("venue", ""),
                "first_entry_fee": info.get('first_entry_fee'),
                "subsequent_entry_fee": info.get('subsequent_entry_fee'),
                "catalogue_price": info.get('catalogue_price'),
                "judge_dogs": info.get('judge_dogs'),
                "judge_bitches": info.get('judge_bitches'),
                "distance_miles": round(distance, 1),
                "drive_time_minutes": travel_minutes,
                "diesel_cost": fuel_cost,
                "jw_point_possible": jw_point
            }
            entry_cost = 0.0
            if info.get('first_entry_fee') is not None:
                entry_cost += float(info['first_entry_fee'])
            if info.get('subsequent_entry_fee') is not None:
                entry_cost += float(info['subsequent_entry_fee'])
            if info.get('catalogue_price') is not None:
                entry_cost += float(info['catalogue_price'])
            total_cost = round(entry_cost + fuel_cost, 2)
            result["total_cost"] = total_cost
            results.append(result)
            processed_shows.add(unique_id)
            count += 1
            if count % 5 == 0:
                with open(RESULTS_JSON, "w") as jf:
                    json.dump(results, jf, indent=2, default=str)
                with open(RESULTS_CSV, "w", newline='') as cf:
                    writer = csv.writer(cf)
                    if results:
                        header = results[0].keys()
                        writer.writerow(header)
                        for r in results:
                            writer.writerow(r.values())
        await context.storage_state(path=STORAGE_STATE_FILE)
        await browser.close()
    for r in results:
        if r.get('date'):
            try:
                r['_date_obj'] = datetime.datetime.strptime(r['date'], "%Y-%m-%d").date()
            except:
                r['_date_obj'] = None
        else:
            r['_date_obj'] = None
    results.sort(key=lambda x: (x.get('_date_obj') or datetime.date.max))
    shows_by_date = {}
    for r in results:
        d = r.get('_date_obj')
        if not d:
            continue
        shows_by_date.setdefault(d, []).append(r)
    for d, show_list in shows_by_date.items():
        if len(show_list) > 1:
            n = len(show_list)
            for i in range(n):
                for j in range(i+1, n):
                    s1 = show_list[i]
                    s2 = show_list[j]
                    venue1 = s1.get('venue', '')
                    venue2 = s2.get('venue', '')
                    pc1 = re.search(r"[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}", venue1, flags=re.IGNORECASE)
                    pc2 = re.search(r"[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}", venue2, flags=re.IGNORECASE)
                    pc1 = pc1.group(0) if pc1 else venue1
                    pc2 = pc2.group(0) if pc2 else venue2
                    if pc1 and pc2 and pc1.strip().upper() == pc2.strip().upper():
                        continue
                    clash_list.append({
                        "type": "Clash",
                        "date": d.strftime("%Y-%m-%d"),
                        "show1": s1['show_name'],
                        "show2": s2['show_name']
                    })
    results_by_date = sorted([r for r in results if r.get('_date_obj')], key=lambda x: x['_date_obj'])
    for i, show_a in enumerate(results_by_date):
        date_a = show_a['_date_obj']
        time_from_home = show_a.get('drive_time_minutes', 0)
        if time_from_home < 180:
            continue
        next_day = date_a + datetime.timedelta(days=1)
        next_day_shows = [r for r in results_by_date if r['_date_obj'] == next_day]
        for show_b in next_day_shows:
            venue_a = show_a['venue']
            venue_b = show_b['venue']
            if not venue_a or not venue_b:
                continue
            combined_key = f"{venue_a}||{venue_b}"
            travel_ab = None
            if 'between' in travel_cache and combined_key in travel_cache['between']:
                travel_ab = travel_cache['between'][combined_key]
            else:
                try:
                    travel_ab = await calculate_travel_info(await async_playwright().start().chromium.launch().new_context().new_page(), venue_a, venue_b)
                except Exception as e:
                    travel_ab = {"distance": 0.0, "time": 999}
            time_ab = travel_ab.get('time', 999)
            if time_ab <= 75:
                clash_list.append({
                    "type": "Overnight Suggestion",
                    "date": f"{date_a.strftime('%Y-%m-%d')} -> {next_day.strftime('%Y-%m-%d')}",
                    "show1": show_a['show_name'],
                    "show2": show_b['show_name'],
                    "travel_time_minutes": time_ab
                })
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
    with open(CLASH_OVERNIGHT_CSV, "w", newline='') as cf:
        writer = csv.writer(cf)
        writer.writerow(["type", "date", "show1", "show2", "travel_time_minutes"])
        for item in clash_list:
            writer.writerow([
                item.get('type', ''),
                item.get('date', ''),
                item.get('show1', ''),
                item.get('show2', ''),
                item.get('travel_time_minutes', '')
            ])
    try:
        with open(PROCESSED_SHOWS_FILE, "w") as pf:
            json.dump(sorted(list(processed_shows)), pf, indent=2)
    except Exception as e:
        print(f"Warning: Could not save {PROCESSED_SHOWS_FILE}: {e}")
    try:
        with open(TRAVEL_CACHE_FILE, "w") as tf:
            json.dump(travel_cache, tf, indent=2)
    except Exception as e:
        print(f"Warning: Could not save {TRAVEL_CACHE_FILE}: {e}")
    try:
        drive_mount_path = None
        try:
            from google.colab import drive as colab_drive
            colab_drive.mount('/content/drive')
            drive_mount_path = '/content/drive/MyDrive/'
        except Exception as e:
            drive_mount_path = None
        if drive_mount_path:
            for fname in [RESULTS_JSON, RESULTS_CSV, CLASH_OVERNIGHT_CSV, PROCESSED_SHOWS_FILE, TRAVEL_CACHE_FILE, STORAGE_STATE_FILE]:
                if os.path.exists(fname):
                    shutil.copy(fname, os.path.join(drive_mount_path, fname))
        else:
            try:
                from pydrive2.auth import GoogleAuth
                from pydrive2.drive import GoogleDrive
                gauth = GoogleAuth()
                cred_file = "drive_credentials.txt"
                if os.path.exists(cred_file):
                    gauth.LoadCredentialsFile(cred_file)
                if not gauth.credentials or gauth.credentials.invalid:
                    gauth.LocalWebserverAuth()
                    gauth.SaveCredentialsFile(cred_file)
                drive = GoogleDrive(gauth)
                for fname in [RESULTS_JSON, RESULTS_CSV, CLASH_OVERNIGHT_CSV, PROCESSED_SHOWS_FILE, TRAVEL_CACHE_FILE, STORAGE_STATE_FILE]:
                    if not os.path.exists(fname):
                        continue
                    file_list = drive.ListFile({'q': f"title='{fname}' and 'root' in parents"}).GetList()
                    if file_list:
                        gfile = file_list[0]
                        gfile.SetContentFile(fname)
                        gfile.Upload()
                    else:
                        gfile = drive.CreateFile({'title': fname})
                        gfile.SetContentFile(fname)
                        gfile.Upload()
            except Exception as e:
                print(f"Google Drive sync via PyDrive failed: {e}")
    except Exception as e:
        print(f"Google Drive sync step encountered an error: {e}")

if __name__ == '__main__':
    asyncio.run(main())
