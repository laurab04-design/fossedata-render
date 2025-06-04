import requests
from bs4 import BeautifulSoup
import csv

def get_year_show_list(session, year, base_viewstate, base_eventvalidation, base_viewstategen):
    """
    Retrieve the list of shows for a given year from the Fosse Data results page.
    Returns a list of tuples (show_name, show_date, show_url).
    """
    # Prepare POST data to filter results by the specified year
    data = {
        "__VIEWSTATE": base_viewstate,
        "__EVENTVALIDATION": base_eventvalidation,
        "__VIEWSTATEGENERATOR": base_viewstategen,
        "__EVENTTARGET": "ctl00$ContentPlaceHolder$ddlYear",
        "__EVENTARGUMENT": "",
        # Include the year dropdown selection and keep show type as "All Types"
        "ctl00$ContentPlaceHolder$ddlYear": str(year),
        "ctl00$ContentPlaceHolder$ddlType": "",  # assuming empty selects "(All Types)"
    }
    # Send POST request to filter by year
    response = session.post(RESULTS_URL, data=data)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    show_list = []
    # The shows are likely listed in a table; find all rows with a "Details" link
    for link in soup.find_all('a', string="Details"):
        href = link.get("href")
        if not href:
            continue
        # Construct full URL if needed
        show_url = href
        if show_url.startswith("/"):
            show_url = "https://www.fossedata.co.uk" + show_url
        # The link cell is in the same row as show name and date
        # Traverse to the parent row (tr) and get text from the first two cells
        row = link.find_parent('tr')
        if row:
            cells = row.find_all('td')
        else:
            # Fallback: try parent of parent (in case anchor is nested in a span or div in the cell)
            cells = link.find_parents('tr')[0].find_all('td') if link.find_parents('tr') else []
        if len(cells) >= 2:
            show_name = cells[0].get_text(strip=True)
            show_date = cells[1].get_text(strip=True)
        else:
            # If not a table, parse sibling text (Name is likely above the link text in HTML flow)
            # Find previous text node for name and date
            show_name = link.find_previous(string=True, recursive=False)
            show_date = link.find_previous(string=True, recursive=True)
            show_name = show_name.strip() if show_name else ""
            show_date = show_date.strip() if show_date else ""
        show_list.append((show_name, show_date, show_url))
    return show_list

def scrape_show_results(session, show_name, show_date, show_url):
    """
    Scrape Golden Retriever results from a single show results page.
    Returns a list of result rows (each a dict) for the given show.
    """
    results = []
    # Fetch the show results page
    res = session.get(show_url)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    page_text = soup.get_text(separator="\n")  # full text for quick checks
    # Skip if no results available or Golden not listed
    if "Show Results are not yet available" in page_text:
        return results  # no results to scrape
    if "Retriever (Golden)" not in page_text:
        return results  # skip shows with no Golden Retriever in breed list
    
    # If multiple breeds, we may need to trigger the Golden Retriever breed filter.
    # Identify the breed dropdown control and Golden's option value.
    breed_option = None
    # Find the option for Retriever (Golden) in the HTML
    for option in soup.find_all('option'):
        if option.get_text(strip=True) == "Retriever (Golden)":
            breed_option = option
            break
    if breed_option and breed_option.get("value"):
        # Prepare postback to select the Golden Retriever breed in Gundog group
        breed_value = breed_option["value"]
        # The dropdown may have a unique name/id we need for __EVENTTARGET.
        # Find the select element containing this option.
        select_elem = breed_option.find_parent('select')
        if select_elem:
            select_name = select_elem.get("name")
            select_id = select_elem.get("id")
        else:
            select_name = None
            select_id = None
        if select_name and select_id:
            # Collect hidden fields from the page for the postback
            viewstate = soup.find("input", {"id": "__VIEWSTATE"})
            eventval = soup.find("input", {"id": "__EVENTVALIDATION"})
            viewstategen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
            post_data = {
                "__VIEWSTATE": viewstate["value"] if viewstate else "",
                "__EVENTVALIDATION": eventval["value"] if eventval else "",
                "__VIEWSTATEGENERATOR": viewstategen["value"] if viewstategen else "",
                "__EVENTTARGET": select_id,
                "__EVENTARGUMENT": "",
                select_name: breed_value
            }
            # Send POST to show page to get Golden Retriever results
            res2 = session.post(show_url, data=post_data)
            res2.raise_for_status()
            soup = BeautifulSoup(res2.text, "html.parser")
            page_text = soup.get_text(separator="\n")
    # Now parse Golden Retriever results from the soup
    # Narrow down to the Golden Retriever section by finding the breed heading
    breed_section = []
    start_found = False
    for line in page_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Identify start of Golden Retriever section
        if not start_found:
            # The breed section typically starts with "Retriever (Golden) - Judge:"
            if line.startswith("Retriever (Golden)") and "Judge:" in line:
                start_found = True
                continue  # don't include this heading line itself in results
        if start_found:
            # Break when we reach the next group or breed heading (i.e., another group name)
            # Group headings are like "Hound Group" or "Pastoral Group"
            # Breed headings for other breeds would include " - Judge:" as well.
            if line.endswith(" Group") or line.endswith("Group") or line.startswith("Hound Group") or " Group" in line:
                break
            if line.startswith("Retriever (Golden)"):
                # If somehow repeated breed heading appears, skip it
                continue
            breed_section.append(line)
    # Parse lines in breed_section to extract classes and awards
    current_class = None
    class_entries = None
    class_absentees = None
    for line in breed_section:
        # Skip any lingering null or empty lines
        if not line or line.lower().startswith("null"):
            continue
        # Check for class header lines e.g. "Class 123. ClassName (Entries: X Absentees: Y)"
        if line.lower().startswith("class"):
            # When a new class starts, reset current_class context
            # Extract class number and name, entries, absentees
            # Example: "Class 883. Minor Puppy Dog (Entries: 9 Absentees: 2)"
            parts = line.split(" ", 2)  # ["Class", "883.", "Minor Puppy Dog (Entries: 9 Absentees: 2)"]
            if len(parts) >= 3:
                # Remove trailing period from class number
                class_num = parts[1].rstrip(".")
                # Split class name and entries/abs if present
                if "(" in parts[2]:
                    name_part, stats_part = parts[2].split("(", 1)
                    class_name = name_part.strip()
                    stats_part = stats_part.rstrip(")")
                    # stats_part like "Entries: 9 Absentees: 2"
                    entries = absentees = ""
                    for stat in stats_part.split():
                        if stat.lower().startswith("entries:"):
                            # The number will be next token
                            # Actually, stat might equal "Entries:" then the number is next in list
                            # Safer to handle outside loop:
                            pass
                    # Simpler: use split on 'Absentees:' 
                    stats_tokens = stats_part.split("Absentees:")
                    if len(stats_tokens) == 2:
                        entries_part = stats_tokens[0].strip()
                        abs_part = stats_tokens[1].strip()
                        # entries_part like "Entries: 9" 
                        if ":" in entries_part:
                            entries = entries_part.split(":")[1].strip()
                        if ":" in abs_part:
                            absentees = abs_part.split(":")[1].strip()
                    else:
                        # If no absentees (e.g., 0), stats might not have "Absentees"
                        if stats_part.startswith("Entries:"):
                            entries = stats_part.split(":")[1].strip()
                        absentees = "0"
                else:
                    # No entries info in line
                    class_name = parts[2].strip()
                    entries = absentees = ""
                current_class = class_name
                class_entries = entries
                class_absentees = absentees
            else:
                current_class = line  # fallback: treat entire line as class name
                class_entries = class_absentees = ""
            continue
        # Check for breed-level awards (CCs, BOB, etc.)
        if " for " in line and line.lower().endswith("cc"):
            # Lines like "1234 - DogName (Owner) for Dog CC"
            # Split at ' for '
            main_part, award_part = line.split(" for ", 1)
            award = award_part.strip()
            # Extract dog name and owners from main_part (which contains number - name (owner))
            # Remove catalogue number and hyphen
            if " - " in main_part:
                _, doginfo = main_part.split(" - ", 1)
            else:
                doginfo = main_part
            # Separate dog name and owners
            doginfo = doginfo.strip()
            if doginfo.endswith(")") and "(" in doginfo:
                # Owners are in parentheses at the end
                idx = doginfo.rfind("(")
                dog_name = doginfo[:idx].strip().rstrip(",")
                owners = doginfo[idx+1:-1].strip()
            else:
                dog_name = doginfo
                owners = ""
            result = {
                "Show": show_name,
                "Date": show_date,
                "Breed": "Retriever (Golden)",
                "Class/Award": award,  # e.g. "Dog CC"
                "Placement": "",      # no numeric placement for awards
                "Dog": dog_name,
                "Owner(s)": owners,
                "Entries": "",
                "Absentees": ""
            }
            results.append(result)
            continue
        if line.lower().startswith("best"):
            # Lines like "Best of Breed: 1234 - DogName (Owner)" or "Best Puppy:" etc.
            if ":" in line:
                award, main_part = line.split(":", 1)
            else:
                award = line
                main_part = ""
            award = award.strip()
            main_part = main_part.strip()
            # Remove catalogue number from main_part if present
            if " - " in main_part:
                _, doginfo = main_part.split(" - ", 1)
            else:
                doginfo = main_part
            doginfo = doginfo.strip()
            if doginfo.endswith(")") and "(" in doginfo:
                idx = doginfo.rfind("(")
                dog_name = doginfo[:idx].strip().rstrip(",")
                owners = doginfo[idx+1:-1].strip()
            else:
                dog_name = doginfo or ""
                owners = ""
            result = {
                "Show": show_name,
                "Date": show_date,
                "Breed": "Retriever (Golden)",
                "Class/Award": award,  # e.g. "Best of Breed"
                "Placement": "",
                "Dog": dog_name,
                "Owner(s)": owners,
                "Entries": "",
                "Absentees": ""
            }
            results.append(result)
            continue
        # Check for group placements within Golden section (if any Golden in group was listed here)
        # They might appear as lines starting with "Group - 1st Place: ..." if included.
        if line.startswith("Group -"):
            # Remove "Group - " prefix and process like class placement
            placement_part = line[len("Group -"):].strip()
            # placement_part like "1st Place: 2749 - DogName, Breed: Retriever (Golden) (Owner)"
            # Remove breed mention
            # Split at ":"
            if ":" in placement_part:
                place_label, rest = placement_part.split(":", 1)
            else:
                place_label, rest = placement_part, ""
            place_label = place_label.strip()  # e.g. "1st Place"
            rest = rest.strip()
            # Remove breed substring if present
            # e.g. rest = "2749 - Name, Breed: Retriever (Golden) (Owner)"
            if "Breed: Retriever (Golden)" in rest:
                rest = rest.replace("Breed: Retriever (Golden)", "")
            # Remove double commas or leftover punctuation
            rest = rest.replace(",,", ",").strip().lstrip("-").strip()
            # Now rest should be like "2749 - DogName (Owner)"
            if " - " in rest:
                _, doginfo = rest.split(" - ", 1)
            else:
                doginfo = rest
            doginfo = doginfo.strip()
            if doginfo.endswith(")") and "(" in doginfo:
                idx = doginfo.rfind("(")
                dog_name = doginfo[:idx].strip().rstrip(",")
                owners = doginfo[idx+1:-1].strip()
            else:
                dog_name = doginfo
                owners = ""
            # Determine if this is overall group or special (check if current_class has 'Special' or by context)
            # We can use the current_class variable if it was set to something like "Special Beginners" before group lines, but group lines are outside class context.
            # Instead, infer from owners string or presence of certain keywords in place_label (doesn't contain).
            # Simpler: if 'Special Beginners' was part of a heading earlier, the code above might not capture it distinctly.
            # However, group placements likely appear outside any class heading, we can just label them as Gundog Group or Gundog Group (Special Beginners).
            group_name = "Gundog Group"
            # If the Golden group placements are from a Special Beginners group, the judge line would have indicated it (but we might not have captured it).
            # We can check if any earlier line (just before group lines in breed_section) contains "Special Beginners".
            # Let's backtrack a bit in breed_section for "Special Beginners - Judge".
            # (Implement simple check: see if the previous stored line contains "Special Beginners - Judge")
            # Note: breed_section is sequential; once we hit group lines, the line before might be a judge line.
            # We'll scan a few lines back from current line in breed_section list:
            prev_idx = breed_section.index(line) - 1
            if prev_idx >= 0 and "Special Beginners" in breed_section[prev_idx]:
                group_name = "Gundog Group (Special Beginners)"
            result = {
                "Show": show_name,
                "Date": show_date,
                "Breed": "Retriever (Golden)",
                "Class/Award": group_name,
                "Placement": place_label,
                "Dog": dog_name,
                "Owner(s)": owners,
                "Entries": "",
                "Absentees": ""
            }
            results.append(result)
            continue
        # Otherwise, handle regular class placements (1st, 2nd, 3rd, Reserve, VHC in classes)
        # These lines typically start with "1st", "2nd", "Reserve", etc.
        # Identify placement labels by presence of a colon after them.
        if ":" in line:
            label, rest = line.split(":", 1)
            placement_label = label.strip()  # e.g. "1st Place" or "Reserve (4th Place)"
            rest = rest.strip()
            # Remove catalogue number and hyphen
            if " - " in rest:
                _, doginfo = rest.split(" - ", 1)
            else:
                doginfo = rest
            doginfo = doginfo.strip()
            # Remove any trailing comma before parentheses (from stray formatting)
            if doginfo.endswith(")") and "(" in doginfo:
                idx = doginfo.rfind("(")
                dog_name = doginfo[:idx].strip().rstrip(",")
                owners = doginfo[idx+1:-1].strip()
            else:
                dog_name = doginfo
                owners = ""
            # Use current_class as class name (if available)
            class_name = current_class if current_class else ""
            result = {
                "Show": show_name,
                "Date": show_date,
                "Breed": "Retriever (Golden)",
                "Class/Award": class_name,
                "Placement": placement_label,
                "Dog": dog_name,
                "Owner(s)": owners,
                "Entries": class_entries or "",
                "Absentees": class_absentees or ""
            }
            results.append(result)
            continue
    return results

def scrape_all_results(start_year=2007, end_year=None, output_csv="golden_retriever_results.csv"):
    """
    Scrape Golden Retriever results from all shows between start_year and end_year (inclusive).
    Writes the results to a CSV file specified by output_csv.
    """
    if end_year is None:
        from datetime import datetime
        end_year = datetime.now().year
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    # Load the initial results page to get hidden form fields
    resp = session.get(RESULTS_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    base_viewstate = soup.find("input", {"id": "__VIEWSTATE"})
    base_eventvalidation = soup.find("input", {"id": "__EVENTVALIDATION"})
    base_viewstategen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    base_viewstate = base_viewstate["value"] if base_viewstate else ""
    base_eventvalidation = base_eventvalidation["value"] if base_eventvalidation else ""
    base_viewstategen = base_viewstategen["value"] if base_viewstategen else ""
    all_results = []
    for year in range(start_year, end_year+1):
        try:
            show_list = get_year_show_list(session, year, base_viewstate, base_eventvalidation, base_viewstategen)
        except Exception as e:
            print(f"Error retrieving show list for year {year}: {e}")
            continue
        for show_name, show_date, show_url in show_list:
            try:
                show_results = scrape_show_results(session, show_name, show_date, show_url)
            except Exception as e:
                print(f"Error scraping show {show_name} ({show_date}): {e}")
                continue
            if show_results:
                all_results.extend(show_results)
    # Write all results to CSV
    fieldnames = ["Show", "Date", "Breed", "Class/Award", "Placement", "Dog", "Owner(s)", "Entries", "Absentees"]
    with open(output_csv, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)

# Constants
RESULTS_URL = "https://www.fossedata.co.uk/show-results/"
