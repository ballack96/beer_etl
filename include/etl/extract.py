import requests
import json
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import time
import asyncio
import aiohttp

######################################################
## Beer data from catalog.beer                      ##
######################################################
def extract_beer_data():
    base_url = "https://api.catalog.beer/brewer"
    breweries = []
    page = 1
    per_page = 50

    while True:
        response = requests.get(base_url, params={"page": page, "per_page": per_page})
        if response.status_code != 200:
            raise Exception(f"Failed on page {page} with status code {response.status_code}")

        data = response.json()
        breweries.extend(data["data"])

        if page >= data["pagination"]["last_visible_page"]:
            break

        page += 1

    # Fixed path - removed "beer_etl_project/" prefix
    output_path = Path("include/data/raw")
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"catalog_breweries_{timestamp}.json"

    with open(output_file, "w") as f:
        json.dump(breweries, f, indent=2)

    print(f"✅ Extracted {len(breweries)} breweries from catalog.beer to {output_file}")

######################################################
## Beer style data from BJCP                        ##
######################################################
STYLES_URL = "https://raw.githubusercontent.com/ascholer/bjcp-styleview/main/styles.json"

def fetch_styles() -> pd.DataFrame:
    """
    Fetches the BJCP beer styles JSON and normalizes into a flat DataFrame.
    """
    resp = requests.get(url=STYLES_URL)
    resp.raise_for_status()
    data = resp.json()

    # Normalize nested JSON into flat table
    df_styles = pd.json_normalize(data)
    return df_styles

######################################################
## Scrape hop data from https://beermaverick.com    ##
######################################################
BASE_URL = "https://beermaverick.com"
LIST_URL = f"{BASE_URL}/hops/"
SEM_LIMIT = 10

def fetch_hop_links():
    """Scrape hop name, origin, purpose, and full URL from beermaverick.com/hops"""
    import requests
    res = requests.get(LIST_URL)
    soup = BeautifulSoup(res.text, "html.parser")

    hops = []
    tables = soup.find_all("table")
    for table in tables:
        origin_tag = table.find("h4")
        if not origin_tag:
            continue
        origin = origin_tag.get_text(strip=True)
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                a_tag = cols[0].find("a")
                name = a_tag.get_text(strip=True)
                href = a_tag["href"].strip()
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                purpose = cols[1].get_text(strip=True)
                hops.append({
                    "name": name,
                    "url": url,
                    "purpose": purpose,
                    "origin": origin
                })
    return hops


def parse_hop_page(hop, soup: BeautifulSoup):
    """Extract hop details from a hop detail page HTML soup"""
    data = {
        "name": hop["name"],
        "origin": hop["origin"],
        "purpose": hop["purpose"],
        "alpha_acid": None,
        "beta_acid": None,
        "cohumulone": None,
        "total_oil": None,
        "myrcene": None,
        "humulene": None,
        "caryophyllene": None,
        "farnesene": None,
        "others_oil": None,
        "descriptors": None,
        "substitutes": None,
    }

    # Find stats table
    table = soup.find("table", class_="brewvalues")
    if not table:
        return data

    rows = table.find_all("tr")
    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue
        label = th.get_text(strip=True).lower()
        value = td.get_text(" ", strip=True)

        if "alpha acid" in label:
            data["alpha_acid"] = value
        elif "beta acid" in label:
            data["beta_acid"] = value
        elif "co-humulone" in label:
            data["cohumulone"] = value
        elif "total oils" in label:
            data["total_oil"] = value
        elif "myrcene" in label:
            data["myrcene"] = value
        elif "humulene" in label:
            data["humulene"] = value
        elif "caryophyllene" in label:
            data["caryophyllene"] = value
        elif "farnesene" in label:
            data["farnesene"] = value
        elif "all others" in label:
            data["others_oil"] = value

    # Descriptors
    descriptors_section = soup.find("div", id="descriptors")
    if descriptors_section:
        tags = descriptors_section.find_all("span", class_="tagtext")
        data["descriptors"] = ", ".join(tag.get_text(strip=True) for tag in tags)

    # Substitutes
    sub_section = soup.find("div", id="subs")
    if sub_section:
        sub_tags = sub_section.find_all("a")
        data["substitutes"] = ", ".join(tag.get_text(strip=True) for tag in sub_tags)

    return data


async def fetch_and_parse_hop(hop, session, semaphore):
    async with semaphore:
        try:
            async with session.get(hop["url"]) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                parsed = parse_hop_page(hop, soup)
                return parsed
        except Exception as e:
            print(f"❌ Error scraping {hop['url']}: {e}")
            return None


async def fetch_hops_data_async(sleep_interval=0.0):
    hop_links = fetch_hop_links()
    hops = []
    semaphore = asyncio.Semaphore(SEM_LIMIT)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_parse_hop(hop, session, semaphore) for hop in hop_links]
        for future in asyncio.as_completed(tasks):
            result = await future
            if result:
                hops.append(result)
            if sleep_interval > 0:
                await asyncio.sleep(sleep_interval)

    return pd.DataFrame(hops)


def fetch_hops_data():
    try:
        return asyncio.run(fetch_hops_data_async())
    except RuntimeError as e:
        if "asyncio.run() cannot be called" in str(e):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(fetch_hops_data_async())
        else:
            raise


#######################################################
## Scrape fermentables from https://beermaverick.com ##
#######################################################
FERMENTABLES_URL = f"{BASE_URL}/fermentables/"
def fetch_fermentables_list():
    """Scrape name, type, and URL from the fermentables listing table."""
    import requests
    res = requests.get(FERMENTABLES_URL)
    soup = BeautifulSoup(res.text, "html.parser")

    data = []
    table = soup.find("table")
    if not table:
        raise ValueError("Fermentables table not found")

    current_type = None
    for row in table.find_all("tr"):
        th = row.find("th")
        td = row.find("td")

        if th:
            current_type = th.get_text(strip=True)
        elif td:
            a_tag = td.find("a")
            if a_tag:
                name = a_tag.get_text(strip=True)
                href = a_tag.get("href").strip()
                url = href if href.startswith("http") else f"https://beermaverick.com{href}"
                data.append({
                    "name": name,
                    "url": url,
                    "type": current_type
                })
    return data


async def fetch_fermentable_detail(row, session, semaphore):
    async with semaphore:
        if not isinstance(row, dict):
            print(f"⚠️ Expected dict, got: {type(row)} → {row}")
            return {}

        try:
            print(f"⏳ Fetching: {row['url']}")
            async with session.get(row["url"]) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                table = soup.find("table", class_="brewvalues")
                if not table:
                    return row

                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if not th or not td:
                        continue
                    label = th.get_text(strip=True).lower()
                    value = td.get_text(strip=True)

                    if "color" in label or "srm" in label:
                        row["srm"] = value.replace("SRM", "").strip()
                    elif "potential yield" in label or "ppg" in label:
                        row["potential_yield"] = value
                    elif "diastatic power" in label:
                        row["diastatic_power"] = value
                    elif "batch max" in label or "max percentage" in label:
                        row["max_usage"] = value
        except Exception as e:
            print(f"❌ Error fetching {row.get('url', 'unknown')}: {e}")
        return row


async def fetch_fermentables_async():
    rows = fetch_fermentables_list()
    assert isinstance(rows[0], dict), f"Expected dict rows, got {type(rows[0])}"
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_fermentable_detail(row, session, semaphore) for row in rows]
        enriched = await asyncio.gather(*tasks)
    return pd.DataFrame(enriched)


def fetch_fermentables():
    return asyncio.run(fetch_fermentables_async())


#########################################################
## Scrape yeasts data from https://beermaverick.com    ##
#########################################################
YEASTS_URL = f"{BASE_URL}/yeasts/"
def fetch_yeasts_list():
    """Scrape yeast name, type, and URL from the yeast listing table."""
    import requests
    res = requests.get(YEASTS_URL)
    soup = BeautifulSoup(res.text, "html.parser")

    data = []
    table = soup.find("table")
    if not table:
        raise ValueError("Yeasts table not found")

    current_type = None
    for row in table.find_all("tr"):
        th = row.find("th")
        td = row.find("td")

        if th:
            current_type = th.get_text(strip=True)
        elif td:
            a_tag = td.find("a")
            if a_tag:
                name = a_tag.get_text(strip=True)
                href = a_tag.get("href").strip()
                url = href if href.startswith("http") else f"https://beermaverick.com{href}"
                data.append({
                    "name": name,
                    "url": url,
                    "type": current_type
                })
    return data

async def fetch_yeast_detail(row, session, semaphore):
    async with semaphore:
        if not isinstance(row, dict):
            print(f"⚠️ Expected dict, got: {type(row)} → {row}")
            return {}

        try:
            print(f"⏳ Fetching: {row['url']}")
            async with session.get(row["url"]) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                table = soup.find("table", class_="brewvalues")
                if not table:
                    return row

                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if not th or not td:
                        continue
                    label = th.get_text(strip=True).lower()
                    value = td.get_text(strip=True)

                    if "attenuation" in label:
                        row["attenuation"] = value
                    elif "flocculation" in label:
                        row["flocculation"] = value
                    elif "alcohol tolerance" in label:
                        row["alcohol_tolerance"] = value
        except Exception as e:
            print(f"❌ Error fetching {row.get('url', 'unknown')}: {e}")
        return row
    
async def fetch_yeasts_async():
    rows = fetch_yeasts_list()
    assert isinstance(rows[0], dict), f"Expected dict rows, got {type(rows[0])}"
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_yeast_detail(row, session, semaphore) for row in rows]
        enriched = await asyncio.gather(*tasks)
    return pd.DataFrame(enriched)

def fetch_yeasts_data():
    return asyncio.run(fetch_yeasts_async())