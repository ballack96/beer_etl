import requests
import json
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import time


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

    output_path = Path("beer_etl_project/include/data/raw")
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"catalog_breweries_{timestamp}.json"

    with open(output_file, "w") as f:
        json.dump(breweries, f, indent=2)

    print(f"✅ Extracted {len(breweries)} breweries from catalog.beer to {output_file}")

######################################################
## Scrape hop data from https://beermaverick.com    ##
######################################################
BASE_URL = "https://beermaverick.com"
LIST_URL = f"{BASE_URL}/hops/"
SEM_LIMIT = 10

def fetch_hop_links():
    """Scrape hop name, origin, purpose, and detail URL from beermaverick.com/hops"""
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
                url = a_tag["href"]
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
    return asyncio.run(fetch_hops_data_async())