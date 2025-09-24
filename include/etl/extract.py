import requests
import json
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import time
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
import boto3
import xml.etree.ElementTree as ET
from typing import List, Dict
from airflow.hooks.base import BaseHook

###########################################################
## Clone recipe data from https://www.brewersfriend.com  ##
###########################################################
BASE = "https://www.brewersfriend.com"
URL_TPL = BASE + "/homebrew-recipes/page/{page}/"

def parse_views(v: str) -> int:
    """
    Turns strings like '1.2K', '3,456', '2M' into integer view-counts.
    """
    v = v.strip().upper()
    if v.endswith("K"):
        return int(float(v.rstrip("K")) * 1_000)
    if v.endswith("M"):
        return int(float(v.rstrip("M")) * 1_000_000)
    return int(v.replace(",", ""))

def fetch_clone_recipes(max_pages: int = 30) -> pd.DataFrame:    
    """
    Scrape up to `max_pages` pages of BrewersFriend clone recipes and return a DataFrame.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))

    all_data = []

    for page in range(1, max_pages + 1):
        url = URL_TPL.format(page=page)
        time.sleep(random.uniform(1.0, 3.0))  # polite pacing

        resp = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            print(f"Stopping early: page {page} → {resp.status_code}")
            break

        soup  = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="ui table")
        if not table:
            print(f"No table on page {page}, stopping.")
            break

        rows = table.find_all("tr", class_=["odd", "even"])
        if not rows:
            print(f"No data rows on page {page}, stopping.")
            break

        for tr in rows:
            tds   = tr.find_all("td", recursive=False)
            title = tds[0].find("a", class_="recipetitle").get_text(strip=True)
            # -- only keep clone recipes --
            if "clone" not in title.lower():
                continue

            all_data.append({
                "page":   page,
                "title":  title,
                "url":    BASE + tds[0].find("a", class_="recipetitle")["href"],
                "style":  tds[1].get_text(strip=True),
                "batch_size":   tds[2].get_text(strip=True),
                "og":     tds[3].get_text(strip=True),
                "fg":     tds[4].get_text(strip=True),
                "abv":    tds[5].get_text(strip=True),
                "ibu":    tds[6].get_text(strip=True),
                "color":  tds[7].get_text(strip=True),
                "views":  parse_views(tds[8].get_text(strip=True)),
                "brewed": tds[9].get_text(strip=True)
            })

        print(f"Page {page} scraped ({len(rows)} recipes), total so far: {len(all_data)}")

    df = pd.DataFrame(all_data)
    return df

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


#########################################################
## Extract BeerXML files from S3 bucket               ##
#########################################################
def fetch_beerxml_from_s3(bucket_name: str = "beer-etl", prefix: str = "raw-clone-recipes/clone-recipes/", connection_id: str = "aws-s3-user") -> List[Dict]:
    """
    Fetches BeerXML files from S3 bucket and extracts recipe data.
    
    Args:
        bucket_name: S3 bucket name (default: "beer-etl")
        prefix: S3 prefix for BeerXML files (default: "raw-clone-recipes/clone-recipes/")
        connection_id: Airflow connection ID for AWS credentials (default: "aws-s3-user")
    
    Returns:
        List of dictionaries containing parsed BeerXML recipe data
    """
    # Get AWS connection from Airflow
    try:
        aws_connection = BaseHook.get_connection(connection_id)
        print(f"✅ Successfully retrieved AWS connection: {connection_id}")
    except Exception as e:
        print(f"❌ Failed to retrieve AWS connection '{connection_id}': {str(e)}")
        raise
    
    # Extract credentials from connection
    aws_access_key_id = aws_connection.login
    aws_secret_access_key = aws_connection.password
    region_name = aws_connection.extra_dejson.get('region_name', 'us-east-1')
    
    # Validate credentials
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(f"❌ Missing AWS credentials in connection '{connection_id}'. Please check Login (Access Key ID) and Password (Secret Access Key) fields.")
    
    print(f"🔑 Using AWS region: {region_name}")
    
    # Initialize S3 client with Airflow connection credentials
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        print(f"✅ Successfully initialized S3 client")
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {str(e)}")
        raise
    
    recipes_data = []
    
    try:
        # List all objects in the S3 bucket with the given prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                # Only process .xml files
                if obj['Key'].endswith('.xml'):
                    print(f"Processing BeerXML file: {obj['Key']}")
                    
                    try:
                        # Download the XML content
                        response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                        xml_content = response['Body'].read().decode('utf-8')
                        
                        # Parse the BeerXML and extract recipe data
                        recipe_data = parse_beerxml_content(xml_content, obj['Key'])
                        
                        if recipe_data:
                            # Add XML content to each recipe for flattening
                            for recipe in recipe_data:
                                recipe['xml_content'] = xml_content
                            recipes_data.extend(recipe_data)
                            
                    except Exception as e:
                        print(f"Error processing {obj['Key']}: {str(e)}")
                        continue
    
    except Exception as e:
        print(f"Error accessing S3 bucket: {str(e)}")
        raise
    
    print(f"Successfully extracted {len(recipes_data)} recipes from BeerXML files")
    return recipes_data


def parse_beerxml_content(xml_content: str, file_key: str) -> List[Dict]:
    """
    Parse BeerXML content and extract recipe information.
    
    Args:
        xml_content: Raw XML content as string
        file_key: S3 object key for reference
    
    Returns:
        List of recipe dictionaries
    """
    recipes = []
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Handle BeerXML namespace
        namespace = {'beerxml': 'http://www.beerxml.com/beerxml_0.9'}
        
        # Find all recipe elements
        recipe_elements = root.findall('.//beerxml:RECIPE', namespace) or root.findall('.//RECIPE')
        
        for recipe_elem in recipe_elements:
            recipe_data = extract_recipe_data(recipe_elem, file_key, namespace)
            if recipe_data:
                recipes.append(recipe_data)
                
    except ET.ParseError as e:
        print(f"XML parsing error for {file_key}: {str(e)}")
    except Exception as e:
        print(f"Error parsing BeerXML content for {file_key}: {str(e)}")
    
    return recipes


def extract_recipe_data(recipe_elem, file_key: str, namespace: Dict) -> Dict:
    """
    Extract recipe data from a BeerXML RECIPE element.
    
    Args:
        recipe_elem: XML element containing recipe data
        file_key: S3 object key for reference
        namespace: XML namespace mapping
    
    Returns:
        Dictionary containing extracted recipe data
    """
    try:
        # Helper function to safely get text content
        def get_text(elem, xpath, default=None):
            try:
                if namespace:
                    found = elem.find(xpath, namespace)
                else:
                    found = elem.find(xpath)
                return found.text if found is not None and found.text else default
            except:
                return default
        
        # Helper function to get numeric value
        def get_numeric(elem, xpath, default=None):
            try:
                text = get_text(elem, xpath, default)
                if text and text != default:
                    return float(text)
                return default
            except:
                return default
        
        # Extract basic recipe information
        recipe_data = {
            'file_key': file_key,
            'recipe_name': get_text(recipe_elem, 'beerxml:NAME') or get_text(recipe_elem, 'NAME', ''),
            'recipe_type': get_text(recipe_elem, 'beerxml:TYPE') or get_text(recipe_elem, 'TYPE', ''),
            'brewer': get_text(recipe_elem, 'beerxml:BREWER') or get_text(recipe_elem, 'BREWER', ''),
            'batch_size': get_numeric(recipe_elem, 'beerxml:BATCH_SIZE') or get_numeric(recipe_elem, 'BATCH_SIZE'),
            'boil_size': get_numeric(recipe_elem, 'beerxml:BOIL_SIZE') or get_numeric(recipe_elem, 'BOIL_SIZE'),
            'boil_time': get_numeric(recipe_elem, 'beerxml:BOIL_TIME') or get_numeric(recipe_elem, 'BOIL_TIME'),
            'efficiency': get_numeric(recipe_elem, 'beerxml:EFFICIENCY') or get_numeric(recipe_elem, 'EFFICIENCY'),
            'created_date': get_text(recipe_elem, 'beerxml:DATE') or get_text(recipe_elem, 'DATE', ''),
            'notes': get_text(recipe_elem, 'beerxml:NOTES') or get_text(recipe_elem, 'NOTES', ''),
        }
        
        # Extract style information
        style_elem = recipe_elem.find('beerxml:STYLE', namespace) or recipe_elem.find('STYLE')
        if style_elem is not None:
            recipe_data.update({
                'style_name': get_text(style_elem, 'beerxml:NAME') or get_text(style_elem, 'NAME', ''),
                'style_category': get_text(style_elem, 'beerxml:CATEGORY') or get_text(style_elem, 'CATEGORY', ''),
                'style_og_min': get_numeric(style_elem, 'beerxml:OG_MIN') or get_numeric(style_elem, 'OG_MIN'),
                'style_og_max': get_numeric(style_elem, 'beerxml:OG_MAX') or get_numeric(style_elem, 'OG_MAX'),
                'style_fg_min': get_numeric(style_elem, 'beerxml:FG_MIN') or get_numeric(style_elem, 'FG_MIN'),
                'style_fg_max': get_numeric(style_elem, 'beerxml:FG_MAX') or get_numeric(style_elem, 'FG_MAX'),
                'style_abv_min': get_numeric(style_elem, 'beerxml:ABV_MIN') or get_numeric(style_elem, 'ABV_MIN'),
                'style_abv_max': get_numeric(style_elem, 'beerxml:ABV_MAX') or get_numeric(style_elem, 'ABV_MAX'),
                'style_ibu_min': get_numeric(style_elem, 'beerxml:IBU_MIN') or get_numeric(style_elem, 'IBU_MIN'),
                'style_ibu_max': get_numeric(style_elem, 'beerxml:IBU_MAX') or get_numeric(style_elem, 'IBU_MAX'),
                'style_color_min': get_numeric(style_elem, 'beerxml:COLOR_MIN') or get_numeric(style_elem, 'COLOR_MIN'),
                'style_color_max': get_numeric(style_elem, 'beerxml:COLOR_MAX') or get_numeric(style_elem, 'COLOR_MAX'),
            })
        
        # Extract calculated values
        recipe_data.update({
            'calculated_og': get_numeric(recipe_elem, 'beerxml:OG') or get_numeric(recipe_elem, 'OG'),
            'calculated_fg': get_numeric(recipe_elem, 'beerxml:FG') or get_numeric(recipe_elem, 'FG'),
            'calculated_abv': get_numeric(recipe_elem, 'beerxml:ABV') or get_numeric(recipe_elem, 'ABV'),
            'calculated_ibu': get_numeric(recipe_elem, 'beerxml:IBU') or get_numeric(recipe_elem, 'IBU'),
            'calculated_color': get_numeric(recipe_elem, 'beerxml:COLOR') or get_numeric(recipe_elem, 'COLOR'),
        })
        
        # Count ingredients
        hops_count = len(recipe_elem.findall('beerxml:HOPS/beerxml:HOP', namespace) or 
                        recipe_elem.findall('HOPS/HOP', []) or 
                        recipe_elem.findall('HOPS', []) or [])
        fermentables_count = len(recipe_elem.findall('beerxml:FERMENTABLES/beerxml:FERMENTABLE', namespace) or 
                               recipe_elem.findall('FERMENTABLES/FERMENTABLE', []) or 
                               recipe_elem.findall('FERMENTABLES', []) or [])
        yeasts_count = len(recipe_elem.findall('beerxml:YEASTS/beerxml:YEAST', namespace) or 
                          recipe_elem.findall('YEASTS/YEAST', []) or 
                          recipe_elem.findall('YEASTS', []) or [])
        
        recipe_data.update({
            'hops_count': hops_count,
            'fermentables_count': fermentables_count,
            'yeasts_count': yeasts_count,
        })
        
        return recipe_data
        
    except Exception as e:
        print(f"Error extracting recipe data: {str(e)}")
        return None