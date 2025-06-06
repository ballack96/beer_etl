import requests
import json
from pathlib import Path
from datetime import datetime

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