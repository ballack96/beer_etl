import json
from pathlib import Path
from datetime import datetime
from textblob import TextBlob

def transform_beer_data():
    # Fixed paths - removed "beer_etl_project/" prefix
    raw_path = Path("include/data/raw")
    processed_path = Path("include/data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)

    json_files = sorted(raw_path.glob("beer_styles_*.json"), reverse=True)
    if not json_files:
        print("No raw data files found.")
        return

    latest_file = json_files[0]
    with open(latest_file, "r") as f:
        beer_styles = json.load(f)

    transformed = []

    for item in beer_styles:
        sentiment_score = TextBlob(item["name"]).sentiment.polarity
        transformed.append({
            "style_name": item["name"],
            "avg_rating": float(item["avg_rating"]) if item["avg_rating"] else None,
            "min_abv": float(item["min_abv"].replace("%", "")) if item["min_abv"] else None,
            "max_abv": float(item["max_abv"].replace("%", "")) if item["max_abv"] else None,
            "num_beers": int(item["num_beers"].replace(",", "")) if item["num_beers"] else None,
            "sentiment": sentiment_score
        })

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = processed_path / f"beer_styles_transformed_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(transformed, f, indent=2)

    print(f"Transformed {len(transformed)} records saved to {output_file}")