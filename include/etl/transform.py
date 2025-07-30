import json
from pathlib import Path
from datetime import datetime
from textblob import TextBlob
import pandas as pd
import re

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

######################################################
## Transform hop data from https://beermaverick.com ##
######################################################
def extract_numeric_range_avg(value):
    """
    Extract min, max, avg values from strings like:
    "10-14% 12% avg" or "0.8-3.9 mL 2.4mL avg"
    Returns: tuple of (min, max, avg) as floats or None
    """
    if pd.isna(value) or not isinstance(value, str):
        return None, None, None

    # Remove units and lowercase
    value = value.replace('%', '').replace('mL', '').lower()
    
    # Match patterns
    range_match = re.search(r'(\d+(\.\d+)?)[\s-]+(\d+(\.\d+)?)', value)
    avg_match = re.search(r'(\d+(\.\d+)?)[\s]*avg', value)

    val_min = float(range_match.group(1)) if range_match else None
    val_max = float(range_match.group(3)) if range_match else None
    val_avg = float(avg_match.group(1)) if avg_match else None

    return val_min, val_max, val_avg


def transform_hop_data(df):
    """
    Transforms raw hop data extracted from BeerMaverick into structured form.
    """
    df = df.copy()
    
    # Columns to extract ranges and averages from
    range_cols = [
        'alpha_acid', 'beta_acid', 'cohumulone', 'total_oil',
        'myrcene', 'humulene', 'caryophyllene', 'farnesene', 'others_oil'
    ]

    for col in range_cols:
        df[[f"{col}_min", f"{col}_max", f"{col}_avg"]] = df[col].apply(
            lambda x: pd.Series(extract_numeric_range_avg(x))
        )

    # Drop original raw columns if needed
    # df.drop(columns=range_cols, inplace=True)

    # Normalize unknowns and blanks
    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)
    return df


###############################################################
## Transform fermentables data from https://beermaverick.com ##
###############################################################
def transform_fermentables_data(df):
    """
    Transforms raw fermentables data into clean schema
    """
    df = df.copy()

    def extract_pct(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d{1,3}(\.\d+)?)%', val)
        return float(match.group(1)) if match else None

    def extract_srm(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d+(\.\d+)?)', val)
        return float(match.group(1)) if match else None

    def extract_ppg(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d{2})\s*ppg', val, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def extract_dpower(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d+(\.\d+)?)\s*°?\s*lintner', val, re.IGNORECASE)
        return float(match.group(1)) if match else None

    df['yield_pct'] = df['potential_yield'].apply(extract_pct) if 'potential_yield' in df else None
    df['ppg'] = df['potential_yield'].apply(extract_ppg) if 'potential_yield' in df else None
    df['max_usage_pct'] = df['max_usage'].apply(extract_pct) if 'max_usage' in df else None
    df['srm'] = df['srm'].apply(extract_srm) if 'srm' in df else None
    df['diastatic_power'] = df['diastatic_power'].apply(extract_dpower) if 'diastatic_power' in df else None

    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)

    return df


######################################################### 
## Transform yeasts data from https://beermaverick.com ##
######################################################### 
def transform_yeast_data(df):
    """
    Transforms raw yeast data into clean schema
    """
    df = df.copy()

    def extract_abv_range(val):
        if pd.isna(val):
            return None, None
        match = re.search(r'(\d+(\.\d+)?)\s*-\s*(\d+(\.\d+)?)\s*%', val)
        if match:
            return float(match.group(1)), float(match.group(3))
        return None, None

    df[['min_abv', 'max_abv']] = df['abv'].apply(
        lambda x: pd.Series(extract_abv_range(x))
    )

    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)

    return df
