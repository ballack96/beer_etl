import sys
import os
import duckdb

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from extract.hop_scraper import fetch_hops_data

def main():
    print("⏳ Fetching hops data...")
    df = fetch_hops_data()
    print(f"✅ Scraped {len(df)} hops. Sample:")
    print(df.head())

    db_path = os.path.join(os.path.dirname(__file__), "../data/hops.duckdb")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)
    con.execute("CREATE OR REPLACE TABLE hops AS SELECT * FROM df")
    con.close()
    print(f"🎉 Done. Data stored in: {db_path}")

if __name__ == "__main__":
    main()
