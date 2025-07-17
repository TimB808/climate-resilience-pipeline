"""
Fetch and save GDP and governance indicators from World Bank API.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import wbdata
import pandas as pd
import datetime
from utils.io_utils import download_file

OUT_DIR = "data/raw"
OUT_PATH = os.path.join(OUT_DIR, "worldbank_indicators.csv")

# Define indicator codes
indicators = {
    "GDP_per_capita": "NY.GDP.PCAP.CD",
    "Government_effectiveness": "GE.EST",
    "Control_of_corruption": "CC.EST"
}

def fetch_worldbank_data():
    os.makedirs(OUT_DIR, exist_ok=True)

    countries = wbdata.get_country(display=False)
    country_codes = [c["id"] for c in countries if c["region"]["id"] != "NA"]

    print("Fetching World Bank indicators...")
    df = wbdata.get_dataframe(
        indicators,
        country=country_codes,
        convert_date=True,
        data_date=(datetime.datetime(2000, 1, 1), datetime.datetime(2023, 1, 1)),
        keep_levels=True
    )
    df.reset_index(inplace=True)
    df.rename(columns={"country": "Country", "date": "Year"}, inplace=True)

    df.to_csv(OUT_PATH, index=False)
    print(f"Saved to {OUT_PATH}")
    print(df.head())

if __name__ == "__main__":
    fetch_worldbank_data()
