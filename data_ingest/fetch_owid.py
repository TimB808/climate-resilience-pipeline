"""
Fetch and save OWID CO2 and energy data from GitHub (CSV format).
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from utils.io_utils import download_file
from config import START_YEAR, END_YEAR

OUT_DIR = "data/raw"
OUT_PATH = os.path.join(OUT_DIR, "owid_co2_data.csv")

def fetch_owid_co2_data():
    os.makedirs(OUT_DIR, exist_ok=True)

    url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"
    print(f"Downloading OWID CO₂ dataset (will be filtered to {START_YEAR}-{END_YEAR} in processing)...")
    download_file(url, OUT_PATH)
    df = pd.read_csv(OUT_PATH)
    df.to_csv(OUT_PATH, index=False)
    print(f"Saved to {OUT_PATH}")
    print(df.head())

if __name__ == "__main__":
    fetch_owid_co2_data()