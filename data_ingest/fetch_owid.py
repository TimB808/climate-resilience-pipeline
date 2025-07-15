"""
Fetch and save OWID CO2 and energy data from GitHub (CSV format).
"""

import pandas as pd
import os

OUT_DIR = "data/raw"
OUT_PATH = os.path.join(OUT_DIR, "owid_co2_data.csv")

def fetch_owid_co2_data():
    os.makedirs(OUT_DIR, exist_ok=True)

    url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"
    print("Downloading OWID CO₂ dataset...")
    df = pd.read_csv(url)
    df.to_csv(OUT_PATH, index=False)
    print(f"Saved to {OUT_PATH}")
    print(df.head())

if __name__ == "__main__":
    fetch_owid_co2_data()