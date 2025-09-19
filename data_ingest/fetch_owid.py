"""
Fetch and save OWID CO2 data:
- download raw CSV to data/raw
- tidy + standardise columns
- write year-partitioned Parquet to data/processed/owid_co2
"""

import os, sys
import io
import pandas as pd
import requests

# import config for year bounds
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import START_YEAR, END_YEAR

RAW_DIR = "data/raw"
RAW_CSV = os.path.join(RAW_DIR, "owid_co2_data.csv")

PROC_DIR = "data/processed/owid_co2"  # partitioned as year=YYYY.parquet
OWID_URL = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"

# minimal column set
RENAME = {
    "iso_code": "iso3",
    "country": "country",
    "year": "year",
    "co2": "co2_mt",                        # Mt CO2 (territorial)
    "co2_per_capita": "co2_t_per_capita",   # t/person
    "share_global_co2": "share_global_co2_pct",
    "consumption_co2": "consumption_co2_mt",
}

KEEP = list(RENAME.values())

def download_raw():
    os.makedirs(RAW_DIR, exist_ok=True)
    r = requests.get(OWID_URL, timeout=60)
    r.raise_for_status()
    with open(RAW_CSV, "wb") as f:
        f.write(r.content)
    print(f"Downloaded → {RAW_CSV}")

def process_and_partition():
    os.makedirs(PROC_DIR, exist_ok=True)
    df = pd.read_csv(RAW_CSV)

    # rename + select
    df = df.rename(columns=RENAME)
    df = df[[c for c in KEEP if c in df.columns]].copy()

    # basic cleaning
    df["year"] = df["year"].astype(int)
    df["iso3"] = df["iso3"].str.upper()

    # drop aggregates (OWID_*, regions like EU27, etc.); keep ISO-3 only
    df = df[df["iso3"].str.len() == 3]
    df = df[~df["iso3"].str.startswith("OWID")]

    # bound by config years
    end = df["year"].max() if END_YEAR is None else END_YEAR
    df = df[(df["year"] >= START_YEAR) & (df["year"] <= end)]

    # write partitioned by year
    for y, part in df.groupby("year"):
        outp = os.path.join(PROC_DIR, f"year={int(y)}.parquet")
        part.to_parquet(outp, index=False)

    print(f"Wrote {df['year'].nunique()} years to {PROC_DIR}")

if __name__ == "__main__":
    download_raw()
    process_and_partition()
