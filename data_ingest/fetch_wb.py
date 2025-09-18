# data_ingest/fetch_wb_wbgapi.py
import os, sys
import pandas as pd
import wbgapi as wb

# import START_YEAR / END_YEAR
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import START_YEAR, END_YEAR

OUT_DIR = "data/processed/worldbank"

# indicator_code -> short column name
INDICATORS = {
    "NY.GDP.PCAP.KD": "gdp_pc_const_usd",
    "GE.EST": "wgi_gov_effectiveness",
    "RL.EST": "wgi_rule_of_law",
    "RQ.EST": "wgi_reg_quality",
    "CC.EST": "wgi_control_corruption",
    "PV.EST": "wgi_political_stability",
    "VA.EST": "wgi_voice_accountability",
}

def fetch_worldbank() -> pd.DataFrame:
    years = range(START_YEAR, (END_YEAR or wb.time.end) + 1)

    # Wide format with codes as columns; expect id columns + yrYYYY columns
    df = wb.data.DataFrame(list(INDICATORS.keys()), time=years, labels=False).reset_index()
    df.columns = [str(c).lower() for c in df.columns]

    # Identify the economy (ISO3) id column and normalise to 'iso3'
    econ_col = "iso3" if "iso3" in df.columns else ("economy" if "economy" in df.columns else None)
    if econ_col is None or "series" not in df.columns:
        raise KeyError(f"Unexpected columns from wbgapi: {df.columns.tolist()}")
    df.rename(columns={econ_col: "iso3"}, inplace=True)

    # Keep only ISO-3 codes (skip aggregates like WLD/EUU)
    df = df[df["iso3"].astype(str).str.len() == 3].copy()

    # Melt wide → long (iso3, series, year, value)
    year_cols = [c for c in df.columns if c.startswith("yr")]
    long_ = df.melt(id_vars=["iso3", "series"], value_vars=year_cols,
                    var_name="year", value_name="value")
    long_["year"] = pd.to_numeric(long_["year"].str.removeprefix("yr"), errors="coerce")

    # Pivot series codes → columns
    wide = long_.pivot_table(index=["iso3", "year"], columns="series",
                             values="value", aggfunc="first").reset_index()

    # Rename indicator code columns to short names (only those that exist)
    rename_map = {code: short for code, short in INDICATORS.items() if code in wide.columns}
    wide = wide.rename(columns=rename_map)

    # Final column order (only those present)
    want = ["iso3", "year"] + list(rename_map.values())
    wide = (wide[[c for c in want if c in wide.columns]]
            .sort_values(["iso3", "year"])
            .drop_duplicates(["iso3", "year"]))

    return wide

def write_partitioned(df: pd.DataFrame, out_dir: str = OUT_DIR):
    os.makedirs(out_dir, exist_ok=True)
    for y, part in df.groupby("year", dropna=True):
        part.to_parquet(os.path.join(out_dir, f"year={int(y)}.parquet"), index=False)

if __name__ == "__main__":
    data = fetch_worldbank()
    write_partitioned(data)
    print(data.head(6))
    print(f"Wrote {data['year'].nunique()} years → {OUT_DIR}")
