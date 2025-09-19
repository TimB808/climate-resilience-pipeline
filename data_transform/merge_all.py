# merge_all.py
import os, glob
import pandas as pd
import pycountry
import unicodedata

ERA5_DIR = "data/processed/annual_temp"
OWID_DIR = "data/processed/owid_co2"
WB_DIR   = "data/processed/worldbank"
OUT_CSV  = "outputs/annual_country_enriched.csv"

def read_parquet_dir(path: str) -> pd.DataFrame:
    patterns = [
        os.path.join(path, "year=*/part.parquet"),  # nested (e.g., ERA5)
        os.path.join(path, "year=*.parquet"),       # flat (e.g., OWID/WB)
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    if not files:
        raise FileNotFoundError(f"No parquet partitions found in {path}")
    return pd.concat((pd.read_parquet(f) for f in sorted(files)), ignore_index=True)

def keep_iso3_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "iso3" in df.columns:
        df = df.dropna(subset=["iso3"])
        df.loc[:, "iso3"] = df["iso3"].astype(str).str.upper()
        df = df[df["iso3"].str.len() == 3]
    df.loc[:, "year"] = df["year"].astype(int)
    return df.drop_duplicates(["iso3", "year"])

# Manual overrides for tricky names
NAME_TO_ISO3 = {
    "Palestine": "PSE",
    "State of Palestine": "PSE",
    "Democratic Republic of the Congo": "COD",
    "Congo, Dem. Rep.": "COD",
    "Congo, Democratic Republic of the": "COD",
    "Republic of the Congo": "COG",
    "Congo": "COG",
    "Ivory Coast": "CIV",
    "Côte d'Ivoire": "CIV",
    "Cote d'Ivoire": "CIV",
    "Turkey": "TUR",
    "Türkiye": "TUR",
    "Russia": "RUS",
    "Russian Federation": "RUS",
    "Kosovo": "XKX",           # widely used though not ISO official
    "Sint Maarten": "SXM",
    "Saint Martin": "MAF",     # French part
    "Brunei": "BRN",
    "East Timor": "TLS",
    "Timor-Leste": "TLS",
    "Somaliland": "SML",    # custom pseudo-code (like XKX for Kosovo)
    "Aland": "ALA",
    "Falkland Islands": "FLK",
    "French Southern and Antarctic Lands": "ATF",
    "Hong Kong S.A.R.": "HKG",
    "Macao S.A.R": "MAC",
    "Northern Cyprus": "NCY",   # custom pseudo-code
    "Pitcairn Islands": "PCN",
    "Saint Barthelemy": "BLM",
    "Saint Helena": "SHN",
    "South Georgia and the Islands": "SGS",
    "United States Virgin Islands": "VIR",
    "Vatican": "VAT",
    "The Bahamas": "BHS",
    # No ISO3 → drop later:
    "Dhekelia Sovereign Base Area": None,
    
}

def _norm(s: str) -> str:
    s = s.strip()
    return unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')

def country_to_iso3(name: str) -> str:
    if not isinstance(name, str):
        return None
    # direct override match (raw and normalized)
    if name in NAME_TO_ISO3:
        return NAME_TO_ISO3[name]
    n = _norm(name)
    for k, v in NAME_TO_ISO3.items():
        if _norm(k) == n:
            return v
    # fallback to pycountry (raw then normalized)
    try:
        return pycountry.countries.lookup(name).alpha_3
    except LookupError:
        try:
            return pycountry.countries.lookup(n).alpha_3
        except LookupError:
            return None

def main():
    # --- Load datasets ---
    era5 = read_parquet_dir(ERA5_DIR)
    owid = read_parquet_dir(OWID_DIR)
    wb   = read_parquet_dir(WB_DIR)

    # --- ERA5 normalisation ---
    if "avg_temp_c" in era5.columns:
        era5 = era5.rename(columns={"avg_temp_c": "temp_c"})
    if "iso3" not in era5.columns:
        era5.loc[:, "iso3"] = era5["country"].apply(country_to_iso3)

    # Log countries that failed ISO3 mapping
    missing = sorted(era5[era5["iso3"].isna()]["country"].dropna().unique().tolist())
    if missing:
        print(f"[WARN] {len(missing)} ERA5 countries missing ISO3 after overrides:")
        for name in missing:
            print(f"   - {name}")


    # --- Clean / dedupe ---
    era5 = keep_iso3_year(era5)
    owid = keep_iso3_year(owid)
    wb   = keep_iso3_year(wb)

    # --- Align to common years ---
    max_common = min(era5["year"].max(), owid["year"].max(), wb["year"].max())
    era5 = era5[era5["year"] <= max_common]
    owid = owid[owid["year"] <= max_common]
    wb   = wb[wb["year"]   <= max_common]

    # --- Select relevant columns ---
    era5_keep = ["iso3","year","country","temp_c"]
    era5 = era5[[c for c in era5_keep if c in era5.columns]]

    owid_keep = ["iso3","year","co2_mt","co2_t_per_capita","share_global_co2_pct","consumption_co2_mt"]
    owid = owid[[c for c in owid_keep if c in owid.columns]]

    wb_keep = ["iso3","year","gdp_pc_const_usd","wgi_gov_effectiveness","wgi_rule_of_law",
               "wgi_reg_quality","wgi_control_corruption","wgi_political_stability","wgi_voice_accountability"]
    wb = wb[[c for c in wb_keep if c in wb.columns]]

    # --- Merge (inner join keeps cleanest rows) ---
    df = era5.merge(owid, on=["iso3","year"], how="inner") \
             .merge(wb,   on=["iso3","year"], how="inner")

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"[OK] Saved {len(df):,} rows for {df['year'].min()}–{df['year'].max()} → {OUT_CSV}")

if __name__ == "__main__":
    main()
