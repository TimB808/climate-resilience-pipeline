"""
Incremental ERA5 processing:
- Fetch ERA5 monthly means idempotently per year into RAW_ERA5_DIR
- Compute country-level annual means (area-weighted) with nearest-cell fallback
- Write partitioned Parquet per year to PROC_ANNUAL_TEMP_DIR
- Keep publishing CSV path unchanged (ANNUAL_TEMP_CSV)
"""

import sys
import os
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import (
    START_YEAR, END_YEAR, VARIABLE, TEMP_VAR, LAT_NAME, LON_NAME, TIME_NAME,
    RAW_DATA_DIR, PROCESSED_DATA_DIR, SHAPEFILE_DIR, SHAPEFILE_PATH, ERA5_OUT_FILE, ANNUAL_TEMP_CSV,
    RAW_ERA5_DIR, RAW_ERA5_FILENAME_TEMPLATE,
    PROC_ANNUAL_TEMP_DIR, PROC_ANNUAL_TEMP_PART_TEMPLATE, FALLBACK_AUDIT_CSV,
    COUNTRY_COL, BUFFER_METERS, FALLBACK_MAX_KM, USE_REPRESENTATIVE_POINT,
    AREA_WEIGHTING, CHUNKS,
    PARQUET_ENGINE, PARQUET_COMPRESSION,
    DEFAULT_YEARS,
    PREVIEW_DIR, PREVIEW_CSV_TEMPLATE,
)
import cdsapi
import regionmask
import xarray as xr
import geopandas as gpd
import pandas as pd
import numpy as np
import warnings
from shapely.strtree import STRtree
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry
from utils.io_utils import download_file
from utils.geo_utils import geojson_to_shapefile, nearest_cell_fallback, sanitize_countries, build_regions

# Suppress regionmask warnings about no gridpoints
warnings.filterwarnings("ignore", message="No gridpoint belongs to any region. Returning an all-NaN mask.")
# Silence region overlap info from regionmask (behavior is correct in >=0.11)
warnings.filterwarnings("ignore", message="Detected overlapping regions.")

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(RAW_ERA5_DIR, exist_ok=True)
os.makedirs(PROC_ANNUAL_TEMP_DIR, exist_ok=True)

# --- Small helpers ---
def ensure_dir(path):
    d = path if os.path.splitext(path)[1] == "" else os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def atomic_replace(src_tmp, dst_final):
    ensure_dir(dst_final)
    os.replace(src_tmp, dst_final)

def parse_years_arg(years_str):
    years_str = years_str.strip()
    if "," in years_str:
        parts = [int(p) for p in years_str.split(",") if p.strip()]
        return sorted(set(parts))
    if "-" in years_str:
        a, b = years_str.split("-", 1)
        start, end = int(a), int(b)
        return list(range(start, end + 1))
    # single year
    y = int(years_str)
    return [y]

def get_time_col(columns):
    lowered = [c.lower() for c in columns]
    for c in ("time", "valid_time", "date", "datetime"):
        if c in lowered:
            # return original case-preserved name
            return columns[lowered.index(c)]
    # fallback to first column containing 'time'
    for i, c in enumerate(lowered):
        if "time" in c:
            return columns[i]
    return None

# --- Step 1: Download ERA5 monthly mean temperature ---
def download_era5_monthly():
    """Legacy full-range downloader (kept for compatibility)."""
    c = cdsapi.Client()
    c.retrieve(
        "reanalysis-era5-single-levels-monthly-means",
        {
            "product_type": "monthly_averaged_reanalysis",
            "format": "netcdf",
            "variable": VARIABLE,
            "year": [str(y) for y in range(START_YEAR, END_YEAR + 1)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "time": "00:00",
        },
        ERA5_OUT_FILE,
    )

def download_naturalearth_shapefile():
    url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    dest = os.path.join(os.path.dirname(SHAPEFILE_DIR), "countries.geojson")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    download_file(url, dest, headers)
    geojson_to_shapefile(dest, SHAPEFILE_DIR)

def is_valid_netcdf(path):
    try:
        with xr.open_dataset(path) as ds:
            return True
    except Exception as e:
        print(f"NetCDF validation failed: {e}")
        return False

def _raw_era5_path(year):
    return os.path.join(RAW_ERA5_DIR, RAW_ERA5_FILENAME_TEMPLATE.format(year=year))

def fetch_era5_year(year):
    """Idempotently fetch a single year's ERA5 monthly-means NetCDF."""
    target = _raw_era5_path(year)
    if os.path.exists(target) and is_valid_netcdf(target):
        return target
    ensure_dir(target)
    tmp = f"{target}.tmp"
    c = cdsapi.Client()
    c.retrieve(
        "reanalysis-era5-single-levels-monthly-means",
        {
            "product_type": "monthly_averaged_reanalysis",
            "format": "netcdf",
            "variable": VARIABLE,
            "year": [str(year)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "time": "00:00",
        },
        tmp,
    )
    if not is_valid_netcdf(tmp):
        raise RuntimeError(f"Failed to download a valid NetCDF file for {year}: {tmp}")
    atomic_replace(tmp, target)
    return target

def ensure_era5_files(years):
    paths = []
    for y in years:
        p = fetch_era5_year(y)
        paths.append(p)
    return paths

def open_era5_years(years):
    paths = [ _raw_era5_path(y) for y in years ]
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(f"Missing ERA5 files: {missing}")
    ds = xr.open_mfdataset(paths, combine="by_coords", chunks=CHUNKS)
    return ds

def compute_country_annual_means_from_years(years):
    # Load country boundaries and sanitize using the new helper
    raw_gdf = gpd.read_file(SHAPEFILE_PATH)
    gdf_clean, country_col = sanitize_countries(
        raw_gdf,
        country_col=COUNTRY_COL if "COUNTRY_COL" in globals() else None,
        out_crs="EPSG:4326",
        metric_crs="EPSG:6933",
        buffer_meters=BUFFER_METERS,
        try_make_valid=True,
    )
    regions = build_regions(gdf_clean, country_col)
    
    # Sanity check
    assert len(regions.names) == len(gdf_clean), "Regions count mismatch with gdf."

    # Accumulators for preview and audit
    preview_frames = []
    audit_frames = []
    n_partitions = 0

    for year in years:
        # Open single-year dataset with configured chunks
        ds = open_era5_years([year])
        ds[TEMP_VAR] = ds[TEMP_VAR] - 273.15
        ds_renamed = ds.rename({LAT_NAME: "lat", LON_NAME: "lon"})

        # Process in batches of countries to limit memory
        year_results = []
        batch_size = 20
        for i in range(0, len(regions.names), batch_size):
            batch_end = min(i + batch_size, len(regions.names))
            batch_regions = regionmask.Regions(
                outlines=list(gdf_clean.geometry.values[i:batch_end]),
                names=gdf_clean[country_col].tolist()[i:batch_end],
                abbrevs=gdf_clean[country_col].tolist()[i:batch_end],
            )

            # Suppress shapely contains_xy runtime warnings only for the mask call
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*contains_xy.*", category=RuntimeWarning)
                mask3d_batch = batch_regions.mask_3D(ds_renamed)
            masked_batch = ds_renamed[TEMP_VAR].where(mask3d_batch)

            if AREA_WEIGHTING:
                coslat = np.cos(np.deg2rad(ds_renamed["lat"]))
                region_time_batch = masked_batch.weighted(coslat).mean(dim=["lat", "lon"])
            else:
                region_time_batch = masked_batch.mean(dim=["lat", "lon"]) 

            df_weighted_batch = region_time_batch.to_dataframe(name="avg_temp_c").reset_index()

            idx_to_name = {j: name for j, name in enumerate(batch_regions.names)}
            if "region" in df_weighted_batch.columns:
                df_weighted_batch["country"] = df_weighted_batch["region"].map(idx_to_name)
                df_weighted_batch.drop(columns=["region"], inplace=True)

            tcol = get_time_col(df_weighted_batch.columns.tolist()) or TIME_NAME
            df_weighted_batch["year"] = pd.to_datetime(df_weighted_batch[tcol]).dt.year

            # Limit to this year only (defensive)
            df_weighted_batch = df_weighted_batch[df_weighted_batch["year"] == year]

            grouped = df_weighted_batch.groupby(["country", "year"], as_index=False)["avg_temp_c"].mean()
            year_results.append(grouped)

        df_year = pd.concat(year_results, ignore_index=True) if year_results else pd.DataFrame(columns=["country","year","avg_temp_c"]) 

        # Identify missing countries for this year
        all_countries = set(regions.names)
        present_countries = set(df_year["country"].unique()) if not df_year.empty else set()
        missing_countries = sorted(list(all_countries - present_countries))

        # Fallback for missing countries using nearest cell (for this year)
        if len(missing_countries) > 0:
            fb_df, audit_df = nearest_cell_fallback(
                ds_renamed,
                gdf_clean,
                country_col,
                missing_countries,
                TEMP_VAR,
                lat_name="lat",
                lon_name="lon",
                max_distance_km=FALLBACK_MAX_KM,
                use_representative_point=USE_REPRESENTATIVE_POINT,
            )
        else:
            fb_df = pd.DataFrame(columns=["country","year","avg_temp_c"]) 
            audit_df = pd.DataFrame(columns=["country","status"]) 

        # Merge for this year and write Parquet partition
        final_year = pd.concat([df_year, fb_df], ignore_index=True)
        final_year = final_year.drop_duplicates(subset=["country", "year"], keep="first")

        # Ensure schema types
        if not final_year.empty:
            final_year["country"] = final_year["country"].astype(str)
            final_year["year"] = final_year["year"].astype(int)
            final_year["avg_temp_c"] = final_year["avg_temp_c"].astype(float)

            final_path = PROC_ANNUAL_TEMP_PART_TEMPLATE.format(year=year)
            ensure_dir(final_path)
            tmp_path = f"{final_path}.tmp"
            final_year.to_parquet(tmp_path, engine=PARQUET_ENGINE, compression=PARQUET_COMPRESSION, index=False)
            atomic_replace(tmp_path, final_path)
            n_partitions += 1

            preview_frames.append(final_year)

        # Collect audit rows
        if not audit_df.empty:
            for col in ["country","status","min_km","near_lat","near_lon"]:
                if col not in audit_df.columns:
                    audit_df[col] = np.nan
            audit_frames.append(audit_df[["country","status","min_km","near_lat","near_lon"]])

        # Close dataset to free memory between years
        try:
            ds.close()
        except Exception:
            pass

    # Write combined audit once per run
    if audit_frames:
        audit_all = pd.concat(audit_frames, ignore_index=True)
        ensure_dir(FALLBACK_AUDIT_CSV)
        audit_all.to_csv(FALLBACK_AUDIT_CSV, index=False)

    # Build preview CSV across processed years
    final = pd.concat(preview_frames, ignore_index=True) if preview_frames else pd.DataFrame(columns=["country","year","avg_temp_c"]) 
    years_suffix = ""
    if years:
        y_min, y_max = min(years), max(years)
        years_suffix = f"_{y_min}-{y_max}"
    preview_path = PREVIEW_CSV_TEMPLATE.format(suffix=years_suffix)
    ensure_dir(preview_path)
    final.sort_values(["country","year"]).drop_duplicates(subset=["country","year"], keep="first").to_csv(preview_path, index=False)

    n_countries = len(final["country"].unique()) if not final.empty else 0
    print(f"Processed years: {len(years)} | Countries: {n_countries} | Partitions written: {n_partitions}")

    return final

# Backward-compatible wrapper (keeps original function name)
def compute_country_annual_means(_nc_file_ignored):
    years = list(range(START_YEAR, END_YEAR + 1))
    ensure_era5_files(years)
    return compute_country_annual_means_from_years(years)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incremental ERA5 processing to partitioned Parquet")
    parser.add_argument("--years", type=str, default=DEFAULT_YEARS, help="Year or range like '2000-2023' or comma list '2000,2001'")
    args = parser.parse_args()

    years = parse_years_arg(args.years)
    years = sorted(set(years))

    ensure_era5_files(years)
    if not os.path.exists(SHAPEFILE_DIR):
        download_naturalearth_shapefile()
    compute_country_annual_means_from_years(years)
