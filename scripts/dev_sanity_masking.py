#!/usr/bin/env python3
"""
Developer Sanity Check: ERA5 Masking + Area-Weighted Averaging

Runs a quick end-to-end check of country masking + area-weighted averaging on a small ERA5 sample.
Reports coverage vs fallback usage, validates output ranges, and provides sample data.
Exit non-zero on fatal issues for CI/CD integration.
"""

import os
import sys
import argparse
import numpy as np
import pandas as pd
import xarray as xr
import regionmask
import geopandas as gpd
import warnings

# Add parent directory to path to import config and utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import (
    START_YEAR, END_YEAR, RAW_ERA5_DIR, RAW_ERA5_FILENAME_TEMPLATE,
    SHAPEFILE_PATH, COUNTRY_COL, TEMP_VAR, LAT_NAME, LON_NAME, TIME_NAME,
    PROC_ANNUAL_TEMP_DIR, BUFFER_METERS, CHUNKS, FALLBACK_MAX_KM
)
from utils.geo_utils import sanitize_countries, build_regions, nearest_cell_fallback

# Suppress regionmask warnings about no gridpoints
warnings.filterwarnings("ignore", message="No gridpoint belongs to any region. Returning an all-NaN mask.")
# Silence region overlap info from regionmask
warnings.filterwarnings("ignore", message="Detected overlapping regions.")


def parse_years(s):
    """Parse year range string like '2022-2023' into list of years."""
    if "-" in s:
        lo, hi = s.split("-")
        return list(range(int(lo), int(hi) + 1))
    else:
        # Single year
        return [int(s)]


def default_two_year_window():
    """Return default 2-year window ending at END_YEAR."""
    return f"{END_YEAR-1}-{END_YEAR}"


def main():
    """Run ERA5 masking sanity check."""
    ap = argparse.ArgumentParser(description="Dev sanity check: ERA5 masking + area-weighted means")
    ap.add_argument("--years", default=default_two_year_window(), help='e.g., "2022-2023"')
    ap.add_argument("--sample-countries", type=int, default=None, 
                   help="Limit number of countries processed for speed")
    ap.add_argument("--max-fallback-km", type=float, default=None,
                   help="Override fallback threshold")
    ap.add_argument("--buffer-meters", type=float, default=None,
                   help="Override buffer distance")
    args = ap.parse_args()

    print("== ERA5 Masking Sanity Check ==")
    print(f"Years: {args.years}")
    print(f"Sample countries: {args.sample_countries or 'all'}")
    print(f"Max fallback km: {args.max_fallback_km or FALLBACK_MAX_KM}")
    print(f"Buffer meters: {args.buffer_meters or BUFFER_METERS}")

    # Parse years and build file paths
    years = parse_years(args.years)
    paths = []
    missing_files = []
    
    for y in years:
        fn = RAW_ERA5_FILENAME_TEMPLATE.format(year=y)
        p = os.path.join(RAW_ERA5_DIR, fn)
        if os.path.exists(p):
            paths.append(p)
        else:
            missing_files.append((y, p))
            print(f"WARNING: missing raw ERA5 file for {y}: {p}")

    if not paths:
        print("ERROR: no input ERA5 files found for the requested years.")
        sys.exit(2)

    print(f"\nOpening {len(paths)} ERA5 files...")
    try:
        # Use smaller chunks for memory efficiency
        ds = xr.open_mfdataset(paths, combine="by_coords", chunks={"time": 1, "lat": 100, "lon": 100})
        print(f"Dataset shape: {ds[TEMP_VAR].shape}")
        print(f"Temperature range: {ds[TEMP_VAR].min().values:.1f} to {ds[TEMP_VAR].max().values:.1f} K")
    except Exception as e:
        print(f"ERROR: failed to open ERA5 files: {e}")
        sys.exit(2)

    # Load and sanitize country geometries
    print(f"\nLoading country geometries from {SHAPEFILE_PATH}...")
    try:
        raw_gdf = gpd.read_file(SHAPEFILE_PATH)
        gdf_clean, country_col = sanitize_countries(
            raw_gdf,
            country_col=COUNTRY_COL if COUNTRY_COL in raw_gdf.columns else None,
            out_crs="EPSG:4326", 
            metric_crs="EPSG:6933",
            buffer_meters=args.buffer_meters if args.buffer_meters is not None else BUFFER_METERS,
            try_make_valid=True
        )
        
        if args.sample_countries:
            gdf_clean = gdf_clean.iloc[:args.sample_countries].copy()
            print(f"Limited to {len(gdf_clean)} countries for speed")
        
        regions = build_regions(gdf_clean, country_col)
        print(f"Built regions for {len(regions.names)} countries")
        
    except Exception as e:
        print(f"ERROR: failed to process country geometries: {e}")
        sys.exit(2)

    # 3D mask + area weighting
    print("\nComputing 3D region mask...")
    try:
        mask3d = regions.mask_3D(ds[TEMP_VAR])
        print(f"Mask shape: {mask3d.shape}")
        
        # Area-weighted averaging
        print("Computing area-weighted regional means...")
        
        # Convert temperature from Kelvin to Celsius
        temp_celsius = ds[TEMP_VAR] - 273.15
        
        # Use the same approach as fetch_climate.py
        coslat = np.cos(np.deg2rad(ds[LAT_NAME]))
        masked_temp = temp_celsius.where(mask3d)
        regional_ts = masked_temp.weighted(coslat).mean(dim=(LAT_NAME, LON_NAME))
        
        print(f"Regional time series shape: {regional_ts.shape}")
        
    except Exception as e:
        print(f"ERROR: failed to compute regional means: {e}")
        sys.exit(2)

    # Convert to DataFrame
    print("\nConverting to DataFrame...")
    try:
        df = regional_ts.to_dataframe(name="avg_temp_c").reset_index()
        
        # Find time column
        time_col = TIME_NAME if TIME_NAME in df.columns else None
        if time_col is None:
            time_candidates = [c for c in df.columns if "time" in c.lower()]
            time_col = time_candidates[0] if time_candidates else df.columns[0]
        
        df["year"] = pd.to_datetime(df[time_col]).dt.year
        name_map = {i: n for i, n in enumerate(regions.names)}
        df["country"] = df["region"].map(name_map)
        df_main = df.groupby(["country", "year"], as_index=False)["avg_temp_c"].mean()
        
        print(f"Main results: {len(df_main)} rows")
        
    except Exception as e:
        print(f"ERROR: failed to convert to DataFrame: {e}")
        sys.exit(2)

    # Coverage vs fallback analysis
    print("\nAnalyzing coverage vs fallback...")
    expected = set(gdf_clean[country_col].unique())
    present = set(df_main["country"].dropna().unique())
    missing = sorted(expected - present)
    
    df_all = df_main.copy()
    fb_count = 0
    audit_df = None
    
    if missing:
        print(f"Fallback needed for {len(missing)} countries: {missing[:5]}{'...' if len(missing) > 5 else ''}")
        
        try:
            fb_df, audit_df = nearest_cell_fallback(
                ds=ds, 
                gdf=gdf_clean, 
                country_col=country_col,
                missing_countries=missing, 
                temp_var=TEMP_VAR,
                lat_name=LAT_NAME, 
                lon_name=LON_NAME,
                max_distance_km=args.max_fallback_km if args.max_fallback_km is not None else FALLBACK_MAX_KM
            )
            fb_count = len(fb_df["country"].unique()) if not fb_df.empty else 0
            
            # Write optional audit preview
            if audit_df is not None and not audit_df.empty:
                os.makedirs(PROC_ANNUAL_TEMP_DIR, exist_ok=True)
                audit_path = os.path.join(PROC_ANNUAL_TEMP_DIR, "_dev_sanity_fallback_audit.csv")
                audit_df.to_csv(audit_path, index=False)
                print(f"Fallback audit written to: {audit_path}")
            
            df_all = pd.concat([df_all, fb_df], ignore_index=True)
            
        except Exception as e:
            print(f"ERROR: fallback processing failed: {e}")
            sys.exit(2)
    else:
        print("All countries have direct coverage - no fallback needed")

    remaining = sorted(expected - set(df_all["country"].dropna().unique()))

    # Validation checks
    print("\nRunning validation checks...")
    
    if df_all.empty:
        print("ERROR: result is empty after masking.")
        sys.exit(3)

    # Check years
    df_years = set(df_all["year"].unique())
    if not set(years).issuperset(df_years):
        print(f"WARNING: some years outside requested range: {df_years - set(years)}")

    # Check temperature range
    out_of_range = df_all[(df_all["avg_temp_c"] < -60) | (df_all["avg_temp_c"] > 60)]
    if len(out_of_range) > 0:
        print(f"WARNING: {len(out_of_range)} rows outside plausible temp range (-60..60 C)")
        print(f"Range: {df_all['avg_temp_c'].min():.1f} to {df_all['avg_temp_c'].max():.1f} C")

    # Final report
    print("\n" + "="*50)
    print("SANITY REPORT")
    print("="*50)
    print(f"Years requested: {years}")
    print(f"Raw files opened: {len(paths)}")
    if missing_files:
        print(f"Missing files: {len(missing_files)}")
    print(f"Countries (sanitized): {len(gdf_clean)}")
    print(f"Direct coverage: {len(present)}")
    print(f"Fallback used for: {fb_count}")
    print(f"Missing after fallback: {len(remaining)}")
    
    if remaining:
        print(f"Remaining missing: {remaining[:10]}{'...' if len(remaining) > 10 else ''}")
    
    # Sample data
    if not df_all.empty:
        latest = max(df_all['year'])
        latest_data = df_all[df_all['year'] == latest]
        print(f"\nSample rows for year {latest} ({len(latest_data)} countries):")
        sample_size = min(5, len(latest_data))
        sample_data = latest_data.sample(sample_size) if len(latest_data) > sample_size else latest_data
        print(sample_data[["country", "year", "avg_temp_c"]].to_string(index=False))
        
        # Temperature statistics
        print(f"\nTemperature statistics for {latest}:")
        print(f"  Mean: {latest_data['avg_temp_c'].mean():.1f} C")
        print(f"  Min: {latest_data['avg_temp_c'].min():.1f} C")
        print(f"  Max: {latest_data['avg_temp_c'].max():.1f} C")
        print(f"  Std: {latest_data['avg_temp_c'].std():.1f} C")

    # Exit code determination
    if len(remaining) > 0:
        print(f"\nNOTE: {len(remaining)} countries still missing after fallback.")
        print("Review shapefile or increase fallback threshold.")
        sys.exit(1)
    
    if len(out_of_range) > 0:
        print(f"\nWARNING: {len(out_of_range)} temperature values outside expected range.")
        print("Review data processing pipeline.")
        # Don't exit with error for this - just warn

    print("\n✅ OK: masking + weighting sanity checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)
