#!/usr/bin/env python3
"""
Geometry Sanity Check Script

Validates country geometry sanitation and regions build process.
Reads SHAPEFILE_PATH and COUNTRY_COL (optional) from config.py,
loads raw shapefile, sanitizes geometries, and builds regions.
Prints a comprehensive report and exits with non-zero code on failures.
"""

import sys
import os
import geopandas as gpd

# Add parent directory to path to import config and utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import SHAPEFILE_PATH, COUNTRY_COL
from utils.geo_utils import sanitize_countries, build_regions


def main():
    """Run geometry sanity check and print report."""
    print("== Geometry Sanity Check ==")
    
    # Check if shapefile exists
    if not os.path.exists(SHAPEFILE_PATH):
        print(f"ERROR: Shapefile not found at {SHAPEFILE_PATH}")
        sys.exit(1)
    
    # Load raw shapefile
    try:
        raw = gpd.read_file(SHAPEFILE_PATH)
    except Exception as e:
        print(f"ERROR: Failed to load shapefile: {e}")
        sys.exit(1)
    
    raw_rows = len(raw)
    raw_crs = raw.crs
    
    print(f"Raw rows: {raw_rows}, raw CRS: {raw_crs}")
    
    # Count initial issues
    empty_before = int((raw.geometry.is_empty).sum())
    null_before = int((raw.geometry.isna()).sum())
    print(f"Empty geometries before clean: {empty_before}, null geometries before clean: {null_before}")
    
    # Sanitize geometries
    try:
        gdf_clean, country_col = sanitize_countries(
            raw,
            country_col=COUNTRY_COL if 'COUNTRY_COL' in globals() else None,
            out_crs="EPSG:4326",
            metric_crs="EPSG:6933",
            buffer_meters=15000.0,
            try_make_valid=True,
        )
    except Exception as e:
        print(f"ERROR: Geometry sanitization failed: {e}")
        sys.exit(1)
    
    cleaned_rows = len(gdf_clean)
    cleaned_crs = gdf_clean.crs
    print(f"Cleaned rows: {cleaned_rows}, cleaned CRS: {cleaned_crs}")
    
    if cleaned_rows == 0:
        print("ERROR: Cleaned GeoDataFrame is empty.")
        sys.exit(2)
    
    # Quick empty/invalid counts on the cleaned set
    empty_after = int((gdf_clean.geometry.is_empty).sum())
    null_after = int((gdf_clean.geometry.isna()).sum())
    print(f"Empty geometries after clean: {empty_after}, null geometries after clean: {null_after}")
    
    # Validate that all geometries are valid after cleaning
    if empty_after > 0 or null_after > 0:
        print("ERROR: Cleaned GeoDataFrame still contains empty or null geometries.")
        sys.exit(3)
    
    # Peek at names
    sample_names = gdf_clean[country_col].head(5).tolist()
    print(f"Sample country names: {sample_names}")
    
    # Build regions
    try:
        regions = build_regions(gdf_clean, country_col)
    except Exception as e:
        print(f"ERROR: Regions build failed: {e}")
        sys.exit(1)
    
    print(f"Regions created: {len(regions.names)}")
    
    # Final validation
    try:
        assert len(regions.names) == cleaned_rows, \
            f"Regions count mismatch with cleaned GeoDataFrame! Expected {cleaned_rows}, got {len(regions.names)}"
    except AssertionError as e:
        print(f"ERROR: {e}")
        sys.exit(4)
    
    # Summary
    print(f"\nSummary:")
    print(f"  Raw rows: {raw_rows} → Cleaned rows: {cleaned_rows}")
    print(f"  Raw CRS: {raw_crs} → Cleaned CRS: {cleaned_crs}")
    print(f"  Empty geometries: {empty_before} → {empty_after}")
    print(f"  Null geometries: {null_before} → {null_after}")
    print(f"  Regions created: {len(regions.names)}")
    
    print("\n✅ OK: geometry sanitation and regions build look good.")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)
