"""
Fetch and process ERA5 monthly average temperature data from CDS API along with country shapefile from Natural Earth,
aggregate to annual means per country, and save as CSV.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import (
    START_YEAR, END_YEAR, VARIABLE, TEMP_VAR, LAT_NAME, LON_NAME, TIME_NAME,
    RAW_DATA_DIR, PROCESSED_DATA_DIR, SHAPEFILE_DIR, SHAPEFILE_PATH, ERA5_OUT_FILE, ANNUAL_TEMP_CSV
)
import cdsapi
import regionmask
import xarray as xr
import geopandas as gpd
import pandas as pd
from shapely.strtree import STRtree
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry
from utils.io_utils import download_file
from utils.geo_utils import geojson_to_shapefile

os.makedirs(RAW_DATA_DIR, exist_ok=True)

# --- Step 1: Download ERA5 monthly mean temperature ---
def download_era5_monthly():
    c = cdsapi.Client()
    print("Requesting ERA5 monthly data...")
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
    print(f"Downloaded: {ERA5_OUT_FILE}")

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

def ensure_era5_file():
    if not os.path.exists(ERA5_OUT_FILE) or not is_valid_netcdf(ERA5_OUT_FILE):
        if os.path.exists(ERA5_OUT_FILE):
            print(f"Deleting invalid file: {ERA5_OUT_FILE}")
            os.remove(ERA5_OUT_FILE)
        download_era5_monthly()
        if not is_valid_netcdf(ERA5_OUT_FILE):
            raise RuntimeError(f"Failed to download a valid NetCDF file: {ERA5_OUT_FILE}")

def compute_country_annual_means(nc_file):
    print("Loading ERA5 data...")
    ds = xr.open_dataset(nc_file)
    ds = ds - 273.15  # Convert Kelvin to Celsius

    print("Loading country boundaries...")
    gdf = gpd.read_file(SHAPEFILE_PATH)

    # Standardise CRS
    if gdf.crs is None or gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    # Find usable country name field
    for col in ['ADMIN', 'NAME', 'name', 'COUNTRY', 'country']:
        if col in gdf.columns:
            country_col = col
            break
    else:
        raise ValueError("No usable country name column found.")

    print(f"Using '{country_col}' for country names")

    # Filter valid geometries
    gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]

    print("Creating region mask for all time steps...")
    results = []
    time_coord = ds[TIME_NAME]
    for i, name in enumerate(gdf[country_col]):
        print(f"Processing region {i}: {name}")
        single_region = regionmask.Regions([gdf.geometry.iloc[i]], names=[name], abbrevs=[name])
        ds_renamed = ds.rename({LAT_NAME: "lat", LON_NAME: "lon"})
        for t_idx, t_val in enumerate(time_coord):
            # Select one time step
            ds_time = ds_renamed.isel({TIME_NAME: t_idx})
            single_mask = single_region.mask(ds_time)
            masked = ds_time[TEMP_VAR].where(single_mask == 0)
            avg_temp = masked.mean(dim=["lat", "lon"]).item()
            year = pd.to_datetime(str(t_val.values)).year
            results.append({"country": name, "year": year, "avg_temp_c": avg_temp})
    final = pd.DataFrame(results)
    final = final.dropna(subset=["country"])
    final.to_csv(ANNUAL_TEMP_CSV, index=False)
    print(f"Saved: {ANNUAL_TEMP_CSV}")

if __name__ == "__main__":
    ensure_era5_file()
    if not os.path.exists(SHAPEFILE_DIR):
        download_naturalearth_shapefile()
    compute_country_annual_means(ERA5_OUT_FILE)
