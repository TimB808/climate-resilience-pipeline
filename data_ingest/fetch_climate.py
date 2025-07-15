"""
Fetch and process ERA5 monthly average temperature data from CDS API along with country shapefile from Natural Earth,
aggregate to annual means per country, and save as CSV.
"""

import cdsapi
import xarray as xr
import geopandas as gpd
import pandas as pd
import os
from shapely.strtree import STRtree
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

# --- Configuration ---
START_YEAR = 2000
END_YEAR = 2023
VARIABLE = "2m_temperature"
OUT_DIR = "data/raw"
OUT_FILE = f"{OUT_DIR}/era5_temp_{START_YEAR}_{END_YEAR}.nc"
CSV_OUT = f"data/processed/annual_country_temp.csv"

os.makedirs(OUT_DIR, exist_ok=True)

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
        OUT_FILE,
    )
    print(f"Downloaded: {OUT_FILE}")

# --- Step 2: Load shapefile and compute country averages ---
import zipfile
import requests
import os

def download_naturalearth_shapefile():
    # Use a more reliable alternative source for country boundaries
    url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    dest = "data/shapefiles/countries.geojson"
    extract_to = "data/shapefiles/ne_110m_admin_0_countries"
    shp_file = os.path.join(extract_to, "countries.shp")

    # Skip download if the file already exists
    if os.path.exists(dest):
        print("Country boundaries already exist. Skipping download.")
        return

    # Download geojson
    os.makedirs("data/shapefiles", exist_ok=True)
    print("Downloading country boundaries from GitHub...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(url, headers=headers)
    print(f"Status code: {r.status_code}")
    if r.status_code != 200:
        if os.path.exists(dest):
            os.remove(dest)
        raise Exception(f"Failed to download country boundaries. Status code: {r.status_code}")

    with open(dest, "wb") as f:
        f.write(r.content)

    # Convert geojson to shapefile for compatibility
    print("Converting to shapefile format...")
    os.makedirs(extract_to, exist_ok=True)
    
    # Read the geojson and save as shapefile
    gdf = gpd.read_file(dest)
    gdf.to_file(os.path.join(extract_to, "countries.shp"))
    
    print("Country boundaries ready.")


# Run once before compute_country_annual_means


def compute_country_annual_means(nc_file):
    print("Loading dataset...")
    ds = xr.open_dataset(nc_file)
    ds = ds - 273.15  # Convert from Kelvin to Celsius

    print("Loading country boundaries...")
    shapefile_path = "data/shapefiles/ne_110m_admin_0_countries/countries.shp"
    gdf = gpd.read_file(shapefile_path)
    
    # Debug: print available columns to see what country name column is available
    print(f"Available columns in country boundaries: {list(gdf.columns)}")
    
    # Determine the country name column - try common variations
    country_col = None
    for col in ['ADMIN', 'NAME', 'name', 'COUNTRY', 'country', 'NAME_0', 'ADMIN_0']:
        if col in gdf.columns:
            country_col = col
            break
    
    if country_col is None:
        print("Warning: Could not find country name column. Available columns:", list(gdf.columns))
        country_col = gdf.columns[0]  # fallback to first column
    
    print(f"Using column '{country_col}' for country names")

    print("Averaging by country...")
    means = []

    # Check what time dimension is called
    time_dim = None
    for dim in ['time', 'valid_time', 'Time']:
        if dim in ds.dims:
            time_dim = dim
            break
    
    if time_dim is None:
        raise ValueError(f"No time dimension found. Available dimensions: {list(ds.dims)}")
    
    print(f"Using time dimension: '{time_dim}'")
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"Processing year: {year}")
        annual = ds.sel({time_dim: slice(f"{year}-01", f"{year}-12")})
        yearly_mean = annual.mean(dim=time_dim).squeeze()
        print(f"Yearly mean calculated for {year}")

        # Convert to DataFrame for country masking
        df = yearly_mean.to_dataframe().reset_index()
        print(f"DataFrame created for {year}, shape: {df.shape}")
        points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
        points.set_crs("EPSG:4326", inplace=True)
        print(f"GeoDataFrame created for {year}, shape: {points.shape}")

        # Build STRtree for country polygons
        print("Building STRtree for country polygons...")
        gdf = gdf[gdf.geometry.notnull() & gdf.is_valid] # Filter for valid, non-null geometries
        country_geoms = list(gdf.geometry)
        country_names = gdf[country_col].values
        tree = STRtree(country_geoms)
        # Map geometry id to country name
        geom_id_to_country = {id(geom): name for geom, name in zip(country_geoms, country_names)}

        # Map each point to a country
        def get_country(point):
            matches = tree.query(point)
            for poly in matches:
                if isinstance(poly, BaseGeometry) and poly.contains(point):
                    return geom_id_to_country[id(poly)]
            return None

        print(f"Assigning countries to points for {year}...")
        points['country'] = points.geometry.apply(get_country)
        points = points.dropna(subset=['country'])
        print(f"Points assigned to countries for {year}, shape: {points.shape}")

        # Now group by country
        grouped = points.groupby('country')["t2m"].mean().reset_index()
        grouped.columns = ["country", "avg_temp_c"]
        grouped["year"] = year
        means.append(grouped)

    print("Saving to CSV...")
    result = pd.concat(means)
    result.to_csv(CSV_OUT, index=False)
    print(f"Done: {CSV_OUT}")

# --- Run steps ---
if __name__ == "__main__":
    # Check and download ERA5 data
    if not os.path.exists(OUT_FILE):
        download_era5_monthly()

    # Check and download shapefile
    shapefile_dir = "data/shapefiles/ne_110m_admin_0_countries"
    if not os.path.exists(shapefile_dir):
        download_naturalearth_shapefile()

    # Process ERA5 to country-level annual means
    compute_country_annual_means(OUT_FILE)
