# config.py
"""
Central configuration for the climate resilience pipeline.
Edit this file to change paths, years, variable names, etc.
"""
import os

# --- Climate Data Ingest Settings ---
START_YEAR = 2000
END_YEAR = 2023
VARIABLE = "2m_temperature"
TEMP_VAR = "t2m"  # Set to the exact variable name in your NetCDF files
LAT_NAME = "latitude"
LON_NAME = "longitude"
TIME_NAME = "valid_time"

# Project root directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Data paths
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
SHAPEFILE_DIR = os.path.join(BASE_DIR, "data", "shapefiles", "ne_110m_admin_0_countries")
SHAPEFILE_PATH = os.path.join(SHAPEFILE_DIR, "countries.shp")
ERA5_OUT_FILE = os.path.join(RAW_DATA_DIR, f"era5_temp_{START_YEAR}_{END_YEAR}.nc")
ANNUAL_TEMP_CSV = os.path.join(PROCESSED_DATA_DIR, "annual_country_temp.csv")

# --- Data Transform Settings ---
TRANSFORMED_DATA_DIR = os.path.join(BASE_DIR, "data", "transformed")

# --- Output/Reporting Settings ---
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

# --- Tableau Settings ---
TABLEAU_DIR = os.path.join(BASE_DIR, "tableau") 

# --- ERA5 raw storage (incremental) ---
RAW_ERA5_DIR = os.path.join(RAW_DATA_DIR, "era5")
RAW_ERA5_FILENAME_TEMPLATE = "era5_t2m_{year}.nc"   # one file per year

# --- Processed (partitioned) ---
PROC_ANNUAL_TEMP_DIR = os.path.join(PROCESSED_DATA_DIR, "annual_temp")
PROC_ANNUAL_TEMP_PART_TEMPLATE = os.path.join(PROC_ANNUAL_TEMP_DIR, "year={year}", "part.parquet")
FALLBACK_AUDIT_CSV = os.path.join(PROC_ANNUAL_TEMP_DIR, "fallback_audit.csv")

# --- Publisher output (Tableau) ---
PUBLISH_CSV = os.path.join(OUTPUTS_DIR, "annual_country_temp.csv")
PUBLISH_SUCCESS_MARK = os.path.join(OUTPUTS_DIR, "_SUCCESS")  # tiny marker file

# --- Shapefile / country metadata ---
COUNTRY_COL = "name"  # or "ADMIN" depending on your shapefile columns

# --- Geospatial tuning ---
BUFFER_METERS = 15000        # ~15 km buffer for tiny islands
FALLBACK_MAX_KM = 25.0       # nearest-cell fallback distance
USE_REPRESENTATIVE_POINT = True  # better than centroid for multipolygons

# --- Weighting ---
AREA_WEIGHTING = True        # cos(lat) weighting for grid-cell area

# --- Dask/xarray chunking (tune to your RAM) ---
# Let xarray/dask choose storage-aligned chunks automatically
CHUNKS = "auto"

# --- Parquet I/O ---
PARQUET_ENGINE = "pyarrow"
PARQUET_COMPRESSION = "snappy"  # or "zstd"

# --- CLI defaults (overridable by flags) ---
DEFAULT_YEARS = f"{START_YEAR}-{END_YEAR}"

# --- Preview CSV (for quick inspection; publisher uses Parquet partitions) ---
PREVIEW_DIR = os.path.join(PROCESSED_DATA_DIR, "preview")
PREVIEW_CSV_TEMPLATE = os.path.join(PREVIEW_DIR, "annual_country_temperature_preview{suffix}.csv")