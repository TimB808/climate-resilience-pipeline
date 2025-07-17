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

# --- Placeholders for Other Pipeline Stages ---
# Data Transform Settings
# TRANSFORMED_DATA_DIR = os.path.join(BASE_DIR, "data", "transformed")
# Example: TRANSFORM_CONFIG = {...}

# Output/Reporting Settings
# REPORTS_DIR = os.path.join(BASE_DIR, "reports")
# Example: REPORT_FORMAT = "pdf"

# Add more config sections as needed for your pipeline 