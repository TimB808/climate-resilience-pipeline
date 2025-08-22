# Development Scripts

This directory contains development and utility scripts for the climate resilience pipeline.

## Scripts

### `dev_sanity_geometries.py`

A lightweight sanity check script for validating country geometry sanitation and regions build process.

**Usage:**
```bash
python scripts/dev_sanity_geometries.py
```

**What it does:**
- Reads `SHAPEFILE_PATH` and `COUNTRY_COL` (optional) from `config.py`
- Loads raw shapefile with GeoPandas
- Calls `utils.geo_utils.sanitize_countries()` and `utils.geo_utils.build_regions()`
- Prints a comprehensive report including:
  - Raw rows vs cleaned rows
  - CRS before/after
  - Count of empty/invalid rows before vs after
  - Example of 5 country names
  - Regions created count
- Validates that regions count matches cleaned rows
- Exits with non-zero code if any fatal assertion fails

**Exit Codes:**
- `0`: Success - all checks passed
- `1`: File not found, load error, or general error
- `2`: Cleaned GeoDataFrame is empty
- `3`: Cleaned GeoDataFrame still contains empty/null geometries
- `4`: Regions count mismatch with cleaned GeoDataFrame
- `130`: Interrupted by user (Ctrl+C)

**Example Output:**
```
== Geometry Sanity Check ==
Raw rows: 258, raw CRS: EPSG:4326
Empty geometries before clean: 0, null geometries before clean: 0
Using country column: name
Initial geometry count: 258
Applying shapely.make_valid for geometry repair...
Applying 15000.0m buffer...
Final geometry count: 258
Cleaned rows: 258, cleaned CRS: EPSG:4326
Empty geometries after clean: 0, null geometries after clean: 0
Sample country names: ['Indonesia', 'Malaysia', 'Chile', 'Bolivia', 'Peru']
Building regions for 258 countries
Regions created: 258

Summary:
  Raw rows: 258 → Cleaned rows: 258
  Raw CRS: EPSG:4326 → Cleaned CRS: EPSG:4326
  Empty geometries: 0 → 0
  Null geometries: 0 → 0
  Regions created: 258

✅ OK: geometry sanitation and regions build look good.
```

This script is useful for:
- Validating that the geometry processing pipeline works correctly
- Debugging issues with country shapefiles
- Ensuring data quality before running the main pipeline
- CI/CD integration for automated testing

### `dev_sanity_masking.py`

A comprehensive end-to-end sanity check for ERA5 masking and area-weighted averaging.

**Usage:**
```bash
python scripts/dev_sanity_masking.py [OPTIONS]
```

**Options:**
- `--years "YYYY-YYYY"`: Year range (default: last 2 years)
- `--sample-countries N`: Limit countries for speed testing
- `--max-fallback-km FLOAT`: Override fallback threshold
- `--buffer-meters FLOAT`: Override buffer distance

**What it does:**
- Loads ERA5 NetCDF files for specified years
- Sanitizes country geometries using `utils.geo_utils`
- Computes 3D region masks and area-weighted averages
- Analyzes direct coverage vs fallback usage
- Validates temperature ranges and data quality
- Reports comprehensive statistics and sample data
- Writes optional fallback audit file

**Exit Codes:**
- `0`: Success - all checks passed
- `1`: Some countries missing after fallback
- `2`: No ERA5 files found or processing errors
- `3`: Empty results after masking
- `130`: Interrupted by user (Ctrl+C)

**Example Output:**
```
== ERA5 Masking Sanity Check ==
Years: 2024-2025
Sample countries: 10
Max fallback km: 25.0
Buffer meters: 15000

Opening 2 ERA5 files...
Dataset shape: (19, 721, 1440)
Temperature range: 204.8 to 316.6 K

Loading country geometries...
Using country column: name
Initial geometry count: 258
Applying shapely.make_valid for geometry repair...
Applying 15000m buffer...
Final geometry count: 258
Limited to 10 countries for speed
Building regions for 10 countries

Computing 3D region mask...
Mask shape: (10, 721, 1440)
Computing area-weighted regional means...
Regional time series shape: (19, 10)

Converting to DataFrame...
Main results: 20 rows

Analyzing coverage vs fallback...
All countries have direct coverage - no fallback needed

==================================================
SANITY REPORT
==================================================
Years requested: [2024, 2025]
Raw files opened: 2
Countries (sanitized): 10
Direct coverage: 10
Fallback used for: 0
Missing after fallback: 0

Sample rows for year 2025 (10 countries):
  country  year  avg_temp_c
  Bolivia  2025   19.928442
    Chile  2025    9.180294
Indonesia  2025   26.010683
 Malaysia  2025   26.176442
     Peru  2025   19.190829

Temperature statistics for 2025:
  Mean: 20.1 C
  Min: 9.2 C
  Max: 26.2 C
  Std: 6.9 C

✅ OK: masking + weighting sanity checks passed.
```

**Example Usage:**
```bash
# Quick check with default settings
python scripts/dev_sanity_masking.py

# Test specific years with limited countries
python scripts/dev_sanity_masking.py --years 2024-2025 --sample-countries 20

# Test with custom parameters
python scripts/dev_sanity_masking.py --years 2023-2024 --max-fallback-km 50 --buffer-meters 10000
```

This script is useful for:
- Validating the complete ERA5 processing pipeline
- Testing different parameter configurations
- Debugging masking and averaging issues
- Ensuring data quality before full pipeline runs
- CI/CD integration for automated testing
