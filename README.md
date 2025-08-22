# Climate Risk Resilience Pipeline

This project provides a data pipeline for ingesting, transforming, and preparing climate and socioeconomic data for analysis and visualization. The pipeline is designed to support climate risk and resilience research, with outputs suitable for dashboards and further analytics.

## Project Structure

```
climate-risk-resilience/
├── data_ingest/
│   ├── fetch_owid.py           # CO₂ & energy
│   ├── fetch_climate.py        # ERA5 or NOAA
│   └── fetch_wb.py             # GDP, WGI
├── data_transform/
│   ├── clean_owid.py
│   ├── process_climate.py
│   ├── merge_all.py
├── data/
│   ├── raw/
│   └── processed/
├── outputs/
│   └── climate_dashboard_data.csv
├── tableau/
│   └── screenshot.png
├── pipeline.py
├── README.md
├── requirements.txt
└── .github/
    └── workflows/
        └── run_pipeline.yml
```

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the pipeline:
   ```bash
   python pipeline.py
   ```

## Development & Testing

### Quick Geometry Sanity Check

Run the quick CLI check to validate country geometry processing:

```bash
python scripts/dev_sanity_geometries.py
```

Or use the Makefile target:

```bash
make dev_check_geoms
```

### ERA5 Masking Sanity Check

Run a comprehensive end-to-end check of ERA5 masking and area-weighted averaging:

```bash
python scripts/dev_sanity_masking.py --years 2024-2025 --sample-countries 20
```

Or use the Makefile target:

```bash
make dev_check_masking
```

### Running Tests

Run the test suite:

```bash
pytest -q
```

Or use the Makefile target:

```bash
make test
```

## Description
- **data_ingest/**: Scripts to fetch data from various sources (OWID, ERA5/NOAA, World Bank).
- **data_transform/**: Scripts to clean, process, and merge ingested data.
- **data/**: Contains raw and processed data files.
- **outputs/**: Final outputs for dashboards or analysis.
- **tableau/**: Visualizations and screenshots.
- **pipeline.py**: Orchestrates the data pipeline.
- **.github/workflows/**: CI/CD workflows for automation. 

## To run: 

```
make fetch_owid
make fetch_climate
make run_all
```

Or reset all data with:

```
make clean_data
```

## How to publish

After processing ERA5 into Parquet partitions, assemble a single CSV for Tableau:

```
python publish_csv.py
```

Filter the published range by year if needed:

```
python publish_csv.py --min-year 2000 --max-year 2024
```

The script writes `outputs/annual_country_temp.csv` and a `_SUCCESS` marker next to it.

### Make targets

Run incremental fetch for a specific year range:

```
make fetch_years YEARS=2000-2024
```

Publish the assembled CSV:

```
make publish_csv
```

### Workflow summary

1) Produce Parquet partitions from ERA5 (required):

```
make fetch_years YEARS=2000-2024
```

This will create yearly Parquet files at `data/processed/annual_temp/year=YYYY/part.parquet`.

2) Build the Tableau-ready CSV from those partitions:

```
python publish_csv.py
```

Use `--min-year/--max-year` to filter if needed.


README section (paste into your repo)
📦 Tableau-ready dataset

File: outputs/annual_country_temperature.csv
Schema: Country, Year, AvgTemperatureC
Coverage: 2000–2023 (configurable in config.py)
Source: ERA5 monthly means (2m temperature), aggregated to annual, country-level using regionmask with area weighting (cos(lat)).
Small-country handling: micro-states are filled via a nearest-grid-cell fallback when no grid cell intersects the polygon; see data/processed/annual_temperature/fallback_audit.csv.

How it was built

1. fetch_climate.py fetches ERA5 per-year NetCDFs and writes partitioned Parquet:

```
data/processed/annual_temperature/year=YYYY/part.parquet
```

2. publish_csv.py concatenates all yearly partitions and writes:

```
outputs/annual_country_temperature.csv
```

3. The CSV is intended for Tableau Public / Desktop.

Notes & caveats

Temperatures are °C (Kelvin → °C).

Area weighted by cos(latitude).

Antarctica / territories: coverage depends on the country polygons in the shapefile.

A small set of countries may use the nearest-cell fallback; see the audit for transparency.

