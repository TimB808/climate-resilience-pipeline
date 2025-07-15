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