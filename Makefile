# === Makefile for Climate Risk Project ===

setup:
	python -m venv venv && source venv/bin/activate && pip install -r requirements.txt

# === Data Ingestion Targets ===
fetch_era5:
	@echo "==> Fetching ERA5 climate data..."
	python data_ingest/fetch_climate.py

fetch_owid:
	@echo "==> Fetching Our World in Data..."
	python data_ingest/fetch_owid.py

fetch_wb:
	@echo "==> Fetching World Bank data..."
	python data_ingest/fetch_wb.py

# === Data Transformation Targets ===
merge_all:
	@echo "==> Merging all datasets..."
	python data_transform/merge_all.py

# === Publishing Targets ===
publish_csv:
	@echo "==> Publishing CSV outputs..."
	python publish_csv.py

# === Pipeline Targets ===
all: fetch_era5 fetch_owid fetch_wb merge_all publish_csv
	@echo "==> Full pipeline complete!"

pipeline:
	python pipeline.py

# === Legacy Aliases ===
fetch_climate: fetch_era5

fetch_worldbank: fetch_wb

run_all: all

fetch_years:
	python data_ingest/fetch_climate.py --years $(YEARS)

# === Cleanup Targets ===
clean_data:
	rm -rf data/raw/*.csv data/processed/*.csv data/processed/*.nc

clean_processed:
	rm -rf data/processed/annual_temp

clean_outputs:
	rm -f outputs/annual_country_temp.csv

dev_check_geoms:
	python scripts/dev_sanity_geometries.py

dev_check_masking:
	python scripts/dev_sanity_masking.py

test:
	pytest -q

.PHONY: setup fetch_era5 fetch_owid fetch_wb merge_all publish_csv all pipeline fetch_climate fetch_worldbank run_all fetch_years clean_data clean_processed clean_outputs dev_check_geoms dev_check_masking test
