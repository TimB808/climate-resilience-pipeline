# === Makefile for Climate Risk Project ===

setup:
	python -m venv venv && source venv/bin/activate && pip install -r requirements.txt

fetch_owid:
	python data_ingest/fetch_owid.py

fetch_climate:
	python data_ingest/fetch_climate.py

fetch_years:
	python data_ingest/fetch_climate.py --years $(YEARS)

fetch_worldbank:
	python data_ingest/fetch_wb.py

run_all: fetch_owid fetch_climate fetch_worldbank

merge_all:
	python data_transform/merge_all.py

pipeline:
	python pipeline.py

clean_data:
	rm -rf data/raw/*.csv data/processed/*.csv data/processed/*.nc

publish_csv:
	python publish_csv.py

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

.PHONY: setup fetch_owid fetch_climate fetch_worldbank fetch_years publish_csv run_all clean_data clean_processed clean_outputs dev_check_geoms dev_check_masking test
