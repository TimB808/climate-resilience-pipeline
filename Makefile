# === Makefile for Climate Risk Project ===

setup:
	python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

fetch_owid:
	python data_ingest/fetch_owid.py

fetch_climate:
	python data_ingest/fetch_climate.py

fetch_worldbank:
	python data_ingest/fetch_wb.py

run_all: fetch_owid fetch_climate fetch_worldbank

clean_data:
	rm -rf data/raw/*.csv data/processed/*.csv data/processed/*.nc

.PHONY: setup fetch_owid fetch_climate fetch_worldbank run_all clean_data
