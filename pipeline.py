#!/usr/bin/env python3
"""
Main pipeline orchestrator for the climate resilience data pipeline.
Runs all data ingestion and processing steps in sequence.
"""

import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import (
    RAW_DATA_DIR, PROCESSED_DATA_DIR, TRANSFORMED_DATA_DIR, 
    OUTPUTS_DIR, REPORTS_DIR, TABLEAU_DIR
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Create all necessary directories."""
    directories = [
        RAW_DATA_DIR, PROCESSED_DATA_DIR, TRANSFORMED_DATA_DIR,
        OUTPUTS_DIR, REPORTS_DIR, TABLEAU_DIR
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")

def run_data_ingestion():
    """Run all data ingestion steps."""
    logger.info("Starting data ingestion...")
    
    try:
        # Import and run ingestion modules
        from data_ingest.fetch_owid import fetch_owid_co2_data
        from data_ingest.fetch_climate import ensure_era5_file, compute_country_annual_means
        from data_ingest.fetch_wb import fetch_worldbank_data
        
        # Step 1: Fetch OWID CO2 data
        logger.info("Fetching OWID CO2 data...")
        fetch_owid_co2_data()
        
        # Step 2: Fetch and process climate data
        logger.info("Fetching and processing climate data...")
        ensure_era5_file()
        compute_country_annual_means("data/raw/era5_temp_2000_2023.nc")
        
        # Step 3: Fetch World Bank data
        logger.info("Fetching World Bank data...")
        fetch_worldbank_data()
        
        logger.info("Data ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise

def run_data_transformation():
    """Run data transformation steps."""
    logger.info("Starting data transformation...")
    
    try:
        from data_transform.merge_all import main as merge_all
        merge_all()
        logger.info("Data transformation completed successfully!")
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

def generate_outputs():
    """Generate final outputs for analysis and visualization."""
    logger.info("Generating outputs...")
    
    try:
        from publish_csv import publish
        n_parts, n_countries, n_rows, y_min, y_max = publish(
            min_year=None,
            max_year=None,
            write_success_marker=True
        )
        logger.info(f"Published CSV | Years: {y_min}..{y_max} | Countries: {n_countries} | Rows: {n_rows}")
    except Exception as e:
        logger.error(f"Output generation failed: {e}")
        raise

def main():
    """Main pipeline execution."""
    start_time = datetime.now()
    logger.info(f"Starting climate resilience pipeline at {start_time}")
    
    try:
        # Setup
        setup_directories()
        
        # Run pipeline steps
        run_data_ingestion()
        run_data_transformation()
        generate_outputs()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Pipeline completed successfully in {duration}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
