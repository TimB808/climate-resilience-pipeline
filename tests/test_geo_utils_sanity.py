"""
Minimal pytest for geometry utilities sanity check.

Tests the sanitize_countries and build_regions functions on the configured shapefile.
This is an integration test that validates the geometry processing pipeline works
with real data.
"""

import geopandas as gpd
import pytest
from config import SHAPEFILE_PATH, COUNTRY_COL
from utils.geo_utils import sanitize_countries, build_regions


def test_sanitize_and_regions():
    """Test that geometry sanitization and regions build work correctly."""
    # Load raw shapefile
    raw = gpd.read_file(SHAPEFILE_PATH)
    
    # Sanitize geometries
    gdf_clean, country_col = sanitize_countries(
        raw,
        country_col=COUNTRY_COL if 'COUNTRY_COL' in globals() else None,
        out_crs="EPSG:4326",
        metric_crs="EPSG:6933",
        buffer_meters=15000.0,
        try_make_valid=True,
    )

    # Validate cleaned GeoDataFrame
    assert len(gdf_clean) > 0, "Cleaned GeoDataFrame should not be empty"
    assert str(gdf_clean.crs) == "EPSG:4326", f"Expected EPSG:4326, got {gdf_clean.crs}"
    assert int((gdf_clean.geometry.is_empty).sum()) == 0, "No geometries should be empty after cleaning"
    assert int((gdf_clean.geometry.isna()).sum()) == 0, "No geometries should be null after cleaning"
    
    # Validate country column exists and has data
    assert country_col in gdf_clean.columns, f"Country column '{country_col}' should exist"
    assert len(gdf_clean[country_col].dropna()) > 0, "Country column should have non-null values"

    # Build regions and validate
    regions = build_regions(gdf_clean, country_col)
    assert len(regions.names) == len(gdf_clean), "Regions count should match cleaned GeoDataFrame rows"
    assert len(regions.names) > 0, "Should have at least one region"


def test_sanitize_countries_parameters():
    """Test sanitize_countries with different parameters."""
    raw = gpd.read_file(SHAPEFILE_PATH)
    
    # Test with no buffering
    gdf_clean, country_col = sanitize_countries(
        raw,
        country_col=None,  # Auto-detect
        out_crs="EPSG:4326",
        metric_crs="EPSG:6933",
        buffer_meters=0.0,  # No buffer
        try_make_valid=True,
    )
    
    assert len(gdf_clean) > 0
    assert str(gdf_clean.crs) == "EPSG:4326"
    assert int((gdf_clean.geometry.is_empty).sum()) == 0


def test_build_regions_properties():
    """Test that build_regions creates regions with expected properties."""
    raw = gpd.read_file(SHAPEFILE_PATH)
    gdf_clean, country_col = sanitize_countries(raw)
    
    regions = build_regions(gdf_clean, country_col)
    
    # Check regions properties
    assert hasattr(regions, 'names'), "Regions should have names attribute"
    assert hasattr(regions, 'abbrevs'), "Regions should have abbrevs attribute"
    assert len(regions.names) == len(regions.abbrevs), "Names and abbrevs should have same length"
    
    # Check that names match the country column
    expected_names = gdf_clean[country_col].tolist()
    assert regions.names == expected_names, "Region names should match country column values"
