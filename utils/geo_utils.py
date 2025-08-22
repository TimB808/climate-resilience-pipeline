import os
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
from typing import Optional, Tuple, Union
import regionmask

# Import guard for shapely.make_valid
try:
    from shapely import make_valid
except Exception:
    make_valid = None

def geojson_to_shapefile(geojson_path, shapefile_dir, shapefile_name="countries.shp"):
    """Convert a GeoJSON file to a shapefile in the specified directory."""
    os.makedirs(shapefile_dir, exist_ok=True)
    gdf = gpd.read_file(geojson_path)
    shapefile_path = os.path.join(shapefile_dir, shapefile_name)
    gdf.to_file(shapefile_path)
    print(f"Saved shapefile to {shapefile_path}")
    return shapefile_path 

## Drop-in fallback helper 
import numpy as np
import pandas as pd
from shapely.geometry import Point

# simple, vectorised haversine (km)
def _haversine_km(lat1, lon1, lats, lons):
    R = 6371.0088
    lat1 = np.deg2rad(lat1)
    lon1 = np.deg2rad(lon1)
    lats = np.deg2rad(lats)
    lons = np.deg2rad(lons)
    dlat = lats - lat1
    dlon = lons - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lats)*np.sin(dlon/2.0)**2
    return 2 * R * np.arcsin(np.sqrt(a))

def _build_grid(ds, lat_name="lat", lon_name="lon"):
    lats = ds[lat_name].values
    lons = ds[lon_name].values
    # meshgrid is lat x lon
    LON, LAT = np.meshgrid(lons, lats)   # shape (nlat, nlon)
    return lats, lons, LAT, LON

def nearest_cell_fallback(
    ds,
    gdf,
    country_col,
    missing_countries,
    temp_var,
    lat_name="lat",
    lon_name="lon",
    max_distance_km=25.0,   # ~2 grid cells at 0.25° near equator
    use_representative_point=True,
):
    """
    For each country in `missing_countries`, pick the nearest ERA5 grid cell to the
    country geometry (centroid or representative_point). Return a DataFrame with
    (country, year, avg_temp_c) and an audit log DataFrame.
    """
    lats, lons, LAT, LON = _build_grid(ds, lat_name=lat_name, lon_name=lon_name)
    flat_LAT = LAT.ravel()
    flat_LON = LON.ravel()

    rows = []
    audit = []

    for name in missing_countries:
        geom = gdf.loc[gdf[country_col] == name, "geometry"].unary_union
        if geom is None or geom.is_empty:
            audit.append({"country": name, "status": "no_geometry"})
            continue

        # representative_point is safer for multipolygons/small islands than plain centroid
        pt = geom.representative_point() if use_representative_point else geom.centroid
        clat, clon = pt.y, pt.x

        # compute distances to all grid cell centers (vectorised)
        dists = _haversine_km(clat, clon, flat_LAT, flat_LON)
        idx = np.argmin(dists)
        dmin = float(dists[idx])

        if dmin > max_distance_km:
            audit.append({"country": name, "status": f"no_cell_within_{max_distance_km}km", "min_km": dmin})
            continue

        # find the (ilat, ilon)
        nlat, nlon = LAT.shape
        ilat = idx // nlon
        ilon = idx % nlon
        near_lat = lats[ilat]
        near_lon = lons[ilon]

        # extract time series at nearest cell and aggregate to annual means
        series = ds[temp_var].sel({lat_name: near_lat, lon_name: near_lon}, method="nearest")
        dfp = series.to_dataframe(name="avg_temp_c").reset_index()

        # find the time column name
        time_col = next((c for c in dfp.columns if c.lower() in ("time","valid_time","date","datetime")), None)
        if time_col is None:
            # fallback to the first datetime-like column
            time_candidates = [c for c in dfp.columns if "time" in c.lower()]
            time_col = time_candidates[0] if time_candidates else dfp.columns[0]

        dfp["year"] = pd.to_datetime(dfp[time_col]).dt.year
        dfp = dfp.groupby("year", as_index=False)["avg_temp_c"].mean()
        dfp["country"] = name

        rows.append(dfp)
        audit.append({
            "country": name,
            "status": "fallback_used",
            "near_lat": float(near_lat),
            "near_lon": float(near_lon),
            "min_km": dmin
        })

    fb = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["year","avg_temp_c","country"])
    audit_df = pd.DataFrame(audit)
    return fb[["country","year","avg_temp_c"]], audit_df

def sanitize_countries(
    gdf: gpd.GeoDataFrame,
    *,
    country_col: Optional[str] = None,
    out_crs: str = "EPSG:4326",
    metric_crs: str = "EPSG:6933",
    buffer_meters: float = 15000.0,
    try_make_valid: bool = True
) -> Tuple[gpd.GeoDataFrame, str]:
    """
    Sanitize and prepare country geometries for analysis.
    
    Args:
        gdf: Input GeoDataFrame with country geometries
        country_col: Name of country column. If None, auto-detect from common names
        out_crs: Output CRS (default: EPSG:4326)
        metric_crs: CRS for metric operations like buffering (default: EPSG:6933)
        buffer_meters: Buffer distance in meters (default: 15000.0)
        try_make_valid: Whether to attempt shapely.make_valid repair (default: True)
    
    Returns:
        Tuple of (cleaned_geodataframe, resolved_country_column_name)
    
    Raises:
        ValueError: If no usable country column is found
    """
    # Ensure input is a GeoDataFrame with geometries
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise ValueError("Input must be a GeoDataFrame")
    
    # Drop null/empty geometries
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[~gdf.geometry.is_empty]
    
    if gdf.empty:
        raise ValueError("No valid geometries found after removing null/empty geometries")
    
    # Determine country name column
    if country_col is None:
        # Auto-detect from common column names
        for col in ["ADMIN", "NAME", "name", "COUNTRY", "country"]:
            if col in gdf.columns:
                country_col = col
                break
        else:
            raise ValueError("No usable country name column found. Available columns: " + 
                           ", ".join(gdf.columns.tolist()))
    else:
        # Validate specified column exists
        if country_col not in gdf.columns:
            raise ValueError(f"Country column '{country_col}' not found. Available columns: " + 
                           ", ".join(gdf.columns.tolist()))
    
    print(f"Using country column: {country_col}")
    print(f"Initial geometry count: {len(gdf)}")
    
    # Reproject to metric CRS for operations
    gdf_metric = gdf.to_crs(metric_crs)
    
    # Repair invalid/self-intersecting polygons
    # First apply buffer(0) to resolve tiny self-intersections
    gdf_metric["geometry"] = gdf_metric.buffer(0)
    
    # Apply shapely.make_valid if available and requested
    if try_make_valid and make_valid is not None:
        print("Applying shapely.make_valid for geometry repair...")
        gdf_metric["geometry"] = gdf_metric["geometry"].apply(make_valid)
    
    # Drop any rows that became empty after repair
    gdf_metric = gdf_metric[~gdf_metric.geometry.is_empty]
    
    # Apply buffering if requested
    if buffer_meters > 0:
        print(f"Applying {buffer_meters}m buffer...")
        gdf_metric["geometry"] = gdf_metric.buffer(buffer_meters)
    
    # Reproject to output CRS
    gdf_out = gdf_metric.to_crs(out_crs)
    
    # Final cleanup - drop null/empty geometries again
    gdf_out = gdf_out[gdf_out.geometry.notnull()]
    gdf_out = gdf_out[~gdf_out.geometry.is_empty]
    
    print(f"Final geometry count: {len(gdf_out)}")
    
    return gdf_out, country_col

def build_regions(
    gdf: gpd.GeoDataFrame,
    country_col: str
) -> regionmask.Regions:
    """
    Construct regionmask.Regions from GeoDataFrame.
    
    Args:
        gdf: GeoDataFrame with country geometries
        country_col: Name of the country column
    
    Returns:
        regionmask.Regions object for masking operations
    """
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise ValueError("Input must be a GeoDataFrame")
    
    if country_col not in gdf.columns:
        raise ValueError(f"Country column '{country_col}' not found in GeoDataFrame")
    
    if gdf.empty:
        raise ValueError("GeoDataFrame is empty")
    
    # Extract geometries and names
    outlines = list(gdf.geometry.values)
    names = gdf[country_col].tolist()
    
    print(f"Building regions for {len(names)} countries")
    
    # Create regions object
    regions = regionmask.Regions(
        outlines=outlines,
        names=names,
        abbrevs=names,  # Use same names as abbreviations
    )
    
    return regions
