import os
import geopandas as gpd

def geojson_to_shapefile(geojson_path, shapefile_dir, shapefile_name="countries.shp"):
    """Convert a GeoJSON file to a shapefile in the specified directory."""
    os.makedirs(shapefile_dir, exist_ok=True)
    gdf = gpd.read_file(geojson_path)
    shapefile_path = os.path.join(shapefile_dir, shapefile_name)
    gdf.to_file(shapefile_path)
    print(f"Saved shapefile to {shapefile_path}")
    return shapefile_path 