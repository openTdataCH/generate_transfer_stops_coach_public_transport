import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, GeometryCollection
import pandas as pd
import os


def extract_swiss_stops_csv(input_path: str, output_path: str, provider_name: str, stop_lat: str, stop_long: str, geojson_path: str = 'RohDaten\\swisstopoinspiregeoportal\\swiss_landesgebiet.geojson'):
    """
    Reads a CSV, filters Swiss stops, and writes the result to output_path.
    """
    df = pd.read_csv(input_path)
    # Ensure lat/lon are floats
    df[stop_lat] = df[stop_lat].astype(float)
    df[stop_long] = df[stop_long].astype(float)
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"GeoJSON file not found at {geojson_path}")
    swiss_landesgebiet = gpd.read_file(geojson_path)
    if stop_lat not in df.columns or stop_long not in df.columns:
        raise ValueError(f"DataFrame must contain '{stop_lat}' and '{stop_long}' columns.")
    # Erzeuge GeoDataFrame mit WGS84-Koordinaten
    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[stop_long], df[stop_lat]), crs='EPSG:4326')
    # Transformiere die Punkte ins Ziel-CRS (EPSG:2056)
    gdf_points = gdf_points.to_crs(swiss_landesgebiet.crs)
    # Prüfe, ob jeder Punkt in der Schweiz liegt
    mask = gdf_points.geometry.apply(lambda pt: swiss_landesgebiet.contains(pt).any())
    swiss_stops_df = gdf_points[mask].copy()
    swiss_stops_df['provider'] = provider_name
    swiss_stops_df.to_csv(output_path, index=False)
    return swiss_stops_df
