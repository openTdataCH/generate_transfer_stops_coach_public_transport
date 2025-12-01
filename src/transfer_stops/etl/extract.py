"""GTFS-Daten herunterladen und Schweizer Haltestellen extrahieren."""
import pandas as pd
import geopandas as gpd
import os
import requests
import zipfile
from io import BytesIO
import hashlib


def get_file_hash(filepath):
    """
    Berechnet MD5-Hash einer Datei.
    Wird verwendet um zu prüfen ob sich der Inhalt von stops.txt geändert hat,
    damit die Pipeline nur bei tatsächlichen Änderungen neu ausgeführt wird.
    """
    if not os.path.exists(filepath):
        return None
    
    md5_hash = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def download_and_extract_gtfs(url: str, output_dir: str):
    """
    Lädt GTFS-ZIP herunter und extrahiert stops.txt.
    Gibt True zurück wenn Datei aktualisiert wurde, False wenn keine Änderungen.
    """
    print(f"Lade Daten herunter von: {url}")
    
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    
    os.makedirs(output_dir, exist_ok=True)
    
    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        if 'stops.txt' not in zip_file.namelist():
            raise FileNotFoundError("stops.txt nicht in ZIP-Datei gefunden")
        new_content = zip_file.read('stops.txt')
    
    new_hash = hashlib.md5(new_content).hexdigest()
    output_file = os.path.join(output_dir, 'stops.txt')
    existing_hash = get_file_hash(output_file)
    
    if existing_hash == new_hash:
        print(f"ℹ️ Keine Änderungen in stops.txt erkannt")
        return False
    
    with open(output_file, 'wb') as f:
        f.write(new_content)
    
    status = "erstellt" if existing_hash is None else "aktualisiert"
    print(f"✅ stops.txt {status} in: {output_dir}")
    return True


def download_provider_data(provider_config):
    """Lädt GTFS-Daten für einen Provider herunter."""
    if 'gtfs_url' not in provider_config:
        return True
    
    output_dir = os.path.dirname(provider_config['input_path'])
    print(f"\nLade {provider_config['name']} herunter...")
    
    try:
        return download_and_extract_gtfs(provider_config['gtfs_url'], output_dir)
    except Exception as e:
        print(f"❌ Fehler: {e}")
        raise


def download_all_providers(providers):
    """Lädt GTFS-Daten für alle Provider herunter."""
    results = {}
    for provider in providers:
        results[provider['name']] = download_provider_data(provider)
    return results


def extract_swiss_stops_csv(input_path: str, output_path: str, provider_name: str, 
                            stop_lat: str, stop_long: str, 
                            geojson_path: str = 'data\\external\\swiss_landesgebiet.geojson'):
    """Liest CSV, filtert Schweizer Haltestellen und schreibt Ergebnis."""
    df = pd.read_csv(input_path)
    df[stop_lat] = df[stop_lat].astype(float)
    df[stop_long] = df[stop_long].astype(float)
    
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"GeoJSON nicht gefunden: {geojson_path}")
    
    swiss_landesgebiet = gpd.read_file(geojson_path)
    gdf_points = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df[stop_long], df[stop_lat]), 
        crs='EPSG:4326'
    )
    gdf_points = gdf_points.to_crs(swiss_landesgebiet.crs)
    
    mask = gdf_points.geometry.apply(lambda pt: swiss_landesgebiet.contains(pt).any())
    swiss_stops_df = gdf_points[mask].copy()
    swiss_stops_df['provider'] = provider_name
    swiss_stops_df.to_csv(output_path, index=False)
    
    return swiss_stops_df
