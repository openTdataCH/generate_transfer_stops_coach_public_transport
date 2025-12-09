"""
ETL Pipeline for Transport Provider Data Processing
Main entry point for the transfer stops generation.
"""
import sys
import os


# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.transfer_stops.etl.extract import download_all_providers, download_oev_sammlung, extract_swiss_stops_csv
from src.transfer_stops.etl.transform import (
    drop_columns, standardize_lat_lon, check_and_add_new_coords, 
    clean_delta_bfkoord_wgs, assign_ids_to_delta, convert_all_bfkoord_to_csv, 
    create_umsteigb_file
)
from src.transfer_stops.create_metabhf.create_osm_metabhf import create_osm_metabhf
from src.transfer_stops.etl.load import write_bahnhof_format, zip_delta_files
from src.transfer_stops import config
from src.transfer_stops.clean_data import clean_data



def process_transport_provider(provider_config):
    """Process a single transport provider's data."""
    print(f"\n=== Verarbeite {provider_config['name']} ===")
    
    # Extract
    print("Extrahiere Daten...")
    df = extract_swiss_stops_csv(
        provider_config['input_path'],
        provider_config['output_path'],
        provider_config['name'],
        provider_config['lat'],
        provider_config['lon']
    )

    # Transform
    print("Transformiere Daten...")
    df = drop_columns(df, provider_config['columns_to_drop'])
    df = standardize_lat_lon(df, provider_config['lat'], provider_config['lon'])
    check_and_add_new_coords(df, provider_config['name'])
    
    print(f"✅ {provider_config['name']} abgeschlossen!")


def main():
    """Main ETL execution."""
    
    # Download ÖV reference data first
    print("\n" + "=" * 50)
    print("Lade ÖV-Referenzdaten herunter...")
    print("=" * 50)
    oev_has_changes = download_oev_sammlung()
    
    # Download GTFS data
    print("\n" + "=" * 50)
    print("Lade GTFS-Daten herunter...")
    print("=" * 50)
    download_results = download_all_providers(config.providers)

    # Check if any provider or ÖV data has changes
    has_any_changes = any(download_results.values()) or oev_has_changes
    
    if not has_any_changes:
        print("\nℹ️ Keine Änderungen bei den Providern oder ÖV-Daten erkannt. Überspringe Verarbeitung.")
        print("\n" + "=" * 50)
        print("✅ ETL-Pipeline abgeschlossen!")
        print("=" * 50)
        return

    # If there are changes, delete all output files to recreate them
    print("\n" + "=" * 50)
    print("Änderungen erkannt - Lösche alte Output-Dateien...")
    print("=" * 50)
    
    output_files = [
        config.PATHS['delta_bfkoord_wgs'],
        config.PATHS['delta_bahnhof'],
        config.PATHS['bfkoord_wgs_csv'],
        config.PATHS['oev_bfkoord_wgs_csv'],
        config.PATHS['provider_stops']
    ]
    
    for output_file in output_files:
        if os.path.exists(output_file):
            os.remove(output_file)
            print(f"🗑️ Gelöscht: {output_file}")

    # Process all providers (recreate everything)
    print("\n" + "=" * 50)
    print("Verarbeite alle Provider...")
    print("=" * 50)
    
    for provider in config.providers:
        try:
            process_transport_provider(provider)
        except Exception as e:
            print(f"❌ Fehler bei Verarbeitung von {provider['name']}: {e}")
            continue
    
    # Clean BFKOORD_WGS (remove FlixTrain and nearby duplicates)
    print("\n" + "=" * 50)
    print("Bereinige gesammelte Koordinaten...")
    print("=" * 50)
    try:
        clean_delta_bfkoord_wgs()
    except Exception as e:
        print(f"❌ Fehler bei Bereinigung: {e}")
    
    # Assign IDs to cleaned data
    print("\n" + "=" * 50)
    print("Vergebe IDs an bereinigte Koordinaten...")
    print("=" * 50)
    try:
        assign_ids_to_delta()
    except Exception as e:
        print(f"❌ Fehler bei ID-Vergabe: {e}")
    
    # Create CSV files after ID assignment
    print("\n" + "=" * 50)
    print("Erstelle CSV-Dateien...")
    print("=" * 50)
    try:
        convert_all_bfkoord_to_csv()
    except Exception as e:
        print(f"❌ Fehler bei CSV-Erstellung: {e}")
    
    # Load: Create BAHNHOF format after cleaning
    print("\n" + "=" * 50)
    print("Erstelle BAHNHOF-Format...")
    print("=" * 50)
    for provider in config.providers:
        try:
            write_bahnhof_format(provider['name'])
        except Exception as e:
            print(f"❌ Fehler bei BAHNHOF-Format für {provider['name']}: {e}")

    # Erstellt METABHF und UMSTEIGB

    print("Erstelle METABHF aus OSM-Daten...")
    print("=" * 50)
    try:
        create_osm_metabhf()
    except Exception as e:
        print(f"❌ Fehler bei METABHF-Erstellung: {e}")
        
    # Erstelle UMSTEIGB
    print("\n" + "=" * 50)
    print("Erstelle UMSTEIGB-Datei...")
    print("=" * 50)
    try:
        create_umsteigb_file()
    except Exception as e:
        print(f"❌ Fehler bei UMSTEIGB-Erstellung: {e}")

    print("\n" + "=" * 50)
    print("✅ ETL-Pipeline abgeschlossen!")
    print("=" * 50)
    
    # Delta-Dateien zippen
    print("\n⚠️  Möchtest du die Delta-Dateien jetzt zippen?")
    response = input("Delta-Dateien zippen? (ja/nein): ").strip().lower()
    
    if response in ['ja', 'j', 'yes', 'y']:
        print("\n" + "=" * 60)
        print("Zippe Delta-Dateien...")
        print("=" * 60)
        zip_delta_files()
        print("=" * 60)
    else:
        print("\n✅ Delta-Dateien werden nicht gezippt.")
    
    # Frage ob Daten gelöscht werden sollen
    print("\n⚠️  Möchtest du die generierten Rohdaten und Output-Dateien löschen?")
    print("   - Alle Dateien in data/processed/ (ausser QGIS-METABHF.csv)")
    print("   - Alle Dateien in data/raw/")
    print()
    
    response = input("Dateien löschen? (ja/nein): ").strip().lower()
    
    if response in ['ja', 'j', 'yes', 'y']:
        clean_data()
    else:
        print("\n✅ Dateien bleiben erhalten.")


if __name__ == "__main__":
    main()
