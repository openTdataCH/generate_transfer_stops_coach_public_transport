"""
ETL Pipeline for Transport Provider Data Processing
==================================================
This script processes transportation stop data from various providers 
(FlixBus, BlaBlaCar) and integrates them into a unified coordinate system.

Main steps:
1. Extract data from source files
2. Transform: clean, filter, and deduplicate
3. Load: save to output formats
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from extract import extract_swiss_stops_csv
from transform import drop_columns, standardize_lat_lon, check_and_add_new_coords
from load import write_bahnhof_format
import configuration


def process_transport_provider(provider_config):
    """
    Process a single transport provider's data through the ETL pipeline.
    
    Args:
        provider_config (dict): Configuration for the transport provider
    """
    print(f"\n=== Processing {provider_config['name']} ===")
    
    # Extract
    print("1. Extracting data...")
    df = extract_swiss_stops_csv(provider_config['input_path'], provider_config['output_path'], provider_config['name'], provider_config['lat'], provider_config['lon'])

    # Transform
    print("2. Transforming data...")
    drop_columns(df, provider_config['columns_to_drop'], provider_config['output_path'])
    df = standardize_lat_lon(df, provider_config['lat'], provider_config['lon'])
    check_and_add_new_coords(df, provider_config['name'])

    # Load
    print("3. Loading data...")
    write_bahnhof_format(provider_config['name'])
    
    print(f"{provider_config['name']} processing complete!")


def main():
    """Main ETL execution function."""

    # Process each provider
    for provider in configuration.providers:
        try:
            process_transport_provider(provider)
        except Exception as e:
            print(f"Error processing {provider['name']}: {e}")
            continue
    
    print("\n" + "=" * 50)
    print("ETL Pipeline completed!")

if __name__ == "__main__":
    main()
