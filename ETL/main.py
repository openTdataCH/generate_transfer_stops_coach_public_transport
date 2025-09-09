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

from extract import extract_swiss_stops_csv
from transform import drop_columns, standardize_lat_lon, check_and_add_new_coords
from load import write_bahnhof_format


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

    # Define transport providers to process
    providers = [
    #     {
    #         'name': 'Flixbus',
    #         'input_path': 'RohDaten/Flixbus/stops.txt',
    #         'output_path': 'Output/Flixbus_stops.csv',
    #         'columns_to_drop': [
    #             'stop_desc', 'zone_id', 'stop_url',
    #             'location_type', 'parent_station',
    #             'wheelchair_boarding', 'platform_code'
    #         ],
    #         'lat': 'stop_lat',
    #         'lon': 'stop_lon'
    #     },
    # Uncomment to process BlaBlaCar as well:
    {
        'name': 'BlaBlaCar',
        'input_path': 'RohDaten/BlaBlaCar/stops.txt',
        'output_path': 'Output/BlaBlaCar_stops.csv',
        'columns_to_drop': ['stop_code', 'stop_desc', 'wheelchair_boarding'],
            'lat': 'stop_lat',
            'lon': 'stop_lon'
        }
    ]
    
    # Process each provider
    for provider in providers:
        try:
            process_transport_provider(provider)
        except Exception as e:
            print(f"Error processing {provider['name']}: {e}")
            continue
    
    print("\n" + "=" * 50)
    print("ETL Pipeline completed!")

if __name__ == "__main__":
    main()
