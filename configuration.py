BEGINNING_ID = 1700000


# Define transport providers to process
providers = [
        {
            'name': 'Flixbus',
            'input_path': 'RohDaten/Flixbus/stops.txt',
            'output_path': 'Output/Flixbus_stops.csv',
            'columns_to_drop': [
                'stop_desc', 'zone_id', 'stop_url',
                'location_type', 'parent_station',
                'wheelchair_boarding', 'platform_code'
            ],
            'lat': 'stop_lat',
            'lon': 'stop_lon'
        },
    {
        'name': 'BlaBlaCar',
        'input_path': 'RohDaten/BlaBlaCar/stops.txt',
        'output_path': 'Output/BlaBlaCar_stops.csv',
        'columns_to_drop': ['stop_code', 'stop_desc', 'wheelchair_boarding'],
            'lat': 'stop_lat',
            'lon': 'stop_lon'
        }
    ]