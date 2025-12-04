"""Konfiguration für Transport-Provider und ID-Vergabe."""

# Start-ID für neue Haltestellen
BEGINNING_ID = 1700000

# ÖV-Referenzdaten URL
OEV_SAMMLUNG_URL = 'https://data.opentransportdata.swiss/dataset/timetable-54-2025-hrdf/resource_permalink/oev_sammlung_ch_hrdf_5_40_41_2025_20251128_211010.zip'

# ÖV-Referenzdateien
oev_files = ['BAHNHOF', 'BFKOORD_WGS', 'METABHF', 'UMSTEIGB']

# Transport-Provider Konfigurationen
providers = [
    {
        'name': 'Flixbus',
        'gtfs_url': 'https://gtfs.gis.flix.tech/gtfs_generic_eu.zip',
        'input_path': 'data/raw/Flixbus/stops.txt',
        'output_path': 'data/processed/Flixbus_stops.csv',
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
        'gtfs_url': 'https://drive.google.com/uc?export=download&id=1VXd01yQl2Mb67C9bkrx0P8sqNIL532_o',
        'input_path': 'data/raw/BlaBlaCar/stops.txt',
        'output_path': 'data/processed/BlaBlaCar_stops.csv',
        'columns_to_drop': ['stop_code', 'stop_desc', 'wheelchair_boarding'],
        'lat': 'stop_lat',
        'lon': 'stop_lon'
    }
]

