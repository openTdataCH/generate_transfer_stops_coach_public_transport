"""Konfiguration für Transport-Provider und ID-Vergabe."""

# Start-ID für neue Haltestellen
BEGINNING_ID = 1700000

# ÖV-Referenzdaten URL
OEV_SAMMLUNG_URL = 'https://data.opentransportdata.swiss/dataset/timetable-54-2025-hrdf/resource_permalink/oev_sammlung_ch_hrdf_5_40_41_2025_20251128_211010.zip'

# ÖV-Referenzdateien
oev_files = ['BAHNHOF', 'BFKOORD_WGS', 'METABHF', 'UMSTEIGB']

# Zentrale Pfad-Konfiguration
PATHS = {
    # Rohdaten
    'oev_bfkoord_wgs': 'data/raw/oevSammlung/BFKOORD_WGS',
    'oev_bahnhof': 'data/raw/oevSammlung/BAHNHOF',
    'oev_metabhf': 'data/raw/oevSammlung/METABHF',
    'oev_umsteigb': 'data/raw/oevSammlung/UMSTEIGB',
    'swiss_boundaries': 'data/external/swissBOUNDARIES3D_1_5_LV95_LN02.gpkg',
    
    # Verarbeitete Daten
    'delta_dir': 'data/processed/delta',
    'delta_bfkoord_wgs': 'data/processed/delta/BFKOORD_WGS',
    'delta_bahnhof': 'data/processed/delta/BAHNHOF',
    'delta_metabhf': 'data/processed/delta/METABHF',
    'delta_umsteigb': 'data/processed/delta/UMSTEIGB',
    'provider_stops': 'data/processed/provider_stops',
    'qgis_metabhf_csv': 'data/processed/QGIS_METABHF.csv',
    'bfkoord_wgs_csv': 'data/processed/BFKOORD_WGS_KOMMAGETRENNT.csv',
    'oev_bfkoord_wgs_csv': 'data/processed/OEV_BFKOORD_WGS_KOMMAGETRENNT.csv',
    'delta_zip': 'data/processed/delta.zip',
}

# Transport-Provider Konfigurationen
providers = [
    {
        'name': 'Flixbus',
        'gtfs_url': 'https://gtfs.gis.flix.tech/gtfs_generic_eu.zip',
        'input_path': 'data/raw/Flixbus/stops.txt',
        'output_path': 'data/processed/provider_stops',
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
        'output_path': 'data/processed/provider_stops',
        'columns_to_drop': ['stop_code', 'stop_desc', 'wheelchair_boarding'],
        'lat': 'stop_lat',
        'lon': 'stop_lon'
    }
]

