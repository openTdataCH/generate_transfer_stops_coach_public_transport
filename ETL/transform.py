def standardize_lat_lon(df, lat_col, lon_col):
    """
    Renames the latitude and longitude columns to 'lat' and 'lon'.
    """
    rename_dict = {}
    if lat_col in df.columns:
        rename_dict[lat_col] = 'lat'
    if lon_col in df.columns:
        rename_dict[lon_col] = 'lon'
    df.rename(columns=rename_dict, inplace=True)
    return df

import os


def drop_columns(df, colnames: list, output_path: str):
    """
    Drops specified columns from the DataFrame and saves the result to CSV.

    Parameters:
    - df: pandas DataFrame
    - colnames: list of columns to drop
    - output_path: path where the cleaned CSV will be saved
    """
    df.drop(columns=[col for col in colnames if col in df.columns], inplace=True)
    df.to_csv(output_path, index=False)

def check_and_add_new_coords(df, transportProvider: str):
    def format_coord(lat, lon):
        return f"{lat:.8f},{lon:.8f}"
    existing_coords_path = 'RohDaten/oevSammlung/BFKOORD_WGS'
    # Load existing coordinates from reference file
    existing_coords = set()
    max_id_existing = 0
    with open(existing_coords_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 3:
                try:
                    id_ = int(parts[0])
                    lon = float(parts[1])
                    lat = float(parts[2])
                    existing_coords.add(format_coord(lat, lon))
                    max_id_existing = max(max_id_existing, id_)
                except ValueError:
                    continue

    # Check unified output file for all transport providers
    os.makedirs("Output", exist_ok=True)
    output_file_path = os.path.join("Output", "new_bfkoord_wgs")

    new_coords = set()
    max_id_new = 0
    if os.path.exists(output_file_path):
        with open(output_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 3:
                    try:
                        id_ = int(parts[0])
                        lon = float(parts[1])
                        lat = float(parts[2])
                        new_coords.add(format_coord(lat, lon))
                        max_id_new = max(max_id_new, id_)
                    except ValueError:
                        continue

    # Find the highest ID from both files
    max_id = max(max_id_existing, max_id_new)

    # Process coordinates from current transport provider
    to_add = []
    for _, row in df.iterrows():
        lon = float(row['lon'])
        lat = float(row['lat'])
        name = row['stop_name']
        coord_key = format_coord(lat, lon)

        if coord_key in existing_coords:
            print(f"Duplicate (in existing data): {name} ({lat}, {lon})")
        elif coord_key in new_coords:
            print(f"Duplicate (in new_bfkoord_wgs): {name} ({lat}, {lon})")
        else:
            to_add.append((lon, lat, name))

    if to_add:
        mode = 'a' if os.path.exists(output_file_path) else 'w'
        with open(output_file_path, mode, encoding='utf-8') as f:
            for i, (lon, lat, name) in enumerate(to_add, start=1):
                new_id = str(max_id + i).zfill(7)
                f.write(f"{new_id:<8}{lon:>11.6f}{lat:>11.6f}{'':>5}    % {name} [{transportProvider}]\n")
        print(f"✅ {len(to_add)} neue Koordinaten von {transportProvider} hinzugefügt nach: {output_file_path}")
    else:
        print(f"ℹ️ Keine neuen Koordinaten von {transportProvider} gefunden - alle waren bereits vorhanden.")



