import configuration

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
    csv_file_path = os.path.join("Output", "new_bfkoord_wgs_kommagetrennt.csv")

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

    # Determine beginning ID and collect used IDs from reference and existing unified file
    beginning_id = configuration.BEGINNING_ID
    existing_ids = set()
    # Re-read reference file to collect IDs (if not already collected)
    with open(existing_coords_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if parts:
                try:
                    existing_ids.add(int(parts[0]))
                except ValueError:
                    continue

    new_ids = set()
    if os.path.exists(output_file_path):
        with open(output_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if parts:
                    try:
                        new_ids.add(int(parts[0]))
                    except ValueError:
                        continue
    # also include ids from comma-separated file if present
    if os.path.exists(csv_file_path):
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                # skip header if present
                first = f.readline()
                if first and first.strip().split(',')[0].isdigit():
                    # first line is data, process it
                    f.seek(0)
                for line in f:
                    parts = line.strip().split(',')
                    if parts:
                        try:
                            new_ids.add(int(parts[0]))
                        except ValueError:
                            continue
        except Exception:
            pass

    # used_ids contains IDs that must not be reused
    used_ids = existing_ids.union(new_ids)

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
        # open both files: fixed-width and comma-separated
        write_csv_header = not os.path.exists(csv_file_path)
        with open(output_file_path, mode, encoding='utf-8') as f, open(csv_file_path, 'a', encoding='utf-8') as fcsv:
            if write_csv_header:
                fcsv.write('id,lon,lat,name,provider\n')
            for (lon, lat, name) in to_add:
                # find next unused id starting from beginning_id
                candidate = beginning_id
                while candidate in used_ids:
                    candidate += 1
                used_ids.add(candidate)
                new_id = str(candidate).zfill(7)
                # write fixed-width line
                f.write(f"{new_id:<8}{lon:>11.6f}{lat:>11.6f}{'':>5}    % {name} [{transportProvider}]\n")
                # write comma-separated line
                # escape double quotes in name
                safe_name = name.replace('"', '""')
                fcsv.write(f'{new_id},{lon:.6f},{lat:.6f},"{safe_name}",{transportProvider}\n')
        print(f"✅ {len(to_add)} neue Koordinaten von {transportProvider} hinzugefügt nach: {output_file_path}")
    else:
        print(f"ℹ️ Keine neuen Koordinaten von {transportProvider} gefunden - alle waren bereits vorhanden.")


def convert_bfkoord_to_csv(existing_coords_path: str = 'RohDaten/oevSammlung/BFKOORD_WGS', out_csv_path: str | None = None):
    """
    Convert the fixed-width/space-separated BFKOORD_WGS file into a comma-separated CSV.

    Parameters:
    - existing_coords_path: path to the original BFKOORD_WGS file
    - out_csv_path: optional path for output CSV. If None, writes to the same directory with suffix '_kommagetrennt.csv'

    The function parses lines like:
      0000002   26.074412   44.446770 0      % Bucuresti
    and writes CSV rows: id,lon,lat,name
    """
    import csv

    if not os.path.exists(existing_coords_path):
        raise FileNotFoundError(f"Reference file not found: {existing_coords_path}")

    if out_csv_path is None:
        base_dir = os.path.dirname(existing_coords_path)
        base_name = os.path.basename(existing_coords_path)
        out_csv_path = os.path.join(base_dir, base_name + '_kommagetrennt.csv')

    rows = []
    with open(existing_coords_path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            # split off name after '%' if present
            if '%' in line:
                left, right = line.split('%', 1)
                name = right.strip()
            else:
                left = line
                name = ''

            parts = left.split()
            # Expect at least id, lon, lat
            if len(parts) >= 3:
                try:
                    id_ = int(parts[0])
                    lon = float(parts[1])
                    lat = float(parts[2])
                except ValueError:
                    # skip lines that do not match
                    continue
                rows.append({'id': id_, 'lon': lon, 'lat': lat, 'name': name})

    # write CSV
    os.makedirs(os.path.dirname(out_csv_path) or '.', exist_ok=True)
    with open(out_csv_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=['id', 'lon', 'lat', 'name'])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    return out_csv_path



