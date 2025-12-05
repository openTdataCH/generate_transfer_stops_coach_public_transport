"""Daten-Transformationen: Koordinaten sammeln, bereinigen, IDs vergeben."""
from transfer_stops import config
import math
import os
import csv


def standardize_lat_lon(df, lat_col, lon_col):
    """Benennt Lat/Lon-Spalten zu 'lat' und 'lon' um."""
    rename_dict = {}
    if lat_col in df.columns:
        rename_dict[lat_col] = 'lat'
    if lon_col in df.columns:
        rename_dict[lon_col] = 'lon'
    df.rename(columns=rename_dict, inplace=True)
    return df


def drop_columns(df, colnames: list, output_path: str):
    """Entfernt angegebene Spalten und speichert DataFrame als CSV."""
    df.drop(columns=[col for col in colnames if col in df.columns], inplace=True)
    df.to_csv(output_path, index=False)


def check_and_add_new_coords(df, transportProvider: str):
    """Sammelt neue Koordinaten ohne ID-Vergabe in data/processed/delta/BFKOORD_WGS."""
    def format_coord(lat, lon):
        return f"{lat:.8f},{lon:.8f}"
    
    existing_coords_path = 'data/raw/oevSammlung/BFKOORD_WGS'
    existing_coords = set()
    
    with open(existing_coords_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 3:
                try:
                    lon, lat = float(parts[1]), float(parts[2])
                    existing_coords.add(format_coord(lat, lon))
                except ValueError:
                    continue

    os.makedirs("data/processed/delta", exist_ok=True)
    output_file_path = os.path.join("data/processed/delta", "BFKOORD_WGS")
    
    new_coords = set()
    if os.path.exists(output_file_path):
        with open(output_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                coords_part = line.split('%')[0].strip() if '%' in line else line.strip()
                parts = coords_part.split()
                if len(parts) >= 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                        new_coords.add(format_coord(lat, lon))
                    except ValueError:
                        continue

    to_add = []
    for _, row in df.iterrows():
        lon, lat, name = float(row['lon']), float(row['lat']), row['stop_name']
        coord_key = format_coord(lat, lon)

        if coord_key in existing_coords:
            print(f"Duplikat (existierend): {name} ({lat}, {lon})")
        elif coord_key in new_coords:
            print(f"Duplikat (gesammelt): {name} ({lat}, {lon})")
        else:
            to_add.append((lon, lat, name))
            new_coords.add(coord_key)

    if to_add:
        mode = 'a' if os.path.exists(output_file_path) else 'w'
        with open(output_file_path, mode, encoding='utf-8') as f:
            for (lon, lat, name) in to_add:
                f.write(f"{lon:>11.6f}{lat:>11.6f}    % {name} [{transportProvider}]\n")
        print(f"✅ {len(to_add)} neue Koordinaten von {transportProvider} gesammelt")
    else:
        print(f"ℹ️ Keine neuen Koordinaten von {transportProvider}")


def bfkoord_wgs_to_csv(input_path: str, output_path: str):
    """Konvertiert BFKOORD_WGS Format (ID LON LAT % NAME) zu CSV."""
    if not os.path.exists(input_path):
        print(f"⚠️ Datei {input_path} existiert nicht - überspringe CSV-Erstellung")
        return
    
    rows = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            if '%' in line:
                left, right = line.split('%', 1)
                name = right.strip()
            else:
                left, name = line, ''
            
            parts = left.split()
            if len(parts) >= 3:
                try:
                    id_, lon, lat = parts[0], float(parts[1]), float(parts[2])
                    provider = name.split('[')[-1].split(']')[0] if '[' in name and ']' in name else ''
                    rows.append({'id': id_, 'lon': lon, 'lat': lat, 'name': name, 'provider': provider})
                except ValueError:
                    continue
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=['id', 'lon', 'lat', 'name', 'provider'])
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✅ CSV erstellt: {output_path} ({len(rows)} Einträge)")


def convert_all_bfkoord_to_csv():
    """Konvertiert delta/BFKOORD_WGS und oevSammlung/BFKOORD_WGS zu CSV."""
    bfkoord_wgs_to_csv('data/raw/oevSammlung/BFKOORD_WGS', 'data/processed/OEV_BFKOORD_WGS_KOMMAGETRENNT.csv')
    bfkoord_wgs_to_csv('data/processed/delta/BFKOORD_WGS', 'data/processed/BFKOORD_WGS_KOMMAGETRENNT.csv')


def clean_delta_bfkoord_wgs(file_path: str = 'data/processed/delta/BFKOORD_WGS',
                              distance_threshold_meters: float = 100):
    """
    Bereinigt BFKOORD_WGS in-place:
    1. Entfernt FlixTrain-Einträge
    2. Entfernt räumlich nahe Duplikate (< distance_threshold_meters)
    """
    def haversine_distance(lat1, lon1, lat2, lon2):
        """
        Berechnet die kürzeste Distanz zwischen zwei GPS-Koordinaten auf der Erdkugel.
        
        Die Haversine-Formel berücksichtigt die Erdkrümmung und berechnet die Luftlinie
        entlang der Erdoberfläche (Großkreis-Distanz). Benannt nach der Haversine-Funktion
        (hav(θ) = sin²(θ/2)), die numerisch stabiler ist als andere trigonometrische Formeln.
        
        Returns: Distanz in Metern
        """
        R = 6371000  # Erdradius in Metern
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    if not os.path.exists(file_path):
        print(f"⚠️ Datei {file_path} existiert nicht")
        return
    
    cleaned_entries = []
    removed_flixtrain = 0
    
    # Einlesen und FlixTrain-Filter
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            coords_part, name = line.split('%', 1) if '%' in line else (line, '')
            name = name.strip()
            
            if 'flixtrain' in name.lower():
                removed_flixtrain += 1
                continue
            
            parts = coords_part.strip().split()
            if len(parts) >= 2:
                try:
                    lon, lat = float(parts[0]), float(parts[1])
                    cleaned_entries.append({'lon': lon, 'lat': lat, 'name': name, 'line': line})
                except ValueError:
                    continue
    
    # Räumliche Duplikate entfernen
    final_entries = []
    removed_duplicates = []
    
    for entry in cleaned_entries:
        is_duplicate = False
        closest_existing = None
        min_distance = float('inf')
        
        for existing in final_entries:
            distance = haversine_distance(entry['lat'], entry['lon'], existing['lat'], existing['lon'])
            if distance < distance_threshold_meters and distance < min_distance:
                is_duplicate = True
                min_distance = distance
                closest_existing = existing
        
        if is_duplicate:
            removed_duplicates.append({
                'name': entry['name'],
                'distance': min_distance,
                'kept_name': closest_existing['name']
            })
        else:
            final_entries.append(entry)
    
    # Zurückschreiben
    with open(file_path, 'w', encoding='utf-8') as f:
        for entry in final_entries:
            f.write(entry['line'] + '\n')
    
    print(f"✅ Bereinigung abgeschlossen:")
    print(f"   - {removed_flixtrain} FlixTrain-Einträge entfernt")
    print(f"   - {len(removed_duplicates)} räumliche Duplikate entfernt (< {distance_threshold_meters}m)")
    print(f"   - {len(final_entries)} Einträge behalten")
    
    if removed_duplicates:
        print(f"\n   Entfernte Duplikate:")
        for dup in removed_duplicates[:10]:
            print(f"     - {dup['name']}")
            print(f"       → {dup['distance']:.1f}m zu: {dup['kept_name']}")
        if len(removed_duplicates) > 10:
            print(f"     ... und {len(removed_duplicates) - 10} weitere")


def assign_ids_to_delta(file_path: str = 'data/processed/delta/BFKOORD_WGS'):
    """Vergibt IDs an bereinigte Koordinaten in BFKOORD_WGS (in-place)."""
    if not os.path.exists(file_path):
        print(f"⚠️ Datei {file_path} existiert nicht")
        return
    
    # Sammle bereits verwendete IDs aus BFKOORD_WGS
    used_ids = set()
    with open('data/raw/oevSammlung/BFKOORD_WGS', 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if parts:
                try:
                    used_ids.add(int(parts[0]))
                except ValueError:
                    continue
    
    # Lese Einträge und weise IDs zu
    entries_with_ids = []
    beginning_id = config.BEGINNING_ID
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            coords_part, name = line.split('%', 1) if '%' in line else (line, '')
            name = name.strip()
            parts = coords_part.strip().split()
            
            if len(parts) >= 2:
                try:
                    lon, lat = float(parts[0]), float(parts[1])
                    
                    # Finde nächste freie ID
                    candidate = beginning_id
                    while candidate in used_ids:
                        candidate += 1
                    used_ids.add(candidate)
                    new_id = str(candidate).zfill(7)
                    
                    entries_with_ids.append({'id': new_id, 'lon': lon, 'lat': lat, 'name': name})
                except ValueError:
                    continue
    
    # Schreibe mit IDs zurück
    with open(file_path, 'w', encoding='utf-8') as f:
        for entry in entries_with_ids:
            f.write(f"{entry['id']:<8}{entry['lon']:>11.6f}{entry['lat']:>11.6f}{'':>5}    % {entry['name']}\n")
    
    print(f"✅ IDs vergeben: {len(entries_with_ids)} Einträge")





