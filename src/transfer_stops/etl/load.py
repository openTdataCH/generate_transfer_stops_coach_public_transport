"""Erstellt BAHNHOF-Format aus data/processed/delta/bfkoord_wgs."""
import os


def write_bahnhof_format(transportProvider: str):
    """Extrahiert Einträge eines Providers und schreibt sie im BAHNHOF-Format."""
    input_file = "data/processed/delta/bfkoord_wgs"
    output_file = "data/processed/delta/bahnhof_format"

    if not os.path.exists(input_file):
        print(f"Datei nicht gefunden: {input_file}")
        return

    # Einträge des Providers extrahieren
    delta_entries = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if '%' in line and f'[{transportProvider}]' in line:
                try:
                    id_ = line[:8].strip()
                    name_part = line.split('%', 1)[1].strip()
                    name = name_part.split(f' [{transportProvider}]')[0].strip()
                    delta_entries.append(f"{id_:<10} {name}$<1>")
                except Exception:
                    continue

    if not delta_entries:
        print(f"Keine Einträge für {transportProvider} gefunden")
        return

    # Bereits vorhandene Einträge laden
    existing_entries = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and '$<1>' in line:
                    parts = line.split(None, 1)
                    if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 7:
                        existing_entries.add(line)

    # Nur neue Einträge hinzufügen
    entries_to_add = [e for e in delta_entries if e not in existing_entries]

    if entries_to_add:
        with open(output_file, 'a', encoding='utf-8') as f:
            for entry in entries_to_add:
                f.write(entry + '\n')
        print(f"✅ {len(entries_to_add)} neue Einträge von {transportProvider} hinzugefügt")
    else:
        print(f"ℹ️ Alle Einträge von {transportProvider} bereits vorhanden")

