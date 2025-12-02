"""Erstellt BAHNHOF-Format aus data/processed/delta/bfkoord_wgs."""
import os


def write_bahnhof_format(transportProvider: str):
    """Extrahiert Einträge eines Providers und schreibt sie im BAHNHOF-Format."""
    input_file = "data/processed/delta/bfkoord_wgs"
    output_file = "data/processed/delta/bahnhof"

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
                    # Format: ID (8 Zeichen linksbündig, mit Spaces gefüllt) + 5 Spaces + Name + $<1>
                    delta_entries.append(f"{id_:<8}     {name}$<1>")
                except Exception:
                    continue

    if not delta_entries:
        print(f"Keine Einträge für {transportProvider} gefunden")
        return

    # Erstelle Ordner falls nicht vorhanden
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Bereits vorhandene Einträge laden
    existing_entries = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                existing_entries.add(line.rstrip('\n'))

    # Nur neue Einträge hinzufügen
    entries_to_add = [e for e in delta_entries if e not in existing_entries]

    if entries_to_add:
        with open(output_file, 'a', encoding='utf-8') as f:
            for entry in entries_to_add:
                f.write(entry + '\n')
        print(f"✅ {len(entries_to_add)} neue Einträge von {transportProvider} hinzugefügt")
    else:
        print(f"ℹ️ Alle Einträge von {transportProvider} bereits vorhanden")

