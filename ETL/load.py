import os
import pandas as pd

def write_bahnhof_format(transportProvider: str):
    input_file = os.path.join("Output", "new_bfkoord_wgs")
    output_file = os.path.join("Output", "new_bahnhof_format.txt")

    # Falls input_file nicht existiert, erstelle eine leere Datei
    if not os.path.exists(input_file):
        with open(input_file, 'w', encoding='utf-8') as f:
            pass
    # Falls output_file nicht existiert, erstelle eine leere Datei
    if not os.path.exists(output_file):
        with open(output_file, 'w', encoding='utf-8') as f:
            pass

    if not os.path.exists(input_file):
        print(f"Datei nicht gefunden: {input_file}")
        return

    # Einträge aus der Eingabedatei lesen und nach Transport Provider filtern
    new_entries = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if '%' in line and f'[{transportProvider}]' in line:
                try:
                    id_ = line[:8].strip()
                    # Extrahiere den Namen und entferne den Provider-Tag
                    name_part = line.split('%', 1)[1].strip()
                    # Entferne [transportProvider] am Ende
                    name = name_part.split(f' [{transportProvider}]')[0].strip()
                    formatted_entry = f"{id_:<10} {name}$<1>"
                    new_entries.append(formatted_entry)
                except Exception:
                    continue

    if not new_entries:
        print(f"Keine gültigen Einträge für {transportProvider} in der Eingabedatei gefunden.")
        return

    # Bestehende Einträge aus der unified Ausgabedatei lesen und validieren (falls vorhanden)
    existing_entries = set()
    corrupted_lines = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    # Validierung: Prüfe ob die Zeile das erwartete Format hat
                    if '$<1>' in line and len(line.split()) >= 2:
                        # Prüfe auf korrekte ID am Anfang (7-stellige Zahl)
                        parts = line.split(None, 1)
                        if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 7:
                            existing_entries.add(line)
                        else:
                            corrupted_lines.append((line_num, line))
                    else:
                        corrupted_lines.append((line_num, line))

    # Warnung bei korrupten Zeilen
    if corrupted_lines:
        print(f"{len(corrupted_lines)} korrupte Zeilen in {output_file} gefunden:")
        for line_num, line in corrupted_lines:
            print(f"   Zeile {line_num}: '{line}'")
        
        # Datei bereinigen (korrupte Zeilen entfernen)
        if existing_entries:
            print(f"Bereinige Datei und entferne korrupte Zeilen...")
            with open(output_file, 'w', encoding='utf-8') as f:
                for entry in sorted(existing_entries):
                    f.write(entry + '\n')

    # Nur neue Einträge identifizieren
    entries_to_add = [entry for entry in new_entries if entry not in existing_entries]

    if entries_to_add:
        # Neue Einträge anhängen
        with open(output_file, 'a', encoding='utf-8') as f:
            for entry in entries_to_add:
                f.write(entry + '\n')
        print(f"{len(entries_to_add)} neue Einträge von {transportProvider} hinzugefügt nach: {output_file}")
    else:
        print(f"Alle {len(new_entries)} Einträge von {transportProvider} waren bereits vorhanden in: {output_file}")

