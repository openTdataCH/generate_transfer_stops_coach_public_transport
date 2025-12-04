"""Erstellt BAHNHOF-Format aus data/processed/delta/BFKOORD_WGS."""
import os
import zipfile
import re


def write_bahnhof_format(transportProvider: str):
    """Extrahiert Einträge eines Providers und schreibt sie im BAHNHOF-Format."""
    input_file = "data/processed/delta/BFKOORD_WGS"
    output_file = "data/processed/delta/BAHNHOF"

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


def zip_delta_files():
    """
    Zipped alle Dateien im delta/ Ordner.
    Prüft zuerst ob metabhf existiert und korrekt formatiert ist.
    """
    delta_dir = "data/processed/delta"
    metabhf_path = os.path.join(delta_dir, "metabhf")
    output_zip = "data/processed/delta.zip"
    
    # Prüfe ob metabhf existiert
    if not os.path.exists(metabhf_path):
        print("\n⚠️  metabhf existiert nicht!")
        print("   Möchtest du die Delta-Dateien trotzdem zippen?")
        response = input("   (ja/nein): ").strip().lower()
        if response not in ['ja', 'j', 'yes', 'y']:
            print("❌ Zippen abgebrochen.")
            return False
    else:
        # Prüfe letzte Zeile von metabhf
        with open(metabhf_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if not lines:
            print("\n⚠️  metabhf ist leer!")
            print("   Bitte führe 'python process_delta_metabhf.py' aus.")
            response = input("   Trotzdem zippen? (ja/nein): ").strip().lower()
            if response not in ['ja', 'j', 'yes', 'y']:
                print("❌ Zippen abgebrochen.")
                return False
        else:
            last_line = lines[-1]
            # Prüfe Format: "Zahl : Zahl"
            if not re.match(r'^\d+\s*:\s*\d+$', last_line):
                print(f"\n⚠️  metabhf ist noch nicht fertig!")
                print(f"   Letzte Zeile: {last_line}")
                print(f"   Erwartet: Format wie '8577245 : 1700007'")
                print(f"\n   Bitte führe 'python process_delta_metabhf.py' aus.")
                response = input("   Trotzdem zippen? (ja/nein): ").strip().lower()
                if response not in ['ja', 'j', 'yes', 'y']:
                    print("❌ Zippen abgebrochen.")
                    return False
    
    # Zippe alle Dateien im delta/ Ordner
    if not os.path.exists(delta_dir):
        print(f"❌ Delta-Ordner nicht gefunden: {delta_dir}")
        return False
    
    files_to_zip = [f for f in os.listdir(delta_dir) if os.path.isfile(os.path.join(delta_dir, f))]
    
    if not files_to_zip:
        print("⚠️  Keine Dateien im Delta-Ordner gefunden.")
        return False
    
    print(f"\n📦 Erstelle ZIP-Archiv: {output_zip}")
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for filename in files_to_zip:
            file_path = os.path.join(delta_dir, filename)
            zipf.write(file_path, arcname=filename)
            print(f"   ✅ {filename}")
    
    print(f"✅ Delta-Dateien erfolgreich gezippt: {output_zip}")
    return True

