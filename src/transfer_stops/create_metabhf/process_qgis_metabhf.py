"""
METABHF Post-Processing Utility

Dieses Skript verarbeitet METABHF NACH der ETL-Pipeline.

Workflow:
1. python main.py                    # ETL-Pipeline ausführen
2. [QGIS] METABHF als CSV erstellen (manueller Schritt)
3. CSV nach data/processed/QGIS_METABHF.csv speichern
4. python process_delta_metabhf.py   # Dieses Skript ausführen

Das Skript:
- Prüft ob QGIS_METABHF.csv in data/processed/ existiert
- Kopiert CSV-Inhalt zu delta/METABHF (behält Original-Format)
- Extrahiert ID-Paare aus dem Format "ID1 ID2 002\n*A Y"
- Fügt am Ende Einträge im Format "ID2 : ID1a, ID1b, ..." hinzu (gruppiert nach ID2)
- Optional: Zipped alle Delta-Dateien
"""

import os
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2]  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
    
from transfer_stops.etl.load import zip_delta_files
from transfer_stops.etl.transform import create_umsteigb_file
from transfer_stops import config

def process_metabhf_file(csv_path=None, output_path=None):
    """
    Verarbeitet METABHF.csv:
    1. Prüft ob CSV existiert
    2. Kopiert Original-Inhalt (ID1 ID2 000 *A Y Format)
    3. Extrahiert ID-Paare
    4. Fügt am Ende Einträge im Format "ID2 : ID1a, ID1b, ..." hinzu
    """
    if csv_path is None:
        csv_path = config.PATHS['qgis_metabhf_csv']
    if output_path is None:
        output_path = config.PATHS['delta_metabhf']
    
    if not os.path.exists(csv_path):
        print(f"\n❌ FEHLER: {csv_path} existiert nicht!")
        print(f"\nDer Prozess kann nicht fortgesetzt werden.")
        print(f"\nBitte stelle sicher, dass:")
        print(f"  1. Die ETL-Pipeline ausgeführt wurde (python main.py)")
        print(f"  2. METABHF in QGIS als CSV erstellt wurde")
        print(f"  3. Die CSV-Datei als {csv_path} gespeichert wurde")
        return False
    
    print(f"\n=== Verarbeite {csv_path} ===")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    entries = []          # (first_id, second_id)
    original_content = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            for line in content.split('\n'):
                cleaned_line = line.strip()
                if cleaned_line.startswith('"') and cleaned_line.endswith('"'):
                    cleaned_line = cleaned_line[1:-1]
                cleaned_line = cleaned_line.replace('"', '')
                
                if cleaned_line and cleaned_line != 'final_line':
                    original_content.append(cleaned_line)
                    
                    parts = cleaned_line.strip().split()
                    if len(parts) >= 2:
                        first_id = parts[0].strip()
                        second_id = parts[1].strip()
                        
                        if first_id.isdigit() and second_id.isdigit() and len(first_id) >= 7 and len(second_id) >= 7:
                            entries.append((first_id, second_id))
    
    except Exception as e:
        print(f"❌ Fehler beim Lesen der CSV: {e}")
        return False
    
    if not entries:
        print("⚠️ Keine gültigen ID-Paare gefunden")
        return False
    
    # Mapping: ID2 -> Liste von ID1
    target_to_starts = {}
    for first_id, second_id in entries:
        target_to_starts.setdefault(second_id, []).append(first_id)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        # 1. Originalinhalt
        f.write('\n'.join(original_content))
        if original_content and not original_content[-1].endswith('\n'):
            f.write('\n')
        
        # 2. Gruppierte Mapping-Zeilen "ID2 : ID1, ID1b, ..."
        for second_id in sorted(target_to_starts.keys()):
            starts_sorted = sorted(target_to_starts[second_id])
            starts_str = ", ".join(starts_sorted)
            f.write(f"{second_id} : {starts_str}\n")
    
    print(f"✅ Original-Inhalt kopiert")
    print(f"✅ {len(entries)} ID-Paare gefunden")
    print(f"✅ {len(target_to_starts)} Ziel-IDs im Format 'ID2 : ID1, ID1b, ...' hinzugefügt")
    print(f"✅ {output_path} erfolgreich erstellt")
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("METABHF Post-Processing")
    print("=" * 60)
    success = process_metabhf_file()
    print("=" * 60)
    
    if not success:
        exit(1)
    
    print("\n" + "=" * 60)
    print("Erstelle UMSTEIGB-Datei...")
    print("=" * 60)
    umsteigb_success = create_umsteigb_file()
    print("=" * 60)
    
    if not umsteigb_success:
        print("\n⚠️  UMSTEIGB konnte nicht erstellt werden")
    
    print("\n⚠️  Möchtest du die Delta-Dateien jetzt zippen?")
    response = input("Delta-Dateien zippen? (ja/nein): ").strip().lower()
    
    if response in ['ja', 'j', 'yes', 'y']:
        print("\n" + "=" * 60)
        print("Zippe Delta-Dateien...")
        print("=" * 60)
        zip_delta_files()
        print("=" * 60)
    else:
        print("\n✅ Delta-Dateien werden nicht gezippt.")
