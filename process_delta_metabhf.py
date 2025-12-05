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
- Fügt am Ende Einträge im Format "ID2 : ID1" hinzu
- Optional: Zipped alle Delta-Dateien
"""
import os
from src.transfer_stops.etl.load import zip_delta_files

def process_metabhf_file(csv_path='data/processed/QGIS_METABHF.csv', output_path='data/processed/delta/METABHF'):
    """
    Verarbeitet METABHF.csv:
    1. Prüft ob CSV existiert
    2. Kopiert Original-Inhalt (ID1 ID2 000 *A Y Format)
    3. Extrahiert ID-Paare
    4. Fügt am Ende Einträge im Format "ID2 : ID1" hinzu
    """
    # Prüfe ob CSV existiert
    if not os.path.exists(csv_path):
        print(f"\n❌ FEHLER: {csv_path} existiert nicht!")
        print(f"\nDer Prozess kann nicht fortgesetzt werden.")
        print(f"\nBitte stelle sicher, dass:")
        print(f"  1. Die ETL-Pipeline ausgeführt wurde (python main.py)")
        print(f"  2. METABHF in QGIS als CSV erstellt wurde")
        print(f"  3. Die CSV-Datei als {csv_path} gespeichert wurde")
        return False
    
    print(f"\n=== Verarbeite {csv_path} ===")
    
    # Erstelle delta-Ordner falls nicht vorhanden
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Lese CSV und sammle ID-Paare
    entries = []
    original_content = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Speichere Original-Inhalt und entferne alle Anführungszeichen
            for line in content.split('\n'):
                cleaned_line = line.strip()
                # Entferne äußere Anführungszeichen falls vorhanden
                if cleaned_line.startswith('"') and cleaned_line.endswith('"'):
                    cleaned_line = cleaned_line[1:-1]
                
                # Entferne auch verbleibende Anführungszeichen innerhalb der Zeile
                cleaned_line = cleaned_line.replace('"', '')
                
                # Überspringe Header "final_line"
                if cleaned_line and cleaned_line != 'final_line':
                    original_content.append(cleaned_line)
                    
                    # Extrahiere ID-Paare nur aus Zeilen die mit IDs beginnen
                    parts = cleaned_line.strip().split()
                    if len(parts) >= 2:
                        first_id = parts[0].strip()
                        second_id = parts[1].strip()
                        
                        # Prüfe ob beide IDs numerisch sind (7-8 Stellen)
                        if first_id.isdigit() and second_id.isdigit() and len(first_id) >= 7 and len(second_id) >= 7:
                            entries.append((first_id, second_id))
    
    except Exception as e:
        print(f"❌ Fehler beim Lesen der CSV: {e}")
        return False
    
    if not entries:
        print("⚠️ Keine gültigen ID-Paare gefunden")
        return False
    
    # Schreibe metabhf-Datei
    with open(output_path, 'w', encoding='utf-8') as f:
        # 1. Schreibe Original-Inhalt (ID1 ID2 000 *A Y Format)
        f.write('\n'.join(original_content))
        if original_content and not original_content[-1].endswith('\n'):
            f.write('\n')
        
        # 2. Füge ID-Paare im Format "ID2 : ID1" hinzu
        for first_id, second_id in entries:
            f.write(f"{second_id} : {first_id}\n")
    
    print(f"✅ Original-Inhalt kopiert")
    print(f"✅ {len(entries)} ID-Paare im Format 'ID2 : ID1' hinzugefügt")
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
    
    # Frage ob Delta-Dateien gezippt werden sollen
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
