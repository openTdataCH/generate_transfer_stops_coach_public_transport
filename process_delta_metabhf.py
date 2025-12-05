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


def create_umsteigb_file(bahnhof_path='data/processed/delta/BAHNHOF', 
                         metabhf_path='data/processed/delta/METABHF',
                         output_path='data/processed/delta/UMSTEIGB'):
    """
    Erstellt UMSTEIGB-Datei aus BAHNHOF und METABHF.
    
    Format: ID NUMMER NUMMER NAME
    - ID: aus BAHNHOF
    - NUMMER: dritte Zahl aus METABHF-Zeilen wo die ID vorkommt (2x wiederholt)
    - NAME: aus BAHNHOF ohne $<1> am Ende
    """
    # Prüfe ob benötigte Dateien existieren
    if not os.path.exists(bahnhof_path):
        print(f"\n❌ FEHLER: {bahnhof_path} existiert nicht!")
        return False
    
    if not os.path.exists(metabhf_path):
        print(f"\n❌ FEHLER: {metabhf_path} existiert nicht!")
        return False
    
    print(f"\n=== Erstelle UMSTEIGB-Datei ===")
    
    # 1. Lese BAHNHOF und extrahiere ID -> Name Mapping
    id_to_name = {}
    with open(bahnhof_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Format: "ID      NAME$<1>"
            parts = line.split(None, 1)  # Split bei erstem Whitespace
            if len(parts) == 2:
                id_str = parts[0].strip()
                name_with_suffix = parts[1].strip()
                
                # Entferne $<1> am Ende
                name = name_with_suffix.replace('$<1>', '').strip()
                id_to_name[id_str] = name
    
    # 2. Lese METABHF und finde dritte Zahl für jede ID
    id_to_number = {}
    with open(metabhf_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('*'):
                continue
            
            # Format: "ID1 ID2 003" oder "ID2 : ID1"
            parts = line.split()
            if len(parts) >= 3 and parts[1] != ':':
                first_id = parts[0].strip()
                third_number = parts[2].strip()
                
                # Speichere nur wenn es eine unserer IDs ist
                if first_id in id_to_name:
                    # Nehme immer den ersten gefundenen Wert (falls ID mehrmals vorkommt)
                    if first_id not in id_to_number:
                        # Nehme nur die letzten 2 Ziffern
                        id_to_number[first_id] = third_number[-2:] if len(third_number) >= 2 else third_number.zfill(2)
    
    # 3. Erstelle UMSTEIGB-Einträge
    entries = []
        
    # Sortierte IDs für konsistente Ausgabe
    for id_str in sorted(id_to_name.keys()):
        name = id_to_name[id_str]
        number = id_to_number.get(id_str, "00")  # Default: 00 falls nicht in METABHF
        
        entries.append(f"{id_str} {number} {number} {name}")
    
    # 4. Schreibe UMSTEIGB-Datei
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(entries) + '\n')
    
    print(f"✅ UMSTEIGB erstellt: {len(entries)-1} Einträge (+ Header)")
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
    
    # Erstelle UMSTEIGB-Datei
    print("\n" + "=" * 60)
    print("Erstelle UMSTEIGB-Datei...")
    print("=" * 60)
    umsteigb_success = create_umsteigb_file()
    print("=" * 60)
    
    if not umsteigb_success:
        print("\n⚠️  UMSTEIGB konnte nicht erstellt werden")
    
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
