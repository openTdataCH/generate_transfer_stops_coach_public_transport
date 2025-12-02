"""
METABHF Post-Processing Utility

Dieses Skript verarbeitet metabhf NACH der ETL-Pipeline.

Workflow:
1. python main.py                    # ETL-Pipeline ausführen
2. [QGIS] metabhf erstellen (manueller Schritt)
3. metabhf nach data/processed/delta/ kopieren
4. python process_delta_metabhf.py   # Dieses Skript ausführen

Das Skript:
- Entfernt Sonderzeichen (Anführungszeichen, Ausrufezeichen)
- Extrahiert ID-Paare aus dem Format "ID1 ID2 000"
- Fügt neue Einträge im Format "ID2 : ID1" hinzu
"""
import os
import re


def process_metabhf_file(metabhf_path='data/processed/delta/metabhf'):
    """
    Verarbeitet metabhf:
    1. Entfernt Ausrufezeichen aus bestehendem Inhalt
    2. Fügt neue Einträge im Format "second_id : first_id" hinzu
    """
    if not os.path.exists(metabhf_path):
        print(f"❌ Datei {metabhf_path} existiert nicht")
        print(f"\nBitte stelle sicher, dass:")
        print(f"  1. Die ETL-Pipeline ausgeführt wurde (python main.py)")
        print(f"  2. metabhf in QGIS erstellt wurde")
        print(f"  3. Die Datei nach {metabhf_path} kopiert wurde")
        return
    
    print(f"\n=== Verarbeite {metabhf_path} ===")
    
    with open(metabhf_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Entferne Anführungszeichen und Ausrufezeichen
    cleaned_content = content.replace('"', '').replace('!', '')
    cleaned_lines = cleaned_content.split('\n')
    
    # Entferne erste Zeile wenn sie nicht mit einer Ziffer beginnt
    if cleaned_lines and cleaned_lines[0].strip() and not cleaned_lines[0].strip()[0].isdigit():
        cleaned_lines.pop(0)
        print("ℹ️ Erste Zeile entfernt (beginnt nicht mit Ziffer)")
    
    # Sammle ID-Paare und existierende Einträge
    entries = []
    existing_colon_entries = set()
    
    for line in cleaned_lines:
        line_stripped = line.strip()
        
        if ' : ' in line_stripped:
            existing_colon_entries.add(line_stripped)
        
        # Extrahiere ID-Paare aus Zeilen wie "1700006 8502056 000"
        if line_stripped and re.match(r'^\d{7,8}\s+\d{7,8}', line_stripped):
            parts = line_stripped.split()
            if len(parts) >= 2:
                entries.append((parts[0], parts[1]))
    
    # Behalte nicht-leere Zeilen
    final_lines = [line for line in cleaned_lines if line.strip()]
    
    # Erstelle neue Einträge im Format "second_id : first_id"
    new_entries = []
    for first_id, second_id in entries:
        new_entry = f"{second_id} : {first_id}"
        if new_entry not in existing_colon_entries:
            new_entries.append(new_entry)
            existing_colon_entries.add(new_entry)
    
    if new_entries:
        final_lines.extend(new_entries)
        print(f"✅ {len(new_entries)} neue Einträge hinzugefügt")
    else:
        print("ℹ️ Keine neuen Einträge")
    
    with open(metabhf_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(final_lines) + '\n')
    
    print(f"✅ {metabhf_path} erfolgreich verarbeitet")


if __name__ == "__main__":
    print("=" * 60)
    print("METABHF Post-Processing")
    print("=" * 60)
    process_metabhf_file()
    print("=" * 60)
