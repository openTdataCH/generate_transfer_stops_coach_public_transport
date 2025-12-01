"""Verarbeitet delta_metabhf.txt: Entfernt Sonderzeichen und fügt ID-Paare hinzu."""
import os
import re


def process_metabhf_file(metabhf_path='data/processed/delta_metabhf.txt'):
    """
    Verarbeitet delta_metabhf.txt:
    1. Entfernt Ausrufezeichen aus bestehendem Inhalt
    2. Fügt neue Einträge im Format "second_id : first_id" hinzu
    """
    if not os.path.exists(metabhf_path):
        print(f"ℹ️ {metabhf_path} existiert nicht")
        return
    
    print(f"\n=== Verarbeite {metabhf_path} ===")
    
    with open(metabhf_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Entferne Anführungszeichen und Ausrufezeichen
    cleaned_content = content.replace('"', '').replace('!', '')
    cleaned_lines = cleaned_content.split('\n')
    
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


if __name__ == "__main__":
    process_metabhf_file()
