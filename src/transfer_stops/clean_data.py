"""Löscht heruntergeladene Rohdaten und generierte Outputs."""
import os
import shutil


def clean_data():
    """Löscht Rohdaten und Output-Dateien."""
    print("=" * 50)
    print("Lösche Rohdaten und Output-Dateien...")
    print("=" * 50)
    
    raw_data_folders = ['data/raw/Flixbus', 'data/raw/BlaBlaCar', 'data/raw/oevSammlung']
    output_folder = 'data/processed'
    deleted_count = 0
    
    # Lösche Inhalte der Rohdaten-Ordner
    print("\n--- Lösche Rohdaten ---")
    for folder in raw_data_folders:
        if os.path.exists(folder):
            try:
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"✅ Gelöscht: {file_path}")
                        deleted_count += 1
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"✅ Gelöscht: {file_path}")
                        deleted_count += 1
            except Exception as e:
                print(f"❌ Fehler beim Löschen in {folder}: {e}")
        else:
            print(f"ℹ️  Nicht vorhanden: {folder}")
    
    # Lösche Output-Dateien
    print("\n--- Lösche Output-Dateien ---")
    if os.path.exists(output_folder):
        for filename in os.listdir(output_folder):
            file_path = os.path.join(output_folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"✅ Gelöscht: {file_path}")
                    deleted_count += 1
                elif os.path.isdir(file_path):
                    for subfile in os.listdir(file_path):
                        subfile_path = os.path.join(file_path, subfile)
                        # Überspringe metabhf
                        if subfile.lower() == 'metabhf':
                            print(f"ℹ️  Übersprungen: {subfile_path}")
                            continue
                        if os.path.isfile(subfile_path):
                            os.remove(subfile_path)
                            print(f"✅ Gelöscht: {subfile_path}")
                            deleted_count += 1
            except Exception as e:
                print(f"❌ Fehler beim Löschen von {file_path}: {e}")
    else:
        print(f"ℹ️  Nicht vorhanden: {output_folder}")
    
    print("\n" + "=" * 50)
    print(f"✅ Bereinigung abgeschlossen! {deleted_count} Elemente gelöscht.")
    print("=" * 50)


if __name__ == "__main__":
    print("\n⚠️  WARNUNG: Dies löscht alle Rohdaten und Output-Dateien!")
    print("   - Alle Dateien in data/raw/")
    print("   - Alle Dateien in data/processed/")
    print()
    
    response = input("Möchtest du fortfahren? (ja/nein): ").strip().lower()
    
    if response in ['ja', 'j', 'yes', 'y']:
        clean_data()
    else:
        print("\n❌ Abgebrochen.")
