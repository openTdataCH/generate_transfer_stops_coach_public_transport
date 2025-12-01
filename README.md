# Generate Transfer Stops Coach Public Transport

ETL-Pipeline zur automatischen Verarbeitung von Fernbus-Haltestellen (Flixbus, BlaBlaCar) und deren Integration in das öffentliche Verkehrsnetz der Schweiz.

## Übersicht

Das Projekt lädt automatisch GTFS-Daten von Fernbus-Anbietern herunter, filtert Haltestellen in der Schweiz und bereitet sie für die Integration in bestehende ÖV-Systeme auf.

**ETL-Pipeline:**
- **Extract**: Automatischer Download von GTFS-Daten, Filterung auf Schweizer Haltestellen (GeoJSON-Boundary)
- **Transform**: Standardisierung der Koordinaten, Duplikatsprüfung, ID-Generierung
- **Load**: Erzeugung von Output-Dateien im HAFAS-kompatiblen Format

## Projektstruktur

```
├─ main.py                          # Haupteinstiegspunkt der Pipeline
├─ README.md
├─ .gitignore
├─ cache/                           # Cache für temporäre Daten
├─ data/
│  ├─ external/                     # Externe Referenzdaten
│  │  └─ swiss_landesgebiet.geojson # Schweizer Landesgrenze
│  ├─ processed/                    # Verarbeitete Output-Dateien
│  └─ raw/                          # Rohdaten (automatisch heruntergeladen)
│     ├─ BlaBlaCar/stops.txt
│     ├─ Flixbus/stops.txt
│     └─ oevSammlung/               # Referenzdaten ÖV Schweiz
└─ src/transfer_stops/
   ├─ config.py                     # Konfiguration (Provider-URLs, IDs)
   ├─ clean_data.py                 # Hilfsskript zum Löschen generierter Daten*
   └─ etl/
      ├─ extract.py                 # GTFS-Download & Schweiz-Filterung
      ├─ transform.py               # Koordinaten-Standardisierung & ID-Vergabe
      ├─ load.py                    # Output-Generierung
      └─ process_metabhf.py         # Metabhf-Datei Verarbeitung
```

*`clean_data.py` ist ein temporäres Hilfsskript für Entwicklung/Testing, um generierte Daten schnell zu löschen und die Pipeline neu zu starten.

## Verwendung

```bash
python main.py
```

Die Pipeline:
1. Lädt GTFS-Daten von Flixbus und BlaBlaCar herunter
2. Erkennt automatisch Änderungen (MD5-Hash-Vergleich)
3. Verarbeitet nur bei erkannten Änderungen
4. Fragt am Ende, ob generierte Daten gelöscht werden sollen

## Ausgabedateien

- `data/processed/delta_bfkoord_wgs` - Neue Haltestellen im HAFAS-Format
- `data/processed/delta_bfkoord_wgs_kommagetrennt.csv` - CSV-Export
- `data/processed/BFKOORD_WGS_kommagetrennt.csv` - Vollständige Koordinatenliste
- `data/processed/{Provider}_stops.csv` - Gefilterte Provider-Haltestellen
