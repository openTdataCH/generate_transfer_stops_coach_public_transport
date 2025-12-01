# Generate Transfer Stops Coach Public Transport

ETL-Pipeline zur automatischen Verarbeitung von Fernbus-Haltestellen (Flixbus, BlaBlaCar) und deren Integration in das öffentliche Verkehrsnetz der Schweiz.

## Übersicht

Das Projekt lädt automatisch GTFS-Daten von Fernbus-Anbietern und ÖV-Referenzdaten herunter, filtert Haltestellen in der Schweiz, bereinigt Duplikate und bereitet sie für die Integration in bestehende ÖV-Systeme auf.

**ETL-Pipeline:**
- **Extract**: Automatischer Download von ÖV-Referenzdaten und GTFS-Daten, Filterung auf Schweizer Haltestellen
- **Transform**: Koordinatensammlung, Datenbereinigung (FlixTrain-Entfernung, räumliche Duplikate), ID-Vergabe
- **Load**: Erzeugung von Output-Dateien im BFKOORD_WGS und BAHNHOF-Format

## Projektstruktur

```
├─ main.py                          # Haupteinstiegspunkt der Pipeline
├─ process_delta_metabhf.py         # Manuelle Nachbearbeitung von METABHF-Daten
├─ README.md
├─ .gitignore
├─ cache/                           # Cache für MD5-Hashes
├─ data/
│  ├─ external/                     # Externe Referenzdaten
│  │  └─ swiss_landesgebiet.geojson # Schweizer Landesgrenze
│  ├─ processed/                    # Verarbeitete Output-Dateien
│  │  ├─ delta/                     # Neue/geänderte Daten
│  │  │  ├─ bfkoord_wgs            # Neue Haltestellen mit IDs
│  │  │  ├─ bfkoord_wgs_kommagetrennt.csv
│  │  │  ├─ bahnhof_format         # BAHNHOF-Format Output
│  │  │  └─ metabhf.txt            # METABHF ID-Paare (aus QGIS)
│  │  ├─ BFKOORD_WGS_kommagetrennt.csv
│  │  └─ {Provider}_stops.csv
│  └─ raw/                          # Rohdaten (automatisch heruntergeladen)
│     ├─ BlaBlaCar/stops.txt
│     ├─ Flixbus/stops.txt
│     └─ oevSammlung/               # ÖV-Referenzdaten Schweiz
│        ├─ BAHNHOF
│        ├─ BFKOORD_WGS
│        ├─ METABHF
│        └─ UMSTEIGB
└─ src/transfer_stops/
   ├─ config.py                     # Konfiguration (Provider-URLs, IDs)
   ├─ clean_data.py                 # Hilfsskript zum Löschen generierter Daten*
   └─ etl/
      ├─ extract.py                 # Download (ÖV + GTFS) & Schweiz-Filterung
      ├─ transform.py               # Datenbereinigung & ID-Vergabe
      └─ load.py                    # BAHNHOF-Format Generierung
```

*`clean_data.py` ist ein temporäres Hilfsskript für Entwicklung/Testing, um generierte Daten schnell zu löschen und die Pipeline neu zu starten.

## Installation

```bash
pip install pandas geopandas shapely requests
```

## Verwendung

```bash
python main.py
```

**Pipeline-Ablauf:**

1. **Download ÖV-Referenzdaten** - Lädt BAHNHOF, BFKOORD_WGS, METABHF, UMSTEIGB von opentransportdata.swiss
2. **Download GTFS-Daten** - Lädt Flixbus und BlaBlaCar Haltestellen
3. **Änderungserkennung** - MD5-Hash-Vergleich, überspringt Verarbeitung wenn keine Änderungen
4. **Provider-Verarbeitung** - Filtert Schweizer Haltestellen, sammelt neue Koordinaten (ohne IDs)
5. **Datenbereinigung** - Entfernt FlixTrain-Einträge und räumliche Duplikate (< 100m)
6. **ID-Vergabe** - Vergibt fortlaufende IDs ab 1700000 an bereinigte Daten
7. **Output-Generierung** - Erstellt CSV und BAHNHOF-Format Dateien

### METABHF Post-Processing (Manueller Schritt)

⚠️ **Hinweis**: Die Verarbeitung von METABHF-Daten erfolgt in einem separaten manuellen Schritt nach der ETL-Pipeline:

1. Führe die automatische ETL-Pipeline aus: `python main.py`
2. Führe die manuelle QGIS-Verarbeitung durch
3. Erstelle `metabhf.txt` manuell in QGIS
4. Kopiere `metabhf.txt` nach `data/processed/delta/`
5. Führe das Post-Processing-Skript aus: `python process_delta_metabhf.py`

Das `process_delta_metabhf.py` Skript entfernt Sonderzeichen und erstellt ID-Paare im Format "ID2 : ID1".

## Datenbereinigung

Die Pipeline führt automatisch folgende Bereinigungen durch:

- **FlixTrain-Filter**: Entfernt alle Einträge mit "FlixTrain" im Namen (case-insensitive)
- **Räumliche Duplikate**: Entfernt Haltestellen die näher als 100m zueinander liegen (Haversine-Formel)
- **Duplikatsprüfung**: Vergleicht mit bestehenden ÖV-Daten um Duplikate zu vermeiden

**Wichtig**: IDs werden erst NACH der Bereinigung vergeben, um keine ID-Lücken zu erzeugen.

## Ausgabedateien

### BFKOORD_WGS Format
```
ID       LON        LAT         % NAME [PROVIDER]
1700000    7.348153  46.223220    % Sion (Rue Traversière) [Flixbus]
```

### BAHNHOF Format
```
ID         NAME$<1>
1700000    Sion (Rue Traversière)$<1>
```

### Generierte Dateien
- `delta/bfkoord_wgs` - Neue Haltestellen mit Koordinaten und IDs
- `delta/bfkoord_wgs_kommagetrennt.csv` - CSV-Export der neuen Haltestellen
- `delta/bahnhof_format` - BAHNHOF-Format für ÖV-Systeme
- `delta/metabhf.txt` - METABHF ID-Paare (manuell erstellt in QGIS)
- `BFKOORD_WGS_kommagetrennt.csv` - Alle ÖV-Referenz-Koordinaten als CSV
- `{Provider}_stops.csv` - Gefilterte Schweizer Haltestellen pro Provider

## Konfiguration

`src/transfer_stops/config.py`:
- `BEGINNING_ID`: Start-ID für neue Haltestellen (Standard: 1700000)
- `OEV_SAMMLUNG_URL`: Permalink zu ÖV-Referenzdaten
- `providers`: Liste der Transport-Provider mit GTFS-URLs

## Clean-Up

```bash
python src/transfer_stops/clean_data.py
```

Löscht alle heruntergeladenen und generierten Daten für einen kompletten Neustart der Pipeline.
