# Generate Transfer Stops Coach Public Transport

ETL-Pipeline zur automatischen Verarbeitung von Fernbus-Haltestellen (Flixbus, BlaBlaCar) und deren Integration in das öffentliche Verkehrsnetz der Schweiz.

## Übersicht

Das Projekt lädt automatisch GTFS-Daten von Fernbus-Anbietern und ÖV-Referenzdaten herunter, filtert Haltestellen in der Schweiz, bereinigt Duplikate und bereitet sie für die Integration in bestehende ÖV-Systeme auf.

**ETL-Pipeline:**
- **Extract**: Automatischer Download von ÖV-Referenzdaten und GTFS-Daten, Filterung auf Schweizer Haltestellen
- **Transform**: Koordinatensammlung, Datenbereinigung (FlixTrain-Entfernung, räumliche Duplikate), ID-Vergabe
- **Load**: Erzeugung von Output-Dateien im BFKOORD_WGS und BAHNHOF-Format

## Wichtige Ressourcen

### Datenquellen
- **ÖV-Referenzdaten Schweiz**: [opentransportdata.swiss](https://opentransportdata.swiss) - BAHNHOF, BFKOORD_WGS, METABHF, UMSTEIGB
- **Flixbus GTFS**: Haltestellen-Daten im GTFS-Format
- **BlaBlaCar GTFS**: Haltestellen-Daten im GTFS-Format
- **Swiss Boundaries**: [swisstopo swissBOUNDARIES3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d) - Schweizer Landesgrenzen

### Formatbeschreibungen
- **BFKOORD_WGS Format**: `ID LON LAT % NAME` - Standard-Koordinatenformat für ÖV-Haltestellen
- **BAHNHOF Format**: `ID (8 Zeichen linksbündig) + 5 Leerzeichen + NAME$<1>` - Legacy-Format für ÖV-Systeme
- **GTFS (General Transit Feed Specification)**: [gtfs.org](https://gtfs.org/) - Standard für öffentliche Verkehrsdaten
- **METABHF Format**: Umsteigebeziehungen zwischen Haltestellen im Format `ID1 ID2 Distanz` bzw. `ID2 : ID1`
- **UMSTEIGB Format**: `ID NUM NUM NAME` - Umsteigezeiten zwischen Haltestellen, NUM = letzte 2 Ziffern der Gehzeit in Minuten aus METABHF

### Tools & Algorithmen
- **QGIS**: [qgis.org](https://qgis.org/) - Open Source GIS-Software für manuelle Datenverarbeitung
- **Haversine-Formel**: Berechnung der Großkreis-Distanz zwischen GPS-Koordinaten auf einer Kugel
  - Verwendet für: Erkennung räumlicher Duplikate (< 100m Schwellwert)
  - [Wikipedia: Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula)

### ID-Vergabe
**Warum 17xxxxx?**
- ÖV-Haltestellen-IDs in der Schweiz folgen dem UIC-System (8-stellig, beginnend mit 85)
- Fernbus-Haltestellen sind NICHT Teil des regulären ÖV-Netzes
- ID-Bereich **1700000-1799999** wurde gewählt um:
  - Kollisionen mit bestehenden ÖV-IDs zu vermeiden
  - Fernbus-Haltestellen klar vom ÖV-Netz zu unterscheiden
  - Raum für bis zu 100.000 neue Haltestellen zu schaffen

## Projektstruktur

```
├─ main.py                          # Haupteinstiegspunkt der Pipeline
├─ requirements.txt                 # Python-Abhängigkeiten
├─ README.md
├─ .gitignore
├─ data/
│  ├─ external/                     # Externe Referenzdaten
│  │  └─ swissBOUNDARIES3D_1_5_LV95_LN02.gpkg  # Schweizer Landesgrenzen (GeoPackage)
│  ├─ processed/                    # Verarbeitete Output-Dateien
│  │  ├─ delta/                     # Neue/geänderte Daten
│  │  │  ├─ BFKOORD_WGS             # Neue Haltestellen mit IDs
│  │  │  ├─ BAHNHOF                 # BAHNHOF-Format Output
│  │  │  ├─ METABHF                 # METABHF mit Umsteigebeziehungen
│  │  │  └─ UMSTEIGB                # Umsteigezeiten
│  │  ├─ delta.zip                  # Gezippte Delta-Dateien für Weitergabe
│  │  ├─ provider_stops             # Konsolidierte Haltestellen aller Provider
│  │  ├─ BFKOORD_WGS_KOMMAGETRENNT.csv      # CSV der neuen Haltestellen
│  │  ├─ QGIS_METABHF.csv                   # METABHF aus QGIS (manuell erstellt)
│  │  └─ OEV_BFKOORD_WGS_KOMMAGETRENNT.csv  # ÖV-Referenzkoordinaten als CSV
│  └─ raw/                          # Rohdaten (automatisch heruntergeladen)
│     ├─ BlaBlaCar/
│     │  └─ stops.txt               # GTFS stops.txt
│     ├─ Flixbus/
│     │  └─ stops.txt               # GTFS stops.txt
│     └─ oevSammlung/               # ÖV-Referenzdaten Schweiz
│        ├─ BAHNHOF
│        ├─ BFKOORD_WGS
│        ├─ METABHF
│        └─ UMSTEIGB
└─ src/
   └─ transfer_stops/
      ├─ __init__.py
      ├─ config.py                  # Zentrale Konfiguration (Provider-URLs, IDs, Pfade)
      ├─ clean_data.py              # Hilfsskript zum Löschen generierter Daten*
      ├─ etl/                       # Extract-Transform-Load Module
      │  ├─ __init__.py
      │  ├─ extract.py              # Download (ÖV + GTFS) & Schweiz-Filterung
      │  ├─ transform.py            # Datenbereinigung, ID-Vergabe & UMSTEIGB
      │  └─ load.py                 # BAHNHOF-Format Generierung & ZIP-Erstellung
      └─ create_metabhf/            # METABHF-Erstellung (OSM & QGIS)
         ├─ __init__.py
         ├─ create_osm_metabhf.py   # OSM-basierte METABHF-Generierung
         └─ process_qgis_metabhf.py # Nachbearbeitung von QGIS METABHF CSV**
```

**Hinweise:**
- *`clean_data.py` ist ein Hilfsskript für Entwicklung/Testing, um generierte Daten zu löschen
- **`process_qgis_metabhf.py` ist ein Standalone-Script für QGIS-basierte METABHF-Verarbeitung
- Alle Pfade sind zentral in `config.py` konfiguriert

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
   - Zweck: Referenzdaten um Duplikate mit bestehendem ÖV-Netz zu erkennen

2. **Download GTFS-Daten** - Lädt Flixbus und BlaBlaCar Haltestellen
   - Zweck: Erfassung aller Fernbus-Haltestellen in Europa

3. **Änderungserkennung** - MD5-Hash-Vergleich, überspringt Verarbeitung wenn keine Änderungen
   - Zweck: Effizienz - Pipeline läuft nur bei tatsächlichen Datenänderungen

4. **Schweiz-Filterung** - Filtert nur Haltestellen innerhalb der Schweizer Landesgrenzen
   - Verwendet: swissBOUNDARIES3D von swisstopo
   - Grund: Fernbusse halten auch in Deutschland/Italien/Österreich - wir wollen nur CH-Haltestellen

5. **Provider-Verarbeitung** - Sammelt neue Koordinaten pro Provider (ohne IDs)
   - Zweck: Zentrale Koordinatensammlung vor Bereinigung

6. **Datenbereinigung** 
   - **FlixTrain-Filter**: Entfernt alle Einträge mit "FlixTrain" im Namen
     - Grund: FlixTrain ist Schienenverkehr und bereits im ÖV-Netz erfasst
   - **Räumliche Duplikate**: Entfernt Haltestellen < 100m Distanz (Haversine-Formel)
     - Grund: Flixbus/BlaBlaCar nutzen oft dieselben Haltestellen mit leicht unterschiedlichen Koordinaten
   - **ÖV-Duplikate**: Vergleich mit BFKOORD_WGS um bereits erfasste Haltestellen zu filtern

7. **ID-Vergabe** - Vergibt fortlaufende IDs ab 1700000 an bereinigte Daten
   - Zweck: Eindeutige Identifikation für ÖV-Systeme (siehe "ID-Vergabe" oben)
   - Wichtig: IDs werden NACH Bereinigung vergeben um Lücken zu vermeiden

8. **CSV-Export** - Erstellt kommagetrennte CSV-Dateien für beide BFKOORD_WGS-Dateien
   - Zweck: Einfacher Import in QGIS

9. **BAHNHOF-Format** - Konvertiert zu Legacy-Format für ÖV-Systeme
   - Zweck: Kompatibilität mit bestehenden ÖV-Import-Systemen

10. **METABHF-Erstellung** - Automatische oder manuelle Generierung
    - **Automatisch (OSM)**: Verwendet OSMnx für Walking-Distance-Berechnung zwischen Coach- und ÖV-Haltestellen
      - Algorithmus: Findet nächste 3 ÖV-Haltestellen, berechnet Gehweg über Straßennetz, wählt kürzeste Route
      - Distanz → Gehzeit (5 km/h) + 2 Minuten Puffer
    - **Manuell (QGIS)**: Räumliche Analyse in QGIS für präzise Umsteigebeziehungen
      - Workflow: QGIS → CSV Export → `process_qgis_metabhf.py` → delta/METABHF

11. **UMSTEIGB-Generierung** - Erstellt Umsteigezeiten aus BAHNHOF + METABHF
    - Format: Siehe oben
    - Zweck: Definition von Umsteigezeiten zwischen Haltestellen

12. **Delta-ZIP** - Optional: Packt alle Delta-Dateien für Weitergabe
    - Enthält: BFKOORD_WGS, BAHNHOF, METABHF, UMSTEIGB

### METABHF-Verarbeitung

Die Pipeline bietet zwei Workflows für METABHF-Erstellung:

#### Option 1: Automatisch via OSM (Empfohlen)
Wird automatisch in `main.py` ausgeführt. Verwendet OpenStreetMap-Daten für Walking-Distance-Berechnung.

#### Option 2: Manuell via QGIS

⚠️ **Hinweis**: Manuelle QGIS-Verarbeitung erfolgt als separater Workflow:

1. Führe die ETL-Pipeline aus: `python main.py`
2. Öffne QGIS und führe räumliche Analyse durch
3. Exportiere METABHF als CSV nach `data/processed/QGIS_METABHF.csv`
4. Führe Post-Processing aus: `python src/transfer_stops/create_metabhf/process_qgis_metabhf.py`
   - ⚠️ Achtung: Überschreibt existierende `delta/METABHF` und `delta/UMSTEIGB` Dateien

Das Skript:
- Liest QGIS CSV und entfernt Sonderzeichen
- Kopiert Original-Format (ID1 ID2 000 / *A Y)
- Gruppiert nach Ziel-ID: `ID2 : ID1a, ID1b, ...`
- Erstellt automatisch UMSTEIGB (überschreibt vorhandene Datei)
- Optional: Zipped Delta-Dateien

## Ausgabedateien

### Generierte Dateien
- `data/processed/delta/BFKOORD_WGS` - Neue Haltestellen mit Koordinaten und IDs
- `data/processed/delta/BAHNHOF` - BAHNHOF-Format für ÖV-Systeme
- `data/processed/delta/METABHF` - Umsteigebeziehungen (OSM oder QGIS)
- `data/processed/delta/UMSTEIGB` - Umsteigezeiten (automatisch generiert)
- `data/processed/BFKOORD_WGS_KOMMAGETRENNT.csv` - CSV der neuen Haltestellen
- `data/processed/OEV_BFKOORD_WGS_KOMMAGETRENNT.csv` - ÖV-Referenz-Koordinaten als CSV
- `data/processed/provider_stops` - Konsolidierte Schweizer Haltestellen aller Provider
- `data/processed/delta.zip` - Gezippte Delta-Dateien (BFKOORD_WGS, BAHNHOF, METABHF, UMSTEIGB)

## Konfiguration

`src/transfer_stops/config.py`:
- `BEGINNING_ID`: Start-ID für neue Haltestellen (Standard: 1700000)
- `OEV_SAMMLUNG_URL`: Permalink zu ÖV-Referenzdaten
- `oev_files`: Liste der ÖV-Dateien (BAHNHOF, BFKOORD_WGS, METABHF, UMSTEIGB)
- `PATHS`: Dictionary mit allen Dateipfaden (zentrale Pfad-Verwaltung)
- `providers`: Liste der Transport-Provider mit GTFS-URLs und Konfiguration

**Pfad-Verwaltung**: Alle Dateipfade sind zentral in `config.PATHS` definiert, um hardcodierte Pfade zu vermeiden.

## Clean-Up

```bash
python src/transfer_stops/clean_data.py
```

Löscht alle heruntergeladenen und generierten Daten für einen kompletten Neustart der Pipeline.
