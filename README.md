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
├─ process_delta_metabhf.py         # Nachbearbeitung von METABHF CSV zu delta-Format
├─ requirements.txt                 # Python-Abhängigkeiten
├─ README.md
├─ .gitignore
├─ cache/                           # Cache für MD5-Hashes (automatisch erstellt)
├─ data/
│  ├─ external/                     # Externe Referenzdaten
│  │  ├─ swissBOUNDARIES3D_1_5_LV95_LN02.gpkg  # Schweizer Landesgrenzen (GeoPackage)
│  ├─ processed/                    # Verarbeitete Output-Dateien
│  │  ├─ delta/                     # Neue/geänderte Daten
│  │  │  ├─ BFKOORD_WGS             # Neue Haltestellen mit IDs
│  │  │  ├─ BFKOORD_WGS_KOMMAGETRENNT.csv  # CSV-Export
│  │  │  ├─ BAHNHOF                 # BAHNHOF-Format Output
│  │  │  └─ METABHF                 # METABHF mit Umsteigebeziehungen
│  │  ├─ delta.zip                  # Gezippte Delta-Dateien für Weitergabe
│  │  ├─ METABHF.csv                # METABHF aus QGIS (manuell erstellt)
│  │  ├─ OEV_BFKOORD_WGS_KOMMAGETRENNT.csv  # ÖV-Referenzkoordinaten als CSV
│  │  ├─ Flixbus_stops.csv          # Gefilterte Schweizer Flixbus-Haltestellen
│  │  └─ BlaBlaCar_stops.csv        # Gefilterte Schweizer BlaBlaCar-Haltestellen
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
      ├─ config.py                  # Konfiguration (Provider-URLs, IDs)
      ├─ clean_data.py              # Hilfsskript zum Löschen generierter Daten*
      └─ etl/
         ├─ extract.py              # Download (ÖV + GTFS) & Schweiz-Filterung
         ├─ transform.py            # Datenbereinigung & ID-Vergabe
         └─ load.py                 # BAHNHOF-Format Generierung & ZIP-Erstellung
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

8. **Output-Generierung** - Erstellt CSV und BAHNHOF-Format Dateien
   - Zweck: Kompatibilität mit bestehenden ÖV-Import-Systemen

9. **METABHF-Verarbeitung (Manuell)** - Erstellt Umsteigebeziehungen in QGIS
   - Zweck: Verbindung zwischen Fernbus-Haltestellen und nahegelegenen ÖV-Haltestellen
   - Grund für manuelle Verarbeitung: Räumliche Nähe allein reicht nicht - nur sinnvolle Gehwege sollen erfasst werden

### METABHF Post-Processing (Manueller Schritt)

⚠️ **Hinweis**: Die Verarbeitung von METABHF-Daten erfolgt in einem separaten manuellen Schritt nach der ETL-Pipeline:

1. Führe die automatische ETL-Pipeline aus: `python main.py`
2. Führe die manuelle QGIS-Verarbeitung durch
3. Erstelle `metabhf.csv` manuell in QGIS
4. Kopiere `metabhf.csv` nach `data/processed/
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
