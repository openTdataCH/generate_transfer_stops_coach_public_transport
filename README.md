# generate_transfer_stops_coach_public_transport

ETL‑Projekt zur Sammlung und Vereinheitlichung von Haltestellen‑Koordinaten (FlixBus, BlaBlaCar) und zur Erzeugung eines einheitlichen Ausgabefiles im projekt‑spezifischen Format.

Kurzbeschreibung
- Extract: Einlesen von Provider‑stops (CSV/GTFS stops.txt von Flixbus und BlaBlaCar) und Filtern auf Haltestellen in der Schweiz (GeoJSON‑Fläche).
- Transform: Spalten vereinheitlichen, irrelevante Spalten entfernen, Duplikate prüfen.
- Load: Neue Koordinaten und ein "bahnhof"‑Format in `Output/` schreiben.

Wichtige Dateien / Ordner
- `ETL/extract.py` - Einlese‑und Filterlogik (z. B. `extract_swiss_stops_csv`).
- `ETL/transform.py` - Transformationen (Standardisierung von lat/lon, Duplikat‑Prüfung, Schreiben in `new_bfkoord_wgs`).
- `ETL/load.py` - Erzeugt `new_bahnhof_format.txt` (und `new_bfkoord_wgs.txt`).
- `RohDaten/` - Rohdatenquellen (z. B. `FlixBus/stops.txt`, `BlaBlaCar/stops.txt`, GeoJSON in `swisstopoinspiregeoportal`).
- `Output/` - Generierte CSV/TXT Ergebnisse.
- `Tests/` - Einfache pytest Tests für ETL‑Module.


