import math
import os
import sys
import osmnx as ox
import networkx as nx
import matplotlib.pyplot as plt
import time
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2]  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from transfer_stops import config
from transfer_stops.utils import haversine_distance, estimate_walking_minutes

DELTA_BFKOORD_WGS = config.PATHS['delta_bfkoord_wgs']
OEV_BFKOORD_WGS = config.PATHS['oev_bfkoord_wgs']
METABHF = config.PATHS['delta_metabhf']

CANDIDATE_COUNT = 3
MIN_GRAPH_RADIUS_M = 200
GRAPH_RADIUS_FACTOR = 2.0
MAX_GRAPH_RADIUS_M = 5000

def build_walk_graph(center_point, dist_m):
    G = ox.graph_from_point(
        center_point,
        dist=dist_m,
        network_type="walk"
    )
    return G

def shortest_path_length_m(G, lon1, lat1, lon2, lat2):
    n1 = ox.distance.nearest_nodes(G, lon1, lat1)
    n2 = ox.distance.nearest_nodes(G, lon2, lat2)
    return nx.shortest_path_length(G, n1, n2, weight="length")

def compute_route(G, lon1, lat1, lon2, lat2):
    n1 = ox.distance.nearest_nodes(G, lon1, lat1)
    n2 = ox.distance.nearest_nodes(G, lon2, lat2)
    route = nx.shortest_path(G, n1, n2, weight="length")
    return route

def load_points(path):
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            left = line.split("%")[0].strip()
            parts = left.split()
            if len(parts) < 3:
                continue
            pid = parts[0]
            lon = float(parts[1])
            lat = float(parts[2])
            items.append({"id": pid, "lon": lon, "lat": lat})
    return items

def create_osm_metabhf():
    """Erstellt METABHF aus OSM-Daten mit Walking-Distanzen."""
    start = time.time()
    print("OSMnx-Version:", ox.__version__)
    print("Loading input data...")
    coach_points = load_points(DELTA_BFKOORD_WGS)
    oev_points = load_points(OEV_BFKOORD_WGS)
    print(f"Coach stops: {len(coach_points)}")
    print(f"Swiss PT stops: {len(oev_points)}")

    best_pairs = []   # list of (startID, endID, minutes_str)

    for coach in coach_points:
        print(f"Start ID {coach['id']}:")
        c_lat = coach["lat"]
        c_lon = coach["lon"]

        dists = []
        for oev in oev_points:
            d_line = haversine_distance(c_lat, c_lon, oev["lat"], oev["lon"])
            dists.append((d_line, oev))
        dists.sort(key=lambda x: x[0])

        if not dists:
            print("  No Swiss stops found, skipping.")
            continue

        nearest_line = dists[0][0]
        radius = int(min(
            MAX_GRAPH_RADIUS_M,
            max(MIN_GRAPH_RADIUS_M, nearest_line * GRAPH_RADIUS_FACTOR)
        ))

        top = dists[:CANDIDATE_COUNT]
        center = (c_lat, c_lon)
        G = build_walk_graph(center, dist_m=radius)

        coach_rows = []
        best_idx = None
        best_dist = None

        for idx, (line_dist, oev) in enumerate(top, start=1):
            try:
                dist_m = shortest_path_length_m(
                    G,
                    c_lon, c_lat,
                    oev["lon"], oev["lat"]
                )
                walk_min = estimate_walking_minutes(dist_m)
                row = {
                    "coach_id": coach["id"],
                    "nearest_oev_id": oev["id"],
                    "distance_m": dist_m,
                    "walk_min": walk_min,
                    "candidate_rank": idx,
                }
            except nx.NetworkXNoPath:
                row = {
                    "coach_id": coach["id"],
                    "nearest_oev_id": oev["id"],
                    "distance_m": None,
                    "walk_min": None,
                    "candidate_rank": idx,
                }
            coach_rows.append(row)

            if row["distance_m"] is not None:
                if best_dist is None or row["distance_m"] < best_dist:
                    best_dist = row["distance_m"]
                    best_idx = idx

        if best_idx is not None:
            best_rows = [r for r in coach_rows if r["candidate_rank"] == best_idx and r["distance_m"] is not None]
            if best_rows:
                br = best_rows[0]
                minutes_int = int(round(br["walk_min"] + 2))  # +2 Minuten
                minutes_str = f"{minutes_int:03d}"
                print("Done.")
                start_id = br["coach_id"]
                end_id = br["nearest_oev_id"]
                best_pairs.append((start_id, end_id, minutes_str))
        else:
            print("  No walkable candidate found, skipping.")

    if best_pairs:
        # Mapping endID -> list of startIDs, gebaut NACH best_pairs
        target_to_starts = {}
        for start_id, end_id, _ in best_pairs:
            target_to_starts.setdefault(end_id, []).append(start_id)

        os.makedirs(os.path.dirname(METABHF), exist_ok=True)
        with open(METABHF, "w", encoding="utf-8") as f:
            # erster Teil: startID endID minutes + "*A Y"
            for coach_id, oev_id, minutes_str in best_pairs:
                f.write(f"{coach_id} {oev_id} {minutes_str}\n")
                f.write("*A Y\n")
            # zweiter Teil: eine Zeile pro zielID mit allen startIDs
            for end_id in sorted(target_to_starts.keys()):
                starts_sorted = sorted(target_to_starts[end_id])
                starts_str = ", ".join(starts_sorted)
                f.write(f"{end_id} : {starts_str}\n")

    end = time.time()
    duration = end - start
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"Execution time: {minutes:02d}:{seconds:02d} (min:sec)")
    print("Done.")
    return True


if __name__ == "__main__":
    create_osm_metabhf()
