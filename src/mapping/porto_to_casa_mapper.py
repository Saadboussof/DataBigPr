"""
Porto → Casablanca Road-Aware Geospatial Mapper (v2 — Clean Rewrite)
=====================================================================
Uses OSMnx's built-in routing and geometry extraction.

Pipeline:
  1. Load Porto + Casablanca road networks
  2. Compute affine transformation (relative position mapping)
  3. For each trip: transform START/END → snap to road nodes → shortest path
  4. Extract full road geometry using OSMnx route_to_gdf
  5. Save as Parquet

Usage:
  python src/mapping/porto_to_casa_mapper.py --input data/train.csv --limit 5000
"""

import os
import json
import time
import argparse
import numpy as np
import pandas as pd
import networkx as nx

try:
    import osmnx as ox
    import geopandas as gpd
except ImportError as e:
    print(f"Missing: {e}\nInstall: pip install osmnx geopandas scikit-learn")
    exit(1)

# ═══════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "road_cache")
PORTO_GRAPH = os.path.join(CACHE_DIR, "porto_roads.graphml")
CASA_GRAPH = os.path.join(CACHE_DIR, "casa_roads.graphml")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "mapped")


def load_graph(place, path):
    """Load cached graph or download from OSM."""
    if os.path.exists(path):
        print(f"  Cached: {os.path.basename(path)}")
        return ox.load_graphml(path)
    print(f"  Downloading {place}...")
    G = ox.graph_from_place(place, network_type="drive")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    ox.save_graphml(G, path)
    return G


def compute_transform(G_porto, G_casa):
    """Compute the mapping from Porto's coordinate space to Casablanca's."""
    pn = ox.graph_to_gdfs(G_porto, edges=False)
    cn = ox.graph_to_gdfs(G_casa, edges=False)

    porto = {
        "min_lon": pn["x"].min(), "max_lon": pn["x"].max(),
        "min_lat": pn["y"].min(), "max_lat": pn["y"].max(),
    }
    casa = {
        "min_lon": cn["x"].min(), "max_lon": cn["x"].max(),
        "min_lat": cn["y"].min(), "max_lat": cn["y"].max(),
    }

    print(f"  Porto: lon[{porto['min_lon']:.4f}, {porto['max_lon']:.4f}] "
          f"lat[{porto['min_lat']:.4f}, {porto['max_lat']:.4f}]")
    print(f"  Casa:  lon[{casa['min_lon']:.4f}, {casa['max_lon']:.4f}] "
          f"lat[{casa['min_lat']:.4f}, {casa['max_lat']:.4f}]")

    return porto, casa


def porto_to_casa(lon, lat, porto, casa):
    """
    Map a Porto coordinate to Casablanca using RELATIVE POSITION.
    A point at 30% across Porto maps to 30% across Casablanca.
    """
    # Relative position within Porto (0.0 = min, 1.0 = max)
    rel_lon = (lon - porto["min_lon"]) / (porto["max_lon"] - porto["min_lon"])
    rel_lat = (lat - porto["min_lat"]) / (porto["max_lat"] - porto["min_lat"])

    # Clamp to [0, 1] — handles points slightly outside Porto
    rel_lon = max(0.0, min(1.0, rel_lon))
    rel_lat = max(0.0, min(1.0, rel_lat))

    # Map to Casablanca
    casa_lon = casa["min_lon"] + rel_lon * (casa["max_lon"] - casa["min_lon"])
    casa_lat = casa["min_lat"] + rel_lat * (casa["max_lat"] - casa["min_lat"])

    return casa_lon, casa_lat


def extract_route_geometry(G, route):
    """
    Extract full road geometry from a route (list of node IDs).
    Handles both directions and edge geometries with curves.
    Returns list of (lon, lat) tuples.
    """
    coords = []

    for i in range(len(route) - 1):
        u, v = route[i], route[i + 1]

        # Try u→v, then v→u
        edge_data = G.get_edge_data(u, v)
        reverse = False
        if edge_data is None:
            edge_data = G.get_edge_data(v, u)
            reverse = True
        if edge_data is None:
            # Fallback: node coordinates
            if i == 0:
                coords.append((G.nodes[u]["x"], G.nodes[u]["y"]))
            coords.append((G.nodes[v]["x"], G.nodes[v]["y"]))
            continue

        key = list(edge_data.keys())[0]
        edge = edge_data[key]

        if "geometry" in edge:
            geom = list(edge["geometry"].coords)
            if reverse:
                geom = list(reversed(geom))
            else:
                ux = G.nodes[u]["x"]
                uy = G.nodes[u]["y"]
                if abs(geom[0][0] - ux) + abs(geom[0][1] - uy) > 0.0001:
                    geom = list(reversed(geom))
            start = 0 if i == 0 else 1
            coords.extend(geom[start:])
        else:
            if i == 0:
                coords.append((G.nodes[u]["x"], G.nodes[u]["y"]))
            coords.append((G.nodes[v]["x"], G.nodes[v]["y"]))

    return coords


def process_trip(points, porto, casa, G_casa, G_undirected):
    """
    Map one trip: START→END via Casablanca's road network.

    Returns (route_coords, origin_casa, dest_casa) or None on failure.
    route_coords is a list of (lon, lat) tuples following actual roads.
    """
    start_lon, start_lat = points[0]
    end_lon, end_lat = points[-1]

    # Skip round trips (start ≈ end)
    if abs(start_lon - end_lon) < 0.001 and abs(start_lat - end_lat) < 0.001:
        return None

    # Transform to Casablanca
    casa_start = porto_to_casa(start_lon, start_lat, porto, casa)
    casa_end = porto_to_casa(end_lon, end_lat, porto, casa)

    # Snap to nearest road nodes
    origin = ox.distance.nearest_nodes(G_casa, X=casa_start[0], Y=casa_start[1])
    dest = ox.distance.nearest_nodes(G_casa, X=casa_end[0], Y=casa_end[1])

    if origin == dest:
        return None

    # Shortest path on UNDIRECTED graph (no one-way blocking)
    try:
        route = nx.shortest_path(G_undirected, origin, dest, weight="length")
    except nx.NetworkXNoPath:
        return None

    if len(route) < 2:
        return None

    # Extract full road geometry
    coords = extract_route_geometry(G_casa, route)

    if len(coords) < 2:
        return None

    return coords


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

def main(input_csv, output_dir, limit):
    t0 = time.time()
    print("=" * 60)
    print("  Porto → Casablanca Mapper v2")
    print("=" * 60)

    # Phase 1: Load graphs
    print("\n[1/4] Loading road networks...")
    G_porto = load_graph("Porto, Portugal", PORTO_GRAPH)
    G_casa = load_graph("Casablanca, Morocco", CASA_GRAPH)
    print(f"  Porto: {G_porto.number_of_nodes()} nodes")
    print(f"  Casa:  {G_casa.number_of_nodes()} nodes")

    # Pre-compute undirected graph ONCE (expensive)
    print("  Building undirected routing graph...")
    G_undirected = G_casa.to_undirected()

    # Phase 2: Compute transform
    print("\n[2/4] Computing coordinate mapping...")
    porto, casa = compute_transform(G_porto, G_casa)

    # Phase 3: Process trips
    print(f"\n[3/4] Loading {limit} trips from {input_csv}...")
    df = pd.read_csv(input_csv, nrows=limit, usecols=["TAXI_ID", "POLYLINE", "MISSING_DATA"])
    df = df[df["MISSING_DATA"] == False].reset_index(drop=True)
    print(f"  {len(df)} valid trips loaded")

    print("\n  Mapping trips...")
    results = []
    ok = 0
    skip_short = 0
    skip_round = 0
    skip_err = 0

    for i, row in df.iterrows():
        try:
            pts = json.loads(row["POLYLINE"])
            if not pts or len(pts) < 2:
                skip_short += 1
                results.append("[]")
                continue

            route = process_trip(pts, porto, casa, G_casa, G_undirected)

            if route is None:
                skip_round += 1
                results.append("[]")
            else:
                polyline = [[round(c[0], 6), round(c[1], 6)] for c in route]
                results.append(json.dumps(polyline))
                ok += 1

        except Exception as e:
            skip_err += 1
            if skip_err <= 3:
                print(f"    [!] Trip {i}: {e}")
            results.append("[]")

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"    {i+1}/{len(df)} — {ok} ok, {skip_short} short, "
                  f"{skip_round} skip, {skip_err} err ({(i+1)/elapsed:.0f}/sec)")

    df["CASA_POLYLINE"] = results
    df = df[df["CASA_POLYLINE"] != "[]"].reset_index(drop=True)
    print(f"\n  ✅ {ok} trips mapped successfully")

    # Phase 4: Save
    print(f"\n[4/4] Saving...")
    os.makedirs(output_dir, exist_ok=True)
    out = os.path.join(output_dir, "casa_trips_road_snapped.parquet")
    df[["TAXI_ID", "CASA_POLYLINE"]].to_parquet(out, index=False)
    print(f"  → {out}")

    # Phase 5: Upload to MinIO (Curated Bucket)
    print(f"\n[5/5] Uploading to MinIO (curated bucket)...")
    try:
        from minio import Minio
        # Connect to MinIO (assuming script runs on Windows host)
        minio_client = Minio(
            "localhost:9000",
            access_key="admin",
            secret_key="password",
            secure=False
        )
        
        if not minio_client.bucket_exists("curated"):
            minio_client.make_bucket("curated")
            
        object_name = "porto/casa_trips_road_snapped.parquet"
        minio_client.fput_object("curated", object_name, out)
        print(f"  ✅ Uploaded to s3://curated/{object_name}")
        
    except ImportError:
        print("  [!] Skipping MinIO upload. 'minio' python package not installed.")
        print("  [!] To enable auto-upload, run: pip install minio")
    except Exception as e:
        print(f"  [!] Failed to upload to MinIO: {e}")

    print(f"\n{'='*60}")
    print(f"  DONE in {time.time()-t0:.1f}s — {len(df)} road-snapped trips")
    print(f"{'='*60}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/train.csv")
    p.add_argument("--output", default=OUTPUT_DIR)
    p.add_argument("--limit", type=int, default=5000)
    args = p.parse_args()
    main(args.input, args.output, args.limit)
