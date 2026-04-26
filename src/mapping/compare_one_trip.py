"""
Compare ONE trip on Casablanca map:
  RED dashed + dots = BEFORE (raw transformed points, off-road)
  GREEN solid = AFTER (shortest path on actual roads)

Usage: python src/mapping/compare_one_trip.py
"""

import json, sys
import pandas as pd
import numpy as np
import folium
import osmnx as ox
import networkx as nx

# Import from our mapper
sys.path.insert(0, "src/mapping")
from porto_to_casa_mapper import (
    load_graph, compute_transform, porto_to_casa, extract_route_geometry,
    PORTO_GRAPH, CASA_GRAPH
)

print("Loading graphs...")
G_porto = load_graph("Porto, Portugal", PORTO_GRAPH)
G_casa = load_graph("Casablanca, Morocco", CASA_GRAPH)
G_undirected = G_casa.to_undirected()

porto, casa = compute_transform(G_porto, G_casa)

# Find a good trip
print("Finding trip...")
df = pd.read_csv("data/train.csv", nrows=500, usecols=["TAXI_ID", "POLYLINE", "MISSING_DATA"])
df = df[df["MISSING_DATA"] == False].reset_index(drop=True)

chosen = None
for i, row in df.iterrows():
    pts = json.loads(row["POLYLINE"])
    if 15 <= len(pts) <= 50:
        if abs(pts[0][0] - pts[-1][0]) > 0.005 or abs(pts[0][1] - pts[-1][1]) > 0.005:
            chosen = (row["TAXI_ID"], pts)
            print(f"  Trip #{i}: Taxi {row['TAXI_ID']}, {len(pts)} GPS points")
            break

if not chosen:
    print("No suitable trip found!")
    exit(1)

taxi_id, porto_pts = chosen

# ═══════════════════════════════════════════
# BEFORE: Raw transform of ALL points
# ═══════════════════════════════════════════
before = []
for p in porto_pts:
    c = porto_to_casa(p[0], p[1], porto, casa)
    before.append(c)

before_latlon = [[b[1], b[0]] for b in before]

# ═══════════════════════════════════════════
# AFTER: Shortest path START → END on roads
# ═══════════════════════════════════════════
casa_start = porto_to_casa(porto_pts[0][0], porto_pts[0][1], porto, casa)
casa_end = porto_to_casa(porto_pts[-1][0], porto_pts[-1][1], porto, casa)

origin = ox.distance.nearest_nodes(G_casa, X=casa_start[0], Y=casa_start[1])
dest = ox.distance.nearest_nodes(G_casa, X=casa_end[0], Y=casa_end[1])

print(f"  Start: ({casa_start[1]:.5f}, {casa_start[0]:.5f}) → node {origin}")
print(f"  End:   ({casa_end[1]:.5f}, {casa_end[0]:.5f}) → node {dest}")

route = nx.shortest_path(G_undirected, origin, dest, weight="length")
print(f"  Route: {len(route)} road segments")

after = extract_route_geometry(G_casa, route)
after_latlon = [[c[1], c[0]] for c in after]

print(f"  BEFORE: {len(before)} raw points")
print(f"  AFTER:  {len(after)} road geometry points")

# ═══════════════════════════════════════════
# Build map
# ═══════════════════════════════════════════
center = [np.mean([b[0] for b in before_latlon]), np.mean([b[1] for b in before_latlon])]
m = folium.Map(location=center, zoom_start=13, tiles="OpenStreetMap")

# RED = BEFORE
folium.PolyLine(before_latlon, color="red", weight=3, opacity=0.7,
                dash_array="8", tooltip="BEFORE: Raw points (off-road)").add_to(m)
for i, c in enumerate(before_latlon):
    folium.CircleMarker(c, radius=4, color="red", fill=True, fill_opacity=0.8,
                        tooltip=f"Raw #{i}").add_to(m)

# GREEN = AFTER
folium.PolyLine(after_latlon, color="green", weight=5, opacity=0.9,
                tooltip="AFTER: Road-snapped route").add_to(m)

# Markers — START
folium.Marker(before_latlon[0], popup="START",
              icon=folium.Icon(color="green", icon="play")).add_to(m)
# END for red
folium.Marker(before_latlon[-1], popup="END (raw)",
              icon=folium.Icon(color="red", icon="stop")).add_to(m)
# END for green
folium.Marker(after_latlon[-1], popup="END (road route)",
              icon=folium.Icon(color="darkgreen", icon="flag")).add_to(m)

# Legend
legend = """
<div style="position:fixed; bottom:30px; left:30px; z-index:1000;
     background:white; padding:15px 20px; border-radius:10px;
     border:2px solid #333; font-size:14px; font-family:Arial;
     box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
    <b style="font-size:16px;">🚕 Trip Mapping Comparison</b><br><br>
    <span style="color:red; font-weight:bold;">━ ━ ━ ●</span> BEFORE: Raw transformed points<br>
    <small style="color:#666;">  (off-road, through buildings)</small><br><br>
    <span style="color:green; font-weight:bold;">━━━━━</span> AFTER: Road-snapped route<br>
    <small style="color:#666;">  (follows actual Casablanca streets)</small><br><br>
    <span style="color:green;">▶</span> START &nbsp;
    <span style="color:red;">■</span> END (raw) &nbsp;
    <span style="color:darkgreen;">⚑</span> END (road)
</div>
"""
m.get_root().html.add_child(folium.Element(legend))

output = "notebooks/trip_comparison.html"
m.save(output)
print(f"\n✅ {output}")
