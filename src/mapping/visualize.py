"""
Visualize mapped trips on an interactive Casablanca map.
Uses Folium to render road-snapped taxi trajectories.

Usage:
  python src/mapping/visualize.py [--trips 100]
"""

import os
import json
import random
import argparse
import pandas as pd

try:
    import folium
    from folium.plugins import MarkerCluster
except ImportError:
    print("Missing library: pip install folium")
    exit(1)

# Casablanca center
CASA_CENTER = [33.5731, -7.5898]

# Colors for different trips
TRIP_COLORS = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
    "#dcbeff", "#9A6324", "#800000", "#aaffc3", "#808000",
    "#000075", "#a9a9a9",
]


def create_map(parquet_path, num_trips=100, output_html="notebooks/casablanca_taxi_map.html"):
    """Load road-snapped trips and render them on a Folium map."""

    print(f"Loading trips from {parquet_path}...")
    df = pd.read_parquet(parquet_path)

    # Sample trips
    sample_size = min(num_trips, len(df))
    sample = df.sample(sample_size, random_state=42).reset_index(drop=True)

    print(f"Rendering {sample_size} trips on map...")

    # Create base map
    m = folium.Map(
        location=CASA_CENTER,
        zoom_start=12,
        tiles="OpenStreetMap",
    )

    # Add GeoJSON Arrondissements Overlay
    geojson_path = os.path.join("data", "geo", "arrondissements.geojson")
    if os.path.exists(geojson_path):
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geo_data = json.load(f)

        folium.GeoJson(
            geo_data,
            name="Arrondissements",
            style_function=lambda feature: {
                "fillColor": "#3186cc",
                "color": "#3186cc",
                "weight": 2,
                "fillOpacity": 0.1,
            },
            tooltip=folium.GeoJsonTooltip(fields=["Arrondissement", "Prefecture"])
        ).add_to(m)
        print(f"Added GeoJSON boundaries from {geojson_path}")
    else:
        print(f"WARNING: GeoJSON not found at {geojson_path}")

    # Add each trip as a polyline
    for i, row in sample.iterrows():
        try:
            points = json.loads(row["CASA_POLYLINE"])
            if len(points) < 2:
                continue

            # Folium wants [lat, lon], our data is [lon, lat]
            coords = [[p[1], p[0]] for p in points]
            color = TRIP_COLORS[i % len(TRIP_COLORS)]

            folium.PolyLine(
                coords,
                weight=2.5,
                opacity=0.7,
                color=color,
                tooltip=f"Taxi {row['TAXI_ID']} ({len(points)} points)",
            ).add_to(m)

            # Add start marker (green) and end marker (red)
            folium.CircleMarker(
                coords[0], radius=4, color="green",
                fill=True, popup=f"Start: Taxi {row['TAXI_ID']}"
            ).add_to(m)
            folium.CircleMarker(
                coords[-1], radius=4, color="red",
                fill=True, popup=f"End: Taxi {row['TAXI_ID']}"
            ).add_to(m)

        except Exception:
            continue

    # Save
    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    m.save(output_html)
    print(f"Map saved to: {output_html}")
    print(f"Open it in your browser to see {sample_size} road-snapped trips!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--parquet",
        default="data/mapped/casa_trips_road_snapped.parquet",
        help="Path to road-snapped Parquet file",
    )
    parser.add_argument("--trips", type=int, default=100, help="Number of trips to display")
    parser.add_argument("--output", default="notebooks/casablanca_taxi_map.html")
    args = parser.parse_args()

    create_map(args.parquet, args.trips, args.output)
