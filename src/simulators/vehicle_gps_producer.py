"""
Vehicle GPS Producer — Reads road-snapped Casablanca trips and streams to Kafka.

v2: Reads from the pre-mapped Parquet file (output of porto_to_casa_mapper.py)
    instead of doing on-the-fly coordinate transformation.
    Every GPS point is guaranteed to be on an actual Casablanca road.

Usage:
  python src/simulators/vehicle_gps_producer.py --broker localhost:9092
"""

import json
import time
import random
import os
import argparse
import pandas as pd
from kafka import KafkaProducer

# Use the mapped Parquet by default, fallback to raw CSV
DEFAULT_PARQUET = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "mapped", "casa_trips_road_snapped.parquet"
)
DEFAULT_CSV = os.getenv("PORTO_CSV_PATH", "data/train.csv")

# ═══════════════════════════════════════════════════════════════
# Casablanca bounds (from OSMnx road network)
# ═══════════════════════════════════════════════════════════════
CASA_LON_MIN, CASA_LON_MAX = -7.6895, -7.4008
CASA_LAT_MIN, CASA_LAT_MAX = 33.5072, 33.6527


def create_kafka_producer(broker="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )


def load_trips(parquet_path, csv_fallback):
    """
    Load pre-mapped road-snapped trips from Parquet.
    Falls back to raw CSV with simple linear mapping if Parquet doesn't exist.
    """
    if os.path.exists(parquet_path):
        print(f"  Loading road-snapped trips from: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        trips = []
        for _, row in df.iterrows():
            points = json.loads(row["CASA_POLYLINE"])
            if len(points) >= 2:
                trips.append({
                    "taxi_id": str(row["TAXI_ID"]),
                    "points": points,  # Already [lon, lat] on real roads
                    "current_idx": 0,
                })
        print(f"  Loaded {len(trips)} road-snapped trips")
        return trips

    # Fallback: raw CSV with simple linear mapping
    print(f"  ⚠ Parquet not found, using raw CSV fallback: {csv_fallback}")
    print(f"  Run `python src/mapping/porto_to_casa_mapper.py` first for road-snapped data!")
    df = pd.read_csv(csv_fallback, nrows=50000, usecols=['TAXI_ID', 'POLYLINE', 'MISSING_DATA'])
    df = df[df['MISSING_DATA'] == False]

    PORTO_LON_MIN, PORTO_LON_MAX = -8.6922, -8.5594
    PORTO_LAT_MIN, PORTO_LAT_MAX = 41.1396, 41.1848

    trips = []
    for _, row in df.iterrows():
        try:
            raw_pts = json.loads(row['POLYLINE'])
            if len(raw_pts) < 2:
                continue
            # Simple linear mapping as fallback
            mapped = []
            for p in raw_pts:
                rel_lon = (p[0] - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
                rel_lat = (p[1] - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
                rel_lon = max(0, min(1, rel_lon))
                rel_lat = max(0, min(1, rel_lat))
                casa_lon = CASA_LON_MIN + rel_lon * (CASA_LON_MAX - CASA_LON_MIN)
                casa_lat = CASA_LAT_MIN + rel_lat * (CASA_LAT_MAX - CASA_LAT_MIN)
                mapped.append([round(casa_lon, 6), round(casa_lat, 6)])
            trips.append({
                "taxi_id": str(row['TAXI_ID']),
                "points": mapped,
                "current_idx": 0,
            })
        except Exception:
            continue

    print(f"  Loaded {len(trips)} trips (fallback mode)")
    return trips


def main(parquet_path, csv_fallback, broker, speed_multiplier):
    print("=" * 60)
    print("  Vehicle GPS Producer v2 (Road-Snapped)")
    print("=" * 60)

    trips = load_trips(parquet_path, csv_fallback)
    if not trips:
        print("No trips loaded!")
        return

    producer = create_kafka_producer(broker)
    topic = "raw.gps"
    print(f"\nStreaming to Kafka topic '{topic}' at {speed_multiplier}x speed...")

    # Select active subset
    active_trips = random.sample(trips, min(1000, len(trips)))

    while True:
        real_time_sleep = 15.0 / speed_multiplier
        timestamp = int(time.time() * 1000)

        active_count = 0
        for trip in active_trips:
            idx = trip['current_idx']
            points = trip['points']

            if idx >= len(points):
                continue

            active_count += 1

            # 5% chance of GPS blackout (simulates signal loss)
            if random.random() < 0.05:
                trip['current_idx'] += 1
                continue

            lon, lat = points[idx]

            event = {
                "taxi_id": trip['taxi_id'],
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "speed": random.uniform(20.0, 60.0),
                "status": "engaged" if idx < len(points) - 1 else "available"
            }

            producer.send(topic, key=trip['taxi_id'], value=event)
            trip['current_idx'] += 1

        producer.flush()

        if active_count == 0:
            print("All trips finished. Restarting cycle.")
            for t in active_trips:
                t['current_idx'] = 0

        time.sleep(real_time_sleep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", default=DEFAULT_PARQUET,
                        help="Path to road-snapped Parquet (from porto_to_casa_mapper.py)")
    parser.add_argument("--csv", default=DEFAULT_CSV,
                        help="Fallback raw CSV if Parquet not found")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--speed", type=float, default=10.0)
    args = parser.parse_args()

    main(args.parquet, args.csv, args.broker, args.speed)
