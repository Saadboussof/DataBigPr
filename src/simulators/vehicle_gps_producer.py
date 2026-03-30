import json
import time
import random
import uuid
import numpy as np
import pandas as pd
from kafka import KafkaProducer
import argparse

# --- Constants & Bounding Boxes ---
# Porto rough bounding box
PORTO_LON_MIN, PORTO_LON_MAX = -8.69, -8.56
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.14, 41.20

# Casablanca bounding box (from PDF: Lat 33.4-33.7 N, Lon 7.4-7.8 W)
CASA_LON_MIN, CASA_LON_MAX = -7.80, -7.40
CASA_LAT_MIN, CASA_LAT_MAX = 33.40, 33.70

def transform_coordinate(lon, lat):
    """Linear map from Porto bounded box to Casablanca bounded box."""
    # Normalize Porto between 0 and 1
    lon_norm = (lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
    lat_norm = (lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
    
    # Scale to Casablanca
    casa_lon = CASA_LON_MIN + (lon_norm * (CASA_LON_MAX - CASA_LON_MIN))
    casa_lat = CASA_LAT_MIN + (lat_norm * (CASA_LAT_MAX - CASA_LAT_MIN))
    
    # Add noise (sigma ~ 0.0002 degrees ~ 20m)
    casa_lon += random.gauss(0, 0.0002)
    casa_lat += random.gauss(0, 0.0002)
    
    return casa_lon, casa_lat

def create_kafka_producer(broker="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )

def main(csv_path, broker, speed_multiplier):
    print(f"Loading data from {csv_path}...")
    try:
        # Load small chunk for simulation, ignoring malformed lines
        df = pd.read_csv(csv_path, nrows=50000, usecols=['TAXI_ID', 'POLYLINE', 'MISSING_DATA'])
        df = df[df['MISSING_DATA'] == False]
    except Exception as e:
        print(f"Error loading CSV: {e}. Make sure you downloaded the Porto dataset to the correct path.")
        return

    producer = create_kafka_producer(broker)
    topic = "raw.gps"
    print(f"Starting GPS simulation on topic '{topic}' at {speed_multiplier}x speed...")

    # For simulation, we randomly select a subset of active taxis
    active_trips = df.sample(min(1000, len(df))).to_dict('records')
    
    # Each row has a JSON array of [lon, lat] points recorded every 15 seconds
    # We will simulate them moving concurrently
    for t in active_trips:
        try:
            t['points'] = json.loads(t['POLYLINE'])
        except json.JSONDecodeError:
            t['points'] = []
        t['current_idx'] = 0

    while True:
        # Pings happen roughly every 15 seconds in real time, adjusted by speed
        real_time_sleep = 15.0 / speed_multiplier
        timestamp = int(time.time() * 1000)

        active_count = 0
        for trip in active_trips:
            points = trip['points']
            idx = trip['current_idx']

            if idx >= len(points):
                continue # Trip finished
                
            active_count += 1
            
            # PDF constraint: 5% chance of 60s+ blackout
            if random.random() < 0.05:
                # We skip this coordinate but advance the index as if the car moved
                trip['current_idx'] += 1
                continue

            raw_lon, raw_lat = points[idx]
            casa_lon, casa_lat = transform_coordinate(raw_lon, raw_lat)

            # Generate Kafka payload
            taxi_id = str(trip['TAXI_ID'])
            event = {
                "taxi_id": taxi_id,
                "timestamp": timestamp,
                "lat": casa_lat,
                "lon": casa_lon,
                "speed": random.uniform(20.0, 60.0), # Simplification
                "status": "engaged" if idx < len(points) - 1 else "available"
            }

            producer.send(topic, key=taxi_id, value=event)
            trip['current_idx'] += 1

        producer.flush()
        if active_count == 0:
            print("All active trips finished. Restarting simulation cycle.")
            # Restart
            for t in active_trips: t['current_idx'] = 0

        time.sleep(real_time_sleep)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, required=True, help="Path to raw Porto train.csv")
    parser.add_argument("--broker", type=str, default="localhost:9092")
    parser.add_argument("--speed", type=float, default=10.0, help="Simulation speed multiplier")
    args = parser.parse_args()
    
    main(args.csv, args.broker, args.speed)
