import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

# 16 Arrondissements in Casablanca
NUM_ZONES = 16

# Casablanca bounding box (from OSMnx road network — matches all other files)
CASA_LON_MIN, CASA_LON_MAX = -7.6895, -7.4008
CASA_LAT_MIN, CASA_LAT_MAX = 33.5072, 33.6527
GRID_COLS, GRID_ROWS = 4, 4

def zone_to_random_point(zone_id):
    """
    Convert a zone_id (1-16) into a random lat/lon inside that zone's grid cell.
    
    The grid is 4×4:
      Zone 1  = bottom-left   (grid_x=0, grid_y=0)
      Zone 16 = top-right     (grid_x=3, grid_y=3)
    
    Formula (reverse of gps_job.py):
      grid_x = (zone_id - 1) % 4
      grid_y = (zone_id - 1) // 4
    """
    grid_x = (zone_id - 1) % GRID_COLS
    grid_y = (zone_id - 1) // GRID_COLS

    # Width/height of one cell
    cell_w = (CASA_LON_MAX - CASA_LON_MIN) / GRID_COLS   # 0.10 degrees
    cell_h = (CASA_LAT_MAX - CASA_LAT_MIN) / GRID_ROWS   # 0.075 degrees

    # Random point inside the cell
    lon = CASA_LON_MIN + (grid_x + random.random()) * cell_w
    lat = CASA_LAT_MIN + (grid_y + random.random()) * cell_h

    return round(lat, 6), round(lon, 6)

def get_demand_multiplier(current_hour):
    """Simulates peak demand hours (7-9 AM, 5-7 PM)."""
    if 7 <= current_hour <= 9:
        return random.uniform(3.0, 5.0)
    elif 17 <= current_hour <= 19:
        return random.uniform(3.0, 5.0)
    return random.uniform(0.5, 1.5)

def create_kafka_producer(broker="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )

def generate_request():
    origin = random.randint(1, NUM_ZONES)
    dest = random.randint(1, NUM_ZONES)
    while dest == origin:
        dest = random.randint(1, NUM_ZONES)

    # Generate exact GPS coordinates for the passenger's pickup and dropoff
    origin_lat, origin_lon = zone_to_random_point(origin)
    dest_lat, dest_lon = zone_to_random_point(dest)

    return {
        "trip_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "origin_zone": origin,
        "destination_zone": dest,
        "origin_lat": origin_lat,
        "origin_lon": origin_lon,
        "dest_lat": dest_lat,
        "dest_lon": dest_lon,
        "requested_at": int(time.time() * 1000),
        "call_type": random.choice(["A", "B", "C"])  # A: Central, B: Stand, C: Hail
    }

def main(broker):
    producer = create_kafka_producer(broker)
    topic = "raw.trips"
    
    print(f"Starting trip request simulation on topic '{topic}'...")

    while True:
        current_hour = datetime.now().hour
        multiplier = get_demand_multiplier(current_hour)
        
        # Base rate: 2 requests per second globally, affected by multiplier
        requests_per_sec = int(2 * multiplier) 
        
        for _ in range(requests_per_sec):
            event = generate_request()
            producer.send(topic, key=event["origin_zone"], value=event)
            
        producer.flush()
        print(f"Emitted {requests_per_sec} trip requests (Current hour: {current_hour}:00, Multiplier: {multiplier:.2f})")
        
        time.sleep(1) # Emit in 1s batches

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", type=str, default="localhost:9092")
    args = parser.parse_args()
    
    main(args.broker)
