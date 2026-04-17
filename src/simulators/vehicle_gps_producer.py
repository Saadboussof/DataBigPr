import json
import time
import random
import os
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

DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
DEFAULT_MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
DEFAULT_MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password"))
DEFAULT_CSV_PATH = os.getenv("PORTO_CSV_PATH", "s3a://raw/porto/train.csv")

# Approximate Casablanca coastline polyline inside the project bounding box.
# Coordinates are [lon, lat].
COASTLINE_POLYLINE = [
    [-7.47, 33.70],
    [-7.52, 33.69],
    [-7.56, 33.67],
    [-7.60, 33.65],
    [-7.64, 33.62],
    [-7.68, 33.59],
    [-7.72, 33.57],
    [-7.76, 33.55],
    [-7.79, 33.53],
    [-7.74, 33.40],
]

# Land polygon: coastline plus east/south closure of the Casablanca bbox.
LAND_POLYGON = COASTLINE_POLYLINE + [
    [CASA_LON_MAX, CASA_LAT_MIN],
    [CASA_LON_MAX, CASA_LAT_MAX],
]

COASTLINE_EPSILON = 0.001
INLAND_NUDGE = 2e-5
MIN_GEOM_DENOM = 1e-12

LAND_CENTROID_LON = sum(p[0] for p in LAND_POLYGON) / len(LAND_POLYGON)
LAND_CENTROID_LAT = sum(p[1] for p in LAND_POLYGON) / len(LAND_POLYGON)

def point_in_polygon(lon, lat, polygon):
    inside = False
    j = len(polygon) - 1

    for i in range(len(polygon)):
        xi, yi = polygon[i]
        xj, yj = polygon[j]

        if (yi > lat) != (yj > lat):
            x_intersect = (xj - xi) * (lat - yi) / ((yj - yi) + MIN_GEOM_DENOM) + xi
            if lon < x_intersect:
                inside = not inside

        j = i

    return inside

def point_to_segment_projection(lon, lat, seg_start, seg_end):
    x1, y1 = seg_start
    x2, y2 = seg_end
    dx = x2 - x1
    dy = y2 - y1
    seg_len_sq = dx * dx + dy * dy

    if seg_len_sq < MIN_GEOM_DENOM:
        return [x1, y1], (lon - x1) ** 2 + (lat - y1) ** 2

    t = ((lon - x1) * dx + (lat - y1) * dy) / seg_len_sq
    t = max(0.0, min(1.0, t))
    px = x1 + t * dx
    py = y1 + t * dy
    return [px, py], (lon - px) ** 2 + (lat - py) ** 2

def distance_to_coastline_sq(lon, lat):
    best = float("inf")
    for i in range(len(COASTLINE_POLYLINE) - 1):
        _, d = point_to_segment_projection(lon, lat, COASTLINE_POLYLINE[i], COASTLINE_POLYLINE[i + 1])
        if d < best:
            best = d
    return best

def is_land_point(lon, lat, epsilon=COASTLINE_EPSILON):
    if point_in_polygon(lon, lat, LAND_POLYGON):
        return True

    return distance_to_coastline_sq(lon, lat) <= (epsilon * epsilon)

def clamp_to_bbox(lon, lat):
    lon = min(max(lon, CASA_LON_MIN), CASA_LON_MAX)
    lat = min(max(lat, CASA_LAT_MIN), CASA_LAT_MAX)
    return lon, lat

def project_to_land_boundary(lon, lat):
    """Project an invalid point to the closest point on the land polygon boundary."""
    best_point = None
    best_dist = float("inf")

    for i in range(len(LAND_POLYGON)):
        a = LAND_POLYGON[i]
        b = LAND_POLYGON[(i + 1) % len(LAND_POLYGON)]
        projected, d = point_to_segment_projection(lon, lat, a, b)
        if d < best_dist:
            best_dist = d
            best_point = projected

    if best_point is None:
        return clamp_to_bbox(lon, lat)

    return clamp_to_bbox(best_point[0], best_point[1])

def nudge_inside_land(lon, lat):
    if point_in_polygon(lon, lat, LAND_POLYGON):
        return lon, lat

    dx = LAND_CENTROID_LON - lon
    dy = LAND_CENTROID_LAT - lat
    norm = (dx * dx + dy * dy) ** 0.5
    if norm < MIN_GEOM_DENOM:
        return lon, lat

    ux = dx / norm
    uy = dy / norm

    step = INLAND_NUDGE
    for _ in range(10):
        candidate_lon = lon + ux * step
        candidate_lat = lat + uy * step
        candidate_lon, candidate_lat = clamp_to_bbox(candidate_lon, candidate_lat)
        if point_in_polygon(candidate_lon, candidate_lat, LAND_POLYGON):
            return candidate_lon, candidate_lat
        step *= 2.0

    return lon, lat

def coerce_to_land_point(lon, lat):
    lon, lat = clamp_to_bbox(lon, lat)

    if point_in_polygon(lon, lat, LAND_POLYGON):
        return lon, lat

    # Epsilon-adjacent coastline points are projected to boundary and nudged inland
    # so emitted Kafka coordinates always remain inside the land polygon.
    lon, lat = project_to_land_boundary(lon, lat)
    lon, lat = nudge_inside_land(lon, lat)
    return lon, lat

def transform_coordinate(lon, lat):
    """Map Porto point to Casablanca and clamp post-noise anomalies to land."""
    # Normalize Porto between 0 and 1
    lon_norm = (lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
    lat_norm = (lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
    
    # Scale to Casablanca
    casa_lon = CASA_LON_MIN + (lon_norm * (CASA_LON_MAX - CASA_LON_MIN))
    casa_lat = CASA_LAT_MIN + (lat_norm * (CASA_LAT_MAX - CASA_LAT_MIN))
    
    # Add noise (sigma ~ 0.0002 degrees ~ 20m)
    casa_lon += random.gauss(0, 0.0002)
    casa_lat += random.gauss(0, 0.0002)

    if not is_land_point(casa_lon, casa_lat):
        casa_lon, casa_lat = project_to_land_boundary(casa_lon, casa_lat)

    casa_lon, casa_lat = coerce_to_land_point(casa_lon, casa_lat)
    
    return casa_lon, casa_lat

def create_kafka_producer(broker="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )

def parse_s3_path(path):
    normalized = path
    if normalized.startswith("s3a://"):
        normalized = "s3://" + normalized[len("s3a://"):]

    if not normalized.startswith("s3://"):
        return None, None

    payload = normalized[len("s3://"):]
    bucket, _, key = payload.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 path: {path}")

    return bucket, key

def load_porto_dataframe(csv_path):
    bucket, key = parse_s3_path(csv_path)

    if bucket is not None:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for s3a:// paths. Install it with: pip install boto3"
            ) from exc

        s3 = boto3.client(
            "s3",
            endpoint_url=DEFAULT_MINIO_ENDPOINT,
            aws_access_key_id=DEFAULT_MINIO_ACCESS_KEY,
            aws_secret_access_key=DEFAULT_MINIO_SECRET_KEY,
        )
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj["Body"], nrows=50000, usecols=['TAXI_ID', 'POLYLINE', 'MISSING_DATA'])

    return pd.read_csv(csv_path, nrows=50000, usecols=['TAXI_ID', 'POLYLINE', 'MISSING_DATA'])

def main(csv_path, broker, speed_multiplier):
    print(f"Loading data from {csv_path}...")
    try:
        # Load a small chunk for simulation from local path or MinIO s3a path.
        df = load_porto_dataframe(csv_path)
        df = df[df['MISSING_DATA'] == False]
    except Exception as e:
        print(f"Error loading source data: {e}")
        print("For MinIO input, verify MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY and the object path.")
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
    parser.add_argument(
        "--csv",
        type=str,
        default=DEFAULT_CSV_PATH,
        help=f"Path to raw Porto train.csv (local or s3a). Default: {DEFAULT_CSV_PATH}",
    )
    parser.add_argument("--broker", type=str, default="localhost:9092")
    parser.add_argument("--speed", type=float, default=10.0, help="Simulation speed multiplier")
    args = parser.parse_args()
    
    main(args.csv, args.broker, args.speed)
