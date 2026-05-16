"""
Flink Job 3 — Trip Matcher
============================
Week 4 Deliverable (Dev 3 — Soufiane)

Pipeline:
  Kafka [raw.trips] → Parse JSON → Query vehicle_positions
                    → Find nearest available taxi (Haversine)
                    → INSERT INTO Cassandra trips
                    → Emit match event to Kafka [processed.matches]
"""

import json
import math
import time
import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
)
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# ═══════════════════════════════════════════════════════════════════
# Constants — must match gps_job.py exactly so zone IDs are consistent
# ═══════════════════════════════════════════════════════════════════
CASA_LON_MIN, CASA_LON_MAX = -7.6895, -7.4008
CASA_LAT_MIN, CASA_LAT_MAX = 33.5072, 33.6527
GRID_COLS, GRID_ROWS = 4, 4
NUM_ZONES = GRID_COLS * GRID_ROWS  # 16


# ═══════════════════════════════════════════════════════════════════
# Utility: Haversine distance (meters)
# ═══════════════════════════════════════════════════════════════════
# Calculates the great-circle distance between two GPS points.
# Used to rank which available taxi is physically closest to the
# passenger's pickup location.
# ═══════════════════════════════════════════════════════════════════
def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ═══════════════════════════════════════════════════════════════════
# Utility: Grid-aware adjacent zone lookup
# ═══════════════════════════════════════════════════════════════════
# The city is a 4×4 grid of 16 zones. We need to find taxis not just
# in the passenger's zone but also in neighboring zones (8-directional).
#
# Why grid-aware? Zone 4 (col=3, row=0) and Zone 5 (col=0, row=1)
# are numerically adjacent but on OPPOSITE sides of the city.
# Grid coordinates prevent false neighbor matches.
#
#   Zone layout:
#     13 14 15 16
#      9 10 11 12
#      5  6  7  8
#      1  2  3  4
# ═══════════════════════════════════════════════════════════════════
def get_adjacent_zones(zone_id):
    gx = (zone_id - 1) % GRID_COLS
    gy = (zone_id - 1) // GRID_COLS

    zones = []
    for dx in [-1, 0, 1]:
        for dy in [-1, 0, 1]:
            nx, ny = gx + dx, gy + dy
            if 0 <= nx < GRID_COLS and 0 <= ny < GRID_ROWS:
                zones.append((ny * GRID_COLS) + nx + 1)
    return zones


# ═══════════════════════════════════════════════════════════════════
# Watermark: TimestampAssigner
# ═══════════════════════════════════════════════════════════════════
# Same pattern as gps_job.py's GPSTimestampAssigner.
# Flink uses this to track event-time progress. We extract the
# 'requested_at' field (epoch ms) from each trip request JSON.
# ═══════════════════════════════════════════════════════════════════
class TripTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            return int(data.get('requested_at', record_timestamp))
        except Exception:
            return record_timestamp


# ═══════════════════════════════════════════════════════════════════
# Core: TripMatcherFunction (MapFunction)
# ═══════════════════════════════════════════════════════════════════
# This is the heart of Job 3. Same pattern as gps_job.py's
# ProcessAndSaveGPS class:
#   - open()  → connect to Cassandra once
#   - map()   → called for every single Kafka message
#
# For each trip request:
#   1. Parse JSON from raw.trips
#   2. Query Cassandra vehicle_positions for available taxis
#   3. Pick the closest one using Haversine distance
#   4. Compute ETA = distance / 30 km/h
#   5. INSERT the matched trip into Cassandra trips table
#   6. Return a JSON match event (goes to processed.matches via KafkaSink)
# ═══════════════════════════════════════════════════════════════════
class TripMatcherFunction(MapFunction):

    def __init__(self):
        self.session = None
        self.find_vehicles_stmt = None
        self.insert_trip_stmt = None

    def open(self, runtime_context):
        """Called once when the Flink TaskManager worker starts."""
        from cassandra.cluster import Cluster

        cluster = Cluster(['cassandra'], port=9042)
        self.session = cluster.connect('taasim')

        # Prepared statement: find taxis in a given zone
        # Partition key is (city, zone_id), clustered by event_time DESC
        # so LIMIT 50 gives us the 50 most recent positions in that zone
        self.find_vehicles_stmt = self.session.prepare("""
            SELECT taxi_id, lat, lon, speed, status, event_time
            FROM vehicle_positions
            WHERE city = 'casablanca' AND zone_id = ?
            LIMIT 50
        """)

        # Prepared statement: insert a matched trip
        # Partition key is (city, date_bucket), clustered by created_at DESC
        self.insert_trip_stmt = self.session.prepare("""
            INSERT INTO trips
            (city, date_bucket, created_at, trip_id, rider_id, taxi_id,
             origin_zone, dest_zone, status, eta_seconds)
            VALUES ('casablanca', ?, ?, ?, ?, ?, ?, ?, 'MATCHED', ?)
        """)

    def map(self, value):
        try:
            # ── Parse the trip request JSON ──
            trip = json.loads(value)

            trip_id     = trip['trip_id']
            rider_id    = trip['rider_id']
            origin_zone = int(trip['origin_zone'])
            dest_zone   = int(trip['destination_zone'])
            origin_lat  = float(trip.get('origin_lat', 0))
            origin_lon  = float(trip.get('origin_lon', 0))

            # ── Search for available taxis ──
            # get_adjacent_zones returns the origin zone + all grid neighbors
            # Example: zone 6 → [1, 2, 3, 5, 6, 7, 9, 10, 11]
            search_zones = get_adjacent_zones(origin_zone)

            best_taxi = None
            best_dist = float('inf')
            matched_zone = origin_zone

            for zone in search_zones:
                rows = self.session.execute(self.find_vehicles_stmt, [zone])
                for row in rows:
                    # Only consider taxis with status 'available'
                    if row.status != 'available':
                        continue
                    # Calculate how far this taxi is from the passenger
                    dist = haversine(origin_lat, origin_lon, row.lat, row.lon)
                    if dist < best_dist:
                        best_dist = dist
                        best_taxi = row
                        matched_zone = zone

            # ── No taxi found in any zone ──
            if best_taxi is None:
                no_match = {
                    "trip_id": trip_id,
                    "rider_id": rider_id,
                    "origin_zone": origin_zone,
                    "status": "NO_TAXI_AVAILABLE"
                }
                return json.dumps(no_match)

            # ── Compute ETA ──
            # Assumption: average city driving speed = 30 km/h = 8.33 m/s
            avg_speed_ms = 30_000 / 3600
            eta_seconds = max(int(best_dist / avg_speed_ms), 30)  # minimum 30s

            # ── Write to Cassandra trips table ──
            now = datetime.now(timezone.utc)
            date_bucket = now.strftime('%Y-%m-%d')

            self.session.execute(self.insert_trip_stmt, [
                date_bucket,
                now,
                trip_id,
                rider_id,
                best_taxi.taxi_id,
                origin_zone,
                dest_zone,
                eta_seconds
            ])

            # ── Build match event (returned → KafkaSink → processed.matches) ──
            match_event = {
                "trip_id": trip_id,
                "taxi_id": best_taxi.taxi_id,
                "rider_id": rider_id,
                "origin_zone": origin_zone,
                "destination_zone": dest_zone,
                "matched_zone": matched_zone,
                "distance_m": round(best_dist, 1),
                "eta_seconds": eta_seconds,
                "status": "MATCHED",
                "matched_at": int(now.timestamp() * 1000)
            }

            return json.dumps(match_event)

        except Exception as e:
            error = {"error": str(e), "raw": value[:200]}
            return json.dumps(error)


# ═══════════════════════════════════════════════════════════════════
# Main: Wire everything together
# ═══════════════════════════════════════════════════════════════════
# Same structure as gps_job.py:
#   1. StreamExecutionEnvironment (parallelism=2, checkpointing=60s)
#   2. KafkaSource  → reads from raw.trips
#   3. WatermarkStrategy + TripTimestampAssigner
#   4. MapFunction   → TripMatcherFunction (the core logic)
#   5. KafkaSink     → writes to processed.matches
#   6. env.execute()
# ═══════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  Flink Job 3: Trip Matcher")
    print("  Reads: raw.trips  |  Writes: trips + processed.matches")
    print("=" * 60)

    # ── 1. Streaming Environment ──
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Use 1 slot (Job 1 already uses 2 of 2 available)

    # Checkpointing: save Flink state every 60 seconds
    # If the job crashes, it restarts from the last checkpoint
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)

    # Load the Kafka Connector JAR (same JAR as gps_job.py uses)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # ── 2. Kafka Source (raw.trips) ──
    # group_id "flink-trip-matcher" is different from Job 1's "flink-gps-job"
    # so each job tracks its own consumer offsets independently
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("raw.trips") \
        .set_group_id("flink-trip-matcher") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # ── 3. Watermark Strategy ──
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(TripTimestampAssigner())

    # ── 4. Build the DataStream ──
    stream = env.from_source(source, watermark_strategy, "Kafka Trip Source")

    # ── 5. Transform: run the matching logic on each trip request ──
    matched_stream = stream.map(
        TripMatcherFunction(),
        output_type=Types.STRING()
    )

    # ── 6. Kafka Sink (processed.matches) ──
    # Every JSON string returned by TripMatcherFunction.map() is sent
    # to the processed.matches topic for downstream consumers (e.g. FastAPI)
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("processed.matches")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    matched_stream.sink_to(sink)

    # Also print to TaskManager stdout for live debugging
    matched_stream.print()

    # ── 7. Execute ──
    env.execute("Trip Matcher Job")


if __name__ == '__main__':
    main()
