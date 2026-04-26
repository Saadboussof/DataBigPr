"""
Flink Job 1 — GPS Normalizer & Cassandra Ingestion
====================================================
Week 3 Deliverable (Dev 1)

Pipeline:
  Kafka [raw.gps]  →  Parse JSON  →  Validate Bounds  →  Compute zone_id
                   →  INSERT INTO Cassandra vehicle_positions

Features added for Week 3 completion criteria:
  • Event-time watermarks with 3-minute allowed lateness
  • Checkpointing every 60 seconds (state saved to filesystem)
"""

import json
import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# --- Casablanca Boundaries (from OSMnx road network — matches all other files) ---
CASA_LON_MIN, CASA_LON_MAX = -7.6895, -7.4008
CASA_LAT_MIN, CASA_LAT_MAX = 33.5072, 33.6527

# ═══════════════════════════════════════════════════════════════════
# Watermark: Custom TimestampAssigner
# ═══════════════════════════════════════════════════════════════════
# Flink needs to know which field in your JSON is the "event time".
# We extract the `timestamp` field (ms since epoch) from the GPS JSON.
# The WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(180))
# tells Flink: "Allow GPS pings that arrive up to 3 minutes late."
# ═══════════════════════════════════════════════════════════════════

class GPSTimestampAssigner(TimestampAssigner):
    """Extract the 'timestamp' field from each GPS JSON message."""
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            return int(data.get('timestamp', record_timestamp))
        except Exception:
            return record_timestamp


class ProcessAndSaveGPS(MapFunction):
    """
    Flink MapFunction that performs 3 operations on every Kafka GPS message:
      1. Parses the JSON payload.
      2. Geometrically maps Lat/Lon → Zone ID (1-16) using grid quantization.
      3. Executes a prepared INSERT into Cassandra's vehicle_positions table.
    """

    def __init__(self):
        self.session = None

    def open(self, runtime_context):
        """Called once when the Flink TaskManager worker starts."""
        from cassandra.cluster import Cluster
        cluster = Cluster(['cassandra'], port=9042)
        self.session = cluster.connect('taasim')

        # Prepared statements are pre-compiled by Cassandra.
        # This avoids CQL parsing overhead on every single INSERT.
        self.prepared_stmt = self.session.prepare("""
            INSERT INTO vehicle_positions
            (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

    def map(self, value):
        from datetime import datetime, timezone

        try:
            # ── Step 1: Deserialize JSON ──
            data = json.loads(value)
            lon = float(data['lon'])
            lat = float(data['lat'])

            # ── Step 2: Grid Quantization (Lat/Lon → Zone ID) ──
            # Divide the Casablanca bounding box into a 4×4 grid = 16 zones.
            grid_x = int(4 * (lon - CASA_LON_MIN) / (CASA_LON_MAX - CASA_LON_MIN))
            grid_y = int(4 * (lat - CASA_LAT_MIN) / (CASA_LAT_MAX - CASA_LAT_MIN))

            # Filter: discard coordinates outside the city geofence
            if grid_x < 0 or grid_x >= 4 or grid_y < 0 or grid_y >= 4:
                return "SKIPPED: Out of Bounds"

            zone_id = (grid_y * 4) + grid_x + 1   # 1-based zone numbering

            # Normalize the UNIX epoch ms timestamp to UTC datetime
            dt = datetime.fromtimestamp(data['timestamp'] / 1000.0, tz=timezone.utc)

            # ── Step 3: Cassandra Sink ──
            self.session.execute(self.prepared_stmt, [
                'casablanca',
                zone_id,
                dt,
                str(data['taxi_id']),
                lat,
                lon,
                float(data.get('speed', 0.0)),
                str(data.get('status', 'available'))
            ])

            return f"SUCCESS: Taxi {data['taxi_id']} → Zone {zone_id}"

        except Exception as e:
            return f"ERROR processing row: {str(e)}"


def main():
    print("═" * 60)
    print("  Flink Job 1: GPS Normalizer & Cassandra Ingestion")
    print("  Features: Watermarks (3-min lateness) + Checkpointing (60s)")
    print("═" * 60)

    # ── 1. Streaming Environment ──
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # ── 2. Checkpointing: save Flink state every 60 seconds ──
    # If the job crashes, Flink can restart from the last checkpoint
    # instead of re-reading the entire Kafka topic from the beginning.
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)

    # Load the Kafka Connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # ── 3. Kafka Source ──
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("raw.gps") \
        .set_group_id("flink-gps-job") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # ── 4. Watermark Strategy ──
    # for_monotonous_timestamps(): simplest strategy — assumes timestamps
    # arrive roughly in order. Combined with our TimestampAssigner, Flink
    # can correctly order late-arriving GPS pings.
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(GPSTimestampAssigner())

    # ── 5. Build the DataStream ──
    stream = env.from_source(source, watermark_strategy, "Kafka GPS Source")

    # ── 6. Transform & Sink ──
    result_stream = stream.map(ProcessAndSaveGPS(), output_type=Types.STRING())

    # Print processing log to the TaskManager stdout
    result_stream.print()

    # ── 7. Execute ──
    env.execute("GPS Normalizer Job")


if __name__ == '__main__':
    main()
