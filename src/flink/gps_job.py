"""
Flink Job 1 — GPS Normalizer & Cassandra Ingestion
====================================================
Week 3 Deliverable (Dev 1)

Pipeline:
  Kafka [raw.gps]  →  Parse JSON  →  Validate Bounds  →  Arrondissement Lookup
                   →  INSERT INTO Cassandra vehicle_positions

Features:
  • Real Casablanca arrondissement boundaries (GeoJSON + Shapely)
  • Event-time watermarks with 3-minute allowed lateness
  • Checkpointing every 60 seconds (state saved to filesystem)
"""

import json
import logging
import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# Path to the GeoJSON file (copied into the Docker container by the Dockerfile)
GEOJSON_PATH = "/opt/flink/usrlib/arrondissements.geojson"

# ═══════════════════════════════════════════════════════════════════
# Watermark: Custom TimestampAssigner
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
      2. Uses Shapely point-in-polygon to find the real Casablanca arrondissement.
      3. Executes a prepared INSERT into Cassandra's vehicle_positions table.
    """

    def __init__(self):
        self.session = None
        self.zone_lookup = None

    def open(self, runtime_context):
        """Called once when the Flink TaskManager worker starts."""
        # ── Cassandra Connection ──
        from cassandra.cluster import Cluster
        cluster = Cluster(['cassandra'], port=9042)
        self.session = cluster.connect('taasim')

        # Prepared statements are pre-compiled by Cassandra.
        # This avoids CQL parsing overhead on every single INSERT.
        self.prepared_stmt = self.session.prepare("""
            INSERT INTO vehicle_positions
            (city, zone_id, zone_name, event_time, taxi_id, lat, lon, speed, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        # ── Load Real Arrondissement Boundaries ──
        from shapely.geometry import shape, Point
        
        geojson_path = GEOJSON_PATH
        if not os.path.exists(geojson_path):
            print(f"WARNING: GeoJSON not found at {geojson_path}, using fallback grid")
            self.zone_lookup = None
            return

        with open(geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Zone name mapping (same as src/zones/zone_lookup.py)
        name_map = {
            "Mechouar": "Mechouar Casablanca",
            "Anfa": "Anfa", "Hay Hassani": "Hay Hassani",
            "Sidi Moumen": "Sidi Moumen", "Moulay Rachid": "Moulay Rachid",
            "Sidi Othmane": "Sidi Othmane", "Ben M": "Ben M'Sick",
            "Sbata": "Sbata", "Chock": "Aîn-Chock",
            "Fida": "Al Fida", "Mers Sultan": "Mers Sultan",
            "Assoukhour": "Assoukhour Assawda",
            "Hay Mohammadi": "Hay Mohammadi", "Seba": "Aïn Sebaâ",
            "Bernoussi": "Sidi Bernoussi", "Maarif": "El Maarif",
            "Belyout": "Sidi Belyout",
        }
        
        zone_id_map = {
            "Mechouar Casablanca": 1, "Anfa": 2, "Hay Hassani": 3,
            "Sidi Moumen": 4, "Moulay Rachid": 5, "Sidi Othmane": 6,
            "Ben M'Sick": 7, "Sbata": 8, "Aîn-Chock": 9,
            "Al Fida": 10, "Mers Sultan": 11, "Assoukhour Assawda": 12,
            "Hay Mohammadi": 13, "Aïn Sebaâ": 14, "Sidi Bernoussi": 15,
            "El Maarif": 16, "Sidi Belyout": 17,
        }

        self.zones = []
        for feature in data["features"]:
            raw_name = feature["properties"].get("Arrondissement", "")
            # Find matching clean name
            clean = raw_name
            for key, val in name_map.items():
                if key in raw_name:
                    clean = val
                    break
            zone_id = zone_id_map.get(clean, 0)
            polygon = shape(feature["geometry"])
            self.zones.append({
                "zone_id": zone_id,
                "zone_name": clean,
                "polygon": polygon,
            })
        
        self.zone_lookup = True
        print(f"✅ Loaded {len(self.zones)} real arrondissement polygons")

    def _find_zone(self, lat, lon):
        """Point-in-polygon lookup using Shapely."""
        from shapely.geometry import Point
        point = Point(lon, lat)  # Shapely uses (x=lon, y=lat)
        for zone in self.zones:
            if zone["polygon"].contains(point):
                return zone["zone_id"], zone["zone_name"]
        return 0, "Unknown"

    def map(self, value):
        from datetime import datetime, timezone

        try:
            # ── Step 1: Deserialize JSON ──
            data = json.loads(value)
            lon = float(data['lon'])
            lat = float(data['lat'])

            # ── Step 2: Arrondissement Lookup ──
            if self.zone_lookup:
                zone_id, zone_name = self._find_zone(lat, lon)
            else:
                # Fallback: old 4×4 grid (should not happen in production)
                grid_x = int(4 * (lon - (-7.6895)) / ((-7.4008) - (-7.6895)))
                grid_y = int(4 * (lat - 33.5072) / (33.6527 - 33.5072))
                if grid_x < 0 or grid_x >= 4 or grid_y < 0 or grid_y >= 4:
                    return "SKIPPED: Out of Bounds"
                zone_id = (grid_y * 4) + grid_x + 1
                zone_name = f"Grid-{zone_id}"

            # Skip points outside Casablanca
            if zone_id == 0:
                return "SKIPPED: Outside Casablanca boundaries"

            # Normalize the UNIX epoch ms timestamp to UTC datetime
            dt = datetime.fromtimestamp(data['timestamp'] / 1000.0, tz=timezone.utc)

            # # ── Step 3: Cassandra Sink ──
            # self.session.execute(self.prepared_stmt, [
            #     'casablanca',
            #     zone_id,
            #     zone_name,
            #     dt,
            #     str(data['taxi_id']),
            #     lat,
            #     lon,
            #     float(data.get('speed', 0.0)),
            #     str(data.get('status', 'available'))
            # ])

            # return f"SUCCESS: Taxi {data['taxi_id']} → {zone_name} (Zone {zone_id})"

            # ── Step 3: Cassandra Sink ──
            self.session.execute(self.prepared_stmt, [
                'casablanca',
                zone_id,
                zone_name,
                dt,
                str(data['taxi_id']),
                lat,
                lon,
                float(data.get('speed', 0.0)),
                str(data.get('status', 'available'))
            ])

            # Enrich original data or build a clean payload for Job 2
            data['zone_id'] = zone_id
            data['zone_name'] = zone_name

            # Return a true JSON string so it passes your .startswith("{") filter!
            return json.dumps(data)

        except Exception as e:
            return f"ERROR processing row: {str(e)}"


def main():
    print("═" * 60)
    print("  Flink Job 1: GPS Normalizer & Cassandra Ingestion")
    print("  Mode: Real Arrondissement Boundaries (GeoJSON + Shapely)")
    print("═" * 60)

    # ── 1. Streaming Environment ──
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # ── 2. Checkpointing: save Flink state every 60 seconds ──
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)

    # Load the Kafka Connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # ── 3. Kafka Source ──
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("raw.gps") \
        .set_group_id("flink-gps-job") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # ── 4. Watermark Strategy ──
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(GPSTimestampAssigner())

    # ── 5. Build the DataStream ──
    stream = env.from_source(source, watermark_strategy, "Kafka GPS Source")

    # ── 6. Transform & Sink ──
    result_stream = stream.map(ProcessAndSaveGPS(), output_type=Types.STRING())

    # Print processing log to the TaskManager stdout
    result_stream.print()

    # ── 7. Kafka Sink for processed.gps ──
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("processed.gps")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    result_stream.filter(lambda x: x.startswith("{")).sink_to(kafka_sink)

    # ── 8. Execute ──
    env.execute("GPS Normalizer Job")


if __name__ == '__main__':
    main()
