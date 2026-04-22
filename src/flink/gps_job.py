import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# --- Casablanca Boundaries ---
# The logic from the python simulators
CASA_LON_MIN, CASA_LON_MAX = -7.80, -7.40
CASA_LAT_MIN, CASA_LAT_MAX = 33.40, 33.70

class ProcessAndSaveGPS(MapFunction):
    """
    This Flink MapFunction does 3 things concurrently for every Kafka message:
    1. Parses the JSON GPS ping.
    2. Uses geographical logic to map the Lat/Lon into one of 16 Zone IDs.
    3. Uses the Python Cassandra driver to INSERT the result instantly into the database.
    """
    
    def __init__(self):
        self.session = None

    def open(self, runtime_context):
        """Called once when the Flink worker starts."""
        from cassandra.cluster import Cluster
        # Connect to the Cassandra Docker container on its default port
        cluster = Cluster(['cassandra'], port=9042)
        # Connect to the 'taasim' keyspace we built in Week 2
        self.session = cluster.connect('taasim')
        
        # Prepare the SQL statement for extreme speed
        self.prepared_stmt = self.session.prepare("""
            INSERT INTO vehicle_positions 
            (city, zone_id, event_time, taxi_id, lat, lon, speed, status) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

    def map(self, value):
        from datetime import datetime, timezone
        
        try:
            # 1. Parse JSON from Kafka
            data = json.loads(value)
            lon = float(data['lon'])
            lat = float(data['lat'])
            
            # 2. Translate Lat/Lon to Zone ID (1-16)
            # We divide the Casablanca bounding box into a 4x4 grid (16 squares)
            grid_x = int(4 * (lon - CASA_LON_MIN) / (CASA_LON_MAX - CASA_LON_MIN))
            grid_y = int(4 * (lat - CASA_LAT_MIN) / (CASA_LAT_MAX - CASA_LAT_MIN))
            
            # Discard glitches that are out of bounds (e.g. middle of the ocean)
            if grid_x < 0 or grid_x >= 4 or grid_y < 0 or grid_y >= 4:
                return "SKIPPED: Out of Bounds" 
                
            zone_id = (grid_y * 4) + grid_x + 1
            
            # Convert millisecond timestamp to standard Datetime format for Cassandra
            dt = datetime.fromtimestamp(data['timestamp'] / 1000.0, tz=timezone.utc)
            
            # 3. Save to Cassandra Database
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
            
            return f"SUCCESS: Taxi {data['taxi_id']} mapped to Zone {zone_id}"
            
        except Exception as e:
            return f"ERROR processing row: {str(e)}"

def main():
    print("Starting Flink Job 1: GPS Normalization & Cassandra Ingestion...")
    
    # 1. Setup Streaming Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Load the Kafka Connector JAR we downloaded in the Dockerfile
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    
    # 2. Connect to Kafka using the modern KafkaSource API
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("raw.gps") \
        .set_group_id("flink-gps-job") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # 3. Create the DataStream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
    # Pass every Kafka message through our Transformation and Database Saving logic
    result_stream = stream.map(ProcessAndSaveGPS(), output_type=Types.STRING())
    
    # Print the log output so we can see it working in the console
    result_stream.print()
          
    # Execute the program continuously
    env.execute("GPS Normalizer Job")

if __name__ == '__main__':
    main()
