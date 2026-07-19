import json
import logging
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Types, Time, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from cassandra.cluster import Cluster

# --- 1. Timestamp Assigners ---
class GpsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            return int(data['timestamp'])
        except Exception as e:
            return record_timestamp

class TripTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            return int(data['requested_at'])
        except Exception as e:
            return record_timestamp

# --- 2. Map Functions ---
# CHANGEMENT: Ajout de l'extraction de zone_name
class GpsJsonMapFunction(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            return (int(data['zone_id']), str(data['taxi_id']), "gps", str(data.get('zone_name', 'Unknown')))
        except Exception as e:
            logging.warning(f"GPS parsing failed: {e}")
            return None

# CHANGEMENT: Ajout de l'extraction de zone_name
class TripJsonMapFunction(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            return (int(data['origin_zone']), "dummy_id", "trip", str(data.get('zone_name', 'Unknown')))
        except Exception as e:
            logging.warning(f"Trip parsing failed: {e}")
            return None

# --- 3. Logic ---
class AggregateDemand(ProcessWindowFunction):
    def process(self, key, context, elements):
        active_vehicles = set()
        pending_requests = 0
        zone_name = "Unknown"
        
        for e in elements:
            zone_name = e[3]  # CHANGEMENT: Récupération du zone_name depuis le tuple
            if e[2] == "gps":
                active_vehicles.add(e[1])
            else:
                pending_requests += 1
                
        num_vehicles = len(active_vehicles)
        ratio = pending_requests / max(num_vehicles, 1)
        window_start = datetime.fromtimestamp(context.window().start / 1000.0, tz=timezone.utc).isoformat()
        
        # CHANGEMENT: Ajout de zone_name dans le tuple de retour
        yield ("casablanca", key, zone_name, window_start, num_vehicles, pending_requests, float(ratio))

# --- 4. Main Job Execution ---
def run_demand_aggregator_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)
    env.set_state_backend(EmbeddedRocksDBStateBackend())

    # Kafka Sources
    gps_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("processed.gps") \
        .set_group_id("flink-demand-aggregator-gps") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    trips_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("raw.trips") \
        .set_group_id("flink-demand-aggregator-trips") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    gps_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(180)) \
        .with_timestamp_assigner(GpsTimestampAssigner()) \
        .with_idleness(Duration.of_seconds(10))

    trip_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(180)) \
        .with_timestamp_assigner(TripTimestampAssigner()) \
        .with_idleness(Duration.of_seconds(10))

    # CHANGEMENT: Ajout de Types.STRING() pour zone_name dans output_type
    gps_stream = env.from_source(gps_source, gps_strategy, "Kafka GPS Source") \
        .map(GpsJsonMapFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()])) \
        .filter(lambda x: x is not None)

    # CHANGEMENT: Ajout de Types.STRING() pour zone_name dans output_type
    trips_stream = env.from_source(trips_source, trip_strategy, "Kafka Trips Source") \
        .map(TripJsonMapFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()])) \
        .filter(lambda x: x is not None)

    unioned_stream = gps_stream.union(trips_stream)

    class SaveToCassandraAndFormat(MapFunction):
        def __init__(self):
            self.session = None
            self.kafka_producer = None

        def open(self, runtime_context):
            from cassandra.cluster import Cluster
            cluster = Cluster(['cassandra'], port=9042)
            self.session = cluster.connect('taasim')
            
            # CHANGEMENT: Ajout de zone_name dans l'INSERT Cassandra
            self.prepared_stmt = self.session.prepare("""
                INSERT INTO demand_zones
                (city, zone_id, zone_name, window_start, active_vehicles, pending_requests, ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """)
            
            from kafka import KafkaProducer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

        def map(self, value):
            city = value[0]
            zone_id = value[1]
            zone_name = value[2]          # CHANGEMENT: Extraction de zone_name
            window_start_str = value[3]
            active_vehicles = value[4]
            pending_requests = value[5]
            ratio = value[6]
            
            dt = datetime.fromisoformat(window_start_str)

            # CHANGEMENT: Ajout de zone_name dans l'exécution de la requête
            self.session.execute(self.prepared_stmt, [
                city, zone_id, zone_name, dt, active_vehicles, pending_requests, ratio
            ])

            # CHANGEMENT: Ajout de zone_name dans le payload Kafka
            payload = {
                "city": city,
                "zone_id": zone_id,
                "zone_name": zone_name,
                "window_start": window_start_str,
                "active_vehicles": active_vehicles,
                "pending_requests": pending_requests,
                "ratio": ratio
            }
            
            self.kafka_producer.send("processed.demand", value=payload)
            self.kafka_producer.flush() 
            
            return json.dumps(payload)

    # CHANGEMENT: Ajout de Types.STRING() (pour zone_name) dans output_type du process
    demand_stream = unioned_stream.key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .process(
            AggregateDemand(), 
            output_type=Types.TUPLE([
                Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT()
            ])
        )
    
    demand_stream.map(SaveToCassandraAndFormat(), output_type=Types.STRING())

    env.execute("Real-Time Demand Aggregator")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_demand_aggregator_job()