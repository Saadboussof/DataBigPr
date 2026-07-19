import json
import uuid
import time
from kafka import KafkaProducer
from config import KAFKA_BROKER

_producer = None

def get_kafka_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    return _producer

def publish_trip_request(origin_zone: int, dest_zone: int, rider_id: str):
    trip_id = str(uuid.uuid4())
    event = {
        "trip_id": trip_id,
        "rider_id": rider_id,
        "origin_zone": origin_zone,
        "destination_zone": dest_zone,
        "requested_at": int(time.time() * 1000),
        "call_type": "A"  # Central-hail booking
    }
    
    producer = get_kafka_producer()
    # Use origin_zone as Kafka partition key for downstream zone grouping
    producer.send("raw.trips", key=str(origin_zone).encode("utf-8"), value=event)
    producer.flush()
    
    return {"trip_id": trip_id, "status": "PENDING", "message": "Reservation submitted to pipeline"}
