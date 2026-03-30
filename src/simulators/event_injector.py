import json
import time
import uuid
import random
from kafka import KafkaProducer
import argparse

def create_kafka_producer(broker="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )

def inject_demand_spike(producer, zone_id, factor, duration_mins):
    """Multiplies demand in a specific zone to simulate a stadium exit."""
    print(f"Injecting DEMAND SPIKE in Zone {zone_id} ({factor}x) for {duration_mins} mins")
    topic = "raw.trips"
    
    end_time = time.time() + (duration_mins * 60)
    
    while time.time() < end_time:
        requests = int(5 * factor) # Base 5 requests per sec during spike
        for _ in range(requests):
            event = {
                "trip_id": str(uuid.uuid4()),
                "rider_id": f"rider_spike_{random.randint(100, 999)}",
                "origin_zone": zone_id,
                "destination_zone": random.randint(1, 16),
                "requested_at": int(time.time() * 1000),
                "call_type": "C" # Hail
            }
            producer.send(topic, key=str(zone_id), value=event)
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", type=str, default="localhost:9092")
    parser.add_argument("--type", type=str, choices=["spike", "rain"], required=True)
    parser.add_argument("--zone", type=int, default=1)
    parser.add_argument("--factor", type=float, default=3.0)
    parser.add_argument("--duration", type=int, default=5, help="Duration in minutes")
    
    args = parser.parse_args()
    prod = create_kafka_producer(args.broker)
    
    if args.type == "spike":
        inject_demand_spike(prod, args.zone, args.factor, args.duration)
    elif args.type == "rain":
        print("Rain anomaly increases global demand by 1.4x (Not implemented in standalone yet)")
