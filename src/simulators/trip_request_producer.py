import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

# 16 Arrondissements in Casablanca
NUM_ZONES = 16

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
        
    return {
        "trip_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "origin_zone": origin,
        "destination_zone": dest,
        "requested_at": int(time.time() * 1000),
        "call_type": random.choice(["A", "B", "C"]) # A: Central, B: Stand, C: Hail
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
