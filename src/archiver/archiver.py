"""
Kafka-to-MinIO Archiver
========================
Lightweight consumer that reads from Kafka topics (raw.gps, raw.trips)
and writes JSON files to MinIO kafka-archive/ bucket.

This replaces the heavy Confluent Kafka Connect S3 Sink connector (~1.5 GB)
with a simple Python script (~50 MB) that does the same thing.

How it works:
1. Connects to Kafka as a consumer group
2. Reads messages from raw.gps and raw.trips
3. Batches messages (100 messages or 30 seconds, whichever comes first)
4. Writes each batch as a JSON file to MinIO:
   kafka-archive/raw.gps/2026/04/20/14/batch_1713621600_001.json
   kafka-archive/raw.trips/2026/04/20/14/batch_1713621600_002.json
"""

import os
import json
import time
import io
from datetime import datetime, timezone

from kafka import KafkaConsumer
from minio import Minio

# ── Configuration from environment variables ──
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "kafka-archive")
TOPICS = os.getenv("TOPICS", "raw.gps,raw.trips").split(",")

BATCH_SIZE = 100          # Flush after 100 messages
FLUSH_INTERVAL_SEC = 30   # Or flush after 30 seconds

def create_minio_client():
    """Create MinIO client, stripping http:// prefix."""
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    secure = MINIO_ENDPOINT.startswith("https")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=secure)


def ensure_bucket(client, bucket_name):
    """Create the bucket if it doesn't exist."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")


def flush_batch(minio_client, topic, batch, batch_counter):
    """Write a batch of messages as a single JSON file to MinIO."""
    if not batch:
        return batch_counter

    now = datetime.now(timezone.utc)
    # Path: kafka-archive/raw.gps/2026/04/20/14/batch_<unix>_<counter>.json
    object_path = (
        f"{topic}/"
        f"{now.strftime('%Y/%m/%d/%H')}/"
        f"batch_{int(now.timestamp())}_{batch_counter:04d}.json"
    )

    # Write as JSON lines (one JSON object per line)
    content = "\n".join(json.dumps(msg) for msg in batch) + "\n"
    data = content.encode("utf-8")

    minio_client.put_object(
        MINIO_BUCKET,
        object_path,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

    print(f"[{now.strftime('%H:%M:%S')}] Flushed {len(batch)} msgs → s3://{MINIO_BUCKET}/{object_path}")
    return batch_counter + 1


def main():
    print(f"Kafka Archiver starting...")
    print(f"  Broker:  {KAFKA_BROKER}")
    print(f"  Topics:  {TOPICS}")
    print(f"  MinIO:   {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    print(f"  Batch:   {BATCH_SIZE} msgs or {FLUSH_INTERVAL_SEC}s")

    # Wait for Kafka to be ready
    print("Waiting 20s for Kafka to initialize...")
    time.sleep(20)

    # Initialize MinIO
    minio_client = create_minio_client()
    ensure_bucket(minio_client, MINIO_BUCKET)

    from kafka import TopicPartition
    
    # Initialize Kafka consumer WITHOUT a group_id to bypass the KRaft bug
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    
    # We know from docker-compose that there are 4 partitions per topic (0, 1, 2, 3)
    partitions = []
    for topic in TOPICS:
        for p in range(4):
            partitions.append(TopicPartition(topic, p))
            
    # Manually assign and seek to beginning
    consumer.assign(partitions)
    consumer.seek_to_beginning()

    print(f"Connected to Kafka. Listening on topics: {TOPICS}", flush=True)

    # Per-topic batches
    batches = {topic: [] for topic in TOPICS}
    counters = {topic: 1 for topic in TOPICS}
    last_flush = time.time()

    while True:
        try:
            # Use poll() instead of iterator for robust timeout handling
            records = consumer.poll(timeout_ms=1000)
            
            if records:
                for topic_partition, messages in records.items():
                    topic = topic_partition.topic
                    if topic not in batches:
                        batches[topic] = []
                        counters[topic] = 1

                    for message in messages:
                        batches[topic].append(message.value)

                    # Flush if batch is full
                    if len(batches[topic]) >= BATCH_SIZE:
                        counters[topic] = flush_batch(minio_client, topic, batches[topic], counters[topic])
                        batches[topic] = []
                        last_flush = time.time()

            # Time-based flush (every FLUSH_INTERVAL_SEC)
            if time.time() - last_flush >= FLUSH_INTERVAL_SEC:
                for topic in TOPICS:
                    if batches.get(topic):
                        counters[topic] = flush_batch(minio_client, topic, batches[topic], counters[topic])
                        batches[topic] = []
                last_flush = time.time()

        except Exception as e:
            print(f"Error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
