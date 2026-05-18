
---

## Required Local Data Check

Job 1 needs GPS data from the simulator. The current GPS simulator expects this file:

```bash
ls -lh data/mapped/casa_trips_road_snapped.parquet

```

If the file is missing, generate it first before continuing.

Also check the arrondissement GeoJSON used by the current Flink GPS job:

```bash
ls -lh src/flink/arrondissements.geojson

```

---

## Install Host Python Dependencies

Build Docker Images. Run this the first time, and also after changing `src/flink/Dockerfile`.

```bash
docker compose build --no-cache

```

---

## Start The Full Stack

For a clean first run, use this only if you accept deleting old Kafka, Cassandra, and MinIO data:

```bash
docker compose down -v

```

> ⚠️ **Note:** If you want to keep old data, skip the command above.

Now start the stack:

```bash
docker compose up -d

```

---

## Start Producers

### 1. Start GPS Producer

Open a new terminal and run:

```bash
python src/simulators/vehicle_gps_producer.py --broker localhost:9092 --speed 10

```

### 2. Start Trip Request Producer

Open another new terminal and run:

```bash
python src/simulators/trip_request_producer.py --broker localhost:9092

```

---

## Verify Raw Kafka Streams

Check raw GPS offsets:

```bash
docker exec taasim-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic raw.gps

```

Check raw trips offsets:

```bash
docker exec taasim-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic raw.trips

```

---

## Submit Flink Job 1: GPS Normalizer

```bash
docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/gps_job.py

```

### Verify Job 1 Writes to Cassandra

Wait 30 seconds, then run:

```bash
docker exec taasim-cassandra cqlsh -e "SELECT city, zone_id, zone_name, event_time, taxi_id, status FROM taasim.vehicle_positions LIMIT 5;"

```

**Expected output should look like:**

```text
    city    | zone_id |   zone_name    |           event_time            | taxi_id  | status
------------+---------+----------------+---------------------------------+----------+---------
 casablanca |      15 | Sidi Bernoussi | 2026-05-17 18:22:05.204000+0000 | 20000386 | engaged
 casablanca |      15 | Sidi Bernoussi | 2026-05-17 18:22:03.631000+0000 | 20000386 | engaged
 casablanca |      15 | Sidi Bernoussi | 2026-05-17 18:22:02.075000+0000 | 20000013 | engaged
 casablanca |      15 | Sidi Bernoussi | 2026-05-17 18:22:00.525000+0000 | 20000386 | engaged
 casablanca |      15 | Sidi Bernoussi | 2026-05-17 18:21:58.898000+0000 | 20000386 | engaged

(5 rows)

```

### Verify Job 1 Publishes to `processed.gps`

This step is important because Job 2 needs `processed.gps`.

```bash
docker exec taasim-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic processed.gps

```

---

## Submit Flink Job 2: Demand Aggregator

```bash
docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/demand_aggregator_job.py

```

### Verify Job 2 Writes to Cassandra

Wait at least 60 seconds after starting Job 2.

```bash
docker exec taasim-cassandra cqlsh -e "SELECT * FROM taasim.demand_zones LIMIT 10;"

```

**Expected output:**

```text
    city    | zone_id | zone_name |          window_start           | active_vehicles | forecast_demand | pending_requests | ratio
------------+---------+-----------+---------------------------------+-----------------+-----------------+------------------+-------
 casablanca |       1 |      null | 2026-05-17 15:45:00.000000+0000 |               4 |            null |                2 |   0.5
 casablanca |       1 |      null | 2026-05-17 15:44:30.000000+0000 |               4 |            null |                4 |     1
 casablanca |       1 |      null | 2026-05-17 15:44:00.000000+0000 |               4 |            null |                1 |  0.25

```

### Verify Job 2 Publishes to `processed.demand`

Check offsets:

```bash
docker exec taasim-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic processed.demand

```