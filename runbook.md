# TaaSim Runbook

## 0. Prerequisites

```bash
pip install kafka-python pandas pyarrow
ls -lh data/mapped/casa_trips_road_snapped.parquet src/flink/arrondissements.geojson
```

## 1. Build + Start Stack

```bash
docker compose build --no-cache
docker compose up -d
# Wait 60s for init containers
```

## 2. Quick Checks

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"

# Kafka topics
docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
# Expected: raw.gps, raw.trips, processed.gps, processed.demand, processed.matches

# MinIO buckets
docker exec taasim-minio mc alias set local http://minio:9000 admin password
docker exec taasim-minio mc ls local
# Expected: raw/, curated/, ml-store/, kafka-archive/

# Cassandra schema
docker exec taasim-cassandra cqlsh -e "DESCRIBE KEYSPACE taasim;"
# Expected: vehicle_positions, trips, demand_zones, kpi_summary
```

## 3. Start Producers

```bash
# Terminal 1: GPS producer
python src/simulators/vehicle_gps_producer.py --broker localhost:9092 --speed 10

# Terminal 2: Trip request producer
python src/simulators/trip_request_producer.py --broker localhost:9092
```

## 4. Flink Jobs

```bash
# Job 1 - GPS Normalizer
docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/gps_job.py

# Wait 30s, verify Cassandra writes
docker exec taasim-cassandra cqlsh -e "SELECT city, zone_id, zone_name, taxi_id, status FROM taasim.vehicle_positions LIMIT 5;"

# Job 2 - Demand Aggregator
docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/demand_aggregator_job.py

# Wait 60s, verify demand_zones
docker exec taasim-cassandra cqlsh -e "SELECT zone_id, window_start, active_vehicles, pending_requests, ratio FROM taasim.demand_zones LIMIT 10;"

# Job 3 - Trip Matcher
docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/trip_matcher_job.py

# Wait 30s, verify trips
docker exec taasim-cassandra cqlsh -e "SELECT trip_id, taxi_id, origin_zone, dest_zone, status FROM taasim.trips LIMIT 10;"

# All 3 jobs running
docker exec taasim-flink-jm flink list
```

## 5. Upload Data to MinIO

```bash
for f in data/train.csv data/zone_mapping.csv data/yellow_tripdata_2024-01.parquet data/yellow_tripdata_2024-02.parquet data/yellow_tripdata_2024-03.parquet; do
  docker cp "$f" taasim-minio:/tmp/$(basename "$f")
done

docker exec taasim-minio mc cp /tmp/train.csv local/raw/porto-trips/train.csv
docker exec taasim-minio mc cp /tmp/zone_mapping.csv local/raw/zone-mapping/zone_mapping.csv
docker exec taasim-minio mc cp /tmp/yellow_tripdata_2024-01.parquet local/raw/nyc-tlc/
docker exec taasim-minio mc cp /tmp/yellow_tripdata_2024-02.parquet local/raw/nyc-tlc/
docker exec taasim-minio mc cp /tmp/yellow_tripdata_2024-03.parquet local/raw/nyc-tlc/
```

## 6. Spark ETLs (run in Jupyter container)

```bash
# Install h3
docker exec -it taasim-jupyter pip install h3

# ETL Porto (~10-15 min)
docker exec -it taasim-jupyter spark-submit \
  --master "local[*]" --driver-memory 4G --executor-memory 4G \
  --conf spark.sql.adaptive.enabled=true \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /home/jovyan/src/spark/etl_porto.py

# ETL NYC (~2-3 min)
docker exec -it taasim-jupyter spark-submit \
  --master "local[*]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /home/jovyan/src/spark/etl_nyc.py
```

## 7. KPIs Week 5

```bash
docker exec -it taasim-jupyter spark-submit \
  --master "local[*]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /home/jovyan/src/spark/kpi_batch_week5.py

# KPI Analytics → Cassandra (~5-7 min)
docker exec -it taasim-jupyter spark-submit \
  --master "local[*]" --driver-memory 4G --executor-memory 4G \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1" \
  /home/jovyan/src/spark/kpi_analytics.py
```

## 8. ML Model Week 6 (~10-15 min)

```bash
docker cp src/spark/ml_demand_forecasting_week6.py taasim-jupyter:/home/jovyan/src/spark/ml_demand_forecasting_week6.py

docker exec -it taasim-jupyter spark-submit \
  --master "local[*]" --driver-memory 4G --executor-memory 4G \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /home/jovyan/src/spark/ml_demand_forecasting_week6.py
```

## 9. Final Verification

```bash
echo "=== Flink Jobs ===" && docker exec taasim-flink-jm flink list
echo "=== demand_zones ===" && docker exec taasim-cassandra cqlsh -e "SELECT zone_id, ratio FROM taasim.demand_zones LIMIT 10;"
echo "=== kpi_summary ===" && docker exec taasim-cassandra cqlsh -e "SELECT zone_id, total_trips FROM taasim.kpi_summary LIMIT 10;"
echo "=== MinIO curated ===" && docker exec taasim-minio mc ls local/curated --recursive
echo "=== MinIO ml-store ===" && docker exec taasim-minio mc ls local/ml-store --recursive
```

## 10. Stop

```bash
docker compose down          # keep data
docker compose down -v       # delete all data
```

## GUIs

| Tool | URL | Login |
|------|-----|-------|
| Flink | http://localhost:8081 | — |
| Spark | http://localhost:8080 | — |
| MinIO | http://localhost:9001 | admin/password |
| Jupyter | http://localhost:8888 | token: taasim |
| Grafana | http://localhost:3000 | admin/admin |
