# Architecture Decision Record — ADR-01: Storage Design
**Date:** April 2026  
**Status:** Accepted  
**Author:** TaaSim Team — ENSA Al Hoceima

---

## Context

TaaSim is a real-time urban mobility platform for Casablanca. It ingests live GPS pings from ~500 taxis and ride requests from passengers, processes them in real-time, and serves predictions via an API. The storage layer must support:

- **High write throughput:** Up to 500 GPS pings/second + 10 ride requests/second
- **Low-latency reads:** Dashboard queries must respond in < 100ms
- **Historical archival:** Raw events must be preserved for batch analytics and ML training
- **Fault tolerance:** No data loss if a processing node crashes

---

## Decision 1: Kappa Architecture (not Lambda)

### Choice
We adopt the **Kappa Architecture** where all data flows through Apache Kafka as a single unified stream. There is no separate batch ingestion path.

### Alternatives Considered
**Lambda Architecture:** Maintains two parallel pipelines — a real-time stream layer and a batch layer that periodically reprocesses historical data. Both outputs are merged in a "serving layer."

### Justification
| Criteria | Lambda | Kappa (chosen) |
|----------|--------|-----------------|
| Codebase | Two pipelines to maintain | One pipeline |
| Complexity | High — must reconcile batch + stream results | Low — single source of truth |
| Historical replay | Separate batch job | Replay from Kafka or MinIO archive |
| Latency | Batch layer adds hours of delay | Everything is real-time |

Kafka's 7-day retention policy combined with the Kafka-to-MinIO archiver (which preserves all events indefinitely) allows us to replay any historical period through the same Flink pipeline. This eliminates the need for a separate batch ingestion path.

---

## Decision 2: Cassandra Partition Keys

### Table: `vehicle_positions` — Partition Key: `(city, zone_id)`

**Query pattern:** *"Show me all taxis currently in Zone 7 of Casablanca"*

**Alternatives considered:**
- `(taxi_id)` — Would make "find taxi X" fast, but "find all taxis in zone X" would require scanning ALL partitions (full cluster scan). Since our dashboard and trip matcher both query by zone, this is unacceptable.
- `(city)` alone — Would put millions of rows in a single partition (all taxis in Casablanca). Exceeds the 100MB partition size guideline.

**Why `(city, zone_id)` is optimal:**
- Casablanca has 16 arrondissements → 16 partitions per city → well-distributed
- The main consumers (Grafana live map, Flink trip matcher) always query by zone
- `event_time DESC` clustering gives the latest position with `LIMIT 1`
- TTL of 24 hours auto-deletes old positions (they are transient)

### Table: `trips` — Partition Key: `(city, date_bucket)`

**Query pattern:** *"Show me all trips today"*

**Why `date_bucket` (not just `city`):**
Without date bucketing, the partition for `city = 'casablanca'` would grow indefinitely — after 8 weeks of simulation, it could contain millions of rows in a single partition. The Cassandra hard limit is ~2 billion cells per partition, and performance degrades above ~100MB.

By bucketing with `date_bucket = '2026-04-20'`, each partition contains only one day of trips (~50K-200K rows), keeping partitions compact and fast.

### Table: `demand_zones` — Partition Key: `(city, zone_id)`

Same reasoning as `vehicle_positions`. The query is "What is the current demand in Zone 7?" — not "What is the demand across all zones?" Each zone gets its own partition, and `window_start DESC` clustering returns the latest 30-second window first.

TTL of 7 days auto-deletes historical demand snapshots (they're also archived in MinIO).

---

## Decision 3: MinIO as Data Lake (not HDFS)

### Choice
We use **MinIO** (S3-compatible object storage) as our data lake, accessed via the `s3a://` protocol.

### Alternatives Considered
- **HDFS (Hadoop Distributed File System):** The traditional big data storage layer. Requires at least 3 DataNodes for replication, complex to configure, heavy resource usage.
- **Local filesystem:** Simplest, but not accessible from Spark workers running in separate containers.

### Justification
| Criteria | HDFS | MinIO (chosen) |
|----------|------|-----------------|
| Setup complexity | High (3+ nodes) | Single container |
| RAM usage | ~2GB per node | ~256MB |
| S3 compatibility | No | Yes — same API as AWS S3 |
| Cloud migration | Must rewrite to S3 | Zero code changes |
| Spark integration | Native | Native via `s3a://` |

MinIO runs as a single Docker container using ~256MB of RAM. Spark, Flink, and our Python archiver all read/write using the standard S3 protocol (`s3a://`). If TaaSim were deployed to AWS, we would simply change the endpoint URL from `http://minio:9000` to `https://s3.amazonaws.com` — no code changes required.

### Bucket Structure
| Bucket | Purpose | Written by | Read by |
|--------|---------|-----------|---------|
| `raw/` | Original datasets (Porto CSV, NYC Parquet) | Manual upload | Spark ETL |
| `curated/` | Cleaned, transformed data (Parquet) | Spark ETL | Spark ML |
| `ml-store/` | Feature matrices and trained models | Spark ML | FastAPI |
| `kafka-archive/` | Mirror of all Kafka topics (JSON) | Kafka Archiver | Spark ETL (replay) |

---

## Decision 4: Kafka-to-MinIO Archiver (not Confluent Kafka Connect)

### Choice
We use a **lightweight Python consumer** (`archiver.py`) instead of the Confluent Kafka Connect S3 Sink connector.

### Justification
| Criteria | Confluent S3 Sink | Python Archiver (chosen) |
|----------|-------------------|--------------------------|
| Docker image size | ~1.5 GB | ~50 MB |
| Configuration | Complex JSON connector config | 10 lines of environment variables |
| Dependencies | JVM + Confluent Platform | Python + kafka-python + minio |
| Functionality | Full enterprise features | Exactly what we need: batch + flush |
| Understandability | Opaque Java plugin | Readable 130-line Python script |

For a capstone project running on a single laptop (16GB RAM), the Confluent connector is unnecessarily heavy. Our Python archiver provides identical functionality: batch messages (100 msgs or 30 seconds), write as JSON to MinIO with hourly partitioned paths.

---

*This ADR documents the storage design decisions made in Week 2 of the TaaSim capstone project.*
