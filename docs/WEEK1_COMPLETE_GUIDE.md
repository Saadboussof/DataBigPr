# TaaSim — Week 1: Complete Project Guide
## Everything You Need to Know for the Presentation

> **Project:** TaaSim — Transport as a Service — Casablanca Urban Mobility
> **School:** ENSA Al Hoceima · Advanced Big Data Capstone · 2025–2026
> **Professor:** Mohamed El Marouani

---

# PART 1: WHAT IS THIS PROJECT?

## 1.1 The Real-World Problem

Casablanca is the biggest city in Morocco (4+ million people). It has taxis (grands taxis, petits taxis), buses, trams, and trains — but **none of them share data with each other**. This creates huge problems:

| Problem | What happens in reality |
|---------|------------------------|
| **No shared data layer** | Grand taxis, petits taxis, and minibuses have no GPS, no digital booking, no shared schedule |
| **Demand blindness** | Drivers cruise empty looking for passengers. Passengers wait with no idea where taxis are |
| **No interoperability** | ONCF trains, BRT buses, and taxis share zero data |
| **Cash only** | No electronic payment → no trip history → no analytics possible |
| **Underserved periphery** | New districts (Bouskoura, Ain Sebaâ, Sidi Moumen) grow faster than routes are planned |

## 1.2 Our Solution: TaaSim

TaaSim is a **data platform** that treats urban mobility as a data engineering problem. Think of it as the **backend engine of an app like Uber or InDrive**, but designed for Casablanca's entire transport network.

**What TaaSim does:**
1. **Ingests** live GPS streams from all vehicles in real time
2. **Processes** citizen trip requests as they come in
3. **Matches** riders to the nearest available vehicle
4. **Forecasts** future demand using Machine Learning
5. **Visualizes** everything on a live dashboard for city planners

## 1.3 The Data Challenge

**Problem:** There is NO public open dataset for Casablanca taxis. Nobody has published GPS traces, trip logs, or demand data for this city. This is a real-world constraint in emerging markets.

**Solution:** We use TWO proxy datasets:

| Dataset | What it provides | How we use it |
|---------|-----------------|---------------|
| **Porto Taxi Trajectories** (Portugal) | 1.7M real taxi trips with GPS traces | We **remap** Porto coordinates → Casablanca. This gives us realistic trip patterns for streaming simulation |
| **NYC Yellow Taxi** (New York) | 9.5M trips in 3 months | We use this for **batch processing at scale** — testing that our Spark pipeline can handle millions of rows |

---

# PART 2: ARCHITECTURE — HOW THE SYSTEM WORKS

## 2.1 The Kappa Architecture

TaaSim uses the **Kappa Architecture**. This is an important concept:

- **Old approach (Lambda Architecture):** You build TWO separate pipelines — one for real-time data, one for historical batch data. This is complex and hard to maintain.
- **Our approach (Kappa Architecture):** **Everything is a stream.** The central nervous system is Apache Kafka. Even historical data gets replayed through Kafka as if it were real-time. ONE processing engine (Flink) handles both cases.

**Why Kappa?** Simpler. One codebase. One pipeline. Less bugs.

## 2.2 Data Flow Diagram

```
  DATA SOURCES
  ┌─────────────────────────────────────────────────┐
  │  Porto GPS Simulator  ·  Trip Request Simulator │
  │  Porto Batch CSV      ·  NYC TLC Parquet (S3)   │
  └────────────────┬────────────────────────────────┘
                   │
         ┌─────────▼──────────┐
         │   Apache Kafka      │   ← 3 topics:
         │  (KRaft, 1 broker)  │     raw.gps
         └──────┬──────┬───────┘     raw.trips
                │      │              processed.demand
        ┌───────▼──┐  ┌▼────────────────────────┐
        │  Flink   │  │  Kafka → MinIO S3 Sink  │
        │  3 Jobs  │  │  (raw zone archival)    │
        └───┬──┬───┘  └─────────────────────────┘
            │  │                    │
       ETAs │  │ Demand         ┌───▼──────────┐
            │  │ zones          │    MinIO     │
            │  └──────────────► │  raw/        │
            │                   │  curated/    │
            │              Spark│  ml/         │
            │            ◄──────┤              │
            │                   └──────────────┘
            │  Spark: ETL + MLlib demand forecast
            │                         │
        ┌───▼─────────────────────────▼──────┐
        │        Apache Cassandra             │
        │  trips · positions · demand_forecast│
        └─────────────────┬──────────────────┘
                          │
              ┌───────────▼───────────┐
              │  FastAPI  +  Grafana   │
              │  Live map · Heatmap    │
              │  KPI dashboard · Demo  │
              └───────────────────────┘
```

---

# PART 3: EVERY TOOL EXPLAINED IN DETAIL

## 3.1 Docker & Docker Compose 🐳

### What is Docker?
Docker is a tool that lets you run software in **containers** — lightweight, isolated environments. Instead of installing Kafka, Spark, Cassandra etc. directly on your Windows/Mac machine (which would be a nightmare), Docker packages each tool into a container that "just works."

### What is Docker Compose?
Docker Compose is a file (`docker-compose.yml`) that defines ALL the containers your project needs. With **one command** (`docker-compose up -d`), it starts everything simultaneously.

### How it works in TaaSim:
```yaml
# Our docker-compose.yml starts 9 containers:
1. taasim-kafka          → Message broker (port 9092)
2. taasim-minio          → Object storage / data lake (port 9000, console 9001)
3. taasim-minio-init     → Creates S3 buckets automatically (runs once, exits)
4. taasim-cassandra      → NoSQL database (port 9042)
5. taasim-flink-jm       → Flink Job Manager (port 8081)
6. taasim-flink-tm       → Flink Task Manager (worker)
7. taasim-spark-master   → Spark Master (port 8080)
8. taasim-spark-worker   → Spark Worker (2 cores, 2GB RAM)
9. taasim-jupyter         → Jupyter Lab with PySpark (port 8888)
10. taasim-grafana        → Dashboard visualization (port 3000)
```

### Key Docker concepts:
- **Image:** A recipe/template (e.g., `apache/kafka:3.7.0`)
- **Container:** A running instance of an image
- **Volume:** Persistent storage that survives container restarts (e.g., `kafka_data`)
- **Port mapping:** Makes internal services accessible from your browser (e.g., `8888:8888`)

---

## 3.2 Apache Kafka 📬

### What is Kafka?
Apache Kafka is a **distributed event streaming platform**. Think of it as a massive, ultra-fast post office for data.

### The problem Kafka solves:
Imagine 10,000 taxis sending GPS coordinates at the exact same second. If you send all of this directly to a database, it crashes. Kafka acts as a **shock absorber** — it receives all the messages, stores them safely, and lets consumers read them at their own pace.

### Key concepts:

| Concept | Explanation |
|---------|-------------|
| **Topic** | A named category of messages. Like a mailbox label. We have 3 topics: `raw.gps`, `raw.trips`, `processed.demand` |
| **Producer** | A program that SENDS messages to a Kafka topic. Our Python simulators are producers |
| **Consumer** | A program that READS messages from a Kafka topic. Flink is our consumer |
| **Broker** | A Kafka server that stores and serves messages. We use 1 broker |
| **KRaft mode** | New Kafka architecture that eliminates the old dependency on Zookeeper |
| **Retention** | How long messages are kept. We set 7 days — allows historical replay |

### In TaaSim:
- `vehicle_gps_producer.py` → publishes GPS pings to `raw.gps`
- `trip_request_producer.py` → publishes ride requests to `raw.trips`
- Flink reads from these topics and processes them in real-time
- Events are also archived to MinIO via Kafka Connect S3 Sink

---

## 3.3 Apache Spark (PySpark) ⚡

### What is Spark?
Apache Spark is a **distributed compute engine** for processing large datasets. "Distributed" means it splits the work across multiple CPU cores (or machines) to process data in parallel. It's like having 100 workers sorting mail instead of 1.

### Key concepts:

| Concept | Explanation |
|---------|-------------|
| **SparkSession** | The entry point to use Spark. You create one at the start of every notebook |
| **DataFrame** | A table of data (like an Excel sheet or SQL table) distributed across workers |
| **Transformation** | A lazy operation that defines a computation (`.filter()`, `.groupBy()`, `.withColumn()`) |
| **Action** | An operation that triggers actual computation (`.count()`, `.show()`, `.write()`) |
| **UDF** | User Defined Function — custom Python function applied to each row by Spark workers |
| **Parquet** | A columnar file format optimized for big data. Much faster than CSV |
| **PySpark** | The Python API for Spark. What we use in Jupyter notebooks |
| **Spark MLlib** | Spark's machine learning library (used in Week 6 for demand forecasting) |

### In TaaSim (Week 1):
- We load Porto's `train.csv` (1.7M rows) using `spark.read.csv()`
- We explore, clean, and transform the data using DataFrames
- We apply a UDF to remap GPS coordinates from Porto → Casablanca
- We save transformed data as Parquet files
- We load 9.5M NYC taxi trips using `spark.read.parquet()`
- We compute demand aggregations and save them

### Why Spark and not just Python/Pandas?
Pandas loads ALL data into RAM on ONE machine. If you have 10GB of data and 8GB of RAM, Pandas crashes. Spark distributes the work across multiple workers and can process terabytes. Our Porto dataset is 1.5GB, NYC is several GB — Spark handles this easily.

---

## 3.4 Apache Flink 🌊

### What is Flink?
Apache Flink is a framework for **real-time stream processing**. While Spark is great for batch processing (process a file, get results), Flink continuously processes data as it arrives — event by event, millisecond by millisecond.

### Key concepts:

| Concept | Explanation |
|---------|-------------|
| **Stream** | An infinite, continuous flow of events (like GPS pings arriving forever) |
| **Event time** | The timestamp when the event ACTUALLY happened (when the taxi sent the GPS ping) |
| **Processing time** | The timestamp when the server RECEIVES the event (could be later due to network delay) |
| **Watermark** | A mechanism to handle late-arriving data. "I'll wait up to 3 minutes for late events" |
| **Tumbling window** | A fixed time window (e.g., every 30 seconds, count all events in that window) |
| **Checkpoint** | A snapshot of Flink's state saved to MinIO every 60 seconds for fault tolerance |
| **JobManager** | The brain of Flink — coordinates all the work |
| **TaskManager** | The worker of Flink — actually processes the data |

### In TaaSim (Weeks 3–4):
- **Job 1 (GPS Normalizer):** Reads raw GPS from Kafka → validates coordinates → removes duplicates → handles late events with watermarks → maps to Casablanca zone → writes to Cassandra
- **Job 2 (Demand Aggregator):** 30-second tumbling window per zone → counts vehicles + pending requests → computes supply/demand ratio → writes to Cassandra
- **Job 3 (Trip Matcher):** When a trip request arrives → finds nearest available taxi in that zone → assigns match → computes ETA → writes to Cassandra

### Spark vs Flink — What's the difference?

| Feature | Spark | Flink |
|---------|-------|-------|
| Best for | Batch processing (process a whole file) | Stream processing (process event by event) |
| Latency | Seconds to minutes | Milliseconds |
| State | Stateless (each batch is independent) | Stateful (remembers past events) |
| In TaaSim | Offline ETL, ML training | Live GPS processing, trip matching |

---

## 3.5 MinIO (S3-Compatible Object Storage) 📦

### What is MinIO?
MinIO is your **own private AWS S3** running locally on your laptop. It's an object storage system where you store files (CSV, Parquet, ML models) in "buckets."

### What is "Object Storage"?
Unlike a normal file system (folders on your Desktop), object storage is designed for the cloud. Files are stored as "objects" inside "buckets" with metadata. Spark, Flink, and any big data tool can read from MinIO using the standard S3 protocol (`s3a://`).

### MinIO bucket structure (from the project PDF):

| Bucket Path | What's stored | Written by | Read by |
|-------------|--------------|------------|---------|
| `raw/porto-trips/` | Raw Porto CSV files as downloaded | Manual (Week 1) | Spark ETL |
| `raw/nyc-tlc/` | NYC TLC Parquet files (3 months) | Manual (Week 1) | Spark ETL |
| `raw/kafka-archive/` | Mirror of all Kafka topics | Kafka Connect | Spark ETL |
| `curated/trips/` | Cleaned + geo-enriched trips (Parquet) | Spark ETL | Spark ML |
| `ml/features/` | Feature matrix for ML training | Spark ML Prep | Spark MLlib |
| `ml/models/demand_v1/` | Trained ML model artifact | Spark ML Train | FastAPI |

### How to access MinIO:
- **Console:** http://localhost:9001 (username: `admin`, password: `password`)
- **API endpoint:** http://localhost:9000 (used by Spark with `s3a://` protocol)

---

## 3.6 Apache Cassandra 💾

### What is Cassandra?
Apache Cassandra is a **NoSQL database** designed for extremely fast writes and reads at scale. Unlike MySQL/PostgreSQL (relational databases), Cassandra has no JOINs, no foreign keys — tables are designed around **query patterns**.

### Key concepts:

| Concept | Explanation |
|---------|-------------|
| **Keyspace** | Equivalent to a "database" in SQL. Groups related tables together |
| **Partition Key** | Determines WHICH physical node stores the row. Chosen by what you query most |
| **Clustering Key** | Determines ORDER within a partition. Typically `DESC` for "latest first" |
| **NoSQL design rule** | Design tables around queries, NOT around normalization. One table per query pattern |

### TaaSim Cassandra tables:

| Table | Partition Key | Clustering Key | What it stores |
|-------|--------------|----------------|----------------|
| `vehicle_positions` | `(city, zone_id)` | `event_time DESC` | Current GPS position of every taxi |
| `trips` | `(city, date_bucket)` | `created_at DESC` | All trip records (request → match → completion) |
| `demand_zones` | `(city, zone_id)` | `window_start DESC` | 30-second demand snapshots per zone |

### Why `(city, zone_id)` and not `taxi_id`?
Because the main query is "show me all taxis in zone X" — not "show me all trips by taxi Y." Partition keys must match your query pattern.

---

## 3.7 Grafana 📊

### What is Grafana?
Grafana is an **open-source dashboard and visualization platform**. It connects to databases (Cassandra, PostgreSQL, etc.) and shows real-time charts, maps, and KPI panels.

### In TaaSim:
- **Live vehicle map:** Shows taxis moving in Casablanca in real-time
- **Demand heatmap:** Color-coded zones showing where demand is hot
- **KPI panels:** Trips per minute, average wait time, supply/demand ratio
- **ML forecast overlay:** Shows predicted demand vs actual demand

### Access: http://localhost:3000 (username: `admin`, password: `admin`)

---

## 3.8 Jupyter Lab 📓

### What is Jupyter?
Jupyter Lab is an **interactive development environment** that runs in your browser. You write Python code in "cells" and run them one by one, seeing the results immediately. Perfect for data exploration and visualization.

### In TaaSim:
- Our notebook `week1_exploration.ipynb` runs PySpark inside Jupyter
- Jupyter connects to the Spark cluster inside Docker
- Files are mounted from your Windows machine:
  - `./notebooks` → `/home/jovyan/work` (your code)
  - `./data` → `/home/jovyan/data` (your datasets)
  - `./src` → `/home/jovyan/src` (your Python source files)

### Access: http://localhost:8888 (token: `taasim`)

---

# PART 4: THE DATASETS — EVERY COLUMN EXPLAINED

## 4.1 Porto Taxi Trajectories (`train.csv`)

### Overview
| Property | Value |
|----------|-------|
| **Full name** | Taxi Service Trajectory — ECML/PKDD 2015 Challenge Dataset |
| **Source** | UCI Machine Learning Repository + Kaggle (CC BY 4.0) |
| **URL** | https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i |
| **Volume** | ~1.7 million completed taxi trips |
| **File size** | ~1.5 GB compressed CSV |
| **Time period** | July 2013 – June 2014 (12 months) |
| **Fleet size** | 442 taxis operating in Porto, Portugal |
| **Format** | CSV (Comma Separated Values) |

### Every Column Explained

#### 1. `TRIP_ID` — Trip Identifier
- **Type:** Long integer (e.g., `1372636858620000589`)
- **What it is:** A unique identifier for each trip. Every trip in the dataset has a different TRIP_ID.
- **How it's generated:** Composite of the timestamp and taxi ID.
- **In TaaSim:** Used to track individual trips through the pipeline.

#### 2. `CALL_TYPE` — How the Trip Was Requested
- **Type:** String, one character (`A`, `B`, or `C`)
- **Values:**

| Value | Meaning | Real Casablanca equivalent |
|-------|---------|---------------------------|
| `A` | **Central dispatch** — Customer called the taxi company, dispatcher assigned a taxi | A ride-hailing app sending a request |
| `B` | **Taxi stand** — Customer went to a designated taxi stand and took the next available taxi | Grand taxi stations (like Oulfa, Hay Hassani stands) |
| `C` | **Street hail** — Customer flagged a taxi from the sidewalk | Petits taxis flagged on Boulevard Mohammed V |

- **Why this matters:** This mirrors exactly how Casablanca works! Call type distribution tells us the demand mix (app vs stand vs street).
- **Distribution in our data:** A = 364,769 trips, B = 817,878 trips, C = 528,013 trips

#### 3. `ORIGIN_CALL` — Customer Phone ID
- **Type:** Integer (nullable)
- **What it is:** A unique identifier for the customer who called the dispatch center.
- **Only present when:** `CALL_TYPE = A` (central dispatch). For B and C, this is NULL.
- **In TaaSim:** Could be used to model repeat customer behavior.

#### 4. `ORIGIN_STAND` — Taxi Stand ID
- **Type:** Integer (nullable)
- **What it is:** The identifier of the taxi stand where the trip started.
- **Only present when:** `CALL_TYPE = B` (taxi stand). For A and C, this is NULL.
- **In TaaSim:** Maps to Casablanca taxi stand locations.

#### 5. `TAXI_ID` — Vehicle Identifier
- **Type:** Integer (e.g., `20000589`)
- **What it is:** A unique identifier for the taxi that completed this trip. 442 unique taxis in fleet.
- **In TaaSim:** Used in the GPS producer to simulate individual vehicle movements.

#### 6. `TIMESTAMP` — Trip Start Time
- **Type:** Integer (Unix timestamp — seconds since January 1, 1970)
- **Example:** `1372636858` = July 1, 2013 at 00:00:58 UTC
- **What it is:** The exact moment when the trip started (meter turned on).
- **How to convert:** In Python: `datetime.fromtimestamp(1372636858)` → `2013-07-01 00:00:58`
- **In TaaSim:** We extract `hour_of_day`, `day_of_week` from this to build the demand curve.

#### 7. `DAY_TYPE` — Type of Day
- **Type:** String, one character (`A`, `B`, or `C`)
- **Values:**

| Value | Meaning |
|-------|---------|
| `A` | **Normal day** (weekday or regular weekend) |
| `B` | **Public holiday** |
| `C` | **Day before a holiday** (pre-holiday) |

- **In our data:** All 1,710,660 clean rows are type `A` — meaning no holidays appear in the filtered data.
- **In TaaSim:** Used as a feature for the demand forecasting ML model.

#### 8. `MISSING_DATA` — Data Quality Flag
- **Type:** Boolean (`True` / `False`)
- **What it is:** `True` means the GPS polyline is incomplete (the taxi lost signal at some point during the trip).
- **In TaaSim:** We **filter out** all rows where `MISSING_DATA = True`. This removes ~10 bad rows out of 1.7M.

#### 9. `POLYLINE` — The GPS Trajectory ⭐ (Most Important Column)
- **Type:** String (JSON array of coordinate pairs)
- **Example:** `[[-8.618643,41.141412],[-8.618499,41.141376],[-8.620326,41.14251]]`
- **What it is:** The complete GPS trajectory of the trip. Each `[longitude, latitude]` pair is a GPS point recorded every **15 seconds**.
- **How to calculate trip duration:** Count the number of points × 15 seconds. Example: 20 points = 20 × 15 = 300 seconds = 5 minutes.
- **Empty polyline `[]`:** Means no GPS data was recorded.
- **In TaaSim:**
  - We parse this JSON with `json.loads()` to extract coordinates
  - We **remap** each coordinate from Porto's bounding box to Casablanca's bounding box
  - The GPS producer replays these polylines at 10× speed through Kafka

### Porto Bounding Box (used for coordinate remapping):
```
Porto:       Longitude [-8.69, -8.56]   Latitude [41.14, 41.20]
Casablanca:  Longitude [-7.80, -7.40]   Latitude [33.40, 33.70]
```

---

## 4.2 NYC Yellow Taxi Trip Records (Parquet)

### Overview
| Property | Value |
|----------|-------|
| **Full name** | NYC Taxi & Limousine Commission Trip Record Data |
| **Source** | NYC Open Data / TLC (Public Domain) |
| **URL** | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| **Volume** | ~10M rows per month |
| **Files we downloaded** | January 2024, February 2024, March 2024 |
| **Our total rows** | ~9.55 million trips |
| **File size** | ~150–200 MB per month (Parquet format) |
| **Format** | Apache Parquet (columnar, compressed, schema-embedded) |

### Why Parquet and not CSV?
| Feature | CSV | Parquet |
|---------|-----|--------|
| Format | Text-based, row-oriented | Binary, columnar |
| Compression | None or external | Built-in (Snappy/Gzip) |
| Schema | No — must be inferred | Embedded in the file |
| Read speed | Slow (reads ALL columns) | Fast (reads only columns you need) |
| File size | Large | 5-10× smaller than CSV |
| Used by | Excel, simple scripts | Spark, Flink, big data tools |

### Every Column Explained

#### 1. `VendorID` — TPEP Technology Provider
- **Type:** Integer (`1` or `2`)
- **Values:** `1` = Creative Mobile Technologies, LLC; `2` = VeriFone Inc.
- **What it is:** Which company's taximeter hardware recorded this trip. NYC has two approved TPEP (Taxicab Passenger Enhancement Program) vendors.
- **In TaaSim:** Not directly used — informational only.

#### 2. `tpep_pickup_datetime` — Pickup Time ⭐
- **Type:** Timestamp (e.g., `2024-01-15 08:23:45`)
- **What it is:** The exact date and time when the meter was engaged (passenger got in, trip started).
- **In TaaSim:** This is the critical column. We extract `hour_of_day`, `day_of_week` from this to compute hourly demand per zone.
- **⚠️ Data quality issue:** Some rows have timestamps from 2002, 2008, 2009 — these are errors. We filter to keep only `>= 2024-01-01`.

#### 3. `tpep_dropoff_datetime` — Dropoff Time
- **Type:** Timestamp (e.g., `2024-01-15 08:41:12`)
- **What it is:** The exact date and time when the meter was disengaged (passenger got out, trip ended).
- **In TaaSim:** Used to calculate trip duration: `dropoff - pickup = duration`.

#### 4. `passenger_count` — Number of Passengers
- **Type:** Integer (e.g., `1`, `2`, `3`)
- **What it is:** The number of passengers in the vehicle. This is a driver-entered value, so it may not always be accurate.
- **In TaaSim:** Can be used to study shared ride potential. Most NYC trips have 1 passenger.

#### 5. `trip_distance` — Distance in Miles
- **Type:** Float (e.g., `3.20`)
- **What it is:** The elapsed trip distance in miles, as reported by the taximeter.
- **In TaaSim:** Used to compute average trip distance KPIs.

#### 6. `RatecodeID` — Rate Code
- **Type:** Integer
- **Values:**

| Value | Meaning |
|-------|---------|
| `1` | Standard rate |
| `2` | JFK Airport |
| `3` | Newark Airport |
| `4` | Nassau or Westchester |
| `5` | Negotiated fare |
| `6` | Group ride |
| `99` | Unknown |

- **In TaaSim:** Can identify airport trips vs city trips.

#### 7. `store_and_fwd_flag` — Store and Forward
- **Type:** String (`Y` or `N`)
- **What it is:** `Y` means the trip record was held in the taximeter's memory because the vehicle had no connection to the server, and was sent later. `N` means it was transmitted immediately.
- **In TaaSim:** Demonstrates the real-world problem of late-arriving data (same problem our Flink watermarks solve!).

#### 8. `PULocationID` — Pickup Location Zone ⭐
- **Type:** Integer (e.g., `132`, `236`)
- **What it is:** The TLC Taxi Zone where the passenger was picked up. NYC is divided into 263 taxi zones (not GPS coordinates — already anonymized to zones).
- **In TaaSim:** This is our spatial dimension for demand aggregation. We group trips by `PULocationID` to count demand per zone per hour.

#### 9. `DOLocationID` — Dropoff Location Zone
- **Type:** Integer (e.g., `79`, `170`)
- **What it is:** The TLC Taxi Zone where the passenger was dropped off.
- **In TaaSim:** Combined with PULocationID, this gives us origin-destination (OD) pairs.

#### 10. `payment_type` — How the Passenger Paid
- **Type:** Integer
- **Values:**

| Value | Meaning |
|-------|---------|
| `0` | Flex Fare trip |
| `1` | Credit card |
| `2` | Cash |
| `3` | No charge |
| `4` | Dispute |
| `5` | Unknown |
| `6` | Voided trip |

- **In TaaSim:** Can be used to analyze payment trends.

#### 11. `fare_amount` — Meter Fare
- **Type:** Float (e.g., `15.50`)
- **What it is:** The time-and-distance fare calculated by the taximeter (in USD).
- **In TaaSim:** Used for revenue analytics and KPI computation.

#### 12. `extra` — Surcharges
- **Type:** Float (e.g., `1.00`)
- **What it is:** Miscellaneous extras and surcharges. Includes the $0.50 rush hour surcharge and $1.00 overnight surcharge.

#### 13. `mta_tax` — MTA Tax
- **Type:** Float (usually `0.50`)
- **What it is:** The $0.50 MTA tax automatically triggered based on the metered rate.

#### 14. `tip_amount` — Tip
- **Type:** Float (e.g., `3.20`)
- **What it is:** Tip amount. Automatically populated for credit card tips. Cash tips are NOT included in this field.

#### 15. `tolls_amount` — Tolls
- **Type:** Float (e.g., `0.00` or `6.55`)
- **What it is:** Total amount of all tolls paid during the trip (bridges, tunnels).

#### 16. `improvement_surcharge` — Improvement Surcharge
- **Type:** Float (usually `0.30`)
- **What it is:** A $0.30 improvement surcharge assessed at the flag drop (when the meter starts).

#### 17. `total_amount` — Total Charged
- **Type:** Float (e.g., `22.30`)
- **What it is:** The total amount charged to the passenger. Equals `fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge`.
- **Note:** Does NOT include cash tips.

#### 18. `congestion_surcharge` — Congestion Surcharge
- **Type:** Float (e.g., `2.50`)
- **What it is:** The NYS congestion surcharge for trips in lower Manhattan.

#### 19. `airport_fee` — Airport Fee
- **Type:** Float (e.g., `1.75`)
- **What it is:** Fee for pickups at LaGuardia and JFK airports.

---

# PART 5: WHAT WE DID IN WEEK 1 — DETAILED SUMMARY

## 5.1 Deliverable Checklist (from PDF)

| # | Required Deliverable | Status |
|---|---------------------|--------|
| 1 | ✅ Docker stack running (screenshot) | **DONE** — All 9 containers running |
| 2 | ✅ Jupyter data profiling notebook | **DONE** — `week1_exploration.ipynb` |
| 3 | ✅ Zone-remapped trips visualized on Casablanca map | **DONE** — Folium map with 30 trips |
| 4 | ✅ Data uploaded to MinIO | **DONE** — Synced to `raw/porto-trips/` and `raw/nyc-tlc/` |
| 5 | ⬜ Team name + 1-slide startup concept | **Pending your choice!** |
| 6 | ⬜ Kafka producers running + verified | **Next in Week 2** |

## 5.2 Work Done Step by Step

### Step 1: Docker Infrastructure Setup ✅
- Created `docker-compose.yml` with all 9+ services
- Successfully launched: Kafka, MinIO, Cassandra, Flink, Spark, Jupyter, Grafana
- All containers healthy and accessible on their respective ports

### Step 2: Porto Dataset Loading & Profiling ✅
- Downloaded `train.csv` (1.7M trips) from Kaggle
- Loaded into PySpark via `spark.read.csv()`
- **Schema discovery:** 9 columns (TRIP_ID, CALL_TYPE, ORIGIN_CALL, ORIGIN_STAND, TAXI_ID, TIMESTAMP, DAY_TYPE, MISSING_DATA, POLYLINE)
- **Cleaned:** Filtered out `MISSING_DATA = True` rows → 1,710,660 clean rows
- **Analyzed call type breakdown:**
  - A (Central dispatch): 364,769
  - B (Taxi stand): 817,878
  - C (Street hail): 528,013

### Step 3: Trip Duration Analysis ✅
- Created a UDF (`count_points`) to parse POLYLINE JSON and count GPS points
- Duration = number of points × 15 seconds ÷ 60 = minutes
- Generated a histogram showing most Porto taxi trips last 5–15 minutes
- **Bug fixed:** `import json` had to be placed INSIDE the UDF function body because PySpark serializes UDFs to worker nodes

### Step 4: Temporal Demand Curve ✅
- Extracted `hour_of_day` from TIMESTAMP
- Plotted bar chart: "Porto Taxi Demand by Hour of Day"
- **Key finding:** Peak demand at 8–9am (morning rush) and 5–7pm (evening rush) — exactly matching real-world patterns
- This demand curve drives our `trip_request_producer.py` simulator

### Step 5: Geospatial Transformation — Porto → Casablanca ✅
- Defined bounding boxes for both cities
- Applied linear normalization formula:
  ```
  lon_normalized = (porto_lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
  casablanca_lon = CASA_LON_MIN + lon_normalized × (CASA_LON_MAX - CASA_LON_MIN)
  ```
- Created a UDF (`remap_polyline`) to transform every GPS point
- Applied to 10,000 sample trips and verified on a Folium OpenStreetMap map
- **Result:** Taxi routes are now correctly overlaid on Casablanca streets

### Step 6: Save Transformed Data ✅
- Wrote the full transformed Porto dataset to Parquet format
- **Bug fixed:** Spark's `.mode('overwrite')` fails on Docker-mounted Windows volumes. Solution: manually delete the output directory with `shutil.rmtree()` before writing

### Step 7: NYC Yellow Taxi Data & MinIO Sync ✅
- Downloaded 3 months of NYC Yellow Taxi Parquet files (Jan–Mar 2024)
- Uploaded (synced) `train.csv` + NYC Parquet files via the MinIO `mc` client to their proper buckets:
  - `local/raw/porto-trips/`
  - `local/raw/nyc-tlc/`
- Loaded NYC files with Spark (`spark.read.parquet('*.parquet')`) — **9,554,778 trips**
- Computed hourly demand per zone (`PULocationID` × `hour`) and saved aggregated demand.

---

# PART 6: WHAT COMES NEXT (Weeks 2–8 Preview)

| Week | Theme | Key Tasks |
|------|-------|-----------|
| **W2** | Storage Design | Upload data to MinIO, configure Kafka S3 Sink, create Cassandra schema |
| **W3** | Streaming I — GPS | Implement Flink Job 1 (GPS Normalizer), watermarks, Grafana live map |
| **W4** | Streaming II — Matching | Flink Jobs 2 & 3 (demand aggregation + trip matching), end-to-end flow |
| **W5** | Batch ETL + Analytics | Full Spark ETL on Porto + NYC, KPI dashboard in Grafana |
| **W6** | ML — Forecasting | Feature engineering, train GBTRegressor, FastAPI prediction endpoint |
| **W7** | Security + Integration | JWT auth, GPS anonymization, full integration test, SLA measurement |
| **W8** | Demo + Investor Pitch | Polish Grafana, live demo with event injection, 10-slide pitch, technical report |

---

*Document generated for TaaSim Week 1 presentation preparation.*
