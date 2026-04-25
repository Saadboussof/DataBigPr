# TaaSim — Complete Data Pipeline: From Raw Data to Live Demo
## Every Step Explained in Detail

> This document traces the journey of every piece of data in the TaaSim platform, from the moment a taxi sends a GPS ping to the moment the professor sees your Grafana dashboard turn red during the live demo.

---

# Table of Contents
1. [Kafka Topics — The Mailboxes](#1-kafka-topics--the-mailboxes)
2. [Phase 1 — Data Producers (Weeks 1-2)](#2-phase-1--data-producers-weeks-1-2)
3. [Phase 2 — Real-Time Stream Processing with Flink (Weeks 3-4)](#3-phase-2--real-time-stream-processing-with-flink-weeks-3-4)
4. [Phase 3 — Batch Analytics with Spark (Week 5)](#4-phase-3--batch-analytics-with-spark-week-5)
5. [Phase 4 — Machine Learning Demand Forecasting (Week 6)](#5-phase-4--machine-learning-demand-forecasting-week-6)
6. [Phase 5 — REST API & Security (Week 7)](#6-phase-5--rest-api--security-week-7)
7. [Phase 6 — Live Demo & Investor Pitch (Week 8)](#7-phase-6--live-demo--investor-pitch-week-8)
8. [Complete Data Flow Summary](#8-complete-data-flow-summary)

---

# 1. Kafka Topics — The Mailboxes

Apache Kafka is the **central nervous system** of TaaSim. Think of it as a post office with labeled mailboxes called **topics**. Programs send messages to a topic (producers), and other programs read from that topic (consumers).

TaaSim uses **5 Kafka topics**:

| Topic Name | Purpose | Who Writes | Who Reads |
|------------|---------|-----------|-----------|
| `raw.gps` | Live GPS coordinates from every taxi | `vehicle_gps_producer.py` | Flink Job 1, Kafka Connect |
| `raw.trips` | Ride requests from passengers | `trip_request_producer.py`, `event_injector.py` | Flink Job 2, Flink Job 3, Kafka Connect |
| `processed.gps` | Cleaned, validated GPS (after Flink processing) | Flink Job 1 | Flink Job 2 |
| `processed.demand` | Demand aggregates per zone per 30-second window | Flink Job 2 | Grafana (via Cassandra) |
| `processed.matches` | Trip match results (rider assigned to taxi) | Flink Job 3 | FastAPI |

### What a message in `raw.gps` looks like:
```json
{
  "taxi_id": "20000589",
  "timestamp": 1713600000000,
  "lat": 33.5521,
  "lon": -7.6234,
  "speed": 42.5,
  "status": "engaged"
}
```
**Translation:** *"I am Taxi 589. At this exact moment, I am at latitude 33.55, longitude -7.62 in Casablanca. I'm going 42.5 km/h and I currently have a passenger (engaged)."*

### What a message in `raw.trips` looks like:
```json
{
  "trip_id": "a3f8c2d1-4e5f-4a2b-8c9d-1234567890ab",
  "rider_id": "rider_4521",
  "origin_zone": 7,
  "destination_zone": 12,
  "requested_at": 1713600500000,
  "call_type": "A"
}
```
**Translation:** *"I am Rider 4521. I need a taxi. I'm in Zone 7 (Hay Hassani) and I want to go to Zone 12 (Ain Chock). I booked through the app (call_type A)."*

### Why not send directly to the database?

Imagine 10,000 taxis sending GPS coordinates at the exact same second. If every taxi writes directly to Cassandra, the database crashes under the load. Kafka acts as a **shock absorber**:
- It receives ALL messages instantly (it can handle millions per second)
- It stores them safely on disk (even if consumers are slow)
- Consumers (Flink) read at their own pace
- If Flink crashes and restarts, it picks up where it left off — no data lost

---

# 2. Phase 1 — Data Producers (Weeks 1-2)

## 2.1 `vehicle_gps_producer.py` — The Taxi Fleet Simulator

### What it does, step by step:

1. **Reads** the Porto `train.csv` file (or from MinIO `s3a://raw/porto/train.csv`)
2. **Selects** 1,000 random taxi trips from the dataset
3. **Parses** each trip's `POLYLINE` JSON — this is an array of `[longitude, latitude]` GPS points recorded every 15 seconds in Porto
4. **Transforms** each Porto coordinate to Casablanca using a bounding box formula:
   ```
   casablanca_lon = -7.80 + normalized_porto_lon × (-7.40 - (-7.80))
   casablanca_lat = 33.40 + normalized_porto_lat × (33.70 - 33.40)
   ```
5. **Adds GPS noise:** ±20 meters of random drift (simulating real GPS inaccuracy)
6. **Clips to land:** Uses a coastline polygon to ensure no taxi ends up in the Atlantic Ocean
7. **Simulates blackouts:** 5% chance per point that the GPS signal drops (taxi goes through a tunnel)
8. **Sends** the coordinate as a JSON message to Kafka topic `raw.gps`
9. **Sleeps** 1.5 seconds (simulating 15 seconds of real time at 10x speed)
10. **Loops** — when all trips finish, it restarts from the beginning

### Why the blackouts and noise matter:
The professor deliberately designed this! When you build Flink Job 1 in Week 3, you MUST handle:
- **Late-arriving data:** GPS pings that arrive 3 minutes late due to network delay
- **Missing data:** Blackout gaps where a taxi disappears for 60 seconds
- **Duplicate data:** The same coordinate arriving twice due to network retry

If you use "processing time" (when the server receives the ping) instead of "event time" (when the taxi actually sent the ping), your demand calculations will be **measurably wrong**. The professor will verify this during evaluation.

---

## 2.2 `trip_request_producer.py` — The Passenger App Simulator

### What it does, step by step:

1. **Checks** the current hour of the day
2. **Calculates** a demand multiplier based on the time:
   - 7-9 AM (morning rush): 3x-5x more requests
   - 5-7 PM (evening rush): 3x-5x more requests
   - Other hours: 0.5x-1.5x (quiet)
3. **Generates** ride requests: random origin zone (1-16), random destination zone (different from origin), random rider ID, random call type (A/B/C)
4. **Sends** each request to Kafka topic `raw.trips`
5. **Base rate:** 2 requests per second × multiplier = during rush hour, up to 10 requests per second

### Why the demand curve matters:
This creates realistic traffic patterns. Zone 7 at 8 AM has 10 ride requests per second. Zone 7 at 3 AM has 1 request per second. Your Flink demand aggregator and ML model must detect and predict these patterns.

---

## 2.3 `event_injector.py` — The Chaos Generator

### What it does:
This script is your **demo superpower** for Week 8. You run it manually during the live presentation to trigger dramatic events:

**Demand Spike (stadium exit):**
```bash
python event_injector.py --type spike --zone 7 --factor 5 --duration 5
```
This floods Zone 7 with 25 requests/second for 5 minutes — simulating 50,000 football fans leaving a stadium and all wanting taxis at once.

**Rain Event:**
```bash
python event_injector.py --type rain --factor 1.4 --duration 10
```
Increases global demand across ALL zones by 40% for 10 minutes — simulating a sudden rainstorm where everyone wants a taxi instead of walking.

---

## 2.4 Kafka Connect S3 Sink (Week 2)

### What it does:
Kafka Connect is a separate container that automatically **copies** every message from Kafka topics into MinIO as JSON files. It runs continuously in the background.

### The flow:
```
Kafka [raw.gps]    ──► Kafka Connect ──► MinIO s3a://kafka-archive/raw.gps/
Kafka [raw.trips]  ──► Kafka Connect ──► MinIO s3a://kafka-archive/raw.trips/
```

### Why this matters:
- **Backup:** If Kafka's 7-day retention expires, the data is still safe in MinIO
- **Replay:** You can re-read historical data from MinIO and push it back through Kafka
- **ML Training:** Spark can read from `kafka-archive/` to build training datasets for the ML model

---

# 3. Phase 2 — Real-Time Stream Processing with Flink (Weeks 3-4)

This is the **heart of the project**. Three Flink jobs run 24/7, continuously processing the live stream of events.

## 3.1 Flink Job 1: GPS Normalizer (Week 3)

### Input: Kafka topic `raw.gps`
### Output: Cassandra table `vehicle_positions` + Kafka topic `processed.gps`

### What it does, step by step:

1. **Reads** GPS events from `raw.gps` in real-time
2. **Assigns event-time watermark** with 3-minute allowed lateness:
   - "I'll wait up to 3 minutes for late GPS pings before considering them too old"
   - This handles taxis that went through tunnels and sent delayed pings
3. **Validates coordinates:**
   - Is the latitude between 33.40 and 33.70? (Casablanca bounds)
   - Is the longitude between -7.80 and -7.40?
   - If not → discard the event
4. **Removes duplicates:**
   - Taxis ping every 4 seconds. GPS noise can create near-identical coordinates
   - Flink uses **stateful deduplication** — it remembers the last position of each taxi and ignores pings that are too close
5. **Maps to Casablanca zone (1-16):**
   - Takes the GPS coordinate and determines which of the 16 arrondissements the taxi is in
   - This is a geospatial lookup using the zone mapping table
6. **Anonymizes GPS:**
   - Snaps the exact lat/lon to the zone centroid (center point)
   - Raw GPS coordinates are NEVER stored in Cassandra (privacy requirement)
7. **Writes to Cassandra `vehicle_positions`:**
   ```
   INSERT INTO taasim.vehicle_positions 
   (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
   VALUES ('casablanca', 7, '2026-04-20 08:15:30', '589', 33.55, -7.62, 42.5, 'engaged')
   ```
8. **Writes to Kafka `processed.gps`** — clean, validated GPS for Job 2 to consume

### What the professor tests:
- Run the GPS producer with intentionally late events (3-minute delay)
- Check Cassandra: the late events should appear correctly ordered by event_time
- If you used processing-time instead of event-time, the order would be wrong → fail

---

## 3.2 Flink Job 2: Demand Aggregator (Week 4)

### Input: Kafka topics `processed.gps` + `raw.trips`
### Output: Cassandra table `demand_zones` + Kafka topic `processed.demand`

### What it does, step by step:

1. **Creates a 30-second tumbling window** for each of the 16 zones
   - "Every 30 seconds, count what's happening in each zone"
2. **From `processed.gps`:** Counts how many active taxis are in each zone = **SUPPLY**
3. **From `raw.trips`:** Counts how many ride requests came from each zone = **DEMAND**
4. **Calculates the ratio:** `supply / demand`
   - ratio > 1.0 → plenty of taxis (green on heatmap)
   - ratio < 1.0 → not enough taxis (red on heatmap)
   - ratio = 0.0 → demand exists but ZERO taxis available (critical!)
5. **Writes to Cassandra `demand_zones` every 30 seconds:**
   ```
   INSERT INTO taasim.demand_zones 
   (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecast_demand)
   VALUES ('casablanca', 7, '2026-04-20 08:15:00', 5, 12, 0.41, NULL)
   ```
6. **Writes to Kafka `processed.demand`** so other systems can react

### What "30-second tumbling window" means:
```
Time:   08:15:00 ────────── 08:15:30 ────────── 08:16:00
        [    Window 1     ] [    Window 2     ] [    Window 3    ]
        Count everything    Count everything    Count everything
        in this 30 sec      in this 30 sec      in this 30 sec
```
Each window is independent. No overlap. Every 30 seconds, a fresh count is emitted.

---

## 3.3 Flink Job 3: Trip Matcher (Week 4)

### Input: Kafka topic `raw.trips` + Cassandra `vehicle_positions`
### Output: Cassandra table `trips` + Kafka topic `processed.matches`

### What it does, step by step:

1. **Listens** for new trip requests on `raw.trips`
2. **On each request:** "Rider 4521 wants a taxi in Zone 7"
3. **Queries Cassandra `vehicle_positions`:** "Which taxis are in Zone 7 right now with status = available?"
4. **Finds the nearest one:** Taxi 589 is 400 meters away, Taxi 203 is 1.2 km away → Pick Taxi 589
5. **Fallback rule:** If NO taxi is in Zone 7, expand search to adjacent zones (Zone 6, Zone 8). Wait up to 5 seconds before giving up.
6. **Computes ETA:** Simple calculation: distance ÷ average speed (e.g., 400m ÷ 30 km/h = ~48 seconds)
7. **Writes match to Cassandra `trips`:**
   ```
   INSERT INTO taasim.trips 
   (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare)
   VALUES ('casablanca', '2026-04-20', NOW(), 'abc123', 'rider_4521', '589', 7, 12, 'MATCHED', NULL)
   ```
8. **Emits match event to Kafka `processed.matches`:**
   ```json
   {
     "trip_id": "abc123",
     "taxi_id": "589",
     "rider_id": "rider_4521",
     "eta_seconds": 48,
     "status": "MATCHED"
   }
   ```

### Performance requirement:
The entire flow (request arrives → match written to Cassandra) must complete in **< 5 seconds** at P95. The professor measures this.

---

# 4. Phase 3 — Batch Analytics with Spark (Week 5)

While Flink handles the real-time stream, Spark handles the **offline, historical, heavy-lifting** analytics.

## What Spark does:

### ETL Job 1: Porto Data
```
MinIO [raw/porto/train.csv]
    │
    ▼ spark.read.csv()
    │
    │ Filter MISSING_DATA = True
    │ Apply zone remapping (Porto → Casablanca)
    │ Clip ocean trips using coastline polygon
    │ Compute trip duration (count GPS points × 15 sec)
    │ Extract hour_of_day, day_of_week from TIMESTAMP
    │
    ▼
MinIO [curated/trips/porto_casablanca.parquet]
```

### ETL Job 2: NYC Data
```
MinIO [raw/nyc/yellow_tripdata_2024-*.parquet]
    │
    ▼ spark.read.parquet()
    │
    │ Filter timestamps >= 2024-01-01 (remove bad rows from 2002/2008)
    │ Group by PULocationID + hour → count = hourly_demand
    │ Compute KPIs per zone:
    │   - Total trips per week
    │   - Average trip duration
    │   - Average trip distance
    │   - Peak demand hours
    │   - Coverage gaps (zones with demand but < 2 taxis)
    │
    ▼
MinIO [curated/demand/nyc_demand_by_zone.parquet]
    │
    ▼ Load KPI aggregates into Cassandra demand_zones
    │
    ▼ Grafana KPI PANELS 📊
      "Peak demand: Zone 7 at 8:15 AM, 47 requests"
      "Average trip: 12 min, 4.2 km"
      "Coverage gap: Zone 14 — 8 requests, 0 taxis"
```

### Why both Porto AND NYC?
| Dataset | Purpose |
|---------|---------|
| **Porto** | Realistic trip trajectories (GPS polylines) for streaming simulation and geospatial work |
| **NYC** | Massive scale (10M rows/month) to force real Spark optimization (partitioning, broadcast joins) |

The professor WANTS you to struggle with the NYC dataset. If your Spark job takes 30 minutes instead of 5 minutes, you haven't optimized your partitioning.

---

# 5. Phase 4 — Machine Learning Demand Forecasting (Week 6)

## The Prediction Problem:
> "Given a zone, a day of the week, and an hour of the day, how many taxi requests will arrive in the next 30 minutes?"

### Feature Engineering (what the model learns from):

| Feature Group | Features | How They're Created |
|--------------|----------|-------------------|
| **Temporal** | `hour_of_day` (0-23), `day_of_week` (0-6), `is_weekend`, `is_friday` | Extracted from TIMESTAMP |
| **Spatial** | `zone_id` (1-16), `zone_population_density`, `zone_type` (residential/commercial/transit_hub) | Joined from zone reference table |
| **Weather** | `is_raining` (binary), `temperature_bucket` (cold/mild/hot) | Joined from Open-Meteo historical API |
| **Lag** | `demand_lag_1d` (same hour yesterday), `demand_lag_7d` (same hour last week), `rolling_7d_mean` | Window functions over trip history |

### Training Pipeline:
```
MinIO [curated/trips/] ──► Spark Feature Engineering
                                │
                                │ Compute ALL features above
                                │ Write feature matrix to MinIO
                                │
                                ▼
                           MinIO [ml/features/]
                                │
                                │ Split: first 10 months = TRAIN, last 2 months = TEST
                                │ (Temporal split — no cheating with future data!)
                                │
                                │ Train GBTRegressor (Gradient Boosted Trees)
                                │   maxDepth = 5
                                │   maxIter = 50
                                │   CrossValidator with 2 hyperparameters
                                │
                                ▼
                           Evaluate on test set:
                           - RMSE (Root Mean Square Error)
                           - MAE (Mean Absolute Error)
                           - Compare vs NAIVE BASELINE:
                             "Predict same demand as 7 days ago"
                           
                           YOUR MODEL MUST BEAT THE BASELINE!
                           If RMSE_model > RMSE_baseline → FAIL
                                │
                                ▼
                           MinIO [ml/models/demand_v1/]
                           (Saved PipelineModel artifact)
```

### Feature Importance Chart (required):
After training, you must produce a bar chart showing which features matter most. Example:
- `hour_of_day`: 35% importance (the hour matters most!)
- `demand_lag_7d`: 22% importance (last week's pattern repeats)
- `is_friday`: 15% importance (Friday night is special in Casablanca)

The professor will ask: *"What drives demand in Casablanca? Explain your top 3 predictors."*

---

# 6. Phase 5 — REST API & Security (Week 7)

## FastAPI Endpoints:

### `POST /api/trips/reserve` — Book a Ride
```json
// Request:
{
  "origin_zone": 7,
  "destination_zone": 12
}

// Response:
{
  "trip_id": "abc123",
  "status": "PENDING",
  "message": "Your request has been submitted"
}
```
Behind the scenes: FastAPI publishes this to Kafka `raw.trips` → Flink matches it → rider gets notified.

### `GET /api/vehicles?zone=7` — Find Nearby Taxis
```json
// Response:
{
  "zone_id": 7,
  "vehicles": [
    {"taxi_id": "589", "lat": 33.55, "lon": -7.62, "status": "available", "eta": "2 min"},
    {"taxi_id": "203", "lat": 33.56, "lon": -7.61, "status": "available", "eta": "4 min"}
  ]
}
```
Behind the scenes: FastAPI queries Cassandra `vehicle_positions WHERE zone_id = 7`.

### `POST /api/demand/forecast` — Predict Future Demand
```json
// Request:
{
  "zone_id": 7,
  "datetime": "2026-04-20T09:00:00"
}

// Response:
{
  "zone_id": 7,
  "datetime": "2026-04-20T09:00:00",
  "predicted_demand": 47,
  "confidence": "high"
}
```
Behind the scenes: FastAPI loads the trained ML model from MinIO, extracts features from the input, and returns the prediction.

## Security (JWT Authentication):
- Two roles: `rider` (can reserve trips, view vehicles) and `admin` (full access including demand forecast)
- Every API call requires a JWT token in the `Authorization` header
- Library: `python-jose`

---

# 7. Phase 6 — Live Demo & Investor Pitch (Week 8)

## The Demo Script (what the professor sees):

### Act 1: "Normal Morning" (2 minutes)
- GPS producer is running → Grafana shows taxis moving on the Casablanca map
- Trip producer is running → Grafana heatmap shows moderate demand
- Trip matches appear in the dashboard: "Trip abc123: Matched in 2.3 seconds"

### Act 2: "Stadium Exit" (3 minutes)
You run the event injector:
```bash
python event_injector.py --type spike --zone 7 --factor 5 --duration 3
```
- Zone 7 on the Grafana heatmap turns **BRIGHT RED** 🔴
- Demand spikes from 5 to 25 requests per second
- The system copes: matches still happen in < 5 seconds
- The ML forecast panel shows: "Predicted: 42 requests. Actual: 47 requests" (close!)

### Act 3: "Resilience Test" (2 minutes)
The professor asks: *"What if your streaming engine crashes?"*
- You manually kill the Flink TaskManager:
  ```bash
  docker stop taasim-flink-tm
  ```
- Flink restarts from checkpoint (saved to MinIO every 60 seconds)
- No GPS events are lost — they were stored in Kafka (7-day retention)
- Dashboard resumes within seconds

### Act 4: "Architecture Defense" (10 minutes Q&A)
Professor asks questions like:
- *"Why Kappa and not Lambda?"* → Single pipeline, simpler, Kafka as system of record
- *"Why (city, zone_id) as Cassandra partition key?"* → Because the query is "all taxis in zone X", not "all trips by taxi Y"
- *"Why `date_bucket` in trips table?"* → Prevent unbounded partition growth
- *"What if a GPS event arrives 5 minutes late?"* → Watermark allows 3 minutes, events later than 3 minutes are dropped

---

# 8. Complete Data Flow Summary

## Visual: The Full Circle

```
  ┌─────────────── PRODUCERS ───────────────┐
  │                                          │
  │  🚕 vehicle_gps_producer.py              │
  │     Reads Porto CSV/MinIO                │
  │     Remaps GPS to Casablanca             │
  │     Adds noise + blackouts               │
  │     Sends to Kafka [raw.gps]             │
  │                                          │
  │  📱 trip_request_producer.py             │
  │     Follows demand curve (peaks 8am/6pm) │
  │     Sends to Kafka [raw.trips]           │
  │                                          │
  │  💥 event_injector.py                    │
  │     Demand spikes, rain events           │
  │     Sends to Kafka [raw.trips]           │
  └──────────────────┬───────────────────────┘
                     │
           ┌─────────▼──────────┐
           │   APACHE KAFKA      │
           │   raw.gps           │
           │   raw.trips         │
           │   processed.gps     │
           │   processed.demand  │
           │   processed.matches │
           └──┬──────┬───────┬──┘
              │      │       │
   ┌──────────▼──┐   │   ┌──▼──────────────────┐
   │ FLINK (3 jobs) │   │   │ KAFKA CONNECT        │
   │                │   │   │ Copies raw.gps +     │
   │ Job 1: GPS     │   │   │ raw.trips to MinIO   │
   │   Normalize    │   │   │ kafka-archive/       │
   │                │   │   └──────────────────────┘
   │ Job 2: Demand  │   │
   │   Aggregate    │   │
   │   (30s window) │   │
   │                │   │
   │ Job 3: Trip    │   │
   │   Matcher      │   │
   └──┬─────────┬───┘   │
      │         │        │
      ▼         │        ▼
┌──────────┐    │   ┌──────────────┐
│CASSANDRA │    │   │   MinIO (S3) │
│          │    │   │              │
│vehicle_  │    │   │ raw/         │
│positions │    │   │  porto/      │
│          │    │   │  nyc/        │
│trips     │    │   │              │
│          │    │   │ curated/     │
│demand_   │    │   │  trips/      │
│zones     │    │   │              │
└────┬─────┘    │   │ kafka-       │
     │          │   │  archive/    │
     │          │   │              │
     ▼          │   │ ml/          │
┌──────────┐    │   │  features/   │
│ GRAFANA  │    │   │  models/     │
│          │    │   └──────┬───────┘
│ Live Map │    │          │
│ Heatmap  │    │          │
│ KPI Panel│    │          ▼
│ ML Overlay│   │   ┌──────────────┐
└──────────┘    │   │SPARK (PySpark)│
                │   │              │
                │   │ ETL: clean   │
                │   │ data, remap  │
                │   │ Porto, agg   │
                │   │ NYC demand   │
                │   │              │
                │   │ ML: feature  │
                │   │ engineering, │
                │   │ train GBT,   │
                │   │ save model   │
                │   └──────┬───────┘
                │          │
                │          ▼
                │   ┌──────────────┐
                └──►│  FASTAPI     │
                    │              │
                    │ /trips/reserve│
                    │ /vehicles     │
                    │ /demand/      │
                    │  forecast     │
                    │              │
                    │ JWT Auth     │
                    │ (rider/admin)│
                    └──────────────┘
```

## Table: Every Step of the Data Journey

| Step | What Happens | Component | Data Store | When |
|------|-------------|-----------|-----------|------|
| 1 | Taxi sends GPS ping | GPS Producer → Kafka `raw.gps` | Kafka | Continuous (every 1.5s at 10x) |
| 2 | Rider requests a ride | Trip Producer → Kafka `raw.trips` | Kafka | Continuous (2-10 req/sec) |
| 3 | Raw events backed up | Kafka Connect → MinIO `kafka-archive/` | MinIO | Continuous |
| 4 | GPS cleaned & validated | Flink Job 1 → Cassandra `vehicle_positions` | Cassandra | Real-time (< 15 sec) |
| 5 | Demand counted per zone | Flink Job 2 → Cassandra `demand_zones` | Cassandra | Every 30 seconds |
| 6 | Rider matched to taxi | Flink Job 3 → Cassandra `trips` | Cassandra | Real-time (< 5 sec) |
| 7 | Live dashboard updates | Grafana ← reads Cassandra | Grafana | Continuous |
| 8 | Historical data crunched | Spark ETL → MinIO `curated/` | MinIO | Batch (offline) |
| 9 | ML model trained | Spark MLlib → MinIO `ml/models/` | MinIO | Batch (once) |
| 10 | Predictions served | FastAPI ← loads model from MinIO | FastAPI | On-demand (< 500ms) |
| 11 | Chaos injected for demo | Event Injector → Kafka `raw.trips` | Kafka | Manual (Week 8 demo) |

---

## The Cassandra Tables (What Gets Stored Permanently)

### `vehicle_positions` — Where Is Every Taxi Right Now?
```
┌────────────┬─────────┬─────────────────────┬──────────┬───────┬────────┬───────┬──────────┐
│ city       │ zone_id │ event_time          │ taxi_id  │ lat   │ lon    │ speed │ status   │
├────────────┼─────────┼─────────────────────┼──────────┼───────┼────────┼───────┼──────────┤
│ casablanca │ 7       │ 2026-04-20 08:15:30 │ 589      │ 33.55 │ -7.62  │ 42.5  │ engaged  │
│ casablanca │ 7       │ 2026-04-20 08:15:28 │ 203      │ 33.56 │ -7.61  │ 35.0  │ available│
│ casablanca │ 12      │ 2026-04-20 08:15:31 │ 417      │ 33.48 │ -7.55  │ 50.2  │ engaged  │
└────────────┴─────────┴─────────────────────┴──────────┴───────┴────────┴───────┴──────────┘
```
**Query pattern:** "Show me all taxis in Zone 7" → `SELECT * FROM vehicle_positions WHERE city='casablanca' AND zone_id=7`

### `demand_zones` — Supply vs Demand Per Zone
```
┌────────────┬─────────┬─────────────────────┬─────────────────┬──────────────────┬───────┬─────────────────┐
│ city       │ zone_id │ window_start        │ active_vehicles │ pending_requests │ ratio │ forecast_demand │
├────────────┼─────────┼─────────────────────┼─────────────────┼──────────────────┼───────┼─────────────────┤
│ casablanca │ 7       │ 2026-04-20 08:15:00 │ 5               │ 12               │ 0.41  │ 47              │
│ casablanca │ 3       │ 2026-04-20 08:15:00 │ 8               │ 2                │ 4.00  │ 3               │
│ casablanca │ 7       │ 2026-04-20 08:14:30 │ 6               │ 10               │ 0.60  │ 44              │
└────────────┴─────────┴─────────────────────┴─────────────────┴──────────────────┴───────┴─────────────────┘
```
**Query pattern:** "What's the current demand in Zone 7?" → `SELECT * FROM demand_zones WHERE city='casablanca' AND zone_id=7 LIMIT 1`

### `trips` — Every Trip Record
```
┌────────────┬─────────────┬─────────────────────┬─────────┬────────────┬──────────┬─────────────┬───────────┬─────────┬──────┐
│ city       │ date_bucket │ created_at          │ trip_id │ rider_id   │ taxi_id  │ origin_zone │ dest_zone │ status  │ fare │
├────────────┼─────────────┼─────────────────────┼─────────┼────────────┼──────────┼─────────────┼───────────┼─────────┼──────┤
│ casablanca │ 2026-04-20  │ 2026-04-20 08:15:32 │ abc123  │ rider_4521 │ 589      │ 7           │ 12        │ MATCHED │ NULL │
│ casablanca │ 2026-04-20  │ 2026-04-20 08:14:58 │ def456  │ rider_3312 │ 203      │ 3           │ 9         │ COMPLETED│ 45.0│
└────────────┴─────────────┴─────────────────────┴─────────┴────────────┴──────────┴─────────────┴───────────┴─────────┴──────┘
```
**Query pattern:** "Show all trips today" → `SELECT * FROM trips WHERE city='casablanca' AND date_bucket='2026-04-20'`

---

*This document is your complete reference for understanding the TaaSim data pipeline from end to end. Use it for your presentation, your technical report, and your Q&A defense in Week 8.*
