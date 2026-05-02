# 🗄️ TaaSim Data Architecture Explained: Topics & Tables

This document explains the exact role of every Kafka topic and Cassandra table in your project, and how data flows through your 3 Flink jobs.

---

## 🟢 1. The Kafka Topics (The Streaming Highways)

Kafka acts as the central nervous system. We have **5 distinct topics** separated into `raw` (coming directly from simulators) and `processed` (outputted by Flink).

### `raw.gps`
*   **What it is:** The live heartbeat of every taxi.
*   **Produced by:** `vehicle_gps_producer.py` (The Python Simulator).
*   **Consumed by:** Flink Job 1 (`gps_job.py`) & Archiver (MinIO).
*   **Contains:** Bare-bones coordinates: `{"taxi_id": 200043, "lat": 33.65, "lon": -7.60}`.

### `raw.trips`
*   **What it is:** Customers opening the app and requesting a ride. 
*   **Produced by:** `trip_request_producer.py`.
*   **Consumed by:** Flink Job 2 (Demand Aggregation) & Archiver (MinIO).
*   **Contains:** Ride requests: `{"rider_id": "user_49", "pickup_lat": 33.55, ...}`.

### `processed.gps`
*   **What it is:** Enriched GPS data. Instead of just latitude/longitude, this tells the system exactly what **zone** the car is in.
*   **Produced by:** Flink Job 1 (`gps_job.py`).
*   **Consumed by:** Flink Job 2 & Job 3.
*   **Contains:** `{"taxi_id": 200043, "zone_id": 15, "status": "available"}`.

### `processed.demand`
*   **What it is:** A 30-second windowed summary of supply vs demand.
*   **Produced by:** Flink Job 2.
*   **Consumed by:** Flink Job 3 (to trigger surge pricing or reject rides).
*   **Contains:** `{"zone_id": 15, "active_vehicles": 10, "pending_requests": 2, "ratio": 5.0}`.

### `processed.matches`
*   **What it is:** The final business decision. It links a specific rider to a specific taxi.
*   **Produced by:** Flink Job 3 (The Matching Engine).
*   **Consumed by:** External systems (like sending a notification to the driver's phone).
*   **Contains:** `{"rider_id": "user_49", "taxi_id": 200043, "fare": 45.50}`.

---

## 🔵 2. The Cassandra Tables (The Real-Time Database)

Cassandra stores the current "State of the World" so that dashboards (like Grafana) or mobile apps can query the data instantly.

### Table 1: `vehicle_positions`
*   **Written by:** Flink Job 1
*   **Purpose:** Operational tracking. If you want to draw a map showing where every taxi in Zone 15 is right now, you query this table. 
*   **Data Lifespan:** 24 hours (TTL). We don't need to keep live positions in the fast database forever; old data is safely stored in MinIO for historical use.

### Table 2: `trips`
*   **Written by:** Flink Job 3
*   **Purpose:** The ledger of all business transactions. It tracks the status of a ride from `PENDING` -> `MATCHED` -> `COMPLETED`. 
*   **Data Lifespan:** Permanent.

### Table 3: `demand_zones`
*   **Written by:** Flink Job 2
*   **Purpose:** Analytics and Surge Pricing. If the dashboard needs to draw a heatmap showing which areas of Casablanca are the busiest *right now*, it queries this table.
*   **Data Lifespan:** 7 days (TTL).

---

## 🚀 3. Summary of the Flow

1. A taxi moves $\\rightarrow$ `raw.gps` $\\rightarrow$ **Job 1** calculates the zone $\\rightarrow$ saves to `vehicle_positions` & outputs to `processed.gps`.
2. A user requests a ride $\\rightarrow$ `raw.trips` $\\rightarrow$ **Job 2** counts the requests vs available cars $\\rightarrow$ saves to `demand_zones` & outputs to `processed.demand`.
3. **Job 3** reads both `processed.gps` and `processed.demand` $\\rightarrow$ assigns the car to the user $\\rightarrow$ saves the final receipt to the `trips` table & outputs to `processed.matches`.
