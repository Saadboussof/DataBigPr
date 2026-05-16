# Big Data Architecture: Concepts & Workflows 

This document consolidates all the core data concepts of the TaaSim project. It explains exactly what the datasets are, how the simulators generate data, and how the dual-database storage system (MinIO vs Cassandra) works.

---

## 1. The Core Datasets: Why Porto and NYC?

Since there is no public open-source dataset for taxis in Casablanca, we use two of the most famous datasets in the world to "fake" our city and prove our Big Data capabilities.

### `train.csv` (The Porto Dataset)
* **What it is:** 1.7 million rides from 442 real taxis in Porto, Portugal in 2013.
* **What value it adds:** This dataset contains a column called `POLYLINE`. A polyline is a list of exact GPS coordinates (latitude and longitude) taken every 15 seconds telling us exactly how the car drove through the streets.
* **Why we need it:** If we just generated random GPS coordinates using Python, our taxis would teleport across buildings and oceans. By using Porto, we get realistic driving behavior (stopping at traffic lights, taking curves). Our `vehicle_gps_producer.py` simulator takes these Porto coordinates, mathematically shifts them onto the map of Casablanca, and then "replays" them live into our system.

### New York TLC Data (The NYC Parquet Files)
* **What it is:** 9.5 million taxi rides from New York City (yellow cabs) stored using the compressed `.parquet` format.
* **What value it adds:** Massive Volume. The NYC dataset is the ultimate test of Big Data. More importantly, NYC doesn't use exact GPS coordinates; it divides the city into 263 "Taxi Zones" (using `PULocationID` for Pickup, and `DOLocationID` for Dropoff).
* **Why we need it:** We use this dataset in Week 1 and Week 5 to prove to the professor that our Apache Spark clusters are highly optimized. If you run a data aggregation on 10 million NYC rows and it finishes in 2 minutes, you proved you know Big Data optimization. We also copy NYC's logic by dividing Casablanca up into 16 `zone_id`s.

---

## 2. The Data Generators (Simulators)

If we let our Python simulators run for exactly 24 hours, here is exactly how much data they blast into Apache Kafka:

1. **`trip_request_producer.py` (The Passengers)**
   * **Workflow:** It checks your computer's clock. If it's rush hour (7am-9am or 5pm-7pm), it generates up to **10 requests per second**. During normal hours, it generates **1 to 3 requests per second**.
   * **Volume:** Generates roughly **250,000 to 300,000 trips a day**.
   
2. **`vehicle_gps_producer.py` (The Taxis)**
   * **Workflow:** It takes 1,000 of the real Porto trips, translates them to Casablanca, and runs them at 10x real-life speed. It sends a heartbeat ping from the taxi every 1.5 seconds.
   * **Volume:** Blasts Kafka with **100 to 500 GPS pings per second**. This equals over **15 Million rows of data per day**. Standard databases would crash trying to handle this!

---

## 3. The Dual-Storage System (Cassandra vs MinIO)

Because the simulator generates 15 Million rows of heavy traffic data a day, we have to store it in two totally different places, treating the data entirely differently.

### A. The Live Engine: Apache Cassandra
Cassandra is our "Operational Database". It is a NoSQL engine designed for extreme speed (reading and writing in milliseconds).

* **The Goal:** It powers the Live Grafana map and the passenger's mobile app.
* **How it handles data:** It only cares about the *Present Moment* and the *Recent Past*.
* **The `cql` Schema:** We programmed Cassandra with a `time_to_live` (TTL) command. 
   * When Flink writes a Taxi's GPS ping into Cassandra, Cassandra holds it for 24 hours and then automatically self-destructs (deletes) the row.
   * By constantly deleting old data, Cassandra stays tiny, lightning-fast, and never gets overwhelmed.
* **The Partition Keys (`zone_id`):** We strictly organize the tables by `zone_id`. When the Trip Matcher searches for available taxis, it doesn't search the whole city (which takes too long); it only scans the shelf assigned to `Zone 7`, retrieving the results instantly.

### B. The Archive: MinIO
MinIO is our "Data Lake". It is essentially a private, unlimited Amazon S3 bucket. It is slow to fetch single records, but amazing at holding massive, heavy files.

* **The Goal:** It powers the long-term Machine Learning model. (A model needs to look at *years* of history to predict future demand). 
* **How it handles data:** MinIO is a hoarder. It never deletes anything. It stores everything permanently.
* **The Robot (`archiver.py`):** Because Kafka automatically deletes messages after 7 days, we wrote `archiver.py`. This script sits next to Kafka. Every time a message flies by, the Archiver catches it.

---

## 4. Deep Dive: How the Archiver Works 

If we look at the python code for `archiver.py`:
1. **The Loop:** It runs an infinite `while True:` loop listening to Kafka.
2. **The Batching:** It creates an empty list. It puts every single GPS ping into the list until the list has exactly **100 messages** (or 30 seconds have passed).
3. **The Flush:** Once it hits 100, it converts that list into a text file and permanently uploads it to MinIO inside the `kafka-archive/` folder.

#### What the MinIO JSON file looks like inside
If you download one of these files from MinIO, it is written in "JSON Lines" format (one JSON object exactly per line). It looks exactly like this:

**A file inside `kafka-archive/raw.trips/2026/04/20/batch_1713621.json`:**
```json
{"trip_id": "8a3b21-4f9e", "rider_id": "rider_1042", "origin_zone": 7, "destination_zone": 12, "requested_at": 1713600500000, "call_type": "A"}
{"trip_id": "4c9d88-1a2b", "rider_id": "rider_8831", "origin_zone": 3, "destination_zone": 16, "requested_at": 1713600500500, "call_type": "B"}
{"trip_id": "1b2c3d-9f8e", "rider_id": "rider_2201", "origin_zone": 7, "destination_zone": 1, "requested_at": 1713600501200, "call_type": "A"}
```
*(and 97 more lines exactly like that)*

**Why this format?** Because 4 weeks from now, you will tell Apache Spark to read this folder. Because every single line is perfectly uniform, Spark can read millions of these lines in seconds, allowing you to train your GBTRegressor Machine Learning model on the exact history of the city!
