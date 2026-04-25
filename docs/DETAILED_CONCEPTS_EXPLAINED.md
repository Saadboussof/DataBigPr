# TaaSim — Detailed Concepts Explained
## Everything You Need to Understand, Step by Step

---

## Part 1: How Does `vehicle_gps_producer.py` Simulate 1,000 Taxis?

### The Raw Data (Porto `train.csv`)

The Porto dataset has **1.7 million rows**. Each row is **one complete taxi trip**. The most important column is `POLYLINE`. It looks like this:

```
TAXI_ID = 20000412
POLYLINE = [[-8.62, 41.15], [-8.63, 41.16], [-8.64, 41.17], [-8.63, 41.18]]
```

That `POLYLINE` is a **list of GPS coordinates** recorded every **15 seconds**. So this taxi drove for 4 × 15 = 60 seconds, and we have 4 snapshots of where it was.

### Step-by-Step: What the Simulator Does

```
Step 1: LOAD
   → Reads the first 50,000 rows from train.csv
   → Filters out broken rows (MISSING_DATA = True)

Step 2: SAMPLE
   → Randomly picks 1,000 trips from those 50,000
   → Each trip becomes one "active taxi" driving on the road

Step 3: PARSE
   → For each of those 1,000 trips, it reads the POLYLINE column
   → Converts the JSON string into a Python list of [lon, lat] points
   → Sets current_idx = 0 (the taxi starts at the first GPS point)

Step 4: SIMULATE (infinite loop)
   → Every 1.5 seconds (real time), the simulator does this:
```

### The Simulation Loop (Visual Example)

Imagine we have 3 taxis (simplified from 1,000):

```
Taxi A: POLYLINE has 5 points  → [P1, P2, P3, P4, P5]
Taxi B: POLYLINE has 3 points  → [P1, P2, P3]
Taxi C: POLYLINE has 4 points  → [P1, P2, P3, P4]
```

**At time T=0 (first loop iteration):**
```
Taxi A → reads point P1 → transform Porto→Casablanca → send to Kafka
Taxi B → reads point P1 → transform Porto→Casablanca → send to Kafka
Taxi C → reads point P1 → transform Porto→Casablanca → send to Kafka
  (3 messages sent to Kafka in this batch)
  (sleep 1.5 seconds)
```

**At time T=1.5s:**
```
Taxi A → reads point P2 → transform → send to Kafka
Taxi B → reads point P2 → transform → send to Kafka
Taxi C → reads point P2 → transform → send to Kafka
  (3 more messages sent)
```

**At time T=3.0s:**
```
Taxi A → reads point P3 → send to Kafka
Taxi B → reads point P3 → send to Kafka  ← THIS IS TAXI B'S LAST POINT
Taxi C → reads point P3 → send to Kafka
```

**At time T=4.5s:**
```
Taxi A → reads point P4 → send to Kafka
Taxi B → SKIP (trip finished, no more points)
Taxi C → reads point P4 → send to Kafka  ← THIS IS TAXI C'S LAST POINT
```

**At time T=6.0s:**
```
Taxi A → reads point P5 → send to Kafka  ← THIS IS TAXI A'S LAST POINT
Taxi B → SKIP
Taxi C → SKIP
```

**At time T=7.5s:**
```
All 3 taxis finished → "All active trips finished. Restarting simulation cycle."
→ Reset all current_idx to 0
→ Start over from P1 again (infinite loop)
```

### The Coordinate Transformation

Each Porto point gets transformed before sending:

```
Porto Point: lon=-8.63, lat=41.16

Step 1: Normalize to [0,1]
   lon_norm = (-8.63 - (-8.69)) / (-8.56 - (-8.69)) = 0.46
   lat_norm = (41.16 - 41.14) / (41.20 - 41.14) = 0.33

Step 2: Scale to Casablanca
   casa_lon = -7.80 + (0.46 × 0.40) = -7.616
   casa_lat = 33.40 + (0.33 × 0.30) = 33.50

Step 3: Add noise (±20 meters)
   casa_lon += random.gauss(0, 0.0002) → -7.6158
   casa_lat += random.gauss(0, 0.0002) → 33.5003

Step 4: Check if on land (not in the ocean)
   → If in ocean, clamp to nearest coastline point
```

### What Gets Sent to Kafka

Each GPS ping becomes this JSON message on the `raw.gps` topic:
```json
{
  "taxi_id": "20000412",
  "timestamp": 1745330400000,
  "lat": 33.5003,
  "lon": -7.6158,
  "speed": 42.7,
  "status": "engaged"
}
```

With 1,000 taxis, each loop sends ~950 messages (5% are randomly "blacked out" to simulate GPS failures).

---

## Part 2: What is `raw.gps` vs `raw.trips`?

These are **two completely different Kafka topics** (think: two separate mailboxes).

### `raw.gps` — The Taxi Driver's Phone

**Who writes to it?** → `vehicle_gps_producer.py`
**What does it contain?** → GPS coordinates of taxis moving on the road
**How often?** → ~950 messages every 1.5 seconds (continuous)
**One message looks like:**
```json
{
  "taxi_id": "20000412",
  "timestamp": 1745330400000,
  "lat": 33.50,
  "lon": -7.62,
  "speed": 42.7,
  "status": "engaged"
}
```
**Purpose:** Track where every taxi is RIGHT NOW.

### `raw.trips` — The Passenger's App

**Who writes to it?** → `trip_request_producer.py`
**What does it contain?** → Ride requests from citizens pressing "Request a Taxi"
**How often?** → 2–10 requests per second (depends on rush hour)
**One message looks like:**
```json
{
  "trip_id": "a3f7c2e1-...",
  "rider_id": "rider_4521",
  "origin_zone": 7,
  "destination_zone": 12,
  "requested_at": 1745330405000,
  "call_type": "A"
}
```
**Purpose:** Record that a passenger in Zone 7 wants to go to Zone 12.

### The Difference

```
raw.gps  = "I am a TAXI. I am HERE (lat/lon). I am MOVING."
raw.trips = "I am a PASSENGER. I am in ZONE 7. I want a RIDE to ZONE 12."
```

Both topics exist simultaneously in Kafka. Different consumers read different topics.

---

## Part 3: How Does the Archiver Work? (The 100 Messages / 30 Seconds Rule)

### The Problem
Kafka only keeps messages for 7 days (`KAFKA_LOG_RETENTION_HOURS: 168`). After 7 days, Kafka automatically deletes them. But for Machine Learning, we need data from **months** ago. We need a permanent backup.

### The Solution: `archiver.py`

The archiver is a separate Python script running 24/7 inside Docker. It subscribes to BOTH `raw.gps` AND `raw.trips` at the same time.

### The Batching Logic — A Visual Example

Imagine the archiver starts at 14:00:00. Messages are arriving:

```
14:00:01 → Message 1 from raw.gps    → add to batch (batch size: 1)
14:00:01 → Message 2 from raw.gps    → add to batch (batch size: 2)
14:00:01 → Message 3 from raw.gps    → add to batch (batch size: 3)
14:00:02 → Message 4 from raw.gps    → add to batch (batch size: 4)
...
14:00:05 → Message 99 from raw.gps   → add to batch (batch size: 99)
14:00:05 → Message 100 from raw.gps  → add to batch (batch size: 100)

  ⚡ BATCH SIZE = 100 REACHED! FLUSH NOW!
  → Write all 100 messages as one JSON file to MinIO:
  → s3://kafka-archive/raw.gps/2026/04/23/14/batch_1745330400_0001.json
  → Reset batch to empty, reset timer

14:00:06 → Message 101 from raw.gps  → add to NEW batch (batch size: 1)
14:00:06 → Message 102 from raw.gps  → add to batch (batch size: 2)
...
```

**But what if messages arrive very slowly?** (e.g., at 3 AM, barely any taxis are driving)

```
03:00:00 → Message 1 from raw.gps    → add to batch (batch size: 1)
03:00:10 → Message 2 from raw.gps    → add to batch (batch size: 2)
03:00:20 → Message 3 from raw.gps    → add to batch (batch size: 3)
03:00:30 → (no more messages arriving...)

  ⏰ 30 SECONDS PASSED! FLUSH NOW! (even though we only have 3 messages)
  → Write 3 messages to MinIO:
  → s3://kafka-archive/raw.gps/2026/04/23/03/batch_1745290830_0001.json
```

**Summary:** The archiver flushes when EITHER condition is met first:
- **100 messages accumulated** (high-traffic scenario, e.g., rush hour)
- **30 seconds passed** (low-traffic scenario, e.g., 3 AM)

This prevents two problems:
1. Writing 1 file per message (too many tiny files = slow)
2. Holding messages in memory forever during quiet hours (data loss if crash)

### The File Format in MinIO

Each file is **JSON Lines** (one JSON object per line):
```
{"taxi_id":"20000412","timestamp":1745330401000,"lat":33.50,"lon":-7.62,"speed":42.7,"status":"engaged"}
{"taxi_id":"20000388","timestamp":1745330401500,"lat":33.55,"lon":-7.58,"speed":35.2,"status":"available"}
{"taxi_id":"20000401","timestamp":1745330402000,"lat":33.48,"lon":-7.65,"speed":28.1,"status":"engaged"}
```

---

## Part 4: Watermarks — The Hardest Concept, Explained Simply

### The Problem: Late-Arriving GPS Pings

Imagine Flink is processing GPS messages in real-time. In a perfect world, messages arrive in perfect chronological order:

```
14:00:00 → Taxi A at Zone 3  (timestamp: 14:00:00) ✓ arrives on time
14:00:01 → Taxi B at Zone 7  (timestamp: 14:00:01) ✓ arrives on time
14:00:02 → Taxi C at Zone 5  (timestamp: 14:00:02) ✓ arrives on time
```

**But in the real world, messages arrive LATE.** Why?
- The taxi drove through a tunnel (no GPS signal for 2 minutes)
- The taxi's phone had bad internet (message stuck in a buffer)
- Network congestion between Kafka and Flink

So what actually happens:

```
14:00:00 → Taxi A at Zone 3  (timestamp: 14:00:00)  ✓ on time
14:00:01 → Taxi B at Zone 7  (timestamp: 14:00:01)  ✓ on time
14:00:02 → Taxi C at Zone 5  (timestamp: 14:00:02)  ✓ on time
14:00:03 → Taxi D at Zone 2  (timestamp: 14:00:03)  ✓ on time
14:00:04 → ??? SUDDENLY: Taxi A appears again!
           (timestamp: 14:00:00)  ← THIS IS 4 SECONDS LATE!
           Why? Taxi A was in a tunnel, the GPS ping was delayed.
```

### Without Watermarks: The Disaster

If Flink uses **wall-clock time** (the time the message arrives at Flink), it would think:
- "Taxi A sent me a message at 14:00:04. I'll process it as a 14:00:04 event."
- **WRONG!** Taxi A was actually at Zone 3 at 14:00:00, not 14:00:04.

This causes incorrect analytics:
- "How many taxis were in Zone 3 between 14:00:00 and 14:00:01?" → Answer: 0 (wrong! it was 1)
- "How many taxis were in Zone 3 between 14:00:04 and 14:00:05?" → Answer: 1 (wrong! Taxi A wasn't there at 14:00:04)

### With Watermarks: The Solution

A **watermark** tells Flink: *"Don't use the arrival time. Look INSIDE the JSON message and use the `timestamp` field as the real event time."*

This is what our `GPSTimestampAssigner` does:
```python
class GPSTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        data = json.loads(value)
        return int(data.get('timestamp', record_timestamp))
        #                    ↑
        # "Hey Flink, THIS is the real time this event happened"
```

Now Flink knows:
```
Arrives at 14:00:00 → timestamp inside JSON = 14:00:00 → Process as 14:00:00 ✓
Arrives at 14:00:04 → timestamp inside JSON = 14:00:00 → Process as 14:00:00 ✓
                       ↑ Flink ignores the arrival time!
                       ↑ It uses the event time from the JSON!
```

### A Full Simulation Example

Let's simulate 5 seconds of the pipeline with 4 taxis:

```
═══════════════════════════════════════════════════════════════
 SECOND 1: Simulator sends 4 GPS pings to Kafka
═══════════════════════════════════════════════════════════════

Simulator Loop:
  Taxi A → point P3 → transform → lon=-7.55, lat=33.62 → send to Kafka
  Taxi B → point P7 → transform → lon=-7.72, lat=33.45 → send to Kafka
  Taxi C → BLACKOUT (5% chance, GPS skipped)
  Taxi D → point P2 → transform → lon=-7.48, lat=33.68 → send to Kafka

Kafka topic raw.gps now has 3 new messages:
  Offset 0: {"taxi_id":"A","timestamp":1000,"lat":33.62,"lon":-7.55,"speed":45,"status":"engaged"}
  Offset 1: {"taxi_id":"B","timestamp":1000,"lat":33.45,"lon":-7.72,"speed":30,"status":"available"}
  Offset 2: {"taxi_id":"D","timestamp":1000,"lat":33.68,"lon":-7.48,"speed":55,"status":"engaged"}

═══════════════════════════════════════════════════════════════
 SECOND 1: Flink reads those 3 messages from Kafka
═══════════════════════════════════════════════════════════════

Flink MapFunction processes Taxi A:
  1. Parse JSON → lon=-7.55, lat=33.62
  2. Watermark: extract timestamp=1000 → "This event happened at time 1000"
  3. Grid math:
     grid_x = int(4 × (-7.55 - (-7.80)) / (-7.40 - (-7.80))) = int(4 × 0.625) = 2
     grid_y = int(4 × (33.62 - 33.40) / (33.70 - 33.40)) = int(4 × 0.733) = 2
     zone_id = (2 × 4) + 2 + 1 = 11
  4. Is grid_x in [0,3] AND grid_y in [0,3]? → YES → valid
  5. INSERT INTO vehicle_positions:
     city='casablanca', zone_id=11, event_time=1000, taxi_id='A', ...

Flink MapFunction processes Taxi B:
  1. Parse JSON → lon=-7.72, lat=33.45
  2. Grid math:
     grid_x = int(4 × (-7.72 - (-7.80)) / 0.40) = int(4 × 0.20) = 0
     grid_y = int(4 × (33.45 - 33.40) / 0.30) = int(4 × 0.167) = 0
     zone_id = (0 × 4) + 0 + 1 = 1
  3. INSERT INTO vehicle_positions:
     city='casablanca', zone_id=1, taxi_id='B', ...

Flink MapFunction processes Taxi D:
  1. Parse JSON → lon=-7.48, lat=33.68
  2. Grid math:
     grid_x = int(4 × (-7.48 - (-7.80)) / 0.40) = int(4 × 0.80) = 3
     grid_y = int(4 × (33.68 - 33.40) / 0.30) = int(4 × 0.933) = 3
     zone_id = (3 × 4) + 3 + 1 = 16
  3. INSERT INTO vehicle_positions:
     city='casablanca', zone_id=16, taxi_id='D', ...

═══════════════════════════════════════════════════════════════
 SECOND 1: Archiver also reads those same 3 messages
═══════════════════════════════════════════════════════════════

Archiver batch for raw.gps: [msg1, msg2, msg3] → size=3
  Batch not full yet (3 < 100) and timer not expired (0s < 30s)
  → Keep waiting for more messages...

═══════════════════════════════════════════════════════════════
 SECOND 2-3: 194 more GPS pings arrive (97 per loop × 2 loops)
═══════════════════════════════════════════════════════════════

Flink: processes each one, computes zone_id, INSERTs into Cassandra
Archiver batch: [msg1...msg100] → size=100
  ⚡ FLUSH! Write to MinIO:
  → s3://kafka-archive/raw.gps/2026/04/23/14/batch_1745330402_0001.json
  (remaining 97 messages start a new batch)

═══════════════════════════════════════════════════════════════
 SECOND 4: A LATE message arrives! (Taxi C from the tunnel)
═══════════════════════════════════════════════════════════════

Taxi C's GPS ping from Second 1 finally arrives at Kafka:
  {"taxi_id":"C","timestamp":1000,"lat":33.58,"lon":-7.60,"speed":20,"status":"engaged"}
                     ↑ timestamp says 1000 (Second 1) but it arrived at Second 4!

WITHOUT watermarks: Flink would think this is a Second-4 event → WRONG
WITH watermarks:    Flink reads timestamp=1000 → "This is a Second-1 event" → CORRECT

Flink processes it correctly using the event time, not the arrival time.
```

### The Zone Grid (Visual)

```
Casablanca City Map (4×4 grid = 16 zones)
Lon: -7.80 ←――――――――――――――――――――――――→ -7.40
     ┌──────────┬──────────┬──────────┬──────────┐  Lat: 33.70
     │ Zone 13  │ Zone 14  │ Zone 15  │ Zone 16  │    ↑
     │          │          │          │    D      │    |
     ├──────────┼──────────┼──────────┼──────────┤    |
     │ Zone  9  │ Zone 10  │ Zone 11  │ Zone 12  │    |
     │          │          │    A     │          │    |
     ├──────────┼──────────┼──────────┼──────────┤    |
     │ Zone  5  │ Zone  6  │ Zone  7  │ Zone  8  │    |
     │          │    C     │          │          │    |
     ├──────────┼──────────┼──────────┼──────────┤    |
     │ Zone  1  │ Zone  2  │ Zone  3  │ Zone  4  │    ↓
     │    B     │          │          │          │  Lat: 33.40
     └──────────┴──────────┴──────────┴──────────┘
```

From our simulation:
- Taxi A (lon=-7.55, lat=33.62) → Zone 11 ✓
- Taxi B (lon=-7.72, lat=33.45) → Zone 1 ✓
- Taxi C (lon=-7.60, lat=33.58) → Zone 6 ✓
- Taxi D (lon=-7.48, lat=33.68) → Zone 16 ✓

---

## Part 5: Who Reads What? (The Full Consumer Map)

```
                              ┌─────────────────────────┐
                              │       Apache Kafka       │
                              │                         │
                              │  ┌───────────────────┐  │
  vehicle_gps_producer.py ───→│  │     raw.gps       │  │──→ Flink Job 1 (gps_job.py)
                              │  └───────────────────┘  │──→ Archiver (archiver.py)
                              │                         │
                              │  ┌───────────────────┐  │
  trip_request_producer.py ──→│  │     raw.trips     │  │──→ Flink Job 2 (future, Dev 2)
  event_injector.py ─────────→│  └───────────────────┘  │──→ Archiver (archiver.py)
                              │                         │
                              └─────────────────────────┘

  Flink Job 1 ──→ Cassandra vehicle_positions
  Flink Job 2 ──→ Cassandra demand_zones        (future)
  Flink Job 3 ──→ Cassandra trips               (future)
  Archiver    ──→ MinIO kafka-archive/ bucket
```

**Key Point:** Kafka uses **Pub/Sub**. Multiple consumers can read the same topic independently. Flink and the Archiver both read `raw.gps`, but they don't interfere with each other. Each has its own `group.id` and its own offset tracking.

---

## Summary Table

| Concept | One-Line Explanation |
|---------|---------------------|
| **POLYLINE** | A JSON array of GPS points recorded every 15 seconds during a trip |
| **1,000 taxis** | The simulator randomly picks 1,000 trips and replays their GPS points simultaneously |
| **raw.gps** | Kafka topic where taxi GPS coordinates are published |
| **raw.trips** | Kafka topic where passenger ride requests are published |
| **Watermark** | Tells Flink to use the timestamp INSIDE the JSON, not the arrival time |
| **Checkpointing** | Flink saves its progress every 60s so it can recover after a crash |
| **Archiver batch** | Collects 100 messages (or waits 30s), then writes one file to MinIO |
| **Zone ID** | A number 1–16 computed by dividing Casablanca into a 4×4 grid |
| **Prepared Statement** | Cassandra pre-compiles the INSERT query once for maximum speed |
