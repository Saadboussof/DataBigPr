# WEEK 4-6: TECHNICAL DOCUMENTATION

Comprehensive technical reference for TaaSim Week 4-6 implementations.

---

## TABLE OF CONTENTS

1. [WEEK 4: FLINK JOB 2 - DEMAND AGGREGATOR](#week-4-flink-job-2-demand-aggregator)
2. [WEEK 5: SPARK ETL & KPI ANALYTICS](#week-5-spark-etl--kpi-analytics)
3. [WEEK 6: MACHINE LEARNING - DEMAND FORECASTING](#week-6-machine-learning---demand-forecasting)
4. [DATA SCHEMA REFERENCE](#data-schema-reference)
5. [ERROR HANDLING & MONITORING](#error-handling--monitoring)

---

# WEEK 4: FLINK JOB 2 - DEMAND AGGREGATOR

## File: `src/flink/demand_aggregator_job.py`

### 1. Overview

**Purpose:** Aggregate real-time demand signals using 30-second tumbling windows

**Processing Model:** Kafka Streams → Flink Tumbling Window → Cassandra + Kafka Output

**Key Requirement:** Calculate `pending_requests / max(active_vehicles, 1)` per zone every 30 seconds

### 2. Architecture

#### Input Streams

1. **processed.gps** (from Flink Job 1)
   - JSON format: `{"timestamp": ms, "zone_id": int, "taxi_id": str, ...}`
   - One record per GPS ping (every 5-30 seconds per vehicle)
   - Represents active vehicle positions

2. **raw.trips** (from Trip Request Simulator)
   - JSON format: `{"requested_at": ms, "origin_zone": int, ...}`
   - One record per trip request
   - Represents pending ride requests

#### Processing Steps

```
GPS Stream          Trips Stream
     |                   |
     v                   v
[Timestamp Assignment] (parallel for each stream)
     |                   |
     +----> Union <------+
            |
            v
       [Key by Zone_ID]
            |
            v
     [30-sec Tumbling Window]
            |
            v
   [Aggregate Per Window]
    - Count unique taxis (active_vehicles)
    - Count trips (pending_requests)
    - Calculate ratio
            |
            +----> [Cassandra Sink]
            |
            +----> [Kafka Sink (processed.demand)]
```

### 3. Core Components

#### A. Timestamp Assigners

**GpsTimestampAssigner:**
```python
class GpsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Extract 'timestamp' field from GPS JSON
        # Returns milliseconds since epoch
        # Fallback to record_timestamp if parsing fails
```

**Purpose:** Extract event time from GPS records for watermarking

**TripTimestampAssigner:**
```python
class TripTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Extract 'requested_at' field from trip request JSON
        # Returns milliseconds since epoch
        # Fallback to record_timestamp if parsing fails
```

**Purpose:** Extract event time from trip request records

#### B. Map Functions

**GpsJsonMapFunction:**
```python
# Input: {"timestamp": 1234567890000, "zone_id": 1, "taxi_id": "taxi_001", ...}
# Output: (1, "taxi_001", "gps")
# Fields: (zone_id, taxi_id, type)
```

**Purpose:** Parse GPS JSON and extract zone + vehicle identifier

**TripJsonMapFunction:**
```python
# Input: {"requested_at": 1714930800000, "origin_zone": 1, ...}
# Output: (1, "dummy_id", "trip")
# Fields: (zone_id, dummy_identifier, type)
```

**Purpose:** Parse trip request JSON and extract zone

#### C. Window Aggregation

**AggregateDemand (ProcessWindowFunction):**

```python
def process(self, key, context, elements):
    # key = zone_id
    # elements = all (zone_id, taxi_id/dummy_id, type) tuples in 30-sec window
    
    # 1. Separate GPS from trip records
    active_vehicles = set()  # unique taxi IDs from GPS records
    pending_requests = 0     # count of trip records
    
    for e in elements:
        if e[2] == "gps":
            active_vehicles.add(e[1])
        else:
            pending_requests += 1
    
    # 2. Calculate metrics
    num_vehicles = len(active_vehicles)
    ratio = pending_requests / max(num_vehicles, 1)
    
    # 3. Extract window timestamp
    window_start = datetime.fromtimestamp(
        context.window().start / 1000.0,
        tz=timezone.utc
    )
    
    # 4. Output tuple
    yield (zone_id, window_start, num_vehicles, pending_requests, float(ratio))
```

**Key Logic:**
- **Active Vehicles:** Number of UNIQUE taxis in window (not count of GPS points)
- **Pending Requests:** Total count of trip requests
- **Ratio:** Requests per vehicle, calculated as `pending_requests / max(active_vehicles, 1)`
- **Window Start:** Exact UTC timestamp of window boundary

### 4. Data Flow

#### Input Examples

**GPS Record (processed.gps topic):**
```json
{
  "timestamp": 1714930800000,
  "zone_id": 3,
  "taxi_id": "taxi_042",
  "lat": 33.5532,
  "lon": -7.5892,
  "speed": 45.2
}
```

**Trip Request Record (raw.trips topic):**
```json
{
  "request_time": "2024-05-08T14:30:00Z",
  "zone_id": 3,
  "pickup_lat": 33.5532,
  "pickup_lon": -7.5892,
  "dropoff_lat": 33.5701,
  "dropoff_lon": -7.6012
}
```

#### Output Example (Cassandra)

**demand_zones table:**
```
zone_id | window_start                      | active_vehicles | pending_requests | demand_ratio
--------|-----------------------------------|-----------------|--------------------|----------
1       | 2024-05-08 14:30:00+00:00        | 5               | 3                  | 0.60
2       | 2024-05-08 14:30:00+00:00        | 8               | 12                 | 1.50
3       | 2024-05-08 14:30:00+00:00        | 12              | 8                  | 0.67
```

#### Output Example (Kafka)

**processed.demand topic:**
```json
{
  "zone_id": 3,
  "window_start": "2024-05-08T14:30:00+00:00",
  "active_vehicles": 12,
  "pending_requests": 8,
  "demand_ratio": 0.67
}
```

### 5. Configuration

**Window Size:** 30 seconds (TumblingEventTimeWindows)
**Watermarks:** 3-minute bounded out-of-orderness with 10-second idleness
**Parallelism:** Per-zone after `key_by(zone_id)`; local default is configurable
**Kafka Servers:** kafka:9092
**Cassandra Host:** cassandra:9042

### 6. Error Handling

- **JSON Parsing Errors:** Caught in MapFunctions, logged with `logging.warning`
- **Invalid Timestamps:** Fallback to record_timestamp
- **Null Records:** Filtered out with `.filter(lambda x: x is not None)`
- **Window Processing:** Empty windows yield no output (0 ratio is implicit)

### 7. Monitoring Metrics

**Track in Flink Dashboard:**
- Records in / Records out (lag detection)
- Window size consistency
- Backpressure indicators

**Track in Cassandra:**
- Record insertion rate
- Data freshness (window_start vs current time)

---

# WEEK 5: SPARK ETL & KPI ANALYTICS

## File: `src/spark/etl_nyc.py`

### 1. NYC Data ETL

**Purpose:** Process historical NYC TLC taxi dataset for demand analysis

#### Input Schema
```
PULocationID (int)
tpep_pickup_datetime (timestamp)
... (other fields)
```

#### Processing
```
Raw Parquet (s3a://raw/nyc-tlc/)
        |
        v
[Group by PULocationID + hour(pickup_time)]
        |
        v
[Count records per group]
        |
        v
Curated Parquet (s3a://curated/demand-by-zone/)
```

#### Output Schema
```
zone_id (int) - Renamed from PULocationID
hour (int) - Hour of day (0-23)
demand_count (long) - Number of trips in that hour
```

#### Configuration
```python
MINIO_ENDPOINT = "http://minio:9000"
INPUT_PATH_NYC = "s3a://raw/nyc-tlc/"
OUTPUT_PATH_NYC = "s3a://curated/demand-by-zone/"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,..."
```

---

## File: `src/spark/etl_porto.py`

### 2. Porto Data ETL

**Purpose:** Transform Porto GPS trajectory data to Casablanca geography

**Complexity:** HIGH - involves geospatial transformations

#### Input Schema
```
TRIP_ID (string)
TAXI_ID (string)
TIMESTAMP (timestamp)
POLYLINE (string) - JSON array of [lon, lat] pairs
MISSING_DATA (boolean)
```

#### Processing Pipeline

```
Raw CSV (s3a://raw/porto-trips/)
        |
        v
[Parse & Deduplicate]
        |
        v
[Parse POLYLINE → JSON array]
        |
        v
[Filter valid trajectories (length > 1)]
        |
        v
[Extract start/end coordinates]
        |
        v
[Compute H3 geohashes (resolution 9)]
        |
        v
[Join with zone mapping]
        |
        v
[Extract temporal features (year, month)]
        |
        v
[Partition by year_month]
        |
        v
Curated Parquet (s3a://curated/porto-trips/)
```

#### Key Transformations

**1. Polyline Parsing**
```python
polyline_schema = ArrayType(ArrayType(DoubleType()))
porto_df = porto_df.withColumn(
    "polyline_json",
    from_json(col("POLYLINE"), polyline_schema)
)
```
- Converts JSON string to array of coordinate pairs
- Enables efficient extraction of start/end points

**2. H3 Geohashing**
```python
def get_h3_id(lat, lon):
    if lat is None or lon is None:
        return None
    return h3.geo_to_h3(lat, lon, H3_RESOLUTION=9)

start_h3_id = h3_udf(col("start_lat"), col("start_lon"))
```
- Creates hierarchical hexagonal grid cells
- Resolution 9 ≈ 357m accuracy
- Enables efficient spatial indexing

**3. Output Schema**
```
TRIP_ID (string)
TAXI_ID (string)
TIMESTAMP (timestamp)
start_lat (double)
start_lon (double)
end_lat (double)
end_lon (double)
start_h3_id (string)
end_h3_id (string)
year (int)
month (int)
year_month (string)
[... other fields ...]
```

#### Configuration
```python
H3_RESOLUTION = 9  # ~357m cells
INPUT_PATH_PORTO = "s3a://raw/porto-trips/"
INPUT_PATH_ZONES = "s3a://raw/zone-mapping/zone_mapping.csv"
OUTPUT_PATH_PORTO = "s3a://curated/porto-trips/"
MINIO_ENDPOINT = "http://minio:9000"
```

---

## File: `src/spark/kpi_analytics.py`

### 3. KPI Analytics

**Purpose:** Calculate business intelligence metrics from curated data

**Inputs:**
- `s3a://curated/porto-trips/` - Curated trip data
- `s3a://curated/demand-by-zone/` - NYC hourly demand
- `taasim.vehicle_positions` - Real-time vehicle data from Cassandra

#### KPI Calculations

**KPI 1: Total Trips Per Zone**
```sql
SELECT 
    arrondissement_id AS zone_id,
    COUNT(TRIP_ID) AS total_trips
FROM porto_trips
WHERE arrondissement_id IS NOT NULL
GROUP BY arrondissement_id
```

**Purpose:** Baseline demand metric per zone

**KPI 2: Average Trip Duration Per Zone**
```python
duration_seconds = size(polyline_json) * 15  # 15 sec per coordinate
avg_duration = duration_seconds / 60  # Convert to minutes

# Group by zone and average
```

**Purpose:** Understand typical trip length per zone

**KPI 3: Peak Demand Hours Per Zone**
```python
window_spec = Window.partitionBy("zone_id").orderBy(col("demand_count").desc())
peak_hours_kpi = demand_df \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("zone_id", "hour")
```

**Purpose:** Identify busiest hour for operational planning

**KPI 4: Coverage Gaps**
```sql
SELECT
    d.zone_id,
    d.demand_count,
    COALESCE(v.available_vehicles, 0) as vehicle_count
FROM nyc_demand d
LEFT JOIN vehicle_locations v ON d.zone_id = v.zone_id
WHERE d.demand_count > 0 AND COALESCE(v.available_vehicles, 0) < 2
```

**Purpose:** Identify zones with demand but insufficient supply

#### Output Schema
```
zone_id (int)
total_trips (long)
avg_trip_duration_minutes (double)
peak_demand_hour (int)
coverage_gap_demand (long)
city (string)
window_start (timestamp)
```

#### Storage
```python
.format("org.apache.spark.sql.cassandra")
.options(table="demand_zones", keyspace="taasim")
.mode("append")  # Append mode preserves historical data
```

---

# WEEK 6: MACHINE LEARNING - DEMAND FORECASTING

## File: `src/spark/ml_demand_forecasting.py`

### 1. ML Pipeline Overview

**Goal:** Predict future taxi demand at zone-level, 30-minute granularity

**Approach:** Supervised regression using historical data + temporal features

**Model:** Gradient Boosted Tree (GBT) Regressor

### 2. Data Preparation (Unit 2)

#### Loading
```python
trips_df = spark.read.parquet("s3a://curated/porto-trips/")

# Extract columns for time-series
trips_df = trips_df.select(
    col("TRIP_ID"),
    col("TIMESTAMP").cast("timestamp").alias("pickup_datetime"),
    col("arrondissement_id").cast("int").alias("pickup_zone_id")
).filter(col("pickup_datetime").isNotNull())
```

#### Aggregation
```python
# Create 30-minute windows
trips_df = trips_df.withColumn(
    "time_window",
    (floor(unix_timestamp(col("pickup_datetime")) / (30 * 60)) * (30 * 60))
    .cast("timestamp")
)

# Aggregate demand
demand_df = trips_df \
    .groupBy("time_window", "pickup_zone_id") \
    .count() \
    .withColumnRenamed("count", "demand") \
    .withColumnRenamed("pickup_zone_id", "zone_id")
```

#### Complete Time-Zone Grid
```python
# Create full grid to handle zero-demand periods
time_grid_df = spark.sql(
    f"SELECT sequence(...) as time_window"
).withColumn("time_window", explode(col("time_window")))

zones_df = spark.range(1, 17).withColumnRenamed("id", "zone_id")

full_grid_df = time_grid_df.crossJoin(zones_df)

# Left join with actual demand, fill zeros
demand_timeseries_df = full_grid_df.join(
    demand_df,
    ["time_window", "zone_id"],
    "left"
).na.fill(0, ["demand"])
```

**Purpose:** Handle zero-demand periods correctly in time series

### 3. Feature Engineering (Unit 3)

#### Temporal Features
```python
features_df = features_df.withColumn("hour_of_day", hour(col("time_window")))
features_df = features_df.withColumn("day_of_week", dayofweek(col("time_window")))
features_df = features_df.withColumn(
    "is_weekend",
    when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
)
features_df = features_df.withColumn(
    "is_friday",
    when(col("day_of_week") == 6, 1).otherwise(0)
)
```

**Why:** Demand patterns vary by time of day and day of week

#### Zone Features
```python
zone_data = [
    (i, int(1000 + (i * 100)), 'residential' if i % 3 == 0 else 'commercial')
    for i in range(1, 17)
]
zones_mock_df = spark.createDataFrame(
    zone_data,
    ["zone_id", "zone_population_density", "zone_type"]
)
```

**Why:** Zone characteristics influence demand levels

#### Weather Features (Mocked)
```python
weather_mock_df = features_df.select("time_window", "hour_of_day").distinct()
weather_mock_df = weather_mock_df.withColumn(
    "is_raining",
    when(rand() > 0.8, 1).otherwise(0)  # 20% chance
).withColumn(
    "temperature_bucket",
    when(col("hour_of_day") < 8, "cold")
    .when(col("hour_of_day") < 18, "mild")
    .otherwise("hot")
)
```

**Why:** Weather affects demand (mocked for demo, use real data in production)

#### Lag Features (CRITICAL)
```python
window_spec = Window.partitionBy("zone_id").orderBy("time_window")
DAYS_TO_LAG_1 = 1
DAYS_TO_LAG_7 = 7
ROWS_PER_DAY = 48  # 24 hours * 2 intervals/hour

# 1-day lag
features_df = features_df.withColumn(
    "demand_lag_1d",
    lag("demand", count=DAYS_TO_LAG_1 * ROWS_PER_DAY).over(window_spec)
)

# 7-day lag
features_df = features_df.withColumn(
    "demand_lag_7d",
    lag("demand", count=DAYS_TO_LAG_7 * ROWS_PER_DAY).over(window_spec)
)

# 7-day rolling average
rolling_window_spec = window_spec.rowsBetween(-(DAYS_TO_LAG_7 * ROWS_PER_DAY), -1)
features_df = features_df.withColumn(
    "rolling_7d_mean",
    avg("demand").over(rolling_window_spec)
)

# Drop rows with nulls from lag calculations
final_features_df = features_df.na.dropna()
```

**Why:** Demand autocorrelation is strong (similar demand yesterday/last week)

**Output Rows:** All rows with valid lag features (first 7 days of data per zone removed)

### 4. ML Pipeline Construction (Unit 4)

#### Feature Encoding
```python
zone_type_indexer = StringIndexer(
    inputCol="zone_type",
    outputCol="zone_type_indexed",
    handleInvalid="keep"
)

temp_bucket_indexer = StringIndexer(
    inputCol="temperature_bucket",
    outputCol="temperature_bucket_indexed",
    handleInvalid="keep"
)
```

**Purpose:** Convert categorical features to numeric indices

#### Feature Assembler
```python
feature_columns = [
    "zone_id", "hour_of_day", "day_of_week", "is_weekend", "is_friday",
    "zone_population_density", "zone_type_indexed", "is_raining",
    "temperature_bucket_indexed", "demand_lag_1d", "demand_lag_7d",
    "rolling_7d_mean"
]

vector_assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features",
    handleInvalid="keep"
)
```

**Purpose:** Combine all features into single vector for ML algorithm

#### Model Definition
```python
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="demand",
    maxDepth=GBT_MAX_DEPTH,  # 5
    maxIter=GBT_MAX_ITER,    # 50
    seed=42  # For reproducibility
)
```

**GBT Parameters:**
- **maxDepth:** 5 (prevent overfitting)
- **maxIter:** 50 (gradient boosting iterations)
- **seed:** 42 (reproducibility)

#### Pipeline Construction
```python
pipeline = Pipeline(stages=[
    zone_type_indexer,
    temp_bucket_indexer,
    vector_assembler,
    gbt
])
```

**Order matters:** Stages executed sequentially

### 5. Model Training (Unit 5)

#### Train-Test Split
```python
# Temporal split to prevent data leakage
first_date = final_features_df.select(min("time_window")).first()[0]
split_date = first_date + relativedelta(months=+10)

train_df = final_features_df.filter(col("time_window") < split_date)
test_df = final_features_df.filter(col("time_window") >= split_date)
```

**Why:** Time-series requires temporal split, not random split

**Ratio:** ~77% train, 23% test (10 of 13 months)

#### Hyperparameter Tuning
```python
param_grid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [5, 7]) \
    .build()

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="demand",
    predictionCol="prediction"
)

cross_validator = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3
)

cv_model = cross_validator.fit(train_df)
```

**Cross-Validation:** Tests all parameter combinations, selects best

### 6. Model Evaluation (Unit 6)

#### Test Set Predictions
```python
predictions_df = cv_model.transform(test_df)
model_rmse = evaluator.evaluate(predictions_df)
```

#### Baseline Comparison
```python
# Baseline: Predict last week's demand
baseline_predictions_df = test_df.withColumn(
    "prediction",
    col("demand_lag_7d")
)
baseline_rmse = evaluator.evaluate(baseline_predictions_df)
```

**Success Metric:** `model_rmse < baseline_rmse` (typically 20-30% improvement)

#### Feature Importance
```python
gbt_model = cv_model.bestModel.stages[-1]
feature_names = vector_assembler_stage.getInputCols()
importances = gbt_model.featureImportances

# Display top 5
feature_importance_list = sorted(
    zip(feature_names, importances),
    key=lambda x: x[1],
    reverse=True
)[:5]
```

**Expected:** Lag features dominate (demand_lag_1d, demand_lag_7d)

### 7. Model Export (Unit 7)

#### Save to MinIO
```python
best_pipeline_model.write() \
    .overwrite() \
    .save("s3a://ml/models/demand_v1/")
```

**Format:** Spark PipelineModel (binary format)

**Includes:**
- All transformers (indexers, assembler)
- Trained GBT model
- All metadata

#### Loading in Production
```python
model = PipelineModel.load("s3a://ml/models/demand_v1/")

# Make predictions
predictions = model.transform(new_data_df)
```

### 8. Performance Metrics Interpretation

**RMSE (Root Mean Squared Error)**
- Formula: √(Σ(predicted - actual)² / n)
- Units: Same as target (trips per 30-min window)
- Interpretation: Average prediction error

**MAE (Mean Absolute Error)**
- Formula: Σ|predicted - actual| / n
- Units: Same as target
- Less sensitive to outliers than RMSE

**Example Output:**
```
GBT Model RMSE: 2.34 trips per 30-min window
Baseline RMSE: 3.12 trips per 30-min window
Improvement: (3.12 - 2.34) / 3.12 = 25%
```

---

# DATA SCHEMA REFERENCE

## Cassandra: demand_zones

```
PRIMARY KEY (zone_id, window_start)

zone_id: int
window_start: timestamp
active_vehicles: int
pending_requests: int
demand_ratio: float
total_trips: long
avg_trip_duration_minutes: double
peak_demand_hour: int
coverage_gap_demand: long
city: string
```

## MinIO Paths

```
s3a://raw/
  ├── nyc-tlc/ (Parquet files)
  ├── porto-trips/ (CSV files)
  └── zone-mapping/ (CSV file)

s3a://curated/
  ├── demand-by-zone/ (NYC ETL output)
  ├── porto-trips/ (Porto ETL output, partitioned by year_month)
  └── temp-data/ (intermediate processing)

s3a://ml/
  └── models/
      └── demand_v1/ (Spark PipelineModel)
```

## Kafka Topics

```
raw.gps: GPS coordinates from simulators
raw.trips: Trip requests from simulators
processed.gps: Zone-enriched GPS (Job 1 output)
processed.demand: Aggregated demand (Job 2 output)
```

---

# ERROR HANDLING & MONITORING

## Common Errors

### Flink Job 2

**Error:** "Cannot deserialize JSON"
- **Cause:** GPS or Trip JSON format mismatch
- **Fix:** Verify simulator output format matches expected schema

**Error:** "Cassandra timeout"
- **Cause:** Cassandra not running or overloaded
- **Fix:** Check Cassandra container status, increase timeouts

### Spark ETL

**Error:** "Out of memory"
- **Cause:** Large dataset with insufficient executor memory
- **Fix:** Increase `--executor-memory` parameter

**Error:** "MinIO connection refused"
- **Cause:** MinIO endpoint configuration
- **Fix:** Verify `MINIO_ENDPOINT` environment variable

### ML Pipeline

**Error:** "Cannot load data from parquet"
- **Cause:** Porto ETL not completed or path incorrect
- **Fix:** Verify ETL job completed, check output path in MinIO

**Error:** "Null values in lag features"
- **Cause:** Not enough historical data
- **Fix:** Ensure sufficient historical data (>7 days), check time_window sorting

## Monitoring Commands

```bash
# Flink Jobs
curl http://localhost:8081/api/v1/jobs/overview

# Cassandra
docker exec -it taasim-cassandra cqlsh -e "SELECT COUNT(*) FROM taasim.demand_zones;"

# MinIO
docker exec -it taasim-minio mc du minio/curated/

# Kafka
docker exec taasim-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

**Documentation Version:** 2.0  
**Last Updated:** May 2024  
**Status:** Complete
