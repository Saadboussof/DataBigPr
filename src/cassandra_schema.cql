-- ==============================================================================
-- TaaSim Cassandra Schema — Week 2: Storage Design
-- ==============================================================================
-- Run with: cqlsh cassandra 9042 -f /schema.cql
-- Or:       docker exec taasim-cassandra cqlsh -f /schema.cql
-- ==============================================================================

-- Keyspace: SimpleStrategy with RF=1 (single-node dev environment)
CREATE KEYSPACE IF NOT EXISTS taasim
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- ------------------------------------------------------------------------------
-- Table 1: vehicle_positions
-- Stores the latest GPS position of every taxi, partitioned by zone.
--
-- Partition Key: (city, zone_id)
--   → Why? The main query is "Show me all taxis currently in Zone 7"
--   → All taxis in Zone 7 are co-located on the same physical node
--   → If we used taxi_id as partition key, every zone query would scatter
--     across ALL partitions (full cluster scan = slow)
--
-- Clustering Key: event_time DESC
--   → Most recent position first. "LIMIT 1" gives the latest location.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taasim.vehicle_positions (
  city        TEXT,
  zone_id     INT,
  zone_name   TEXT,       -- Real arrondissement name (e.g., 'Anfa', 'Hay Hassani')
  event_time  TIMESTAMP,
  taxi_id     TEXT,
  lat         DOUBLE,
  lon         DOUBLE,
  speed       DOUBLE,
  status      TEXT,       -- 'available', 'engaged', 'offline'
  PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 86400;  -- Auto-delete after 24 hours (positions are transient)

-- ------------------------------------------------------------------------------
-- Table 2: trips
-- Stores every trip record from request to completion.
--
-- Partition Key: (city, date_bucket)
--   → Why? Without date bucketing, one partition would grow unbounded
--     (millions of rows over weeks). date_bucket = '2026-04-20' keeps
--     each partition to ~1 day of trips (~50K-200K rows), comfortably
--     below the recommended 100MB limit.
--   → Query pattern: "Show all trips today" → WHERE date_bucket = '2026-04-20'
--
-- Clustering Key: created_at DESC
--   → Most recent trips first.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taasim.trips (
  city           TEXT,
  date_bucket    TEXT,       -- e.g., '2026-04-20'
  created_at     TIMESTAMP,
  trip_id        TEXT,
  rider_id       TEXT,
  taxi_id        TEXT,
  origin_zone    INT,
  dest_zone      INT,
  status         TEXT,       -- 'PENDING', 'MATCHED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED'
  pickup_time    TIMESTAMP,
  dropoff_time   TIMESTAMP,
  fare           DOUBLE,
  eta_seconds    INT,
  PRIMARY KEY ((city, date_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- ------------------------------------------------------------------------------
-- Table 3: demand_zones
-- Stores 30-second demand snapshots per zone (written by Flink Job 2).
--
-- Partition Key: (city, zone_id)
--   → Same reasoning as vehicle_positions: query is "What is demand in Zone 7?"
--
-- Clustering Key: window_start DESC
--   → Most recent window first. "LIMIT 1" gives the current demand snapshot.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taasim.demand_zones (
  city              TEXT,
  zone_id           INT,
  zone_name         TEXT,       -- Real arrondissement name (e.g., 'Anfa', 'Hay Hassani')
  window_start      TIMESTAMP,
  active_vehicles   INT,        -- Supply: how many taxis are in this zone
  pending_requests  INT,        -- Demand: how many ride requests are pending
  ratio             DOUBLE,     -- demand intensity: pending_requests / max(active_vehicles, 1)
  forecast_demand   DOUBLE,     -- ML prediction for next 30 min (filled by Spark ML)
  PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;  -- Auto-delete after 7 days

-- ------------------------------------------------------------------------------
-- Table 4: kpi_summary
-- Stores batch-computed KPIs (written by Spark kpi_analytics.py).
-- SEPARATE from demand_zones to avoid overwriting Flink Job 2's streaming data.
--
-- Partition Key: (city, zone_id)
--   → Same pattern as other zone-keyed tables.
--   → Query: "Show me all KPIs for Zone 7"
--
-- Clustering Key: window_start DESC
--   → Most recent KPI computation first.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taasim.kpi_summary (
  city                      TEXT,
  zone_id                   INT,
  window_start              TIMESTAMP,
  total_trips               BIGINT,
  avg_trip_duration_minutes DOUBLE,
  peak_demand_hour          INT,
  coverage_gap_demand       BIGINT,
  PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 2592000;  -- 30 days (KPIs are semi-static)
