"""
TaaSim — ML Demand Forecasting Pipeline (Merged)
==================================================
Combines the feature engineering richness of ml_demand_forecasting.py
with the production structure and correct bucket of ml_demand_forecasting_week6.py.

Pipeline:
  1. Load curated Porto trips from MinIO
  2. Aggregate hourly demand per zone
  3. Create complete time × zone grid (handles zero-demand periods)
  4. Engineer features:
     - Temporal: hour_of_day, day_of_week, is_weekend, is_friday
     - Spatial: zone_population_density, zone_type (mocked)
     - Weather: temperature_bucket, is_raining (Open-Meteo with mock fallback)
     - Lag: demand_lag_1d, demand_lag_7d, rolling_7d_mean
  5. Split train/test temporally (80/20)
  6. Train GBTRegressor with CrossValidator (hyperparameter tuning)
  7. Evaluate + compare against baseline (7-day lag)
  8. Extract feature importances
  9. Save model to MinIO (ml-store/models/gbt_hour_zone.model)
"""

import os
import json
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_trunc, hour, from_unixtime, udf, lit, when, rand,
    min as min_, max as max_, dayofweek, floor, unix_timestamp,
    explode, lag, avg, count, row_number
)
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType
)
from pyspark.sql.window import Window

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

try:
    import h3
    H3_AVAILABLE = True
except ImportError:
    H3_AVAILABLE = False
    print("WARNING: h3 library not available. Using raw zone_id without parent conversion.")

# ── Configuration ──────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

TRIPS_INPUT_PATH = "s3a://curated/porto-trips/"
MODEL_OUTPUT_PATH = "s3a://ml-store/models/gbt_hour_zone.model"

GBT_MAX_DEPTH = 5
GBT_MAX_ITER = 30
TRAIN_SPLIT_RATIO = 0.8

ROWS_PER_DAY = 24
DAYS_LAG_1 = 1
DAYS_LAG_7 = 7

# Casablanca coordinates for weather
CASA_LAT = 33.5731
CASA_LON = -7.5898

# ── Zone Parent UDF (reduce H3 cardinality) ───────────────────

if H3_AVAILABLE:
    @udf(StringType())
    def zone_parent(h3_code):
        if h3_code is None:
            return None
        try:
            return h3.h3_to_parent(h3_code, res=7)
        except Exception:
            return str(h3_code)
else:
    @udf(StringType())
    def zone_parent(h3_code):
        return str(h3_code) if h3_code is not None else None


# ── Weather UDFs ──────────────────────────────────────────────

try:
    import requests as _requests
    WEATHER_HTTP_AVAILABLE = True
except ImportError:
    WEATHER_HTTP_AVAILABLE = False
    print("WARNING: requests library not available. Using mocked weather data.")


def fetch_casablanca_weather(spark, start_date, end_date):
    """
    Fetch hourly weather for Casablanca from Open-Meteo archive API.
    Falls back to mocked data if the API call fails.
    Returns a DataFrame with columns: time_window, temperature_bucket, is_raining.
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={CASA_LAT}&longitude={CASA_LON}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,precipitation&timezone=auto"
    )
    try:
        if not WEATHER_HTTP_AVAILABLE:
            raise RuntimeError("requests library not installed")
        resp = _requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        precips = hourly.get("precipitation", [])
        if not times:
            raise ValueError("Empty weather response")
        records = []
        for t, temp, precip in zip(times, temps, precips):
            temp_bucket = "cold"
            if temp is not None:
                if temp < 10:
                    temp_bucket = "cold"
                elif temp < 25:
                    temp_bucket = "mild"
                else:
                    temp_bucket = "hot"
            is_raining = 1 if (precip is not None and precip > 0.1) else 0
            records.append((t, temp_bucket, is_raining))
        weather_df = spark.createDataFrame(
            records, ["time_str", "temperature_bucket", "is_raining"]
        )
        weather_df = weather_df.withColumn(
            "time_window", col("time_str").cast("timestamp")
        ).drop("time_str")
        print(f"  ✅ Open-Meteo: {len(times)} hourly records loaded")
        return weather_df
    except Exception as e:
        print(f"  ⚠ Open-Meteo fetch failed ({e}). Using mocked weather data.")
        return None


def build_mock_weather(spark, time_windows_df):
    """Create mocked weather features (20% rain chance, temperature by hour)."""
    mock_df = time_windows_df.select("time_window", "hour_of_day").distinct()
    mock_df = mock_df.withColumn(
        "is_raining",
        when(rand() > 0.8, 1).otherwise(0)
    ).withColumn(
        "temperature_bucket",
        when(col("hour_of_day") < 8, "cold")
        .when(col("hour_of_day") < 18, "mild")
        .otherwise("hot")
    )
    print("  ℹ Using mocked weather data")
    return mock_df


# ── Spark Session ──────────────────────────────────────────────

def get_spark_session():
    """Initializes a Spark session configured for MinIO."""
    return (
        SparkSession.builder.appName("Week6_GBT_Demand_Forecast")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .getOrCreate()
    )


# ── Data Loading & Aggregation ────────────────────────────────

def build_demand_timeseries(trips_df):
    """Aggregate trips into hourly demand per zone with H3 parent reduction."""
    zone_col = "zone_id" if "zone_id" in trips_df.columns else "arrondissement_id"
    base_df = (
        trips_df.select(
            col(zone_col).cast("string").alias("zone_id"),
            col("TIMESTAMP").cast("long").alias("timestamp_unix"),
        )
        .filter(col("zone_id").isNotNull())
        .filter(col("timestamp_unix").isNotNull())
        .withColumn(
            "pickup_time",
            from_unixtime(col("timestamp_unix")).cast("timestamp"),
        )
        .filter(col("pickup_time").isNotNull())
    )
    base_df = base_df.withColumn("zone_id", zone_parent(col("zone_id")))
    demand_df = (
        base_df.groupBy(
            date_trunc("hour", col("pickup_time")).alias("time_window"),
            col("zone_id"),
        )
        .count()
        .withColumnRenamed("count", "demand")
    )
    return demand_df


def create_full_grid(demand_df):
    """
    Build a complete time × zone grid so periods with zero demand
    are represented (critical for time-series forecasting).
    """
    bounds = demand_df.agg(
        min_("time_window").alias("min_ts"),
        max_("time_window").alias("max_ts"),
    ).first()
    min_ts, max_ts = bounds["min_ts"], bounds["max_ts"]
    print(f"  Grid range: {min_ts} to {max_ts}")
    time_grid_df = spark.sql(
        f"SELECT sequence(to_timestamp('{min_ts}'), to_timestamp('{max_ts}'), interval 1 hour) as tw"
    ).withColumn("time_window", explode(col("tw")))
    all_zones = demand_df.select("zone_id").distinct().collect()
    zone_list = [row["zone_id"] for row in all_zones]
    zones_df = spark.createDataFrame(
        [(z,) for z in zone_list], ["zone_id"]
    )
    full_grid_df = time_grid_df.crossJoin(zones_df)
    demand_timeseries_df = full_grid_df.join(
        demand_df, ["time_window", "zone_id"], "left"
    ).na.fill(0, ["demand"])
    num_zones = len(zone_list)
    print(f"  Grid: {num_zones} zones × {time_grid_df.count()} time slots")
    return demand_timeseries_df


# ── Feature Engineering ────────────────────────────────────────

def engineer_features(features_df):
    """Add temporal, spatial, weather, and lag features."""
    # ── Temporal features ──
    features_df = features_df.withColumn("hour_of_day", hour(col("time_window")))
    features_df = features_df.withColumn(
        "day_of_week", dayofweek(col("time_window"))
    )
    features_df = features_df.withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
    ).withColumn(
        "is_friday",
        when(col("day_of_week") == 6, 1).otherwise(0)
    )

    # ── Spatial features (mocked — would come from a reference table in prod) ──
    all_zones = features_df.select("zone_id").distinct().collect()
    zone_data = []
    for i, row in enumerate(all_zones):
        zid = row["zone_id"]
        density = 1000 + (i * 100)
        ztype = "residential" if i % 3 == 0 else "commercial"
        zone_data.append((zid, density, ztype))
    zones_mock_df = spark.createDataFrame(
        zone_data, ["zone_id", "zone_population_density", "zone_type"]
    )
    features_df = features_df.join(zones_mock_df, "zone_id", "left")

    # ── Weather features ──
    bounds = features_df.agg(
        min_("time_window").alias("min_ts"),
        max_("time_window").alias("max_ts"),
    ).first()
    start_date_str = bounds["min_ts"].strftime("%Y-%m-%d")
    end_date_str = bounds["max_ts"].strftime("%Y-%m-%d")
    weather_df = fetch_casablanca_weather(spark, start_date_str, end_date_str)
    if weather_df is None:
        weather_df = build_mock_weather(spark, features_df)
    features_df = features_df.join(weather_df, "time_window", "left")

    # ── Lag features (CRITICAL for time-series) ──
    window_spec = Window.partitionBy("zone_id").orderBy("time_window")
    features_df = features_df.withColumn(
        "demand_lag_1d",
        lag(col("demand"), DAYS_LAG_1 * ROWS_PER_DAY).over(window_spec)
    )
    features_df = features_df.withColumn(
        "demand_lag_7d",
        lag(col("demand"), DAYS_LAG_7 * ROWS_PER_DAY).over(window_spec)
    )
    rolling_window_spec = window_spec.rowsBetween(
        -(DAYS_LAG_7 * ROWS_PER_DAY), -1
    )
    features_df = features_df.withColumn(
        "rolling_7d_mean",
        avg("demand").over(rolling_window_spec)
    )

    # Drop rows with nulls from lag computation (first 7 days per zone)
    final_df = features_df.na.drop()
    print(f"  After lag features: {final_df.count()} rows")
    return final_df


# ── Train/Test Split ───────────────────────────────────────────

def split_train_test(features_df, ratio=TRAIN_SPLIT_RATIO):
    """Temporal split — trains on past, tests on future."""
    bounds = features_df.agg(
        min_("time_window").alias("min_ts"),
        max_("time_window").alias("max_ts"),
    ).first()
    min_ts, max_ts = bounds["min_ts"], bounds["max_ts"]
    if min_ts is None or max_ts is None:
        raise ValueError("No data for train/test split.")
    total_sec = (max_ts - min_ts).total_seconds()
    split_ts = min_ts + timedelta(seconds=int(total_sec * ratio))
    train_df = features_df.filter(col("time_window") < split_ts)
    test_df = features_df.filter(col("time_window") >= split_ts)
    return train_df, test_df, split_ts


# ── ML Pipeline Construction ───────────────────────────────────

def build_pipeline(num_zones):
    """Create a Pipeline with StringIndexers, VectorAssembler, and GBTRegressor."""
    zone_id_indexer = StringIndexer(
        inputCol="zone_id",
        outputCol="zone_id_indexed",
        handleInvalid="keep",
    )
    zone_type_indexer = StringIndexer(
        inputCol="zone_type",
        outputCol="zone_type_indexed",
        handleInvalid="keep",
    )
    temp_indexer = StringIndexer(
        inputCol="temperature_bucket",
        outputCol="temperature_bucket_indexed",
        handleInvalid="keep",
    )
    feature_columns = [
        "zone_id_indexed",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "is_friday",
        "zone_population_density",
        "zone_type_indexed",
        "is_raining",
        "temperature_bucket_indexed",
        "demand_lag_1d",
        "demand_lag_7d",
        "rolling_7d_mean",
    ]
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="keep",
    )
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="demand",
        maxDepth=GBT_MAX_DEPTH,
        maxIter=GBT_MAX_ITER,
        maxBins=max(num_zones + 1, 100),
    )
    return Pipeline(stages=[
        zone_id_indexer,
        zone_type_indexer,
        temp_indexer,
        assembler,
        gbt,
    ]), gbt


# ── Evaluation ─────────────────────────────────────────────────

def evaluate_model(predictions_df, label_col="demand", pred_col="prediction"):
    """Compute RMSE and MAE."""
    evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions_df)
    evaluator.setMetricName("mae")
    mae = evaluator.evaluate(predictions_df)
    return rmse, mae


def baseline_rmse(test_df, label_col="demand"):
    """Compute RMSE of the naive 7-day lag baseline."""
    baseline_df = test_df.withColumn("prediction", col("demand_lag_7d"))
    evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="rmse"
    )
    return evaluator.evaluate(baseline_df)


# ── Main ───────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("═" * 60)
    print("  TaaSim — ML Demand Forecasting (Merged Pipeline)")
    print("═" * 60)

    # 1. Load data
    print(f"\n[1/7] Loading curated Porto trips from {TRIPS_INPUT_PATH}")
    trips_df = spark.read.parquet(TRIPS_INPUT_PATH)
    total_trips = trips_df.count()
    print(f"  {total_trips} trips loaded")

    # 2. Build hourly demand time series
    print("\n[2/7] Building hourly demand time series...")
    demand_df = build_demand_timeseries(trips_df)
    print(f"  {demand_df.count()} zone-hour records")

    # 3. Create complete time × zone grid
    print("\n[3/7] Creating complete time × zone grid...")
    grid_df = create_full_grid(demand_df)

    # 4. Engineer features
    print("\n[4/7] Engineering features...")
    features_df = engineer_features(grid_df)
    features_df.printSchema()
    num_zone_distinct = features_df.select("zone_id").distinct().count()
    print(f"  Unique zones: {num_zone_distinct}")

    # 5. Train/test split
    print("\n[5/7] Splitting train/test temporally...")
    train_df, test_df, split_ts = split_train_test(features_df)
    print(f"  Split at: {split_ts}")
    print(f"  Train: {train_df.count()} rows, Test: {test_df.count()} rows")

    # 6. Build & train pipeline directly (no CrossValidator — single param config)
    print("\n[6/7] Training GBT...")
    pipeline, gbt = build_pipeline(num_zone_distinct)
    model = pipeline.fit(train_df)
    best_model = model

    # Evaluate on test set
    predictions = best_model.transform(test_df)
    model_rmse, model_mae = evaluate_model(predictions)
    print(f"\n  📊 GBT Model — Test RMSE: {model_rmse:.4f}, MAE: {model_mae:.4f}")

    # Baseline comparison (7-day lag)
    print("\n  Computing baseline (7-day lag)...")
    try:
        base_rmse = baseline_rmse(test_df)
        print(f"  📊 Baseline (7d lag) — Test RMSE: {base_rmse:.4f}")
        improvement = ((base_rmse - model_rmse) / base_rmse) * 100
        if model_rmse < base_rmse:
            print(f"  ✅ GBT outperforms baseline by {improvement:.1f}%")
        else:
            print(f"  ⚠ GBT does NOT outperform baseline ({improvement:.1f}%)")
    except Exception as e:
        print(f"  ⚠ Baseline comparison skipped: {e}")

    # Feature importance
    print("\n  Extracting feature importances...")
    gbt_stage = [s for s in best_model.stages if isinstance(s, GBTRegressor)]
    if gbt_stage:
        gbt_model = gbt_stage[0]
        assembler_stage = [
            s for s in best_model.stages if isinstance(s, VectorAssembler)
        ]
        if assembler_stage:
            feature_names = assembler_stage[0].getInputCols()
            importances = gbt_model.featureImportances
            ranked = sorted(
                zip(feature_names, importances),
                key=lambda x: x[1], reverse=True
            )
            print("  Top 5 features:")
            for feat, imp in ranked[:5]:
                print(f"    {feat}: {imp:.4f}")

    # 7. Save model
    print(f"\n[7/7] Saving model to {MODEL_OUTPUT_PATH}")
    try:
        best_model.write().overwrite().save(MODEL_OUTPUT_PATH)
        print("  ✅ Model saved successfully")
    except Exception as e:
        print(f"  ❌ Error saving model: {e}")

    spark.stop()
    print("\n  ✅ Week 6 ML job finished.")
