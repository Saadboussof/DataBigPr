import os
from datetime import datetime
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

# Globals to cache the Spark Session and Pipeline Model
_spark = None
_model = None

def init_spark_and_model():
    """Download ML Model from MinIO and initialize PySpark Session."""
    global _spark, _model
    
    # Check if model exists in the local caching directory or S3
    model_path = "s3a://ml-store/models/gbt_hour_zone.model"
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel
        
        print("Initializing Spark Session for ML inference...")
        _spark = SparkSession.builder \
            .appName("TaaSim-Inference-API") \
            .master("local[1]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        print(f"Loading PipelineModel from {model_path}...")
        _model = PipelineModel.load(model_path)
        print("✅ Spark ML Model loaded successfully!")
    except Exception as e:
        print(f"⚠️ Could not load Spark ML model: {e}")
        print("Falling back to Rule-Based heuristic model.")
        _spark = None
        _model = None

def predict_demand(zone_id: int, dt_str: str):
    try:
        dt = datetime.fromisoformat(dt_str)
    except Exception:
        # If timezone suffix like Z or +00:00 causes parsing issues in old python
        # replace Z with +00:00 or parse robustly
        cleaned_dt_str = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned_dt_str)

    hour = dt.hour
    day_of_week = dt.isoweekday()
    # In pyspark dayofweek returns: 1 = Sunday, 2 = Monday, ..., 7 = Saturday
    # In python isoweekday() returns: 1 = Monday, ..., 7 = Sunday
    # Wait! Let's check how the features were engineered in the pipeline:
    # "day_of_week", dayofweek(col("time_window"))
    # dayofweek in PySpark maps Sunday to 1, Monday to 2, etc.
    # Let's map our Python day_of_week to match PySpark's dayofweek:
    # Python isoweekday() maps Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6, Sun=7.
    # PySpark dayofweek maps Sun=1, Mon=2, Tue=3, Wed=4, Thu=5, Fri=6, Sat=7.
    pyspark_day_of_week = (day_of_week % 7) + 1
    
    # "is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
    # Since pyspark_day_of_week is Sun=1 and Sat=7, this matches exactly!
    is_weekend = 1 if pyspark_day_of_week in [1, 7] else 0
    
    # "is_friday", when(col("day_of_week") == 6, 1).otherwise(0)
    # Since pyspark_day_of_week maps Friday to 6, this matches exactly!
    is_friday = 1 if pyspark_day_of_week == 6 else 0
    
    # --- Try Spark ML Inference ---
    if _model is not None and _spark is not None:
        try:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
            
            schema = StructType([
                StructField("zone_id", StringType()),
                StructField("hour_of_day", IntegerType()),
                StructField("day_of_week", IntegerType()),
                StructField("is_weekend", IntegerType()),
                StructField("is_friday", IntegerType()),
                StructField("zone_population_density", IntegerType()),
                StructField("zone_type", StringType()),
                StructField("is_raining", IntegerType()),
                StructField("temperature_bucket", StringType()),
                StructField("demand_lag_1d", DoubleType()),
                StructField("demand_lag_7d", DoubleType()),
                StructField("rolling_7d_mean", DoubleType()),
            ])
            
            # Format inputs matching Hicham's VectorAssembler exactly
            input_df = _spark.createDataFrame([(
                str(zone_id),  # zone_id must match MLIndexer string format
                hour,
                pyspark_day_of_week,
                is_weekend,
                is_friday,
                1000,          # Population density fallback
                "commercial",  # Zone type fallback
                0,             # Rain fallback
                "mild",        # Temperature bucket
                0.0,           # Lag fallbacks
                0.0,
                0.0
            )], schema=schema)
            
            pred_df = _model.transform(input_df)
            prediction_val = pred_df.select("prediction").collect()[0][0]
            
            return {
                "zone_id": zone_id,
                "datetime": dt_str,
                "predicted_demand": int(max(0, prediction_val)),
                "confidence": "high",
                "model_version": "Spark_GBT_v1"
            }
        except Exception as ex:
            print(f"Spark inference failed, utilizing heuristic: {ex}")
            
    # --- Heuristic Fallback (Normal operating bounds) ---
    base_demand = 5
    if (7 <= hour <= 9) or (17 <= hour <= 19):
        # Peak congestion multiplication
        predicted = base_demand * 4.2 + (zone_id % 3)
    elif 0 <= hour <= 5:
        # Off-peak night
        predicted = base_demand * 0.4
    else:
        # Normal daytime
        predicted = base_demand * 1.8
        
    return {
        "zone_id": zone_id,
        "datetime": dt_str,
        "predicted_demand": int(predicted),
        "confidence": "medium",
        "model_version": "Heuristic_Rule_v1"
    }
