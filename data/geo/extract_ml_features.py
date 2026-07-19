import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, hour, from_unixtime, when, dayofweek

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
TRIPS_INPUT_PATH = "s3a://curated/porto-trips/"

spark = (
    SparkSession.builder.appName("Extract_ML_Features")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    print("Loading curated trips from MinIO...")
    trips_df = spark.read.parquet(TRIPS_INPUT_PATH)
    
    # Setup zone_id column
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
    
    # Hourly demand aggregation per zone
    demand_df = (
        base_df.groupBy(
            date_trunc("hour", col("pickup_time")).alias("time_window"),
            col("zone_id"),
        )
        .count()
        .withColumnRenamed("count", "demand")
    )
    
    # Feature Engineering (Temporal Features)
    features_df = demand_df.withColumn("hour_of_day", hour(col("time_window")))
    features_df = features_df.withColumn("day_of_week", dayofweek(col("time_window")))
    features_df = features_df.withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    features_df = features_df.withColumn("is_friday", when(col("day_of_week") == 6, 1).otherwise(0))
    
    # Collect a small sample of 5 rows
    sample_rows = features_df.limit(5).collect()
    result = []
    for row in sample_rows:
        row_dict = {}
        for field in features_df.schema.fieldNames():
            val = row[field]
            if hasattr(val, 'isoformat'):
                val = val.isoformat()
            row_dict[field] = val
        result.append(row_dict)
        
    out_dir = "/app/geo/donnees_extraits"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "ml_features_sample.json"), "w") as f:
        json.dump(result, f, indent=2)
    print("ML training features sample exported successfully!")
except Exception as e:
    print("Error during PySpark execution:", e)
finally:
    spark.stop()
