import os
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    hour,
    count,
    avg,
    dense_rank,
    row_number,
    from_json,
    size,
    lit,
    from_unixtime,
    udf,
)
from pyspark.sql.types import ArrayType, DoubleType

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

INPUT_PATH_NYC = "s3a://raw/nyc-tlc/"
# This expects curated Porto trips already produced by etl_porto.py
INPUT_PATH_PORTO = "s3a://curated/porto-trips/"

OUTPUT_AVG_TRIP_LENGTH = "s3a://curated/kpis/week5/avg-trip-length/"
OUTPUT_MOST_REQUESTED = "s3a://curated/kpis/week5/most-requested-zones/"
OUTPUT_PEAK_HOURS = "s3a://curated/kpis/week5/peak-congestion-hours/"

# --- Spark Session ---

def get_spark_session():
    """Initializes and returns a Spark session configured for MinIO."""
    return (
        SparkSession.builder.appName("Week5_KPI_Batch")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

# --- Distance Utilities ---

def haversine_km(lat1, lon1, lat2, lon2):
    """Returns the great-circle distance in km between two points."""
    radius_km = 6371.0088
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


def polyline_distance_km(points):
    """Returns total distance for a Porto polyline (lon, lat) list."""
    if not points or len(points) < 2:
        return None

    total = 0.0
    prev = points[0]
    for point in points[1:]:
        if not prev or not point or len(prev) < 2 or len(point) < 2:
            return None
        lon1, lat1 = prev[0], prev[1]
        lon2, lat2 = point[0], point[1]
        if None in (lon1, lat1, lon2, lat2):
            return None
        total += haversine_km(lat1, lon1, lat2, lon2)
        prev = point
    return total


polyline_distance_udf = udf(polyline_distance_km, DoubleType())

# --- KPI Builders ---

def build_nyc_kpis(spark):
    print(f"Reading NYC data from {INPUT_PATH_NYC}")
    nyc_df = spark.read.parquet(INPUT_PATH_NYC)

    base_df = (
        nyc_df.select(
            col("PULocationID").cast("int").alias("zone_id"),
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("trip_distance").cast("double").alias("trip_distance_miles"),
        )
        .filter(col("zone_id").isNotNull())
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("trip_distance_miles") > 0)
        .withColumn("trip_length_km", col("trip_distance_miles") * lit(1.60934))
        .withColumn("hour", hour(col("pickup_datetime")))
    )

    avg_trip = (
        base_df.groupBy("zone_id")
        .agg(
            avg("trip_length_km").alias("avg_trip_length_km"),
            count("*").alias("trip_count"),
        )
        .withColumn("city", lit("NYC"))
    )

    most_requested = base_df.groupBy("zone_id").agg(count("*").alias("request_count"))
    rank_window = Window.orderBy(col("request_count").desc())
    most_requested = most_requested.withColumn(
        "rank", dense_rank().over(rank_window)
    ).withColumn("city", lit("NYC"))

    zone_hour = base_df.groupBy("zone_id", "hour").agg(
        count("*").alias("demand_count")
    )
    peak_window = Window.partitionBy("zone_id").orderBy(
        col("demand_count").desc()
    )
    peak_hours = (
        zone_hour.withColumn("rank", row_number().over(peak_window))
        .filter(col("rank") == 1)
        .select(
            col("zone_id"),
            col("hour").alias("peak_hour"),
            col("demand_count").alias("peak_demand_count"),
        )
        .withColumn("city", lit("NYC"))
    )

    return avg_trip, most_requested, peak_hours


def build_porto_kpis(spark):
    print(f"Reading Porto data from {INPUT_PATH_PORTO}")
    porto_df = spark.read.parquet(INPUT_PATH_PORTO)

    zone_col = "zone_id" if "zone_id" in porto_df.columns else "arrondissement_id"
    base_df = (
        porto_df.select(
            col(zone_col).alias("zone_id"),
            col("TIMESTAMP").cast("long").alias("timestamp_unix"),
            col("POLYLINE"),
        )
        .filter(col("zone_id").isNotNull())
        .filter(col("timestamp_unix").isNotNull())
        .withColumn("pickup_datetime", from_unixtime(col("timestamp_unix")).cast("timestamp"))
        .filter(col("pickup_datetime").isNotNull())
        .withColumn("hour", hour(col("pickup_datetime")))
    )

    polyline_schema = ArrayType(ArrayType(DoubleType()))
    base_df = base_df.withColumn(
        "polyline_json", from_json(col("POLYLINE"), polyline_schema)
    ).filter(size(col("polyline_json")) > 1)

    base_df = base_df.withColumn(
        "trip_length_km", polyline_distance_udf(col("polyline_json"))
    ).filter(col("trip_length_km").isNotNull())

    avg_trip = (
        base_df.groupBy("zone_id")
        .agg(
            avg("trip_length_km").alias("avg_trip_length_km"),
            count("*").alias("trip_count"),
        )
        .withColumn("city", lit("Porto"))
    )

    most_requested = base_df.groupBy("zone_id").agg(count("*").alias("request_count"))
    rank_window = Window.orderBy(col("request_count").desc())
    most_requested = most_requested.withColumn(
        "rank", dense_rank().over(rank_window)
    ).withColumn("city", lit("Porto"))

    zone_hour = base_df.groupBy("zone_id", "hour").agg(
        count("*").alias("demand_count")
    )
    peak_window = Window.partitionBy("zone_id").orderBy(
        col("demand_count").desc()
    )
    peak_hours = (
        zone_hour.withColumn("rank", row_number().over(peak_window))
        .filter(col("rank") == 1)
        .select(
            col("zone_id"),
            col("hour").alias("peak_hour"),
            col("demand_count").alias("peak_demand_count"),
        )
        .withColumn("city", lit("Porto"))
    )

    return avg_trip, most_requested, peak_hours


if __name__ == "__main__":
    spark = get_spark_session()

    nyc_avg, nyc_requested, nyc_peak = build_nyc_kpis(spark)
    porto_avg, porto_requested, porto_peak = build_porto_kpis(spark)

    avg_all = nyc_avg.select(
        "city", "zone_id", "avg_trip_length_km", "trip_count"
    ).unionByName(
        porto_avg.select("city", "zone_id", "avg_trip_length_km", "trip_count")
    )

    requested_all = nyc_requested.select(
        "city", "zone_id", "request_count", "rank"
    ).unionByName(
        porto_requested.select("city", "zone_id", "request_count", "rank")
    )

    peak_all = nyc_peak.select(
        "city", "zone_id", "peak_hour", "peak_demand_count"
    ).unionByName(
        porto_peak.select("city", "zone_id", "peak_hour", "peak_demand_count")
    )

    print(f"Writing avg trip length KPI to {OUTPUT_AVG_TRIP_LENGTH}")
    avg_all.write.mode("overwrite").partitionBy("city").parquet(OUTPUT_AVG_TRIP_LENGTH)

    print(f"Writing most requested zones KPI to {OUTPUT_MOST_REQUESTED}")
    requested_all.write.mode("overwrite").partitionBy("city").parquet(OUTPUT_MOST_REQUESTED)

    print(f"Writing peak congestion hours KPI to {OUTPUT_PEAK_HOURS}")
    peak_all.write.mode("overwrite").partitionBy("city").parquet(OUTPUT_PEAK_HOURS)

    spark.stop()
    print("Week 5 KPI batch job finished.")
