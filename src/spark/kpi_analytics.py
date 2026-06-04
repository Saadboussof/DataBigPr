
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, count, countDistinct, lit, current_timestamp, from_json, size, row_number, when, hour
from pyspark.sql.types import ArrayType, DoubleType, IntegerType

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = "taasim"
CASSANDRA_TABLE = "kpi_summary"

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
)

INPUT_PATH_PORTO = "s3a://curated/porto-trips/"
INPUT_PATH_NYC = "s3a://raw/nyc-tlc/"

# --- Spark Session ---
def get_spark_session():
    """Initializes a Spark session for MinIO and Cassandra."""
    return SparkSession.builder \
        .appName("KPIAnalytics") \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .getOrCreate()

# --- Main Analytics Logic ---
def calculate_and_save_kpis(spark):
    """
    Calculates weekly KPIs and saves them to Cassandra.
    """
    print(f"Reading curated Porto data from {INPUT_PATH_PORTO}")
    porto_df = spark.read.parquet(INPUT_PATH_PORTO)
    porto_df.createOrReplaceTempView("porto_trips")

    print(f"Reading aggregated demand data from {INPUT_PATH_NYC}")
    nyc_df = spark.read.parquet(INPUT_PATH_NYC).filter(
        col("tpep_pickup_datetime").isNotNull() & col("PULocationID").isNotNull()
    )
    nyc_df.createOrReplaceTempView("nyc_raw")

    print("Reading vehicle positions from Cassandra...")
    vehicles_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vehicle_positions", keyspace=CASSANDRA_KEYSPACE) \
        .load() \
        .groupBy("zone_id") \
        .agg(countDistinct("taxi_id").alias("available_vehicles"))
    vehicles_df.createOrReplaceTempView("vehicle_locations")


    # 1. KPI: Total trips per zone
    print("Calculating: Total trips per zone")
    total_trips_kpi = spark.sql("""
        SELECT
            zone_id,
            COUNT(TRIP_ID) AS total_trips
        FROM porto_trips
        WHERE zone_id IS NOT NULL
        GROUP BY zone_id
    """)

    # 2. KPI: Average trip duration
    print("Calculating: Average trip duration per zone")
    porto_df_duration = porto_df.withColumn(
        "duration_seconds",
        size(from_json(col("POLYLINE"), ArrayType(ArrayType(DoubleType())))) * 15
    )
    avg_duration_kpi = porto_df_duration.groupBy("zone_id") \
        .agg(avg("duration_seconds").alias("avg_duration_seconds")) \
        .select(col("zone_id"), (col("avg_duration_seconds") / 60).alias("avg_trip_duration_minutes"))


    # 3. KPI: Peak demand hours
    print("Calculating: Peak demand hours per zone")
    nyc_hourly = nyc_df.withColumn(
        "hour", hour(col("tpep_pickup_datetime"))
    ).groupBy(
        col("PULocationID").alias("zone_id"), col("hour")
    ).agg(count("*").alias("demand_count"))
    window_spec = Window.partitionBy("zone_id").orderBy(col("demand_count").desc())
    peak_hours_kpi = nyc_hourly.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select(col("zone_id"), col("hour").alias("peak_demand_hour"))

    # 4. KPI: Coverage gaps
    print("Calculating: Coverage gaps")
    nyc_zones = nyc_df.groupBy(
        col("PULocationID").alias("zone_id")
    ).agg(count("*").alias("demand_count"))
    coverage_gaps_kpi = nyc_zones \
        .join(vehicles_df, "zone_id", "left") \
        .filter(
            (col("demand_count") > 0) &
            (col("available_vehicles").isNull() | (col("available_vehicles") < 2))
        ) \
        .select(col("zone_id"), col("demand_count").alias("coverage_gap_demand"))


    # --- Join KPIs and Save to Cassandra ---
    print("Joining KPIs...")
    final_kpis = total_trips_kpi \
        .join(avg_duration_kpi, "zone_id", "outer") \
        .join(peak_hours_kpi, "zone_id", "outer") \
        .join(coverage_gaps_kpi, "zone_id", "outer") \
        .withColumn("city", lit("Casablanca")) \
        .withColumn("window_start", current_timestamp()) \
        .fillna(0)

    print(f"Writing KPIs to Cassandra table {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")
    final_kpis.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark_session = get_spark_session()
    calculate_and_save_kpis(spark_session)
    spark_session.stop()
    print("KPI Analytics job finished.")
