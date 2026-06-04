
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

INPUT_PATH_NYC = "s3a://raw/nyc-tlc/"
OUTPUT_PATH_NYC = "s3a://curated/demand-by-zone/"

# --- Spark Session ---
def get_spark_session():
    """Initializes and returns a Spark session configured for MinIO."""
    return SparkSession.builder \
        .appName("NYCDemandETL") \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

# --- Main ETL Logic ---
def process_nyc_demand(spark):
    """
    Processes the NYC TLC dataset to calculate hourly demand per zone.
    1. Reads raw Parquet data from MinIO.
    2. Aggregates trip counts by pickup location and hour.
    3. Writes the aggregated data back to MinIO.
    """
    print(f"Reading NYC TLC data from {INPUT_PATH_NYC}")
    nyc_df = spark.read.parquet(INPUT_PATH_NYC)

    nyc_df = nyc_df.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("PULocationID").isNotNull()
    )

    print("Aggregating demand by zone and hour...")
    demand_df = nyc_df.groupBy(
        col("PULocationID").alias("zone_id"),
        hour(col("tpep_pickup_datetime")).alias("hour")
    ).agg(
        count("*").alias("demand_count")
    )

    print(f"Writing aggregated demand data to {OUTPUT_PATH_NYC}")
    demand_df.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH_NYC)

if __name__ == "__main__":
    spark_session = get_spark_session()
    process_nyc_demand(spark_session)
    spark_session.stop()
    print("NYC Demand ETL job finished.")
