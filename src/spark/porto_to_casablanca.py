"""
Spark ETL: Porto → Casablanca coordinate remapping.
Updated to use OSMnx road-network bounds (matching the new mapper v2).

NOTE: For road-snapped mapping, use src/mapping/porto_to_casa_mapper.py instead.
      This Spark job does simple linear mapping for batch processing on MinIO.
"""

import sys
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Bounds from OSMnx road network (matches all other files)
PORTO_LON_MIN, PORTO_LON_MAX = -8.6922, -8.5594
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.1396, 41.1848

CASA_LON_MIN, CASA_LON_MAX = -7.6895, -7.4008
CASA_LAT_MIN, CASA_LAT_MAX = 33.5072, 33.6527

DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
DEFAULT_MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
DEFAULT_MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password"))
DEFAULT_SPARK_PACKAGES = os.getenv(
    "SPARK_EXTRA_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
)

DEFAULT_INPUT_PATH = "s3a://raw/porto/train.csv"
DEFAULT_OUTPUT_PATH = "s3a://curated/porto/porto_casablanca_parquet"


def transform_poly(points_str):
    """Map Porto GPS points to Casablanca using relative-position mapping."""
    try:
        points = json.loads(points_str)
        if not points or len(points) < 2:
            return "[]"

        mapped = []
        for lon, lat in points:
            rel_lon = (lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
            rel_lat = (lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
            rel_lon = max(0.0, min(1.0, rel_lon))
            rel_lat = max(0.0, min(1.0, rel_lat))

            c_lon = CASA_LON_MIN + rel_lon * (CASA_LON_MAX - CASA_LON_MIN)
            c_lat = CASA_LAT_MIN + rel_lat * (CASA_LAT_MAX - CASA_LAT_MIN)
            mapped.append([round(c_lon, 6), round(c_lat, 6)])

        return json.dumps(mapped) if len(mapped) >= 2 else "[]"
    except Exception:
        return "[]"


def build_spark_session(app_name):
    ssl_enabled = str(DEFAULT_MINIO_ENDPOINT.startswith("https")).lower()

    builder = SparkSession.builder.appName(app_name)

    if DEFAULT_SPARK_PACKAGES:
        builder = builder.config("spark.jars.packages", DEFAULT_SPARK_PACKAGES)

    return builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", DEFAULT_MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", DEFAULT_MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", DEFAULT_MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main(input_csv, output_parquet):
    spark = build_spark_session("PortoToCasablancaRemapper")
        
    print(f"Reading {input_csv}...")
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    
    df = df.filter(col("MISSING_DATA") == False)
    
    transform_udf = udf(transform_poly, StringType())
    
    print("Applying geospatial transformation...")
    transformed_df = df.withColumn("CASA_POLYLINE", transform_udf(col("POLYLINE"))) \
        .filter(col("CASA_POLYLINE") != "[]")
    
    print(f"Writing to {output_parquet}...")
    transformed_df.write.mode("overwrite").parquet(output_parquet)
    
    print("Done!")
    spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Remap Porto raw trips to Casablanca and write curated parquet using MinIO (s3a)."
    )
    parser.add_argument("input_csv", nargs="?", default=DEFAULT_INPUT_PATH)
    parser.add_argument("output_parquet", nargs="?", default=DEFAULT_OUTPUT_PATH)

    args = parser.parse_args()
    main(args.input_csv, args.output_parquet)
