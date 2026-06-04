import os
import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, broadcast, year, month, concat_ws, from_json, element_at, size, from_unixtime, floor, lit, when
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ArrayType, IntegerType

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

INPUT_PATH_PORTO = "s3a://raw/porto-trips/"
INPUT_PATH_ZONES = "s3a://raw/zone-mapping/zone_mapping.csv"
OUTPUT_PATH_PORTO = "s3a://curated/porto-trips/"

H3_RESOLUTION = 9

# Porto coordinate bounds (derived from the dataset)
PORTO_LON_MIN, PORTO_LON_MAX = -8.72, -8.55
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.14, 41.21
PORTO_LON_RANGE = PORTO_LON_MAX - PORTO_LON_MIN
PORTO_LAT_RANGE = PORTO_LAT_MAX - PORTO_LAT_MIN

# Grid: 22 Porto zones arranged in 6 columns × 4 rows (last row has 4 zones only)
ZONE_GRID_COLS = 6
ZONE_GRID_ROWS = 4
NUM_PORTO_ZONES = 22

# --- Spark Session ---
def get_spark_session():
    """Initializes and returns a Spark session configured for MinIO."""
    return SparkSession.builder \
        .appName("PortoETL") \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# --- UDFs ---
def get_h3_id(lat, lon):
    """Returns the H3 index for a given lat/lon at the configured resolution."""
    if lat is None or lon is None:
        return None
    if hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lon, H3_RESOLUTION)
    return h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

h3_udf = udf(get_h3_id, StringType())

# --- Main ETL Logic ---
def process_porto_trips(spark):
    """
    Processes the Porto trips dataset:
    1. Reads raw data and zone mapping from MinIO.
    2. Deduplicates, filters, and joins with zone data.
    3. Computes H3 IDs for start/end points.
    4. Writes curated data to MinIO, partitioned by year_month.
    """
    print(f"Reading Porto trips from {INPUT_PATH_PORTO}")
    porto_df = spark.read.option("header", "true").csv(INPUT_PATH_PORTO)

    print(f"Reading zone mapping from {INPUT_PATH_ZONES}")
    zone_schema = StructType([
        StructField("porto_zone_id", StringType(), True),
        StructField("arrondissement_id", StringType(), True),
        StructField("zone_type", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ])
    zone_df = spark.read.option("header", "true").schema(zone_schema).csv(INPUT_PATH_ZONES)

    # 1. Deduplicate and filter
    porto_df = porto_df.dropDuplicates(["TRIP_ID"])
    porto_df = porto_df.filter(col("MISSING_DATA") == "False")

    # 2. Parse POLYLINE to JSON array ONCE to optimize CPU usage
    polyline_schema = ArrayType(ArrayType(DoubleType()))
    porto_df = porto_df.withColumn("polyline_json", from_json(col("POLYLINE"), polyline_schema))
    
    # 3. Filter valid trajectories using the newly created column
    porto_df = porto_df.filter(size(col("polyline_json")) > 1)

    # 4. Get start and end coordinates from the parsed JSON array
    porto_df = porto_df.withColumn("start_point", element_at(col("polyline_json"), 1))
    porto_df = porto_df.withColumn("end_point", element_at(col("polyline_json"), -1))
    porto_df = porto_df.withColumn("start_lon", col("start_point")[0])
    porto_df = porto_df.withColumn("start_lat", col("start_point")[1])
    porto_df = porto_df.withColumn("end_lon", col("end_point")[0])
    porto_df = porto_df.withColumn("end_lat", col("end_point")[1])


    # 3. Compute H3 zone IDs (spatial index)
    print("Computing H3 zone IDs...")
    porto_df = porto_df.withColumn("start_h3_id", h3_udf(col("start_lat"), col("start_lon")))
    porto_df = porto_df.withColumn("end_h3_id", h3_udf(col("end_lat"), col("end_lon")))

    # 4. Assign Porto zone ID via coordinate grid, then map to Casablanca arrondissement
    #
    # Instead of a fragile H3 spatial join (which needs exact centroids),
    # we divide Porto's metro area into a 6x4 grid matching the 22 Porto zones.
    # Each cell maps to porto_zone_id (1-22), then zone_mapping.csv gives the
    # corresponding Casablanca arrondissement_id (1-16).
    print("Assigning Porto zone via coordinate grid...")
    col_width = PORTO_LON_RANGE / ZONE_GRID_COLS
    row_height = PORTO_LAT_RANGE / ZONE_GRID_ROWS

    porto_df = porto_df.withColumn(
        "grid_col",
        floor((col("start_lon") - PORTO_LON_MIN) / col_width).cast("int")
    ).withColumn(
        "grid_row",
        floor((col("start_lat") - PORTO_LAT_MIN) / row_height).cast("int")
    )
    porto_df = porto_df.withColumn(
        "grid_col",
        when(col("grid_col") < 0, lit(0))
        .when(col("grid_col") >= ZONE_GRID_COLS, lit(ZONE_GRID_COLS - 1))
        .otherwise(col("grid_col"))
    ).withColumn(
        "grid_row",
        when(col("grid_row") < 0, lit(0))
        .when(col("grid_row") >= ZONE_GRID_ROWS, lit(ZONE_GRID_ROWS - 1))
        .otherwise(col("grid_row"))
    )
    porto_df = porto_df.withColumn(
        "porto_zone_id",
        (col("grid_row") * ZONE_GRID_COLS + col("grid_col") + lit(1)).cast("int")
    )
    porto_df = porto_df.withColumn(
        "porto_zone_id",
        when(col("porto_zone_id") > NUM_PORTO_ZONES, lit(NUM_PORTO_ZONES))
        .otherwise(col("porto_zone_id"))
    )

    # Join with zone_mapping.csv to get Casablanca arrondissement_id
    porto_df = porto_df.join(
        broadcast(zone_df.select(
            col("porto_zone_id").cast("int").alias("map_porto_zone_id"),
            col("arrondissement_id").cast("int").alias("mapped_arrondissement_id")
        )),
        col("porto_zone_id") == col("map_porto_zone_id"),
        "left"
    )
    porto_df = porto_df.withColumn(
        "arrondissement_id",
        when(col("mapped_arrondissement_id").isNotNull(), col("mapped_arrondissement_id"))
        .otherwise(col("start_h3_id"))
    )
    porto_df = porto_df.withColumn(
        "zone_id",
        col("arrondissement_id")
    )


    # 5. Add partitioning column and write to Parquet
    porto_df = porto_df.withColumn("timestamp_ts", from_unixtime(col("TIMESTAMP")))
    porto_df = porto_df.withColumn("year", year(col("timestamp_ts")))
    porto_df = porto_df.withColumn("month", month(col("timestamp_ts")))
    porto_df = porto_df.withColumn("year_month", concat_ws("-", col("year"), col("month")))

    print(f"Dropping intermediate columns to reduce memory pressure...")
    porto_df = porto_df.drop(
        "polyline_json", "start_point", "end_point",
        "grid_col", "grid_row", "porto_zone_id",
        "map_porto_zone_id", "mapped_arrondissement_id"
    )

    print(f"Writing curated Porto data to {OUTPUT_PATH_PORTO}")
    porto_df.write \
        .partitionBy("year_month") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .option("maxRecordsPerFile", 500000) \
        .parquet(OUTPUT_PATH_PORTO)

if __name__ == "__main__":
    spark_session = get_spark_session()
    process_porto_trips(spark_session)
    spark_session.stop()
    print("Porto ETL job finished.")
