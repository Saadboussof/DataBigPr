import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf
from pyspark.sql.types import ArrayType, FloatType, StringType

# Casablanca and Porto bounding boxes (same as simulator)
PORTO_LON_MIN, PORTO_LON_MAX = -8.69, -8.56
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.14, 41.20

CASA_LON_MIN, CASA_LON_MAX = -7.80, -7.40
CASA_LAT_MIN, CASA_LAT_MAX = 33.40, 33.70

def transform_poly(points_str):
    """Parses JSON array string of [[lon,lat],...] and applies mapping"""
    import json
    try:
        points = json.loads(points_str)
        mapped = []
        for lon, lat in points:
            lon_norm = (lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
            lat_norm = (lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
            c_lon = CASA_LON_MIN + (lon_norm * (CASA_LON_MAX - CASA_LON_MIN))
            c_lat = CASA_LAT_MIN + (lat_norm * (CASA_LAT_MAX - CASA_LAT_MIN))
            mapped.append([c_lon, c_lat])
        return json.dumps(mapped)
    except:
        return "[]"

def main(input_csv, output_parquet):
    spark = SparkSession.builder \
        .appName("PortoToCasablancaRemapper") \
        .getOrCreate()
        
    print(f"Reading {input_csv}...")
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    
    # Filter MISSING_DATA = False
    df = df.filter(col("MISSING_DATA") == False)
    
    # Register UDF for Python-based transformation
    transform_udf = udf(transform_poly, StringType())
    
    print("Applying geospatial transformation (this may take a while)...")
    transformed_df = df.withColumn("CASA_POLYLINE", transform_udf(col("POLYLINE")))
    
    print(f"Writing to {output_parquet}...")
    transformed_df.write.mode("overwrite").parquet(output_parquet)
    
    print("Done!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit porto_to_casablanca.py <input_csv> <output_parquet>")
        sys.exit(1)
        
    main(sys.argv[1], sys.argv[2])
