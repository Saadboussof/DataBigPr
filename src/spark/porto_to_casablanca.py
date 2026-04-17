import sys
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Casablanca and Porto bounding boxes (same as simulator)
PORTO_LON_MIN, PORTO_LON_MAX = -8.69, -8.56
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.14, 41.20

CASA_LON_MIN, CASA_LON_MAX = -7.80, -7.40
CASA_LAT_MIN, CASA_LAT_MAX = 33.40, 33.70

DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
DEFAULT_MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
DEFAULT_MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password"))
DEFAULT_SPARK_PACKAGES = os.getenv(
    "SPARK_EXTRA_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
)

DEFAULT_INPUT_PATH = "s3a://raw/porto/train.csv"
DEFAULT_OUTPUT_PATH = "s3a://curated/porto/porto_casablanca_parquet"

# Approximate Casablanca coastline polyline inside the project bounding box.
# Coordinates are [lon, lat].
COASTLINE_POLYLINE = [
    [-7.47, 33.70],
    [-7.52, 33.69],
    [-7.56, 33.67],
    [-7.60, 33.65],
    [-7.64, 33.62],
    [-7.68, 33.59],
    [-7.72, 33.57],
    [-7.76, 33.55],
    [-7.79, 33.53],
    [-7.74, 33.40],
]

# Land polygon: coastline plus east/south closure of the Casablanca bbox.
LAND_POLYGON = COASTLINE_POLYLINE + [
    [CASA_LON_MAX, CASA_LAT_MIN],
    [CASA_LON_MAX, CASA_LAT_MAX],
]

COASTLINE_EPSILON = 0.001
INLAND_NUDGE = 2e-5
MIN_GEOM_DENOM = 1e-12

LAND_CENTROID_LON = sum(p[0] for p in LAND_POLYGON) / len(LAND_POLYGON)
LAND_CENTROID_LAT = sum(p[1] for p in LAND_POLYGON) / len(LAND_POLYGON)

def clamp_to_bbox(lon, lat):
    lon = min(max(lon, CASA_LON_MIN), CASA_LON_MAX)
    lat = min(max(lat, CASA_LAT_MIN), CASA_LAT_MAX)
    return lon, lat

def point_in_polygon(lon, lat, polygon):
    inside = False
    j = len(polygon) - 1

    for i in range(len(polygon)):
        xi, yi = polygon[i]
        xj, yj = polygon[j]

        if (yi > lat) != (yj > lat):
            x_intersect = (xj - xi) * (lat - yi) / ((yj - yi) + MIN_GEOM_DENOM) + xi
            if lon < x_intersect:
                inside = not inside

        j = i

    return inside

def point_to_segment_projection(lon, lat, seg_start, seg_end):
    x1, y1 = seg_start
    x2, y2 = seg_end
    dx = x2 - x1
    dy = y2 - y1
    seg_len_sq = dx * dx + dy * dy

    if seg_len_sq < MIN_GEOM_DENOM:
        return [x1, y1], (lon - x1) ** 2 + (lat - y1) ** 2

    t = ((lon - x1) * dx + (lat - y1) * dy) / seg_len_sq
    t = max(0.0, min(1.0, t))
    px = x1 + t * dx
    py = y1 + t * dy
    return [px, py], (lon - px) ** 2 + (lat - py) ** 2

def point_to_segment_distance_sq(lon, lat, seg_start, seg_end):
    return point_to_segment_projection(lon, lat, seg_start, seg_end)[1]

def distance_to_coastline_sq(lon, lat):
    best = float("inf")
    for i in range(len(COASTLINE_POLYLINE) - 1):
        d = point_to_segment_distance_sq(lon, lat, COASTLINE_POLYLINE[i], COASTLINE_POLYLINE[i + 1])
        if d < best:
            best = d
    return best

def is_land_point(lon, lat, epsilon=COASTLINE_EPSILON):
    if point_in_polygon(lon, lat, LAND_POLYGON):
        return True

    return distance_to_coastline_sq(lon, lat) <= (epsilon * epsilon)

def project_to_land_boundary(lon, lat):
    best_point = None
    best_dist = float("inf")

    for i in range(len(LAND_POLYGON)):
        a = LAND_POLYGON[i]
        b = LAND_POLYGON[(i + 1) % len(LAND_POLYGON)]
        projected, d = point_to_segment_projection(lon, lat, a, b)
        if d < best_dist:
            best_dist = d
            best_point = projected

    if best_point is None:
        return clamp_to_bbox(lon, lat)

    return clamp_to_bbox(best_point[0], best_point[1])

def nudge_inside_land(lon, lat):
    if point_in_polygon(lon, lat, LAND_POLYGON):
        return lon, lat

    dx = LAND_CENTROID_LON - lon
    dy = LAND_CENTROID_LAT - lat
    norm = (dx * dx + dy * dy) ** 0.5
    if norm < MIN_GEOM_DENOM:
        return lon, lat

    ux = dx / norm
    uy = dy / norm

    step = INLAND_NUDGE
    for _ in range(10):
        candidate_lon = lon + ux * step
        candidate_lat = lat + uy * step
        candidate_lon, candidate_lat = clamp_to_bbox(candidate_lon, candidate_lat)
        if point_in_polygon(candidate_lon, candidate_lat, LAND_POLYGON):
            return candidate_lon, candidate_lat
        step *= 2.0

    return lon, lat

def coerce_to_land_point(lon, lat):
    lon, lat = clamp_to_bbox(lon, lat)

    if point_in_polygon(lon, lat, LAND_POLYGON):
        return [lon, lat]

    # Points accepted only by coastline epsilon are projected to the shoreline,
    # then nudged a few meters inland to guarantee strict polygon inclusion.
    lon, lat = project_to_land_boundary(lon, lat)
    lon, lat = nudge_inside_land(lon, lat)
    return [lon, lat]

def segment_intersection_with_t(p1, p2, q1, q2):
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = q1
    x4, y4 = q2

    rx, ry = (x2 - x1), (y2 - y1)
    sx, sy = (x4 - x3), (y4 - y3)
    denom = rx * sy - ry * sx

    if abs(denom) < MIN_GEOM_DENOM:
        return None

    qpx, qpy = (x3 - x1), (y3 - y1)
    t = (qpx * sy - qpy * sx) / denom
    u = (qpx * ry - qpy * rx) / denom

    if 0.0 <= t <= 1.0 and 0.0 <= u <= 1.0:
        return [x1 + t * rx, y1 + t * ry], t

    return None

def first_polygon_intersection(p1, p2, polygon):
    best_point = None
    best_t = 2.0

    for i in range(len(polygon)):
        q1 = polygon[i]
        q2 = polygon[(i + 1) % len(polygon)]
        hit = segment_intersection_with_t(p1, p2, q1, q2)
        if hit is None:
            continue

        point, t = hit
        if t < best_t:
            best_t = t
            best_point = point

    if best_point is None:
        return None

    return [best_point[0], best_point[1]]

def clip_polyline_to_land(mapped_points):
    """Keep valid land points and clip trajectories at first ocean crossing."""
    cleaned = []

    for lon, lat in mapped_points:
        point = [lon, lat]

        if not cleaned:
            if not is_land_point(lon, lat):
                continue
            cleaned.append(coerce_to_land_point(lon, lat))
            continue

        prev = cleaned[-1]
        if not is_land_point(lon, lat):
            boundary = first_polygon_intersection(prev, point, LAND_POLYGON)
            if boundary is not None:
                cleaned.append(coerce_to_land_point(boundary[0], boundary[1]))
            break

        cleaned.append(coerce_to_land_point(lon, lat))

    if len(cleaned) < 2:
        return []

    return [[lon, lat] for lon, lat in cleaned]

def transform_poly(points_str):
    """Map Porto GPS points to Casablanca and clean ocean anomalies in Silver."""
    try:
        points = json.loads(points_str)
        mapped = []

        for lon, lat in points:
            if not (PORTO_LON_MIN <= lon <= PORTO_LON_MAX and PORTO_LAT_MIN <= lat <= PORTO_LAT_MAX):
                continue

            lon_norm = (lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
            lat_norm = (lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
            c_lon = CASA_LON_MIN + (lon_norm * (CASA_LON_MAX - CASA_LON_MIN))
            c_lat = CASA_LAT_MIN + (lat_norm * (CASA_LAT_MAX - CASA_LAT_MIN))
            mapped.append([c_lon, c_lat])

        cleaned = clip_polyline_to_land(mapped)
        return json.dumps(cleaned) if cleaned else "[]"
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
    
    # Filter MISSING_DATA = False
    df = df.filter(col("MISSING_DATA") == False)
    
    # Register UDF for Python-based transformation
    transform_udf = udf(transform_poly, StringType())
    
    print("Applying geospatial transformation (this may take a while)...")
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
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=DEFAULT_INPUT_PATH,
        help=f"Input CSV path (default: {DEFAULT_INPUT_PATH})",
    )
    parser.add_argument(
        "output_parquet",
        nargs="?",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output parquet path (default: {DEFAULT_OUTPUT_PATH})",
    )

    args = parser.parse_args()
    main(args.input_csv, args.output_parquet)
