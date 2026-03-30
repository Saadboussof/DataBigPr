
# ==============================================================================
# FIXES FOR week1_exploration.ipynb
# Copy each section below into the corresponding cell in Jupyter
# ==============================================================================


# ==============================================================================
# FIX 1 — TRIP DURATION HISTOGRAM (Cell with count_points / count_udf)
# Problem: json.loads used without importing json inside UDF → empty chart
# Solution: add "import json" inside the function body
#
# FIND the cell that starts with:
#   def count_points(polyline_str):
#
# REPLACE the ENTIRE cell with the code below:
# ==============================================================================

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import IntegerType

# Trip duration: each GPS polyline point = 15 seconds apart
# CRITICAL: "import json" must be INSIDE the UDF function.
# PySpark serializes UDFs to worker nodes — outer module imports are NOT available.
def count_points(polyline_str):
    import json  # <-- THIS IS THE FIX
    try:
        pts = json.loads(polyline_str)
        return len(pts)
    except:
        return 0

count_udf = udf(count_points, IntegerType())

df_duration = df_clean.withColumn('num_points', count_udf(col('POLYLINE'))) \
                       .withColumn('duration_min', col('num_points') * 15 / 60)

df_duration.select('duration_min').summary().show()

# Plot — only trips with at least 1 GPS point and under 60 minutes
durations = df_duration.select('duration_min') \
    .filter((col('duration_min') > 0) & (col('duration_min') < 60)) \
    .limit(50000).toPandas()

plt.figure(figsize=(12, 4))
plt.hist(durations['duration_min'], bins=60, color='steelblue', edgecolor='white')
plt.title('Porto Taxi Trip Duration Distribution (minutes)', fontsize=14)
plt.xlabel('Duration (min)')
plt.ylabel('Number of Trips')
plt.tight_layout()
plt.savefig('/home/jovyan/work/trip_duration_dist.png', dpi=100)
plt.show()
print('Chart saved!')


# ==============================================================================
# FIX 2 — PARQUET WRITE (Cell with df_full.write.mode('overwrite').parquet(...))
# Problem: Spark cannot delete existing directory on Docker-mounted Windows volume
# Solution: manually delete the directory with shutil BEFORE writing
#
# FIND the cell that starts with:
#   OUTPUT_PATH = '/home/jovyan/data/porto_casablanca_parquet'
#
# REPLACE the ENTIRE cell with the code below:
# ==============================================================================

import shutil, os, time

OUTPUT_PATH = '/home/jovyan/data/porto_casablanca_parquet'

# Manually remove existing directory — Spark's 'overwrite' fails on Docker volumes
if os.path.exists(OUTPUT_PATH):
    shutil.rmtree(OUTPUT_PATH)
    print(f'Cleared existing output at {OUTPUT_PATH}')

t0 = time.time()

df_full = df_clean \
    .withColumn('CASA_POLYLINE', remap_udf(col('POLYLINE'))) \
    .filter(col('CASA_POLYLINE') != '[]') \
    .drop('POLYLINE')

# Note: no .mode('overwrite') needed since we manually cleared the path
df_full.write.parquet(OUTPUT_PATH)

elapsed = time.time() - t0
print(f'Done in {elapsed:.1f}s — full transformed dataset saved to {OUTPUT_PATH}')


# ==============================================================================
# BONUS — NYC YELLOW TAXI DATA (Add as a NEW cell at the bottom)
# Make sure you put the parquet files in /home/jovyan/data/ first.
#
# To download inside Jupyter terminal, run:
#   cd /home/jovyan/data
#   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
#   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
#   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
# ==============================================================================

# Check files exist first
import os
files = [f'/home/jovyan/data/yellow_tripdata_2024-0{m}.parquet' for m in [1,2,3]]
for f in files:
    status = 'OK' if os.path.exists(f) else 'MISSING'
    print(f'{status} — {f}')

# Load all 3 months at once
NYC_PATH = '/home/jovyan/data/yellow_tripdata_2024-0*.parquet'
df_nyc = spark.read.parquet(NYC_PATH)

print('\nNYC Yellow Taxi (Jan–Mar 2024)')
print('Total rows:', df_nyc.count())
print('Columns:', df_nyc.columns)
df_nyc.printSchema()

# Quick stats
df_nyc.select('trip_distance', 'fare_amount', 'passenger_count').summary().show()
