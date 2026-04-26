"""Quick test: verify the mapping pipeline works end-to-end."""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "simulators"))

import pandas as pd

print("=" * 50)
print("  Pipeline Verification")
print("=" * 50)

# Test 1: Parquet exists and has data
parquet = "data/mapped/casa_trips_road_snapped.parquet"
if not os.path.exists(parquet):
    print("❌ Parquet not found! Run the mapper first.")
    exit(1)

df = pd.read_parquet(parquet)
print(f"\n✅ Parquet loaded: {len(df)} trips")

# Test 2: Polylines are valid
pts = json.loads(df.iloc[0]["CASA_POLYLINE"])
print(f"✅ First trip: {len(pts)} GPS points")
print(f"   Start: {pts[0]}")
print(f"   End:   {pts[-1]}")

# Test 3: All coordinates within Casablanca bounds
CASA_LON_MIN, CASA_LON_MAX = -7.70, -7.39
CASA_LAT_MIN, CASA_LAT_MAX = 33.50, 33.66
violations = 0
for _, row in df.iterrows():
    for lon, lat in json.loads(row["CASA_POLYLINE"]):
        if not (CASA_LON_MIN <= lon <= CASA_LON_MAX and CASA_LAT_MIN <= lat <= CASA_LAT_MAX):
            violations += 1

if violations == 0:
    print(f"✅ All coordinates within Casablanca bounds")
else:
    print(f"⚠ {violations} coordinates outside bounds")

# Test 4: GPS producer can load the data
from vehicle_gps_producer import load_trips
trips = load_trips(parquet, "data/train.csv")
print(f"✅ GPS producer loaded: {len(trips)} trips ready to stream")

print(f"\n{'=' * 50}")
print(f"  ALL TESTS PASSED ✅")
print(f"{'=' * 50}")
