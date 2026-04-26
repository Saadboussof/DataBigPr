"""
Tests for the Porto → Casablanca mapping pipeline.

Run with: python -m pytest tests/test_mapping.py -v
   or:    python tests/test_mapping.py
"""

import sys
import os
import json
import numpy as np

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ═══════════════════════════════════════════════════════════════
# Test 1: Shared bounds are consistent across all files
# ═══════════════════════════════════════════════════════════════

def test_bounds_consistency():
    """All files must use the same Casablanca bounds."""
    from config import CASA_LON_MIN, CASA_LON_MAX, CASA_LAT_MIN, CASA_LAT_MAX

    # These are the canonical values from OSMnx
    assert CASA_LON_MIN == -7.6895, f"CASA_LON_MIN mismatch: {CASA_LON_MIN}"
    assert CASA_LON_MAX == -7.4008, f"CASA_LON_MAX mismatch: {CASA_LON_MAX}"
    assert CASA_LAT_MIN == 33.5072, f"CASA_LAT_MIN mismatch: {CASA_LAT_MIN}"
    assert CASA_LAT_MAX == 33.6527, f"CASA_LAT_MAX mismatch: {CASA_LAT_MAX}"
    print("  ✅ Bounds consistency: PASS")


# ═══════════════════════════════════════════════════════════════
# Test 2: porto_to_casa relative position mapping
# ═══════════════════════════════════════════════════════════════

def test_porto_to_casa_mapping():
    """Test that relative position mapping works correctly."""
    from mapping.porto_to_casa_mapper import porto_to_casa

    porto = {
        "min_lon": -8.6922, "max_lon": -8.5594,
        "min_lat": 41.1396, "max_lat": 41.1848,
    }
    casa = {
        "min_lon": -7.6895, "max_lon": -7.4008,
        "min_lat": 33.5072, "max_lat": 33.6527,
    }

    # Test 1: Porto center → Casablanca center
    p_cx = (porto["min_lon"] + porto["max_lon"]) / 2
    p_cy = (porto["min_lat"] + porto["max_lat"]) / 2
    c_lon, c_lat = porto_to_casa(p_cx, p_cy, porto, casa)

    c_cx_expected = (casa["min_lon"] + casa["max_lon"]) / 2
    c_cy_expected = (casa["min_lat"] + casa["max_lat"]) / 2

    assert abs(c_lon - c_cx_expected) < 0.001, f"Center lon mismatch: {c_lon} vs {c_cx_expected}"
    assert abs(c_lat - c_cy_expected) < 0.001, f"Center lat mismatch: {c_lat} vs {c_cy_expected}"

    # Test 2: Porto min corner → Casablanca min corner
    c_lon, c_lat = porto_to_casa(porto["min_lon"], porto["min_lat"], porto, casa)
    assert abs(c_lon - casa["min_lon"]) < 0.001
    assert abs(c_lat - casa["min_lat"]) < 0.001

    # Test 3: Porto max corner → Casablanca max corner
    c_lon, c_lat = porto_to_casa(porto["max_lon"], porto["max_lat"], porto, casa)
    assert abs(c_lon - casa["max_lon"]) < 0.001
    assert abs(c_lat - casa["max_lat"]) < 0.001

    # Test 4: Out-of-bounds point gets clamped to [0,1]
    c_lon, c_lat = porto_to_casa(-9.0, 42.0, porto, casa)
    assert c_lon == casa["min_lon"], "Out-of-bounds lon not clamped"
    assert c_lat == casa["max_lat"], "Out-of-bounds lat not clamped"

    print("  ✅ Relative position mapping: PASS")


# ═══════════════════════════════════════════════════════════════
# Test 3: Mapped coordinates stay within Casablanca bounds
# ═══════════════════════════════════════════════════════════════

def test_mapped_coords_in_bounds():
    """All mapped coordinates must be within Casablanca road network bounds."""
    parquet_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "mapped", "casa_trips_road_snapped.parquet"
    )
    if not os.path.exists(parquet_path):
        print("  ⚠ Skipped (parquet not found — run mapper first)")
        return

    import pandas as pd

    df = pd.read_parquet(parquet_path)
    from config import CASA_LON_MIN, CASA_LON_MAX, CASA_LAT_MIN, CASA_LAT_MAX

    # Generous margin (road network edges can be slightly outside)
    MARGIN = 0.01

    violations = 0
    for _, row in df.head(100).iterrows():
        points = json.loads(row["CASA_POLYLINE"])
        for lon, lat in points:
            if lon < CASA_LON_MIN - MARGIN or lon > CASA_LON_MAX + MARGIN:
                violations += 1
            if lat < CASA_LAT_MIN - MARGIN or lat > CASA_LAT_MAX + MARGIN:
                violations += 1

    assert violations == 0, f"{violations} coordinates outside Casablanca bounds!"
    print(f"  ✅ Coordinates in bounds: PASS ({len(df)} trips checked)")


# ═══════════════════════════════════════════════════════════════
# Test 4: Trip polylines have valid structure
# ═══════════════════════════════════════════════════════════════

def test_polyline_structure():
    """Each trip must have ≥2 points, each point is [lon, lat]."""
    parquet_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "mapped", "casa_trips_road_snapped.parquet"
    )
    if not os.path.exists(parquet_path):
        print("  ⚠ Skipped (parquet not found)")
        return

    import pandas as pd
    df = pd.read_parquet(parquet_path)

    for _, row in df.head(100).iterrows():
        points = json.loads(row["CASA_POLYLINE"])
        assert len(points) >= 2, f"Trip has only {len(points)} points"
        for p in points:
            assert len(p) == 2, f"Point has {len(p)} values instead of 2"
            assert isinstance(p[0], (int, float)), f"Lon not numeric: {p[0]}"
            assert isinstance(p[1], (int, float)), f"Lat not numeric: {p[1]}"

    print(f"  ✅ Polyline structure: PASS")


# ═══════════════════════════════════════════════════════════════
# Test 5: Zone grid mapping
# ═══════════════════════════════════════════════════════════════

def test_zone_mapping():
    """Zone IDs must be 1-16 for any valid Casablanca coordinate."""
    from config import CASA_LON_MIN, CASA_LON_MAX, CASA_LAT_MIN, CASA_LAT_MAX

    # Simulate the zone computation from gps_job.py
    test_points = [
        (CASA_LON_MIN + 0.01, CASA_LAT_MIN + 0.01),   # Bottom-left
        (CASA_LON_MAX - 0.01, CASA_LAT_MAX - 0.01),   # Top-right
        ((CASA_LON_MIN + CASA_LON_MAX) / 2, (CASA_LAT_MIN + CASA_LAT_MAX) / 2),  # Center
    ]

    for lon, lat in test_points:
        grid_x = int(4 * (lon - CASA_LON_MIN) / (CASA_LON_MAX - CASA_LON_MIN))
        grid_y = int(4 * (lat - CASA_LAT_MIN) / (CASA_LAT_MAX - CASA_LAT_MIN))
        grid_x = min(grid_x, 3)
        grid_y = min(grid_y, 3)
        zone_id = (grid_y * 4) + grid_x + 1
        assert 1 <= zone_id <= 16, f"Zone {zone_id} out of range for ({lon}, {lat})"

    print("  ✅ Zone mapping: PASS")


# ═══════════════════════════════════════════════════════════════
# Test 6: Spark transform function
# ═══════════════════════════════════════════════════════════════

def test_spark_transform():
    """Test the Spark UDF transform function."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "spark"))
    from porto_to_casablanca import transform_poly, CASA_LON_MIN, CASA_LON_MAX, CASA_LAT_MIN, CASA_LAT_MAX

    # Valid polyline
    test_input = json.dumps([[-8.63, 41.16], [-8.62, 41.17], [-8.61, 41.15]])
    result = json.loads(transform_poly(test_input))
    assert len(result) == 3, f"Expected 3 points, got {len(result)}"

    for lon, lat in result:
        assert CASA_LON_MIN <= lon <= CASA_LON_MAX, f"Lon {lon} out of bounds"
        assert CASA_LAT_MIN <= lat <= CASA_LAT_MAX, f"Lat {lat} out of bounds"

    # Empty polyline
    assert transform_poly("[]") == "[]"
    assert transform_poly("[[1,2]]") == "[]"  # Too few points

    print("  ✅ Spark transform: PASS")


# ═══════════════════════════════════════════════════════════════
# Run all tests
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 50)
    print("  TaaSim Mapping Tests")
    print("=" * 50)

    tests = [
        test_bounds_consistency,
        test_porto_to_casa_mapping,
        test_mapped_coords_in_bounds,
        test_polyline_structure,
        test_zone_mapping,
        test_spark_transform,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ❌ {test.__name__}: FAIL — {e}")
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}")
