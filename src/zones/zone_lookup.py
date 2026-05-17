"""
Casablanca Zone Lookup — Real Arrondissement Boundaries
========================================================
Loads the GeoJSON file of Casablanca's 16 arrondissements and provides
a fast point-in-polygon lookup to determine which zone a GPS coordinate
belongs to.

Usage:
    from zone_lookup import ZoneLookup
    zl = ZoneLookup("path/to/arrondissements.geojson")
    zone = zl.get_zone(lat=33.58, lon=-7.62)
    # Returns: {"zone_id": 2, "zone_name": "Anfa", "prefecture": "Anfa"}
"""

import json
import os

# ═══════════════════════════════════════════════════════════════════
# Zone ID mapping — assigns a stable numeric ID to each arrondissement.
# This ensures that zone_id stays consistent across all components
# (Flink, Cassandra, Grafana, Spark ML).
# ═══════════════════════════════════════════════════════════════════
ZONE_NAME_TO_ID = {
    "Mechouar Casablanca": 1,
    "Anfa": 2,
    "Hay Hassani": 3,
    "Sidi Moumen": 4,
    "Moulay Rachid": 5,
    "Sidi Othmane": 6,
    "Ben M'Sick": 7,
    "Sbata": 8,
    "Aîn-Chock": 9,
    "Al Fida": 10,
    "Mers Sultan": 11,
    "Assoukhour Assawda": 12,
    "Hay Mohammadi": 13,
    "Aïn Sebaâ": 14,
    "Sidi Bernoussi": 15,
    "El Maarif": 16,
    "Sidi Belyout": 17,
}


def _clean_name(raw_name):
    """
    Extract a short, clean arrondissement name from the GeoJSON's 
    long Arabic+French name string.
    
    Example: "arrondissement d'Anfa (أنفا (المقاطعة" → "Anfa"
    """
    # Handle special cases first
    if "Mechouar" in raw_name:
        return "Mechouar Casablanca"
    if "Anfa" in raw_name and "Maarif" not in raw_name and "Belyout" not in raw_name:
        return "Anfa"
    if "Hay Hassani" in raw_name:
        return "Hay Hassani"
    if "Sidi Moumen" in raw_name:
        return "Sidi Moumen"
    if "Moulay Rachid" in raw_name:
        return "Moulay Rachid"
    if "Sidi Othmane" in raw_name:
        return "Sidi Othmane"
    if "Ben M" in raw_name and "Sick" in raw_name:
        return "Ben M'Sick"
    if "Sbata" in raw_name:
        return "Sbata"
    if "Chock" in raw_name or "Chok" in raw_name:
        return "Aîn-Chock"
    if "Fida" in raw_name:
        return "Al Fida"
    if "Mers Sultan" in raw_name:
        return "Mers Sultan"
    if "Assoukhour" in raw_name or "Sawda" in raw_name:
        return "Assoukhour Assawda"
    if "Hay Mohammadi" in raw_name:
        return "Hay Mohammadi"
    if "Seba" in raw_name:
        return "Aïn Sebaâ"
    if "Bernoussi" in raw_name:
        return "Sidi Bernoussi"
    if "Maarif" in raw_name:
        return "El Maarif"
    if "Belyout" in raw_name:
        return "Sidi Belyout"
    
    # Fallback: return the raw name
    return raw_name


class ZoneLookup:
    """
    Fast point-in-polygon lookup for Casablanca arrondissements.
    
    Loads once, queries many times. Uses Shapely for geometry operations.
    Falls back to a bounding-box check if Shapely is not installed.
    """

    def __init__(self, geojson_path):
        """Load the GeoJSON and build polygon index."""
        if not os.path.exists(geojson_path):
            raise FileNotFoundError(f"GeoJSON not found: {geojson_path}")

        with open(geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.zones = []
        
        try:
            from shapely.geometry import shape, Point
            self._use_shapely = True
        except ImportError:
            self._use_shapely = False
            print("  ⚠ Shapely not installed. Using bounding-box fallback (less accurate).")

        for feature in data["features"]:
            props = feature["properties"]
            raw_name = props.get("Arrondissement", "Unknown")
            clean = _clean_name(raw_name)
            zone_id = ZONE_NAME_TO_ID.get(clean, 0)

            zone_entry = {
                "zone_id": zone_id,
                "zone_name": clean,
                "prefecture": props.get("Prefecture", "Unknown"),
                "population": props.get("Population", 0),
            }

            if self._use_shapely:
                from shapely.geometry import shape
                zone_entry["polygon"] = shape(feature["geometry"])
            else:
                # Bounding box fallback
                coords = feature["geometry"]["coordinates"][0]
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                zone_entry["bbox"] = {
                    "min_lon": min(lons), "max_lon": max(lons),
                    "min_lat": min(lats), "max_lat": max(lats),
                }

            self.zones.append(zone_entry)

        print(f"  ✅ Loaded {len(self.zones)} arrondissements from {os.path.basename(geojson_path)}")

    def get_zone(self, lat, lon):
        """
        Find which arrondissement a GPS point (lat, lon) belongs to.
        
        Returns:
            dict with keys: zone_id, zone_name, prefecture
            Returns None if the point is outside all arrondissements.
        """
        if self._use_shapely:
            from shapely.geometry import Point
            point = Point(lon, lat)  # Shapely uses (x=lon, y=lat)
            for zone in self.zones:
                if zone["polygon"].contains(point):
                    return {
                        "zone_id": zone["zone_id"],
                        "zone_name": zone["zone_name"],
                        "prefecture": zone["prefecture"],
                    }
        else:
            # Bounding box fallback (less accurate but works without Shapely)
            for zone in self.zones:
                bb = zone["bbox"]
                if bb["min_lon"] <= lon <= bb["max_lon"] and bb["min_lat"] <= lat <= bb["max_lat"]:
                    return {
                        "zone_id": zone["zone_id"],
                        "zone_name": zone["zone_name"],
                        "prefecture": zone["prefecture"],
                    }

        return None  # Point is outside Casablanca

    def get_all_zones(self):
        """Return a list of all zone metadata (for dashboards/reports)."""
        return [
            {
                "zone_id": z["zone_id"],
                "zone_name": z["zone_name"],
                "prefecture": z["prefecture"],
                "population": z["population"],
            }
            for z in self.zones
        ]
