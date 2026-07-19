import json
from shapely.geometry import shape

geojson_path = "/opt/flink/usrlib/arrondissements.geojson"
with open(geojson_path, "r", encoding="utf-8") as f:
    data = json.load(f)

name_map = {
    "Mechouar": "Mechouar Casablanca",
    "Anfa": "Anfa", "Hay Hassani": "Hay Hassani",
    "Sidi Moumen": "Sidi Moumen", "Moulay Rachid": "Moulay Rachid",
    "Sidi Othmane": "Sidi Othmane", "Ben M": "Ben M'Sick",
    "Sbata": "Sbata", "Chock": "Aîn-Chock",
    "Fida": "Al Fida", "Mers Sultan": "Mers Sultan",
    "Assoukhour": "Assoukhour Assawda",
    "Hay Mohammadi": "Hay Mohammadi", "Seba": "Aïn Sebaâ",
    "Bernoussi": "Sidi Bernoussi", "El Maarif": "El Maarif",
    "Belyout": "Sidi Belyout",
}

zone_id_map = {
    "Mechouar Casablanca": 1, "Anfa": 2, "Hay Hassani": 3,
    "Sidi Moumen": 4, "Moulay Rachid": 5, "Sidi Othmane": 6,
    "Ben M'Sick": 7, "Sbata": 8, "Aîn-Chock": 9,
    "Al Fida": 10, "Mers Sultan": 11, "Assoukhour Assawda": 12,
    "Hay Mohammadi": 13, "Aïn Sebaâ": 14, "Sidi Bernoussi": 15,
    "El Maarif": 16, "Sidi Belyout": 17,
}

zones = []
for feature in data["features"]:
    raw_name = feature["properties"].get("Arrondissement", "")
    clean = raw_name
    for key, val in name_map.items():
        if key in raw_name:
            clean = val
            break
    zone_id = zone_id_map.get(clean, 0)
    polygon = shape(feature["geometry"])
    zones.append({
        "zone_id": zone_id,
        "zone_name": clean,
        "polygon": polygon
    })

adjacency = {}
for z1 in zones:
    adj = []
    for z2 in zones:
        if z1["zone_id"] == z2["zone_id"]:
            adj.append(z2["zone_id"])
        elif z1["polygon"].intersects(z2["polygon"]) or z1["polygon"].touches(z2["polygon"]):
            adj.append(z2["zone_id"])
    adjacency[z1["zone_id"]] = sorted(adj)

print("REAL_ADJACENCY = {")
for zid in sorted(adjacency.keys()):
    print(f"    {zid}: {adjacency[zid]},")
print("}")
