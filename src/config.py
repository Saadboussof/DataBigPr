"""
Shared Casablanca bounds derived from the OSMnx road network.
All simulators and Flink jobs must use these exact values.

These bounds were extracted from the cached Casablanca road graph
and represent the actual extent of the driveable road network.

To regenerate: python -c "
import osmnx as ox
G = ox.load_graphml('data/road_cache/casa_roads.graphml')
n = ox.graph_to_gdfs(G, edges=False)
print(f'LON: {n.x.min():.6f} to {n.x.max():.6f}')
print(f'LAT: {n.y.min():.6f} to {n.y.max():.6f}')
"
"""

# Casablanca road network bounds (from OSMnx graph)
CASA_LON_MIN = -7.6895
CASA_LON_MAX = -7.4008
CASA_LAT_MIN = 33.5072
CASA_LAT_MAX = 33.6527

# Porto road network bounds (from OSMnx graph)
PORTO_LON_MIN = -8.6922
PORTO_LON_MAX = -8.5594
PORTO_LAT_MIN = 41.1396
PORTO_LAT_MAX = 41.1848

# Grid for zone mapping (4×4 = 16 zones)
GRID_COLS = 4
GRID_ROWS = 4
NUM_ZONES = GRID_COLS * GRID_ROWS
