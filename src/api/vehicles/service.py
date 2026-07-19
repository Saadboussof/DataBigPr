from cassandra.cluster import Cluster
from config import CASSANDRA_HOST

_session = None

def get_cassandra_session():
    global _session
    if _session is None:
        cluster = Cluster([CASSANDRA_HOST], port=9042)
        _session = cluster.connect("taasim")
    return _session

def get_vehicles_in_zone(zone_id: int):
    session = get_cassandra_session()
    # Partition-bound query for Casablanca zone. Order by event_time is desc by default.
    query = """
        SELECT taxi_id, lat, lon, speed, status, event_time
        FROM vehicle_positions
        WHERE city = 'casablanca' AND zone_id = %s
        LIMIT 50
    """
    rows = session.execute(query, [zone_id])
    
    # Since we want to present only the LATEST state of each unique taxi:
    latest_taxis = {}
    for row in rows:
        if row.taxi_id not in latest_taxis:
            latest_taxis[row.taxi_id] = {
                "taxi_id": row.taxi_id,
                "lat": row.lat,
                "lon": row.lon,
                "speed": row.speed,
                "status": row.status,
                "last_seen": row.event_time.isoformat() if row.event_time else None
            }
            
    return {
        "zone_id": zone_id,
        "active_count": len(latest_taxis),
        "vehicles": list(latest_taxis.values())
    }
