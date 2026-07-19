from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import json
import os
from auth.routes import router as auth_router
from trips.routes import router as trips_router
from vehicles.routes import router as vehicles_router
from forecast.routes import router as forecast_router
from forecast.service import init_spark_and_model
from vehicles.service import get_cassandra_session

app = FastAPI(
    title="TaaSim Real-Time Backend API",
    version="1.0.0",
    description="Casablanca Taxi-as-a-Service Platform API"
)

# Enable CORS for Grafana frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Wire up routers
app.include_router(auth_router)
app.include_router(trips_router)
app.include_router(vehicles_router)
app.include_router(forecast_router)

def update_geojson_loop():
    import shutil
    import time
    
    src_path = "geo/arrondissements.geojson"
    tpl_path = "geo/arrondissements_template.geojson"
    
    # Create a backup template on boot if not already present
    if os.path.exists(src_path) and not os.path.exists(tpl_path):
        try:
            shutil.copy(src_path, tpl_path)
            print("Successfully created arrondissements_template.geojson")
        except Exception as e:
            print(f"Failed to create template backup: {e}")

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
        if "Mechouar" in raw_name: return "Mechouar Casablanca"
        if "Anfa" in raw_name and "Maarif" not in raw_name and "Belyout" not in raw_name: return "Anfa"
        if "Hay Hassani" in raw_name: return "Hay Hassani"
        if "Sidi Moumen" in raw_name: return "Sidi Moumen"
        if "Moulay Rachid" in raw_name: return "Moulay Rachid"
        if "Sidi Othmane" in raw_name: return "Sidi Othmane"
        if "Ben M" in raw_name and "Sick" in raw_name: return "Ben M'Sick"
        if "Sbata" in raw_name: return "Sbata"
        if "Chock" in raw_name or "Chok" in raw_name: return "Aîn-Chock"
        if "Fida" in raw_name: return "Al Fida"
        if "Mers Sultan" in raw_name: return "Mers Sultan"
        if "Assoukhour" in raw_name or "Sawda" in raw_name: return "Assoukhour Assawda"
        if "Hay Mohammadi" in raw_name: return "Hay Mohammadi"
        if "Seba" in raw_name: return "Aïn Sebaâ"
        if "Bernoussi" in raw_name: return "Sidi Bernoussi"
        if "Maarif" in raw_name: return "El Maarif"
        if "Belyout" in raw_name: return "Sidi Belyout"
        return raw_name

    while True:
        try:
            if not os.path.exists(tpl_path):
                time.sleep(5)
                continue
                
            # 1. Fetch latest ratios from Cassandra
            session = get_cassandra_session()
            query = """
                SELECT zone_id, ratio 
                FROM demand_zones 
                WHERE city = 'casablanca' 
                PER PARTITION LIMIT 1 
                ALLOW FILTERING;
            """
            ratios = {}
            try:
                rows = session.execute(query)
                for row in rows:
                    ratios[row.zone_id] = row.ratio
            except Exception as e:
                print(f"Cassandra query in updater loop failed: {e}")
                
            # 2. Load template GeoJSON
            with open(tpl_path, "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
                
            # 3. Inject styling properties
            for feature in geojson_data.get("features", []):
                props = feature.get("properties", {})
                raw_name = props.get("Arrondissement", "")
                clean = _clean_name(raw_name)
                zone_id = ZONE_NAME_TO_ID.get(clean, 0)
                
                ratio = ratios.get(zone_id, 0.0)
                if ratio == 0.0:
                    color = "green"
                elif ratio < 0.8:
                    color = "green"
                elif ratio <= 1.2:
                    color = "yellow"
                else:
                    color = "red"
                
                props["zone_id"] = zone_id
                props["zone_name"] = clean
                props["ratio"] = ratio
                props["color"] = color
                props["fill"] = color
                props["fill-opacity"] = 0.6
                
            # 4. Overwrite src file in place to maintain the Docker bind mount inode
            with open(src_path, "w", encoding="utf-8") as f:
                json.dump(geojson_data, f)
                
        except Exception as e:
            print(f"Error in GeoJSON updater loop: {e}")
            
        time.sleep(5)

@app.on_event("startup")
def startup_event():
    # Initialize the PySpark connection for GBT inference on boot in a background thread
    import threading
    threading.Thread(target=init_spark_and_model, daemon=True).start()
    # Start the background GeoJSON file updater thread
    threading.Thread(target=update_geojson_loop, daemon=True).start()

@app.get("/health", tags=["System"])
def health_check():
    return {"status": "ok", "timestamp": int(datetime.utcnow().timestamp())}

@app.get("/api/zones/geojson", tags=["Geomap"])
def get_zones_geojson():
    # 1. Fetch latest ratios from Cassandra
    session = get_cassandra_session()
    query = """
        SELECT zone_id, ratio 
        FROM demand_zones 
        WHERE city = 'casablanca' 
        PER PARTITION LIMIT 1 
        ALLOW FILTERING;
    """
    ratios = {}
    try:
        rows = session.execute(query)
        for row in rows:
            ratios[row.zone_id] = row.ratio
    except Exception as e:
        print(f"Cassandra query failed: {e}")
        
    # 2. Load the arrondissements.geojson
    geojson_path = "geo/arrondissements.geojson"
    if not os.path.exists(geojson_path):
        return {"error": "GeoJSON file not found"}
        
    with open(geojson_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)
        
    # 3. Mapping Arrondissement name to zone_id
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
        if "Mechouar" in raw_name: return "Mechouar Casablanca"
        if "Anfa" in raw_name and "Maarif" not in raw_name and "Belyout" not in raw_name: return "Anfa"
        if "Hay Hassani" in raw_name: return "Hay Hassani"
        if "Sidi Moumen" in raw_name: return "Sidi Moumen"
        if "Moulay Rachid" in raw_name: return "Moulay Rachid"
        if "Sidi Othmane" in raw_name: return "Sidi Othmane"
        if "Ben M" in raw_name and "Sick" in raw_name: return "Ben M'Sick"
        if "Sbata" in raw_name: return "Sbata"
        if "Chock" in raw_name or "Chok" in raw_name: return "Aîn-Chock"
        if "Fida" in raw_name: return "Al Fida"
        if "Mers Sultan" in raw_name: return "Mers Sultan"
        if "Assoukhour" in raw_name or "Sawda" in raw_name: return "Assoukhour Assawda"
        if "Hay Mohammadi" in raw_name: return "Hay Mohammadi"
        if "Seba" in raw_name: return "Aïn Sebaâ"
        if "Bernoussi" in raw_name: return "Sidi Bernoussi"
        if "Maarif" in raw_name: return "El Maarif"
        if "Belyout" in raw_name: return "Sidi Belyout"
        return raw_name

    # 4. Inject ratio and color into feature properties
    for feature in geojson_data.get("features", []):
        props = feature.get("properties", {})
        raw_name = props.get("Arrondissement", "")
        clean = _clean_name(raw_name)
        zone_id = ZONE_NAME_TO_ID.get(clean, 0)
        
        ratio = ratios.get(zone_id, 0.0)
        # Flink calculates ratio = pending_requests / active_vehicles (demand / supply)
        # ratio < 0.8: Plenty of taxis (low demand relative to supply) -> Green
        # ratio 0.8 - 1.2: Balanced -> Yellow
        # ratio > 1.2: Taxi shortage (high demand relative to supply) -> Red
        if ratio == 0.0:
            color = "green"
        elif ratio < 0.8:
            color = "green"
        elif ratio <= 1.2:
            color = "yellow"
        else:
            color = "red"
        
        props["zone_id"] = zone_id
        props["zone_name"] = clean
        props["ratio"] = ratio
        props["color"] = color
        props["fill"] = color
        props["fill-opacity"] = 0.6
        
    return geojson_data
