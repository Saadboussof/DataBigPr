# Dev 2 → Dev 3




## Livraison

Dev3 a besoin de 4 choses de la part de dev2. Les voici, avec schémas, endpoints, et exemples concrets.

---

## 1. 📊 Ratios de Demande Temps Réel → Cassandra `demand_zones`

**Produit par :** Flink Job 2 — `src/flink/demand_aggregator_job.py`  
**Fréquence :** Toutes les **30 secondes** (tumbling window)  
**Destination :** Cassandra `taasim.demand_zones`

### Schéma Cassandra

```sql
CREATE TABLE taasim.demand_zones (
    city              TEXT,       -- 'casablanca'
    zone_id           INT,        -- 1-16 (arrondissement Casablancais)
    zone_name         TEXT,       -- 'Anfa', 'Hay Hassani', ...
    window_start      TIMESTAMP,  -- Début de la fenêtre de 30s
    active_vehicles   INT,        -- Offre : nb de taxis dans la zone
    pending_requests  INT,        -- Demande : nb de requêtes en attente
    ratio             DOUBLE,     -- = pending_requests / max(active_vehicles, 1)
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
```

### Exemple de ligne

| city | zone_id | zone_name | window_start | active_vehicles | pending_requests | ratio |
|------|---------|-----------|-------------|----------------|-----------------|-------|
| casablanca | 7 | Hay Hassani | 2026-05-31T14:30:00 | 12 | 3 | 0.25 |

### Comment le consommer (Grafana)

```sql
-- Dernier ratio pour chaque zone (pour la heatmap)
SELECT zone_id, zone_name, ratio, window_start
FROM taasim.demand_zones
WHERE city = 'casablanca'
PER PARTITION LIMIT 1;
```

### Comment le consommer (API Python)

```python
from cassandra.cluster import Cluster
cluster = Cluster(['cassandra'], port=9042)
session = cluster.connect('taasim')

rows = session.execute("""
    SELECT zone_id, zone_name, ratio, active_vehicles, pending_requests
    FROM demand_zones WHERE city = 'casablanca'
""")
for row in rows:
    print(f"Zone {row.zone_id} ({row.zone_name}): ratio={row.ratio}")
```

### Topic Kafka correspondant

```
Topic: processed.demand
Format: JSON
{
  "city": "casablanca",
  "zone_id": 7,
  "zone_name": "Hay Hassani",
  "window_start": "2026-05-31T14:30:00",
  "active_vehicles": 12,
  "pending_requests": 3,
  "ratio": 0.25
}
```

---

## 2. 🤖 Modèle ML Prédictif → MinIO `ml-store/`

**Produit par :** `src/spark/ml_demand_forecasting_week6.py`  
**Modèle :** GBT Regressor (Gradient Boosted Tree) via Spark MLlib  
**Chemin :** `s3a://ml-store/models/gbt_hour_zone.model`

### Architecture du modèle

```
PipelineModel (Spark ML)
├── Stage 0: StringIndexer(zone_type)        # "residential"/"commercial" → 0/1
├── Stage 1: StringIndexer(temperature_bucket) # "cold"/"mild"/"hot" → 0/1/2
├── Stage 2: VectorAssembler(12 features)
└── Stage 3: GBTRegressor
```

### 12 Features d'entrée (dans l'ordre pour VectorAssembler)

| # | Feature | Type | Description | Valeur par défaut (temps réel) |
|---|---------|------|-------------|-------------------------------|
| 1 | `zone_id` | string | H3 parent ou arrondissement_id | Fourni par l'appel API |
| 2 | `hour_of_day` | int | 0-23 | Extrait du timestamp |
| 3 | `day_of_week` | int | 1=Lun…7=Dim | Extrait du timestamp |
| 4 | `is_weekend` | int | 0 ou 1 | 1 si sam/dim |
| 5 | `is_friday` | int | 0 ou 1 | 1 si vendredi |
| 6 | `zone_population_density` | int | Mocké (1000-3100) | 1000 (défaut) |
| 7 | `zone_type` | string | 'residential'/'commercial' | 'commercial' (défaut) |
| 8 | `is_raining` | int | 0 ou 1 | 0 (défaut) |
| 9 | `temperature_bucket` | string | 'cold'/'mild'/'hot' | 'mild' (défaut) |
| 10 | `demand_lag_1d` | double | Demande à J-1 même heure | 0.0 (temps réel) |
| 11 | `demand_lag_7d` | double | Demande à J-7 même heure | 0.0 (temps réel) |
| 12 | `rolling_7d_mean` | double | Moyenne glissante 7 jours | 0.0 (temps réel) |

### Comment charger le modèle (FastAPI)

```python
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("DemandAPI") \
    .master("local[1]") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

model = PipelineModel.load("s3a://ml-store/models/gbt_hour_zone.model")
```

### Comment faire une prédiction

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import col

PREDICTION_SCHEMA = StructType([
    StructField("zone_id", StringType()),
    StructField("hour_of_day", IntegerType()),
    StructField("day_of_week", IntegerType()),
    StructField("is_weekend", IntegerType()),
    StructField("is_friday", IntegerType()),
    StructField("zone_population_density", IntegerType()),
    StructField("zone_type", StringType()),
    StructField("is_raining", IntegerType()),
    StructField("temperature_bucket", StringType()),
    StructField("demand_lag_1d", DoubleType()),
    StructField("demand_lag_7d", DoubleType()),
    StructField("rolling_7d_mean", DoubleType()),
])

def predict_demand(spark, model, zone_id, timestamp):
    """zone_id: string, timestamp: datetime"""
    input_df = spark.createDataFrame([(
        zone_id,                # zone_id
        timestamp.hour,         # hour_of_day
        timestamp.isoweekday(), # day_of_week
        1 if timestamp.isoweekday() >= 6 else 0,  # is_weekend
        1 if timestamp.isoweekday() == 5 else 0,  # is_friday
        1000,                   # zone_population_density (default)
        "commercial",           # zone_type (default)
        0,                      # is_raining (default)
        "mild",                 # temperature_bucket (default)
        0.0,                    # demand_lag_1d (default)
        0.0,                    # demand_lag_7d (default)
        0.0,                    # rolling_7d_mean (default)
    )], schema=PREDICTION_SCHEMA)

    pred_df = model.transform(input_df)
    return pred_df.select("prediction").collect()[0][0]
```

---

## 3. 📈 KPIs Historiques → Cassandra `kpi_summary`

**Produit par :** `src/spark/kpi_analytics.py`  
**Destination :** Cassandra `taasim.kpi_summary`

### Schéma

```sql
CREATE TABLE taasim.kpi_summary (
    city                      TEXT,
    zone_id                   INT,
    window_start              TIMESTAMP,
    total_trips               BIGINT,
    avg_trip_duration_minutes DOUBLE,
    peak_demand_hour          INT,
    coverage_gap_demand       BIGINT,
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
```

### Exemple

| zone_id | total_trips | avg_trip_duration_minutes | peak_demand_hour | coverage_gap_demand |
|---------|------------|--------------------------|-----------------|-------------------|
| 7 | 45230 | 12.5 | 18 | 1203 |

### Requêtes utiles (Grafana)

```sql
-- Top zones par activité
SELECT zone_id, total_trips, avg_trip_duration_minutes
FROM kpi_summary WHERE city = 'casablanca'
ORDER BY total_trips DESC LIMIT 5;

-- Zones sous-approvisionnées
SELECT zone_id, peak_demand_hour, coverage_gap_demand
FROM kpi_summary WHERE city = 'casablanca' AND coverage_gap_demand > 0;
```

---

## 4. 📂 Données Curated → MinIO `s3a://curated/`

### Porto Trips (pour analyses historiques)

```
Chemin : s3a://curated/porto-trips/
Format : Parquet (Snappy compressé)
Partitionné par : year_month (e.g. '2013-06')
Schéma : TRIP_ID, TIMESTAMP, POLYLINE, start_lon, start_lat, end_lon, end_lat,
         start_h3_id, end_h3_id, zone_id (INT 1-16), year, month, year_month
```

### NYC Demand by Zone

```
Chemin : s3a://curated/demand-by-zone/
Format : Parquet
Schéma : zone_id (INT), hour (INT), demand_count (BIGINT)
```

### KPIs batch

```
s3a://curated/kpis/week5/avg-trip-length/
s3a://curated/kpis/week5/most-requested-zones/
s3a://curated/kpis/week5/peak-congestion-hours/
```

---

## Résumé des connexions

| Ressource | Adresse | Credentials |
|-----------|---------|-------------|
| Cassandra | `cassandra:9042` | keyspace `taasim` |
| MinIO S3 | `http://minio:9000` | `admin:password` |
| Kafka | `kafka:29092` | (pas d'auth) |

---


## ✅ Checklist :

```bash
# 1. Vérifier que Flink Job 2 tourne (demand aggregator)
docker exec taasim-flink-jm flink list
# Attendu: "Real-Time Demand Aggregator (RUNNING)"

# 2. Vérifier les ratios en temps réel dans Cassandra
docker exec taasim-cassandra cqlsh -e \
  "SELECT zone_id, zone_name, ratio FROM taasim.demand_zones LIMIT 10;"

# 3. Vérifier le modèle ML dans MinIO
docker exec taasim-minio mc ls local/ml-store/models/

# 4. Vérifier les KPIs historiques
docker exec taasim-cassandra cqlsh -e \
  "SELECT zone_id, total_trips, peak_demand_hour FROM taasim.kpi_summary LIMIT 10;"

# 5. Vérifier le topic Kafka processed.demand
docker exec taasim-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic processed.demand \
  --max-messages 3 --timeout-ms 5000
```
