# TaaSim Casablanca — Guide de Test et Déploiement (Évaluation)

Ce projet implémente une plateforme complète de traitement de données géospatiales et temporelles en temps réel (Streaming) et par lots (Batch) pour la gestion du trafic des taxis de Casablanca (TaaSim).

Le code source, les données (extraits dans `submission/donnees_extraits/`) et le rapport de synthèse (`submission/rapport/rapport.tex`) sont inclus dans ce livrable.

---

## 🛠️ 1. Prérequis et Initialisation de l'Infrastructure

Assurez-vous que Docker et Docker Compose sont installés sur votre machine de test.

1. **Lancer les services de l'infrastructure** (Kafka, Flink, Cassandra, MinIO, Spark, FastAPI, Grafana) :
   ```bash
   docker compose up -d
   ```
2. **Attendre environ 40 secondes** pour laisser les conteneurs s'initialiser et appliquer automatiquement le schéma Cassandra.
3. **Vérifier l'état des conteneurs** :
   ```bash
   docker ps --format "table {{.Names}}\t{{.Status}}"
   ```

---

## ⚡ 2. Lancement du Pipeline Temps Réel (Streaming PyFlink)

Les jobs PyFlink lisent les flux Kafka, agrègent la demande glissante par arrondissement de Casablanca, effectuent le matching de trajets, et écrivent les résultats dans Cassandra en temps réel.

Exécutez les commandes suivantes pour lancer les 3 jobs Flink :

1. **Normalisateur de Positions GPS** :
   ```bash
   docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/gps_job.py
   ```
2. **Agrégateur de Demande (fenêtre de 30s)** :
   ```bash
   docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/demand_aggregator_job.py
   ```
3. **Moteur de Matching de Trajets** :
   ```bash
   docker exec -it taasim-flink-jm flink run -d -py /opt/flink/usrlib/trip_matcher_job.py
   ```

Vous pouvez suivre l'exécution de ces jobs sur le tableau de bord Flink : [http://localhost:8081](http://localhost:8081).

---

## 🚕 3. Lancement des Simulateurs (Producers Kafka)

Les simulateurs génèrent des données synthétiques de positions de véhicules et de demandes de trajets clients à Casablanca.

1. **Créer un environnement virtuel local et installer les dépendances** :
   ```bash
   python -m venv venv
   # Sur Windows :
   .\venv\Scripts\activate
   # Installez les packages :
   pip install kafka-python pandas pyarrow
   ```
2. **Lancer le producteur GPS (Terminal 1)** :
   ```bash
   python src/simulators/vehicle_gps_producer.py --broker localhost:9092 --speed 10
   ```
3. **Lancer le producteur de demandes de trajets (Terminal 2)** :
   ```bash
   python src/simulators/trip_request_producer.py --broker localhost:9092
   ```

---

## 📊 4. Visualisation et Inférence ML

Une fois le flux actif, les données agrégées alimentent Cassandra et le tableau de bord Grafana en temps réel.

1. **Grafana Dashboard** :
   * URL : [http://localhost:3000](http://localhost:3000) (Identifiants : `admin` / `admin`).
   * Allez dans les Dashboards pour voir le tableau de bord provisionné **TaaSim Casablanca** qui affiche la répartition géographique de la demande, les temps d'attente (ETA) moyens et le statut de la flotte.
2. **FastAPI (Inférence ML)** :
   * URL de la documentation Swagger : [http://localhost:8000/docs](http://localhost:8000/docs).
   * L'API charge automatiquement le modèle GBTRegressor pré-entraîné depuis MinIO et expose un endpoint `/api/predict` pour estimer la demande et l'ETA futures par arrondissement.

---

## 🗄️ 5. Exécution du Pipeline Batch (Apache Spark ML)

Le traitement batch traite l'archivage historique brut stocké sur MinIO pour calculer les KPIs analytiques globaux et réentraîner le modèle de Machine Learning.

1. **Préparer les données brutes dans le conteneur MinIO** :
   ```bash
   # Copier le dataset de trajets Porto (train.csv) et le mapping de Casablanca
   docker cp data/train.csv taasim-minio:/tmp/train.csv
   docker cp data/zone_mapping.csv taasim-minio:/tmp/zone_mapping.csv

   # Charger les données brutes dans le bucket local S3 "raw"
   docker exec taasim-minio mc cp /tmp/train.csv local/raw/porto-trips/train.csv
   docker exec taasim-minio mc cp /tmp/zone_mapping.csv local/raw/zone-mapping/zone_mapping.csv
   ```
2. **Installer la bibliothèque H3 dans l'environnement Spark/Jupyter** :
   ```bash
   docker exec -it taasim-jupyter pip install h3
   ```
3. **Lancer le job ETL Spark (Nettoyage et structuration Curated)** :
   ```bash
   docker exec -it taasim-jupyter spark-submit \
     --master "local[*]" --driver-memory 4G --executor-memory 4G \
     --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
     /home/jovyan/src/spark/etl_porto.py
   ```
4. **Lancer le script de réentraînement du modèle de demande (Gradient Boosted Trees)** :
   ```bash
   docker exec -it taasim-jupyter spark-submit \
     --master "local[*]" --driver-memory 4G --executor-memory 4G \
     --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
     /home/jovyan/src/spark/ml_demand_forecasting_week6.py
   ```
   Le modèle entraîné sera directement sauvegardé dans le bucket MinIO `ml-store/models/gbt_hour_zone.model` pour être rechargé par FastAPI.

---

## 🌐 6. Annuaires des Consoles et Services Web

| Service / Interface | URL Locale | Identifiants / Notes |
|:---|:---|:---|
| **Grafana Dashboard** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password` |
| **FastAPI Backend / Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Endpoint `/api/zones/geojson`, `/api/predict` |
| **Flink Dashboard** | [http://localhost:8081](http://localhost:8081) | Visualisation des graphes et latences PyFlink |
| **Jupyter Lab** | [http://localhost:8888](http://localhost:8888) | Token d'accès : `taasim` |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Statut du cluster Spark |
