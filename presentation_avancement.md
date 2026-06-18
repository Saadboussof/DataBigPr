# TaaSim — Présentation Avancement (9 min)

> **Format:** 3 membres × 3 min chacun | **Focus:** Concepts & Conception uniquement

---

## 🎤 Dev 1 (Saad) — Architecture Globale & Ingestion [3 min]

**Tu ouvres la présentation. Tu poses le contexte et tu expliques comment les données entrent dans le système.**

### Slide 1 — Contexte & Problématique (45s)
- "TaaSim = Taxi-as-a-Service Simulator pour la ville de Casablanca"
- Le problème : pas de dataset taxi Casablanca → on utilise Porto (1.7M rides, GPS polylines) + NYC TLC (9.5M rides, zones)
- Porto nous donne le **mouvement réaliste**, NYC nous donne le **volume massif**
- On transpose les coordonnées Porto → 16 arrondissements de Casablanca

### Slide 2 — Architecture Kappa (1 min)
- Montrer le diagramme d'architecture global
- Kappa Architecture : tout passe par le **stream** (pas de batch séparé)
- Les composants : Kafka → Flink → Cassandra (temps réel) + MinIO (archive)
- Docker-Compose : **8 services** orchestrés (Kafka, Cassandra, MinIO, Flink, Spark, Jupyter, Grafana, Archiver)

### Slide 3 — Couche d'Ingestion : Simulateurs + Kafka (1 min)
- 2 simulateurs Python qui bombardent Kafka :
  - `vehicle_gps_producer` : 100-500 GPS pings/sec → **15M lignes/jour**
  - `trip_request_producer` : rush hour = 10 req/sec, normal = 1-3 req/sec → **250K-300K trips/jour**
- 5 topics Kafka créés : `raw.gps`, `raw.trips`, `processed.gps`, `processed.demand`, `processed.matches`
- KRaft mode (sans Zookeeper)

### Transition → Dev 2 (15s)
- "Maintenant que les données sont dans Kafka, Hicham va expliquer comment Flink les traite en temps réel"

---

## 🎤 Dev 2 (Hicham) — Traitement Temps Réel avec Flink [3 min]

**Tu expliques les 3 jobs Flink qui constituent le cœur du traitement stream.**

### Slide 4 — Flink Job 1 : GPS Normalizer (1 min)
- Consomme `raw.gps` → filtre les coordonnées aberrantes (ocean, hors Casablanca)
- **Le calcul clé** : à partir de (lat, lon), déterminer le `zone_id` parmi les 16 arrondissements
- Enrichit le payload avec `zone_id` → INSERT dans Cassandra `vehicle_positions`
- Gestion des Event-Time Watermarks pour les pings en retard

### Slide 5 — Flink Job 2 : Demand Counter (1 min)
- **Tumbling Window de 30 secondes**
- Chaque fenêtre calcule : `ratio = taxis_disponibles / demandes_en_attente` par zone
- ratio > 1 = surplus de taxis, ratio < 1 = pénurie
- Écrit dans Cassandra `demand_zones` → alimente le heatmap Grafana

### Slide 6 — Flink Job 3 : Trip Matcher (45s)
- Écoute `raw.trips` (demandes passagers)
- Query Cassandra pour trouver le taxi le plus proche **dans la même zone**
- Lock le taxi → crée le match → INSERT dans `trips`
- Grâce au partitionnement par `zone_id`, la recherche est instantanée (pas de scan global)

### Transition → Dev 3 (15s)
- "Maintenant, Soufiane va expliquer comment on stocke et on expose ces résultats"

---

## 🎤 Dev 3 (Soufiane) — Stockage & Couche de Service [3 min]

**Tu expliques le dual-storage et comment le système est exposé aux utilisateurs.**

### Slide 7 — Dual-Storage : Cassandra vs MinIO (1 min 30s)
- **Cassandra = moteur temps réel** (lecture/écriture en ms)
  - 3 tables : `vehicle_positions`, `trips`, `demand_zones`
  - Partition Key = `zone_id` → toutes les requêtes sont ciblées, jamais de full scan
  - TTL 24h sur `vehicle_positions` → auto-nettoyage, la base reste légère
  - TTL 7 jours sur `demand_zones`
- **MinIO = Data Lake** (archive permanente, compatible S3)
  - `archiver.py` : boucle infinie → batch de 100 messages → upload JSON Lines dans MinIO
  - 4 buckets : `raw`, `curated`, `ml-store`, `kafka-archive`
  - Pourquoi JSON Lines ? → Spark peut lire des millions de lignes en secondes

### Slide 8 — Prochaines Étapes (1 min)
- Spark ETL : connecter PySpark à MinIO, calculer les KPIs (durée moyenne, zones les plus demandées, heures de pointe)
- Spark ML : entraîner un GBT Regressor pour prédire la demande future (heure × zone)
- FastAPI : endpoints REST (`POST /trips/reserve`, `GET /vehicles`, `/demand/forecast`) + JWT auth
- Grafana : heatmap temps réel (rouge/vert selon le ratio) + points GPS en mouvement

### Slide 9 — Conclusion (30s)
- Récap rapide : Simulateurs → Kafka → Flink (3 jobs) → Cassandra + MinIO → API + Dashboard
- Ce qui est fait vs ce qui reste
- Questions ?

---

## ⏱️ Résumé Timing

| Membre | Sujet | Durée |
|--------|-------|-------|
| **Dev 1 (You)** | Contexte + Architecture Kappa + Ingestion | 3 min |
| **Dev 2 (Hicham)** | Flink Job 1, 2, 3 (traitement stream) | 3 min |
| **Dev 3 (Soufiane)** | Dual-Storage + Prochaines étapes + Conclusion | 3 min |
| | **Total** | **9 min** |

---

## 📌 Règles Importantes

1. **Pas de code à montrer** — on explique les concepts et la conception, pas les lignes de code
2. **Un diagramme d'architecture par slide** — c'est visuel, pas textuel
3. **Chaque transition doit nommer la personne suivante** — enchaînement fluide
4. **Si une question arrive** — "On répondra aux questions à la fin" pour garder le rythme
