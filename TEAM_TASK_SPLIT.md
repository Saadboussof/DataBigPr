# TaaSim — Team 3-Way Task Split & Roadmap

As a team of 3 developers, it is incredibly important that you don't step on each other's toes. Big Data pipelines are modular by nature, so we can divide the work into 3 distinct roles: **Data Engineering (Dev 1), Data Science (Dev 2), and Backend/UX (Dev 3).**

This split guarantees absolute equality in workload and ensures that everyone gets hands-on experience with modern tools.

---

## 🤝 Collective Team Accomplishments (Week 1)
Before splitting the work, the entire team collaborated to complete the first phase:
*   **Week 1 (Data Exploration & Setup):** Setup the Docker-Compose network. Profiled the raw datasets (Porto & NYC) in Jupyter Notebooks. Built and tested the Python Simulators (`vehicle_gps_producer.py`, etc.) ensuring Kafka gets bombarded with realistic traffic.

---

## 👨‍💻 Developer 1 (You): The Architect 
**Role:** You built the bedrock of the project. You handle the infrastructure, the data ingestion, and the first layer of real-time streaming.

### ✅ Your Completed Tasks (Week 2)
*   **Week 2:** Designed the Kappa Architecture. Programmed the Cassandra `cql` Schema with optimal partition keys (`zone_id`). Wrote the custom `archiver.py` to seamlessly back up Kafka traffic into the MinIO Data Lake. Delivered the Official ADR.

### 🚀 Your Next Task (Week 3)
*   **Write Flink Job 1 (GPS Normalizer):**
    *   Initialize a PyFlink or Java Flink project.
    *   Tell Flink to consume the `raw.gps` topic.
    *   Filter out any glitched coordinates that landed in the ocean.
    *   **The Math:** Write the logic to take the GPS Lat/Lon from the ping, calculate which of the 16 Casablanca Arrondissements it belongs to, and append the `zone_id` to the payload.
    *   **The Database:** Program the driver connection to `INSERT` that row directly into the Cassandra `vehicle_positions` table.
    *   **The Check:** Implement "Event-Time Watermarks" so your job doesn't crash if a taxi ping arrives 2 minutes late.

---

## 🧑‍💻 Developer 2 (Hicham): The Data Scientist
**Role:** They are responsible for historical number crunching, batch analytics, and predictive AI using Apache Spark.

### 🚀 Their Upcoming Tasks (Weeks 4, 5 & 6)
*   **Week 4 (Flink Job 2 - Demand Counter):** 
    *   While you are writing Job 1, Dev 2 writes Flink Job 2. 
    *   They must write a "30-Second Tumbling Window". Every 30 seconds, their script calculates the mathematical ratio of (Total Available Taxis / Total Ride Requests) per Zone.
    *   They write that resulting ratio to Cassandra `demand_zones`.
*   **Week 5 (Spark ETL & Batch):**
    *   They connect PySpark directly to MinIO. 
    *   They load the massive 9.5M rows of NYC data and the Porto history. 
    *   They write Spark code to discover KPIs: Average trip length, most requested zones, and peak congestion hours. They save the cleaned results back into `s3a://curated`.
*   **Week 6 (Machine Learning):**
    *   Using Spark MLlib, they train a **Gradient Boosted Tree (GBT) Regressor** on the curated S3 data.
    *   The goal: Predict future demand based on Hour of the day and Zone ID.
    *   They save their trained `.model` file to MinIO so Dev 3 can use it.

---

## 👩‍💻 Developer 3 (Soufiane): The Backend & Presentation Lead
**Role:** They are responsible for business logic, the User Interface, and making sure the system can actually be used by citizens via APIs and Dashboards.

### 🚀 Their Upcoming Tasks (Weeks 4, 7 & 8)
*   **Week 4 (Flink Job 3 - The Trip Matcher):**
    *   They write the final Flink job. Their script listens for `raw.trips` (riders). 
    *   When a rider asks for a trip, their code queries the Cassandra database to find the closest available taxi in the same zone. 
    *   They "lock" the taxi, match them together, and insert the final receipt into the Cassandra `trips` table.
*   **Week 7 (FastAPI & Security):**
    *   They build a Python FastAPI web server. 
    *   They program endpoints like `POST /trips/reserve` and `GET /vehicles`. 
    *   They implement JWT Token authentication so unauthorized random people can't crash your server.
    *   *Teamwork hook up:* They write the `/demand/forecast` endpoint, which downloads Dev 2's trained ML model from MinIO and returns a 30-minute prediction via JSON.
*   **Week 8 (Grafana & The Final Demo):**
    *   They hook up Grafana to Cassandra.
    *   They design the Heatmap that changes red/green based on Dev 2's demand ratios, and plots moving dots from Your (Dev 1's) GPS locations.
    *   During the final presentation, they are the one who triggers the `event_injector` stadium spike and proves the dashboard survives.

---

### Wait, how do you work on Flink at the same time?
Because Dev 1, Dev 2, and Dev 3 are all writing Flink code in Weeks 3/4, **you don't have to wait for each other!**
*   **Dev 1** builds a script reading from `raw.gps` and writing to Cassandra `#table 1`.
*   **Dev 2** builds a script reading from `raw.gps`/`raw.trips` and writing counting math to Cassandra `#table 2`.
*   **Dev 3** builds a script reading from `raw.trips` and matching it against Cassandra `#table 3`.

You can all write your code in parallel in 3 separate python files and push to GitHub without creating merge conflicts!
