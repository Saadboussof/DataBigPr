# TaaSim Capstone: Project Architecture & File Structure

Here is a complete breakdown of your project's architecture. It explains exactly what every folder and file in your repository does, and how they connect together to form the "TaaSim" (Transport as a Service) platform.

---

## 1. The Core Infrastructure

### `docker-compose.yml`
* **What it does:** This is the heart of your local server. It acts as the "recipe" for Docker.
* **Why it matters:** Instead of installing Kafka, Cassandra, MinIO, and Spark directly on your computer, this file tells Docker to automatically download and run these systems in isolated containers. When you run `docker-compose up -d`, this file starts your entire Big Data cluster.

---

## 2. The `data/` Directory (Storage)
This folder acts as the local holding area for your raw data before it gets moved to MinIO (your virtual Amazon S3).

* **`train.csv` / `yellow_tripdata_*.parquet`**
  * **What they do:** These are the massive datasets from Porto (GPS trajectories) and New York City (taxi demand). They are ignored by Git (using `.gitignore`) because they are too large for GitHub.
* **`zone_mapping.csv`**
  * **What it does:** A simple dictionary file. It maps Porto's 22 taxi zones to Casablanca's 16 arrondissements. The Spark jobs read this to translate the map.

---

## 3. The `notebooks/` Directory (Data Exploration)
This folder is your data science laboratory. It is used for experimenting before writing permanent code.

* **`week1_exploration.ipynb`**
  * **What it does:** The main Jupyter Notebook where you wrote PySpark code to explore the Porto data, generate charts, and clean up the NYC data to predict demand.
* **`casablanca_taxi_map.html`**
  * **What it does:** A front-end web page generated automatically by the Jupyter Notebook (using Folium). It shows the interactive OpenStreetMap of Casablanca with the translated taxi paths drawn on it.
* **`trip_duration_dist.png` / `demand_by_hour.png`**
  * **What they are:** Images of the charts generated inside the notebook, saved as pictures so you can put them in your presentation.

---

## 4. The `src/` Directory (Production Source Code)
This is where the real "software engineering" happens. It contains the scripts that will run constantly in the background to simulate a live taxi platform.

### Sub-folder: `src/simulators/` 
These files pretend to be the mobile apps on the taxi drivers' and customers' phones.
* **`vehicle_gps_producer.py`**
  * **What it does:** This script acts like the GPS tracker inside 400+ simulated taxis. It reads the trajectory data and constantly sends live GPS coordinates (`[Lat, Long]`) to a Kafka stream (a live data pipeline).
* **`trip_request_producer.py`**
  * **What it does:** This script acts like the passenger's mobile app (like the Uber app). It generates fake requests ("I am in Zone 2 and I need a taxi!") and sends them to a Kafka queue based on the real demand curve you found in Week 1.
* **`event_injector.py`**
  * **What it does:** A utility tool to forcefully "inject" specific events (like a late GPS signal) into the system to test if your platform handles errors properly.

### Sub-folder: `src/spark/`
These files are your heavy-lifting ETL (Extract, Transform, Load) pipelines.
* **`porto_to_casablanca.py`**
  * **What it does:** The production-grade Spark script. While the Jupyter notebook was just for testing, this `.py` file is the actual script you submit to the Apache Spark cluster to automatically process millions of rows cleanly from end to end.

---

## 5. The Documentation (Markdown Files)
Files ending in `.md` (Markdown) are text files meant to explain the project to humans.

* **`README.md`**
  * **What it does:** The front page of your GitHub repository. It tells anyone visiting your project what TaaSim is and alerts them that they need to download dataset files.
* **`Week1_Presentation_Guide.md`**
  * **What it does:** The guide created for your professor's presentation, outlining the tools, context, and logic.
* **`Dataset_and_MinIO_Details.md`**
  * **What it does:** A detailed breakdown of every single column in the CSV and Parquet files, along with the explanation of what MinIO is used for.
* **`Output_Explanation.md`**
  * **What it does:** An explanation of the Jupyter output logs.

---

## Summary of the Data Flow Architecture:
1. **Simulators (`src/simulators/`)** generate fake live traffic and send it to **Kafka** (Running via `docker-compose`).
2. **Spark (`src/spark/` & `notebooks/`)** takes historic massive files, crunches the math, translates coordinates, and saves the results into **MinIO** (Your virtual S3 cloud storage).
3. Later in the weeks, **Cassandra** will store the specific live positions of cars, and **Grafana** will connect to it to show a beautiful dashboard map!