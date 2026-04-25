# TaaSim — Codebase & Components Explained
## A Deep Dive into Every File and Script in Your Project

This document directly explains the code and files you have in your project folder. If your professor asks you *"What does this file do?"* or *"How does this Jupyter cell work?"*, you will find the answers here.

---

## 1. The Infrastructure File: `docker-compose.yml`

This file is written in YAML and sits at the root of your project. It’s a set of instructions for Docker on how to build your entire Big Data server network locally.

### Key Sections inside `docker-compose.yml`:
* **`kafka` (Apache Kafka):** Configured in KRaft mode (which means it doesn't need Zookeeper anymore). It opens port `9092` so our Python producers can send messages to it.
* **`minio` & `minio-init`:** MinIO is the S3 data lake. `minio-init` is a temporary container that wakes up, automatically creates our buckets (`local/raw`, `local/curated`, `local/ml-store`, `local/kafka-archive`), and then shuts itself down.
* **`cassandra`:** The NoSQL database, opening port `9042`. It uses the `cassandra_data` volume so if you restart Docker, your database tables aren't deleted.
* **`flink-jobmanager` & `flink-taskmanager`:** The streaming engine. JobManager is the boss (port `8081` for the UI), TaskManager is the worker that executes the stream processing.
* **`spark-master` & `spark-worker`:** The batch engine. It's clustered: the boss runs on port `8080`, and the worker connects to it via port `7077`.
* **`jupyter`:** The Jupyter Lab environment. Crucially, it maps your local Windows folders (`./notebooks`, `./data`, `./src`) into the container so you can edit code on your laptop and run it inside the Docker container.
* **`grafana`:** The visualization dashboard running on port `3000`.

---

## 2. The Jupyter Notebook: `week1_exploration.ipynb`

This notebook proves you can load massive datasets, clean them, and perform complex transformations using PySpark.

### Cell by Cell Explanation:

**1. Spark Session Initialization**
```python
spark = SparkSession.builder ... .getOrCreate()
```
* **Explanation:** You cannot use Spark without a SparkSession. This cell tells the Spark Master that we want to start a batch job. We also give it access to libraries in case we need extra functions.

**2. Loading the Porto Data**
```python
df = spark.read.csv('/home/jovyan/data/train.csv', header=True)
```
* **Explanation:** Reads the 1.7M rows of Porto taxi data. Because we set `header=True`, Spark knows the first row contains the column names. We then filter out rows where `MISSING_DATA` is True to ensure data quality.

**3. Data Profiling & Aggregation**
```python
df.groupBy("CALL_TYPE").count().show()
```
* **Explanation:** A classic MapReduce operation. Spark shuffles the data across workers, grouping every trip by how it was hailed (App, Taxi Stand, Street). This is exploratory data analysis.

**4. The Trip Duration UDF (User Defined Function)**
```python
def count_points(polyline_str):
    import json
    ...
```
* **Explanation:** A UDF allows us to apply custom Python logic to a Spark DataFrame. Spark runs this function on parallel nodes. The polyline is a JSON string of GPS points taken every 15 seconds. By importing `json` *inside* the function and counting the points, we multiply by 15 to get the total trip duration in seconds, then convert to minutes.

**5. Geospatial Remapping (Porto → Casablanca)**
* **Explanation:** We don’t have data for Casablanca. The notebook contains a mathematical bounding-box transformation. It takes the min/max latitude and longitude of Porto and the min/max of Casablanca. It uses a linear transformation formula to smoothly slide and stretch the trajectory of a Porto taxi trip into the streets of Casablanca. Note that `remap_polyline` applies this formula to every single GPS coordinate inside the JSON array.

**6. Folium Visualization**
* **Explanation:** Folium is a python library that generates interactive HTML maps. We convert a tiny sample of our Spark DataFrame back to a normal Pandas locally, and plot the remapped polyline points in Blue onto an OpenStreetMap overlay of Casablanca. This proves the math worked.

**7. Saving to Parquet**
```python
df_full.write.parquet(OUTPUT_PATH)
```
* **Explanation:** Writing back to CSV is slow and inefficient. We write the cleaned, Casablanca-mapped data into Parquet format. Parquet compresses the data heavily and stores it in columns, making future machine learning much faster. We use `shutil.rmtree` first because Docker volumes sometimes block Spark's native overwrite functions.

**8. Loading NYC TLC Parquet Files**
* **Explanation:** We load 3 massive months of NYC yellow taxi data (almost 10 million rows) instantly because it's in Parquet. We aggregate the demand per zone per hour. This proves our pipeline scales to "real" big data sizes.

---

## 3. The Real-Time Simulators (`src/simulators/`)

When you move to Week 3 & 4 streaming, you need live data. Because we can't connect to real Casablanca taxis, your professor provided simulator scripts. **These scripts act as Kafka Producers.**

### `vehicle_gps_producer.py`
* **What it does:** It reads the Porto dataset, grabs the `.POLYLINE` array, and physically replays the GPS points one by one by sending them natively to the `raw.gps` Kafka topic.
* **Why it's clever:** It plays the trips at 10x speed. Instead of staring at a screen for 20 minutes for a trip to finish, it finishes in 2 minutes. 
* **The challenge:** It intentionally adds "GPS drift" (noise) and sometimes drops a signal (blackout) to simulate a taxi going through a tunnel. Your Flink streaming job will be forced to fix this messy real-world data later.

### `trip_request_producer.py`
* **What it does:** It acts exactly like a mobile App (like InDrive). It constantly sends simulated JSON messages to the `raw.trips` Kafka topic. Example message: *"Rider 152 wants a taxi in Zone 4 right now."*
* **The challenge:** It doesn't send requests randomly. It follows the Porto demand curve we verified in the Jupyter notebook (busy at 8 AM, dead at 3 AM). It will put stress on your matching engine during virtual rush hours.

### `event_injector.py`
* **What it does:** This is the "Chaos Monkey" script specifically designed for your final demo presentation in Week 8.
* **How it works:** You can manually run this exact script during your presentation to trigger a "Demand Spike" (simulating a huge football stadium emptying out) or a "Rain Event" (which spikes taxi demand across the whole map by 1.4x). It injects anomalies into Kafka so the professor can watch your Grafana heatmap instantly turn red.

---

## Summary for Presentation / Defense

If asked to explain your components:
> *"Our architecture relies on **Docker Compose** to containerize our infrastructure. We use a **Jupyter Notebook** with PySpark as our offline batch environment to ingest raw datasets, clean them, apply a geospatial remapping function to project the data onto Casablanca, and write the gold-standard data out to Parquet. For our real-time streaming, we utilize three custom **Python simulators** that act as Kafka producers, simulating live GPS pings, user app requests, and chaotic weather events."*
