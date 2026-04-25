# TaaSim Capstone: Week 1 Concept & Presentation Guide

This document is a complete breakdown of everything you achieved in Week 1. It explains the project's logic, the tools you used, the data you processed, and exactly what you are doing (and *why*). It also includes a template at the bottom so you can easily copy-paste it into your Professor's presentation slide.

---

## 1. The Big Picture: What are we doing?
You are building **TaaSim** (Transport as a Service), an Urban Mobility Platform (like Uber, InDrive, or Careem) for the city of **Casablanca**. 
Because you don't have real taxi data for Casablanca, you are using real-world open data from two major cities (**Porto, Portugal** and **New York City, USA**) to simulate vehicles and predict passenger demand. Week 1 is all about **setting up the laboratory, looking at the raw data, and transforming it to fit Casablanca.**

---

## 2. The Data: What are these huge files?

### A. Porto Taxi Trajectories (`train.csv`)
* **What is it?** A massive Excel-like file (1.7 million rows) recording real taxi trips in Porto over a year.
* **Why do we need it?** It contains a column called `POLYLINE`. This column has the exact GPS coordinates (Longitude/Latitude) of the taxi every 15 seconds. We use this to simulate cars driving around a map!
* **What did we do to it?** Porto is in Portugal, but our project is in Casablanca. In our code, we wrote a mathematical function (`remap_polyline`) to strictly translate and move those GPS coordinates into the Casablanca city map boundaries. 

### B. NYC Yellow Taxi Trip Records (`yellow_tripdata_2024-*.parquet`)
* **What is it?** Real data from New York City yellow cabs covering 3 months of 2024 (nearly 9.5 million trips).
* **Why do we need it?** We need to train an Artificial Intelligence (Machine Learning) model later in the project to predict "how many people will need a taxi in Zone X at 8:00 AM?". To train an AI, you need millions of rows of *demand* data. NYC TLC is the best dataset in the world for this.
* **What did we do to it?** Instead of keeping 9.5 million heavy rows of individual NYC trips, our code grouped them by hour and by zone to simply count the total demand (e.g., "On Jan 1st at 1:00 AM in Zone 4, there were 25 requests").

### C. CSV vs Parquet format
* **CSV** (`train.csv`): Human-readable flat text. It's very slow for Big Data systems to read. 
* **Parquet** (`.parquet` files): A highly compressed, columnar format invented for Big Data. We took the heavy CSV file and saved our clean results as Parquet at the end of the notebook because Parquet is 10x to 100x faster for Machine Learning later on!

---

## 3. The Tools: How does it all work?

Since this is an "Advanced Big Data" capstone, you aren't just using standard Python. You are using Big Data tools:

* **Docker & Docker Compose**: 
  * *What it is:* A virtual machine manager. 
  * *Why we use it:* Instead of installing 10 heavy databases and servers on your laptop and ruining your Windows environment, Docker downloads isolated "Containers" that hold everything exactly as configured. Your `docker-compose.yml` spins up the whole Big Data ecosystem in one command.
* **Jupyter Notebook (`.ipynb`)**: 
  * *What it is:* An interactive coding environment where you can write a block of Python, run it, and immediately see graphs or tables below it.
* **Apache Spark (PySpark)**: 
  * *What it is:* The ultimate Big Data processing engine. Standard Python (like standard `pandas`) would freeze and crash your computer trying to load 10 million rows. Spark distributes the work across memory mathematically, crunching datasets extremely fast.
* **Folium & Matplotlib**: 
  * *What they are:* Python libraries for drawing. Matplotlib draws our charts (like the trip duration histogram). Folium talks to OpenStreetMap to draw the interactive map of Casablanca with the taxi routes painted on top.

---

## 4. The Conception / The Tiny Details (Step-by-Step Logic)

Here is exactly what your script did in Week 1:
1. **Spin up Spark**: Woke up the Big Data engine.
2. **Load Porto Data**: Read 1.7M rows from `train.csv`.
3. **Clean the Data**: Looked for trips where `MISSING_DATA` was True (broken GPS) and deleted those rows.
4. **Calculate Trip Durations**: Counted how many GPS points were in each trip, multiplied by 15 seconds, converting it to minutes to make a statistical chart of average trip times.
5. **Remap Zones**: Used math to stretch the GPS map of Porto so it perfectly fits over the GPS map of Casablanca.
6. **Visualize**: Painted 200 of these newly transformed trips onto an actual map of Morocco.
7. **Process NYC Demand Data**: Loaded, filtered out bad timestamps (dirty data from 2002), grouped by time, and counted the hourly demand per zone.
8. **Save as Parquet**: Wiped cleanly and exported all our hard work into lightweight, fast `.parquet` directories so Week 2's tasks can load them instantly.

---

## 5. TEMPLATE FOR YOUR PRESENTATION SLIDE

Copy this text into your PowerPoint for your professor!

---

**(Slide Title) Week 1 Progress: Data Preparation & Exploration**

**Objective:** Extract, Transform, and Visualize raw datasets to simulate urban transport in Casablanca.

**Tools Deployed:**
* **Docker Compose:** Provisioned our scalable Big Data infrastructure.
* **Apache Spark (PySpark):** Executed heavy data processing (over 11 Million rows total).
* **Jupyter & Folium:** Interactive data profiling and geospatial mapping.

**Key Achievements:**
1. **Porto Data ETL (1.7M rows):** 
   * Loaded Kaggle Porto CSV dataset via PySpark.
   * Cleared missing values (`MISSING_DATA = False`).
   * Generated statistical distributions on Call Types and exact Trip Durations.
2. **Geospatial Translation:** 
   * Developed a mathematical User-Defined Function (UDF) to remap Porto's GPS Polylines onto a Casablanca bounding box (Long/Lat).
   * Verified output by rendering a 200-trip sample onto an OpenStreetMap using Folium.
3. **NYC TLC Demand Aggregation (9.5M rows):**
   * Loaded massive `.parquet` datasets containing 3 months of NYC Yellow Taxi trips.
   * Applied Time-Series transformations (`date_trunc`) to aggregate spatial demand (`PULocationID`) per hour, creating our baseline for future Machine Learning (Demand Forecasting).
4. **Data Sink:** 
   * Saved all finalized datasets into highly compressed `Curated Parquet` format for fast ingestion in Week 2's streaming architectures.

*(Tip: Add a screenshot of your Folium Map with the colored lines, and a screenshot of the Trip Duration blue bar chart to your slide to make it look professional!)*