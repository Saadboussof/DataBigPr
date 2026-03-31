# TaaSim Capstone: Dataset Schemas and MinIO Clarification

## Regarding MinIO: Is the Professor correct?
**Yes, your Professor's instruction in the PDF to "upload the data into MinIO" is absolutely correct.**

### What is MinIO and why use it?
MinIO is a high-performance distributed object storage server. It is built to mimic **Amazon S3** (the most popular cloud storage system in the world). 
In the real world, you don't keep 10-gigabyte Big Data files on your laptop's regular hard drive folder. Companies like Netflix or Uber store their massive datasets inside Amazon S3 buckets. Since this Capstone simulates a real-world enterprise pipeline using Docker on your local machine, **MinIO acts as your local "fake Amazon S3".**

### Why did our Notebook use `/home/jovyan/data` instead of MinIO?
The Jupyter Notebook you currently have uses a **local file mount** (`/home/jovyan/data/`) to read the files directly from your computer's `data/` folder. This was just a quick shortcut for the "Exploration" phase to make sure the paths work without configuring complex S3 credentials in PySpark. 

**However, according to the Week 1 & 2 engineering tasks in your `page_13.txt` PDF:**
You are expected to manually log into the MinIO dashboard (usually `http://localhost:9001` when Docker is running), create a bucket named `raw/`, and physically upload `train.csv` and the `yellow_tripdata` Parquet files there. Later in the project (Week 2 and Week 4), your Spark jobs and Flink jobs will be reconfigured to read data using the S3 protocol (`s3a://raw/porto-trips/`) straight out of MinIO instead of the local hard drive!

---

## The Data in Tiny Detail: Column By Column Definition

Below is the definitive, tiny-detail explanation of every single column inside the two massive datasets you are using.

### 1. The Porto Taxi Trajectories Dataset (`train.csv`)
This data comes from 442 taxis running in Porto, Portugal, tracking an entire year of trips.

* **`TRIP_ID`** *(String/Long)* 
  * **What it is:** A deeply unique numerical ID generated for every single trip. (e.g., `1372636858620000455`)
  * **Why we need it:** So we can identify deduplicate requests and track the lifestyle of one exact ride.
* **`CALL_TYPE`** *(String)* 
  * **What it is:** How did the customer get the taxi? 
    * `A` = They phoned the central dispatch to send a taxi to their house.
    * `B` = They walked up to a specific taxi sitting at an official Taxi Stand.
    * `C` = They randomly hailed/waved down a taxi driving past them on the street. 
* **`ORIGIN_CALL`** *(Integer)*
  * **What it is:** The phone number/customer ID. 
  * *Note:* If `CALL_TYPE` isn't `A`, this column is blank (Null).
* **`ORIGIN_STAND`** *(Integer)*
  * **What it is:** The ID of the physical Taxi Stand they walked up to (ranging from 1 to 63). 
  * *Note:* If `CALL_TYPE` isn't `B`, this column is blank.
* **`TAXI_ID`** *(Integer)*
  * **What it is:** The specific license plate or vehicle tracker ID of the car. (e.g., Taxi #20000589)
* **`TIMESTAMP`** *(Integer - Unix Epoch)* 
  * **What it is:** The exact start time of the trip, written in computer seconds since Jan 1, 1970. (e.g., `1372636858` = July 1, 2013).
  * **Why we need it:** To figure out peak rush hours!
* **`DAY_TYPE`** *(String)*
  * **What it is:** `A` (Normal Day), `B` (Holiday), or `C` (Day before a Holiday).
* **`MISSING_DATA`** *(Boolean: True/False)*
  * **What it is:** Did the GPS tracker break halfway through the trip in the middle of the street? If `True`, the data is corrupted, and we threw it away in our Spark script.
* **`POLYLINE`** *(Stringified JSON Array)*
  * **What it is:** The most important column. It is a long list of coordinates formatted as `[ [Longitude, Latitude], [Longitude, Latitude] ]`. 
  * **The Rule:** A coordinate is snapped exactly every **15 Seconds**. If a polyline has 20 coordinates, the trip took exactly 5 minutes (20 x 15s = 300s). We use this to animate the cars moving in TaaSim!

---

### 2. The NYC Yellow Taxi Trip Records (`yellow_tripdata_2024.parquet`)
This is the official data from the New York City Taxi & Limousine Commission. We use it *only* to simulate demand forecasting (predicting how many people want a taxi) because NYC has the best demand density data in the world. 

* **`VendorID`** *(Integer)*
  * **What it is:** A code indicating which technology company provided the meter hardware inside the taxi (1= Creative Mobile, 2= VeriFone).
* **`tpep_pickup_datetime`** *(Timestamp)*
  * **What it is:** The exact date/time the meter was turned on when the passenger entered the cab.
  * **Why we need it:** This is the most crucial column! We use this to say "Ah, this rider demanded a taxi at 8:15 AM on a Monday."
* **`tpep_dropoff_datetime`** *(Timestamp)*
  * **What it is:** The exact date/time the meter was turned off.
* **`passenger_count`** *(Integer)*
  * **What it is:** How many passengers were in the vehicle.
* **`trip_distance`** *(Float)*
  * **What it is:** The distance traveled in miles.
* **`RatecodeID`** *(Integer)*
  * **What it is:** The final rate type applied (1 = Standard fare, 2= JFK Airport fare, 3= Newark out of town, etc.)
* **`store_and_fwd_flag`** *(String)*
  * **What it is:** `Y` or `N`. Did the vehicle lose internet connection and have to store the trip memory on the hardware before sending it to the server later? 
* **`PULocationID`** *(Integer)* 
  * **What it is:** Pick-Up Location ID. The ID of the NYC neighborhood/zone where the ride started (Ranges from 1 to 265).
  * **Why we need it:** Alongside the pickup time, we count these up! "At 8:00 AM, `PULocationID` Zone #4 had 100 requests".
* **`DOLocationID`** *(Integer)*
  * **What it is:** Drop-Off Location ID. The destination zone.
* **`payment_type`** *(Integer)*
  * **What it is:** How they paid (1= Credit card, 2= Cash, 3= No charge, etc.)
* **`fare_amount` / `tip_amount` / `tolls_amount` / `total_amount`** *(Flots)*
  * **What it is:** The financial breakdown of what the ride cost. *(We largely ignore these for the Capstone, because our focus is mobility routing and demand requests, not finance).*