# 🚀 TaaSim Project Runbook

This guide contains all the terminal commands you need to run the entire project from start to finish, test the pipelines, start the simulators, and verify the data in Cassandra.

## 1. Start the Infrastructure (Docker)
First, make sure all your databases and streaming platforms are running.

```bash
# Start all containers in the background
docker-compose up -d

# Check if everything is running (Kafka, Flink, Cassandra, MinIO, Grafana)
docker ps
```

## 2. Generate the Mapped Data (If you haven't already)
Before running the simulator, you need to map the raw Porto data to Casablanca roads.

```bash
# Map 5,000 trips for testing (fast)
python src/mapping/porto_to_casa_mapper.py --input data/train.csv --limit 5000
```

## 3. Submit the Flink Job
Flink containers don't run your Python code automatically. You must submit the job to the Flink cluster so it can start listening to Kafka.

```bash
# Submit the GPS processing job to Flink
docker exec -it taasim-flink-jm flink run -py /opt/flink/usrlib/gps_job.py
```

## 4. Run the Simulators
Now that Flink is listening, start the Python simulators to push live data into Kafka.

**Open a new terminal and run the GPS Producer:**
*(This simulates taxis driving around the city, sending a GPS ping every second)*
```bash
python src/simulators/vehicle_gps_producer.py --broker localhost:9092 --speed 10
```

**Open another terminal and run the Trip Request Producer:**
*(This simulates people opening the app and requesting rides)*
```bash
python src/simulators/trip_request_producer.py --broker localhost:9092
```

## 5. Verify the Data in Cassandra
While the simulators are running, let's verify that Flink is correctly catching the data, figuring out the Zones, and saving it to the database.

**Check the live Taxi Positions:**
```bash
docker exec -it taasim-cassandra cqlsh -e "USE taasim; SELECT taxi_id, zone_id, lat, lon, event_time FROM vehicle_positions LIMIT 10;"
```

**Check the Trips table:**
```bash
docker exec -it taasim-cassandra cqlsh -e "USE taasim; SELECT trip_id, pickup_time, origin_zone, dest_zone, status FROM trips LIMIT 10;"
```

## 6. View the Dashboard (Optional)
If you have Grafana configured in your `docker-compose.yml`, you can view the live data visually!
1. Open your browser and go to: `http://localhost:3000`
2. Login with `admin` / `admin` (or whatever you configured).

## 7. Clean Up
When you are done testing, you can shut everything down gracefully:

```bash
# Stop all containers
docker-compose down
```
