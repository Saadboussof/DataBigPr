# Understanding `vehicle_gps_producer.py`

This script is the **"Live Simulator"** for your taxi ecosystem. In the real world, the TaaSim platform would receive a constant stream of location data from the GPS trackers installed in the taxi drivers' smartphones. Because you don't have 1,000 real taxis driving around Casablanca right now, this file pretends to be them.

It takes the historical route data from the Porto `train.csv`, transforms the coordinates so they look like they are in Casablanca, and fires them into your live streaming server (Kafka) one second at a time. It mimics the behavior of live cars driving, stopping, and picking up people.

---

## 1. What exactly does this script generate?

Every time it runs through its loop (simulating a "tick" of the clock), it generates a JSON message (a "payload") for **every single active taxi** and sends it to the Kafka topic named `raw.gps`. 

Here is an exact example of what this script generates and pushes entirely automatically into the Kafka system:

### Example of the Generated Output (The Payload)
```json
{
  "taxi_id": "20000589",
  "timestamp": 1711867200000,
  "lat": 33.5781,
  "lon": -7.6123,
  "speed": 42.4,
  "status": "engaged"
}
```

### Breaking down the Payload fields:
* **`taxi_id`**: A unique ID for the car ("Car #20000589").
* **`timestamp`**: The live, current computer time in milliseconds, acting as if the taxi sent this "right now".
* **`lat` / `lon`**: The calculated, remapped GPS coordinates. Thanks to the `transform_coordinate` function in the script, these Porto points have been shifted to accurately represent streets somewhere in Casablanca, Morocco.
* **`speed`**: A randomized speed (between 20.0 km/h and 60.0 km/h) generated to simulate how fast the car is moving.
* **`status`**: Are there passengers in the car right now? 
   * **`engaged`**: The car is still progressing through its list of GPS route points (driving passengers).
   * **`available`**: The car has reached the final point in its trajectory array (the trip is over), so it is now empty and looking for work.

---

## 2. How the Generation Process Works (Step-by-Step)

Here is exactly how the script creates that output:

1. **Loads the History**: It looks into the massive `train.csv` file and scoops up 50,000 rows. It randomly picks 1,000 trips from that chunk to act as the "Active Vehicles" for this simulation.
2. **Parses the Polylines**: It breaks the `POLYLINE` text string into an actual List/Array of coordinate points `[[lon,lat], [lon,lat], ...]`. It starts every single taxi at `index 0` (the start of their journey).
3. **The Infinite Loop (`while True`)**: The script loops endlessly. In each loop:
   * It looks at a taxi.
   * **The 5% Trap (`random.random() < 0.05`)**: It rolls a dice. Your professor demanded a "5% chance of a 60s blackout". This simulates a taxi driving under a bridge or losing internet. If it lands on the 5%, it skips generating a payload for that car entirely for this round!
   * **Coordinate Translation**: It passes the Porto coordinates into the `transform_coordinate` math function to lock them into Casablanca boundaries, adding a tiny sprinkle of "GPS Noise" (`random.gauss`) to make the data look authentically messy.
   * **Transmission**: It packages the translated data up into the JSON block shown above and fires it at `localhost:9092` (the Kafka Broker).
   * **Move Forward**: It advances `current_idx += 1`, meaning the next time the loop hits this taxi, it will read the *next* coordinate in the street path so the car physically "moves".
   * **Sleep**: It pauses (`time.sleep`) for a configured fraction of 15 seconds to respect the simulated speed before firing the next batch of coordinates. 

When the Kafka server receives these payloads, Flink (your stream processor) is listening to the pipe, picking up these coordinates instantly, calculating exactly what zone in Casablanca the car is in, and sending that to Cassandra so Grafana can draw it on a map!