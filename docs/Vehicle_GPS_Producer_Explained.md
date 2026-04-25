# How `vehicle_gps_producer.py` Works

This Python script is the "Simulator" that acts as the physical GPS tracker sitting inside of thousands of moving Taxis in Casablanca. 

To answer your two main questions directly:
1. **Does every iteration insert a `[lon, lat]` into the table?** 
   * Technically, it sends it to **Kafka** (a messaging queue), not directly to a static database table. Every cycle, it looks at the *next* coordinate for every active taxi and broadcasts its position live to the network.
2. **Does it start from [0, 0]?** 
   * **No.** It reads from the massive JSON Array recorded inside the CSV files `POLYLINE` column, starting at `[longitude, latitude]` Index `0` of that specific trip. It places the Taxi exactly where the ride started.

---

## Step-by-Step Explanation With An Example

Imagine we load just **one single row** from the `train.csv` file. 

### 1. Data Loading (`main` function)
The script reads `train.csv` and randomly selects 1,000 trips. Let's pretend it selects Taxi `20000380`.
The `POLYLINE` column for this Taxi might look like this:
```json
[ [-8.618643, 41.141412], [-8.618499, 41.141376], [-8.618364, 41.141331] ]
```

The script gives this Taxi a tracker called `current_idx` and sets it to `0`. 
This index represents "where the car is right now".

### 2. The Main Loop (`while True:`): 
The script goes into an infinite loop, acting as time passing by. 
In the real world, the GPS hardware sends a signal every 15 seconds. The script calculates a sleep timer to simulate this.

#### **Iteration 1 (Time = 0 seconds)**
* The script looks at Taxi `20000380`. Its `current_idx` is `0`.
* It grabs the 0th GPS point: `[-8.618643, 41.141412]` (in Porto).
* **Geospatial Mapping**: It runs `transform_coordinate()`, squeezing those Porto coordinates into Casablanca, maybe changing it to: `[-7.59001, 33.57012]`.
* **Kafka Event**: It builds a package of JSON data:
  ```json
  {
      "taxi_id": "20000380",
      "timestamp": 1711900000000,
      "lat": 33.57012,
      "lon": -7.59001,
      "speed": 45.2,
      "status": "engaged"
  }
  ```
* It throws this JSON package at the Kafka server (topic `raw.gps`).
* It updates the taxi's `current_idx` to `1`. 

*Then the script sleeps for 1.5 seconds (simulating 15 real-world seconds via `speed_multiplier=10`).*

#### **Iteration 2 (Time = 15 simulated seconds later)**
* The script looks at Taxi `20000380` again. Its `current_idx` is now `1`.
* It grabs the 1st GPS point (the next step in the journey): `[-8.618499, 41.141376]`.
* *Wait!* The code has this special logic: 
  ```python
  # PDF constraint: 5% chance of 60s+ blackout
  if random.random() < 0.05:
      trip['current_idx'] += 1
      continue
  ```
  **What is this 5% rule exactly? And what about the 60s sleep?**
  * The PDF requires simulating a situation where a taxi goes through a tunnel and loses GPS signal for 60 seconds. 
  * **Does the whole program sleep for 60s?** **NO.** The sleep timer (`time.sleep(real_time_sleep)`) applied at the very end of the loop does *not* change. The whole program keeps ticking every 1.5 seconds (which represents 15s in the simulation).
  * **What actually happens when it hits 5%?** If this specific taxi hits the 5% chance during its turn, the script simply `continues` (skips). It throws away this coordinate instead of sending it to Kafka, but it *does* move the car forward (`current_idx += 1`). 
  * **The 15s vs 60s flaw:** You noticed something very smart! Because the original data was recorded every 15 seconds, skipping *one* iteration only creates a gap of **30 seconds** (15s missed + 15s until the next one), not 60 seconds! 
  * **To actually create a 60s blackout**, the code *should* technically skip 4 coordinates forward (`trip['current_idx'] += 4`). Right now, the code only skips 1 coordinate at a time. It seems whoever wrote the python script left the comment `60s+ blackout` but only programmed it to skip a single 15-second interval.
* Assuming no blackout, it transforms the point to Casablanca and sends a new Kafka JSON message with the new Latitude/Longitude.
* `current_idx` becomes `2`.

*Sleeps again.*

#### **Iteration 3 (Time = 30 simulated seconds later)**
* It grabs `current_idx` `2`, processes the final point `[-8.618364, 41.141331]`.
* Sends the last Kafka event. Since this is the last coordinate in the array, it changes `"status": "available"` so the system knows the dropoff happened!
* `current_idx` becomes `3`.

#### **Iteration 4 (Time = 45 simulated seconds later)**
* The script checks Taxi `20000380`. `current_idx = 3`.
* The list only has 3 items (indices 0, 1, 2). 3 is out of bounds!
* The script hits `if idx >= len(points): continue`. It ignores this car. The trip is officially over.

### 3. The Reboot
Once every single one of the 1,000 selected taxis finishes dragging themselves to the end of their `POLYLINE` array, the variable `active_count` hits `0`. 

The script prints:
`"All active trips finished. Restarting simulation cycle."`

It resets every car's `current_idx` back to `0`, and the entire digital fleet starts replaying their routes all over again.