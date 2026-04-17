# Detailed Explanation: How We Fixed the "Trips in Ocean" Issue

## 1) Problem We Observed
Some transformed Casablanca trajectories were still drawn in ocean areas instead of staying on land.

This happened in both places where coordinates are produced/used:
1. Batch transformation path (Spark).
2. Streaming simulation path (Kafka producer).

So even if one side was cleaned, the other side could still emit coastal/ocean outliers.

## 2) Why It Was Happening
The original Porto to Casablanca mapping was a linear bounding-box remap.
That is fine for coarse relocation, but not enough near coasts.

Main causes:
1. Coastline geometry is irregular, but bounding-box mapping is rectangular.
2. Added GPS noise can move a valid near-coast point slightly into water.
3. Simple coastline tolerance alone can still leave tiny outside-polygon points.
4. Floating-point rounding near boundary can re-create outside points after clipping.

## 3) Fix Strategy We Applied
We used a shift-left geospatial cleaning strategy:
1. Enforce land validity at generation time (producer).
2. Enforce land validity again in batch transformation (Spark Silver cleaning).
3. Keep notebook transformation logic aligned with the same strict rules.
4. Clip trajectories at first ocean crossing, do not allow continuation into ocean.
5. Avoid road snapping (by design), keep synthetic trajectory shape while forcing land validity.

## 4) Exact Code Changes

### 4.1 Spark Batch Cleaner
File: `src/spark/porto_to_casablanca.py`

We added shared land/coast geometry and strict helper functions:
1. `COASTLINE_POLYLINE`: approximate Casablanca coastline segment.
2. `LAND_POLYGON`: coastline plus eastern/southern bbox closure.
3. `point_in_polygon(...)`: ray-casting inclusion test.
4. `point_to_segment_projection(...)`: nearest-point projection math.
5. `distance_to_coastline_sq(...)`: distance to coastline polyline.
6. `is_land_point(...)`: accepts inside polygon or coastline-epsilon near points.
7. `project_to_land_boundary(...)`: projects invalid points to nearest boundary segment.
8. `nudge_inside_land(...)`: moves boundary/near-boundary point slightly inland toward centroid.
9. `coerce_to_land_point(...)`: final strict point coercion pipeline.
10. `segment_intersection_with_t(...)` and `first_polygon_intersection(...)`: find first crossing point with land boundary.
11. `clip_polyline_to_land(...)`: trajectory-level clipping and cleanup.

Then we updated core flow:
1. `transform_poly(...)`
   - Parses original polyline.
   - Drops invalid source points outside Porto bounds.
   - Maps remaining points to Casablanca bbox.
   - Applies `clip_polyline_to_land(...)`.
   - Returns cleaned JSON polyline or `[]`.
2. `main(...)`
   - Keeps only rows where transformed polyline is not `[]`.

Important final precision fix:
- We removed aggressive rounding in critical boundary return paths.
- Reason: rounding near coastline can move a barely-valid boundary point back outside polygon.

### 4.2 Streaming Producer Guardrail
File: `src/simulators/vehicle_gps_producer.py`

We mirrored the strict geometry logic in the producer so events are safe before Kafka publish:
1. Added same coastline/land polygon model.
2. Added `is_land_point`, `project_to_land_boundary`, `nudge_inside_land`, `coerce_to_land_point`.
3. Updated `transform_coordinate(...)`:
   - Porto point is mapped to Casablanca.
   - Noise is added.
   - If point becomes invalid, project to boundary.
   - Always run strict coercion to guarantee final in-land coordinate.

Also cleaned minor code noise:
- Removed unused imports (`uuid`, `numpy`).

### 4.3 Notebook Transformation Sync
File: `notebooks/week1_exploration.ipynb`

We updated the transformation cell to match the same strict logic used by Spark and producer:
1. Same coastline/land polygon assumptions.
2. Same boundary projection and inland nudge behavior.
3. Defensive clipping before rendering.

This keeps offline exploration behavior consistent with pipeline behavior.

## 5) The Core Algorithm (Point Level)
For each mapped coordinate `(lon, lat)`:
1. Clamp to Casablanca bbox.
2. If strictly inside land polygon, keep it.
3. Otherwise project to nearest land boundary segment.
4. Nudge inward (small step toward land centroid, adaptive retries).
5. Emit only the corrected in-land point.

Pseudo-flow:

```text
if inside_polygon(point):
    return point
boundary_point = project_to_boundary(point)
inland_point = nudge_inward(boundary_point)
return inland_point
```

## 6) The Core Algorithm (Trajectory Level)
For each trajectory polyline:
1. Start with first valid land point.
2. For each next point:
   - If valid land point: coerce and append.
   - If invalid: compute first segment-boundary intersection, append corrected boundary point, stop trajectory.
3. Drop trajectories with fewer than 2 valid points after cleaning.

This ensures no line continues deep into ocean once it exits land.

## 7) Why This Solves the Issue Better Than Before
Previous logic reduced errors but could still leave tiny coastal leakage.
New logic removes that residual by combining:
1. Coastal epsilon acceptance.
2. Nearest-boundary projection.
3. Guaranteed inland nudge.
4. No rounding that can re-break boundary validity.

In short: every final emitted point is coerced to land, not just filtered by tolerance.

## 8) Validation Evidence We Collected
During debugging and metric checks:
1. Parsed map artifact showed large pre-cleaning leakage (`before_outside = 501`).
2. Iterative fixes reduced residuals (`after_outside = 17`, then `7`).
3. Final strict Spark logic check reached `after_outside = 0` in exact module-level evaluation.
4. Producer Monte Carlo validation (20,000 samples, dependency-stubbed local run) produced `outside = 0`.
5. Python compile checks for updated modules passed.

## 9) Constraints and Remaining Operational Step
One local notebook runtime had Spark/Java mismatch, so full visual notebook rerun was pending in that environment.

Final operational step is to rerun notebook/pipeline in the project docker runtime and regenerate map artifacts.
That gives final visual confirmation with the newest strict code path.

## 10) Final Outcome
We did not apply a UI-only mask.
We fixed the problem at the data generation and data transformation layers.

Result:
- Producer emits in-land coordinates.
- Spark transformation clips/coerces trajectories to land.
- Notebook logic is aligned with the same strict geospatial rules.

This is why the ocean-trip issue is now resolved at its source.
