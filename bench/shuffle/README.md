# The shuffle benchmark

The taxi benchmark (`deploy/taxi.mojo`) spends most of its time reading Parquet from S3. This one spends it in the frameworks' own code and in moving data between tasks: per-trip features computed in each framework's language, a group-by with too many groups to send to the driver, done as a shuffle, then a top-k. It runs on dista ([deploy/shuffle.mojo](../../deploy/shuffle.mojo)) and Ray ([bench/ray/shuffle_ray.py](../ray/shuffle_ray.py)), over the same NYC taxi Iceberg table, 79.5 million trips in 24 monthly files.

## The query: trips by zone pair and hour

**Columns read:** `PULocationID`, `DOLocationID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime` (timestamps, microseconds), `trip_distance`, `fare_amount`, `tip_amount`.

**A trip counts** when none of those is null, both zones are in 1..265, `trip_distance > 0`, `fare_amount > 0`, and its duration, dropoff minus pickup, is more than 0 and at most 4 hours.

**Per trip:**
- `hour = floor(pickup_us / 3_600_000_000) mod 24`, the pickup's hour of day;
- `speed = trip_distance / (duration_us / 3_600_000_000)`, in miles per hour;
- `tip_ratio = tip_amount / fare_amount`.

**Group key:** `key = (PULocationID * 266 + DOLocationID) * 24 + hour`. There are 266 × 266 × 24 = 1,698,144 possible keys; the table's 76.8 million counted trips fall in 589,165 of them.

**Per group:** the trips `n`, and the sums of `speed`, `tip_ratio` and `fare_amount`.

**The shuffle.** One map task per data file computes its trips' groups and splits them into `R` partitions, partition `key mod R`. Reducer `j` takes partition `j` from every map task and merges them. Each reducer returns its totals (groups, trips, and the three sums over every group) and its 10 groups with the most trips, ties broken by the smaller key. The driver adds the totals and merges the top-10s. `R` is `SHUFFLE_REDUCERS`, default 16. Partitions stay where their map task made them, and each reducer fetches its partitions from there; nothing but the reducers' small answers reaches the driver.

**The answer**, printed by each framework:

```
ANSWER zone_hours <groups> <trips> <sum_speed> <sum_tip_ratio> <sum_fare>
TOP <rank> <key> <pu> <do> <hour> <n> <mean_speed> <mean_tip_ratio>
```

Groups, trips, and the top 10's keys and counts must match exactly between frameworks and against one process; sums and means to 1e-9 relative, since the order sums are added in differs.

## Running it

| Variable | Meaning | Default |
|---|---|---|
| `TAXI_METADATA` | the table's metadata location, `file://` or `s3://` | required |
| `SHUFFLE_REDUCERS` | reduce tasks, `R` | 16 |
| `TAXI_PASSES` | distributed runs; each prints `TIME zone_hours <pass> <ms>`, and later passes are checked against the first | 1 |
| `TAXI_MODE` | `both`: pass 1 is checked against the same query in one process; `distributed`; `local`: one process only | `both` |

dista's also takes `TAXI_PREFIX` and `TAXI_ROOT`, as `deploy/taxi.mojo` does.

```
pixi run -e data bash -c 'mojo build -I src $DEP_INC -I deploy deploy/shuffle.mojo -o build/shuffle'
TAXI_METADATA=... pixi run -e data ./build/shuffle    # dista, 4 local workers
TAXI_METADATA=... python bench/ray/shuffle_ray.py    # Ray, a local Ray; PYTHONPATH has taxibench and bench/ray
tests/kind_taxi.sh                                   # dista on kind: the taxi queries, then this
RAY_SCRIPT=shuffle_ray.py bench/ray/run_kind.sh      # Ray on kind
JOB=shuffle bench/cloud/bench matrix                 # both on AWS, 3, 6 and 12 nodes
JOB=shuffle bench/cloud/bench local                  # both in one process on AWS
```

Both give the same answer on the local table: 589,165 groups, 76,833,462 trips, the same top 10, and sums equal to about 1e-15. There's no Rust version: its reference has no way for workers to fetch from each other, and a shuffle through the driver wouldn't measure the same thing.
