# Day 9 – Distributed Execution & RDD Layer

## Goal
Re-implement a slice of Day 8 using the RDD API to make Spark internals visible: how shuffles happen, what data skew looks like in partitions, how lineage enables fault tolerance, and when caching actually helps.

---

### Task 1 – Re-implement regional revenue with RDD

**What to do:**  
Load `orders.csv` as raw text, parse it manually, compute revenue per region using `reduceByKey`.

```python
sc = spark.sparkContext

raw_rdd = sc.textFile(f"{DATA_ROOT}/orders.csv")

# Remove header
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

# CSV columns by position:
# 0=order_id, 1=customer_id, 2=product_id, 3=product_name, 4=category,
# 5=subcategory, 6=region, 7=city, 8=segment, 9=order_date, 10=order_year,
# 11=order_month, 12=order_quarter, 13=quantity, 14=unit_price, 15=discount,
# 16=total_amount, 17=status, 18=payment_method, 19=is_high_value

def parse_order(line):
    cols = line.split(",")
    try:
        region       = cols[6].strip()
        total_amount = float(cols[16].strip())
        status       = cols[17].strip()
        return (region, total_amount, status)
    except (IndexError, ValueError):
        return None

parsed_rdd = data_rdd.map(parse_order).filter(lambda x: x is not None)

# Keep only Completed, emit (region, total_amount)
completed_rdd = (parsed_rdd
    .filter(lambda x: x[2] == "Completed")
    .map(lambda x: (x[0], x[1])))

region_revenue_rdd = completed_rdd.reduceByKey(lambda a, b: a + b)

print("Revenue by region (RDD):")
for region, revenue in sorted(region_revenue_rdd.collect(), key=lambda x: -x[1]):
    print(f"  {region:<10} {revenue:>12,.2f}")
```

**Expected output:**
```
Revenue by region (RDD):
  East       3,973,904.84
  North      2,353,210.13
  West       1,867,649.18
  South      1,612,675.72
  Central    1,523,525.95
```

**Cross-check against Day 8:**  
The numbers must match the DataFrame output from Day 8, Task 3 (same filter, same field). If they don't, the parse logic has an off-by-one column index.

**Why `reduceByKey` instead of `groupBy` on RDD:**  
`reduceByKey` performs a partial aggregation on the map side before shuffling — only partial sums travel across the network, not raw rows. `groupByKey` shuffles every raw row. For 10,000 rows this is invisible; for 500,000 rows (full dataset) it is a ~10× difference in shuffle data volume.

---

### Task 2 – Make data skew visible with a custom partitioner

**What to do:**  
Apply a `HashPartitioner` (default) first, then a custom region-based partitioner, and compare partition sizes.

```python
from pyspark import StorageLevel

# Step 1: Default hash partitioning — observe how East records spread
default_partitioned = completed_rdd.partitionBy(5)  # 5 partitions, hash on region key
default_sizes = default_partitioned.glom().map(len).collect()
print("Default HashPartitioner sizes:", default_sizes)

# Step 2: Custom partitioner — pin each region to a specific partition
REGION_MAP = {"North": 0, "South": 1, "East": 2, "West": 3, "Central": 4}

def region_partitioner(region):
    return REGION_MAP.get(region, 0)

custom_partitioned = completed_rdd.partitionBy(5, region_partitioner)
custom_sizes = custom_partitioned.glom().map(len).collect()
print("Custom RegionPartitioner sizes:", custom_sizes)
```

**Expected output:**
```
Default HashPartitioner sizes : [1718, 1606, 1554, 1564, 1584]  # roughly even (hash spreads keys)
Custom RegionPartitioner sizes: [1691, 1248, 2834, 1289,  964]  # East (index 2) dominates
```

**Explanation of sizes:**  
North=1691, South=1248, East=2834, West=1289, Central=964 — matching the exact Completed order counts from Task 1.

**Why this is the point of the exercise:**  
The custom partitioner makes data skew **physically visible** in partition row counts. East's partition has nearly 2× the work of any other partition. This directly causes one task to run 2× longer than all others — that is what you will see in the Spark UI as a "slow task". In Day 11 you will deal with it using salting.

---

### Task 3 – RDD persistence: memory vs disk, timed comparison

**What to do:**  
Take the `parsed_rdd` (before filter and map), run the same computation twice under three persistence strategies, time the second run each time.

```python
import time
from pyspark import StorageLevel

def run_twice(rdd, label):
    rdd.count()                     # first run — computes from scratch
    t0 = time.time()
    rdd.count()                     # second run — hits cache (or recomputes if no cache)
    elapsed = time.time() - t0
    print(f"  {label:<20}  second run: {elapsed:.3f}s")

# Baseline — no caching
run_twice(parsed_rdd, "No cache")

# MEMORY_ONLY — deserialized Python objects in JVM heap
mem_rdd = parsed_rdd.persist(StorageLevel.MEMORY_ONLY)
run_twice(mem_rdd, "MEMORY_ONLY")
mem_rdd.unpersist()

# DISK_ONLY — serialised to local disk, evicted from memory
disk_rdd = parsed_rdd.persist(StorageLevel.DISK_ONLY)
run_twice(disk_rdd, "DISK_ONLY")
disk_rdd.unpersist()
```

**Expected pattern (dev dataset is small, so differences are modest):**

| Strategy | Second run |
|---|---|
| No cache | ~same as first run |
| MEMORY_ONLY | noticeably faster (sub-50ms) |
| DISK_ONLY | faster than no-cache, slower than memory |

**Why the pattern matters more than the numbers:**  
On the dev dataset the absolute times are tiny. The lesson is the **shape**: cache helps only when the same RDD is reused. If you only use an RDD once, caching wastes memory and adds serialization overhead. The rule: cache when an RDD feeds multiple downstream operations — which is exactly the pattern in the batch pipeline where `orders_clean` feeds both the regional and category aggregations.

---

### Task 4 – Broadcast a lookup table to avoid a shuffle join

**What to do:**  
Load `regions.csv` as a plain Python dict and broadcast it. Use it in a map to tag each order with whether it exceeded its region's revenue target.

```python
import csv

# Load regions.csv as a Python dict — 5 rows, pure Python, negligible size
with open(f"{DATA_ROOT}/regions.csv") as f:
    reader = csv.DictReader(f)
    region_targets = {
        row["region"]: float(row["target_revenue"])
        for row in reader
    }

# Broadcast to all executors — sent once, reused on every partition
bc_targets = sc.broadcast(region_targets)

# Tag each completed order with its region's annual target
def tag_with_target(record):
    region, amount = record
    target = bc_targets.value.get(region, 1.0)
    daily_target = target / 365
    above_daily = amount > daily_target
    return (region, amount, daily_target, above_daily)

tagged_rdd = completed_rdd.map(tag_with_target)

print("Sample tagged records:")
for row in tagged_rdd.take(5):
    print(f"  region={row[0]:<8}  amount={row[1]:>8.2f}  daily_target={row[2]:>8.2f}  above={row[3]}")
```

**Why this is the right use case for broadcast:**  
`regions.csv` is 5 rows — 300 bytes. Without broadcast you would do `completed_rdd.join(regions_rdd)`, which triggers a shuffle of all 10,000 order rows. With broadcast, the 5-row dict is sent once to each executor (zero shuffle). This pattern scales: even with 500,000 orders, the region dict stays 5 rows. Day 10 will use the same pattern via the DataFrame `broadcast()` hint.

---

### Task 5 – Inspect RDD lineage

**What to do:**  
Print the debug string of the `tagged_rdd` to see the full dependency chain.

```python
print(tagged_rdd.toDebugString().decode("utf-8"))
```

**What to look for:**  
```
(N) PythonRDD[...] at RDD at PythonRDD.scala
 |  MapPartitionsRDD[...]   <-- our .map(tag_with_target)
 |  PythonRDD[...]           <-- .filter(Completed)
 |  MapPartitionsRDD[...]    <-- .map(parse_order)
 |  /opt/spark/jobs/.../orders.csv MapPartitionsRDD[...]   <-- sc.textFile
```

Now persist `tagged_rdd` and re-print the lineage:

```python
tagged_rdd.persist(StorageLevel.MEMORY_ONLY)
tagged_rdd.count()  # trigger materialisation
print(tagged_rdd.toDebugString().decode("utf-8"))
```

The lineage above the checkpoint point is still there — Spark doesn't erase it. This is how fault tolerance works: if an executor holding a cached partition dies, Spark re-executes only the lineage steps needed to recompute **that partition**, not the entire dataset.

---

### Day 9 Deliverables

| Artifact | What it looks like |
|---|---|
| RDD revenue output | 5 `(region, revenue)` tuples matching Day 8 numbers exactly |
| Partition size comparison | Default roughly even; custom: North=1691, South=1248, **East=2834**, West=1289, Central=964 |
| Timing table | 3 rows: no-cache / MEMORY_ONLY / DISK_ONLY, second-run times |
| Broadcast sample | 5 rows showing `region`, `amount`, `daily_target`, `above_daily` |
| `toDebugString` output | Full lineage chain from `sc.textFile` to `tagged_rdd` |
