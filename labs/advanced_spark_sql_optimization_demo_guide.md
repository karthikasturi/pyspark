# Advanced Spark SQL Optimization Demo Guide

**Spark Version:** Apache Spark 4.1.1
**Mode:** Docker cluster (spark-master + 2 workers, local machine)
**Language:** Python (PySpark)

---

## 0. Setup

### Spark Version

This guide uses **Apache Spark 4.1.1** (`apache/spark:4.1.1-scala2.13-java21-python3-ubuntu`).
All APIs, configuration keys, and plan output descriptions are accurate for this
version. Spark 4.x continues to ship with Adaptive Query Execution (AQE) enabled
by default, ANSI SQL support, and Catalyst optimizer improvements that were
introduced in the Spark 3.x line.

### Starting PySpark Locally

```bash
# Interactive shell inside the spark-master container
docker exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077

# Submit a job
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_spark_sql/<script>.py
```

### Docker (quickstart cluster)

```bash
# From the quickstart/ directory:
cd quickstart
docker compose up -d

# Wait for the cluster to be healthy, then submit any script:
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_spark_sql/01_shuffle_in_aggregation.py
```

UIdefaults once the cluster is running:

| UI | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| History Server | http://localhost:18080 |
| Driver (during job) | http://localhost:4040 |

### Basic SparkSession (used in every example)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("optimization_demo")
    # master is supplied by spark-submit --master spark://spark-master:7077
    # do NOT hard-code .master() here; let spark-submit control it
    .config("spark.sql.shuffle.partitions", "8")   # keep low for demo datasets
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Reading Execution Plans

Every example uses `df.explain("formatted")`. The plan is read **bottom-up**:

- The **bottom node** is the first operation (scan / in-memory relation).
- The **top node** is the last operation (the result you receive).
- `Exchange` nodes represent **shuffles** (data movement across the network).
- `Sort` nodes represent **in-partition sorting**.
- `Broadcast` nodes represent data being copied to every executor.

---

## 1. Challenge: Unexpected Shuffle in Aggregation

### Problem Explanation

A `groupBy().agg()` forces Spark to move all rows with the same key to the
same executor before aggregating. This data movement is called a **shuffle**
and is the most expensive operation in Spark: it involves serialization,
network I/O, disk spills (if memory is insufficient), and deserialization.

### Why It Happens

Data is initially distributed across partitions by row index (or by the source
file layout). When you group by a column, rows for the same key may live on
different executors. Spark inserts an `Exchange hashpartitioning(key, N)` node
in the physical plan to co-locate them.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/01_shuffle_in_aggregation.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = (
    SparkSession.builder.appName("shuffle_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Skewed dataset: 90% of rows have category "A"
data = (
    [("A", i) for i in range(900)]
    + [("B", i) for i in range(50)]
    + [("C", i) for i in range(30)]
    + [("D", i) for i in range(20)]
)

schema = StructType([
    StructField("category", StringType(), False),
    StructField("value",    LongType(),   False),
])

df = spark.createDataFrame(data, schema)

# Baseline aggregation
agg_df = df.groupBy("category").agg(
    F.count("value").alias("cnt"),
    F.sum("value").alias("total"),
)
agg_df.show()
```

### Inspecting with explain("formatted")

```python
agg_df.explain("formatted")
```

**What to look for in the plan:**

```
== Physical Plan ==
AdaptiveSparkPlan (1)
+- HashAggregate (2)                         <-- final aggregation
   +- Exchange hashpartitioning(category, 8) <-- THE SHUFFLE
      +- HashAggregate (4)                   <-- partial aggregation (pre-shuffle)
         +- Scan ExistingRDD (5)
```

The `Exchange hashpartitioning(category, 8)` node confirms the shuffle.
Spark actually performs a two-phase aggregation (partial pre-shuffle, final
post-shuffle) to reduce data volume -- but the shuffle itself remains.

### Optimized Version

```python
# Pre-partition by the groupBy key before aggregating
df_repartitioned = df.repartition(8, "category")

agg_df_opt = df_repartitioned.groupBy("category").agg(
    F.count("value").alias("cnt"),
    F.sum("value").alias("total"),
)
agg_df_opt.explain("formatted")
```

### What Improved and Why

- The shuffle is now an **explicit `repartition(8, "category")`** step rather
  than an implicit one inside `groupBy`. This gives you control over when it
  happens and on how many partitions.
- When you chain **multiple aggregations on the same key**, the data is already
  co-located after the first `repartition`, so each subsequent `groupBy` on
  that key does **not** add another `Exchange` node.
- The `spark.sql.shuffle.partitions` config controls the number of output
  partitions after every shuffle. Lower it from the default `200` in
  development (e.g., `8`) to avoid creating 200 tiny partitions on small data.

---

## 2. Challenge: Slow Window Function

### Problem Explanation

`Window.partitionBy("col").orderBy("other_col")` forces two operations:
a **shuffle** (to co-locate rows with the same partition key) and a **sort**
(to order rows within each partition for the window frame). Both are expensive.

### Why It Happens

Window functions require a well-defined order within each partition boundary.
Spark cannot apply a window function until all rows for a given `partitionBy`
value are on the same executor AND sorted. The physical plan always contains
an `Exchange` followed by a `Sort`.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/02_slow_window_function.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("window_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Employee dataset: 5 departments, 20 employees each
data = []
depts = ["Engineering", "Sales", "HR", "Finance", "Marketing"]
for idx, dept in enumerate(depts):
    for i in range(20):
        data.append((f"emp_{idx}_{i}", dept, 30000 + i * 1000 + idx * 500))

df = spark.createDataFrame(data, ["name", "dept", "salary"])

window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))
df_ranked = df.withColumn("rank", F.rank().over(window_spec))
df_ranked.filter(F.col("rank") <= 3).show()
```

### Inspecting with explain("formatted")

```python
df_ranked.explain("formatted")
```

**What to look for:**

```
== Physical Plan ==
Window [rank() windowspecdefinition(dept, salary DESC, ...)]
+- Sort [dept ASC, salary DESC]
   +- Exchange hashpartitioning(dept, 8)   <-- shuffle
      +- Scan ExistingRDD
```

Both `Exchange hashpartitioning` and `Sort` are present.

### Optimized Version A: Filter Before the Window

Push filters upstream so the window operates on fewer rows.

```python
# Only compute rank for employees earning above a threshold
df_filtered = df.filter(F.col("salary") >= 35000)
df_ranked_opt = df_filtered.withColumn("rank", F.rank().over(window_spec))
df_ranked_opt.filter(F.col("rank") <= 3).explain("formatted")
```

**What to look for:** The `Filter` node now appears **before** the `Exchange`
in the plan, meaning fewer rows are shuffled.

### Optimized Version B: repartitionByRange

Align the physical partition layout with the window spec before applying it.

```python
df_pre = df.repartitionByRange(8, "dept", F.desc("salary"))
df_ranked_pre = df_pre.withColumn("rank", F.rank().over(window_spec))
df_ranked_pre.explain("formatted")
```

**What to look for:** `RangePartitioning` exchange on `(dept, salary DESC)`
matches the window spec. Spark may be able to skip a re-sort step inside
the window operator because the data is already in the correct order.

### What Improved and Why

- **Filter first**: reduce the rows the window function processes. Every row
  eliminated before the shuffle saves network bytes and sort time.
- **repartitionByRange**: when the physical data layout already matches the
  window's `partitionBy + orderBy`, Spark can avoid a redundant in-partition
  sort. This matters most when the same window spec is applied repeatedly
  within a pipeline.
- Avoid applying a window function on an **unbounded dataset** without a
  preceding filter. Always scope the dataset to the population of interest
  before windowing.

---

## 3. Challenge: Inefficient Subquery

### Problem Explanation

SQL `WHERE col IN (SELECT ...)` or scalar subqueries are convenient to write
but can hide expensive operations. Spark rewrites them internally as join
operations. Without explicit control, the join strategy (broadcast vs.
sort-merge) is decided by statistics that may be unavailable or inaccurate.

### Why It Happens

- Spark translates `IN (SELECT ...)` into a **LEFT SEMI JOIN**.
- A scalar subquery `(SELECT MAX(x) FROM t)` is rewritten as a
  **BroadcastNestedLoopJoin** if the subquery cannot be proven to return one row.
- Without table statistics (`ANALYZE TABLE`), Spark may miss the opportunity
  to broadcast the inner result, falling back to a shuffle join.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/03_inefficient_subquery.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("subquery_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto-broadcast
    .getOrCreate()
)

orders_data = [(i, f"customer_{i % 10}", float(100 + i * 3)) for i in range(200)]
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])

high_value_data = [
    (f"customer_{c}", float(sum(100 + i * 3 for i in range(200) if i % 10 == c)))
    for c in range(10)
]
customer_totals_df = spark.createDataFrame(high_value_data, ["customer_id", "total_spend"])

orders_df.createOrReplaceTempView("orders")
customer_totals_df.createOrReplaceTempView("customer_totals")

# Subquery approach
df_subquery = spark.sql("""
    SELECT *
    FROM orders
    WHERE customer_id IN (
        SELECT customer_id FROM customer_totals WHERE total_spend > 1000
    )
""")
df_subquery.show(5)
```

### Inspecting with explain("formatted")

```python
df_subquery.explain("formatted")
```

**What to look for:**

```
== Physical Plan ==
BroadcastHashJoin [customer_id], [customer_id], LeftSemi, BuildRight
:- Scan ExistingRDD [order_id, customer_id, amount]
+- Subquery
   +- Filter (total_spend > 1000)
      +- Scan ExistingRDD [customer_id, total_spend]
```

The inner `SELECT` is shown as a separate subquery tree. With auto-broadcast
disabled you will see an `Exchange` on both sides instead.

### Optimized Version

```python
# Step 1: compute high-value customers once, explicitly
high_value = customer_totals_df.filter(F.col("total_spend") > 1000).select("customer_id")

# Step 2: broadcast + explicit join (left semi is equivalent to IN)
df_opt = orders_df.join(
    F.broadcast(high_value),
    on="customer_id",
    how="left_semi",
)
df_opt.explain("formatted")
```

**What to look for in the optimized plan:**

```
== Physical Plan ==
BroadcastHashJoin [customer_id], [customer_id], LeftSemi, BuildRight
:- Scan ExistingRDD [order_id, customer_id, amount]
+- BroadcastExchange HashedRelationBroadcastMode
   +- Filter (total_spend > 1000)
      +- Scan ExistingRDD [customer_id, total_spend]
```

Single scan per table, the small side is broadcast, no shuffle on the large side.

### What Improved and Why

- Writing the join **explicitly** gives you control over join strategy. You can
  apply `F.broadcast()` without relying on statistics.
- **Pre-aggregating** (filtering high-value customers in a separate step) makes
  the operation visible in the lineage. It also allows reuse: if you need the
  high-value customer list multiple times, cache it after computation.
- For scalar subqueries (`SELECT MAX(...)`), pre-compute the scalar in Python
  (`.agg(F.max(...)).first()[0]`) and pass it as a literal to avoid a join
  entirely.

---

## 4. Challenge: Large-Small Join Without Broadcast

### Problem Explanation

Joining a large DataFrame (~millions of rows) with a small lookup table
(tens or hundreds of rows) should be nearly free -- but Spark defaults to
**SortMergeJoin** for safety, which shuffles and sorts **both** sides.

### Why It Happens

Spark estimates DataFrame size from table statistics. For programmatically
created DataFrames or views without `ANALYZE TABLE`, size estimates are
unavailable and Spark falls back to SortMergeJoin. The threshold
`spark.sql.autoBroadcastJoinThreshold` (default 10 MB) only kicks in when
size statistics are known.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/04_large_small_join_broadcast.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("broadcast_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # force SMJ
    .getOrCreate()
)

# Large side: 100,000 transactions
large_df = (
    spark.range(0, 100_000)
    .withColumn("product_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount",      F.round(F.rand(seed=42) * 500, 2))
    .drop("id")
)

# Small side: 20 product names
product_data = [(i, f"Product_{chr(65 + i)}", f"Cat_{i % 4}") for i in range(20)]
small_df = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])

# Baseline join (no broadcast)
joined = large_df.join(small_df, on="product_id", how="inner")
joined.show(5)
```

### Inspecting with explain("formatted")

```python
joined.explain("formatted")
```

**What to look for (baseline):**

```
== Physical Plan ==
SortMergeJoin [product_id], [product_id], Inner
:- Sort [product_id ASC]
:  +- Exchange hashpartitioning(product_id, 8)  <-- shuffle large side
+- Sort [product_id ASC]
   +- Exchange hashpartitioning(product_id, 8)  <-- shuffle small side too
      +- Scan ExistingRDD [product_id, ...]
```

Both sides are shuffled and sorted -- very expensive for a 20-row lookup table.

### Optimized Version

```python
# Add F.broadcast() hint on the small side
joined_opt = large_df.join(F.broadcast(small_df), on="product_id", how="inner")
joined_opt.explain("formatted")
```

**What to look for (optimized):**

```
== Physical Plan ==
BroadcastHashJoin [product_id], [product_id], Inner, BuildRight
:- Scan ExistingRDD [product_id, amount]      <-- large side: NO shuffle
+- BroadcastExchange HashedRelationBroadcastMode
   +- Scan ExistingRDD [product_id, ...]      <-- small side: broadcast only
```

The large side is **not shuffled**. The small side is broadcast (copied) to
every executor once and held in memory as a hash table.

### What Improved and Why

- **Eliminated two Exchange (shuffle) nodes and two Sort nodes**.
  The large DataFrame never moves -- each executor holds the full small side
  in memory and probes it locally.
- **Always use `F.broadcast()` explicitly** for known-small DataFrames.
  Do not rely on `autoBroadcastJoinThreshold` when DataFrame size statistics
  are not computed.
- Broadcast join limit: if the broadcasted side is unexpectedly large (e.g.,
  the result of a large join that Spark underestimates), executor OOM errors
  can occur. Know your data sizes before broadcasting.

---

## 5. Challenge: Data Skew in Join

### Problem Explanation

When one join key value accounts for a large fraction of rows (e.g., 80%),
the partition assigned to that key becomes a **hotspot**. All other partitions
finish quickly while one straggler task processes the majority of the data,
making the entire stage as slow as that single task.

### Why It Happens

Spark hash-partitions join keys. Every row with key `"A"` lands in the same
partition by design. If `"A"` has 800 rows and every other key has 20 rows,
the `"A"` partition is 40x larger than average. It will process 40x more data,
spill to disk, and delay the job completion.

### Detecting Skew

```python
df.groupBy("key").count().orderBy(F.desc("count")).show()
```

In the **Spark UI**: open the skewed stage, view the **Tasks** tab, and sort by
**Duration**. If the 99th percentile task takes 10x longer than the median, you
have skew.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/05_data_skew_in_join.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("skew_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")   # disable AQE first
    .getOrCreate()
)

# Left side: 80% of rows have key "A"
left_data = (
    [("A", i) for i in range(800)]
    + [("B", i) for i in range(100)]
    + [("C", i) for i in range(50)]
    + [("D", i) for i in range(50)]
)
left_df  = spark.createDataFrame(left_data, ["key", "value"])
right_df = spark.createDataFrame([("A","Alpha"),("B","Beta"),("C","Gamma"),("D","Delta")], ["key","label"])

joined_skewed = left_df.join(right_df, on="key", how="inner")
joined_skewed.count()
```

### Mitigation A: Manual Salting

Salting appends a random bucket number to skewed keys on the left side, then
**explodes** the right side to have one copy per bucket. This spreads the
skewed partition across `SALT` partitions.

```python
SALT = 4

# Salt the left side with a random bucket suffix
left_salted = left_df.withColumn(
    "salted_key",
    F.concat(F.col("key"), F.lit("_"), (F.rand(seed=7) * SALT).cast("int").cast("string"))
)

# Explode the right side: each key gets SALT copies
right_exploded = (
    right_df
    .withColumn("salt_bucket", F.explode(F.array([F.lit(i) for i in range(SALT)])))
    .withColumn("salted_key",  F.concat(F.col("key"), F.lit("_"), F.col("salt_bucket").cast("string")))
    .drop("salt_bucket")
)

joined_salted = (
    left_salted
    .join(right_exploded, on="salted_key", how="inner")
    .drop("salted_key")
)
joined_salted.explain("formatted")
```

**What to look for:** `Exchange hashpartitioning(salted_key, 8)` -- the higher
cardinality of `salted_key` distributes load more evenly across partitions.

### Mitigation B: Adaptive Query Execution (AQE)

AQE detects and automatically splits skewed partitions at runtime -- no code
change required beyond enabling it.

```python
spark.conf.set("spark.sql.adaptive.enabled",                           "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                  "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",    "2")

joined_aqe = left_df.join(right_df, on="key", how="inner")
joined_aqe.explain("formatted")
```

**What to look for:**

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true      <-- AQE is active
+- ... SkewJoin ...                     <-- AQE split the skewed partition
```

After execution (with `isFinalPlan=true`), the plan shows that the skewed
partition was split into sub-tasks.

### What Improved and Why

| Approach | How it works | Trade-off |
|---|---|---|
| Salting | Distributes skewed key across N buckets manually | Right side grows N times; manual code |
| AQE | Detects and splits skewed partitions at runtime | Requires Spark 3.0+; skew must exceed thresholds |

- AQE is the preferred production approach in Spark 3.x. Enable it globally
  (`spark.sql.adaptive.enabled=true`) -- it is the default since Spark 3.2.
- Use salting when AQE is unavailable or the skew is so extreme that even
  AQE's sub-splitting is insufficient.

---

## 6. Challenge: Executor Memory Pressure via Caching

### Problem Explanation

Calling `.cache()` or `.persist()` stores a DataFrame in executor memory.
If the cached data exceeds available storage memory, Spark **evicts** blocks
(using LRU policy). Evicted partitions are silently recomputed on the next
access -- making the job slower than if you had never cached.

### Why It Happens

Spark uses a **unified memory model** (introduced in Spark 1.6). A single memory
pool is shared between Storage (cache) and Execution (shuffle buffers, sort
buffers, hash tables):

```
JVM Heap
+--- Reserved Memory (300 MB fixed)
+--- spark.memory.fraction = 0.6  -->  Unified Pool
|         +--- Storage Region  (spark.memory.storageFraction = 0.5 of unified)
|         +--- Execution Region (can borrow from Storage)
+--- User Memory (0.4 of remaining)
```

Storage can borrow from Execution (and vice versa), but only cached data in the
Storage region will be evicted (not execution memory). Once the Storage region
is full and Execution borrows it, old cache blocks are evicted.

### Code: Demonstrating Cache and Storage Levels

Source file: `quickstart/jobs/advanced_spark_sql/06_executor_memory_caching.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder.appName("cache_demo")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

df = (
    spark.range(0, 50_000)
    .withColumn("category",    (F.col("id") % 10).cast("string"))
    .withColumn("amount",       F.round(F.rand(seed=1) * 1000, 2))
    .withColumn("description",  F.concat(F.lit("item_"), F.col("id").cast("string")))
)

# MEMORY_ONLY (default .cache())
df_mem = df.persist(StorageLevel.MEMORY_ONLY)
df_mem.count()   # trigger cache population
df_mem.groupBy("category").sum("amount").show()
df_mem.unpersist()

# MEMORY_AND_DISK (safe fallback to disk)
df_disk = df.persist(StorageLevel.MEMORY_AND_DISK)
df_disk.count()
df_disk.groupBy("category").sum("amount").show()
df_disk.unpersist()
```

### Storage Level Comparison

| Level | Stored As | Memory Use | Disk Fallback | CPU Cost |
|---|---|---|---|---|
| `MEMORY_ONLY` | JVM objects | High | No (evict + recompute) | Low |
| `MEMORY_AND_DISK` | JVM objects | High | Yes (spill to disk) | Low |
| `MEMORY_ONLY_SER` | Serialized bytes | Lower | No (evict + recompute) | Medium |
| `MEMORY_AND_DISK_SER` | Serialized bytes | Lower | Yes | Medium |
| `DISK_ONLY` | Serialized bytes | None | Always | High (I/O) |

### What to Inspect

After calling `.count()` to materialize the cache, open the **Spark UI Storage
tab** and verify:

- **Fraction Cached**: should be 100% if data fits.
- **Size in Memory**: compare against your executor memory.
- If Fraction Cached < 100%: consider `MEMORY_AND_DISK` or reduce what you cache.

### What Improved and Why

- **`MEMORY_AND_DISK` over `MEMORY_ONLY`** when data size is uncertain.
  A disk spill is slower than a memory read but far faster than recomputing
  an upstream lineage chain.
- **Select before caching**: only cache the columns you actually reuse.
  `df.select("id", "amount").persist()` can reduce memory footprint by 50-80%
  compared to caching a wide DataFrame.
- **Always call `unpersist()`** when a cached DataFrame is no longer needed.
  Spark does not automatically free cache when a variable goes out of scope in
  Python.
- **Never cache a DataFrame that is used only once.** Cache overhead (memory
  consumption + eviction risk) is only justified when the DataFrame is
  accessed two or more times in the job.

---

## 7. Challenge: Too Many Small Output Files

### Problem Explanation

Spark writes exactly **one file per task** (one file per output partition).
If a DataFrame has 200 partitions (the default), the output directory contains
~200 files. Hundreds or thousands of small files cause:

- Slow reads in subsequent jobs (file listing overhead, metadata pressure on
  HDFS NameNode or S3).
- High driver-side plan overhead when reading many small files.
- Wasted storage (file system block padding per file).

### Why It Happens

`spark.sql.shuffle.partitions` defaults to `200`, optimized for computation
throughput. After the last shuffle, the partition count remains at 200. Unless
explicitly reduced before writing, every partition becomes a separate output file.

### Code: Reproducing the Issue

Source file: `quickstart/jobs/advanced_spark_sql/07_too_many_small_files.py`

```python
import os, tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("small_files_demo")
    .config("spark.sql.shuffle.partitions", "40")
    .getOrCreate()
)

df_raw = spark.range(0, 10_000).withColumn("category", (F.col("id") % 20).cast("string"))
df_shuffled = df_raw.groupBy("category").agg(F.count("id").alias("cnt"))

print(f"Partition count after shuffle: {df_shuffled.rdd.getNumPartitions()}")
# Output: Partition count after shuffle: 40

with tempfile.TemporaryDirectory() as base:
    df_shuffled.write.mode("overwrite").parquet(base + "/baseline")
    files = [f for f in os.listdir(base + "/baseline") if not f.startswith("_")]
    print(f"Baseline write -> {len(files)} files")   # 40 files
```

### Optimized Version A: coalesce

```python
df_shuffled.coalesce(4).write.mode("overwrite").parquet(out_path)
```

### Inspecting with explain("formatted")

```python
df_shuffled.coalesce(4).explain("formatted")
```

**What to look for:**

```
== Physical Plan ==
Coalesce 4                                  <-- no Exchange: free operation
+- HashAggregate
   +- Exchange hashpartitioning(category, 40)
      +- HashAggregate
         +- Scan ExistingRDD
```

`Coalesce` merges existing partitions on the same executor. **No new Exchange
node is added.**

### Optimized Version B: repartition

```python
df_shuffled.repartition(4).explain("formatted")
```

**What to look for:**

```
== Physical Plan ==
Exchange RoundRobinPartitioning(4)          <-- adds a shuffle stage
+- HashAggregate
   +- Exchange hashpartitioning(category, 40)
      ...
```

`repartition` adds an `Exchange RoundRobinPartitioning(4)` node -- a second
shuffle. Uses more resources but produces **balanced output partitions**.

### coalesce vs repartition Decision Guide

| Situation | Use |
|---|---|
| Reducing file count before write | `coalesce(N)` |
| Need balanced file sizes | `repartition(N)` |
| Changing partition key (e.g., for partitioned table write) | `repartition(N, col)` |
| Increasing partition count | `repartition(N)` only (coalesce cannot increase) |
| Writing a partitioned table (partitionBy) | `repartition(N, partitionCol)` |

### What Improved and Why

- `coalesce(4)` reduced 40 files to 4 files with **zero additional shuffle
  overhead**. It simply narrows the upstream task graph so multiple partitions
  are written by the same task.
- When writing a **Hive-style partitioned dataset** with `.partitionBy("col")`,
  combine with `repartition(N, col)` first. Without it, every task writes to
  every partition directory, creating `#tasks x #partitionValues` small files.
- In production, the target file size for efficient reads is typically
  **128 MB -- 256 MB** per file (matching HDFS block size or S3 multipart
  thresholds). Compute: `target_files = total_data_size_MB / 128`.

---

## Final Optimization Checklist

Use this checklist when diagnosing a slow Spark job:

### 1. Read the Query Plan First

- Run `df.explain("formatted")` before running any action.
- Count the number of `Exchange` nodes. Each one is a shuffle.
- Check for `Sort` nodes outside of window functions -- they indicate a
  heavyweight sort-merge join or explicit sort.
- Look for `BroadcastExchange` -- confirms a broadcast join is active.

### 2. Check for Unnecessary Shuffles

- Is `groupBy` / `join` / `window` on a column that is already partitioned
  the same way? Use `repartition(N, col)` first to eliminate repeated shuffles.
- Is `spark.sql.shuffle.partitions` set appropriately?
  Rule of thumb: `total_data_bytes / target_partition_size_bytes` (aim for
  128 MB -- 256 MB per partition).

### 3. Detect and Address Skew

- Run `df.groupBy(joinKey).count().orderBy(desc("count"))`.
- If one key holds > 30% of total rows, apply salting or enable AQE.
- AQE is on by default since Spark 3.2. Verify with
  `spark.conf.get("spark.sql.adaptive.enabled")`.

### 4. Join Strategy

- Is one side small (< 100 MB)? Use `F.broadcast(small_df)`.
- Never rely solely on `autoBroadcastJoinThreshold` for programmatic DataFrames.
  Statistics are not automatically computed.
- Sort-merge join is safe for two large sides; prefer it over shuffle-hash
  join for very large datasets.

### 5. Caching

- Cache only DataFrames used **more than once** in the same job.
- Select to the minimum required columns before caching.
- Use `MEMORY_AND_DISK` when data size versus available memory is unclear.
- Always call `unpersist()` when done.

### 6. Output File Count

- Before writing, always check `df.rdd.getNumPartitions()`.
- Apply `coalesce(N)` to reduce file count without the cost of an extra shuffle.
- Apply `repartition(N, col)` when writing a partitioned table.

### 7. Adaptive Query Execution (AQE)

- Enable: `spark.sql.adaptive.enabled=true` (default since Spark 3.2).
- AQE handles: coalescing small shuffle partitions, switching join strategies
  at runtime, and splitting skewed partitions.
- It cannot fix issues that are **plan-static** (e.g., a broadcast that you
  explicitly disabled, or a poor pre-shuffle filter you forgot to add).

### 8. Pushdown and Predicate Pruning

- Filter early in the pipeline -- `filter()` before `join()`, `groupBy()`,
  and `window()`.
- When reading from Parquet or ORC, verify that `explain()` shows `PushedFilters`
  in the scan node -- this means the filter is being applied at the file reader
  level, not after loading all data into memory.

### 9. General Rules

- Avoid `collect()` on large DataFrames -- it moves all data to the driver.
- Avoid `count()` in a loop -- each call is a separate Spark job.
- Avoid UDFs where a built-in Spark SQL function exists; UDFs disable Catalyst
  optimizations and vectorized execution.
- Use `spark.sql.functions` built-ins instead of Python `map` on RDDs for
  DataFrame operations -- they are Catalyst-aware and JVM-side accelerated.
