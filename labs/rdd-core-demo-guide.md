# Spark Core — RDD & Distributed Execution  
## Demo Guide

> **Spark version:** 4.1.1  
> **Cluster:** standalone, 1 master + 2 workers (2 cores / 2 GB each)  
> **All examples are in:** `quickstart/jobs/rdd_core/`  
> **Sample data file:** `quickstart/jobs/rdd_core/data/words.txt`

---

### How to run an example

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/rdd_core/<filename>.py
```

Replace `<filename>` with the file listed under each topic.

---

## Topic 01 — RDD Partitions Overview

### Explanation
An RDD (Resilient Distributed Dataset) is Spark's core data structure — a
collection split into **partitions** that live across worker nodes.  
Each partition becomes **one task**; more partitions mean more tasks that
can run in parallel.  
You control the number of partitions when calling `parallelize()` via the
`numSlices` argument.  
Too few partitions → workers sit idle. Too many → too much scheduling overhead.  
The sweet spot is usually **2–4× the number of available CPU cores**.

### Code file
`quickstart/jobs/rdd_core/01_rdd_partitions_overview.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("01_RDD_Partitions_Overview") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

data = list(range(1, 21))  # [1, 2, ..., 20]

default_rdd = sc.parallelize(data)
print(f"Default partitions : {default_rdd.getNumPartitions()}")

rdd_4 = sc.parallelize(data, 4)
print(f"Custom 4 partitions: {rdd_4.getNumPartitions()}")

rdd_2 = sc.parallelize(data, 2)
print(f"Custom 2 partitions: {rdd_2.getNumPartitions()}")

print("\nPartition contents (4-partition RDD):")
for i, partition in enumerate(rdd_4.glom().collect()):
    print(f"  Partition {i}: {partition}")

print(f"\nEach partition becomes 1 task. 4 partitions → 4 tasks run in parallel.")
spark.stop()
```

### Expected Output
```
Default partitions : 4
Custom 4 partitions: 4
Custom 2 partitions: 2

Partition contents (4-partition RDD):
  Partition 0: [1, 2, 3, 4, 5]
  Partition 1: [6, 7, 8, 9, 10]
  Partition 2: [11, 12, 13, 14, 15]
  Partition 3: [16, 17, 18, 19, 20]

Each partition becomes 1 task. 4 partitions → 4 tasks run in parallel.
```

### Concept demonstrated
`parallelize()` with `numSlices`, `getNumPartitions()`, `glom()`.

### Why this matters in real-world distributed systems
Partition count directly controls **parallelism**. In production you tune it
to match cluster cores so that every CPU is busy without wasting resources on
scheduling millions of tiny tasks (e.g., Kafka consumers, file ingestion jobs).

---

## Topic 02 — Logical vs Physical Partitioning

### Explanation
`parallelize()` creates an RDD entirely in memory — you choose the partition
count (**logical** split, you are in control).  
`textFile()` reads a file from the filesystem/HDFS and splits it based on
underlying file blocks (**physical** split, Spark decides).  
For HDFS the default block size is 128 MB, so a 1 GB file → ~8 partitions.  
For small local files you often get 1–2 partitions unless you set `minPartitions`.  
Understanding this difference is essential when tuning data ingestion jobs.

### Code file
`quickstart/jobs/rdd_core/02_logical_vs_physical_partitioning.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("02_Logical_vs_Physical_Partitioning") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Logical — you decide
mem_rdd = sc.parallelize(range(100), numSlices=6)
print(f"parallelize() with 6 slices → partitions: {mem_rdd.getNumPartitions()}")

# Physical — Spark decides from file blocks
file_rdd = sc.textFile("/opt/spark/jobs/rdd_core/data/words.txt")
print(f"textFile() default         → partitions: {file_rdd.getNumPartitions()}")

# Hint a minimum
file_rdd_min = sc.textFile("/opt/spark/jobs/rdd_core/data/words.txt", minPartitions=4)
print(f"textFile() minPartitions=4 → partitions: {file_rdd_min.getNumPartitions()}")
spark.stop()
```

### Expected Output
```
parallelize() with 6 slices → partitions: 6
textFile() default           → partitions: 2
textFile() minPartitions=4   → partitions: 4
```

### Concept demonstrated
`parallelize(numSlices)`, `textFile(minPartitions)`, `getNumPartitions()`.

### Why this matters in real-world distributed systems
When reading from S3/HDFS in production you rarely get the partition count you
want by default. Knowing how to set `minPartitions` (or later `repartition()`)
lets you ensure enough parallelism for large file ingestion pipelines.

---

## Topic 03 — Partitioning of File-based RDDs

### Explanation
When Spark reads a text file it creates one partition per logical file split.
For HDFS each split corresponds to one HDFS block (default 128 MB).
For local files on a single machine the granularity is the file itself — you
often get 1–2 partitions regardless of file size.
`glom()` is a debug-friendly transformation that groups each partition's
elements into a Python list so you can inspect the split.
`getNumPartitions()` is the quick one-liner check.

### Code file
`quickstart/jobs/rdd_core/03_file_based_rdd_partitioning.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("03_File_Based_RDD_Partitioning") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

file_path = "/opt/spark/jobs/rdd_core/data/words.txt"
rdd = sc.textFile(file_path)
print(f"File: {file_path}")
print(f"Number of partitions: {rdd.getNumPartitions()}")
print(f"Total lines        : {rdd.count()}")

print("\nLines per partition:")
for i, lines in enumerate(rdd.glom().collect()):
    print(f"  Partition {i}: {len(lines)} lines")

rdd_multi = sc.textFile(file_path, minPartitions=3)
print(f"\nWith minPartitions=3 → actual partitions: {rdd_multi.getNumPartitions()}")
for i, lines in enumerate(rdd_multi.glom().collect()):
    print(f"  Partition {i}: {len(lines)} lines")
spark.stop()
```

### Expected Output
```
File: /opt/spark/jobs/rdd_core/data/words.txt
Number of partitions: 2
Total lines        : 15

Lines per partition:
  Partition 0: 8 lines
  Partition 1: 7 lines

With minPartitions=3 → actual partitions: 3
Lines per partition (minPartitions=3):
  Partition 0: 5 lines
  Partition 1: 5 lines
  Partition 2: 5 lines
```

### Concept demonstrated
File-based RDD creation, `glom()`, `getNumPartitions()`, `minPartitions`.

### Why this matters in real-world distributed systems
In production, knowing how file partitions map to tasks helps you right-size
your cluster. A 10 TB dataset with 128 MB blocks → ~80,000 tasks. Too many
small files → partition explosion → scheduling overhead (the "small files problem").

---

## Topic 04 — HDFS and Data Locality (conceptual simulation)

### Explanation
**Data locality** means Spark tries to run a task on the same node that holds
the data it needs, avoiding expensive network transfers.  
Locality levels from best to worst: `PROCESS_LOCAL` → `NODE_LOCAL` → `RACK_LOCAL` → `ANY`.  
`getPreferredLocations()` returns the list of nodes that hold a partition's data.  
In standalone/local mode this list is empty because there is no HDFS block
metadata — Spark falls back to `ANY` and sends data over the network.  
On a real HDFS cluster this list contains DataNode hostnames, and the Spark
scheduler delays a task briefly to achieve a better locality level.

### Code file
`quickstart/jobs/rdd_core/04_hdfs_data_locality.py`

### Expected Output
```
Number of partitions: 3

  Partition 0 preferred locations: (none — local/standalone mode)
  Partition 1 preferred locations: (none — local/standalone mode)
  Partition 2 preferred locations: (none — local/standalone mode)

Data Locality levels Spark uses (best → worst):
  PROCESS_LOCAL  : data in same JVM (cached RDD)
  NODE_LOCAL     : data on same node, different process/disk
  RACK_LOCAL     : data on same rack, different node
  ANY            : data anywhere in the cluster

Spark waits a short time for a better locality before falling back.
This minimises network I/O and speeds up computation.
```

### Concept demonstrated
`getPreferredLocations()`, data locality tiers, scheduler locality awareness.

### Why this matters in real-world distributed systems
Data locality is the #1 reason why co-locating compute and storage (Hadoop-style,
or HDFS on the same nodes as Spark workers) dramatically reduces job time.
When using cloud object storage (S3/GCS) there is no locality → network becomes
the bottleneck, which is why tools like Delta Lake's file-skipping matter.

---

## Topic 05 — Transformations vs Actions

### Explanation
**Transformations** (e.g., `map`, `filter`, `flatMap`) are **lazy** — they
return a new RDD that represents a computation plan, but no work is done.  
**Actions** (e.g., `collect`, `count`, `reduce`, `saveAsTextFile`) **trigger**
execution — Spark compiles the DAG and sends tasks to workers.  
This lazy model lets Spark optimise the entire pipeline before running it.  
Chaining transformations is cheap; each action is a new job submission.  
Rule of thumb: minimise the number of actions in a loop — each one starts a job.

### Code file
`quickstart/jobs/rdd_core/05_transformations_vs_actions.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("05_Transformations_vs_Actions") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

doubled = numbers.map(lambda x: x * 2)           # lazy
evens   = doubled.filter(lambda x: x % 4 == 0)   # lazy

print("Transformations registered (no computation yet).")

result = evens.collect()   # ACTION — triggers everything
print(f"\nResult of collect() (action): {result}")

total  = evens.reduce(lambda a, b: a + b)
print(f"Sum via reduce()    (action): {total}")

count  = evens.count()
print(f"Count via count()   (action): {count}")
spark.stop()
```

### Expected Output
```
Transformations registered (no computation yet).

Result of collect() (action): [4, 8, 12, 16, 20]
Sum via reduce()    (action): 60
Count via count()   (action): 5
```

### Concept demonstrated
Lazy evaluation, transformation chaining, action-triggered execution.

### Why this matters in real-world distributed systems
Lazy evaluation lets Spark apply **pipelining** — combining multiple
transformations into a single pass over the data without materialising
intermediate results. This is the key to Spark outperforming MapReduce, which
writes intermediate results to disk after every step.

---

## Topic 06 — Stages, Tasks, and RDD Lineage

### Explanation
Spark compiles the transformation DAG into **stages** separated by shuffle
boundaries.  
A **shuffle** (e.g., `reduceByKey`, `groupByKey`, `join`) requires data to be
redistributed across the network — it marks the start of a new stage.  
Within a stage, transformations are **pipelined** into a single task per partition.  
`toDebugString()` prints the lineage so you can see exactly where stages begin.  
`ShuffledRDD` in the output = a shuffle boundary = a new stage.

### Code file
`quickstart/jobs/rdd_core/06_stages_tasks_lineage.py`

### Expected Output
```
=======================================================
RDD Lineage (toDebugString):
=======================================================
(2) PythonRDD[5] at collect at 06_stages_tasks_lineage.py:35 []
 |  MapPartitionsRDD[4] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[3] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[2] at reduceByKey at 06_stages_tasks_lineage.py:30 []
    |  PythonRDD[1] at RDD at PythonRDD.scala:53 []
    |  ParallelCollectionRDD[0] at readRDDFromFile at ...

Word counts:
  easy        : 1
  fast        : 1
  is          : 3
  powerful    : 1
  rdd         : 2
  spark       : 4
```

### Concept demonstrated
`toDebugString()`, shuffle boundary, stage splitting, `reduceByKey`.

### Why this matters in real-world distributed systems
Understanding stages is critical for **optimization**. Wide transformations
(shuffles) are expensive because they involve disk I/O + network. Grouping
narrow transformations together and minimising shuffles is the single biggest
lever for reducing Spark job time.

---

## Topic 07 — Lazy Evaluation

### Explanation
Transformations build a **logical plan (DAG)** — they do not execute.
Only when an **action** is called does Spark compile the DAG into physical
stages and submit tasks to workers.  
You can prove this with side-effect `print` statements inside `map()`:
the prints appear on worker logs **only after** an action is called.  
In cluster mode, worker-side prints go to executor stderr/stdout (visible in
Spark UI or container logs), not the driver stdout.  
This is normal — it demonstrates that lazy evaluation defers ALL work.

### Code file
`quickstart/jobs/rdd_core/07_lazy_evaluation.py`

### Expected Output (driver stdout)
```
[DRIVER] About to register map() transformation...
[DRIVER] map() registered. No worker activity yet.

[DRIVER] About to register filter() transformation...
[DRIVER] filter() registered. Still no worker activity.

[DRIVER] Calling collect() — THIS triggers execution...

[DRIVER] collect() complete. Result: [60, 80]

Observation: [WORKER] prints appeared ONLY after collect() was called.
That proves transformations are lazy — they build a plan, not results.
```

> **Note:** `[WORKER]` lines print to worker executor stderr (visible in
> Spark UI → executor stdout/stderr), not the driver terminal.
> The driver output proves the ordering: transformations register first,
> execution happens only at `collect()`.

### Concept demonstrated
Lazy DAG construction, action-triggered execution, driver vs worker output.

### Why this matters in real-world distributed systems
Lazy evaluation enables **query optimisation** (Catalyst in DataFrames,
RDD pipelining in Core). It also means you can chain hundreds of
transformations with zero overhead until you actually need a result.

---

## Topic 08 — Fault Tolerance and Lineage

### Explanation
Spark does **not** store all intermediate RDD data by default.
Instead it records the **lineage** — the recipe of transformations used to
create each RDD from its source.  
If a partition is lost (worker crash), Spark **replays only the lineage
steps for the missing partition** from the nearest available ancestor.  
This makes Spark fault-tolerant **without replication overhead** (unlike
Hadoop which replicates blocks 3×).  
`toDebugString()` shows you the lineage that Spark would use to rebuild lost data.

### Code file
`quickstart/jobs/rdd_core/08_fault_tolerance_lineage.py`

### Expected Output
```
First collect()  : [6, 8, 10, 12]

Lineage (how Spark would recompute a lost partition):
(3) PythonRDD[1] at collect at 08_fault_tolerance_lineage.py:31 []
 |  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:...

Second collect() (recomputed from lineage): [6, 8, 10, 12]
Results match: True

Key insight: Spark NEVER needs to checkpoint every step.
It stores only the lineage recipe. Lost data → recompute from source.
```

### Concept demonstrated
RDD lineage, fault tolerance through recomputation, `toDebugString()`.

### Why this matters in real-world distributed systems
Lineage-based fault tolerance means Spark workers can fail at any time and
jobs still complete — only the lost partitions are recomputed. For long
pipelines with wide transformations, `checkpoint()` can cut the lineage to
avoid exponentially long recomputation chains.

---

## Topic 09 — RDD Persistence (cache)

### Explanation
By default Spark **recomputes** an RDD from scratch every time you trigger an
action on it. If you use the same RDD multiple times, this is wasteful.  
`cache()` (short for `persist(MEMORY_ONLY)`) tells Spark to store the RDD
in memory after the first computation.  
Subsequent actions on that RDD are served **from the in-memory copy** — much faster.  
The benefit shows clearly when the same count is run twice: the second run
after caching is 3–5× faster.  
Always call `unpersist()` when done to free memory.

### Code file
`quickstart/jobs/rdd_core/09_rdd_persistence.py`

### Expected Output
```
Without cache — 1st count(): 333333  | time: 4.38s
Without cache — 2nd count(): 333333  | time: 1.34s

With    cache — 1st count(): 333333  | time: 2.42s  (compute + cache)
With    cache — 2nd count(): 333333  | time: 0.62s  (from cache)

Cache unpersisted.
```
*(Exact times vary by hardware)*

### Concept demonstrated
`cache()`, `unpersist()`, performance benefit of in-memory reuse.

### Why this matters in real-world distributed systems
Machine-learning training loops (e.g., gradient descent) iterate over the
same training RDD dozens or hundreds of times. Without caching, each iteration
reloads and recomputes the entire dataset — orders of magnitude slower than
one-time cache.

---

## Topic 10 — Persistence Levels

### Explanation
`cache()` is just syntax sugar for `persist(StorageLevel.MEMORY_ONLY)`.
Spark offers multiple storage levels for different memory/disk/speed trade-offs.  
`MEMORY_ONLY` — fastest but evicts partitions if RAM is full (they are recomputed on demand).  
`MEMORY_AND_DISK` — spills overflow partitions to disk rather than discarding; safer for large datasets.  
`DISK_ONLY` — always on disk; slowest but uses no heap memory.  
`_SER` variants serialise data before storing — less memory, more CPU.  
`_2` variants replicate to 2 nodes — useful for fault tolerance without recompute.

### Code file
`quickstart/jobs/rdd_core/10_persistence_levels.py`

### Expected Output
```
MEMORY_ONLY:
  Fill  : 4.29s   | Reuse: 0.37s
  Stored in JVM heap memory only. If not enough RAM → recomputed.

MEMORY_AND_DISK:
  Fill  : 1.02s   | Reuse: 0.55s
  Overflow partitions written to disk. Slower than MEMORY_ONLY but safe.

Storage Level Summary:
  Level                       Memory   Disk   Serialized   Replicas
  ---------------------------------------------------------------
  MEMORY_ONLY                   True  False        False          1
  MEMORY_ONLY_SER               True  False         True          1
  MEMORY_AND_DISK               True   True        False          1
  MEMORY_AND_DISK_SER           True   True         True          1
  DISK_ONLY                    False   True         True          1
  MEMORY_ONLY_2                 True  False        False          2
```

### Concept demonstrated
`StorageLevel`, `persist()`, memory vs disk trade-offs.

### Why this matters in real-world distributed systems
Wrong persistence levels are a common cause of OOM (out-of-memory) errors in
production. On cloud nodes with limited RAM (e.g., 8 GB workers), 
`MEMORY_AND_DISK` is often safer than `MEMORY_ONLY` for medium-sized datasets
that don't quite fit in memory.

---

## Topic 11 — Broadcast Variables

### Explanation
When you reference a Python variable inside a `map()`, Spark serialises it
and ships it with **every task** — even if there are 10,000 tasks, the
variable is sent 10,000 times.  
`broadcast()` sends the variable **once per worker node** and caches it
locally, dramatically reducing network traffic for lookup tables / configs.  
Use broadcast for any read-only dataset that is small enough to fit in
executor memory (typically < a few hundred MB).  
Access the broadcast value inside tasks with `.value`.  
Free the broadcast with `.destroy()` when done.

### Code file
`quickstart/jobs/rdd_core/11_broadcast_variables.py`

### Expected Output
```
Enriched user records:
   user_id   code country     
  ----------------------------
       101      1 USA         
       102     91 India       
       103     44 UK          
       104     81 Japan       
       105     49 Germany     
       106     91 India       

Broadcast value (driver access): {1: 'USA', 44: 'UK', 91: 'India', 81: 'Japan', 49: 'Germany'}
Broadcast variable destroyed.
```

### Concept demonstrated
`sc.broadcast()`, `.value`, `.destroy()`, task-level read-only shared state.

### Why this matters in real-world distributed systems
Enriching a large event stream with a small lookup table (product catalog,
user country mapping, ML model weights) is a classic broadcast pattern.
Without broadcast, shipping a 50 MB dictionary with every task in a
100-partition job means 5 GB of redundant network traffic.

---

## Topic 12 — Accumulators

### Explanation
Accumulators are **write-only** from worker tasks and **read-only** on the
driver after an action completes.  
They are Spark's safe way to count events, sum values, or track errors across
distributed tasks — without returning per-element results.  
Workers call `.add(value)`; the driver reads `.value`.  
**Critical limitation:** if a task is retried (worker failure), the accumulator
may be incremented twice for that task. Use them for diagnostics, not
business-critical aggregations.

### Code file
`quickstart/jobs/rdd_core/12_accumulators.py`

### Expected Output
```
Squared values     : [100, 9, 49, 1, 0, 25, 64, 4]
Total records seen : 8
Negative numbers   : 3

Accumulator rules:
  1. Workers can only ADD to an accumulator (write-only from tasks).
  2. Driver can only READ the accumulator (read-only on driver).
  3. Retried tasks can double-count — use for debugging, NOT business logic.
```

### Concept demonstrated
`sc.accumulator()`, `.add()`, `.value`, shared mutable state across tasks.

### Why this matters in real-world distributed systems
Accumulators are used in production to count: bad records, filtered rows,
network errors, schema violations — anything you want to monitor in a job
without adding a separate aggregation action.

---

## Topic 13 — Custom Partitioner

### Explanation
By default `partitionBy()` uses Spark's built-in `HashPartitioner`.
You can replace it with any Python function `key → partition_index` to
control exactly which partition each key lands in.  
Co-locating related keys in the same partition **eliminates shuffles** for
downstream joins or `reduceByKey` on the same partitioned RDD.  
The custom function must return an integer in `[0, numPartitions)`.  
`partitionBy()` requires a **pair RDD** (key-value tuples).

### Code file
`quickstart/jobs/rdd_core/13_custom_partitioner.py`

### Expected Output
```
Number of partitions: 3

Partition distribution:
  Partition 0 (electronics): [('electronics', 'Laptop'), ('electronics', 'Phone'), ('electronics', 'Tablet')]
  Partition 1 (clothing)   : [('clothing', 'Jacket'), ('clothing', 'Shoes')]
  Partition 2 (others)     : [('food', 'Apple'), ('food', 'Bread')]

Partitioner set: True
```

### Concept demonstrated
`partitionBy()`, custom partitioner function, `glom()`, co-location.

### Why this matters in real-world distributed systems
In a **join** between two large datasets, if both are pre-partitioned by
the same key using the same partitioner, Spark can do a **map-side join**
(no shuffle). This is the biggest single optimisation for iterative
aggregation pipelines (e.g., sessionisation, graph algorithms).

---

## Topic 14 — Caching vs Persistence for Performance

### Explanation
This example benchmarks all common persistence strategies side-by-side.
The **1st count** includes computation time (+ cache fill time for cached
strategies). The **2nd count** shows the benefit of reuse.  
`MEMORY_ONLY` has the fastest reuse but uses the most JVM heap.  
`MEMORY_AND_DISK` is slightly slower on reuse (some disk I/O) but safer.  
`DISK_ONLY` reuse speed is similar to `MEMORY_AND_DISK` since the cached
data is already serialised — only disk read overhead.  
No caching always recomputes from scratch — worst for repeated access.

### Code file
`quickstart/jobs/rdd_core/14_caching_vs_persistence_performance.py`

### Expected Output
```
======================================================================
  Strategy                     |    1st count  |    2nd count
======================================================================
  No caching (recompute)       | 1st count: 3.40s  | 2nd count: 0.76s
  cache() MEMORY_ONLY          | 1st count: 1.55s  | 2nd count: 0.47s
  persist MEMORY_AND_DISK      | 1st count: 1.05s  | 2nd count: 0.45s
  persist DISK_ONLY            | 1st count: 0.85s  | 2nd count: 0.47s
======================================================================

Performance ranking (fastest 2nd count → slowest):
  cache() MEMORY_ONLY  >  MEMORY_AND_DISK  >  DISK_ONLY  >  No caching

Choose based on:
  MEMORY_ONLY      → dataset fits in RAM, accessed many times
  MEMORY_AND_DISK  → large dataset, tolerate some disk spill
  DISK_ONLY        → very large dataset, RAM is scarce
  No caching       → RDD used only once
```
*(Exact times vary by hardware and cluster load)*

### Concept demonstrated
`cache()`, `persist(StorageLevel.*)`, wall-clock timing comparison.

### Why this matters in real-world distributed systems
In ETL pipelines where a filtered/cleaned RDD is used by multiple downstream
branches (write to Parquet, write to Kafka, generate report), a single
`cache()` call before the branch point avoids recomputing the expensive
upstream pipeline once per branch — sometimes cutting total job time by 60–80%.

---

## Quick Reference

| File | Topic |
|------|-------|
| `01_rdd_partitions_overview.py` | RDD Partitions Overview |
| `02_logical_vs_physical_partitioning.py` | Logical vs Physical Partitioning |
| `03_file_based_rdd_partitioning.py` | Partitioning of File-based RDDs |
| `04_hdfs_data_locality.py` | HDFS and Data Locality |
| `05_transformations_vs_actions.py` | Transformations vs Actions |
| `06_stages_tasks_lineage.py` | Stages, Tasks, and RDD Lineage |
| `07_lazy_evaluation.py` | Lazy Evaluation |
| `08_fault_tolerance_lineage.py` | Fault Tolerance and Lineage |
| `09_rdd_persistence.py` | RDD Persistence (cache) |
| `10_persistence_levels.py` | Persistence Levels |
| `11_broadcast_variables.py` | Broadcast Variables |
| `12_accumulators.py` | Accumulators |
| `13_custom_partitioner.py` | Custom Partitioner |
| `14_caching_vs_persistence_performance.py` | Caching vs Persistence Performance |

---

## Run all examples in sequence

```bash
for i in $(seq -w 1 14); do
  file=$(ls /opt/spark/jobs/rdd_core/${i}_*.py 2>/dev/null | head -1)
  [ -z "$file" ] && continue
  echo "=============================="
  echo "Running: $file"
  echo "=============================="
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 "$file"
done
```

Run inside the master container:

```bash
docker exec -it spark-master bash
# then paste the loop above
```
