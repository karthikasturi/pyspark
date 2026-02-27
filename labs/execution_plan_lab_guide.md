# Spark Execution Plans – Lab Guide

This guide walks through every section of `execution_plan_lab.py` and explains the
underlying Spark concepts. Use this alongside the running script and the Spark UI.

**Script location:** `quickstart/jobs/execution_plan/execution_plan_lab.py`

---

## How to Run

```bash
# Local mode
spark-submit quickstart/jobs/execution_plan/execution_plan_lab.py

# Cluster mode (docker)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/execution_plan/execution_plan_lab.py
```

---

## Section 1 – Spark Initialization

### What the code does

```python
spark = (
    SparkSession.builder
    .appName("ExecutionPlanLab")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
print(f"Spark UI : {sc.uiWebUrl}")
```

### Explanation

| Item | Purpose |
|---|---|
| `SparkSession.builder` | Entry point for all Spark DataFrame and SQL operations |
| `appName` | Sets the name shown in the Spark UI Jobs list |
| `spark.sql.shuffle.partitions` | Controls how many partitions are created **after a shuffle**. Default is 200. Setting it to 4 keeps the lab output small and easy to read in the UI |
| `uiWebUrl` | The address of the Spark web UI (usually `http://localhost:4040`) |

**Key point:** Open the Spark UI before the script finishes. Each `.show()` or `.count()`
call in the script corresponds to one entry in the **Jobs** tab.

---

## Section 2 – Create Sample Dataset

### What the code does

```python
raw_data = [
    (i, CATEGORIES[random.randint(0, 3)], random.randint(1, 100))
    for i in range(1, 100_001)
]
base_df = spark.createDataFrame(raw_data, ["id", "category", "value"])
base_df = base_df.repartition(4)
```

### Explanation

- **100,000 rows** are generated on the driver using a Python list comprehension.
  This is intentionally kept in memory (not read from disk) so the lab works in
  any environment without external data.

- The schema is simple: `id` (integer), `category` (string: A/B/C/D), `value` (integer 1–100).

- **`repartition(4)`** distributes the data across 4 partitions. This is a wide
  transformation that triggers a shuffle when an action is eventually called.
  At this point, no job runs — the repartition is recorded in the logical plan.

- **`getNumPartitions()`** reads partition metadata from the plan. It does **not**
  trigger a job.

**Spark UI check:** No job should appear after Section 2.

---

## Section 3 – Narrow Transformations

### What the code does

```python
filtered_df = base_df.filter(col("value") > 20)
transformed_df = filtered_df.withColumn("value_squared", col("value") * col("value"))
transformed_df.explain("formatted")
```

### What are narrow transformations?

A transformation is **narrow** when every output partition depends on exactly one
input partition. No data needs to move between executors.

```
Input Partition 1  -->  Output Partition 1   (filter applied locally)
Input Partition 2  -->  Output Partition 2   (filter applied locally)
Input Partition 3  -->  Output Partition 3   (filter applied locally)
Input Partition 4  -->  Output Partition 4   (filter applied locally)
```

Examples of narrow transformations: `filter()`, `map()`, `withColumn()`, `select()`, `union()`.

### What does `explain("formatted")` show?

`explain()` is a **driver-side call only**. It prints the query plan without sending
any task to an executor. No Spark job is triggered.

The output has two parts:

```
== Parsed Logical Plan ==        <- SQL-like tree, unresolved column names
== Analyzed Logical Plan ==      <- column names resolved against schema
== Optimized Logical Plan ==     <- Catalyst optimizer has reordered / pruned
== Physical Plan ==              <- what executors will actually run
```

For this section's plan, notice that there is **no Exchange node**. Exchange is Spark's
term for a shuffle operation. Without an Exchange, all work stays within partitions.

**Spark UI check:** No new job should appear after Section 3 runs.

---

## Section 4 – Wide Transformation: groupBy + orderBy

### What the code does

```python
agg_df = (
    transformed_df
    .groupBy("category")
    .agg(
        _sum("value").alias("total_value"),
        _avg("value").alias("avg_value"),
        count("id").alias("row_count"),
    )
    .orderBy(col("total_value").desc())
)
agg_df.explain("formatted")
agg_df.show(truncate=False)
```

### What are wide transformations?

A transformation is **wide** when an output partition may need rows from **multiple**
input partitions. This requires a **shuffle** — data moves across the network between
executors.

```
Input Partition 1  ---\
Input Partition 2  ----|-->  All "A" rows  -->  Output Partition for A
Input Partition 3  ----|-->  All "B" rows  -->  Output Partition for B
Input Partition 4  ---/                    ...
```

### How many stages does this create?

Each shuffle boundary creates a new stage. For `groupBy + orderBy` you should see:

```
Stage 1:  Read 4 partitions -> apply filter -> compute partial aggregates (map side)
          [4 tasks]

Stage 2:  Shuffle by category key -> finalize aggregation (reduce side)
          [spark.sql.shuffle.partitions = 4 tasks]

Stage 3:  Shuffle for global sort (orderBy needs all rows to determine rank)
          [spark.sql.shuffle.partitions = 4 tasks]
```

### Reading the physical plan

Look for these nodes in the plan output:

| Plan Node | What it means |
|---|---|
| `Exchange hashpartitioning` | Shuffle for groupBy — rows hashed by key and moved to the correct partition |
| `HashAggregate` | Aggregation in two phases: partial (before shuffle) and final (after shuffle) |
| `Exchange rangepartitioning` | Shuffle for orderBy — rows sorted globally across all partitions |
| `Sort` | Local sort within each partition after the range shuffle |

**Spark UI check:** Go to **Jobs** tab — a new job appears. Click it, then click
**Stages** to see the stage breakdown. Each stage shows task count and shuffle
read/write bytes.

---

## Section 5 – Join Demo

### What the code does

```python
category_df = spark.createDataFrame(
    [("A", "Segment Alpha"), ("B", "Segment Beta"), ...],
    ["category", "description"]
)
joined_df = agg_df.join(category_df, on="category", how="left")
joined_df.explain("formatted")
joined_df.show(truncate=False)
```

### Two join strategies Spark uses

**BroadcastHashJoin (no shuffle):**
- When one side of the join is small (below `spark.sql.autoBroadcastJoinThreshold`,
  default 10 MB), Spark copies the small table to every executor.
- The large table is never moved — each partition joins locally against the broadcasted copy.
- Result: 0 new shuffle stages.

```
Executor 1: partition 1 of agg_df + full copy of category_df  --> joined rows
Executor 2: partition 2 of agg_df + full copy of category_df  --> joined rows
...
```

**SortMergeJoin (shuffle on both sides):**
- Both tables are shuffled so rows with the same join key end up on the same executor.
- Each side is then sorted by key and merged.
- Result: 2 shuffle stages (one per side).

Since `category_df` has only 4 rows, Spark will almost certainly choose `BroadcastHashJoin`.
You can confirm this in the physical plan — look for the node name.

**Spark UI check:**
- Go to **SQL / DAG** tab. Find the query for this job.
- Hover over each node to see row counts, data size, and time spent.
- If BroadcastHashJoin was chosen, you will see a `BroadcastExchange` node feeding
  into the join — not a `ShuffleExchange`.

---

## Section 6 – Repartition Demo

### What the code does

```python
repartitioned_df = base_df.repartition(8)
repart_agg_df = repartitioned_df.groupBy("category").agg(count("id").alias("cnt"))
repart_agg_df.show(truncate=False)
```

### What repartition() does

`repartition(n)` triggers a **full shuffle** that redistributes all rows across
exactly `n` partitions using round-robin assignment (no key hashing).

This is different from `coalesce(n)` which reduces partitions using a narrow
merge (no shuffle) but cannot increase partition count.

### Effect on task count

Every partition in a stage becomes one task. After `repartition(8)`:

```
Stage 1 (read + repartition):  8 tasks   <-- one per new partition
Stage 2 (groupBy shuffle):     4 tasks   <-- spark.sql.shuffle.partitions = 4
```

This directly demonstrates the rule: **tasks = partitions**.

**Spark UI check:** Go to Stages tab for this job. The first stage will show
exactly 8 tasks. Compare to the 4 tasks you saw in Section 4's first stage.

---

## Section 7 – Summary

The script prints a text summary. This section reinforces the concepts:

### Stage boundaries — full list from this lab

| Transformation | Shuffle? | New Stage? | Reason |
|---|---|---|---|
| `filter()` | No | No | Narrow — each partition processed independently |
| `withColumn()` | No | No | Narrow — local column computation |
| `repartition(4)` | Yes | Yes | Redistributes data across partitions |
| `groupBy().agg()` | Yes | Yes | Rows with same key must be co-located |
| `orderBy()` | Yes | Yes | Global sort requires all rows in one pass |
| `repartition(8)` | Yes | Yes | Full redistribution |
| `join()` (broadcast) | No (small side) | No | Small table copied to executors |
| `join()` (sort-merge) | Yes (both sides) | Yes | Both sides shuffled by key |

### Why tasks = partitions

Each executor processes one task at a time. A task operates on exactly one partition.
Therefore:

```
More partitions  -->  more parallelism  -->  faster execution (up to available cores)
Too many partitions  -->  excessive task overhead  -->  slower execution
```

The default `spark.sql.shuffle.partitions = 200` works well for large clusters.
For small labs or small datasets, reduce it (as this lab does, setting it to 4)
to avoid creating 200 near-empty partitions after every shuffle.

---

## Spark UI Quick Reference

| Tab | What to look for |
|---|---|
| **Jobs** | One row per action (`.show()`, `.count()`, `.collect()`) |
| **Stages** | Stages within a job; separated by shuffle boundaries; task count per stage |
| **SQL / DAG** | Visual query graph; hover nodes for row counts and byte metrics |
| **Storage** | Cached / persisted DataFrames and their memory usage |
| **Environment** | Active Spark configuration values |
| **Executors** | Core count, memory usage, shuffle read/write bytes per executor |

---

## Common Questions

**Q: Why does explain() show a plan but no job appears in Spark UI?**

`explain()` is a driver-only call. It only analyzes and prints the query plan.
No tasks are sent to executors. Jobs only appear when an action is called
(`.show()`, `.count()`, `.collect()`, `.write`, etc.).

**Q: What is Catalyst?**

Catalyst is Spark's query optimization engine. It takes your logical plan
(what you asked for) and rewrites it into an optimized physical plan
(how Spark will actually execute it). Optimizations include predicate pushdown
(moving filters earlier in the plan) and column pruning (dropping unused columns
before a shuffle).

**Q: What is the difference between repartition() and coalesce()?**

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Always yes | No (merges locally) |
| Can increase partition count | Yes | No |
| Data distribution | Even (round-robin) | Can be uneven |
| Use case | Increase parallelism or evenly redistribute | Reduce partitions cheaply before writing |

**Q: When should I change spark.sql.shuffle.partitions?**

Set it to roughly 2–3x the number of available CPU cores for your cluster.
Too low means large partitions that can cause memory pressure. Too high means
many small tasks with excessive scheduling overhead.
