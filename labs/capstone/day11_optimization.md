# Day 11 – Query Optimization & Performance Engineering

## Goal
Take the enriched dataset built on Day 10 and systematically make it faster. Every task starts with a deliberately slow or unoptimized query, asks you to measure it, then asks you to apply a specific technique and measure again. By the end, you will have a mental model of *why* each optimization works — not just *how* to apply it.

**Starting point:** Read `enriched_orders.parquet` (partitioned by region) from the Day 10 output directory.

---

### Task 1 – Read and interpret a multi-stage execution plan

**What to do:**  
Load the enriched orders Parquet and run the monthly revenue trend query from Day 10, Task 3A again. Before calling `.show()`, call `.explain(mode="extended")`.

**Hints:**
- Look for `== Optimized Logical Plan ==` — find any `Filter` nodes. Did Catalyst push the date filter *before* the join, or after? Explain why that matters.
- In `== Physical Plan ==`, count the number of `Exchange` nodes. Each one is a network shuffle. How many did you find? Which operations caused them?
- Look for `BroadcastHashJoin` vs `SortMergeJoin`. Which tables were broadcast? Were they the same ones you hinted with `broadcast()` in Day 10?
- Run `.explain()` on the `monthly_with_delta` query (which includes a window function). Find the `Window` node in the physical plan. What partitioning does it use?

**Why:**  
Reading the plan is not optional when debugging slow jobs. Every optimization in this lab starts here: you cannot fix what you cannot see.

---

### Task 2 – Rewrite an inefficient subquery using CTE

**What to do:**  
The following *conceptual* query is slow: "For each order, label it as above or below the average `total_amount` for its region."

A naive implementation does a `groupBy` to get the regional average, then uses that result inside a filter or join back to the original DataFrame — effectively two scans.

**Hints:**
- Rewrite using a window function (`avg()` over a `Window.partitionBy("region")`) so Spark computes the average and the label in a single pass.
- Alternatively, express this as a Spark SQL query using a CTE (`WITH regional_avg AS (...)`) and compare the execution plans of both approaches using `.explain()`.
- Check: does the CTE version produce the same physical plan as the window version? What does this tell you about Catalyst's view of SQL vs DataFrame API?

**Expected observation:**  
The window function approach has **one** `Exchange` node (to partition by region). The naive join-back approach has **two** (one for the aggregation, one for the join). Record both counts in your notes.

**Why:**  
Subqueries that re-aggregate the same data are among the most common causes of unnecessary shuffles. Recognising this pattern and replacing it with a window function is a day-one optimization skill.

---

### Task 3 – Diagnose and mitigate data skew on East region

**What to do:**  
East carries ~35% of order volume (you proved this on Day 9). Run the regional revenue aggregation and open the Spark UI while it runs.

**Hints:**
- In the Spark UI → Stages tab → find the shuffle stage for the `groupBy("region")`. Look at the task duration timeline. One task should be visibly longer than the others — which region is it?
- Apply a **salting** strategy: add a random salt suffix to the `region` key (e.g., `East_0`, `East_1`, `East_2`), aggregate by the salted key, then perform a second aggregation on the original region (stripping the salt). This distributes East's work across multiple tasks.
- Use `spark.range(0, N)` and `pmod` (modulo) or `expr("concat(region, '_', cast(pmod(rand()*N, N) as int))")` to generate the salt column.
- After salting, check the Spark UI again — the task duration timeline should be more even.

**Expected partition sizes (before vs after salting, with 3 salts for East):**
```
# Before salting: groupBy("region") → 5 shuffle partitions via hash
# One task handles all East rows — 2834 records vs ~964 for Central

# After salting with 3 buckets:
# East_0: ~945  East_1: ~944  East_2: ~945
# North, South, West, Central each land in their own partition (unchanged)
# Maximum partition size drops from 2834 → ~945 — a 3× improvement
```

**Why:**  
Salting is the standard technique for handling hot keys. It trades one shuffle for two, but the second shuffle operates on pre-aggregated (much smaller) data. The net result is lower total task time because no single task is a bottleneck.

---

### Task 4 – Compare join strategies explicitly

**What to do:**  
You have been using `broadcast()` hints. Now deliberately remove the hint from the `products_slim` join and force a `SortMergeJoin` by setting the broadcast threshold below the table size.

**Hints:**
- Use `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")` to disable auto-broadcast for this experiment.
- Run `enriched_df.explain()` — confirm you now see `SortMergeJoin` instead of `BroadcastHashJoin`.
- Time the `.count()` action with a `SortMergeJoin`. Then restore the broadcast hint and time again.
- Re-enable the threshold with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")` (10 MB) after the experiment.

**What to record:**  
Two execution plan snippets (one with each join strategy) and both elapsed times. Even on the dev dataset you should observe a measurable difference because `SortMergeJoin` adds a sort + shuffle stage.

**Why:**  
The broadcast threshold is a cluster-tuning parameter. Knowing how to force and observe both strategies lets you confidently choose the right one — and prove to stakeholders that the choice matters.

---

### Task 5 – Enable Adaptive Query Execution (AQE) and observe its effect

**What to do:**  
Run the category revenue aggregation (`groupBy("region", "category")`) with AQE disabled, note the number of output partitions. Then enable AQE and run again.

**Hints:**
- Disable: `spark.conf.set("spark.sql.adaptive.enabled", "false")`
- Run the aggregation and call `.rdd.getNumPartitions()` on the result — note the count (it will be `spark.sql.shuffle.partitions`, default 200).
- Enable: `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- Run again and check partition count — AQE should coalesce the many near-empty shuffle partitions down to a much smaller number (likely single digits for a 10,000-row dataset).
- In Spark UI → SQL tab, look for the "AQE" label on the physical plan node that changed.

**Expected result:**
```
# AQE disabled:
agg_df.rdd.getNumPartitions()  →  200
# 200 tasks, most processing 0-5 rows each

# AQE enabled:
agg_df.rdd.getNumPartitions()  →  2 to 5  (exact count depends on Spark version and config)
# AQE coalesces near-empty shuffle partitions at runtime
# Spark UI → SQL shows AdaptiveSparkPlan node with "isFinalPlan = true"
```

**Why:**  
`spark.sql.shuffle.partitions = 200` is the default and is reasonable for large datasets. For a 10,000-row aggregation it produces 200 mostly-empty partitions — each one is a task with near-zero work. AQE detects this at runtime and collapses them. The lesson: AQE is not magic; it corrects the over-partitioning that occurs when default config meets small data.

---

### Task 6 – Repartition vs coalesce: when each is correct

**What to do:**  
Take `enriched_df` (loaded from the Parquet output). Experiment with three approaches to reduce partitions from 200 to 5.

**Hints:**
- `df.repartition(5)` — triggers a full shuffle; every row moves across the network.
- `df.coalesce(5)` — no shuffle; merges existing partitions in-place. Check: are the resulting 5 partitions balanced in size? Use `.rdd.glom().map(len).collect()` to inspect.
- `df.repartition(5, col("region"))` — hash-partitions by region; all East records go to one partition. How does this compare to `coalesce` in terms of partition balance?
- Run `.explain()` on each variant. Which produces an `Exchange` node? Which does not?

**Expected partition sizes for the dev dataset (8026 Completed rows, read from Parquet):**
```
df.repartition(5).rdd.glom().map(len).collect()
→  roughly even, e.g. [1606, 1604, 1605, 1606, 1605]  # full shuffle redistributes evenly

df.coalesce(5).rdd.glom().map(len).collect()
→  uneven, e.g. [1, 1, 1, 1, 8022]  # merges in-place; last partition absorbs remaining
# Note: very skewed because the Parquet for 5 region partitions may have 1 file each

df.repartition(5, col("region")).rdd.glom().map(len).collect()
→  [1691, 1248, 2834, 1289, 964]    # East partition has 3× Central's size (skew preserved)
```

**What `.explain()` shows:**
```
repartition(5)          →  contains "Exchange RoundRobinPartitioning(5)"
coalesce(5)             →  contains "Coalesce" node only, NO Exchange
repartition(5, col)     →  contains "Exchange hashpartitioning(region, 5)"
```

**Rule to derive from the experiment:**  
Use `coalesce` when reducing before a write (avoid the shuffle cost). Use `repartition(n, col)` when you need data re-distributed by a column for a downstream join or when partition sizes are severely uneven. Never use `coalesce` to go from 5 → 200 (it cannot increase partition count).

---

### Task 7 – Accumulator for pipeline quality metrics

**What to do:**  
Add an accumulator to the data cleaning step to count how many dirty `discount` records were corrected at runtime (without a separate `.filter().count()` action).

**Hints:**
- Create a `LongAccumulator` via `sc.accumulator(0)` (or `spark.sparkContext.longAccumulator("dirty_discount_count")`).
- In a `udf` or `rdd.map()` applied during the discount-cleaning step, increment this accumulator whenever a blank discount is replaced with `"0"`.
- After the cleaning action completes, print `accumulator.value`. It should match the dirty row count you found in Day 8, Task 2.
- Important: accumulators are unreliable inside transformations that may be re-executed (e.g., on task retry). Note this limitation in your writeup.

**Expected accumulator value:**
```
accumulator.value  →  41
# Matches the dirty_disc count found in Day 8, Task 2 (41 rows, 0.41%)
# If your value differs, check whether the accumulator was incremented
# inside a filter vs a map — filter is called during count() only; map may
# be re-evaluated if the stage is re-run due to a failed task.
```

**Why:**  
Accumulators let you count errors, skipped records, or anomalies during a single pipeline pass — no extra action, no extra shuffle. They are the Spark equivalent of a metrics counter in a streaming pipeline.

---

### Task 8 – Caching strategy: when to cache, when to skip

**What to do:**  
`enriched_df` is used by at least three downstream operations: regional summary, category ranking, and loyalty analysis. Cache it and measure the effect.

**Hints:**
- Without cache: run all three aggregations back-to-back and total the time. Each one re-reads and re-joins the Parquet files from scratch.
- With `enriched_df.cache()` (or `.persist(StorageLevel.MEMORY_AND_DISK)`): run all three aggregations and total the time.
- Use `spark.catalog.isCached("table_name")` or check the Spark UI → Storage tab to confirm the DataFrame is actually cached (caching is lazy — you must trigger an action first).
- After the experiment, call `.unpersist()` to release memory.

**Expected outcome:**  
The first run after `.cache()` is not faster (it also materialises the cache). The second and third downstream operations are noticeably faster because they skip re-reading Parquet and re-executing the joins.

**Typical timing pattern (dev dataset, local mode):**
```
Without cache:
  regional_summary.count()   : ~3–6 s  (reads Parquet + joins + aggregates)
  category_ranking.count()   : ~3–6 s  (same full lineage re-executed)
  loyalty_summary.count()    : ~3–6 s  (same full lineage re-executed)
  Total                      : ~9–18 s

With enriched_df.cache() (after materialisation on first action):
  regional_summary.count()   : ~3–6 s  (triggers materialisation into cache)
  category_ranking.count()   : ~0.5–1 s (reads from in-memory cache, no disk I/O)
  loyalty_summary.count()    : ~0.5–1 s (reads from in-memory cache)
  Total                      : ~4–8 s
```
(Absolute timings depend on hardware; the shape — second/third ops are 3–8× faster — is the lesson.)

**Rule:**  
Cache when the same lineage feeds **multiple downstream actions**. Do not cache a DataFrame that is only consumed once — it wastes memory and adds serialization overhead.

---

### Day 11 Deliverables

| Artifact | What it looks like |
|---|---|
| Execution plan annotation | Exchange node count for monthly trend query + BroadcastHashJoin vs SortMergeJoin screenshots |
| Skew mitigation output | Before/after task duration screenshots from Spark UI; salted partition sizes |
| CTE vs window function plan comparison | Two plan snippets; Exchange node count for each |
| AQE partition count | 200 (AQE off) vs reduced count (AQE on) |
| Repartition vs coalesce notes | Which triggers Exchange, partition balance comparison |
| Accumulator output | Dirty discount count matching Day 8 Task 2 baseline |
| Cache timing table | 3 aggregations: time without cache vs with cache (second/third runs) |
