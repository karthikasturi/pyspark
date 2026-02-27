"""
execution_plan_lab.py

Live training lab: Spark Execution Plans, Stages, Shuffles, and DAG Visualization.

Run with:
    spark-submit execution_plan_lab.py
    spark-submit --master spark://spark-master:7077 execution_plan_lab.py
"""

import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count, pow as _pow


# ==============================================================================
# SECTION 1 – Spark Initialization
# ==============================================================================

print("")
print("=" * 70)
print("SECTION 1 – Spark Initialization")
print("=" * 70)

spark = (
    SparkSession.builder
    .appName("ExecutionPlanLab")
    .config("spark.sql.shuffle.partitions", "4")  # Keep shuffle partitions low for lab clarity
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"Spark version      : {spark.version}")
print(f"Application ID     : {sc.applicationId}")
print(f"Spark UI           : {sc.uiWebUrl}")
print("")
print(">>> ACTION: Open the Spark UI URL above in your browser before continuing.")
print("")


# ==============================================================================
# SECTION 2 – Create Sample Dataset
# ==============================================================================

print("=" * 70)
print("SECTION 2 – Create Sample Dataset")
print("=" * 70)

NUM_ROWS = 100_000
CATEGORIES = ["A", "B", "C", "D"]

# Generate data on the driver and parallelize into Spark
raw_data = [
    (i, CATEGORIES[random.randint(0, 3)], random.randint(1, 100))
    for i in range(1, NUM_ROWS + 1)
]

# Create DataFrame with explicit column names
columns = ["id", "category", "value"]
base_df = spark.createDataFrame(raw_data, columns)

# Repartition to 4 partitions for predictable parallelism
base_df = base_df.repartition(4)

print(f"Dataset rows       : {NUM_ROWS}")
print(f"Partitions         : {base_df.rdd.getNumPartitions()}")
print("")
print(">>> NOTE: No job ran yet. repartition() is a transformation, not an action.")
print("    The getNumPartitions() call returns metadata, no shuffle was triggered here.")
print("")


# ==============================================================================
# SECTION 3 – Narrow Transformations
# ==============================================================================

print("=" * 70)
print("SECTION 3 – Narrow Transformations (no action triggered)")
print("=" * 70)

# filter() and withColumn() are narrow transformations.
# Each output partition depends only on one input partition. No shuffle.
filtered_df = base_df.filter(col("value") > 20)

# Adds a derived column: value_squared
transformed_df = filtered_df.withColumn("value_squared", col("value") * col("value"))

print("")
print("--- Logical + Physical Plan for narrow transformation chain ---")
print("")

# explain() is a driver-side call. It prints the query plan but triggers NO job.
transformed_df.explain("formatted")

print("")
print(">>> CHECK: Look at the plan above.")
print("    - Single Exchange node should NOT appear (no shuffle).")
print("    - Spark UI job list should still show 0 new jobs.")
print("    - No stage was created yet because no action was called.")
print("")


# ==============================================================================
# SECTION 4 – Wide Transformation (GroupBy + OrderBy = Two Shuffles)
# ==============================================================================

print("=" * 70)
print("SECTION 4 – Wide Transformation: groupBy + orderBy")
print("=" * 70)

# groupBy triggers a shuffle: rows with the same category key must be co-located.
# orderBy triggers a second shuffle: all rows across all partitions must be sorted globally.
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

print("")
print("--- Logical + Physical Plan for groupBy + orderBy ---")
print("")

# Two Exchange nodes should appear in the plan: one for groupBy, one for orderBy.
agg_df.explain("formatted")

print("")
print(">>> ACTION: .show() is about to run. This will trigger a Spark job.")
print("    Watch Spark UI -> Jobs tab for a new job to appear.")
print("    Expect at least 2 stages separated by a shuffle boundary.")
print("")

agg_df.show(truncate=False)

print("")
print(">>> CHECK Spark UI – Stages tab:")
print("    - Stage 1: reads partitions, filters, computes partial aggregates (map side).")
print("    - Stage 2: shuffles by category, finalizes aggregation (reduce side).")
print("    - Stage 3 (if present): re-shuffles all rows for global sort (orderBy).")
print("    Task count per stage = number of partitions at that stage.")
print("")


# ==============================================================================
# SECTION 5 – Join Demo (Broadcast vs Sort-Merge Shuffle)
# ==============================================================================

print("=" * 70)
print("SECTION 5 – Join Demo")
print("=" * 70)

# Small reference DataFrame (4 rows) – Spark may choose broadcast join automatically.
category_descriptions = [
    ("A", "Segment Alpha"),
    ("B", "Segment Beta"),
    ("C", "Segment Gamma"),
    ("D", "Segment Delta"),
]
category_df = spark.createDataFrame(category_descriptions, ["category", "description"])

# Join aggregated results with the category description lookup table.
# Since category_df is tiny, Spark's CBO / auto-broadcast threshold may broadcast it,
# avoiding a shuffle on the larger side.
joined_df = agg_df.join(category_df, on="category", how="left")

print("")
print("--- Logical + Physical Plan for Join ---")
print("")

# Watch for BroadcastHashJoin vs SortMergeJoin in the plan.
joined_df.explain("formatted")

print("")
print(">>> ACTION: .show() is about to run. New job will appear in Spark UI.")
print("    Check SQL tab -> DAG for this query and look at the join strategy.")
print("")

joined_df.show(truncate=False)

print("")
print(">>> CHECK Spark UI – SQL / DAG tab:")
print("    - If plan shows BroadcastHashJoin: small table was broadcast, NO shuffle.")
print("    - If plan shows SortMergeJoin: both sides were sorted and shuffled.")
print("    - Hover over each node in the DAG to see rows, bytes, spill metrics.")
print("")


# ==============================================================================
# SECTION 6 – Repartition Demo (Effect on Task Parallelism)
# ==============================================================================

print("=" * 70)
print("SECTION 6 – Repartition Demo")
print("=" * 70)

# Repartition the base dataset to 8 partitions.
# This triggers a full shuffle that redistributes all rows round-robin across 8 partitions.
repartitioned_df = base_df.repartition(8)

print(f"Partitions after repartition(8) : {repartitioned_df.rdd.getNumPartitions()}")

# A simple groupBy on the repartitioned data.
# The shuffle here uses the new partition count upstream.
repart_agg_df = (
    repartitioned_df
    .groupBy("category")
    .agg(count("id").alias("cnt"))
)

print("")
print("--- Plan for groupBy on repartitioned DataFrame ---")
print("")

repart_agg_df.explain("formatted")

print("")
print(">>> ACTION: .show() is about to run.")
print("    Watch Spark UI -> Stages tab. The first stage should show 8 tasks,")
print("    one per partition created by repartition(8).")
print("")

repart_agg_df.show(truncate=False)

print("")
print(">>> CHECK Spark UI – Stages tab:")
print("    - First stage tasks = 8 (matches repartition count).")
print("    - Second stage tasks = spark.sql.shuffle.partitions (set to 4 in this lab).")
print("    This shows the direct relationship: tasks = number of partitions.")
print("")


# ==============================================================================
# SECTION 7 – Summary Output
# ==============================================================================

print("=" * 70)
print("SECTION 7 – Lab Summary")
print("=" * 70)
print("""
Stage Boundaries
----------------
A new stage is created every time data must cross partition boundaries, i.e.,
whenever a shuffle is required. In this lab:
  - groupBy("category")  -> shuffle: rows with the same key moved to the same partition.
  - orderBy()            -> shuffle: all rows sorted globally across all partitions.
  - repartition(8)       -> shuffle: full data redistribution by round-robin.
  - join()               -> shuffle for SortMergeJoin; NO shuffle for BroadcastHashJoin.

Narrow vs Wide Transformations
-------------------------------
  - filter(), withColumn()      -> narrow: each output partition reads exactly one input partition.
  - groupBy(), orderBy(), join() -> wide: output partitions may depend on ALL input partitions.

Tasks and Partitions
---------------------
  Number of tasks in a stage = number of partitions processed by that stage.
  Increasing partitions increases parallelism (up to available executor cores).
  spark.sql.shuffle.partitions controls partition count AFTER a shuffle.

Where Shuffle Occurred in This Lab
------------------------------------
  Section 4: groupBy + orderBy produced at least 2 shuffle boundaries.
  Section 5: join produced a shuffle (SortMergeJoin) or no shuffle (BroadcastHashJoin).
  Section 6: repartition(8) produced one explicit shuffle stage.

Key Spark UI Tabs to Review
-----------------------------
  Jobs     -> each .show() / .count() action maps to one job entry.
  Stages   -> each job is broken into stages at shuffle boundaries.
  SQL/DAG  -> graphical plan; hover nodes to see row counts and metrics.
  Storage  -> shows cached / persisted RDDs and DataFrames.
""")

spark.stop()
