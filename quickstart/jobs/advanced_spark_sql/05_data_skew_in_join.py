"""
Challenge 5: Data Skew in Join
================================
Spark Version: 4.1.1
Topic: Skewed join keys cause one executor to process the majority of data
       (hotspot), making the job as slow as its slowest task.

Problem:
    When a join key is highly skewed (e.g., 80% of rows share key "A"),
    the partition that handles "A" becomes a hotspot -- all other partitions
    finish quickly while one straggler task runs for a long time.

Why it happens:
    Spark hash-partitions join keys.  All rows with key "A" land in the same
    partition regardless of how many there are. If the skewed partition
    exceeds memory it spills to disk, making it even slower.

Two mitigations are shown:
    1. Manual salting -- add a random suffix to the skewed key.
    2. Adaptive Query Execution (AQE) skew join optimization.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/05_data_skew_in_join.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = (
    SparkSession.builder
    .appName("05_data_skew_in_join")
    .config("spark.sql.shuffle.partitions", "8")
    # Disable AQE first so we can show the un-optimized behavior
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create skewed left dataset
#    Key "A": 800 rows  (80 % of data)
#    Key "B": 100 rows
#    Key "C":  50 rows
#    Key "D":  50 rows
# ---------------------------------------------------------------------------
left_data = (
    [("A", i)        for i in range(800)]
    + [("B", i)      for i in range(100)]
    + [("C", i)      for i in range(50)]
    + [("D", i)      for i in range(50)]
)

left_schema = StructType([
    StructField("key",   StringType(), False),
    StructField("value", LongType(),   False),
])

left_df = spark.createDataFrame(left_data, left_schema)

# ---------------------------------------------------------------------------
# 2. Create right (lookup) dataset -- no skew
# ---------------------------------------------------------------------------
right_data = [("A", "Alpha"), ("B", "Beta"), ("C", "Gamma"), ("D", "Delta")]
right_schema = StructType([
    StructField("key",   StringType(), False),
    StructField("label", StringType(), False),
])

right_df = spark.createDataFrame(right_data, right_schema)

print(f"Left DF row count  : {left_df.count()}")
print(f"Right DF row count : {right_df.count()}")
print("\nLeft skew distribution:")
left_df.groupBy("key").count().orderBy(F.desc("count")).show()

# ---------------------------------------------------------------------------
# 3. Baseline join (AQE disabled) -- skew is unmitigated
# ---------------------------------------------------------------------------
joined_skewed = left_df.join(right_df, on="key", how="inner")

print("\n--- Baseline join result (5 rows) ---")
joined_skewed.show(5)

print("\n=== EXPLAIN (baseline -- skewed join, AQE off) ===")
# Look for:
#   SortMergeJoin or BroadcastHashJoin depending on right_df size
#   In real workloads with SortMergeJoin: one partition will be much larger
#   than others. The Spark UI Tasks tab shows task duration imbalance.
joined_skewed.explain("formatted")

# ---------------------------------------------------------------------------
# 4. Mitigation A: Manual salting
#    Append a random bucket number (0..SALT-1) to the left key.
#    Explode the right key to match all salt buckets.
#    This spreads the "A" rows across SALT partitions.
# ---------------------------------------------------------------------------
SALT = 4  # number of buckets -- tune based on skew factor

# Salt the left side
left_salted = left_df.withColumn(
    "salted_key",
    F.concat(F.col("key"), F.lit("_"), (F.rand(seed=7) * SALT).cast("int").cast("string"))
)

# Explode the right side: each key gets SALT copies
right_exploded = right_df.withColumn(
    "salt_bucket",
    F.explode(F.array([F.lit(i) for i in range(SALT)]))
).withColumn(
    "salted_key",
    F.concat(F.col("key"), F.lit("_"), F.col("salt_bucket").cast("string"))
).drop("salt_bucket")

joined_salted = left_salted.join(right_exploded, on="salted_key", how="inner").drop("salted_key")

print("\n--- Salted join result (5 rows) ---")
joined_salted.show(5)
print(f"Salted join row count: {joined_salted.count()}")

print("\n=== EXPLAIN (salted join) ===")
# Look for:
#   Exchange is now partitioned on "salted_key" -- a higher-cardinality key
#   Rows for original key "A" are now spread across SALT partitions
joined_salted.explain("formatted")

# ---------------------------------------------------------------------------
# 5. Mitigation B: Adaptive Query Execution (AQE) skew join
#    AQE automatically detects and splits skewed partitions at runtime.
#    Re-run the original (un-salted) join with AQE enabled.
# ---------------------------------------------------------------------------
spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
# Lower threshold so AQE detects skew in our small dataset
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",   "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1b")

joined_aqe = left_df.join(right_df, on="key", how="inner")

print("\n--- AQE join result (5 rows) ---")
joined_aqe.show(5)

print("\n=== EXPLAIN (AQE enabled) ===")
# Look for:
#   AdaptiveSparkPlan isFinalPlan=true
#   SkewJoin hint inside the plan (may appear after execution in final plan)
#   AQE re-partitions the skewed partition automatically at runtime
joined_aqe.explain("formatted")

print("""
Key Takeaway
------------
- Data skew makes one executor the bottleneck; the job cannot finish faster
  than its slowest partition.
- Detect skew: df.groupBy(joinKey).count().orderBy(desc) -- look for outliers.
  In Spark UI: check Task Metrics in the stage with the join for duration spread.
- Salting: manually spread skewed keys by appending a bucket suffix.
  Trade-off: you must also "explode" the right side, increasing its size by SALT x.
- AQE (Spark 3.0+): automatically splits skewed partitions at runtime.
  Enable via spark.sql.adaptive.enabled=true (default true since Spark 3.2).
  AQE is the preferred solution for most production workloads.
""")

spark.stop()
