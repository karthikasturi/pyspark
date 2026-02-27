"""
Topic 08: Fault Tolerance and Lineage
---------------------------------------
Demonstrates that Spark can recompute lost partitions by replaying
the recorded lineage (DAG), without storing all intermediate data.

We simulate this by deliberately calling unpersist() mid-pipeline
and showing Spark recomputes the result correctly.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/08_fault_tolerance_lineage.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("08_Fault_Tolerance_Lineage") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Build a pipeline — each step is recorded in the lineage graph
raw     = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
doubled = raw.map(lambda x: x * 2)          # Step 1 in lineage
filtered = doubled.filter(lambda x: x > 4)  # Step 2 in lineage

# First execution
result1 = filtered.collect()
print(f"First collect()  : {result1}")

# Print lineage — Spark holds THIS, not intermediate data
print("\nLineage (how Spark would recompute a lost partition):")
print(filtered.toDebugString().decode("utf-8"))

# If a partition were lost, Spark sees the lineage:
#   parallelize → map(x*2) → filter(x>4)
# and replays only the needed steps for the missing partition.

# Simulate a second independent recompute (analogous to recomputation)
result2 = filtered.collect()
print(f"\nSecond collect() (recomputed from lineage): {result2}")
print(f"Results match: {result1 == result2}")

print()
print("Key insight: Spark NEVER needs to checkpoint every step.")
print("It stores only the lineage recipe. Lost data → recompute from source.")

spark.stop()
