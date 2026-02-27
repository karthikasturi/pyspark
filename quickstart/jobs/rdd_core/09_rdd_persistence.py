"""
Topic 09: RDD Persistence (cache)
-----------------------------------
Compares execution time for the same count() operation
with and without cache().  Without cache, Spark recomputes
the entire lineage on each action.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/09_rdd_persistence.py
"""

import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("09_RDD_Persistence") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Build a moderately expensive RDD (large range + transform)
rdd = sc.parallelize(range(1, 1_000_001), 4) \
        .map(lambda x: x * 2) \
        .filter(lambda x: x % 3 == 0)

# --- WITHOUT cache ---
start = time.time()
count1 = rdd.count()          # full recompute
end   = time.time()
time_no_cache_1 = end - start

start = time.time()
count2 = rdd.count()          # full recompute again
end   = time.time()
time_no_cache_2 = end - start

print(f"Without cache — 1st count(): {count1}  | time: {time_no_cache_1:.3f}s")
print(f"Without cache — 2nd count(): {count2}  | time: {time_no_cache_2:.3f}s")

# --- WITH cache ---
rdd.cache()                   # mark for caching in memory

start = time.time()
count3 = rdd.count()          # triggers computation AND caches result
end   = time.time()
time_cache_1 = end - start

start = time.time()
count4 = rdd.count()          # served from cache — much faster
end   = time.time()
time_cache_2 = end - start

print(f"\nWith    cache — 1st count(): {count3}  | time: {time_cache_1:.3f}s  (compute + cache)")
print(f"With    cache — 2nd count(): {count4}  | time: {time_cache_2:.3f}s  (from cache)")

rdd.unpersist()               # free cache memory
print("\nCache unpersisted.")

spark.stop()
