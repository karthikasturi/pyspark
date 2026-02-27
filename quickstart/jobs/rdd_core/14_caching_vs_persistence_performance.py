"""
Topic 14: Caching vs Persistence — Performance Comparison
-----------------------------------------------------------
Runs the same computation four ways and compares wall-clock time:
  1. No caching (recompute every time)
  2. cache()  — MEMORY_ONLY (default)
  3. persist(MEMORY_AND_DISK)
  4. persist(DISK_ONLY)

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/14_caching_vs_persistence_performance.py
"""

import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("14_Caching_vs_Persistence") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

def build_rdd():
    return (sc.parallelize(range(1, 600_001), 4)
              .map(lambda x: x * 3)
              .filter(lambda x: x % 7 == 0))

def timed_double_count(rdd, label):
    """Run count() twice and report both timings."""
    t0 = time.time(); rdd.count(); t1 = time.time()   # first: may fill cache
    t2 = time.time(); rdd.count(); t3 = time.time()   # second: reuse/recompute
    print(f"  {label:<28} | 1st count: {t1-t0:.3f}s  | 2nd count: {t3-t2:.3f}s")

print(f"{'='*70}")
print(f"  {'Strategy':<28} | {'1st count':>12}  | {'2nd count':>12}")
print(f"{'='*70}")

# 1. No caching
r = build_rdd()
timed_double_count(r, "No caching (recompute)")

# 2. cache() = MEMORY_ONLY
r = build_rdd().cache()
timed_double_count(r, "cache() MEMORY_ONLY")
r.unpersist()

# 3. MEMORY_AND_DISK
r = build_rdd().persist(StorageLevel.MEMORY_AND_DISK)
timed_double_count(r, "persist MEMORY_AND_DISK")
r.unpersist()

# 4. DISK_ONLY
r = build_rdd().persist(StorageLevel.DISK_ONLY)
timed_double_count(r, "persist DISK_ONLY")
r.unpersist()

print(f"{'='*70}")
print()
print("Performance ranking (fastest 2nd count → slowest):")
print("  cache() MEMORY_ONLY  >  MEMORY_AND_DISK  >  DISK_ONLY  >  No caching")
print()
print("Choose based on:")
print("  MEMORY_ONLY      → dataset fits in RAM, accessed many times")
print("  MEMORY_AND_DISK  → large dataset, tolerate some disk spill")
print("  DISK_ONLY        → very large dataset, RAM is scarce")
print("  No caching       → RDD used only once")

spark.stop()
