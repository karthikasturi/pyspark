"""
Topic 10: Persistence Levels
------------------------------
Shows how to set different StorageLevel options and explains
the trade-offs between them.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/10_persistence_levels.py
"""

import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("10_Persistence_Levels") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

base_rdd = sc.parallelize(range(1, 500_001), 4).map(lambda x: x ** 2)

# --- MEMORY_ONLY (same as cache()) ---
rdd_mem = base_rdd.persist(StorageLevel.MEMORY_ONLY)
t0 = time.time(); rdd_mem.count(); t1 = time.time()   # fill cache
t2 = time.time(); rdd_mem.count(); t3 = time.time()   # from cache
print("MEMORY_ONLY:")
print(f"  Fill  : {t1-t0:.3f}s   | Reuse: {t3-t2:.3f}s")
print(f"  Stored in JVM heap memory only. If not enough RAM → recomputed.")
rdd_mem.unpersist()

# --- MEMORY_AND_DISK ---
rdd_md = base_rdd.persist(StorageLevel.MEMORY_AND_DISK)
t0 = time.time(); rdd_md.count(); t1 = time.time()    # fill cache
t2 = time.time(); rdd_md.count(); t3 = time.time()    # from cache/disk
print("\nMEMORY_AND_DISK:")
print(f"  Fill  : {t1-t0:.3f}s   | Reuse: {t3-t2:.3f}s")
print(f"  Overflow partitions written to disk. Slower than MEMORY_ONLY but safe.")
rdd_md.unpersist()

print()
print("Storage Level Summary:")
print(f"  {'Level':<25} {'Memory':>8} {'Disk':>6} {'Serialized':>12} {'Replicas':>10}")
print(f"  {'-'*63}")
levels = [
    ("MEMORY_ONLY",         True,  False, False, 1),
    ("MEMORY_ONLY_SER",     True,  False, True,  1),
    ("MEMORY_AND_DISK",     True,  True,  False, 1),
    ("MEMORY_AND_DISK_SER", True,  True,  True,  1),
    ("DISK_ONLY",           False, True,  True,  1),
    ("MEMORY_ONLY_2",       True,  False, False, 2),
]
for name, mem, disk, ser, rep in levels:
    print(f"  {name:<25} {str(mem):>8} {str(disk):>6} {str(ser):>12} {rep:>10}")

spark.stop()
