"""
Challenge 6: Executor Memory Pressure via Caching
===================================================
Spark Version: 4.1.1
Topic: Caching large DataFrames without considering storage vs. execution
       memory can cause cache eviction, GC pressure, and spills.

Problem:
    spark.persist() / .cache() stores data in the Storage memory region.
    If the cached data is larger than available Storage memory, Spark evicts
    old blocks (LRU). Evicted blocks must be recomputed on next access --
    defeating the purpose of caching.

Why it happens:
    Spark's unified memory model (Spark 1.6+) shares a single memory pool
    between Storage and Execution (shuffle buffers, sort buffers, etc.).
    By default:
        spark.memory.fraction         = 0.6  (60% of JVM heap)
        spark.memory.storageFraction  = 0.5  (50% of the 60% = 30% of heap for storage)
    Everything cached above that 30% can be evicted under execution pressure.

StorageLevels explained:
    MEMORY_ONLY          -- store as JVM objects; evict (recompute) if no room
    MEMORY_AND_DISK      -- spill to local disk if no room; slower but safe
    MEMORY_ONLY_SER      -- store as serialized bytes; smaller footprint
    MEMORY_AND_DISK_SER  -- serialized + disk fallback; best balance for large data
    DISK_ONLY            -- always on disk; no memory use; slow reads

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/06_executor_memory_caching.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder
    .appName("06_executor_memory_caching")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create a moderately large synthetic dataset (~50_000 rows)
# ---------------------------------------------------------------------------
df = (
    spark.range(0, 50_000)
    .withColumn("category",    (F.col("id") % 10).cast("string"))
    .withColumn("sub_category",(F.col("id") % 50).cast("string"))
    .withColumn("amount",       F.round(F.rand(seed=1) * 1000, 2))
    .withColumn("qty",         (F.rand(seed=2) * 100).cast("int"))
    .withColumn("description",  F.concat(F.lit("item_"), F.col("id").cast("string")))
)

print(f"Dataset rows    : {df.count()}")
print(f"Dataset partitions: {df.rdd.getNumPartitions()}")

# ---------------------------------------------------------------------------
# 2. MEMORY_ONLY cache (default .cache())
#    Good for: frequently accessed DataFrames that fit in memory.
#    Risk:     eviction and silent recomputation if memory is under pressure.
# ---------------------------------------------------------------------------
df_memory = df.persist(StorageLevel.MEMORY_ONLY)
df_memory.count()  # trigger cache population

print("\n--- MEMORY_ONLY: aggregation query ---")
df_memory.groupBy("category").agg(
    F.sum("amount").alias("total_amount"),
    F.avg("qty").alias("avg_qty")
).orderBy("category").show()

# Check what is cached in the Spark UI Storage tab (programmatic proxy):
sc = spark.sparkContext
print("\nCached RDD entries:")
for rdd_info in sc.statusTracker().getExecutorInfos():
    pass  # In real clusters you would inspect storageInfo here

# ---------------------------------------------------------------------------
# 3. MEMORY_AND_DISK cache
#    Good for: DataFrames that are too large for available memory.
#    Spark spills to local disk when storage memory is exhausted instead of
#    evicting and recomputing.
# ---------------------------------------------------------------------------
df_memory.unpersist()

df_disk = df.persist(StorageLevel.MEMORY_AND_DISK)
df_disk.count()  # trigger cache population

print("\n--- MEMORY_AND_DISK: same aggregation ---")
df_disk.groupBy("category").agg(
    F.sum("amount").alias("total_amount"),
    F.avg("qty").alias("avg_qty")
).orderBy("category").show()

df_disk.unpersist()

# ---------------------------------------------------------------------------
# 4. MEMORY_ONLY_SER -- serialized Java objects
#    Smaller memory footprint than MEMORY_ONLY at the cost of CPU for
#    serialization/deserialization. Useful when data is large but access
#    is frequent enough that CPU overhead is acceptable.
# ---------------------------------------------------------------------------
df_ser = df.persist(StorageLevel.MEMORY_ONLY_SER)
df_ser.count()

print("\n--- MEMORY_ONLY_SER: same aggregation ---")
df_ser.groupBy("category").agg(
    F.sum("amount").alias("total_amount"),
).orderBy("category").show()

df_ser.unpersist()

# ---------------------------------------------------------------------------
# 5. Rules of thumb
# ---------------------------------------------------------------------------
print("""
Storage Level Comparison
-------------------------
Level                | Memory Use | Disk Fallback | CPU Overhead
---------------------|------------|---------------|-------------
MEMORY_ONLY          | High       | No (evict)    | Low
MEMORY_AND_DISK      | High       | Yes           | Low
MEMORY_ONLY_SER      | Lower      | No (evict)    | Medium (ser/deser)
MEMORY_AND_DISK_SER  | Lower      | Yes           | Medium
DISK_ONLY            | None       | Always        | High (IO)

Decision Guide
--------------
- DataFrame accessed many times + fits in memory        -> MEMORY_ONLY
- DataFrame accessed many times + does NOT fit          -> MEMORY_AND_DISK
- Memory is scarce but CPU is available                 -> MEMORY_ONLY_SER
- Truly large DataFrame, disk is fast (SSD)             -> MEMORY_AND_DISK_SER
- Always call .unpersist() when a cached DF is no longer needed.
- Never cache a DataFrame that is used only once.
- Caching a DataFrame with wide schema (many columns) wastes memory;
  select() to the columns you need before caching.
""")

spark.stop()
