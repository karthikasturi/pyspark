"""
Challenge 4: Large-Small Join Without Broadcast
================================================
Spark Version: 4.1.1
Topic: A join between a large and a small DataFrame defaults to SortMergeJoin
       (shuffle both sides) unless told otherwise. Broadcast the small side.

Problem:
    When Spark joins two DataFrames it defaults to SortMergeJoin for safety.
    SortMergeJoin shuffles AND sorts BOTH sides -- very expensive when one
    side is small enough to fit in executor memory.

Why it happens:
    Spark uses autoBroadcastJoinThreshold (default 10 MB) to decide
    automatically.  In practice the threshold can be missed due to:
      - Statistics not being computed (no ANALYZE TABLE).
      - The DataFrame being derived from a transformation (size estimate lost).
      - The threshold being set too low for your cluster.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/04_large_small_join_broadcast.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("04_large_small_join_broadcast")
    .config("spark.sql.shuffle.partitions", "8")
    # Disable auto-broadcast so we can show the un-optimized plan clearly
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Large DataFrame: 100_000 transaction rows
# ---------------------------------------------------------------------------
large_df = (
    spark.range(0, 100_000)
    .withColumn("product_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount",      F.round(F.rand(seed=42) * 500, 2))
    .drop("id")
)

# ---------------------------------------------------------------------------
# 2. Small lookup DataFrame: 20 product names
# ---------------------------------------------------------------------------
product_data = [(i, f"Product_{chr(65 + i)}", f"Category_{i % 4}") for i in range(20)]
small_df = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])

print(f"Large DF partitions : {large_df.rdd.getNumPartitions()}")
print(f"Small DF row count  : {small_df.count()}")

# ---------------------------------------------------------------------------
# 3. Baseline join -- no broadcast hint
#    With autoBroadcastJoinThreshold=-1, Spark falls back to SortMergeJoin.
# ---------------------------------------------------------------------------
joined_baseline = large_df.join(small_df, on="product_id", how="inner")

print("\n--- Baseline join (5 rows) ---")
joined_baseline.show(5)

print("\n=== EXPLAIN (baseline -- SortMergeJoin) ===")
# Look for:
#   SortMergeJoin [product_id], [product_id], Inner
#   Two Exchange (shuffle) nodes -- one per side
#   Two Sort nodes -- one per side
joined_baseline.explain("formatted")

# ---------------------------------------------------------------------------
# 4. Optimized join -- broadcast hint on the small side
# ---------------------------------------------------------------------------
joined_opt = large_df.join(F.broadcast(small_df), on="product_id", how="inner")

print("\n--- Optimized join with broadcast (5 rows) ---")
joined_opt.show(5)

print("\n=== EXPLAIN (with broadcast hint) ===")
# Look for:
#   BroadcastHashJoin [product_id], [product_id], Inner, BuildRight
#   BroadcastExchange HashedRelationBroadcastMode on the small side
#   NO Exchange shuffle on the large side -- it is never moved
joined_opt.explain("formatted")

# ---------------------------------------------------------------------------
# 5. Re-enable auto-broadcast and show Spark choosing it automatically
# ---------------------------------------------------------------------------
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # 10 MB

joined_auto = large_df.join(small_df, on="product_id", how="inner")

print("\n=== EXPLAIN (auto-broadcast enabled, Spark decides) ===")
# Spark should now automatically choose BroadcastHashJoin for small_df
joined_auto.explain("formatted")

print("""
Key Takeaway
------------
- SortMergeJoin: shuffles + sorts both sides. Use when both sides are large.
- BroadcastHashJoin: broadcasts the small side to every executor, no shuffle
  on the large side. Use when one side fits in memory (< autoBroadcastJoinThreshold).
- Always prefer F.broadcast() explicitly when you KNOW a DataFrame is small --
  do not rely on Spark's size estimate alone (statistics may be stale).
- Rule of thumb: anything under ~100 MB is a strong broadcast candidate.
- Watch out for: broadcasting a DataFrame that is the result of a large
  join/aggregation -- Spark may overestimate its size after transformations.
""")

spark.stop()
