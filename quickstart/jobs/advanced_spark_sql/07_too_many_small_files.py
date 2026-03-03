"""
Challenge 7: Too Many Small Output Files
=========================================
Spark Version: 4.1.1
Topic: Writing a highly partitioned DataFrame produces one file per partition,
       causing the "small files problem" downstream.

Problem:
    Spark writes exactly one file per task (one file per partition).
    If a DataFrame has 200 partitions, the output directory will contain
    ~200 files. Hundreds or thousands of small files:
      - Slow down subsequent reads (metadata overhead).
      - Overwhelm HDFS NameNode or S3 list operations.
      - Increase driver-side planning time for downstream jobs.

Why it happens:
    Default parallelism (spark.default.parallelism) or
    spark.sql.shuffle.partitions (default 200) is optimized for computation
    throughput, not for write file count. After computation, the partition
    count is rarely reduced before writing.

Two remedies are shown:
    1. coalesce(N)   -- reduce partitions WITHOUT a shuffle (preferred for writes)
    2. repartition(N) -- reduce partitions WITH a full shuffle (use when you
                         also need to balance partition sizes or change key layout)

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/07_too_many_small_files.py
"""

import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("07_too_many_small_files")
    # Simulate the typical 200-partition default
    .config("spark.sql.shuffle.partitions", "40")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create a dataset and force a shuffle to generate many partitions
# ---------------------------------------------------------------------------
df_raw = spark.range(0, 10_000).withColumn("category", (F.col("id") % 20).cast("string"))

# groupBy triggers a shuffle; result has spark.sql.shuffle.partitions (40) partitions
df_shuffled = df_raw.groupBy("category").agg(F.count("id").alias("cnt"))

print(f"Partition count after shuffle: {df_shuffled.rdd.getNumPartitions()}")

# ---------------------------------------------------------------------------
# 2. Baseline write -- one file per partition
# ---------------------------------------------------------------------------
with tempfile.TemporaryDirectory() as base_dir:
    baseline_path = os.path.join(base_dir, "baseline")
    df_shuffled.write.mode("overwrite").parquet(baseline_path)

    baseline_files = [
        f for f in os.listdir(baseline_path)
        if not f.startswith("_") and not f.startswith(".")
    ]
    print(f"\nBaseline write  -> {len(baseline_files)} output file(s) in {baseline_path}")

    # ---------------------------------------------------------------------------
    # 3. Optimized write A: coalesce before writing
    #    coalesce(N) merges partitions WITHOUT a shuffle.
    #    It narrows the upstream DAG -- existing tasks just write to fewer files.
    #    Limitation: partitions may be uneven because no rebalancing occurs.
    # ---------------------------------------------------------------------------
    coalesced_path = os.path.join(base_dir, "coalesced")
    df_shuffled.coalesce(4).write.mode("overwrite").parquet(coalesced_path)

    coalesced_files = [
        f for f in os.listdir(coalesced_path)
        if not f.startswith("_") and not f.startswith(".")
    ]
    print(f"Coalesce(4) write -> {len(coalesced_files)} output file(s) in {coalesced_path}")

    # ---------------------------------------------------------------------------
    # 4. Optimized write B: repartition before writing
    #    repartition(N) triggers a full shuffle to evenly distribute rows.
    #    Use when you ALSO want balanced file sizes.
    #    Cost: extra shuffle stage.
    # ---------------------------------------------------------------------------
    repartitioned_path = os.path.join(base_dir, "repartitioned")
    df_shuffled.repartition(4).write.mode("overwrite").parquet(repartitioned_path)

    repartitioned_files = [
        f for f in os.listdir(repartitioned_path)
        if not f.startswith("_") and not f.startswith(".")
    ]
    print(f"Repartition(4) write -> {len(repartitioned_files)} output file(s) in {repartitioned_path}")

# ---------------------------------------------------------------------------
# 5. Inspect explain plans for coalesce vs repartition
# ---------------------------------------------------------------------------
print("\n=== EXPLAIN (coalesce) ===")
# Look for:
#   Coalesce N  -- no Exchange node; upstream tasks just combine their output
df_shuffled.coalesce(4).explain("formatted")

print("\n=== EXPLAIN (repartition) ===")
# Look for:
#   Exchange RoundRobinPartitioning(4)  -- a new shuffle stage is added
df_shuffled.repartition(4).explain("formatted")

# ---------------------------------------------------------------------------
# 6. Bonus: write with partitionBy -- creates subdirectories per key value
#    This is the CORRECT way to create a Hive-style partitioned dataset.
#    Use coalesce inside each partition directory via repartition(N, col).
# ---------------------------------------------------------------------------
with tempfile.TemporaryDirectory() as base_dir2:
    partitioned_path = os.path.join(base_dir2, "by_category")

    # Without coalesce: 40 files inside each category= subdirectory
    df_raw.repartition(2, "category").write \
        .mode("overwrite") \
        .partitionBy("category") \
        .parquet(partitioned_path)

    category_dirs = [
        d for d in os.listdir(partitioned_path)
        if d.startswith("category=")
    ]
    sample_dir = os.path.join(partitioned_path, category_dirs[0])
    sample_files = [f for f in os.listdir(sample_dir) if not f.startswith("_")]
    print(f"\npartitionBy write -> {len(category_dirs)} category dirs")
    print(f"Files in '{category_dirs[0]}': {len(sample_files)} file(s)")

print("""
Key Takeaway
------------
coalesce(N):
  - No shuffle. Merges partitions on the same executor by combining tasks.
  - Preferred for reducing file count before a write.
  - Partition sizes may be uneven (some tasks do more work).
  - Cannot INCREASE partition count.

repartition(N):
  - Full shuffle. Distributes rows evenly (round-robin) or by key.
  - Use when you need balanced output file sizes.
  - Use repartition(N, col) to co-locate related rows (e.g., before partitionBy write).
  - Adds an extra Exchange stage -- more expensive than coalesce.

When writing partitioned tables (partitionBy):
  - Use repartition(N, partitionCol) before write to control file count per partition.
  - Without it, each task in the stage writes to every partition directory,
    creating #tasks x #partitions files total.
""")

spark.stop()
