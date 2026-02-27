"""
Topic 02: Logical vs Physical Partitioning
-------------------------------------------
Shows the difference between:
  - logical partitions set by parallelize()
  - physical partitions inferred by textFile() from file blocks.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/02_logical_vs_physical_partitioning.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("02_Logical_vs_Physical_Partitioning") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# --- Logical partitioning via parallelize() ---
# You explicitly control the number of partitions.
mem_rdd = sc.parallelize(range(100), numSlices=6)
print(f"parallelize() with 6 slices → partitions: {mem_rdd.getNumPartitions()}")

# --- Physical partitioning via textFile() ---
# Spark reads the file and divides it into splits based on block size.
# For local small files this is usually 1 (entire file fits in one block).
file_rdd = sc.textFile("/opt/spark/jobs/rdd_core/data/words.txt")
print(f"textFile() default       → partitions: {file_rdd.getNumPartitions()}")

# You can hint a minimum number of partitions for textFile()
file_rdd_min = sc.textFile("/opt/spark/jobs/rdd_core/data/words.txt", minPartitions=4)
print(f"textFile() minPartitions=4 → partitions: {file_rdd_min.getNumPartitions()}")

# Key insight
print("\nKey insight:")
print("  parallelize() → YOU decide the partition count (logical split).")
print("  textFile()    → Spark decides based on file blocks (physical split),")
print("                  but you can set a minimum floor with minPartitions.")

spark.stop()
