"""
Topic 01: RDD Partitions Overview
----------------------------------
Demonstrates how to create an RDD with a custom number of partitions
and inspect partition count.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/01_rdd_partitions_overview.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("01_RDD_Partitions_Overview") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data = list(range(1, 21))  # [1, 2, ..., 20]

# Create RDD with default number of partitions (determined by cluster cores)
default_rdd = sc.parallelize(data)
print(f"Default partitions : {default_rdd.getNumPartitions()}")

# Create RDD with explicitly 4 partitions
rdd_4 = sc.parallelize(data, 4)
print(f"Custom 4 partitions: {rdd_4.getNumPartitions()}")

# Create RDD with 2 partitions
rdd_2 = sc.parallelize(data, 2)
print(f"Custom 2 partitions: {rdd_2.getNumPartitions()}")

# Show which elements live in each partition
print("\nPartition contents (4-partition RDD):")
for i, partition in enumerate(rdd_4.glom().collect()):
    print(f"  Partition {i}: {partition}")

# Each partition maps to one task — more partitions = more parallelism
print(f"\nEach partition becomes 1 task. 4 partitions → 4 tasks run in parallel.")

spark.stop()
