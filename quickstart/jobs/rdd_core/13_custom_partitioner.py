"""
Topic 13: Custom Partitioner
------------------------------
Implements a simple hash-based custom partitioner using partitionBy().
Useful when you want related keys to always land in the same partition
(e.g., same user's data co-located for efficient joins).

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/13_custom_partitioner.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("13_Custom_Partitioner") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

NUM_PARTITIONS = 3

def category_partitioner(key):
    """Route keys to partitions:
       'electronics' → 0, 'clothing' → 1, everything else → 2
    """
    routing = {"electronics": 0, "clothing": 1}
    return routing.get(key, 2)   # default partition = 2

# Pair RDD: (category, product_name)
products = sc.parallelize([
    ("electronics", "Laptop"),
    ("clothing",    "Jacket"),
    ("electronics", "Phone"),
    ("food",        "Apple"),
    ("clothing",    "Shoes"),
    ("food",        "Bread"),
    ("electronics", "Tablet"),
], 2)

# partitionBy requires a pair RDD and a custom partitioner function
partitioned = products.partitionBy(NUM_PARTITIONS, category_partitioner)

print(f"Number of partitions: {partitioned.getNumPartitions()}")
print()

# Show contents of each partition
print("Partition distribution:")
for i, records in enumerate(partitioned.glom().collect()):
    labels = {"0": "electronics", "1": "clothing", "2": "others"}
    print(f"  Partition {i} ({labels[str(i)]}): {records}")

# Verify the partitioner was set
print(f"\nPartitioner set: {partitioned.partitioner is not None}")

spark.stop()
