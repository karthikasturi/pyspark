"""
Topic 12: Accumulators
-----------------------
Creates a shared counter accumulator that workers increment
inside a transformation. The driver reads the final total.

Important limitation: reading an accumulator inside a transformation
may give incorrect results if tasks are retried. Only read on driver
AFTER an action completes.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/12_accumulators.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("12_Accumulators") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Create accumulators on the driver
total_records  = sc.accumulator(0)
negative_count = sc.accumulator(0)

data = sc.parallelize([10, -3, 7, -1, 0, 5, -8, 2], 3)

def inspect(x):
    total_records.add(1)          # count every element
    if x < 0:
        negative_count.add(1)     # count negatives separately
    return x * x                  # actual transformation: square each number

squared = data.map(inspect)

# Action triggers tasks — accumulators are updated on workers
result = squared.collect()

# Safe to read accumulator ONLY after action completes
print(f"Squared values     : {result}")
print(f"Total records seen : {total_records.value}")
print(f"Negative numbers   : {negative_count.value}")

print()
print("Accumulator rules:")
print("  1. Workers can only ADD to an accumulator (write-only from tasks).")
print("  2. Driver can only READ the accumulator (read-only on driver).")
print("  3. Retried tasks can double-count — use for debugging, NOT business logic.")

spark.stop()
