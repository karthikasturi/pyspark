"""
Topic 07: Lazy Evaluation
--------------------------
Proves that transformations do NOT execute until an action is called,
using side-effect print statements inside map() to show timing.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/07_lazy_evaluation.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("07_Lazy_Evaluation") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

def double_with_log(x):
    # This print executes ONLY when a task actually runs on a worker
    print(f"  [WORKER] Processing element: {x}")
    return x * 2

numbers = sc.parallelize([10, 20, 30, 40], 2)

# --- Step 1: Define transformation (NOTHING runs here) ---
print("[DRIVER] About to register map() transformation...")
doubled = numbers.map(double_with_log)
print("[DRIVER] map() registered. No worker activity yet.\n")

# --- Step 2: Define another transformation (NOTHING runs here) ---
print("[DRIVER] About to register filter() transformation...")
large   = doubled.filter(lambda x: x > 40)
print("[DRIVER] filter() registered. Still no worker activity.\n")

# --- Step 3: Action triggers the entire plan ---
print("[DRIVER] Calling collect() — THIS triggers execution...\n")
result = large.collect()

print(f"\n[DRIVER] collect() complete. Result: {result}")
print()
print("Observation: [WORKER] prints appeared ONLY after collect() was called.")
print("That proves transformations are lazy — they build a plan, not results.")

spark.stop()
