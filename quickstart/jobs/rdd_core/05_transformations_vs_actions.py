"""
Topic 05: Transformations vs Actions
--------------------------------------
Demonstrates lazy evaluation:
  - Transformations (map, filter) build a DAG plan but do NOT execute.
  - Actions (collect, count, reduce) trigger actual execution.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/05_transformations_vs_actions.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("05_Transformations_vs_Actions") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

# --- Transformations (lazy — nothing runs yet) ---
doubled = numbers.map(lambda x: x * 2)          # transformation
evens   = doubled.filter(lambda x: x % 4 == 0)  # transformation chained

print("Transformations registered (no computation yet).")
print(f"RDD type after map+filter: {type(evens)}")

# --- Action — triggers the entire chain ---
result = evens.collect()   # ACTION: now Spark executes map + filter
print(f"\nResult of collect() (action): {result}")

total = evens.reduce(lambda a, b: a + b)   # ACTION
print(f"Sum via reduce()    (action): {total}")

count = evens.count()                       # ACTION
print(f"Count via count()   (action): {count}")

print("\nCommon Transformations (lazy): map, filter, flatMap, groupByKey, reduceByKey")
print("Common Actions   (eager)    : collect, count, reduce, take, saveAsTextFile")

spark.stop()
