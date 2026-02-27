"""
Topic 06: Stages, Tasks, and RDD Lineage
------------------------------------------
Uses toDebugString() to print the RDD lineage (DAG).
Shows how a shuffle boundary (reduceByKey) splits the DAG into stages.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/06_stages_tasks_lineage.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("06_Stages_Tasks_Lineage") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Build a word-count pipeline
lines    = sc.parallelize([
    "spark is fast", "spark is easy",
    "rdd is powerful", "spark rdd spark"
], 2)

words    = lines.flatMap(lambda line: line.split())   # Stage 1
pairs    = words.map(lambda w: (w, 1))                # Stage 1 (same stage, no shuffle)
counts   = pairs.reduceByKey(lambda a, b: a + b)      # SHUFFLE → new Stage 2

# Print the lineage DAG — indentation shows dependency depth
print("=" * 55)
print("RDD Lineage (toDebugString):")
print("=" * 55)
print(counts.toDebugString().decode("utf-8"))

# Trigger execution
result = counts.collect()
print("\nWord counts:")
for word, count in sorted(result):
    print(f"  {word:12s}: {count}")

print()
print("Lineage reading guide:")
print("  Each indented block  = one RDD transformation step.")
print("  ShuffledRDD          = shuffle boundary → new Stage.")
print("  Tasks = num partitions × num stages.")

spark.stop()
