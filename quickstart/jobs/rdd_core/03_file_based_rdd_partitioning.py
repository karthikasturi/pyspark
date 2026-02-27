"""
Topic 03: Partitioning of File-based RDDs
------------------------------------------
Reads a text file and inspects how Spark partitions it.
Uses glom() to show which lines land in which partition.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/03_file_based_rdd_partitioning.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("03_File_Based_RDD_Partitioning") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

file_path = "/opt/spark/jobs/rdd_core/data/words.txt"

# Read the file — Spark creates partitions based on HDFS block size (128 MB default).
# For small local files, this is typically 1 partition.
rdd = sc.textFile(file_path)
print(f"File: {file_path}")
print(f"Number of partitions: {rdd.getNumPartitions()}")
print(f"Total lines        : {rdd.count()}")

# glom() groups each partition's elements into a list — reveals the physical split
print("\nLines per partition:")
for i, lines in enumerate(rdd.glom().collect()):
    print(f"  Partition {i}: {len(lines)} lines")

# Force more partitions using minPartitions hint
rdd_multi = sc.textFile(file_path, minPartitions=3)
print(f"\nWith minPartitions=3 → actual partitions: {rdd_multi.getNumPartitions()}")
print("Lines per partition (minPartitions=3):")
for i, lines in enumerate(rdd_multi.glom().collect()):
    print(f"  Partition {i}: {len(lines)} lines")

spark.stop()
