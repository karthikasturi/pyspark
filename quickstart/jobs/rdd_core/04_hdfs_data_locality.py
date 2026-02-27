"""
Topic 04: HDFS and Data Locality (conceptual simulation)
----------------------------------------------------------
Explains data locality and shows how to inspect preferred locations
for each partition using getPreferredLocations().

In a real HDFS cluster, preferred locations are actual DataNode
hostnames. In local/standalone mode this returns an empty list —
which is the honest simulation.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/04_hdfs_data_locality.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("04_HDFS_Data_Locality") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

file_path = "/opt/spark/jobs/rdd_core/data/words.txt"
rdd = sc.textFile(file_path, minPartitions=3)

print(f"Number of partitions: {rdd.getNumPartitions()}")
print()

# getPreferredLocations() returns the host(s) that hold data for a given partition.
# On local/standalone mode this is empty — no HDFS block metadata.
# We use the SparkContext's internal dagScheduler to query preferred locations.
for part_idx in range(rdd.getNumPartitions()):
    try:
        partitions_seq = rdd._jrdd.partitions()
        part = partitions_seq[part_idx]
        # SparkContext.getPreferredLocs returns a Seq[TaskLocation]
        sc_jvm = sc._jvm.org.apache.spark.SparkContext
        jsc = sc._jsc.sc()                         # underlying Java SparkContext
        locs_seq = jsc.getPreferredLocs(rdd._jrdd.rdd(), part_idx)
        loc_list = [str(locs_seq.apply(i)) for i in range(locs_seq.length())]
    except Exception:
        loc_list = []
    print(f"  Partition {part_idx} preferred locations: "
          f"{loc_list if loc_list else '(none — local/standalone mode)'}")

print()
print("Data Locality levels Spark uses (best → worst):")
print("  PROCESS_LOCAL  : data in same JVM (cached RDD)")
print("  NODE_LOCAL     : data on same node, different process/disk")
print("  RACK_LOCAL     : data on same rack, different node")
print("  ANY            : data anywhere in the cluster")
print()
print("Spark waits a short time for a better locality before falling back.")
print("This minimises network I/O and speeds up computation.")

spark.stop()
