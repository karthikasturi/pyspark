"""
Topic 11: Broadcast Variables
-------------------------------
Creates a small lookup dictionary on the driver, broadcasts it
to all workers, and uses it inside a map() transformation.

Without broadcast: the dict would be serialized and sent with
EVERY task. With broadcast: it is sent ONCE per worker and cached.

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/rdd_core/11_broadcast_variables.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("11_Broadcast_Variables") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Small lookup table (lives on driver)
country_codes = {1: "USA", 44: "UK", 91: "India", 81: "Japan", 49: "Germany"}

# Broadcast → sent once to each worker node, cached in worker memory
bc_lookup = sc.broadcast(country_codes)

# RDD of (user_id, dialing_code) tuples
users = sc.parallelize([
    (101, 1), (102, 91), (103, 44),
    (104, 81), (105, 49), (106, 91)
], 2)

# Use .value to access the broadcast variable inside the task
def enrich_user(record):
    user_id, code = record
    country = bc_lookup.value.get(code, "Unknown")  # lookup without shipping dict each time
    return (user_id, code, country)

enriched = users.map(enrich_user).collect()

print("Enriched user records:")
print(f"  {'user_id':>8} {'code':>6} {'country':<12}")
print(f"  {'-'*28}")
for row in enriched:
    print(f"  {row[0]:>8} {row[1]:>6} {row[2]:<12}")

print(f"\nBroadcast value (driver access): {bc_lookup.value}")

bc_lookup.destroy()   # release broadcast from all nodes
print("\nBroadcast variable destroyed.")

spark.stop()
