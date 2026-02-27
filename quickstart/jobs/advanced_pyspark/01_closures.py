"""
Advanced PySpark Concepts – Section 1: Closures
================================================
Demonstrates:
  - How a scalar variable is captured in a closure and sent to executors
  - Why capturing large objects is a bad practice
  - Brief reference to broadcast variables as the correct fix

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_pyspark/01_closures.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("01_Closures").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    separator = "=" * 60

    # ------------------------------------------------------------------
    # EXAMPLE 1: Good practice — capturing a small scalar in a closure
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 1 – Simple closure (good practice)")
    print(f"{separator}")

    threshold = 10
    numbers = sc.parallelize([3, 7, 10, 15, 22, 4, 18])

    # 'threshold' is serialized with the lambda and sent to each executor
    filtered = numbers.filter(lambda x: x > threshold)

    result = filtered.collect()
    print(f"  Input : [3, 7, 10, 15, 22, 4, 18]")
    print(f"  Filter: x > {threshold}")
    print(f"  Output: {result}")
    assert result == [15, 22, 18], f"Unexpected result: {result}"
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 2: Bad practice — large object captured in a closure
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 2 – Large object in closure (bad practice, small scale)")
    print(f"{separator}")

    # In production this could be millions of entries, causing the dict
    # to be serialized and shipped with EVERY task, not just once.
    large_lookup = {i: i * 2 for i in range(1_000)}   # kept small for demo

    rdd = sc.parallelize(list(range(5)))
    result_bad = rdd.map(lambda x: large_lookup.get(x, -1)).collect()

    print(f"  Lookup for [0..4] -> {result_bad}")
    assert result_bad == [0, 2, 4, 6, 8], f"Unexpected: {result_bad}"
    print("  PASS (correct results, but large_lookup was serialized per task!)")

    # ------------------------------------------------------------------
    # EXAMPLE 3: Correct fix — broadcast variable
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 3 – Broadcast variable (correct fix)")
    print(f"{separator}")

    broadcast_lookup = sc.broadcast(large_lookup)

    rdd2 = sc.parallelize(list(range(5)))
    result_bc = rdd2.map(lambda x: broadcast_lookup.value.get(x, -1)).collect()

    print(f"  Broadcast lookup for [0..4] -> {result_bc}")
    assert result_bc == [0, 2, 4, 6, 8], f"Unexpected: {result_bc}"
    print("  PASS (large_lookup distributed ONCE per executor node, not per task)")

    broadcast_lookup.unpersist()

    print(f"\n{separator}")
    print("  Section 1 – Closures: ALL EXAMPLES PASSED")
    print(f"{separator}\n")

    spark.stop()


if __name__ == "__main__":
    main()
