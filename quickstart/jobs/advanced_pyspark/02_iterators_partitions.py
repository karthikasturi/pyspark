"""
Advanced PySpark Concepts – Section 2: Iterators and Partition Processing
=========================================================================
Demonstrates:
  - How Spark splits data into partitions
  - Using glom() to inspect partition contents
  - How map() operates element-by-element within partitions
  - Verifying partition counts and layouts

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_pyspark/02_iterators_partitions.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("02_IteratorsPartitions").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    separator = "=" * 60

    # ------------------------------------------------------------------
    # EXAMPLE 1: glom() — inspect partition contents
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 1 – Viewing partitions with glom()")
    print(f"{separator}")

    data = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    rdd = sc.parallelize(data, numSlices=3)

    partitions = rdd.glom().collect()

    print(f"  Dataset    : {data}")
    print(f"  Num partitions: {rdd.getNumPartitions()}")
    print("  Partition layout:")
    for i, p in enumerate(partitions):
        print(f"    Partition {i}: {p}")

    # Verify all data is present and across exactly 3 partitions
    all_values = [v for part in partitions for v in part]
    assert sorted(all_values) == sorted(data), "Data mismatch after partitioning"
    assert rdd.getNumPartitions() == 3, "Expected 3 partitions"
    assert all(len(p) > 0 for p in partitions), "Empty partition detected"
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 2: map() — element-level transformation across partitions
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 2 – map() operates per element")
    print(f"{separator}")

    rdd2 = sc.parallelize([1, 2, 3, 4, 6], numSlices=2)
    partition_layout = rdd2.glom().collect()
    squared = rdd2.map(lambda x: x ** 2).collect()

    print(f"  Input partition layout : {partition_layout}")
    print(f"  After map(x -> x**2)   : {squared}")

    assert sorted(squared) == [1, 4, 9, 16, 36], f"Unexpected: {squared}"
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 3: Repartitioning — changing partition count
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 3 – Repartition changes partition count")
    print(f"{separator}")

    rdd3 = sc.parallelize(range(12), numSlices=2)
    print(f"  Original partitions : {rdd3.getNumPartitions()}")
    print(f"  Layout: {rdd3.glom().collect()}")

    rdd3_repartitioned = rdd3.repartition(4)
    print(f"  After repartition(4): {rdd3_repartitioned.getNumPartitions()}")
    print(f"  Layout: {rdd3_repartitioned.glom().collect()}")

    assert rdd3_repartitioned.getNumPartitions() == 4
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 4: Each partition processed independently
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 4 – Partition-level sum (demonstrates independence)")
    print(f"{separator}")

    data4 = [1, 2, 3, 4, 5, 6]
    rdd4 = sc.parallelize(data4, numSlices=3)

    # Compute the sum of elements in each partition independently
    partition_sums = rdd4.glom().map(lambda part: sum(part)).collect()
    total = sum(partition_sums)

    print(f"  Input: {data4}  (3 partitions)")
    print(f"  Sum per partition: {partition_sums}")
    print(f"  Grand total      : {total}")

    assert total == sum(data4), "Grand total mismatch"
    print("  PASS")

    print(f"\n{separator}")
    print("  Section 2 – Iterators & Partitions: ALL EXAMPLES PASSED")
    print(f"{separator}\n")

    spark.stop()


if __name__ == "__main__":
    main()
