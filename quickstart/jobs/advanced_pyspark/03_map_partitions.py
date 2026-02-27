"""
Advanced PySpark Concepts – Section 3: mapPartitions for Efficiency
====================================================================
Demonstrates:
  - The difference between map() and mapPartitions()
  - Why mapPartitions() reduces overhead for expensive setup operations
  - How to count how many times setup is triggered vs element processing
  - A side-by-side comparison using an accumulator as a setup counter

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_pyspark/03_map_partitions.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("03_MapPartitions").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    separator = "=" * 60

    data = [1, 2, 3, 4, 5, 6, 7, 8]
    num_partitions = 2

    # ------------------------------------------------------------------
    # EXAMPLE 1: map() — setup runs once per element
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 1 – map(): setup runs per ELEMENT")
    print(f"{separator}")

    rdd = sc.parallelize(data, numSlices=num_partitions)

    # Accumulator to count how many times "setup" was triggered
    setup_counter_map = sc.accumulator(0)

    def process_with_map(x):
        # Simulate expensive setup (runs for every element)
        setup_counter_map.add(1)
        return x * 10

    result_map = rdd.map(process_with_map).collect()
    print(f"  Data           : {data}  ({num_partitions} partitions)")
    print(f"  Result         : {result_map}")
    print(f"  'Setup' calls  : {setup_counter_map.value}  (expected: {len(data)}  = 1 per element)")
    assert result_map == [10, 20, 30, 40, 50, 60, 70, 80], f"Unexpected: {result_map}"
    assert setup_counter_map.value == len(data), \
        f"Expected {len(data)} setup calls, got {setup_counter_map.value}"
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 2: mapPartitions() — setup runs once per partition
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 2 – mapPartitions(): setup runs per PARTITION")
    print(f"{separator}")

    rdd2 = sc.parallelize(data, numSlices=num_partitions)
    setup_counter_mp = sc.accumulator(0)

    def process_partition(iterator):
        # Expensive setup — runs ONCE for the entire partition
        setup_counter_mp.add(1)
        for x in iterator:
            yield x * 10   # reuses the setup for every element in the partition

    result_mp = rdd2.mapPartitions(process_partition).collect()
    print(f"  Data           : {data}  ({num_partitions} partitions)")
    print(f"  Result         : {result_mp}")
    print(f"  'Setup' calls  : {setup_counter_mp.value}  (expected: {num_partitions}  = 1 per partition)")
    assert result_mp == [10, 20, 30, 40, 50, 60, 70, 80], f"Unexpected: {result_mp}"
    assert setup_counter_mp.value == num_partitions, \
        f"Expected {num_partitions} setup calls, got {setup_counter_mp.value}"
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 3: Larger dataset — the difference scales up
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 3 – Scaling effect: 100 elements, 4 partitions")
    print(f"{separator}")

    big_data = list(range(1, 101))   # 100 elements
    big_partitions = 4
    rdd3 = sc.parallelize(big_data, numSlices=big_partitions)

    map_setup    = sc.accumulator(0)
    mp_setup     = sc.accumulator(0)

    def process_map_big(x):
        map_setup.add(1)
        return x * 2

    def process_mp_big(iterator):
        mp_setup.add(1)
        for x in iterator:
            yield x * 2

    result_map_big = rdd3.map(process_map_big).collect()
    result_mp_big  = rdd3.mapPartitions(process_mp_big).collect()

    print(f"  Elements       : {len(big_data)}")
    print(f"  Partitions     : {big_partitions}")
    print(f"  map()   setup calls : {map_setup.value}   (= num elements)")
    print(f"  mapPartitions() calls: {mp_setup.value}     (= num partitions)")
    print(f"  Both produce same result: {sorted(result_map_big) == sorted(result_mp_big)}")

    assert map_setup.value == len(big_data)
    assert mp_setup.value == big_partitions
    assert sorted(result_map_big) == sorted(result_mp_big)
    print("  PASS")

    print(f"\n{separator}")
    print("  Section 3 – mapPartitions: ALL EXAMPLES PASSED")
    print(f"{separator}\n")

    spark.stop()


if __name__ == "__main__":
    main()
