"""
Advanced PySpark Concepts – Section 4: Case Classes and Pattern Matching
========================================================================
Python does not have Scala-style case classes.
This script demonstrates the Python equivalents:
  - @dataclass as a typed row structure
  - Converting RDD tuples to dataclass instances
  - match-case (Python 3.10+) for classification logic
  - Row objects as a Spark-native alternative

Requires Python 3.10+ (available in apache/spark:4.1.1 image)

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/advanced_pyspark/04_case_classes_pattern_matching.py
"""

import sys
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession, Row


# ---------------------------------------------------------------------------
# Equivalent of a Scala case class — typed, readable, auto-repr
# ---------------------------------------------------------------------------
@dataclass
class SalesRecord:
    order_id: int
    product: str
    amount: float
    region: Optional[str] = None


# ---------------------------------------------------------------------------
# Pattern matching function — Python 3.10+ match-case
# ---------------------------------------------------------------------------
def classify_record(record: SalesRecord) -> str:
    match record:
        case SalesRecord(amount=amt, region=None) if amt > 100:
            return f"HIGH-VALUE UNASSIGNED  | {record.product:<12} | ${amt:>8.2f} | region=None"
        case SalesRecord(amount=amt) if amt >= 500:
            return f"PREMIUM                | {record.product:<12} | ${amt:>8.2f} | {record.region}"
        case SalesRecord(amount=amt) if amt >= 50:
            return f"STANDARD               | {record.product:<12} | ${amt:>8.2f} | {record.region}"
        case _:
            return f"SMALL                  | {record.product:<12} | ${amt:>8.2f} | {record.region}"


def main():
    if sys.version_info < (3, 10):
        print(f"ERROR: match-case requires Python 3.10+. Got {sys.version}")
        sys.exit(1)

    spark = SparkSession.builder.appName("04_CaseClassesPatternMatching").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    separator = "=" * 60

    raw_data = [
        (1, "Laptop",   1200.0, "North"),
        (2, "Mouse",      25.0, "South"),
        (3, "Monitor",   450.0, "North"),
        (4, "Keyboard",  150.0, None),      # high-value, unassigned region
        (5, "Webcam",    110.0, "East"),
        (6, "USB Hub",    35.0, "West"),
        (7, "Docking",   750.0, "North"),
    ]

    # ------------------------------------------------------------------
    # EXAMPLE 1: dataclass as a case class equivalent
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 1 – @dataclass as case class equivalent")
    print(f"{separator}")

    rdd = sc.parallelize(raw_data)

    records_rdd = rdd.map(lambda t: SalesRecord(
        order_id=t[0], product=t[1], amount=t[2], region=t[3]
    ))

    records = records_rdd.collect()
    print("  Structured records (with named fields):")
    for r in records:
        print(f"    {r}")

    assert len(records) == len(raw_data)
    assert records[0].product == "Laptop"
    assert records[0].amount  == 1200.0
    assert records[3].region  is None
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 2: Pattern matching with match-case
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 2 – match-case classification (Python 3.10+)")
    print(f"{separator}")

    print(f"  {'Category':<22}   {'Product':<12}   {'Amount':>10}   Region")
    print(f"  {'-'*22}   {'-'*12}   {'-'*10}   {'-'*10}")
    for r in records:
        print(f"  {classify_record(r)}")

    # Spot-check classifications
    laptop  = next(r for r in records if r.product == "Laptop")
    mouse   = next(r for r in records if r.product == "Mouse")
    keyboard = next(r for r in records if r.product == "Keyboard")

    assert "PREMIUM"               in classify_record(laptop)
    assert "SMALL"                 in classify_record(mouse)
    assert "HIGH-VALUE UNASSIGNED" in classify_record(keyboard)
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 3: Apply classification via mapPartitions (combines sections 3+4)
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 3 – Classify via mapPartitions (combines sec 3+4)")
    print(f"{separator}")

    def classify_partition(iterator):
        for record in iterator:
            label = classify_record(record)
            yield (record.order_id, label.split("|")[0].strip())

    classified_rdd = records_rdd.mapPartitions(classify_partition)
    classified = classified_rdd.collect()
    print("  (order_id, category):")
    for item in sorted(classified):
        print(f"    {item}")

    assert len(classified) == len(raw_data)
    print("  PASS")

    # ------------------------------------------------------------------
    # EXAMPLE 4: Spark Row objects — Spark-native alternative
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("  EXAMPLE 4 – Spark Row as lightweight case class alternative")
    print(f"{separator}")

    rows_rdd = sc.parallelize(raw_data).map(
        lambda t: Row(order_id=t[0], product=t[1], amount=t[2], region=t[3])
    )
    rows = rows_rdd.collect()
    print("  Row named field access:")
    sample = rows[0]
    print(f"    row.product  = {sample.product}")
    print(f"    row['amount']= {sample['amount']}")
    print(f"    row.asDict() = {sample.asDict()}")

    # Row -> DataFrame
    df = spark.createDataFrame(rows_rdd)
    print("\n  Row -> DataFrame schema:")
    df.printSchema()
    df.show(truncate=False)

    assert sample.product == "Laptop"
    assert df.count() == len(raw_data)
    print("  PASS")

    print(f"\n{separator}")
    print("  Section 4 – Case Classes & Pattern Matching: ALL EXAMPLES PASSED")
    print(f"{separator}\n")

    spark.stop()


if __name__ == "__main__":
    main()
