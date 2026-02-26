"""
Hello Spark - A simple PySpark job to verify the cluster is working.
Spark version: 4.1.1

Run from the spark-master container:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/hello_spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def main():
    spark = (
        SparkSession.builder
        .appName("HelloSpark")
        .getOrCreate()
    )

    print(f"\n{'='*60}")
    print(f"  Apache Spark {spark.version} - Hello World")
    print(f"{'='*60}\n")

    # --- 1. Basic RDD word count ---
    sc = spark.sparkContext
    words = sc.parallelize(["hello", "spark", "hello", "world", "spark", "spark"])
    word_counts = (
        words
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    print("Word counts (RDD):")
    for word, cnt in sorted(word_counts):
        print(f"  {word}: {cnt}")

    # --- 2. DataFrame API ---
    data = [
        ("Alice",  "Engineering", 95000),
        ("Bob",    "Marketing",   72000),
        ("Carol",  "Engineering", 88000),
        ("David",  "HR",          65000),
        ("Eve",    "Engineering", 102000),
        ("Frank",  "Marketing",   76000),
    ]
    schema = ["name", "department", "salary"]
    df = spark.createDataFrame(data, schema)

    print("\nEmployees DataFrame:")
    df.show()

    print("Average salary by department:")
    df.groupBy("department") \
      .agg(avg(col("salary")).alias("avg_salary"), count("*").alias("headcount")) \
      .orderBy("department") \
      .show()

    # --- 3. Spark SQL ---
    df.createOrReplaceTempView("employees")
    top_earners = spark.sql("""
        SELECT name, department, salary
        FROM employees
        WHERE salary >= 88000
        ORDER BY salary DESC
    """)
    print("Top earners (SQL):")
    top_earners.show()

    print("Job completed successfully!\n")
    spark.stop()


if __name__ == "__main__":
    main()
