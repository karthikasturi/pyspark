"""
Challenge 1: Unexpected Shuffle in Aggregation
===============================================
Spark Version: 4.1.1
Topic: Understanding and reducing shuffle caused by groupBy aggregation.

Problem:
    A groupBy().agg() triggers a full shuffle across the cluster because
    Spark must co-locate all rows with the same key on the same executor
    before it can aggregate them.

Why it happens:
    Data is distributed by row index at read time. When you group by a
    specific key, rows for the same key can sit on completely different
    executors. Spark moves (shuffles) them over the network -- this is
    the most expensive operation in Spark.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/01_shuffle_in_aggregation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("01_shuffle_in_aggregation")
    # Limit shuffle partitions to keep output readable in local mode
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create a small skewed dataset
#    - 1_000 rows
#    - "category" column is heavily skewed: 90 % of rows share category "A"
# ---------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, LongType

data = (
    [("A", i) for i in range(900)]   # 90 % skewed towards "A"
    + [("B", i) for i in range(50)]
    + [("C", i) for i in range(30)]
    + [("D", i) for i in range(20)]
)

schema = StructType([
    StructField("category", StringType(), False),
    StructField("value",    LongType(),   False),
])

df = spark.createDataFrame(data, schema)

print(f"Total rows: {df.count()}")
print("Partition count before any repartition:", df.rdd.getNumPartitions())

# ---------------------------------------------------------------------------
# 2. Baseline aggregation -- causes a shuffle
# ---------------------------------------------------------------------------
agg_df = df.groupBy("category").agg(
    F.count("value").alias("cnt"),
    F.sum("value").alias("total"),
)

print("\n--- Baseline groupBy aggregation ---")
agg_df.show()

print("\n=== EXPLAIN (baseline) ===")
# Look for: Exchange hashpartitioning(category, 8)
# That Exchange node is the shuffle.
agg_df.explain("formatted")

# ---------------------------------------------------------------------------
# 3. Optimized version -- pre-partition on the groupBy key
#    Repartitioning by "category" before the aggregation ensures that all
#    rows for the same category are already on the same executor.  The
#    subsequent groupBy no longer needs a full Exchange (shuffle) node in
#    the physical plan.
# ---------------------------------------------------------------------------
df_repartitioned = df.repartition(8, "category")

agg_df_opt = df_repartitioned.groupBy("category").agg(
    F.count("value").alias("cnt"),
    F.sum("value").alias("total"),
)

print("\n--- Optimized groupBy aggregation (pre-repartitioned) ---")
agg_df_opt.show()

print("\n=== EXPLAIN (optimized) ===")
# Look for: absence of Exchange before HashAggregate, OR
# a RoundRobinPartitioning Exchange from repartition() -- but no
# additional hashpartitioning Exchange for the aggregation itself.
agg_df_opt.explain("formatted")

# ---------------------------------------------------------------------------
# 4. Key takeaway
# ---------------------------------------------------------------------------
print("""
Key Takeaway
------------
- Baseline plan contains: Exchange hashpartitioning(category, N)
- Optimized plan moves the shuffle to an explicit repartition() step.
  When chaining multiple aggregations on the same key this pays off because
  the data is already co-located and each subsequent aggregation avoids
  an extra Exchange node.
- For a single aggregation the win is mainly about controlling WHEN the
  shuffle happens and on HOW MANY partitions, which matters for skew.
""")

spark.stop()
