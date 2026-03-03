"""
Challenge 2: Slow Window Function
==================================
Spark Version: 4.1.1
Topic: Window functions cause shuffle + sort -- optimize by pre-partitioning
       and filtering early.

Problem:
    window().partitionBy("dept").orderBy("salary") forces Spark to:
      1. Shuffle all rows so that each dept lands on one executor.
      2. Sort rows within each partition by salary.
    Both operations are expensive, especially when applied to a large dataset
    or when the PARTITION BY column has high cardinality.

Why it happens:
    Unlike a plain groupBy which only needs a hash partition, a window
    function with orderBy needs a RANGE or ROWS frame, so a sort is mandatory
    on top of the shuffle.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/02_slow_window_function.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("02_slow_window_function")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create dataset: employees across departments
# ---------------------------------------------------------------------------
data = []
depts = ["Engineering", "Sales", "HR", "Finance", "Marketing"]
for dept_idx, dept in enumerate(depts):
    for i in range(20):
        data.append((f"emp_{dept_idx}_{i}", dept, 30000 + (i * 1000) + dept_idx * 500))

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name",   StringType(),  False),
    StructField("dept",   StringType(),  False),
    StructField("salary", IntegerType(), False),
])

df = spark.createDataFrame(data, schema)
print(f"Total rows: {df.count()}")

# ---------------------------------------------------------------------------
# 2. Baseline window function -- shuffle + sort in plan
# ---------------------------------------------------------------------------
window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))

df_ranked = df.withColumn("rank", F.rank().over(window_spec))

print("\n--- Baseline window function (top-3 per dept) ---")
df_ranked.filter(F.col("rank") <= 3).orderBy("dept", "rank").show(20, truncate=False)

print("\n=== EXPLAIN (baseline) ===")
# Look for:
#   Exchange hashpartitioning(dept, N)   <-- shuffle
#   Sort [dept ASC, salary DESC]         <-- sort within partition
df_ranked.explain("formatted")

# ---------------------------------------------------------------------------
# 3. Filter BEFORE the window -- reduce data volume early
#    If your business logic allows it, push filters above the window spec
#    so the window operates on fewer rows.
# ---------------------------------------------------------------------------
df_filtered_first = df.filter(F.col("salary") >= 35000)

df_ranked_opt = df_filtered_first.withColumn("rank", F.rank().over(window_spec))

print("\n--- Optimized: filter before window ---")
df_ranked_opt.filter(F.col("rank") <= 3).orderBy("dept", "rank").show(20, truncate=False)

print("\n=== EXPLAIN (filter first) ===")
# Look for: Filter node appearing BEFORE the Exchange node
df_ranked_opt.explain("formatted")

# ---------------------------------------------------------------------------
# 4. Pre-partition by the window PARTITION BY key
#    repartitionByRange aligns the physical data layout with the window spec,
#    which can eliminate or reduce the Exchange in the window physical plan.
# ---------------------------------------------------------------------------
df_pre_partitioned = df.repartitionByRange(8, "dept", F.desc("salary"))

df_ranked_pre = df_pre_partitioned.withColumn("rank", F.rank().over(window_spec))

print("\n--- Optimized: repartitionByRange before window ---")
df_ranked_pre.filter(F.col("rank") <= 3).orderBy("dept", "rank").show(20, truncate=False)

print("\n=== EXPLAIN (repartitionByRange) ===")
# Look for: RangePartitioning exchange replacing the hashpartitioning one,
# or a reduced sort step because data is already range-ordered.
df_ranked_pre.explain("formatted")

print("""
Key Takeaway
------------
- A window function always introduces Exchange (shuffle) + Sort.
- Push filters before the window to reduce the rows the window sees.
- repartitionByRange() can align physical partitioning with the window
  spec, potentially saving a re-sort step inside the window operator.
- Keep 'spark.sql.shuffle.partitions' tuned -- too many small partitions
  hurt window performance due to overhead per sort-merge step.
""")

spark.stop()
