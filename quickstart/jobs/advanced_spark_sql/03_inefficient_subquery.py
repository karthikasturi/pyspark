"""
Challenge 3: Inefficient Subquery
===================================
Spark Version: 4.1.1
Topic: Correlated / uncorrelated subqueries in Spark SQL can generate extra
       scan + shuffle stages. Rewrite using joins or pre-aggregations.

Problem:
    Writing a WHERE col IN (SELECT ...) or a scalar subquery causes Spark to
    execute the inner query as a separate job and then apply it via a
    BroadcastNestedLoopJoin or a full shuffle join, depending on size.
    This can silently hide extra stages in the query plan.

Why it happens:
    Spark SQL translates subqueries into join operations internally.
    An uncorrelated IN subquery becomes a LEFT SEMI JOIN.
    A scalar subquery re-executes for every outer row unless Spark can prove
    the subquery returns exactly one row and hoists it.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/advanced_spark_sql/03_inefficient_subquery.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("03_inefficient_subquery")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Create datasets
# ---------------------------------------------------------------------------
orders_data = [(i, f"customer_{i % 10}", float(100 + i * 3)) for i in range(200)]
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])

# "High value" customers: those with total spend > 1000
# We will identify them via a subquery vs. a pre-aggregation join
high_value_data = [
    (f"customer_{c}", float(sum(100 + i * 3 for i in range(200) if i % 10 == c)))
    for c in range(10)
]
customer_totals_df = spark.createDataFrame(high_value_data, ["customer_id", "total_spend"])

orders_df.createOrReplaceTempView("orders")
customer_totals_df.createOrReplaceTempView("customer_totals")

print(f"Orders rows: {orders_df.count()}")
print(f"Customer totals rows: {customer_totals_df.count()}")

# ---------------------------------------------------------------------------
# 2. Baseline: SQL subquery using IN (SELECT ...)
# ---------------------------------------------------------------------------
subquery_sql = """
    SELECT *
    FROM orders
    WHERE customer_id IN (
        SELECT customer_id
        FROM customer_totals
        WHERE total_spend > 1000
    )
"""

df_subquery = spark.sql(subquery_sql)

print("\n--- Baseline: IN subquery result (first 5) ---")
df_subquery.show(5)

print("\n=== EXPLAIN (subquery) ===")
# Look for:
#   BroadcastHashJoin (Existence join) or LeftSemi join
#   The inner SELECT is shown as a separate subquery plan tree
df_subquery.explain("formatted")

# ---------------------------------------------------------------------------
# 3. Optimized version A: pre-aggregate then join explicitly
#    We compute the high-value customer list once, then join directly.
#    This makes the optimization intent explicit and gives Spark less guesswork.
# ---------------------------------------------------------------------------
high_value_customers = customer_totals_df.filter(F.col("total_spend") > 1000).select("customer_id")

df_pre_agg_join = orders_df.join(
    F.broadcast(high_value_customers),   # small lookup -- broadcast it
    on="customer_id",
    how="inner",
)

print("\n--- Optimized A: pre-aggregation + broadcast join ---")
df_pre_agg_join.show(5)

print("\n=== EXPLAIN (optimized A) ===")
# Look for:
#   BroadcastHashJoin instead of LeftSemi subquery plan
#   No nested subquery stage -- single scan of each table
df_pre_agg_join.explain("formatted")

# ---------------------------------------------------------------------------
# 4. Optimized version B: left semi join (equivalent to IN, but explicit)
# ---------------------------------------------------------------------------
df_semi_join = orders_df.join(
    F.broadcast(high_value_customers),
    on="customer_id",
    how="left_semi",
)

print("\n--- Optimized B: explicit left semi join with broadcast ---")
df_semi_join.show(5)

print("\n=== EXPLAIN (optimized B) ===")
df_semi_join.explain("formatted")

print("""
Key Takeaway
------------
- SQL IN (SELECT ...) is syntactic sugar -- Spark rewrites it as a join.
- Writing the join explicitly gives you full control over join strategy
  (broadcast, sort-merge, shuffle-hash).
- Pre-aggregating before the join reduces the right-side dataset early,
  reducing the bytes moved during broadcast or shuffle.
- Always broadcast the smaller side: spark.sql.autoBroadcastJoinThreshold
  (default 10 MB) controls automatic broadcast.  For custom sizes, use
  F.broadcast() explicitly.
""")

spark.stop()
