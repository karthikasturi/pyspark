# Day 8 – Foundation & Architecture

## Goal
Load the orders dataset, run basic aggregations, and connect what you wrote (Python code) to what Spark actually does (execution plan, shuffle stage, Spark UI).

---

### Task 1 – Load orders with an explicit schema

**What to do:**  
Define a `StructType` for orders.csv and load using `.schema(...)` instead of `inferSchema=True`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = SparkSession.builder.appName("RetailPulse-Day8").getOrCreate()

orders_schema = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("subcategory",    StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("segment",        StringType(),  True),
    StructField("order_date",     StringType(),  True),
    StructField("order_year",     IntegerType(), True),
    StructField("order_month",    IntegerType(), True),
    StructField("order_quarter",  IntegerType(), True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("discount",       StringType(),  True),  # intentionally String — dirty data
    StructField("total_amount",   DoubleType(),  True),
    StructField("status",         StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("is_high_value",  StringType(),  True),
])

DATA_ROOT = "/opt/spark/jobs/capstone_data/dev/batch"

orders_df = (spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(f"{DATA_ROOT}/orders.csv"))

print(f"Row count: {orders_df.count()}")   # expect 10,000
orders_df.printSchema()
```

**Why `discount` is StringType here:**  
The generator deliberately leaves ~0.5% of discount values as empty strings `""`. If you use `DoubleType` with `inferSchema`, those rows silently become `null`. Keeping it `String` first lets you **see and decide** what to do with dirty data before committing to a type cast — which is Task 2.

**Why explicit schema matters:**  
`inferSchema=True` tells Spark to do a full extra pass over the file just to guess types. In production with millions of rows on distributed storage, this doubles read time. Explicit schema = one pass.

---

### Task 2 – Data quality check before any aggregation

**What to do:**  
Three specific checks on the raw loaded data.

```python
from pyspark.sql.functions import col, count, when

total      = orders_df.count()
completed  = orders_df.filter(col("status") == "Completed").count()

# discount is loaded as String — empty string = dirty record
dirty_disc = orders_df.filter(
    col("discount").isNull() | (col("discount") == "")
).count()

print(f"Total orders       : {total}")
print(f"Completed orders   : {completed}  ({100*completed/total:.1f}%)")
print(f"Dirty discount rows: {dirty_disc}  ({100*dirty_disc/total:.2f}%)")
```

**Expected output:**
```
Total orders       : 10000
Completed orders   : 8026  (80.3%)
Dirty discount rows: 41    (0.41%)
```

**Why bother:**  
Those 41 rows with a missing discount would produce wrong `total_amount` averages if you aggregated without handling them first. Finding nulls **before** aggregating is a non-negotiable first step in any real pipeline.

---

### Task 3 – Regional sales summary

**What to do:**  
Filter to `Completed` only, clean `discount`, then aggregate by region.

```python
from pyspark.sql.functions import sum as _sum, avg, count, round as _round, lit

# Fix dirty data: replace empty string discount with "0", then cast to Double
orders_clean = (orders_df
    .filter(col("status") == "Completed")
    .withColumn("discount",
        when(col("discount").isNull() | (col("discount") == ""), "0")
        .otherwise(col("discount")))
    .withColumn("discount", col("discount").cast("double")))

# Compute overall revenue for percentage calculation
overall_revenue = orders_clean.agg(_sum("total_amount").alias("total")).collect()[0]["total"]

regional_summary = (orders_clean
    .groupBy("region")
    .agg(
        count("order_id").alias("total_orders"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("pct_of_total",
        _round(col("total_revenue") / lit(overall_revenue) * 100, 1))
    .orderBy(col("total_revenue").desc()))

regional_summary.show()
```

**Expected output:**
```
+-------+------------+-------------+---------------+------------+
|region |total_orders|total_revenue|avg_order_value|pct_of_total|
+-------+------------+-------------+---------------+------------+
|East   |        2834|   3973904.84|        1402.22|        35.1|
|North  |        1691|   2353210.13|        1391.61|        20.8|
|West   |        1289|   1867649.18|        1448.91|        16.5|
|South  |        1248|   1612675.72|        1292.21|        14.2|
|Central|         964|   1523525.95|        1580.42|        13.4|
+-------+------------+-------------+---------------+------------+
```

**Note:** East's 35.1% share is the built-in skew. Central's 964 orders vs East's 2834 is the imbalance you will salt in Day 11, Task 3.

**Why `pct_of_total`:**  
This single column will be your reference point on Day 11 when you observe data skew. East at 35% is a built-in characteristic of the dataset — it is not an accident.

---

### Task 4 – Category revenue breakdown (top 5)

**What to do:**

```python
category_summary = (orders_clean
    .groupBy("category")
    .agg(
        count("order_id").alias("order_count"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
    )
    .orderBy(col("total_revenue").desc())
    .limit(5))

category_summary.show()
```

**Expected output:**
```
+------------+-----------+-------------+
|    category|order_count|total_revenue|
+------------+-----------+-------------+
| Electronics|       1582|  5358325.17 |
|   Furniture|       1075|  1986206.85 |
|    Clothing|       1267|  1131868.46 |
|   Groceries|       1537|  1130913.72 |
|      Sports|        842|   642053.09 |
+------------+-----------+-------------+
```

**Why:**  
This is the "category-level metrics" deliverable. Electronics at 5.4 M is 2.7× the next category — this signals it dominates order value, not just volume. It also confirms `products.json` broadcast join on Day 10 is worth doing (200-row lookup, high join frequency = classic broadcast candidate).

---

### Task 5 – Read the execution plan for the regional aggregation

**What to do:**  
Before calling `.show()`, call `.explain()` on `regional_summary`.

```python
regional_summary.explain(mode="extended")
```

**What to look for in the output:**

1. **`== Parsed Logical Plan ==`** — what you wrote in Python
2. **`== Analyzed Logical Plan ==`** — types resolved
3. **`== Optimized Logical Plan ==`** — filters pushed down before aggregation
4. **`== Physical Plan ==`** — look for this specific node:
   ```
   Exchange hashpartitioning(region, 200)
   ```
   This is the shuffle. Spark must redistribute all rows across the network so that all records for "East" land on the same executor before it can sum them.

5. Count the number of `Exchange` nodes. For a single `groupBy`, you should see exactly **one**.

**Why this matters:**  
Every slow Spark job has too many `Exchange` nodes. Learning to read this plan on Day 8 — with a simple query you just wrote — makes optimization on Day 11 concrete rather than abstract.

---

### Task 6 – Write regional summary to Parquet

**What to do:**

```python
OUTPUT_ROOT = "/opt/spark/jobs/capstone_data/dev/output"

regional_summary.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/regional_summary.parquet")

# Verify: read it back and count
spark.read.parquet(f"{OUTPUT_ROOT}/regional_summary.parquet").show()
```

**Why Parquet and not CSV:**  
The regional summary has 5 rows and 4 columns. On this scale it doesn't matter. But the rule is established now: all pipeline outputs are Parquet. When Day 10 reads this back to join with target revenue from regions.csv, it will read only the columns it needs — that is the columnar advantage.

---

### Day 8 Deliverables

| Artifact | What it looks like |
|---|---|
| Execution plan printout | Text showing `Exchange hashpartitioning(region, 200)` |
| Regional summary table | 5 rows, East at ~35% |
| Category top-5 table | 5 rows sorted by revenue |
| Dirty data count | 41 rows with empty discount (0.41%) |
| Parquet file on disk | `output/regional_summary.parquet` |
