# Day 10 – DataFrame & Structured ETL Layer

## Goal
Build the full curated dataset by joining all four sources using the DataFrame API, apply advanced aggregations using window functions, handle dirty data properly, and write the final output as Parquet. By the end, you have a clean, enriched, analysis-ready dataset that the Day 11 optimizer and Day 12 streaming pipeline can consume.

---

### Task 1 – Load all four sources in their native formats

**What to do:**

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

# orders.csv — already defined in Day 8, reuse the schema
orders_df = (spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(f"{DATA_ROOT}/orders.csv"))

# customers.csv
customers_schema = StructType([
    StructField("customer_id",    StringType(), True),
    StructField("customer_name",  StringType(), True),
    StructField("email",          StringType(), True),
    StructField("phone",          StringType(), True),
    StructField("region",         StringType(), True),
    StructField("city",           StringType(), True),
    StructField("segment",        StringType(), True),
    StructField("loyalty_tier",   StringType(), True),
    StructField("join_date",      StringType(), True),
    StructField("is_active",      StringType(), True),
])
customers_df = (spark.read
    .option("header", "true")
    .schema(customers_schema)
    .csv(f"{DATA_ROOT}/customers.csv"))

# products.json — JSON-lines, Spark infers schema correctly here
# (all fields are consistently typed, no dirty data)
products_df = spark.read.json(f"{DATA_ROOT}/products.json")

# regions.csv — tiny, 5 rows
regions_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_ROOT}/regions.csv"))

print("Counts:", orders_df.count(), customers_df.count(),
      products_df.count(), regions_df.count())
# Expect: 10000  1000  200  5
```

**Why `inferSchema=True` is acceptable for `regions.csv` but not `orders.csv`:**  
`regions.csv` is 5 rows with zero dirty data. The schema inference cost is negligible and it correctly detects `target_revenue` as Long. `orders.csv` has 10,000 rows, dirty fields, and you need `discount` as String for the cleaning step — inference would get it wrong.

---

### Task 2 – Clean orders and join all datasets

**What to do:**  
Clean first, then join in a single chain. The join order matters: start with the largest table (orders) and join smaller tables to it.

```python
from pyspark.sql.functions import (
    col, when, broadcast, to_timestamp, coalesce, lit
)

# Step 1: Clean orders — fix dirty discount, cast, drop Cancelled/Returned
orders_clean = (orders_df
    .filter(col("status") == "Completed")
    .withColumn("discount",
        when(col("discount").isNull() | (col("discount") == ""), "0")
        .otherwise(col("discount")))
    .withColumn("discount",   col("discount").cast("double"))
    .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")))

# Step 2: Select only what we need from lookup tables
customers_slim = customers_df.select(
    "customer_id", "segment", "loyalty_tier"
)

products_slim = products_df.select(
    "product_id", "base_price", "stock_quantity"
)

# Step 3: Join — broadcast both small tables (products=200 rows, regions=5 rows)
enriched_df = (orders_clean
    .join(broadcast(products_slim), "product_id", "left")
    .join(broadcast(customers_slim), "customer_id", "left")
    .join(broadcast(regions_df.select("region", "target_revenue")), "region", "left"))

enriched_df.printSchema()
print("Enriched row count:", enriched_df.count())
```

**Why broadcast for all three lookup tables:**  
`products_df` = 200 rows, `customers_slim` = 1,000 rows, `regions_df` = 5 rows. Default Spark broadcast threshold is 10 MB; all three are well under 1 MB. Broadcasting means zero shuffle for these joins. The `.explain()` on `enriched_df` should show `BroadcastHashJoin` nodes — not `SortMergeJoin`. This is the correct join strategy for small-large table combinations.

---

### Task 3 – Advanced aggregations with window functions

**What to do:**  
Two aggregations that go beyond a simple `groupBy`.

**A. Monthly revenue trend per region (with month-over-month change):**

```python
from pyspark.sql.functions import sum as _sum, round as _round, lag
from pyspark.sql.window import Window

monthly_region = (enriched_df
    .groupBy("region", "order_year", "order_month")
    .agg(_round(_sum("total_amount"), 2).alias("monthly_revenue"))
    .orderBy("region", "order_year", "order_month"))

# Window: for each region, look at the previous month's revenue
region_window = Window.partitionBy("region").orderBy("order_year", "order_month")

monthly_with_delta = (monthly_region
    .withColumn("prev_month_revenue", lag("monthly_revenue", 1).over(region_window))
    .withColumn("mom_change_pct",
        _round(
            (col("monthly_revenue") - col("prev_month_revenue"))
            / col("prev_month_revenue") * 100,
        1))
)

monthly_with_delta.filter(col("region") == "East").show(15)
```

**Expected output (East region, dev dataset):**
```
+------+----------+-----------+---------------+------------------+--------------+
|region|order_year|order_month|monthly_revenue|prev_month_revenue|mom_change_pct|
+------+----------+-----------+---------------+------------------+--------------+
|East  |      2024|         10|    1678124.40 |              null|          null|
|East  |      2024|         11|    2295780.44 |       1678124.40 |          36.8|
+------+----------+-----------+---------------+------------------+--------------+
```

**Note — dev dataset has only 2 months:**  
The dev generator targets 10,000 orders across Oct–Dec 2024. November's 3× seasonal multiplier exhausts the 10,000-order cap before December is reached, so only Oct–Nov rows appear. The full dataset (500K orders, 2 years) shows the complete seasonal curve. The +36.8% Oct→Nov spike is the expected seasonality signal.

**B. Category rank within each region (dense_rank):**

```python
from pyspark.sql.functions import dense_rank, count

cat_region = (enriched_df
    .groupBy("region", "category")
    .agg(
        _round(_sum("total_amount"), 2).alias("revenue"),
        count("order_id").alias("orders"),
    ))

rank_window = Window.partitionBy("region").orderBy(col("revenue").desc())

cat_ranked = (cat_region
    .withColumn("rank_in_region", dense_rank().over(rank_window))
    .filter(col("rank_in_region") <= 3)   # top 3 categories per region
    .orderBy("region", "rank_in_region"))

cat_ranked.show(20)
```

**Expected output (15 rows — 3 per region):**
```
+-------+------------+----------+------+--------------+
|region |    category|   revenue|orders|rank_in_region|
+-------+------------+----------+------+--------------+
|Central| Electronics|842792.15 |   215|             1|
|Central|   Furniture|289698.20 |   113|             2|
|Central|    Clothing|135549.94 |   149|             3|
|East   | Electronics|1790324.92|   538|             1|
|East   |   Furniture| 752014.84|   390|             2|
|East   |   Groceries| 585136.27|   540|             3|
|North  | Electronics|1038119.24|   328|             1|
|North  |   Furniture| 325043.22|   228|             2|
|North  |    Clothing| 312821.25|   258|             3|
|South  | Electronics| 859173.34|   247|             1|
|South  |   Furniture| 289430.84|   173|             2|
|South  |      Beauty| 144782.45|   138|             3|
|West   | Electronics| 827915.52|   254|             1|
|West   |   Furniture| 330019.75|   171|             2|
|West   |   Groceries| 244052.92|   258|             3|
+-------+------------+----------+------+--------------+
```

**Why window functions instead of multiple groupBy passes:**  
The month-over-month delta requires comparing the current row to the previous row within a group — that is exactly what `lag()` does. Without a window function, you would need a self-join on the aggregated table, which creates an unnecessary shuffle. Window functions compute the delta in a single pass over the already-aggregated data.

**Key observation:**  
Electronics ranks #1 in every region. Furniture consistently ranks #2. The #3 slot varies: Groceries in East and West, Clothing in North and Central, Beauty in South — this is the regional flavour built into the data generator's customer distribution.

---

### Task 4 – Loyalty tier revenue analysis (structured column from a join)

**What to do:**  
The `loyalty_tier` column only exists in `customers.csv`. We brought it in via the join. Now use it.

```python
from pyspark.sql.functions import avg

loyalty_summary = (enriched_df
    .groupBy("loyalty_tier")
    .agg(
        count("order_id").alias("order_count"),
        _round(avg("total_amount"), 2).alias("avg_order_value"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
    )
    .orderBy(col("avg_order_value").desc()))

loyalty_summary.show()
```

**Expected output:**
```
+------------+-----------+---------------+-------------+
|loyalty_tier|order_count|avg_order_value|total_revenue|
+------------+-----------+---------------+-------------+
|    Platinum|        637|        1753.01|  1116666.56 |
|      Bronze|       3583|        1480.33|  5304036.58 |
|        Gold|       1393|        1443.49|  2010787.14 |
|      Silver|       2413|        1201.61|  2899475.54 |
+------------+-----------+---------------+-------------+
```

**Expected observation:**  
Platinum has the highest `avg_order_value` (1753.01) with only 637 orders (~7.9% of completed orders). Bronze dominates total revenue (5.3 M) through sheer volume — 3583 orders, 44.6% of all completed orders. This is the classic high-value vs high-volume distinction: Platinum = high spend per transaction, Bronze = high number of transactions.

**Why this required the join:**  
`orders.csv` does not contain `loyalty_tier`. It only has `customer_id`. The join brought the dimension in. This is the same pattern used in every star-schema data warehouse: fact table (orders) + dimension table (customers) joined at query time.

---

### Task 5 – Write curated output as Parquet

**What to do:**  
Write three output datasets, each partitioned by a meaningful column.

```python
OUTPUT_ROOT = "/opt/spark/jobs/capstone_data/dev/output"

# 1. Full enriched orders — partitioned by region for Day 11 filter pushdown demo
(enriched_df
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(f"{OUTPUT_ROOT}/enriched_orders.parquet"))

# 2. Monthly revenue trend
(monthly_with_delta
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_ROOT}/monthly_revenue_trend.parquet"))

# 3. Category ranking per region
(cat_ranked
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_ROOT}/category_rank_by_region.parquet"))

# Verify partition layout for enriched_orders
import os
for name in os.listdir(f"{OUTPUT_ROOT}/enriched_orders.parquet"):
    print(name)
# Expect: region=East/  region=North/  region=South/  region=West/  region=Central/
```

**Why partition by `region`:**  
The most common filter in Day 8–11 is `WHERE region = 'East'`. When the Parquet files are partitioned by `region`, a query with that filter reads only the `region=East/` directory — all other partitions are skipped entirely. This is partition pruning. For the dev dataset the gain is modest; for the full 500K-order dataset it becomes a ~5× read reduction for region-specific queries.

---

### Task 6 – DataFrame vs RDD: compare explain output for the same query

**What to do:**  
Run `regional_summary.explain()` (from Day 8, Task 5) and compare it to the RDD lineage printed by `toDebugString()` (from Day 9, Task 5). Both compute revenue by region. Note the differences.

**Discussion points:**

| Aspect | RDD | DataFrame |
|---|---|---|
| **Catalyst optimizer** | No — you wrote exactly what executes | Yes — rewrites the plan for efficiency |
| **Tungsten execution** | Python objects, GC pressure | Off-heap binary format, no Python overhead |
| **Filter pushdown** | Manual — you wrote `.filter()` before `.map()` | Automatic — Catalyst moves filter before scan |
| **Shuffle minimization** | `reduceByKey` does partial aggregation | AQE (Day 11) can dynamically reduce shuffle partitions |
| **When to use RDD** | Custom partitioners, low-level fault tolerance control, arbitrary Python transformations with no SQL equivalent | Avoid for standard ETL — DataFrame is always faster |

The conclusion to draw: RDD is not "lower level and therefore faster". RDD is lower level and **avoids the optimizations** that make DataFrame faster. RDD's one genuine advantage is when you need compute logic that cannot be expressed in expressions — like the custom partitioner in Day 9, Task 2.

---

### Day 10 Deliverables

| Artifact | What it looks like |
|---|---|
| Enriched orders Parquet | Partitioned by region, 5 sub-directories |
| Monthly revenue trend table | Month-over-month % change, Nov spike visible |
| Category rank table | Top 3 categories per region, 15 rows total |
| Loyalty tier table | Platinum highest avg_order_value |
| DataFrame vs RDD comparison notes | Table above, written in your own words |
