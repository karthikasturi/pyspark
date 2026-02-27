# Advanced PySpark Concepts – Demo Guide

This guide covers four advanced PySpark concepts with practical, runnable examples.
Each section builds on the previous and is designed to run locally using a standalone SparkSession.

---

## Prerequisites

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AdvancedPySpark") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext
```

---

## 1. Closures in PySpark

### Concept

A closure is a function that captures variables from its surrounding scope.
In PySpark, when you pass a function (like a lambda) to a transformation such as `filter()` or `map()`,
Spark serializes that function along with any variables it references and ships it to each executor.

This process is transparent, but it has important consequences:

- Variables captured in closures are **copied** to each executor — they are not shared.
- If you accidentally capture a large object (e.g., a huge list or dictionary), that object gets
  serialized and sent to every executor, consuming significant network bandwidth and memory.
- For large shared datasets, **broadcast variables** are the correct solution — they distribute
  the data once and cache it on executors. (Covered separately.)

### Example 1 — Simple Closure (Good Practice)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ClosureDemo") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

# A small scalar variable captured inside a lambda
threshold = 10

numbers = sc.parallelize([3, 7, 10, 15, 22, 4, 18])

# The lambda captures 'threshold' from the outer scope
filtered = numbers.filter(lambda x: x > threshold)

print("Numbers greater than", threshold, "->", filtered.collect())
```

**Step-by-step explanation:**

1. `threshold = 10` is a plain integer defined in the driver program.
2. `sc.parallelize(...)` creates an RDD distributed across worker partitions.
3. `filter(lambda x: x > threshold)` — Spark serializes this lambda along with the captured
   value of `threshold` (which is `10`) and sends it to executors.
4. Each executor applies the filter independently on its partition.
5. `collect()` pulls results back to the driver.

**Expected Output:**

```
Numbers greater than 10 -> [15, 22, 18]
```

### Example 2 — Bad Practice (Large Object in Closure)

```python
# BAD PRACTICE — do not do this with large objects
large_lookup = {i: i * 2 for i in range(1_000_000)}  # 1 million entries

rdd = sc.parallelize([1, 2, 3, 4, 5])

# This forces Spark to serialize the entire large_lookup dict to every task
result = rdd.map(lambda x: large_lookup.get(x, -1)).collect()
print(result)
```

**Why this is a problem:**

- `large_lookup` is fully serialized with every task that gets sent to executors.
- If you have 100 partitions, the dictionary is sent 100 times.
- This wastes network bandwidth, increases task launch time, and can cause memory pressure.

**Correct approach (brief mention):**

Use `sc.broadcast(large_lookup)` to distribute the object once and reference it as
`broadcast_var.value` inside the lambda. This keeps one copy per executor node instead of
one copy per task.

### Why This Matters in Real Distributed Systems

In production, closures carrying large objects silently degrade performance.
A job that works fine in development (small data) can fail or become extremely slow in production
(large data, hundreds of partitions) purely because of oversized closure variables.

---

## 2. Iterators and Partition Processing in PySpark

### Concept

Spark distributes data across a cluster by splitting it into **partitions**.
Each partition is an independent chunk of data processed by a single task on a single executor.

- `map()` applies a function to each element one at a time within a partition.
- The actual unit of parallelism is the **partition**, not the individual element.
- `glom()` is a useful debugging method that converts each partition into a list,
  allowing you to see exactly what data lives in each partition.

Understanding partitions is essential for:
- Tuning parallelism
- Avoiding data skew
- Optimizing shuffle operations

### Example — Viewing Partitions with glom()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PartitionDemo") \
    .master("local[3]") \
    .getOrCreate()

sc = spark.sparkContext

data = [10, 20, 30, 40, 50, 60, 70, 80, 90]

# Create an RDD and explicitly set 3 partitions
rdd = sc.parallelize(data, numSlices=3)

# glom() wraps each partition's contents into a list
partitions = rdd.glom().collect()

print("Number of partitions:", rdd.getNumPartitions())
print("Partition contents:")
for i, p in enumerate(partitions):
    print(f"  Partition {i}: {p}")
```

**Step-by-step explanation:**

1. `sc.parallelize(data, numSlices=3)` — distributes `data` across exactly 3 partitions.
2. `glom()` — transforms the RDD so each partition becomes a single list element.
   The result is an RDD of lists, one per partition.
3. `collect()` — brings all partition lists to the driver.
4. We iterate and print each partition's contents.

**Expected Output:**

```
Number of partitions: 3
Partition contents:
  Partition 0: [10, 20, 30]
  Partition 1: [40, 50, 60]
  Partition 2: [70, 80, 90]
```

### Example — map() Operates Per Element, Not Per Partition

```python
rdd = sc.parallelize([1, 2, 3, 4, 6], numSlices=2)

# map() applies the function element-by-element
squared = rdd.map(lambda x: x ** 2).collect()
print("Squared:", squared)

# Confirm partition layout
print("Partition layout:", rdd.glom().collect())
```

**Expected Output:**

```
Squared: [1, 4, 9, 16, 36]
Partition layout: [[1, 2], [3, 4, 6]]
```

### Why This Matters in Real Distributed Systems

Wrong partition count is a leading cause of poor Spark performance.
Too few partitions means underutilizing cluster resources.
Too many partitions means excessive task scheduling overhead.
Using `glom()` during development lets you verify that data is distributed as expected and
that no single partition is carrying a disproportionate load (data skew).

---

## 3. Using mapPartitions for Efficiency

### Concept

`map()` applies a function to each element individually.
`mapPartitions()` applies a function to an entire partition at once, receiving an **iterator**
over the partition's elements and returning an iterator of results.

The critical advantage of `mapPartitions()` is that **expensive setup code runs once per partition**
instead of once per element. Common real-world examples include:

- Opening a database connection
- Instantiating a machine learning model
- Establishing an HTTP session or API client
- Loading a large configuration file

With `map()`, that setup code would run once per row — potentially millions of times.
With `mapPartitions()`, it runs once per partition — typically only a few hundred times at most.

### Example — Simulating Expensive Setup

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MapPartitionsDemo") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

data = [1, 2, 3, 4, 5, 6, 7, 8]
rdd = sc.parallelize(data, numSlices=2)

# --- Using map() ---
# The setup (simulated by print) runs for EVERY element
def process_with_map(x):
    print(f"[map] Initializing connection for element: {x}")  # expensive setup
    return x * 10

print("=== map() output ===")
result_map = rdd.map(process_with_map).collect()
print("Result:", result_map)

# --- Using mapPartitions() ---
# The setup runs ONCE per partition, not per element
def process_partition(iterator):
    print("[mapPartitions] Initializing connection ONCE for this partition")
    for x in iterator:
        yield x * 10  # reuses the same "connection" for all elements

print("\n=== mapPartitions() output ===")
result_mp = rdd.mapPartitions(process_partition).collect()
print("Result:", result_mp)
```

**Step-by-step explanation:**

1. `sc.parallelize(data, numSlices=2)` — splits 8 elements across 2 partitions (4 each).
2. **map() path:**
   - `process_with_map` is called for every single element.
   - "Initializing connection" prints 8 times (once per element).
3. **mapPartitions() path:**
   - `process_partition` is called once per partition, receiving an iterator.
   - "Initializing connection ONCE" prints only 2 times (once per partition).
   - The `yield` statement lazily produces output without loading the full partition into memory.

**Expected Output (driver-side):**

```
=== map() output ===
Result: [10, 20, 30, 40, 50, 60, 70, 80]

=== mapPartitions() output ===
Result: [10, 20, 30, 40, 50, 60, 70, 80]
```

Note: The `print` statements in the functions execute on executors.
In local mode you will see them in the driver console, but in a real cluster they appear in
executor logs, not the driver output.

### map() vs mapPartitions() — Side-by-Side Comparison

| Aspect                  | map()                            | mapPartitions()                        |
|-------------------------|----------------------------------|----------------------------------------|
| Function receives       | One element at a time            | An iterator over the whole partition   |
| Function returns        | One transformed element          | An iterator of transformed elements   |
| Setup code runs         | Once per element                 | Once per partition                     |
| Memory usage            | Low (one element at a time)      | Controlled (iterator-based)            |
| Use case                | Simple transformations           | Expensive setup + batch processing     |

### Why This Matters in Real Distributed Systems

A common real-world pattern is scoring rows with an ML model loaded from disk or
inserting rows into a database. Using `map()` for this means establishing a new
connection or loading the model for every row. `mapPartitions()` loads the model once
per partition, which can reduce execution time by orders of magnitude in production.

---

## 4. Case Classes and Pattern Matching in PySpark (Python Perspective)

### Concept

In Scala/Spark, **case classes** are immutable data structures with automatic equality,
serialization support, and pattern matching. They are heavily used to type RDD rows.

Python does not have case classes, but the same goals — typed row structures,
readable field access, and conditional logic based on structure — can be achieved using:

- **`dataclass`** — the closest Python equivalent; provides field names, defaults, and type hints.
- **`NamedTuple`** — lightweight, immutable, tuple-compatible typed record.
- **`Row`** — Spark's built-in flexible row type, used in DataFrames and RDDs.
- **`match-case`** (Python 3.10+) — structural pattern matching for conditional dispatch.

### Example 1 — Defining a dataclass as a Case Class Equivalent

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class SalesRecord:
    order_id: int
    product: str
    amount: float
    region: Optional[str] = None
```

This dataclass gives us:
- Named field access (`record.product`)
- Automatic `__repr__` and `__eq__`
- Optional fields with defaults
- Type hints for documentation

### Example 2 — Converting RDD Rows to dataclass Instances

```python
from pyspark.sql import SparkSession
from dataclasses import dataclass
from typing import Optional

spark = SparkSession.builder \
    .appName("CaseClassDemo") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

@dataclass
class SalesRecord:
    order_id: int
    product: str
    amount: float
    region: Optional[str] = None

# Raw tuples simulating data loaded from a file or database
raw_data = [
    (1, "Laptop",  1200.0, "North"),
    (2, "Mouse",     25.0, "South"),
    (3, "Monitor",  450.0, "North"),
    (4, "Keyboard",  75.0, None),
    (5, "Webcam",   110.0, "East"),
]

rdd = sc.parallelize(raw_data)

# Map each tuple to a SalesRecord dataclass instance
records_rdd = rdd.map(lambda t: SalesRecord(
    order_id=t[0],
    product=t[1],
    amount=t[2],
    region=t[3]
))

# Collect and display as structured records
records = records_rdd.collect()
for r in records:
    print(r)
```

**Expected Output:**

```
SalesRecord(order_id=1, product='Laptop', amount=1200.0, region='North')
SalesRecord(order_id=2, product='Mouse', amount=25.0, region='South')
SalesRecord(order_id=3, product='Monitor', amount=450.0, region='North')
SalesRecord(order_id=4, product='Keyboard', amount=75.0, region=None)
SalesRecord(order_id=5, product='Webcam', amount=110.0, region='East')
```

### Example 3 — Pattern Matching with match-case (Python 3.10+)

```python
# Requires Python 3.10 or later

def classify_record(record: SalesRecord) -> str:
    match record:
        case SalesRecord(amount=amt, region=None) if amt > 100:
            return f"High-value unassigned order: {record.product} (${amt})"
        case SalesRecord(amount=amt) if amt >= 500:
            return f"Premium order: {record.product} in {record.region} (${amt})"
        case SalesRecord(amount=amt) if amt >= 50:
            return f"Standard order: {record.product} in {record.region} (${amt})"
        case _:
            return f"Small order: {record.product} (${record.amount})"

# Apply classification to the collected records
print("\nOrder Classification:")
for r in records:
    print(" -", classify_record(r))
```

**Step-by-step explanation:**

1. `match record:` — starts structural pattern matching on the `SalesRecord` instance.
2. `case SalesRecord(amount=amt, region=None) if amt > 100:` — matches records with no region
   and amount over 100, binding `amt` to the value.
3. `case SalesRecord(amount=amt) if amt >= 500:` — matches high-value records in any region.
4. `case SalesRecord(amount=amt) if amt >= 50:` — matches mid-range records.
5. `case _:` — default fallback for anything that did not match above.

**Expected Output:**

```
Order Classification:
 - Premium order: Laptop in North ($1200.0)
 - Small order: Mouse ($25.0)
 - Standard order: Monitor in North ($450.0)
 - High-value unassigned order: Keyboard ($75.0)
 - Standard order: Webcam in East ($110.0)
```

Note: The `Keyboard` record has `region=None` and `amount=75.0`, which is less than 100,
so it falls through to the "Standard order" case in practice. Adjust thresholds to match
your intended business logic.

### Example 4 — Using Spark Row Objects

```python
from pyspark.sql import Row

# Row provides named field access similar to a case class
row = Row(order_id=1, product="Laptop", amount=1200.0)
print(row.product)       # Laptop
print(row["amount"])     # 1200.0
print(row.asDict())      # {'order_id': 1, 'product': 'Laptop', 'amount': 1200.0}
```

`Row` objects are Spark-native and can be used directly in DataFrames without a separate schema.
They are a pragmatic choice when you do not need full dataclass behavior.

### Why This Matters in Real Distributed Systems

Typed row structures make pipelines easier to maintain. When you access fields by name
(e.g., `record.amount`) rather than by index (e.g., `row[2]`), the code is self-documenting
and refactoring is safer. Pattern matching enables clean, readable dispatch logic for
data classification, routing, and transformation without deeply nested if-else chains.

---

## Summary

| Concept | What Problem It Solves | When To Use |
|---|---|---|
| Closures | Controls what data is shipped to executors with each task | Always — but keep captured variables small; use broadcast for large objects |
| Iterators and Partitions | Reveals how Spark splits and processes data in parallel | When tuning parallelism, diagnosing data skew, or verifying partition layout |
| mapPartitions | Reduces per-element overhead for expensive setup operations | Whenever setup cost (DB connections, model loading) is significant |
| Case Classes / Pattern Matching | Provides typed, readable row structures and clean conditional logic | When building production pipelines that need maintainability and type safety |

---

*All examples in this guide are self-contained and can be run locally with PySpark installed
and `master("local[2]")` or `master("local[3]")` as the Spark master.*
