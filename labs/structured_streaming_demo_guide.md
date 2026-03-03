# PySpark Structured Streaming — Demo Guide

## Overview

This guide walks through six self-contained PySpark Structured Streaming examples, designed for learners using **Spark 4.1.1** running in a Docker Compose cluster. Each example builds on the previous one, introducing a new concept progressively.

All scripts live under:
```
quickstart/jobs/structured_streaming/
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Spark version | **4.1.1** (`apache/spark:4.1.1-scala2.13-java21-python3-ubuntu`) |
| Scala version | 2.13 (important for package coordinates) |
| Java version | 21 |
| Mode | Local (`local[*]`) inside the `spark-master` container |
| Language | Python (PySpark) |
| Run method | `docker exec spark-master spark-submit` |
| Cluster | Docker Compose — 1 master + 2 workers + history server |

> No external infrastructure is required for Examples 1, 3, 4, 5, and 6.  
> Example 2 requires a running Kafka broker (not included in the default `docker-compose.yml`).

---

## Docker Cluster Setup

All examples run inside the Docker Compose cluster defined in `quickstart/docker-compose.yml`.

### Start the cluster
```bash
# From the repository root
cd quickstart
docker compose up -d
```

### Verify all services are healthy
```bash
docker compose ps
```
Expected output: `spark-master`, `spark-worker-1`, `spark-worker-2`, and `spark-history` should all show **running** or **healthy**.

### Useful URLs

| Service | URL |
|---|---|
| Spark Master Web UI | http://localhost:8080 |
| Worker 1 Web UI | http://localhost:8081 |
| Worker 2 Web UI | http://localhost:8082 |
| Driver / App UI (live job) | http://localhost:4040 |
| History Server | http://localhost:18080 |

### Job paths inside the container
The `quickstart/jobs/` folder is volume-mounted at `/opt/spark/jobs/` inside each container.  
All `spark-submit` paths below reference `/opt/spark/jobs/...`.

### Stop the cluster
```bash
docker compose down
```

---

## Concept Overview

Structured Streaming treats a live data stream as an **unbounded table** that continuously grows. Each time Spark processes new data it is called a **micro-batch**. Key concepts used in these examples:

| Concept | Short description |
|---|---|
| Source | Where data comes from (rate, Kafka, files) |
| Sink | Where results go (console, files, Kafka) |
| Output mode | `append`, `update`, or `complete` |
| Watermark | Threshold for handling late-arriving data |
| Checkpoint | Persistent state to allow stream recovery |

---

## Example 1 — Basic Streaming with Rate Source

**File:** `quickstart/jobs/structured_streaming/01_rate_source_streaming.py`

### What it does
The built-in `rate` source generates rows automatically. Each row contains a `timestamp` and a monotonically increasing `value`. This is the simplest possible streaming source — no infrastructure needed.

### Key code
```python
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

query = rate_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
```

### Execution steps
```bash
# From the repository root (cluster must already be running)
docker exec spark-master spark-submit \
  /opt/spark/jobs/structured_streaming/01_rate_source_streaming.py
```
Stop the stream with **Ctrl+C** in the terminal running `docker exec`.

### What to expect
```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+-----+
|           timestamp|value|
+--------------------+-----+
|2026-03-02 10:00:00 |    0|
|2026-03-02 10:00:00 |    1|
...
-------------------------------------------
Batch: 1
-------------------------------------------
...
```
- A new micro-batch appears every 2 seconds.  
- The `value` column increments continuously across batches.

### Key learning points
- `trigger(processingTime="2 seconds")` sets the micro-batch interval.  
- `outputMode("append")` means only newly produced rows are written to the sink.  
- `awaitTermination()` keeps the driver alive until the stream is stopped.

---

## Example 2 — Kafka Streaming (Consumer)

**File:** `quickstart/jobs/structured_streaming/02_kafka_streaming.py`

### What it does
Connects to a Kafka topic and reads messages as a stream. The raw Kafka `value` column is binary; it is cast to a readable string.

### Required dependency
The Kafka connector is **not** bundled with Spark. The cluster runs Spark **4.1.1 on Scala 2.13**, so use the `_2.13` artifact:
```
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
```
Or add this line to `quickstart/conf/spark-defaults.conf` (applied to every container on restart):
```
spark.jars.packages  org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
```

> **Common mistake:** Using `_2.12` with a Scala 2.13 image causes a `ClassNotFoundException` at runtime. Always match the Scala suffix to the image.

### Docker networking note
The default `docker-compose.yml` does **not** include a Kafka service.  
Choose one of the following options:

| Option | Description |
|---|---|
| **A — Add Kafka to docker-compose** | Add a `bitnami/kafka` or `confluentinc/cp-kafka` service to the same `spark-network` and update `kafka.bootstrap.servers` to the service name. |
| **B — External Kafka on the host** | Change `kafka.bootstrap.servers` in the script to `host.docker.internal:9092` so the container can reach the host's Kafka broker. |

### Key code
```python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

messages_df = kafka_df.select(
    col("timestamp"),
    col("value").cast("string").alias("message")
)
```

### Execution steps
```bash
# 1. Create the Kafka topic (run inside the Kafka container or with local CLI tools)
kafka-topics.sh --create --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# 2. Submit the Spark stream from inside the spark-master container
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
  /opt/spark/jobs/structured_streaming/02_kafka_streaming.py

# 3. In a separate terminal, produce messages
kafka-console-producer.sh --topic test-topic \
  --bootstrap-server localhost:9092
# Type any text and press Enter
```

### What to expect
```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+------------+
|           timestamp|     message|
+--------------------+------------+
|2026-03-02 10:05:00 | hello world|
```
- Each message you produce appears in the next micro-batch.  
- `startingOffsets="latest"` means messages sent before the stream started are skipped.

### Key learning points
- Kafka columns include `key`, `value`, `topic`, `partition`, `offset`, `timestamp`.  
- Always cast `value` (and `key`) from binary to string or a domain-specific type.  
- Use `startingOffsets="earliest"` to replay all existing messages from the beginning.

---

## Example 3 — Windowed Aggregation with Watermark

**File:** `quickstart/jobs/structured_streaming/03_windowed_aggregation.py`

### What it does
Counts events in tumbling 10-second time windows. A watermark of 5 seconds limits how long Spark holds state for late-arriving data.

### Key code
```python
watermarked_df = rate_df.withWatermark("timestamp", "5 seconds")

windowed_counts = watermarked_df.groupBy(
    window(col("timestamp"), "10 seconds")
).count()

query = windowed_counts.writeStream \
    .outputMode("update") \
    ...
```

### Execution steps
```bash
docker exec spark-master spark-submit \
  /opt/spark/jobs/structured_streaming/03_windowed_aggregation.py
```

### What to expect
```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|{2026-03-02 10:00:00, 2026-03-02 10:00:10}|  10 |
```
- Windows that are still open update their count each batch.  
- Once the watermark surpasses a window's end time, that window is removed from state.

### Key learning points

| Concept | This example |
|---|---|
| Watermark | `"5 seconds"` — data later than 5 seconds after event time is dropped |
| Window width | `"10 seconds"` tumbling window |
| Output mode | `update` — emit only changed rows per batch |

> `"complete"` output mode would reprint ALL windows every batch — expensive for large state.  
> `"append"` is not supported for aggregations with open windows.

---

## Example 4 — Stateful Deduplication

**File:** `quickstart/jobs/structured_streaming/04_stateful_deduplication.py`

### What it does
Uses `dropDuplicates` to remove rows with the same `value` that arrive within a 10-second watermark window. Spark maintains a hash of seen values as state across batches.

### Key code
```python
deduped_df = rate_df \
    .withWatermark("timestamp", "10 seconds") \
    .dropDuplicates(["value"])
```

### Execution steps
```bash
docker exec spark-master spark-submit \
  /opt/spark/jobs/structured_streaming/04_stateful_deduplication.py
```

### What to expect
- With the `rate` source, every `value` is unique, so all rows pass through.  
- Replace the source with one that produces repeated values to see duplicates filtered.  
- Observe that the stream runs indefinitely without the state growing forever, because the watermark bounds how old a seen value is kept.

### Key learning points
- `dropDuplicates` in streaming requires a **watermark** to bound state size.  
- Without a watermark, Spark would need to remember every `value` ever seen.  
- The watermark columns must be included when specifying which columns to deduplicate on.

---

## Example 5 — ForeachBatch

**File:** `quickstart/jobs/structured_streaming/05_foreach_batch.py`

### What it does
`foreachBatch` hands each micro-batch to a user-defined function as a regular (static) DataFrame. This allows writing to multiple sinks, applying arbitrary transformations, or integrating with non-Spark systems.

### Key code
```python
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    print(f"\n>>> Processing Batch ID: {batch_id}")
    print(f"    Row count in this batch: {batch_df.count()}")
    batch_df.show(truncate=False)

query = rate_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()
```

### Execution steps
```bash
docker exec spark-master spark-submit \
  /opt/spark/jobs/structured_streaming/05_foreach_batch.py
```

### What to expect
```
>>> Processing Batch ID: 0
    Row count in this batch: 12
+--------------------+-----+
|timestamp           |value|
+--------------------+-----+
|2026-03-02 10:00:00 |    0|
...
>>> Processing Batch ID: 1
    Row count in this batch: 12
...
```

### Key learning points
- Inside `foreachBatch`, `batch_df` is a **static** DataFrame — all DataFrame APIs work.  
- You can write the same batch to multiple sinks (e.g., Parquet + a database).  
- The function must not return a value; side effects are the intended pattern.  
- `foreachBatch` is the recommended escape hatch when the built-in sinks are insufficient.

---

## Example 6 — File Streaming (JSON)

**File:** `quickstart/jobs/structured_streaming/06_file_streaming.py`  
**Sample data:** `quickstart/jobs/structured_streaming/sample_data/orders_sample.json`

### What it does
Monitors a folder for new JSON files and processes each as a micro-batch. A checkpoint directory records which files have been processed so the stream can resume after a restart without reprocessing old files.

### Important rules for file streaming

| Rule | Why it matters |
|---|---|
| Schema must be explicit | Streaming does not support schema inference |
| Schema must not change | Column additions/removals mid-stream cause errors |
| Input folder must exist before starting | Spark raises an error if the path does not exist |
| Only NEW files are processed | Drop files into the folder after the stream starts |
| Do not modify files after detection | Spark assumes files are immutable once detected |

### Sample JSON content (`orders_sample.json`)
Each line is a separate JSON object (Newline-Delimited JSON / NDJSON format):
```json
{"id": 1, "name": "Alice",   "amount": 150.0}
{"id": 2, "name": "Bob",     "amount": 200.5}
{"id": 3, "name": "Charlie", "amount": 75.0}
{"id": 4, "name": "Diana",   "amount": 320.0}
{"id": 5, "name": "Eve",     "amount": 45.0}
```

### Key code
```python
order_schema = StructType([
    StructField("id",     IntegerType(), nullable=False),
    StructField("name",   StringType(),  nullable=True),
    StructField("amount", DoubleType(),  nullable=True),
])

orders_stream = spark.readStream \
    .schema(order_schema) \
    .format("json") \
    .option("maxFilesPerTrigger", 1) \
    .load("/tmp/streaming_input")

query = enriched.writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/streaming_checkpoint") \
    .outputMode("append") \
    .start()
```

### Execution steps
```bash
# 1. Create the input and checkpoint directories INSIDE the container
docker exec spark-master mkdir -p /tmp/streaming_input /tmp/streaming_checkpoint

# 2. Copy the sample file INTO the container
docker cp quickstart/jobs/structured_streaming/sample_data/orders_sample.json \
  spark-master:/tmp/streaming_input/orders_batch1.json

# 3. Start the stream
docker exec spark-master spark-submit \
  /opt/spark/jobs/structured_streaming/06_file_streaming.py

# 4. While the stream runs, inject more files to trigger new batches:
docker cp quickstart/jobs/structured_streaming/sample_data/orders_sample.json \
  spark-master:/tmp/streaming_input/orders_batch2.json
```

> **Note:** `/tmp/streaming_input` and `/tmp/streaming_checkpoint` are paths **inside the `spark-master` container**, not on the host. Use `docker cp` (not `cp`) to transfer files from the host into the container.

### What to expect
```
-------------------------------------------
Batch: 0
-------------------------------------------
+---+-------+------+----------+
| id|   name|amount|order_size|
+---+-------+------+----------+
|  1|  Alice| 150.0|     large|
|  2|    Bob| 200.5|     large|
|  3|Charlie|  75.0|     small|
|  4|  Diana| 320.0|     large|
|  5|    Eve|  45.0|     small|
+---+-------+------+----------+
```
- Each new file you drop into the folder triggers a new batch.  
- The `order_size` column is derived in-stream from `amount`.  
- Restarting the stream skips already-processed files (tracked via checkpoint).

### Key learning points
- `maxFilesPerTrigger` controls how many files are consumed per batch (useful for rate limiting).  
- The checkpoint directory stores offsets and state — **never delete it** between restarts.  
- Supported file formats: JSON, CSV, Parquet, ORC, text.  
- For CSV, set `.option("header", "true")` but still provide an explicit schema.

---

## Comparison Table

| Example | Source | Sink | Output mode | Stateful? |
|---|---|---|---|---|
| 1 — Rate source | `rate` | console | append | No |
| 2 — Kafka consumer | Kafka | console | append | No |
| 3 — Windowed aggregation | `rate` | console | update | Yes (window state) |
| 4 — Deduplication | `rate` | console | append | Yes (seen values) |
| 5 — ForeachBatch | `rate` | custom function | append | No |
| 6 — File streaming | JSON files | console | append | No (checkpoint only) |

---

## Common Errors and Fixes

| Error | Likely cause | Fix |
|---|---|---|
| `AnalysisException: Queries with streaming sources must be executed with writeStream` | Forgot `.writeStream` | Add `.writeStream.start()` |
| `Cannot infer schema for source` | Schema not defined for file source | Add `.schema(your_schema)` |
| `ClassNotFoundException: kafka...` | Missing Kafka JAR or wrong Scala/Spark version in package coordinate | Verify you use `spark-sql-kafka-0-10_2.13:4.1.0` (Scala 2.13) with `--packages` flag or `spark-defaults.conf` |
| `Path does not exist` | Input folder missing inside the container | Run `docker exec spark-master mkdir -p /tmp/streaming_input` |
| `Append output mode not supported for aggregations` | Using append with groupBy | Change to `outputMode("update")` or `"complete"` |

---

## Output Modes Summary

| Mode | When to use |
|---|---|
| `append` | Only new rows added, no aggregations (or aggregations after watermark finalized) |
| `update` | Only changed rows per batch; works with aggregations + watermark |
| `complete` | Entire result table rewritten every batch; suitable only for small aggregations |

---

## Next Steps

- Replace the `rate` source with a socket source (`format("socket")`) to type data interactively.  
- Write output to Parquet files instead of console using `.format("parquet")`.  
- Combine `foreachBatch` with JDBC to write streaming results to a relational database.  
- Explore `flatMapGroupsWithState` for fully custom stateful processing.
