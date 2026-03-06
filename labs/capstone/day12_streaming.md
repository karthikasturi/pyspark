# Day 12 – Structured Streaming Implementation

## Goal
Build a near real-time order ingestion pipeline using Spark Structured Streaming with a file-based source. By the end, new order files dropped into a landing directory are automatically ingested, windowed revenue is continuously updated, late-arriving events are handled via watermarking, and high-value transactions are flagged as they arrive.

**Architecture overview:**
```
landing/          →  Spark Structured Streaming  →  output/streaming/
(new JSON files)       (micro-batch, file source)     (windowed Parquet sinks)
```

All streaming inputs and outputs use the local filesystem. No Kafka or HDFS is required.

---

### Task 1 – Set up the streaming source and validate the schema

**What to do:**  
Configure a `spark.readStream` to watch a landing directory for new JSON files. The streaming events (in `capstone_data/dev/streaming/input/`) have a **different schema** from the batch `orders.csv` — define a dedicated streaming schema before reading.

**Hints:**
- Copy the provided batch files into a separate `landing/` directory one at a time to simulate event arrival. Do not point the stream at the original `streaming/input/` — Spark will read all 15 files at once.
- Define a `streaming_schema` matching the event fields: `event_id`, `order_id`, `customer_id`, `product_id`, `product_name`, `category`, `region`, `event_time`, `arrival_time`, `quantity`, `unit_price`, `total_amount`, `is_late_event`, `is_high_value`, `payment_method`. Use `StringType` for `event_time` initially.
- Use `spark.readStream.schema(streaming_schema).json("<landing_dir>")` — note `.json()` not `.csv()`, and no `header` option needed for JSON-lines.
- Set `maxFilesPerTrigger` to `1` so you can observe each micro-batch separately.
- Verify by writing a `console` sink with `outputMode("append")` and `trigger(once=True)`. Drop one batch file into the landing directory — you should see 12 rows printed.
- Check `query.lastProgress` to confirm `numInputRows` is 12.

**Expected `console` sink output for one batch file:**
```
-------------------------------------------
Batch: 0
-------------------------------------------
+----------------+----------------+----------+---------+-----+----------+--------+-----------+
|event_id        |order_id        |customer_id|region  |qty  |unit_price|total   |is_high_val|
+----------------+----------------+----------+---------+-----+----------+--------+-----------+
|EVT-0000-47695  |STR-4987106     |CUST-000759|East    |   2 |     20.07|   40.14|      false|
|EVT-0000-20336  |STR-7140737     |CUST-000641|West    |   5 |     59.16|  295.80|      false|
... (12 rows total)
```
`numInputRows: 12` in `query.lastProgress`

**Why `schema` is mandatory for streaming:**  
`inferSchema` requires a full data scan, which is incompatible with streaming (the data does not exist yet at plan time). Always pre-define the schema for any streaming source.

---

### Task 2 – Windowed revenue aggregation on event time

**What to do:**  
Compute total revenue in 1-hour tumbling windows based on `event_time` (the event timestamp field in the streaming JSON schema).

**Hints:**
- Cast `event_time` to `TimestampType` (use `to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")`) before windowing.
- Use `window(col("event_time"), "1 hour")` inside a `groupBy` with `agg(_sum("total_amount"))`.
- Write the result to a Parquet sink with `outputMode("append")`.
- With 12 events per batch and all events within a single minute, each batch file will produce exactly **1 window row** (all events fall in the same 1-hour bucket). Process all 15 files and you will see all events collapsing into the same hour window unless using a smaller window size.
- Try `window(col("event_time"), "1 minute")` to see one row per distinct minute — provides more interesting windowed output for the dev dataset.

**Expected windowed output shape (1-minute windows across 15 batch files, 12 events each):**
```
+------------------------------------------+------------+
|window                                    |total_revenue|
+------------------------------------------+------------+
|{2026-03-03 00:08:00, 2026-03-03 00:09:00}|   xxxxx.xx  |
|{2026-03-03 00:09:00, 2026-03-03 00:10:00}|   xxxxx.xx  |
|{2026-03-03 00:10:00, 2026-03-03 00:11:00}|   xxxxx.xx  |
... (one row per minute, ~15 rows total)
+------------------------------------------+------------+
```
The exact `event_time` range depends on when the data was generated (the generator uses
`datetime.now()` as `reference_time`). Use `window.start` and `window.end` to verify the bucketing.

**Why tumbling windows:**  
A tumbling window produces non-overlapping, fixed-duration buckets. It answers: "What was total revenue in the 9 AM–10 AM hour?" Sliding windows (e.g., 1-hour windows every 15 minutes) would answer "rolling hour revenue" but produce more output rows. Use tumbling unless overlap is required.

---

### Task 3 – Add watermarking to handle late-arriving events

**What to do:**  
The dataset has orders spread across months. In a streaming context, files written out of order simulate late arrivals (an event timestamped 3 hours ago appearing in a new file now).

**Hints:**
- Add `.withWatermark("event_time", "2 hours")` before the `groupBy`. Use the already-cast `TimestampType` column.
- The watermark tells Spark: accept late data up to 2 hours behind the current event-time high-water mark; discard anything older.
- The streaming data has `is_late_event: true` on ~5.6% of events (10 out of 180 total, delay of 10–20 minutes). These are within the 2-hour watermark so they will **not** be dropped by default — try narrowing the watermark to `"5 minutes"` to see them get excluded.
- With `outputMode("append")`, a window's result is only emitted once the watermark has passed its end time. Switch to `outputMode("update")` temporarily and observe the difference — update mode emits partial results as each micro-batch arrives.
- Inspect `query.lastProgress["eventTime"]` to see the current watermark value after each batch.

**What to observe:**  
The late events have `is_late_event = true` and `event_time` that lags `arrival_time` by 10–20 minutes. With a 2-hour watermark, these arrive within the allowed window and are included. With a tight 5-minute watermark, they fall outside and are silently dropped. After processing all 15 batches you should see `numInputRows = 0` for zero-row batches when no new file is added.

**Why watermarking matters:**  
Without a watermark, Spark must keep every window's state in memory forever (a late event for last year could theoretically still arrive). The watermark is the memory bound: once a window's deadline passes, its state is flushed and memory is released. This is what makes stateful streaming scalable.

---

### Task 4 – Detect high-value transactions in real time

**What to do:**  
Flag orders where `total_amount > 5000` as high-value anomalies and write them to a separate sink.

**Hints:**
- From the streaming DataFrame, apply `filter(col("total_amount") > 5000)` — this is a stateless operation (no grouping, no window), so it works with `outputMode("append")` directly.
- Write this filtered stream to a separate Parquet directory (`output/streaming/high_value_alerts/`).
- Also add a `withColumn("alert_ts", current_timestamp())` to capture when Spark detected the event, making it easy to measure detection latency later.
- The streaming data has `is_high_value: true` pre-labelled — about 5% of events (9 out of 180 total across all 15 batches). In any single 12-event batch file you will see 0–2 high-value rows written. After all 15 batches, the alerts Parquet should contain exactly 9 rows.

**Why stateless operations are simpler:**  
A filter is stateless — each row is evaluated independently. No watermark, no state store, no concern about late data. This makes high-value detection cheap to add alongside the windowed aggregation. Run both queries in the same `StreamingContext`; Spark schedules them as independent micro-batch jobs.

---

### Task 5 – Multi-query streaming: run both pipelines simultaneously

**What to do:**  
Run the windowed revenue aggregation (Task 2+3) and the high-value alert filter (Task 4) as two separate streaming queries at the same time.

**Hints:**
- Start both queries: `q1 = windowed_df.writeStream...start()` and `q2 = alerts_df.writeStream...start()`.
- Call `spark.streams.awaitAnyTermination()` to block the driver while both run.
- To stop cleanly: `q1.stop(); q2.stop()`.
- Inspect the Spark UI → Streaming tab. You should see two active streaming queries, each with its own batch ID, input rate, and processing rate graph.

**Expected observation:**  
Both queries consume the same source stream independently. Dropping a file with high-value orders into the landing directory should simultaneously update the windowed revenue output and the alerts output.

**Why multiple queries are preferable to one:**  
Combining two logically separate outputs into one query complicates the sink logic and forces both to share the same `outputMode`. Two queries keep each concern independent and are easier to monitor, restart, and tune.

---

### Task 6 – Inspect streaming query progress and metrics

**What to do:**  
After at least 3 micro-batches have processed, print `query.lastProgress` and `query.recentProgress` for the windowed query.

**Hints:**
- Fields to examine and explain:
  - `batchId` — increments on each micro-batch
  - `numInputRows` — rows ingested this batch
  - `inputRowsPerSecond` vs `processedRowsPerSecond` — is processing keeping up with ingestion?
  - `eventTime.watermark` — current watermark timestamp
  - `stateOperators` — how much state is held in memory for the windowed aggregation?
- Set `trigger(processingTime="30 seconds")` instead of the default and observe how batchId increments on a schedule even when no new files arrive (zero-row batches).

**Recording requirement:**  
Copy the JSON output of `query.lastProgress` into your notes. Annotate each key field with one line of explanation. This is how you would diagnose a streaming lag issue in production.

**Example `query.lastProgress` (annotated):**
```json
{
  "id": "...",
  "runId": "...",
  "batchId": 4,                          // 5th micro-batch processed (0-indexed)
  "numInputRows": 12,                    // rows ingested in this batch (one file = 12 events)
  "inputRowsPerSecond": 12.0,            // arrival rate
  "processedRowsPerSecond": 240.0,       // processing rate >> arrival rate = no lag
  "durationMs": { "triggerExecution": 50, "getBatch": 5, "queryPlanning": 20 },
  "eventTime": {
    "watermark": "2026-03-03T00:12:00.000Z"  // current watermark; late events before this are dropped
  },
  "stateOperators": [{
    "numRowsTotal": 5,                   // 5 open window states held in memory
    "numRowsUpdated": 1                  // 1 window updated in this batch
  }]
}
```

---

### Task 7 – Write streaming output and verify with a batch read

**What to do:**  
After the streaming pipeline has processed 3–5 files, stop the queries. Open a new regular (batch) Spark session and read the output Parquet directories as static DataFrames.

**Hints:**
- `spark.read.parquet("<output>/windowed_revenue/")` — count the rows, show the window ranges. After all 15 batch files, you should see one row per 1-minute window bucket (up to 15 rows if each file covers a distinct minute).
- `spark.read.parquet("<output>/high_value_alerts/")` — verify row count is 9 rows (5% of 180 total events). If you processed fewer than 15 files you will have proportionally fewer.
- Each alert row has `alert_ts` (batch detection time) and `event_time` (when the order happened). The difference is the streaming detection latency — for the dev dataset this will be under a second.

**Why this verification matters:**  
The streaming-batch equivalence guarantee is one of Structured Streaming's core properties. Verifying it gives you confidence that the streaming pipeline is not dropping or double-counting records — which is the main correctness concern in any streaming system.

---

### Day 12 Deliverables

| Artifact | What it looks like |
|---|---|
| Streaming source validation | `console` sink output showing rows read from landing directory |
| Windowed revenue output | Parquet with `window.start`, `window.end`, `total_revenue` columns |
| Watermark demonstration | Evidence that late-arriving rows outside the watermark were dropped |
| High-value alert output | Parquet with 9 rows total (5% of 180 streaming events), includes `alert_ts` and `event_time` columns |
| Multi-query Spark UI screenshot | Two active streaming queries visible simultaneously |
| `lastProgress` annotation | JSON with 6+ fields explained in your own words |
| Batch read verification | Row count and revenue totals matching the batch baseline from Day 10 |
