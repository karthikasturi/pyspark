# Further Learning Reference — Apache Spark 4.1.1 (PySpark)

> **Validation Notes vs. Course Content**
>
> - Mesos is fully dropped in Spark 4.x — removed from cluster manager list
> - SparkR is deprecated as of Spark 4.x
> - DStreams (legacy Spark Streaming API) is superseded — focus entirely on Structured Streaming
> - Spark Connect introduced in Spark 3.4, significantly expanded in 4.x with full PySpark/ML parity
> - Scala 2.13 is now the only supported Scala version (2.12 dropped)
> - Java 17/21 only — JDK 8/11 dropped in Spark 4.0
> - AQE is enabled by default since Spark 3.2
> - Storage Partition Join (SPJ) is a Spark 4.x official optimization topic
> - VARIANT data type, SQL Pipe Syntax, Session Variables, SQL UDFs are official 4.0+ SQL features
> - `transformWithState` / Arbitrary State API v2 is the official replacement for `mapGroupsWithState` in Spark 4.0
> - Python Data Source API (`pyspark.datasources`) is an official Spark 4.0 feature

---

## Table of Contents

- [Track 1: Performance & Optimization](#track-1-performance--optimization)
  - [Adaptive Query Execution (AQE)](#adaptive-query-execution-aqe--deep-dive)
  - [Cost-Based Optimizer (CBO) & Statistics](#cost-based-optimizer-cbo--statistics)
  - [Storage Partition Join (SPJ)](#storage-partition-join-spj)
  - [Partition Tuning Reference](#partition-tuning-reference)
  - [Join Strategy Hints](#join-strategy-hints)
- [Track 2: Advanced PySpark APIs](#track-2-advanced-pyspark-apis-spark-4x-official)
  - [Python Data Source API](#python-data-source-api)
  - [Python UDTFs](#python-udtfs-user-defined-table-functions)
  - [Pandas API on Spark](#pandas-api-on-spark-pysparkpandas)
  - [VARIANT Data Type](#variant-data-type)
  - [PySpark Plotting API](#pyspark-plotting-api)
- [Track 3: Spark SQL — New 4.x Features](#track-3-spark-sql--new-4x-features)
  - [SQL Pipe Syntax](#sql-pipe-syntax)
  - [SQL User-Defined Functions](#sql-user-defined-functions)
  - [Session Variables](#session-variables)
  - [String Collation Support](#string-collation-support)
  - [XML Data Source](#xml-data-source)
- [Track 4: Advanced Structured Streaming](#track-4-advanced-structured-streaming-spark-4x)
  - [Arbitrary State API v2 — transformWithState](#arbitrary-state-api-v2--transformwithstate)
  - [State Data Source](#state-data-source)
  - [RocksDB State Store](#rocksdb-state-store)
  - [Python Streaming Data Sources](#python-streaming-data-sources)
  - [Rate Limiting per Micro-Batch](#rate-limiting-per-micro-batch)
- [Track 5: Spark Connect](#track-5-spark-connect-spark-4x)
  - [Architecture](#architecture)
  - [Supported APIs in Spark 4.x](#supported-apis-in-spark-4x)
  - [Use Cases](#use-cases)
- [Track 6: Production & Observability](#track-6-production--observability)
  - [Spark UI Enhancements](#spark-ui-enhancements-4x)
  - [Structured Logging Framework](#structured-logging-framework)
  - [Testing PySpark](#testing-pyspark)
  - [Deploying on Kubernetes](#deploying-on-kubernetes)
- [Official Reference Links](#official-reference-links)

---

## Track 1: Performance & Optimization

### Adaptive Query Execution (AQE) — Deep Dive

| Config Key | Default | Notes |
| :--- | :--- | :--- |
| `spark.sql.adaptive.enabled` | `true` | Master AQE switch (default since Spark 3.2) |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Post-shuffle partition coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Auto-splits skewed partitions |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0b` | Threshold to promote SMJ → Shuffled Hash Join |
| `spark.sql.adaptive.optimizer.excludedRules` | _(empty)_ | Exclude specific AQE optimizer rules |
| `spark.sql.adaptive.customCostEvaluatorClass` | _(none)_ | Plug in a custom cost evaluator |

- AQE converts sort-merge join → broadcast hash join at runtime when one side shrinks below the broadcast threshold
- AQE converts sort-merge join → shuffled hash join based on `maxShuffledHashJoinLocalMapThreshold`
- 📖 **Reference:** [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

### Cost-Based Optimizer (CBO) & Statistics

- `ANALYZE TABLE ... COMPUTE STATISTICS` for table and column-level stats
- `DESCRIBE EXTENDED` to inspect table statistics
- `EXPLAIN COST` / `df.explain(mode="cost")` to read CBO estimates
- Runtime statistics visible in Spark SQL UI (`isRuntime=true` in plan details)
- 📖 **Reference:** [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

### Storage Partition Join (SPJ)

> **New in Spark 4.0:** `allowJoinKeysSubsetOfPartitionKeys` and partially clustered distribution support added.

- Generalization of Bucket Joins using V2 DataSources' partitioning metadata
- Eliminates shuffle for co-partitioned Iceberg/Delta tables
- `spark.sql.sources.v2.bucketing.enabled` — `true` by default in Spark 4.x (was `false` in Spark 3.x)
- `spark.sql.sources.v2.bucketing.pushPartValues.enabled`
- `spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled` _(new in Spark 4.0)_
- Partially clustered distribution for skew-aware SPJ
- 📖 **Reference:** [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

### Partition Tuning Reference

| Config Key | Default | Description |
| :--- | :--- | :--- |
| `spark.sql.shuffle.partitions` | `200` | Number of partitions for shuffles; tune to data volume |
| `spark.sql.files.maxPartitionBytes` | `128 MB` | Max bytes per partition for file-based sources |
| `spark.sql.files.maxPartitionNum` | _(auto)_ | Cap on the number of split file partitions |

- Coalesce hints in SQL: `COALESCE`, `REPARTITION`, `REPARTITION_BY_RANGE`, `REBALANCE`
- 📖 **Reference:** [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

### Join Strategy Hints

| Hint | Join Strategy |
| :--- | :--- |
| `BROADCAST` / `BROADCASTJOIN` / `MAPJOIN` | Broadcast Hash Join |
| `MERGE` | Sort-Merge Join |
| `SHUFFLE_HASH` | Shuffled Hash Join |
| `SHUFFLE_REPLICATE_NL` | Shuffle-and-Replicate Nested Loop Join |

- Priority order (highest → lowest): `BROADCAST` > `MERGE` > `SHUFFLE_HASH` > `SHUFFLE_REPLICATE_NL`
- 📖 **Reference:** [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

---

## Track 2: Advanced PySpark APIs (Spark 4.x Official)

### Python Data Source API

> **New in Spark 4.0**

- Define custom batch and streaming data sources in pure Python (`pyspark.datasources`)
- `DataSource`, `DataSourceReader`, `DataSourceWriter` base classes
- Arrow-based Python Data Source Writer — enable with `spark.sql.execution.arrow.pyspark.enabled = true`
- Register a Python data source: `spark.dataSource.register(MyDataSource)`
- Fully supported in Spark Connect
- See also: [Python Streaming Data Sources](#python-streaming-data-sources) (Track 4)
- 📖 **Reference:** [Python Data Source API](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.datasources.html)

### Python UDTFs (User-Defined Table Functions)

- `@udtf` decorator for table-generating functions
- Named arguments support in scalar Python/Pandas UDFs
- `applyInArrow` for grouped and cogrouped operations
- 📖 **Reference:** [Python UDF Reference](https://spark.apache.org/docs/4.1.1/sql-ref-functions-udf-python.html)

### Pandas API on Spark (`pyspark.pandas`)

> ⚠️ **Breaking Change in Spark 4.0:** `compute.ops_on_diff_frames` now defaults to `True` (was `False` in Spark 3.x). Code that relied on the error raised by cross-frame operations will silently produce different results after upgrading.

- `json_normalize` supported in Spark 4.0
- `ps.sql()` available via Spark Connect
- Deprecated APIs fully removed in 4.0 — review the [Migration Guide](https://spark.apache.org/docs/4.1.1/migration-guide.html) before upgrading
- 📖 **Reference:** [Pandas API on Spark](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.pandas/index.html)

### VARIANT Data Type

> **New in Spark 4.0**

- Semi-structured data type for flexible schema-on-read workloads
- `parse_json()` / `try_parse_json()` built-in functions
- `VariantVal` in PySpark; Variant support in UDFs/UDTFs/UDAFs
- Supported in Spark Connect Scala client
- 📖 **Reference:** [SQL Data Types](https://spark.apache.org/docs/4.1.1/sql-ref-datatypes.html)

### PySpark Plotting API

- Native `df.plot` API — no conversion to pandas required
- Backends supported: `plotly`, `matplotlib`
- Chart types: `line`, `bar`, `histogram`, `kde`/`density`, `scatter`
- 📖 **Reference:** [PySpark Pandas DataFrame](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.pandas/frame.html)

---

## Track 3: Spark SQL — New 4.x Features

### SQL Pipe Syntax

> **New in Spark 4.0**

- Alternative to nested subqueries using the `|>` pipe operator for more readable SQL

```sql
-- Pipe syntax: filter then aggregate
SELECT * FROM orders
|> WHERE status = 'shipped'
|> AGGREGATE COUNT(*) AS cnt, SUM(amount) AS total GROUP BY region
```

- 📖 **Reference:** [SQL Syntax Reference](https://spark.apache.org/docs/4.1.1/sql-ref-syntax.html)

### SQL User-Defined Functions

> **New in Spark 4.0** — no JVM or Python worker overhead for pure SQL logic.

```sql
CREATE FUNCTION add_one(x INT) RETURNS INT RETURN x + 1;
```

- `EXECUTE IMMEDIATE` for dynamic SQL execution
- 📖 **Reference:** [SQL UDF Reference](https://spark.apache.org/docs/4.1.1/sql-ref-functions-udf.html)

### Session Variables

> **New in Spark 4.0**

```sql
DECLARE var INT DEFAULT 0;  -- session-scoped mutable variable
SET VAR var = 5;             -- update within session
```

- 📖 **Reference:** [DECLARE VARIABLE Syntax](https://spark.apache.org/docs/4.1.1/sql-ref-syntax-ddl-declare-variable.html)

### String Collation Support

- `COLLATE` keyword for locale-aware string comparisons
- ICU4J-backed collation (Unicode-compliant)
- Use cases: case-insensitive joins, accent-insensitive search
- 📖 **Reference:** [SQL Data Types](https://spark.apache.org/docs/4.1.1/sql-ref-datatypes.html)

### XML Data Source

> **New in Spark 4.0** — built-in, no external package required.

- `spark.read.format("xml")` available natively since Spark 4.0
- `from_xml()`, `schema_of_xml()` SQL functions available natively
- 📖 **Reference:** [XML Data Source](https://spark.apache.org/docs/4.1.1/sql-data-sources-xml.html)

---

## Track 4: Advanced Structured Streaming (Spark 4.x)

### Arbitrary State API v2 — `transformWithState`

> **New in Spark 4.0** — replaces `mapGroupsWithState` and `flatMapGroupsWithState`.

- `TransformWithStateInPandas` — Python-native stateful processing
- Multiple state variables and column families per operator
- State types: `ValueState`, `ListState`, `MapState`
- Timer support (event-time and processing-time)
- TTL (Time-to-Live) for automatic state expiry
- Schema evolution for state with Avro encoding
- Batch query support for `transformWithState`
- 📖 **Reference:** [Structured Streaming Programming Guide](https://spark.apache.org/docs/4.1.1/structured-streaming-programming-guide.html)

### State Data Source

- Read state store contents for debugging: `spark.read.format("statestore")`
- Read change feed from state store
- Query registered timers and operator metadata at a given batch ID
- Options: `snapshotStartBatchId`, `snapshotPartitionId`
- 📖 **Reference:** [State Data Source](https://spark.apache.org/docs/4.1.1/structured-streaming-state-data-source.html)

### RocksDB State Store

- Recommended state backend for large-scale stateful streaming
- Set via `spark.sql.streaming.stateStore.providerClass`
- Configurable compression; virtual column families for state isolation
- Checkpoint Structure V2 with RocksDB integration
- 📖 **Reference:** [Structured Streaming Programming Guide](https://spark.apache.org/docs/4.1.1/structured-streaming-programming-guide.html)

### Python Streaming Data Sources

> **New in Spark 4.0** — See also: [Python Data Source API](#python-data-source-api) (Track 2)

- Implement custom streaming sources and sinks in pure Python
- `DataSource` interface with `streamReader()` / `streamWriter()` methods
- 📖 **Reference:** [Python Data Source API](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.datasources.html)

### Rate Limiting per Micro-Batch

> **New in Spark 4.0**

- `spark.sql.streaming.maxBytesPerTrigger` — cap bytes processed per trigger
- Use alongside `maxFilesPerTrigger` for fine-grained ingestion control
- 📖 **Reference:** [Structured Streaming Programming Guide](https://spark.apache.org/docs/4.1.1/structured-streaming-programming-guide.html)

---

## Track 5: Spark Connect (Spark 4.x)

### Architecture

- Client-server decoupled model using gRPC — production-ready in Spark 4.x
- `spark.api.mode` config to switch between Connect and Classic modes
- Lightweight `pyspark-client` package — no JDK required on client
- 📖 **Reference:** [Spark Connect Overview](https://spark.apache.org/docs/4.1.1/spark-connect-overview.html)

### Supported APIs in Spark 4.x

- Full DataFrame/Dataset API parity (Python and Scala)
- `spark.ml` on Spark Connect
- Python Data Sources via Spark Connect
- `foreachBatch` / `DataStreamWriter` for streaming
- Checkpoint and `localCheckpoint` support
- Client-side streaming query listeners
- UDAFs in Spark Connect
- 📖 **Reference:** [Spark Connect Overview](https://spark.apache.org/docs/4.1.1/spark-connect-overview.html)

### Use Cases

- IDE-native Spark from a laptop without a local Spark installation
- Embedding Spark in any application via gRPC client
- Multi-language clients: Python, Scala, Java

---

## Track 6: Production & Observability

### Spark UI Enhancements (4.x)

- Flame graph for executor thread dump
- Selectable plan nodes on SQL execution page
- Task-level thread dump support
- Rolling event logs enabled by default (`spark.eventLog.rolling.enabled = true`)
- Prometheus metrics enabled by default (`spark.ui.prometheus.enabled = true`)
- OpenTelemetry push sink configurable via `spark.metrics.conf`
- 📖 **Reference:** [Monitoring & Instrumentation](https://spark.apache.org/docs/4.1.1/monitoring.html)

### Structured Logging Framework

- `spark.log.structuredLogging.enabled = true` — emits JSON-structured logs
- Machine-parseable format suitable for ELK, Loki, and OpenTelemetry pipelines
- 📖 **Reference:** [Monitoring & Instrumentation](https://spark.apache.org/docs/4.1.1/monitoring.html)

### Testing PySpark

- `assertDataFrameEqual` with enhanced flexible parameters (Spark 4.0)
- `assertSchemaEqual` with flexible parameter support
- `pytest` + `pyspark` unit test patterns with local `SparkSession`
- 📖 **Reference:** [PySpark Testing Utilities](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.testing.html)

### Deploying on Kubernetes

- Spark Kubernetes Operator with `SparkApp` and `SparkCluster` CRDs — official in Spark 4.0
- `spark-submit` to Kubernetes with Java 21-based Docker images
- Fraction-based resource request calculation for K8s pods
- 📖 **Reference:** [Running on Kubernetes](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html)

---

## Official Reference Links

| Topic | URL |
| :--- | :--- |
| Spark 4.1.1 Docs Home | [spark.apache.org/docs/4.1.1](https://spark.apache.org/docs/4.1.1) |
| Performance Tuning | [sql-performance-tuning.html](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html) |
| Structured Streaming Guide | [structured-streaming-programming-guide.html](https://spark.apache.org/docs/4.1.1/structured-streaming-programming-guide.html) |
| State Data Source | [structured-streaming-state-data-source.html](https://spark.apache.org/docs/4.1.1/structured-streaming-state-data-source.html) |
| PySpark API Reference | [api/python/index.html](https://spark.apache.org/docs/4.1.1/api/python/index.html) |
| Python Data Source API | [pyspark.datasources](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.datasources.html) |
| Spark Connect Overview | [spark-connect-overview.html](https://spark.apache.org/docs/4.1.1/spark-connect-overview.html) |
| SQL Reference | [sql-ref.html](https://spark.apache.org/docs/4.1.1/sql-ref.html) |
| Monitoring & Instrumentation | [monitoring.html](https://spark.apache.org/docs/4.1.1/monitoring.html) |
| Running on Kubernetes | [running-on-kubernetes.html](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html) |
| Tuning Guide | [tuning.html](https://spark.apache.org/docs/4.1.1/tuning.html) |
| Configuration Reference | [configuration.html](https://spark.apache.org/docs/4.1.1/configuration.html) |
| Migration Guide (3.x to 4.x) | [migration-guide.html](https://spark.apache.org/docs/4.1.1/migration-guide.html) |
| PySpark Testing Utilities | [pyspark.testing](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.testing.html) |
