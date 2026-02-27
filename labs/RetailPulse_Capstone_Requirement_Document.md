
# Capstone Project Requirement Document  
# RetailPulse – Unified Batch & Streaming Analytics with Spark

---

# 1. Project Overview

## Business Context

A mid-sized retail company wants to modernize its analytics platform using Apache Spark.

The company needs:

1. Daily regional and category-level sales reports  
2. Near real-time revenue visibility  
3. Detection of unusually large transactions  
4. Performance-optimized analytical queries  
5. A scalable design that can move from local to cluster deployment  

---

# 2. Project Objectives

Participants will design and implement:

- A batch analytics pipeline  
- A structured streaming pipeline  
- Optimized Spark jobs  
- Partitioning and caching strategies  
- A scalable architectural design  

---

# 3. Constraints

- Must run in local mode (cluster mode optional demo)  
- No heavy external integrations (Kafka optional conceptual demo only)  
- Streaming must use file-based ingestion  
- No dependency on external databases  
- Outputs stored in Parquet (Delta optional instructor demo)  
- Must be completed progressively across 6 training days  

---

# 4. Functional Requirements

## 4.1 Batch Processing Requirements

The system must:

- Ingest historical order data  
- Join product metadata  
- Generate regional sales aggregates  
- Generate category-level metrics  
- Persist curated output datasets  

---

## 4.2 Streaming Processing Requirements

The system must:

- Ingest incremental order events  
- Compute revenue per time window  
- Handle late-arriving events  
- Detect high-value transactions  
- Produce continuously updating outputs  

---

## 4.3 Optimization Requirements

Participants must demonstrate:

- Partitioning strategies  
- Persistence level comparisons  
- Broadcast joins  
- Adaptive Query Execution (AQE)  
- Shuffle-heavy query optimization  

---

# 5. Non-Functional Requirements

The solution must:

- Demonstrate distributed execution behavior  
- Show DAG and execution plans  
- Compare performance before and after optimization  
- Include discussion of scalability and production considerations  

---

# 6. Day-Wise Requirement Breakdown

---

# Day 08 – Foundation & Architecture

## Goal  
Establish project foundation and build initial batch pipeline.

## Requirements

Participants must:

1. Understand Spark architecture (Driver, Executors, DAG)  
2. Load historical order dataset  
3. Perform basic transformations and aggregations  
4. Generate regional sales summary  
5. Inspect logical and physical execution plans  
6. Use Spark UI to observe stages and tasks  

## Deliverables

- Initial batch aggregation output  
- Execution plan analysis notes  
- DAG observation summary  
- Explanation of shuffle stage observed  

---

# Day 09 – Distributed Execution & RDD Layer

## Goal  
Understand Spark Core internals using RDD API.

## Requirements

Participants must:

1. Re-implement part of the batch logic using RDD  
2. Apply custom partitioning strategy  
3. Compare default vs custom partitioning  
4. Persist RDD using different storage levels  
5. Use broadcast variable for small lookup dataset  
6. Measure and compare execution time  

## Deliverables

- Custom partitioning implementation  
- Performance comparison report (memory vs disk persistence)  
- Explanation of RDD lineage  
- Discussion on data locality  

---

# Day 10 – DataFrame & Structured ETL Layer

## Goal  
Move fully to Spark SQL/DataFrame API.

## Requirements

Participants must:

1. Load multiple formats (CSV, JSON)  
2. Join datasets using DataFrame API  
3. Perform advanced aggregations  
4. Work with nested or structured data  
5. Write output in Parquet format  
6. Conceptually compare DataFrame vs RDD performance  

Optional (Instructor Demo):
- Demonstrate Delta write and table management  

## Deliverables

- Curated batch dataset  
- Aggregated category-level metrics  
- Documentation explaining DataFrame execution advantages  

---

# Day 11 – Query Optimization & Performance Engineering

## Goal  
Optimize inefficient Spark jobs.

## Requirements

Participants must:

1. Analyze logical vs physical plans  
2. Identify shuffle-heavy operations  
3. Implement broadcast joins  
4. Repartition data appropriately  
5. Enable Adaptive Query Execution (AQE)  
6. Compare execution plans before and after optimization  
7. Discuss executor and memory tuning basics  

## Deliverables

- Optimized Spark job  
- Before vs after execution plan comparison  
- Performance observation summary  
- Explanation of join strategy changes  

---

# Day 12 – Structured Streaming Implementation

## Goal  
Add near real-time processing capability.

## Requirements

Participants must:

1. Ingest streaming data from a directory source  
2. Apply micro-batch structured streaming  
3. Compute revenue per time window  
4. Apply watermarking  
5. Handle late-arriving events  
6. Detect unusually high-value transactions  
7. Observe streaming query progress in Spark UI  

## Deliverables

- Running streaming job  
- Windowed revenue output  
- Late-event handling demonstration  
- Explanation of micro-batch execution model  

---

# Day 13 – Final Integration & Architecture Review

## Goal  
Integrate batch and streaming layers and evaluate scalability.

## Requirements

Participants must:

1. Present complete system architecture  
2. Explain partitioning strategy  
3. Explain caching strategy  
4. Demonstrate optimized batch queries  
5. Demonstrate streaming pipeline  
6. Discuss production-readiness gaps  
7. Identify scaling considerations for cluster deployment  

Optional Instructor Demo:

- How Kafka would replace file streaming  
- How Spark would run on YARN or Kubernetes  
- How Delta Lake improves data governance  

---

## Final Deliverables

Participants must submit:

1. Architecture diagram  
2. Batch processing design document  
3. Streaming design explanation  
4. Optimization summary  
5. Production gap analysis  

---

# 7. Production Risks & Gaps (Final Discussion)

Participants must analyze:

- Lack of schema enforcement  
- Limited observability and monitoring  
- No alerting system  
- Incomplete fault-tolerance validation  
- No security model (IAM, encryption, RBAC)  
- No CI/CD or deployment automation  
- No resource auto-scaling strategy  

---

# 8. Evaluation Criteria

Participants will be assessed on:

- Correctness of batch pipeline  
- Understanding of distributed execution  
- Optimization improvements demonstrated  
- Proper streaming implementation  
- Clarity in architectural explanation  
- Awareness of production considerations  

---

# 9. Why This Capstone Is Realistic Yet Manageable

- Single business domain (retail analytics)  
- No heavy infrastructure setup  
- Covers Spark Core, SQL, Optimization, Streaming  
- Demonstrates architectural thinking  
- Feasible within 24 hours  
- Scalable to production conceptually  
