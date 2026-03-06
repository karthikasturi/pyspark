# RetailPulse Capstone – Lab Guide
## Days 8–12: Foundation → RDD Layer → Structured ETL → Optimization → Streaming

---

## Data at a Glance

All files live under `quickstart/jobs/capstone_data/dev/batch/`.

| File | Format | Rows | Key Fields |
|------|--------|------|-----------|
| `orders.csv` | CSV | 10,000 | `order_id`, `customer_id`, `product_id`, `region`, `category`, `total_amount`, `status`, `discount`, `order_date`, `order_month`, `order_year` |
| `customers.csv` | CSV | 1,000 | `customer_id`, `region`, `segment`, `loyalty_tier`, `is_active` |
| `products.json` | JSON-lines | 200 | `product_id`, `category`, `subcategory`, `base_price`, `stock_quantity`, `is_active` |
| `regions.csv` | CSV | 5 | `region`, `target_revenue`, `manager`, `hq_city` |

**Built-in data characteristics you will observe:**
- East region carries ~35% of order volume (data skew)
- ~0.5% of `discount`, `city`, `payment_method` fields are empty strings (dirty data)
- ~5–6% of orders have `total_amount > 5000` (high-value anomalies — driven by high-qty × high-price combinations, not just the 1% unit-price anomalies)

---

## Lab Files

| Day | File | Topic |
|-----|------|-------|
| 8 | [day08_foundation_architecture.md](day08_foundation_architecture.md) | Load data, basic aggregations, execution plans |
| 9 | [day09_rdd_distributed_execution.md](day09_rdd_distributed_execution.md) | RDD API, custom partitioners, lineage, broadcast |
| 10 | [day10_dataframe_etl.md](day10_dataframe_etl.md) | Multi-source joins, window functions, Parquet output |
| 11 | [day11_optimization.md](day11_optimization.md) | AQE, skew salting, caching, join strategies |
| 12 | [day12_streaming.md](day12_streaming.md) | Structured Streaming, watermarking, windowed aggregation |

---

## Progression Summary

| Day | What you built | Key concept proven |
|---|---|---|
| 8 | Regional + category aggregation from raw CSV | `Exchange` node = shuffle; execution plan is readable |
| 9 | Same aggregations via RDD API | Partition skew is physically visible; broadcast avoids shuffle join |
| 10 | Enriched multi-source dataset with window functions | Broadcast join strategy; partition pruning; window functions vs self-joins |
| 11 | Optimized the Day 10 pipeline end-to-end | AQE, salting, cache strategy, accumulator metrics, join strategy comparison |
| 12 | Real-time order ingestion with windowed aggregation | Micro-batch model, watermarking, stateless vs stateful streaming, streaming-batch equivalence |

Each day builds on the last: Day 8's output Parquet is Day 10's starting point. Day 9's custom partitioner is Day 11's skew problem to solve. Day 10's enriched Parquet is Day 11's optimization target. Day 12 introduces incrementally-arriving files that mirror the same schema used in Days 8–11, making the batch-to-streaming transition a direct comparison rather than a context switch.
