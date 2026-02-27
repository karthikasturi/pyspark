# PySpark Training

Hands-on training material for Apache PySpark, covering core concepts through advanced patterns.

## Structure

| Folder | Description |
|---|---|
| `quickstart/` | Docker Compose setup for a local Spark 4.x cluster (master + 2 workers + history server) |
| `quickstart/jobs/` | PySpark example scripts organised by topic |
| `labs/` | Step-by-step lab guides for each topic |
| `presentations/` | Slide decks and supporting material |

## Topics Covered

- **RDD Core** — partitions, transformations vs actions, lazy evaluation, fault tolerance, persistence, broadcast variables, accumulators, custom partitioners
- **Execution Plans** — understanding Spark's logical and physical plans
- **Advanced PySpark** — closures, iterators, `mapPartitions`, pattern matching

## Getting Started

Spin up a local Spark cluster with Docker Compose:

```bash
cd quickstart/
docker compose up -d
```

See [quickstart/README.md](quickstart/README.md) for full setup instructions and how to submit jobs.

## Requirements

- Docker Engine 24+
- Docker Compose v2.20+
- ~6 GB RAM available to Docker
