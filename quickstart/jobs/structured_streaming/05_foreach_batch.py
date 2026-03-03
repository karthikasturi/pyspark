# =============================================================================
# Example 5: ForeachBatch
# =============================================================================
#
# foreachBatch lets you apply arbitrary DataFrame operations to each
# micro-batch.  The function receives (batch_df, batch_id) and can do
# anything a regular DataFrame supports: show, write to multiple sinks,
# apply ML models, etc.
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#
#   1. Submit the job from inside the spark-master container:
#        docker exec spark-master spark-submit \
#          /opt/spark/jobs/structured_streaming/05_foreach_batch.py
#
#   2. Watch the console for per-batch output.
#   3. Press Ctrl+C to stop.
#
# What to Expect:
#   - Every 3 seconds a new micro-batch is triggered.
#   - The batch ID and the rows in that batch are printed to the console.
#   - You will see output like:
#       >>> Processing Batch ID: 0
#       +---------+-----+
#       |timestamp|value|
#       +---------+-----+
#       | ...     |  0  |
# =============================================================================

from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder \
    .appName("Example5_ForeachBatch") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 4) \
    .load()


# -----------------------------------------------------------------------
# Define the function that will be called for every micro-batch.
# batch_df  : a regular (static) DataFrame containing this batch's rows
# batch_id  : a monotonically increasing integer identifying the batch
# -----------------------------------------------------------------------
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    print(f"\n>>> Processing Batch ID: {batch_id}")
    print(f"    Row count in this batch: {batch_df.count()}")
    batch_df.show(truncate=False)


query = rate_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

query.awaitTermination()
