# =============================================================================
# Example 4: Stateful Operation — Deduplication with Watermark
# =============================================================================
#
# Structured Streaming tracks state across micro-batches to remove duplicate
# rows within a watermark window.  Without a watermark, state would grow
# forever; the watermark tells Spark when it is safe to discard old state.
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#
#   1. Submit the job from inside the spark-master container:
#        docker exec spark-master spark-submit \
#          /opt/spark/jobs/structured_streaming/04_stateful_deduplication.py
#
#   2. Observe the console output.
#   3. Press Ctrl+C to stop.
#
# What to Expect:
#   - The rate source produces unique (timestamp, value) pairs, so in this
#     example every row passes the dedup check.
#   - To simulate true duplicates, you can replace the rate source with a
#     socket or file source that intentionally repeats values.
#   - The key takeaway is knowing HOW to express deduplication in a stream;
#     the pattern is identical regardless of the source.
# =============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example4_StatefulDedup") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Generate a small stream — each row has a unique (timestamp, value)
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 3) \
    .load()

# -----------------------------------------------------------------------
# dropDuplicates removes rows with the same "value" seen within the
# watermark window.  The watermark on "timestamp" bounds the state size.
# -----------------------------------------------------------------------
deduped_df = rate_df \
    .withWatermark("timestamp", "10 seconds") \
    .dropDuplicates(["value"])

query = deduped_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

query.awaitTermination()
