# =============================================================================
# Example 1: Basic Streaming using Rate Source
# =============================================================================
#
# The "rate" source generates rows automatically at a fixed rate.
# Each row has a timestamp and a monotonically increasing value.
# This is the easiest way to explore Structured Streaming without
# any external system.
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#
#   1. Submit the job from inside the spark-master container:
#        docker exec spark-master spark-submit \
#          /opt/spark/jobs/structured_streaming/01_rate_source_streaming.py
#
#   2. Watch the console output for streaming micro-batches.
#   3. Press Ctrl+C in the terminal running docker exec to stop the stream.
#
# What to Expect:
#   - Every second, a new micro-batch appears in the console.
#   - Each batch prints rows with a timestamp and an auto-incrementing integer value.
#   - You will see "Batch: 0", "Batch: 1", etc., showing micro-batch progression.
# =============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example1_RateSource") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from the built-in "rate" source
# rowsPerSecond controls how many rows are generated each second
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

# rate_df schema: timestamp (TimestampType), value (LongType)
print("Schema of rate source:")
rate_df.printSchema()

# Write the stream to the console in append mode
query = rate_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

# Keep the stream running until manually stopped (Ctrl+C)
query.awaitTermination()
