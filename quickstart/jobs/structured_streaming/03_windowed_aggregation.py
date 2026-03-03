# =============================================================================
# Example 3: Windowed Aggregation with Watermark
# =============================================================================
#
# Counts events that fall within tumbling 10-second time windows.
# A watermark of 5 seconds tells Spark how late data can arrive before
# it is dropped from the aggregation state.
# Output mode "update" prints only the windows that changed in each batch.
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#
#   1. Submit the job from inside the spark-master container:
#        docker exec spark-master spark-submit \
#          /opt/spark/jobs/structured_streaming/03_windowed_aggregation.py
#
#   2. Watch the console for window counts that grow over time.
#   3. Press Ctrl+C to stop.
#
# What to Expect:
#   - Console shows rows like: window(start, end) | count
#   - Each 10-second window accumulates the rows generated in that interval.
#   - Once the watermark passes a window's end time, that window is finalized
#     and will no longer be updated.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

spark = SparkSession.builder \
    .appName("Example3_WindowedAggregation") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Generate ~2 rows per second using the rate source
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

# Apply a watermark: late data arriving more than 5 seconds after
# the event time will be ignored
watermarked_df = rate_df.withWatermark("timestamp", "5 seconds")

# Count events in tumbling 10-second windows on the event timestamp
windowed_counts = watermarked_df.groupBy(
    window(col("timestamp"), "10 seconds")
).count()

# "update" mode: emit only rows whose count changed in this micro-batch
query = windowed_counts.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
