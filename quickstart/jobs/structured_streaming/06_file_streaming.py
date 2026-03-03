# =============================================================================
# Example 6: File Streaming (JSON)
# =============================================================================
#
# Spark monitors a folder for NEW JSON files and processes each one as a
# micro-batch.  The schema must be declared explicitly — Spark cannot infer
# schemas in streaming mode.
#
# IMPORTANT RULES for File Streaming:
#   1. Spark only processes files that appear AFTER the stream starts.
#      Files already present in the folder when the stream starts are
#      processed only if "latestFirst" or checkpoint recovery allows it.
#   2. The schema of all incoming files must stay consistent.
#      Adding/removing columns mid-stream causes errors.
#   3. The input directory MUST exist before the stream starts.
#   4. Do NOT modify or delete files after Spark has detected them;
#      only append new files.
#
# Folder Setup (run once before starting the stream):
#   mkdir -p /tmp/streaming_input
#   mkdir -p /tmp/streaming_checkpoint
#
# Sample JSON file to place in /tmp/streaming_input/data1.json:
#   {"id": 1, "name": "Alice",   "amount": 150.0}
#   {"id": 2, "name": "Bob",     "amount": 200.5}
#   {"id": 3, "name": "Charlie", "amount": 75.0}
#
# Each line is a separate JSON object (newline-delimited JSON / NDJSON).
# Copy a new file (data2.json, data3.json …) into the folder while the
# stream is running to see Spark pick it up automatically.
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#   NOTE: /tmp paths below are INSIDE the spark-master container.
#
#   1. Create the input and checkpoint directories inside the container:
#        docker exec spark-master mkdir -p /tmp/streaming_input /tmp/streaming_checkpoint
#
#   2. Copy the sample JSON file into the container:
#        docker cp quickstart/jobs/structured_streaming/sample_data/orders_sample.json \
#          spark-master:/tmp/streaming_input/orders_batch1.json
#
#   3. Submit the job:
#        docker exec spark-master spark-submit \
#          /opt/spark/jobs/structured_streaming/06_file_streaming.py
#
#   4. While the stream is running, inject more files to trigger new batches:
#        docker cp quickstart/jobs/structured_streaming/sample_data/orders_sample.json \
#          spark-master:/tmp/streaming_input/orders_batch2.json
#
#   5. Press Ctrl+C to stop.
#
# What to Expect:
#   - Each new file dropped into /tmp/streaming_input/ creates a new micro-batch.
#   - The console prints the id, name, and amount columns from every new file.
#   - Already-processed files are tracked via the checkpoint — they will NOT
#     be reprocessed if the stream is restarted.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Example6_FileStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------
# Schema MUST be defined explicitly for streaming file sources.
# It must match the structure of every JSON file placed in the folder.
# -----------------------------------------------------------------------
order_schema = StructType([
    StructField("id",     IntegerType(), nullable=False),
    StructField("name",   StringType(),  nullable=True),
    StructField("amount", DoubleType(),  nullable=True),
])

INPUT_DIR      = "/tmp/streaming_input"
CHECKPOINT_DIR = "/tmp/streaming_checkpoint"

# Read new JSON files as they appear in INPUT_DIR
orders_stream = spark.readStream \
    .schema(order_schema) \
    .format("json") \
    .option("maxFilesPerTrigger", 1) \
    .load(INPUT_DIR)

# Simple transformation: flag large orders
from pyspark.sql.functions import col, when

enriched = orders_stream.withColumn(
    "order_size",
    when(col("amount") >= 100, "large").otherwise("small")
)

# Write to console; checkpoint tracks which files have been processed
query = enriched.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
