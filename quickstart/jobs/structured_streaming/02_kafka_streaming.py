# =============================================================================
# Example 2: Kafka Streaming (Consumer)
# =============================================================================
#
# Reads a stream of messages from a Kafka topic, casts the binary value
# to a string, and prints each micro-batch to the console.
#
# Required Kafka Dependency:
#   The cluster image is apache/spark:4.1.1-scala2.13-java21-python3-ubuntu.
#   Use the Scala 2.13 Kafka connector matching Spark 4.1.x:
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
#   Or add to quickstart/conf/spark-defaults.conf (applied to every container):
#     spark.jars.packages  org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
#
# Docker Cluster — Steps to Run:
#   Prerequisites: docker compose up already running (cd quickstart && docker compose up -d)
#
#   Option A — Kafka container in the same Docker network
#     The docker-compose.yml does NOT include a Kafka service by default.
#     Add one (e.g., bitnami/kafka) to docker-compose.yml or run a separate
#     Kafka container on the spark-network network, then update
#     kafka.bootstrap.servers below to match the Kafka service name.
#
#   Option B — External Kafka (host machine)
#     Set kafka.bootstrap.servers to "host.docker.internal:9092" so the
#     spark-master container can reach the Kafka broker on the host.
#
#   1. Create the Kafka topic (run inside the Kafka container or with local tools):
#        kafka-topics.sh --create --topic test-topic \
#          --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
#   2. Submit the Spark job from inside the spark-master container:
#        docker exec spark-master spark-submit \
#          --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
#          /opt/spark/jobs/structured_streaming/02_kafka_streaming.py
#
#   3. In a separate terminal, produce messages to the topic:
#        kafka-console-producer.sh --topic test-topic \
#          --bootstrap-server localhost:9092
#      Type any text and press Enter.
#
#   4. Press Ctrl+C (or docker exec spark-master kill <pid>) to stop the stream.
#
# What to Expect:
#   - Each message published to "test-topic" appears in the console output.
#   - The "value" column shows the message content as a plain string.
#   - Messages are grouped into micro-batches as they arrive.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Example2_KafkaStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read messages from a Kafka topic
# startingOffsets="latest" means only new messages after the stream starts
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# The "value" column from Kafka is binary; cast it to a readable string
messages_df = kafka_df.select(
    col("timestamp"),
    col("value").cast("string").alias("message")
)

# Print each micro-batch to the console
query = messages_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
