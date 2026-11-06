from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.functions import max as spark_max

KAFKA_BROKER = "kafka1:19092"
INPUT_TOPIC = "word-counts"
OUTPUT_PATH = "hdfs://namenode:9000/sss/word_counts_per_minute"
CHECKPOINT_PATH = "hdfs://namenode:9000/sss/checkpoints/kafka_to_hdfs"


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession.builder.appName("SSS - Kafka to HDFS").getOrCreate()

quiet_logs(spark)

# Kolone: key, value (binary), topic, partition, offset, timestamp, timestampType
messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .load()
)

# key -> string "word", value -> int "count", zadrži "timestamp"
word_counts = messages.select(
    col("key").cast("string").alias("word"),
    col("value").cast("string").cast("int").alias("count"),
    col("timestamp"),
)

# watermark na "timestamp" (pre groupBy)
with_watermark = word_counts.withWatermark("timestamp", "30 seconds")

# groupBy po prozoru od 1 minuta i reči, max(count) kao "max_count"
windowed = with_watermark.groupBy(
    window(col("timestamp"), "1 minute"),
    col("word"),
).agg(spark_max("count").alias("max_count"))

# window.start i window.end u posebne kolone, pa reč i max_count
result = windowed.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("word"),
    col("max_count"),
)

query = (
    result.writeStream.format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()
