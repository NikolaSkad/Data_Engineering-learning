from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split
from stopwords import STOP_WORDS


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession.builder.appName("SSS - Kafka Wordcount").getOrCreate()

quiet_logs(spark)

comments = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:19092")
    .option("subscribe", "subreddit-politics,subreddit-bluesky-en")
    .load()
)

words = comments.select(
    explode(split(lower(col("value").cast("string")), "[^\\p{L}\\p{N}]+")).alias("word")
).filter(~col("word").isin(STOP_WORDS) & (col("word") != ""))

word_counts = words.groupBy("word").count()

# Kafka sink zahteva kolone key i value (string ili binary)
kafka_output = word_counts.select(
    col("word").alias("key"),
    col("count").cast("string").alias("value"),
)

query = (
    kafka_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:19092")
    .option("topic", "word-counts")
    # offseti i stanje brojanja, da posle restarta nastavi gde je stao
    .option("checkpointLocation", "/tmp/checkpoints/word_count_to_kafka")
    .outputMode("update")
    .start()
)
query.awaitTermination()
