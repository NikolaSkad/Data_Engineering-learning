from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, lower, split
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
    .option("subscribe", "subreddit-politics")
    .load()
)

words = comments.select(explode(split(comments.value, " ")).alias("word")).filter(
    ~lower(col("word")).isin(STOP_WORDS)
)

wordCounts = words.groupBy("word").count().orderBy(desc("count")).limit(10)

query = wordCounts.writeStream.outputMode("complete").format("console").start()

# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='2 seconds') \
#     .start()

query.awaitTermination()
