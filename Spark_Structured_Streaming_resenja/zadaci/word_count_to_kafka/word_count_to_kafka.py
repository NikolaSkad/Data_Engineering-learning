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

# bilo koji od ova dva, isto rade
# lines = comments.selectExpr("CAST(value AS STRING) AS value")
lines = comments.select(col("value").cast("string").alias("value"))

words = comments.select(
    explode(split(lower(col("value").cast("string")), "[^\\p{L}\\p{N}]+")).alias("word")
).filter(~col("word").isin(STOP_WORDS) & (col("word") != ""))

word_counts = words.groupBy("word").count()

query = word_counts.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
