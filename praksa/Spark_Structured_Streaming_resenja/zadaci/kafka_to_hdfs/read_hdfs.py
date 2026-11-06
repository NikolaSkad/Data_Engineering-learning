# Provera zadatka 3: batch čitanje parquet fajlova sa HDFS-a (nije streaming)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Read word counts from HDFS").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/sss/word_counts_per_minute")

print("Ukupno redova:", df.count())
df.orderBy(col("window_start").desc(), col("max_count").desc()).show(20, truncate=False)
