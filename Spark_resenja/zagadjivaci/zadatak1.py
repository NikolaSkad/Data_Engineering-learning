import os

from pyspark.sql import SparkSession


# isključi INFO logove, da se u izlazu vide samo rezultati
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# Spark sesija = ulaz u Spark (ime se vidi u Spark UI na localhost:8080)
spark = SparkSession.builder.appName("Python Spark SQL - Zagadjivaci 1").getOrCreate()

quiet_logs(spark)

# adresa HDFS-a (hdfs://namenode:9000) dolazi iz hadoop.env preko env_file u compose-u
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HDFS_CSV_PATH = "/user/zagadjivaci/zagadjivaci.csv"
csv_url = HDFS_NAMENODE + HDFS_CSV_PATH

# učitaj CSV u DataFrame
# sep=";"                  -> kolone su razdvojene sa ; (ne zarezom)
# encoding="windows-1250"  -> da š, č, ć, ž, đ budu ispravni
# header=True              -> prvi red su nazivi kolona, ne podaci
df = spark.read.csv(csv_url, sep=";", encoding="windows-1250", header=True)

# šema: nazivi i tipovi kolona (sve je string, jer nismo tražili inferSchema)
df.printSchema()

# prvih 5 redova
df.show(5)

# sve različite opštine (select bira kolonu, distinct izbacuje duplikate)
df.select("Opština postrojenja").distinct().show()
