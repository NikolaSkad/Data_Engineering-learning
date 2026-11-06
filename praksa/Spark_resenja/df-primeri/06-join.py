#!/usr/bin/python
# primer iz kursa: radi u Spark-u, mypy je strožiji od pyspark tipova
# mypy: ignore-errors

from pyspark.sql import SparkSession
from pyspark.sql.types import *


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession.builder.appName("Python Spark SQL - Join").getOrCreate()

quiet_logs(spark)

# TODO : optimizing joins
