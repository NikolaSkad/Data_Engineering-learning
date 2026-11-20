import argparse

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql import functions as F  # type: ignore

parser = argparse.ArgumentParser()

parser.add_argument("--input-green", required=True)
parser.add_argument("--input-yellow", required=True)
parser.add_argument("--output", required=True)

args = parser.parse_args()

# fixirane vrednosti za lakse lokalno pokretanje i debug
# parser.add_argument("--input-green")
# parser.add_argument("--input-yellow")
# parser.add_argument("--output")

# args = parser.parse_args(
#     [
#         "--input-green",
#         "../data/pq/green/2020",
#         "--input-yellow",
#         "../data/pq/yellow/2020",
#         "--output",
#         "../data/report-2020",
#     ]
# )

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder.appName("job_revenue").getOrCreate()

# ucitavanje read and yellow parquet
df_green_raw = spark.read.option("recursiveFileLookup", "true").parquet(input_green)
df_yellow_raw = spark.read.option("recursiveFileLookup", "true").parquet(input_yellow)

# preimenovanje kolona da imaju iste nazive
df_green = df_green_raw.withColumnRenamed(
    "lpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
df_yellow = df_yellow_raw.withColumnRenamed(
    "tpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# provera pomocu preseka as & -> vraca sta je u oba skupa
# print(set(df_green.columns) & set(df_yellow.columns))
# print(len(set(df_green.columns) & set(df_yellow.columns)))

# lista kolona koje postoje u oba source-a
common_columns: list[str] = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

# dodavanje zajednicke kolone service_type sa vrednoscu green ili yellow
df_green_st = df_green.select(common_columns).withColumn("service_type", F.lit("green"))
df_yellow_st = df_yellow.select(common_columns).withColumn(
    "service_type", F.lit("yellow")
)

# spajanje dataframe-ova po zajenickim kolonama (unionAll)
df_trips = df_green_st.union(df_yellow_st)

# provera
# print(df_trips.groupBy("service_type").count().show())

# kreiranje tabele
df_trips.createOrReplaceTempView("trips_data")

# provera
# spark.sql("SELECT COUNT(*) FROM trips_data").show()

# SQL upit koji grupise po PULocationID, pickup_datetime i service_type
# i racuna zbir i prosek
df_result = spark.sql("""
SELECT
    -- grupisanje
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    service_type,

    -- zbir
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- prosek
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# upis rezultata kao parquet
df_result.write.parquet(output, mode="overwrite")
