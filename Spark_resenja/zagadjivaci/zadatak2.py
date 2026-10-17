import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# F.sum, F.col... -> Spark funkcije; sum(...) bez F. ostaje Python-ov
from pyspark.sql.window import Window

# Pokretanje (Postgres drajver se daje kroz --packages, ne u kodu):
# /spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.13 /home/zagadjivaci/zadatak2.py


# isključi INFO logove, da se u izlazu vide samo rezultati
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession.builder.appName("Python Spark SQL - Zagadjivaci 2").getOrCreate()

quiet_logs(spark)

# adresa HDFS-a (hdfs://namenode:9000) dolazi iz hadoop.env
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HDFS_CSV_PATH = "/user/zagadjivaci/zagadjivaci.csv"
csv_url = HDFS_NAMENODE + HDFS_CSV_PATH

# isto učitavanje kao u zadatku 1 (svaka Spark aplikacija kreće od nule)
df = spark.read.csv(csv_url, sep=";", encoding="windows-1250", header=True)

godine = [str(g) for g in range(2010, 2018)]

# kolone 2010-2017: string -> double (prazne ćelije postaju null)
# isto ime u withColumn = zamena kolone, df = df... = nadogradnja istog DataFrame-a
for godina in godine:
    df = df.withColumn(godina, F.col(godina).cast("double"))

df.printSchema()

# 1. Preduzeća koja su 2015. proizvela preko 10 tona otpada
# groupBy po firmi -> zbir za 2015 -> filter po zbiru (alias da bi where znao ime)
df.groupBy("PIB", "Preduzece").agg(F.sum("2015").alias("ukupno_tona")).where(
    "ukupno_tona > 10"
).orderBy(F.col("ukupno_tona").desc()).show(truncate=False)

# 2. Pretežne delatnosti sa preko 10 postrojenja
# countDistinct, ne count: isto postrojenje ima više redova (po jedan za svaki otpad)
df.groupBy("Pretežna delatnost").agg(
    F.countDistinct("Postrojenje").alias("broj_postrojenja")
).where("broj_postrojenja > 10").orderBy(F.col("broj_postrojenja").desc()).show(
    truncate=False
)

# 3. Top 5 zagađivača u oblasti Proizvodnja komunikacione opreme
# 2630 = komunikaciona oprema (u fajlu samo 1 firma), 4690 = test sa više firmi
DELATNOSTI = ["2630", "4690"]
# null -> 0 samo u kopiji, original df ostaje sa null (treba za unpivot i skok)
df_nule = df.fillna(0, subset=godine)
# ukupno = 2010 + 2011 + ... + 2017 u istom redu (Python sum sabira kolone)
# Python sum, ne F.sum: F.sum je agregacija jedne kolone kroz redove
# F.lit(0) = početna vrednost kao kolona; bez nje sum počinje od broja 0,
# pa mypy misli da rezultat može biti 0 umesto Column
df_nule = df_nule.withColumn("ukupno", sum((F.col(g) for g in godine), F.lit(0)))

# filter po šifri delatnosti -> zbir po firmi -> sortiraj od najveće -> prvih 5
for delatnost in DELATNOSTI:
    df_nule.where(F.col("Pretežna delatnost").startswith(delatnost)).groupBy(
        "PIB", "Preduzece"
    ).agg(F.sum("ukupno").alias("ukupno_tona")).orderBy(
        F.col("ukupno_tona").desc()
    ).limit(5).show(truncate=False)

# 4. Najveći zagađivači po gradovima
# (po opštinama, jer fajl ima samo Beograd, pa Oblast ima samo 1 vrednost)

# jedan red po firmi u svakoj opštini, sa ukupnom količinom svih godina
po_opstinama = df_nule.groupBy("Opština postrojenja", "PIB", "Preduzece").agg(
    F.sum("ukupno").alias("ukupno_tona")
)
# prozor: svaka opština je posebna grupa, firme sortirane od najveće ka najmanjoj
w = Window.partitionBy("Opština postrojenja").orderBy(F.col("ukupno_tona").desc())
najveci_po_opstinama = (
    # redni broj firme unutar njene opštine (1 = najveća)
    po_opstinama.withColumn("rang", F.row_number().over(w))
    # zadrži samo najveću u svakoj opštini
    .where("rang = 1")
    # rang je bio samo pomoćna kolona
    .drop("rang")
    .orderBy("Opština postrojenja")
)
najveci_po_opstinama.show(truncate=False)

# 5. Kolone 2010-2017 -> jedna kolona godina + jedna kolona kolicina (unpivot)
# '2010', `2010`, '2011', `2011`, ...  ('tekst' -> godina, `kolona` -> kolicina)
parovi = ", ".join(f"'{g}', `{g}`" for g in godine)
# stack(8, ...): od svakog reda napravi 8 redova, po jedan za svaku godinu
stack_izraz = f"stack({len(godine)}, {parovi}) as (godina, kolicina)"

df_dugi = (
    # original df (sa null), ne df_nule: za skok treba razlika "nije prijavila" / "0"
    df.select(
        "Opština postrojenja",
        "PIB",
        "Preduzece",
        "Postrojenje",
        "Indeksni broj",
        "Opis otpada",
        F.expr(stack_izraz),
    )
    # izbaci godine u kojima firma nije prijavila otpad
    .where("kolicina is not null")
    # '2015' (string) -> 2015 (broj), za sortiranje i prethodnu godinu
    .withColumn("godina", F.col("godina").cast("int"))
)

df_dugi.printSchema()
df_dugi.orderBy("PIB", "Postrojenje", "Indeksni broj", "godina").show(
    10, truncate=False
)
# provera: treba da bude jednako broju popunjenih ćelija u godinama (8300)
print("broj redova posle unpivota:", df_dugi.count())


# 6. Najveći skok proizvodnje neke vrste otpada u odnosu na prethodnu godinu
# prozor: ista firma + isto postrojenje + ista vrsta otpada, godine rastuće
# (prethodni red u prozoru = prethodna godina)
w_godine = Window.partitionBy("PIB", "Postrojenje", "Indeksni broj").orderBy("godina")
# lag = vrednost iz prethodnog reda u prozoru (za prvi red grupe je null)
df_skok = df_dugi.withColumn("prethodna", F.lag("kolicina").over(w_godine))
df_skok = df_skok.withColumn("prethodna_godina", F.lag("godina").over(w_godine))
# skok = koliko je više nego prethodne godine (negativno = pad)
df_skok = df_skok.withColumn("skok", F.col("kolicina") - F.col("prethodna"))
# samo uzastopne godine (2014 -> 2015, ne 2012 -> 2015); ujedno izbacuje null redove
df_skok = df_skok.where(F.col("godina") - F.col("prethodna_godina") == 1)
# najveći skok prvi
df_skok = df_skok.orderBy(F.col("skok").desc())
df_skok.select(
    "PIB",
    "Preduzece",
    "Postrojenje",
    "Indeksni broj",
    "Opis otpada",
    "prethodna_godina",
    "godina",
    "prethodna",
    "kolicina",
    "skok",
).show(10, truncate=False)

# 7. Rezultat sačuvati u relacionu bazu (Postgres kontejner iz docker-compose.yml)
# url: jdbc:postgresql://<ime servisa>:<port>/<POSTGRES_DB>
# driver: eksplicitno, jer Spark drajver iz --packages ne nađe sam ("No suitable driver")
# mode("overwrite"): pri ponovnom pokretanju pregazi tabelu (default bi pukao jer postoji)
(
    df_skok.write.format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://postgres:5432/zagadjivaci")
    .option("dbtable", "zagadjivaci_skok_otpada")
    .option("user", "spark")
    .option("password", "spark")
    .mode("overwrite")
    .save()
)
