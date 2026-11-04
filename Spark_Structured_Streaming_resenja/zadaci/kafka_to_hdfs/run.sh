# Zadatak 3: word-counts -> max broj po reči u prozoru od 1 minuta -> HDFS (parquet)
# Ne pokretati celu skriptu odjednom, komande se kucaju ručno liniju po liniju
# Ceo lanac: Bluesky -> producer (zad. 1) -> subreddit-bluesky-en + subreddit-politics -> Spark (zad. 2) -> word-counts -> Spark (zad. 3) -> HDFS


# ===== tab 1: podizanje okruženja =====

cd ~/Desktop/DE_Praksa/moja_resenja/Spark_Structured_Streaming_resenja/docker-spark
# diže Kafku, Spark, HDFS i bluesky_producer (zadatak 1)
docker compose up -d
# posle ~30 s svi treba da su Up, namenode i datanode1 (healthy) može da potraje minut
docker compose ps
# provera da producer šalje postove (treba da piše Received: ...)
docker logs --tail 5 bluesky_producer


# ===== tab 2: zadatak 2 (brojanje reči -> word-counts) =====

# ostaje da radi, posle startnih logova ne ispisuje ništa jer rezultat ide u Kafku
docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.shuffle.partitions=2 --py-files /spark/primeri/stopwords.py /spark/zadaci/word_count_to_kafka/word_count_to_kafka.py


# ===== tab 3: zadatak 3 (word-counts -> HDFS) =====

# pokreće se tek kad tab 2 krene (~20 s), i on ostaje da radi bez ispisa
docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.shuffle.partitions=2 /spark/zadaci/kafka_to_hdfs/kafka_to_hdfs.py


# ===== tab 4: provera =====

# odmah: zadatak 2 piše u word-counts (reč i broj), izlaz Ctrl + C
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic word-counts --property print.key=true
# posle 2-3 min (prozor 1 min + watermark 30 s): lista parquet fajlova (ili http://localhost:9870 -> Utilities -> Browse the file system)
# prvi fajl od ~578 B je prazan (Batch 0 upiše samo šemu), to je normalno
docker exec namenode hdfs dfs -ls /sss/word_counts_per_minute
# sadržaj fajlova, najnoviji prozori i najveći brojevi prvi (local[1] da ne smeta streaming poslovima)
docker exec -it spark-master /spark/bin/spark-submit --master "local[1]" /spark/zadaci/kafka_to_hdfs/read_hdfs.py
# opciono: ručno slanje na subreddit-politics, svaka linija je jedna poruka
docker exec -it kafka1 kafka-console-producer --broker-list kafka1:19092 --topic subreddit-politics


# ===== Gašenje =====

# prvo Ctrl + C u tabovima 2 i 3, pa u tabu 1:
docker compose down
# podaci ostaju u volumenima: zadatak 3 posle restarta nastavlja (checkpoint na HDFS-u),
# zadatak 2 broji od nule (checkpoint u /tmp kontejnera se briše sa down)
