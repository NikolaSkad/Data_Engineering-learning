# Zadatak 2: brojanje reči sa subreddit-politics i subreddit-bluesky-en -> topik word-counts
# Ne pokretati celu skriptu odjednom, komande se kucaju ručno liniju po liniju


# ===== Podizanje okruženja =====

cd ~/Desktop/DE_Praksa/moja_resenja/Spark_Structured_Streaming_resenja/docker-spark
# diže i bluesky-producer, on puni subreddit-bluesky-en
docker compose up -d
# ako je ostao stari Spark program, gasi se (dva drivera pod emulacijom zagušuju kontejner)
docker exec spark-master pkill -f SparkSubmit


# ===== Pokretanje =====

# tab 1: Spark, rezultat ide u Kafku pa se ovde ne ispisuje ništa (gasi se sa Ctrl + C)
# shuffle.partitions=2 umesto 200 jer je pod emulacijom presporo, --py-files jer je stopwords.py u primeri/
docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.shuffle.partitions=2 --py-files /spark/primeri/stopwords.py /spark/zadaci/word_count_to_kafka/word_count_to_kafka.py


# ===== Provera =====

# tab 2: consumer na izlaznom topiku, ispisuje reč i broj
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic word-counts --property print.key=true
# tab 3: ručno slanje na subreddit-politics (umesto Reddit producera), svaka linija je jedna poruka
docker exec -it kafka1 kafka-console-producer --broker-list kafka1:19092 --topic subreddit-politics
