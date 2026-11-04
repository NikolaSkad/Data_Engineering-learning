# Zadatak 1: Bluesky producer (Jetstream -> Kafka topik subreddit-bluesky-en)
# Ne pokretati celu skriptu odjednom, komande se kucaju ručno liniju po liniju


# ===== Pokretanje u kontejneru =====

# compose komande se pokreću iz docker-spark foldera
cd ~/Desktop/DE_Praksa/moja_resenja/Spark_Structured_Streaming_resenja/docker-spark
# --build ponovo pravi sliku, da izmene u producer.py uđu u kontejner (kafka1 se podigne sam zbog depends_on)
docker compose up -d --build bluesky-producer
# logovi producera, Ctrl + C prekida samo praćenje, producer radi dalje
docker logs -f bluesky_producer


# ===== Pokretanje lokalno (bez kontejnera) =====

# iz moja_resenja foldera, sa aktivnim .venv (KAFKA_BROKER je podrazumevano 127.0.0.1:9092)
cd ~/Desktop/DE_Praksa/moja_resenja
source .venv/bin/activate
python Spark_Structured_Streaming_resenja/zadaci/bluesky_producer/producer.py


# ===== Provera =====

# tab 2: consumer, ispisuje did autora i tekst posta
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic subreddit-bluesky-en --property print.key=true
