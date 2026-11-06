# Ne pokretati celu skriptu odjednom, komande se kucaju ručno liniju po liniju
# Komande na Macu se pokreću iz foldera docker-specification


# ===== Pokretanje klastera =====

# Docker Desktop mora da radi (inače "failed to connect to the docker API")
open -a Docker
docker info

# zookeeper, 3 brokera, rest proxy, UI i producer (šalje pek-sha.csv na topik pek-sha na svakih 300 ms)
docker compose up -d
# UI za topike: http://localhost:8000


# ===== Build i pokretanje zadatka =====

# posle SVAKE izmene Java koda ponovo build (Maven pravi novi jar, ~6 s)
docker compose up -d --build producer_and_consumers

# Kafka Streams aplikacije se pokreću u producer_and_consumers (tu je jar), NE u kafka1
docker exec -it producer_and_consumers bash
java -cp /usr/consumers/kafka-streams-examples-0.0.1-SNAPSHOT.jar com.example.kafka_streams_examples.zadaci.letovipodanu.LetoviPoDanuZadatak
# rezultat se ispisuje ovde (System.out), nema izlaznog topika
# isti dan se ispisuje više puta sa sve većim brojem (KTable se ažurira)
# ispis kasni do ~30 s: count kešira rezultate i pušta ih na commit interval


# ===== Provera internih topika (novi tab) =====

# count je stateful, pa Kafka Streams sam pravi interne topike:
#   ...-repartition = zapisi preraspodeljeni po novom ključu (datumu) posle groupBy
#   ...-changelog   = rezervna kopija stanja (broj po danu), da se ne izgubi kad aplikacija padne
docker exec -it kafka1 bash
kafka-topics --bootstrap-server localhost:19092 --list


# ===== Gašenje =====

docker compose down
