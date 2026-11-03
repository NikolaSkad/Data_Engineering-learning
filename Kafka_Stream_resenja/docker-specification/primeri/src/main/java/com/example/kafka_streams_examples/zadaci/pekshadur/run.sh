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
java -cp /usr/consumers/kafka-streams-examples-0.0.1-SNAPSHOT.jar com.example.kafka_streams_examples.zadaci.pekshadur.PekShaDurZadatak
# radi dok se ne zaustavi sa Ctrl + C; peek ispisuje "IZLAZ: ..." za svaki zapis


# ===== Provera izlaznog topika (novi tab) =====

# kafka-* komande se pokreću u brokeru kafka1, adresa je localhost:19092
docker exec -it kafka1 bash

# da li je topik napravljen (.to() ga napravi sam pri prvom upisu)
kafka-topics --bootstrap-server localhost:19092 --list

# sve poruke iz topika od početka, očekuje se npr. CA1517#125#3730 (broj leta#minuti#cena)
kafka-console-consumer --bootstrap-server localhost:19092 --topic pek-sha-dur --from-beginning
# bez --from-beginning prikazuje samo poruke koje stignu posle pokretanja consumer-a
kafka-console-consumer --bootstrap-server localhost:19092 --topic pek-sha-dur

# ulazni topik, za poređenje (red CSV-a bez ID-a, kolone spojene sa #)
kafka-console-consumer --bootstrap-server localhost:19092 --topic pek-sha --from-beginning


# ===== Gašenje =====

docker compose down
