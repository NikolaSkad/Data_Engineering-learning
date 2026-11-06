# Ne pokretati celu skriptu odjednom, komande se kucaju ručno liniju po liniju
# ZooKeeper, brokeri, produceri i consumeri ostaju da rade, pa svaki ide u svoj tab
# Sve bin/... komande se pokreću iz Kafka foldera


# ===== Instalacija (Kafka 2.8.1, Java 8 ili 11) =====

# putanja ne sme imati razmak, Kafka skripte pucaju (DE Praksa -> DE_Praksa)
cd ~/Desktop/DE_Praksa/04_Kafka/kafka_instalacija
# stare verzije su samo na archive.apache.org, na Macu nema wget pa curl -O
curl -O https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xzf kafka_2.12-2.8.1.tgz
rm kafka_2.12-2.8.1.tgz
cd kafka_2.12-2.8.1


# ===== Pokretanje klastera =====

# tab 1: ZooKeeper (port 2181)
bin/zookeeper-server-start.sh config/zookeeper.properties
# tab 2: broker 0 (port 9092), gore je kad u logu piše "started (kafka.server.KafkaServer)"
bin/kafka-server-start.sh config/server.properties


# ===== Topici =====
# svaka kafka-topics.sh komanda mora imati --zookeeper ili --bootstrap-server (gde je klaster)

# kreirati topik test sa jednom particijom i bez replikacije
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
# prikaz svih topika
bin/kafka-topics.sh --list --zookeeper localhost:2181
# brisanje, ime topika ide uz --topic (briše se u pozadini, --list ga par sekundi još pokazuje)
bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092


# ===== Razmena poruka =====

# tab 3: producer, svaka otkucana linija je jedna poruka
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
# tab 4: consumer od prve poruke (ne završava sam, čeka nove poruke, izlaz Ctrl + C)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning


# ===== Key / value poruke =====

# producer: parse.key je opcija producera (kafka-console-producer.sh --help), kuca se kljuc:vrednost
# npr. user1:kupio patike
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=: --topic test
# consumer: print.* su opcije consumera (kafka-console-consumer.sh --help)
# isti ključ uvek završi u istoj particiji, pa je redosled za taj ključ garantovan
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning --property print.key=true --property print.partition=true --property print.offset=true


# ===== Kafka multi-broker =====

cp config/server.properties config/server-1.properties
cp config/server.properties config/server-2.properties
# u nano izmeniti (redom za server-1 i server-2):
#   broker.id=1                       broker.id=2
#   listeners=PLAINTEXT://:9093       listeners=PLAINTEXT://:9094
#   log.dirs=/tmp/kafka-1             log.dirs=/tmp/kafka-2
nano config/server-1.properties
nano config/server-2.properties

# (preporuka) __consumer_offsets na sva 3 brokera, inače sa RF=1 završi na jednom
# i consumer visi kad baš taj broker padne; radi se pre prvog pokretanja brokera
sed -i '' 's/^offsets.topic.replication.factor=1/offsets.topic.replication.factor=3/' config/server.properties config/server-1.properties config/server-2.properties
grep offsets.topic.replication.factor config/server*.properties

# tab 5 i tab 6: dva nova brokera (u odvojenim tabovima umesto & da se vide logovi)
bin/kafka-server-start.sh config/server-1.properties
bin/kafka-server-start.sh config/server-2.properties


# ===== Rad sa repliciranim topikom =====

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic my-replicated-topic
# Leader = ko prima upise, Replicas = gde su kopije (redosled je nasumičan), Isr = sinhronizovane replike
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-replicated-topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic my-replicated-topic


# ===== Simulacija otkaza =====
# ubija se broker koji je LIDER u --describe (kod predavača 2, kod mene je bio i 0)

# PID je drugi broj u redu, posle korisničkog imena
ps aux | grep server-2.properties | grep -v grep
# umesto PID ide taj broj, npr. kill -9 19644
kill -9 PID
# isto u jednoj komandi
pkill -9 -f server-2.properties

# posle kill -9 sačekati ~18 s (ZooKeeper session timeout), tek onda se bira novi lider
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
# navesti sve brokere, klijent proba redom dok ne nađe živog
# (sa samo localhost:9092 consumer puca ako je ubijen broker 0)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --from-beginning --topic my-replicated-topic

# ako consumer visi bez ispisa: Leader: none znači da je interni topik bio na ubijenom brokeru
bin/kafka-topics.sh --describe --topic __consumer_offsets --bootstrap-server localhost:9093


# ===== Consumer grupe =====

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic my-topic
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic
# pokrenuti ovo u više tabova, svaka particija ide tačno jednom consumeru u grupi
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --consumer-property group.id=g1

bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-topic
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
# koji consumer čita koju particiju i koliko poruka kasni (LAG)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group g1


# ===== Gašenje i reset od nule =====

# gasi sve brokere, pa ZooKeeper
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
# provera, ne sme ništa da ispiše
ps aux | grep -E "kafka|zookeeper" | grep -v grep
# brisanje svih podataka (topici, poruke), config fajlovi ostaju
rm -rf /tmp/kafka-logs /tmp/kafka-1 /tmp/kafka-2 /tmp/zookeeper
