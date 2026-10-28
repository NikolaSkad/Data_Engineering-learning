# Kafka danas (4.x) – kako bi se kurs radio sada

Kurs je rađen na Kafki 2.8.1 sa ZooKeeperom. Ovde je **isti tok kursa**, ali onako kako se radi danas: Kafka 4.3, KRaft (bez ZooKeepera), Docker i Python klijent.

**Sadržaj**

1. [Šta se promenilo](#1-šta-se-promenilo)
2. [Jedan broker u Dockeru](#2-jedan-broker-u-dockeru)
3. [Topici, producer, consumer](#3-topici-producer-consumer)
4. [Klaster od 3 brokera (docker compose)](#4-klaster-od-3-brokera-docker-compose)
5. [Replicirani topik i simulacija otkaza](#5-replicirani-topik-i-simulacija-otkaza)
6. [Consumer grupe](#6-consumer-grupe)
7. [Python klijent](#7-python-klijent)
8. [Gašenje i reset](#8-gašenje-i-reset)
9. [Prevod komandi 2.8 → 4.x](#9-prevod-komandi-28--4x)

---

## 1. Šta se promenilo

| Kafka 2.8 (kurs)                        | Kafka 4.x (danas)                                                                |
| --------------------------------------- | -------------------------------------------------------------------------------- |
| **ZooKeeper** čuva metapodatke klastera | **KRaft**: Kafka sama, kroz _controller_ čvorove (Raft protokol)                 |
| ZooKeeper + broker = 2 procesa          | jedan proces može biti i broker i controller                                     |
| `--zookeeper localhost:2181`            | **ne postoji**, samo `--bootstrap-server`                                        |
| `--broker-list` kod producera           | **ne postoji**, samo `--bootstrap-server`                                        |
| ručno skidanje `.tgz`                   | zvanični Docker image `apache/kafka`                                             |
| Java 8+                                 | Java 17+ za brokere (u Dockeru ne brineš o tome)                                 |
| otkaz brokera primećen za ~18 s         | primećen za ~9 s (broker heartbeat ka controlleru)                               |
| rebalans consumer grupe radi klijent    | novi protokol (KIP-848): rebalans vodi broker, brži i bez „stop the world“ pauze |
| `kafka-python`                          | `confluent-kafka` (aktivno održavan, brži)                                       |

Koncepti (topic, partition, offset, leader, replica, ISR, consumer group) su **isti**.

---

## 2. Jedan broker u Dockeru

Umesto instalacije Jave i Kafke, plus pokretanja ZooKeepera i brokera u dva taba:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.0
```

Image je podešen za jedan čvor u KRaft modu (broker i controller u jednom). Port 9092 je odmah dostupan sa Maca.

CLI alati su **u kontejneru**, u `/opt/kafka/bin`:

```bash
docker exec -it kafka bash
cd /opt/kafka
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Ili bez ulaska u kontejner:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 3. Topici, producer, consumer

Sve komande se pokreću iz `/opt/kafka` u kontejneru. Iste su kao u kursu, samo uvek sa `--bootstrap-server`.

```bash
# topik
bin/kafka-topics.sh --create --topic test --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic test --bootstrap-server localhost:9092
bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092

# producer
bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092

# consumer
bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
```

Key / value:

```bash
bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092 \
  --property parse.key=true --property key.separator=:

bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Ako tvoja verzija prijavi da je `--property` zastareo, ista podešavanja idu preko `--reader-property` (producer), odnosno `--formatter-property` (consumer).

---

## 4. Klaster od 3 brokera (docker compose)

Umesto `cp server.properties server-1.properties` i ručnog menjanja `broker.id`, `listeners` i `log.dirs`, sve je u jednom `docker-compose.yml`:

```yaml
x-kafka-env: &kafka-env
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

services:
  kafka-1:
    image: apache/kafka:4.3.0
    container_name: kafka-1
    ports: ["9092:9092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: CONTROLLER://:29093,INTERNAL://:19092,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092

  kafka-2:
    image: apache/kafka:4.3.0
    container_name: kafka-2
    ports: ["9093:9093"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: CONTROLLER://:29093,INTERNAL://:19092,EXTERNAL://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-2:19092,EXTERNAL://localhost:9093

  kafka-3:
    image: apache/kafka:4.3.0
    container_name: kafka-3
    ports: ["9094:9094"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: CONTROLLER://:29093,INTERNAL://:19092,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-3:19092,EXTERNAL://localhost:9094
```

```bash
docker compose up -d
docker compose ps
```

Šta je šta:

| Podešavanje                                 | Zamenjuje iz kursa / značenje                                            |
| ------------------------------------------- | ------------------------------------------------------------------------ |
| `KAFKA_NODE_ID`                             | `broker.id`                                                              |
| `KAFKA_PROCESS_ROLES`                       | svaki čvor je i broker i controller (zamena za ZooKeeper)                |
| `KAFKA_CONTROLLER_QUORUM_VOTERS`            | ko glasa o metapodacima; zamena za `zookeeper.connect`                   |
| `KAFKA_LISTENERS`                           | `listeners`: tri porta: controller, brokeri među sobom, klijenti sa Maca |
| `KAFKA_ADVERTISED_LISTENERS`                | adresa koju broker javlja klijentima (`localhost:909x` za Mac)           |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3` | rešava problem sa `__consumer_offsets` na koji sam naleteo               |
| `CLUSTER_ID`                                | isti za sve čvorove; nov se pravi sa `bin/kafka-storage.sh random-uuid`  |
| `log.dirs`                                  | ne treba, svaki kontejner ima svoj disk                                  |

Postavka gde je svaki čvor i broker i controller (_combined mode_) je dobra za učenje. U produkciji su controlleri obično posebni čvorovi.

---

## 5. Replicirani topik i simulacija otkaza

```bash
docker exec -it kafka-1 bash
cd /opt/kafka

bin/kafka-topics.sh --create --topic my-replicated-topic --partitions 1 --replication-factor 3 --bootstrap-server kafka-1:19092
bin/kafka-topics.sh --describe --topic my-replicated-topic --bootstrap-server kafka-1:19092
```

```
Topic: my-replicated-topic  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

Otkaz: umesto `ps aux | grep` i `kill -9 PID`, sa Maca ubiješ kontejner lidera:

```bash
docker kill kafka-2
```

Posle ~9 s, ponovo `--describe`: novi lider, a `2` nestaje iz `Isr`. Čitanje i dalje radi:

```bash
bin/kafka-console-consumer.sh --topic my-replicated-topic --from-beginning \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092
```

Vraćanje brokera: `docker start kafka-2`.

Sa 3 controllera klaster preživljava pad **jednog** čvora. Ako padnu dva, nema većine (quorum) i klaster ne može da menja metapodatke.

---

## 6. Consumer grupe

```bash
bin/kafka-topics.sh --create --topic my-topic --partitions 3 --replication-factor 3 --bootstrap-server kafka-1:19092

# u više tabova; --group umesto --consumer-property group.id=g1
bin/kafka-console-consumer.sh --topic my-topic --group g1 --bootstrap-server kafka-1:19092

bin/kafka-consumer-groups.sh --list --bootstrap-server kafka-1:19092
bin/kafka-consumer-groups.sh --describe --group g1 --bootstrap-server kafka-1:19092
```

Pravilo je isto kao u kursu: svaka particija ide tačno jednom consumeru u grupi.

---

## 7. Python klijent

```bash
pip install confluent-kafka
```

Producer:

```python
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"})

for i in range(100):
    producer.produce("foobar", key=f"user{i % 3}", value=f"poruka {i}")

# send je asinhron, flush čeka da sve poruke stignu do brokera
producer.flush()
```

Consumer:

```python
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "group.id": "g1",
    "auto.offset.reset": "earliest",  # isto što i --from-beginning za novu grupu
})
consumer.subscribe(["foobar"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
        print(msg.key(), msg.value(), msg.partition(), msg.offset())
finally:
    consumer.close()
```

---

## 8. Gašenje i reset

```bash
# jedan broker
docker rm -f kafka

# klaster: ugasi i obriši podatke (zamena za rm -rf /tmp/kafka-logs /tmp/zookeeper ...)
docker compose down -v
```

---

## 9. Prevod komandi 2.8 → 4.x

| Kurs (2.8)                                                  | Danas (4.x)                                                   |
| ----------------------------------------------------------- | ------------------------------------------------------------- |
| `bin/zookeeper-server-start.sh config/zookeeper.properties` | — (nema ZooKeepera)                                           |
| `bin/kafka-server-start.sh config/server.properties`        | `docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.0`  |
| `cp server.properties server-1.properties` + izmene         | `docker-compose.yml` sa 3 servisa                             |
| `kafka-topics.sh ... --zookeeper localhost:2181`            | `kafka-topics.sh ... --bootstrap-server localhost:9092`       |
| `kafka-console-producer.sh --broker-list localhost:9092`    | `kafka-console-producer.sh --bootstrap-server localhost:9092` |
| `--consumer-property group.id=g1`                           | `--group g1`                                                  |
| `ps aux \| grep server-2.properties` + `kill -9 PID`        | `docker kill kafka-2`                                         |
| `bin/kafka-server-stop.sh` + `rm -rf /tmp/...`              | `docker compose down -v`                                      |
| `from kafka import KafkaProducer` (kafka-python)            | `from confluent_kafka import Producer`                        |

Izvori: [Kafka downloads](https://kafka.apache.org/community/downloads/), [Kafka Docker quickstart](https://kafka.apache.org/41/getting-started/docker), [KIP-975 (Docker image)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-975:+Docker+Image+for+Apache+Kafka)
