bin/kafka-consumer-groups.sh --bootstrap-server
localhost:9092 --describe --group g1bin/kafka-consumer-groups.sh --bootstrap-server
localhost:9092 --describe --group g1# Kafka – beleške

**Sadržaj**

1. [Kafka u jednoj rečenici](#1-kafka-u-jednoj-rečenici)
2. [Osnovni pojmovi](#2-osnovni-pojmovi)
3. [Instalacija (Kafka 2.8.1)](#3-instalacija-kafka-281)
4. [Pokretanje klastera](#4-pokretanje-klastera)
5. [Topici](#5-topici)
6. [Producer i consumer](#6-producer-i-consumer)
7. [Key / value poruke](#7-key--value-poruke)
8. [Multi-broker klaster](#8-multi-broker-klaster)
9. [Replicirani topik i simulacija otkaza](#9-replicirani-topik-i-simulacija-otkaza)
10. [Consumer grupe](#10-consumer-grupe)
11. [Gašenje i reset od nule](#11-gašenje-i-reset-od-nule)
12. [Greške na koje sam naleteo](#12-greške-na-koje-sam-naleteo)
13. [Kafka 2.8 vs 4.x](#13-kafka-28-vs-4x)

---

## 1. Kafka u jednoj rečenici

> Kafka je distribuirani log poruka: produceri upisuju poruke u topike, poruke se trajno čuvaju i repliciraju na više brokera, a consumeri ih čitaju svojim tempom, prateći dokle su stigli (offset).

Radi na **PUB-SUB** principu, ali za razliku od klasičnog message queue-a poruke se **ne brišu kad se pročitaju**, već ostaju dok ne istekne retention.

---

## 2. Osnovni pojmovi

| Pojam              | Šta je                                                                              |
| ------------------ | ----------------------------------------------------------------------------------- |
| **Broker**         | jedan Kafka server; klaster = više brokera                                          |
| **Topic**          | imenovani tok poruka (kao tabela/folder)                                            |
| **Partition**      | deo topika; omogućava **paralelizam**, redosled je garantovan samo unutar particije |
| **Offset**         | redni broj poruke u particiji; consumer pamti do kog offseta je pročitao            |
| **Producer**       | klijent koji **šalje** poruke                                                       |
| **Consumer**       | klijent koji **čita** poruke                                                        |
| **Consumer group** | grupa consumera; svaka particija ide **tačno jednom** članu grupe                   |
| **Leader**         | broker koji prima upise/čitanja za particiju                                        |
| **Replica**        | kopija particije na drugom brokeru                                                  |
| **ISR**            | In-Sync Replicas: replike koje su sinhronizovane sa liderom                         |
| **ZooKeeper**      | (samo stare verzije) čuva metapodatke klastera i bira kontrolera                    |

---

## 3. Instalacija (Kafka 2.8.1)

Java: radi sa **Javom 8 ili 11** (kod mene Zulu 8).

```bash
cd ~/Desktop/DE_Praksa/04_Kafka/kafka_instalacija
curl -O https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xzf kafka_2.12-2.8.1.tgz
rm kafka_2.12-2.8.1.tgz
cd kafka_2.12-2.8.1
```

- Stare verzije su na `archive.apache.org`, link iz materijala ne radi.
- Na Macu nema `wget`, koristi se `curl -O`.
- **Putanja ne sme imati razmak** (`DE Praksa` → `DE_Praksa`), Kafka skripte pucaju.
- Sve `bin/...` komande se pokreću **iz Kafka foldera**.

---

## 4. Pokretanje klastera

Svaki u svom terminalu/tabu:

```bash
# ZooKeeper – port 2181
bin/zookeeper-server-start.sh config/zookeeper.properties

# Broker 0 – port 9092
bin/kafka-server-start.sh config/server.properties
```

Broker je gore kad se u logu pojavi `started (kafka.server.KafkaServer)`.

---

## 5. Topici

Svaka `kafka-topics.sh` komanda mora da zna **gde je klaster**: `--zookeeper localhost:2181` (staro) ili `--bootstrap-server localhost:9092` (novo, jedino na 4.x).

```bash
# kreiranje
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

# lista
bin/kafka-topics.sh --list --zookeeper localhost:2181

# detalji (lider, replike, ISR)
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test

# brisanje – ime topika ide uz --topic
bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092
```

Posle brisanja `--list` još par sekundi može da pokazuje topik (`marked for deletion`), briše se u pozadini.

---

## 6. Producer i consumer

```bash
# producer – svaka linija koju otkucaš je jedna poruka
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test

# consumer – od prve poruke
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

Consumer **ne završava sam**, čeka nove poruke. Izlaz: `Ctrl + C`.

Korisne opcije consumera (`bin/kafka-console-consumer.sh --help`):

| Opcija                            | Šta radi                       |
| --------------------------------- | ------------------------------ |
| `--from-beginning`                | čita od prve poruke            |
| `--group g1`                      | ulazi u consumer grupu `g1`    |
| `--partition 0 --offset 5`        | samo particija 0, od offseta 5 |
| `--max-messages 10`               | izađe posle 10 poruka          |
| `--property print.key=true`       | prikaže ključ                  |
| `--property print.partition=true` | prikaže particiju              |
| `--property print.offset=true`    | prikaže offset                 |

---

## 7. Key / value poruke

Producer (`parse.key` je opcija **producera**, vidi se u `kafka-console-producer.sh --help`):

```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test \
  --property parse.key=true --property key.separator=:
```

```
>user1:kupio patike
>user2:dodao u korpu
>user1:platio
```

Consumer (`print.key` je opcija **consumera**):

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

**Zašto ključ:** poruke sa **istim ključem uvek idu u istu particiju** → redosled za taj ključ je garantovan (svi događaji za `user1` stižu redom).

---

## 8. Multi-broker klaster

```bash
cp config/server.properties config/server-1.properties
cp config/server.properties config/server-2.properties
```

Izmene u novim fajlovima:

| Podešavanje | server-1            | server-2            |
| ----------- | ------------------- | ------------------- |
| `broker.id` | `1`                 | `2`                 |
| `listeners` | `PLAINTEXT://:9093` | `PLAINTEXT://:9094` |
| `log.dirs`  | `/tmp/kafka-1`      | `/tmp/kafka-2`      |

```bash
bin/kafka-server-start.sh config/server-1.properties
bin/kafka-server-start.sh config/server-2.properties
```

Bolje u odvojenim tabovima nego sa `&`, da se vide logovi svakog brokera.

**Preporuka:** u sva tri fajla `offsets.topic.replication.factor=3` (pre prvog pokretanja), da interni topik `__consumer_offsets` preživi pad brokera (vidi [greške](#12-greške-na-koje-sam-naleteo)).

```bash
sed -i '' 's/^offsets.topic.replication.factor=1/offsets.topic.replication.factor=3/' \
  config/server.properties config/server-1.properties config/server-2.properties
grep offsets.topic.replication.factor config/server*.properties
```

---

## 9. Replicirani topik i simulacija otkaza

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic my-replicated-topic
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
```

```
Topic: my-replicated-topic  Partition: 0  Leader: 2  Replicas: 2,1,0  Isr: 2,1,0
```

- **Replicas** – na kojim brokerima su kopije; prvi u listi je _preferred leader_.
- **Leader** – ko trenutno prima upise/čitanja.
- **Isr** – replike koje su u sinhronizaciji.
- Redosled replika je **nasuman** pri kreiranju, kod svakoga ispadne drugačije – to je OK.

**Ubijanje lidera:**

```bash
# 1. nađi PID (drugi broj u redu)
ps aux | grep server-2.properties | grep -v grep

# 2. ubij ga – umesto PID ide broj
kill -9 19644

# ili sve odjednom
pkill -9 -f server-2.properties
```

Posle `kill -9` treba **~18 s** (ZooKeeper session timeout) da klaster primeti da broker ne postoji. Tek onda `--describe` pokazuje novog lidera i broker nestaje iz `Isr`. Kod normalnog gašenja (`Ctrl + C`) promena je odmah.

Čitanje posle otkaza – navesti **sve** brokere, klijent proba redom dok ne nađe živog:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --from-beginning --topic my-replicated-topic
```

---

## 10. Consumer grupe

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic my-topic

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-topic

# pokreni više ovakvih consumera (svaki u svom tabu)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --consumer-property group.id=g1

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group g1
```

- Svaka particija pripada **tačno jednom** consumeru u grupi.
- 3 particije + 3 consumera → svako čita jednu. 4. consumer bi bio besposlen.
- Različite grupe čitaju **iste** poruke nezavisno.

---

## 11. Gašenje i reset od nule

```bash
bin/kafka-server-stop.sh        # gasi sve brokere
bin/zookeeper-server-stop.sh

# provera – ne sme ništa da ispiše
ps aux | grep -E "kafka|zookeeper" | grep -v grep

# brisanje svih podataka (topici, poruke); config fajlovi ostaju
rm -rf /tmp/kafka-logs /tmp/kafka-1 /tmp/kafka-2 /tmp/zookeeper
```

Zatim ponovo [pokretanje](#4-pokretanje-klastera) + [dodatni brokeri](#8-multi-broker-klaster).

---

## 12. Greške na koje sam naleteo

| Greška                                                            | Uzrok / rešenje                                                                                               |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `Only one of --bootstrap-server or --zookeeper must be specified` | fali adresa klastera; dodati `--bootstrap-server localhost:9092`                                              |
| `--delete test` ne radi                                           | ime topika mora uz `--topic test`                                                                             |
| `--boostrap-server` ne radi                                       | typo: **boo`t`strap**                                                                                         |
| `zsh: no such file or directory: bin/...`                         | nisam u Kafka folderu → `cd .../kafka_2.12-2.8.1`                                                             |
| `grep: kill: No such file or directory`                           | spojio sam dve komande u jednu; `PID` je mesto za broj, ne reč                                                |
| `Connection to node -1 (localhost:9092) could not be established` | ubio sam broker 0 (port 9092), a consumer se kačio baš na njega → navesti sve brokere                         |
| consumer visi, ništa ne ispisuje posle pada brokera               | `__consumer_offsets` sa RF=1 je bio na mrtvom brokeru (`Leader: none`) → `offsets.topic.replication.factor=3` |
| posle `kill -9` lider se ne menja                                 | čekati ~18 s (ZooKeeper session timeout)                                                                      |

Provera internog topika:

```bash
bin/kafka-topics.sh --describe --topic __consumer_offsets --bootstrap-server localhost:9093
```

---

## 13. Kafka 2.8 vs 4.x

Koncepti su isti, menjaju se komande:

| Kafka 2.8 (kurs)                  | Kafka 4.x (danas)                                        |
| --------------------------------- | -------------------------------------------------------- |
| ZooKeeper + brokeri               | **KRaft** – Kafka sama vodi metapodatke, nema ZooKeepera |
| `--zookeeper localhost:2181`      | `--bootstrap-server localhost:9092`                      |
| `--broker-list` (producer)        | `--bootstrap-server`                                     |
| Java 8+                           | Java 11+ (klijenti), Java 17+ (brokeri)                  |
| `wurstmeister/kafka` Docker image | zvanični `apache/kafka`                                  |
| `kafka-python`                    | `confluent-kafka` (aktivno održavan, brži)               |
