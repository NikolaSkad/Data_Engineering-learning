# Kafka i Kafka Streams – teorija

**Sadržaj**

1. [Kafka u jednoj rečenici](#1-kafka-u-jednoj-rečenici)
2. [Osnovni pojmovi](#2-osnovni-pojmovi)
3. [Topic, particija, offset](#3-topic-particija-offset)
4. [Producer i ključ poruke](#4-producer-i-ključ-poruke)
5. [Consumer i consumer grupe](#5-consumer-i-consumer-grupe)
6. [Replikacija i otkazi](#6-replikacija-i-otkazi)
7. [ZooKeeper](#7-zookeeper)
8. [Kafka API-ji i klijenti](#8-kafka-api-ji-i-klijenti)
9. [Kafka Streams – šta je i zašto](#9-kafka-streams--šta-je-i-zašto)
10. [Topologija i obrađivači](#10-topologija-i-obrađivači)
11. [KStream, KTable, GlobalKTable](#11-kstream-ktable-globalktable)
12. [Transformacije](#12-transformacije)
13. [Primeri iz kursa](#13-primeri-iz-kursa)
14. [Brza pitanja za intervju](#14-brza-pitanja-za-intervju)

---

## 1. Kafka u jednoj rečenici

> Kafka je distribuirana platforma za tokove podataka: produceri upisuju poruke u topike, Kafka ih trajno čuva i replicira na više brokera, a consumeri ih čitaju svojim tempom.

Radi na **PUB-SUB** principu i omogućava:

- **skaliranje**: više brokera, više particija
- **brz protok** poruka
- **replikaciju** poruka, pa ih pad jednog servera ne gubi
- **trajno čuvanje**: pročitana poruka se ne briše, ostaje dok ne istekne retention

**Primena:** komunikacija između servisa (mikroservisi), prikupljanje logova i događaja (klikovi, transakcije, senzori), ulaz za obradu u realnom vremenu (Kafka Streams, Spark Streaming), most između izvora podataka i data lake-a.

---

## 2. Osnovni pojmovi

**Analogija – pošta sa pregradama:**

| Kafka              | Pošta                                                                   |
| ------------------ | ----------------------------------------------------------------------- |
| **Broker**         | jedna poslovnica; više poslovnica = klaster                             |
| **Topic**          | vrsta pošiljke (npr. „računi“), ima svoje pregrade                      |
| **Partition**      | jedna pregrada; pisma u njoj su poređana po redu                        |
| **Offset**         | redni broj pisma u pregradi                                             |
| **Producer**       | onaj ko šalje pisma                                                     |
| **Consumer**       | onaj ko čita pisma i pamti dokle je stigao                              |
| **Consumer group** | tim čitača koji dele pregrade, svaku pregradu čita tačno jedan iz tima  |
| **Replika**        | fotokopija pregrade u drugoj poslovnici                                 |

---

## 3. Topic, particija, offset

- **Topic** je imenovani tok poruka, na primer `pek-sha` ili `orders`.
- Topic je podeljen na **particije**. Particije se nalaze na različitim brokerima, pa se upis i čitanje **paralelizuju**.
- Particija je **log**: poruke se samo **dodaju na kraj** i ne menjaju se.
- **Offset** je redni broj poruke u particiji (0, 1, 2...).
- Za svakog consumera topika postoji offset, odnosno broj do kog su poruke pročitane. Zato consumer posle pada nastavlja tamo gde je stao.
- **Redosled je garantovan samo unutar jedne particije**, ne u celom topiku.

```
topic: orders
  particija 0:  [0][1][2][3][4] →
  particija 1:  [0][1][2] →
  particija 2:  [0][1][2][3] →
```

---

## 4. Producer i ključ poruke

Poruka je par **ključ – vrednost** (ključ nije obavezan).

| Poruka       | U koju particiju ide                                            |
| ------------ | --------------------------------------------------------------- |
| **bez ključa** | raspoređuje se po particijama, bez garancije redosleda        |
| **sa ključem** | `hash(ključ) % broj_particija` → **isti ključ uvek u istu particiju** |

Zato se ključ koristi kada je redosled bitan po entitetu. Na primer, svi događaji za `user1` završe u istoj particiji i čitaju se tačno redom kojim su poslati.

---

## 5. Consumer i consumer grupe

- **Consumer grupa:** poruka dolazi **tačno jednom članu grupe**.
- Svaka particija je dodeljena **jednom** consumeru u grupi, a jedan consumer može čitati više particija.
- **Više consumera = više paralelizma**, ali najviše do broja particija.

| Particija / consumera u grupi | Rezultat                                    |
| ----------------------------- | ------------------------------------------- |
| 3 particije, 1 consumer       | jedan čita sve tri                          |
| 3 particije, 3 consumera      | svaki čita po jednu                         |
| 3 particije, 4 consumera      | jedan je **besposlen**                      |

- **Različite grupe** čitaju **iste** poruke nezavisno, svaka sa svojim offsetom. Tako isti topik čitaju, na primer, i servis za analitiku i servis za notifikacije.
- Kad consumer uđe u grupu ili izađe iz nje, Kafka **ponovo raspoređuje** particije (*rebalance*).

---

## 6. Replikacija i otkazi

- **Replication factor** određuje koliko kopija ima svaka particija (npr. 3).
- Za svaku particiju jedan broker je **leader**: na njega se šalju poruke i on se stara o replikama.
- Ostali brokeri su **followeri**: kopiraju podatke od lidera.
- **ISR** (In-Sync Replicas) su replike koje su sustigle lidera.

```
Partition: 0   Leader: 2   Replicas: 2,1,0   Isr: 2,1,0
```

**Otkaz:** kad lider padne, za novog lidera se bira jedna od replika iz ISR. Poruke su i dalje dostupne, a otkazani broker nestaje iz ISR.

- Replike su na **različitim brokerima**, pa replication factor ne može biti veći od broja brokera.
- Particije služe za **paralelizam**, a replike za **otpornost na otkaze**.

---

## 7. ZooKeeper

U verziji iz kursa (2.8), **ZooKeeper** čuva metapodatke klastera: koji brokeri su živi, koji topici postoje i ko je lider. Kafka bez njega ne može da radi.

Danas (Kafka 4.x) ZooKeepera **više nema**. Kafka sama vodi metapodatke kroz **KRaft** (vidi [kafka_danas.md](kafka_danas.md)).

---

## 8. Kafka API-ji i klijenti

| API              | Šta radi                                                            |
| ---------------- | ------------------------------------------------------------------- |
| **Producer API** | slanje poruka u topike                                              |
| **Consumer API** | čitanje poruka iz topika                                            |
| **Streams API**  | obrada tokova: čita iz topika, transformiše, upisuje u topike       |
| **Connect API**  | gotovi konektori za prenos podataka između Kafke i baza i sistema   |

Izvorni API je u **Javi**, a postoje klijenti za većinu jezika: C/C++, Python, Go, Node.js, .NET...

```python
from kafka import KafkaProducer
producer = KafkaProducer()
producer.send('foobar', b'test')

from kafka import KafkaConsumer
for msg in KafkaConsumer('foobar'):
    print(msg)
```

---

## 9. Kafka Streams – šta je i zašto

> Kafka Streams je **klijentska Java biblioteka** za pravljenje aplikacija koje obrađuju tokove podataka. Ulaz i izlaz su Kafka topici.

**Prednosti:**

- **nije potreban poseban klaster** za obradu (za razliku od Sparka): to je obična Java aplikacija
- obrada **zapis po zapis** (*record-at-a-time*), bez micro-batch-a
- paralelizam, particionisanje, skalabilnost i otpornost na otkaze dobija **od Kafke**, jer ispod koristi producere i consumere
- lako rukuje **zakasnelim** i **out-of-order** podacima

**Ključna ideja:** teške probleme (distribucija, otkazi, redosled) prepušta Kafki.

**Pojmovi:**

- **Data stream**: neograničen, stalno ažuriran niz **nepromenljivih** zapisa. Uređen je, može se ponovo pročitati (*replayable*) i otporan je na otkaze.
- **Data record**: par **ključ – vrednost**.

---

## 10. Topologija i obrađivači

**Topologija** je **usmereni aciklični graf** (DAG):

- **čvorovi** su obrađivači toka (*stream processors*), a **ivice** su tokovi podataka između njih
- obrađivač prima zapis po zapis, primeni operaciju i, ako treba, pošalje rezultat dalje (nizvodno)

```
[Source: pek-sha] → [filter] → [map] → [Sink: pek-sha-night]
```

| Vrsta obrađivača     | Uloga                                         |
| -------------------- | --------------------------------------------- |
| **Source processor** | čita iz topika, nema obrađivača pre sebe       |
| **Sink processor**   | upisuje u topik, nema obrađivača posle sebe    |
| običan obrađivač     | transformiše podatke između njih               |

**Kafka Streams DSL** je deklarativan, funkcionalan način da se opiše topologija (`filter`, `map`, `groupBy`...). To je apstrakcija nad nižim *Processor API*-jem.

---

## 11. KStream, KTable, GlobalKTable

| Apstrakcija      | Kako tumači zapis                                                                 | Primer                       |
| ---------------- | --------------------------------------------------------------------------------- | ---------------------------- |
| **KStream**      | svaki zapis je **novi događaj**, zapisi se samo **dodaju**                        | svaka kupovina               |
| **KTable**       | svaki zapis je **ažuriranje** vrednosti za taj ključ (*upsert*); `null` = brisanje | trenutno stanje računa       |
| **GlobalKTable** | kao KTable, ali svaka instanca aplikacije dobija podatke iz **svih** particija    | mali šifarnik (npr. kodovi)  |

```
Zapisi:   (A, 1)  (B, 5)  (A, 3)

KStream:  A=1, B=5, A=3      → tri događaja
KTable:   A=3, B=5           → poslednja vrednost po ključu
```

Odnos tabele i toka: **tok** je istorija promena, a **tabela** je trenutno stanje dobijeno iz te istorije.

---

## 12. Transformacije

**Stateless** (svaki zapis se obrađuje nezavisno, ništa se ne pamti):

| Operacija                 | Šta radi                                          |
| ------------------------- | ------------------------------------------------- |
| `filter` / `filterNot`    | propušta / odbacuje zapise po uslovu              |
| `map` / `mapValues`       | menja ključ i vrednost / samo vrednost            |
| `flatMap` / `flatMapValues` | jedan zapis → više zapisa                       |
| `branch`                  | deli tok na više tokova po uslovima               |
| `merge`                   | spaja dva toka u jedan                            |
| `selectKey`               | postavlja novi ključ                              |
| `groupBy` / `groupByKey`  | grupiše (priprema za agregaciju)                  |
| `foreach`, `peek`, `print` | sporedni efekti, ispis                           |
| `toStream`                | KTable → KStream                                  |

**Stateful** (pamte stanje između zapisa):

| Operacija                       | Šta radi                                      |
| ------------------------------- | --------------------------------------------- |
| `count`, `reduce`, `aggregate`  | agregacije po ključu (rezultat je KTable)     |
| `join`, `leftJoin`, `outerJoin` | spajanje tokova i tabela po ključu            |
| windowing                       | agregacija u vremenskom prozoru               |

Stateful operacije čuvaju stanje lokalno (*state store*), a Kafka ga čuva i u pozadinskom topiku, pa se stanje ne gubi kad aplikacija padne.

---

## 13. Primeri iz kursa

Skup podataka: letovi **Peking – Šangaj** (cena, tip letelice, broj leta...). **Producer** na svakih 300 ms čita red iz `pek-sha.csv` i šalje ga na topik `pek-sha`.

| Primer                   | Operacija                                            |
| ------------------------ | ---------------------------------------------------- |
| `FlightNumberCount`      | `groupBy` broj leta + `count`                        |
| `BranchByCraftExample`   | `branch` po tipu letelice                            |
| `JoinExample`            | `join` toka letova sa topikom `codes` (ključ `HO1252`) |
| `PriceClassMeanExample`  | `aggregate`: prosečna cena po klasi (sopstveni serde) |
| `TraFilterExample`       | `filter`                                             |

**Zadaci:**

1. map → broj leta, dužina leta i cena → topik `pek-sha-dur`
2. filter poletanja između 0h i 7h → topik `pek-sha-night`
3. broj letova po danu → `System.out`
4. brojanje reči iz reddit komentara

**Serde** (*serializer/deserializer*) govori Kafki kako da pretvori ključ i vrednost u bajtove i nazad. Za sopstvene klase (npr. par suma i broj za prosek) piše se namenski serde.

---

## 14. Brza pitanja za intervju

| Pitanje                                         | Odgovor                                                                                         |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Da li Kafka briše poruku kad se pročita?        | **Ne.** Poruke ostaju dok ne istekne retention, a consumer samo pomera svoj offset.             |
| Čemu služe particije?                           | **Paralelizmu**: više brokera piše, više consumera čita istovremeno.                            |
| Da li je redosled garantovan?                   | Samo **unutar particije**.                                                                      |
| Kako garantovati redosled za jednog korisnika?  | Poslati ga sa **ključem**, jer isti ključ uvek ide u istu particiju.                            |
| Šta je offset?                                  | Redni broj poruke u particiji. Consumer pamti do kog je stigao.                                  |
| Šta je consumer grupa?                          | Grupa consumera gde svaku particiju čita **tačno jedan** član.                                   |
| Više consumera nego particija?                  | Višak consumera je **besposlen**.                                               |
| Particija vs replika?                           | Particija daje **paralelizam**, replika daje **otpornost na otkaze**.                            |
| Šta je leader?                                  | Broker koji prima upise i čitanja za particiju. Ostali ga kopiraju.                             |
| Šta je ISR?                                     | Replike koje su sinhronizovane sa liderom. Iz njih se bira novi lider.                          |
| Čemu je služio ZooKeeper?                       | Metapodaci klastera, izbor kontrolera. U Kafki 4.x zamenjen je **KRaft**-om.                    |
| Kafka Streams vs Spark Streaming?               | Kafka Streams: biblioteka bez klastera, zapis po zapis. Spark: poseban klaster, micro-batch.     |
| KStream vs KTable?                              | KStream: svaki zapis je **novi događaj**. KTable: svaki zapis je **ažuriranje** po ključu.       |
| Stateless vs stateful?                          | Stateless ne pamti ništa (filter, map). Stateful pamti stanje (count, join, aggregate).          |
| Šta je topologija?                              | DAG obrađivača: source → transformacije → sink.                                                  |

**Ključni termini:** broker, topic, partition, offset, producer, consumer, consumer group, rebalance, leader, follower, replica, replication factor, ISR, retention, ZooKeeper, KRaft, Kafka Streams, topologija, source/sink processor, DSL, KStream, KTable, GlobalKTable, stateless, stateful, state store, serde, windowing.
